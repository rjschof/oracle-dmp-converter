"""High-level conversion orchestration.

Format-specific branching (modern Data Pump vs legacy exp/imp) lives in
:class:`~oracle_dmp_converter.datapump.workflow.DumpWorkflow` and its
sub-package implementations.  The converter owns a single ``_workflow``
attribute and delegates all dump-format-specific work to it.
"""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import ConverterConfig, column_override
from oracle_dmp_converter.datapump.workflow import (
    DumpWorkflow,
    WorkflowConfig,
    create_workflow,
)
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.io.state import ChunkState, StateStore
from oracle_dmp_converter.models import (
    ChunkPlan,
    ConversionPlan,
    DumpFormat,
    DumpManifest,
    OutputFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    count_rows,
    drop_schema,
    ensure_schema,
    ensure_tablespace,
    grant_quota_unlimited,
    oracle_connection,
    truncate_table,
)
from oracle_dmp_converter.oracle.exporter import export_table
from oracle_dmp_converter.oracle.identifiers import filesystem_safe_identifier
from oracle_dmp_converter.oracle.metadata import discover_table_metadata

LOGGER = logging.getLogger(__name__)

_STAGE_SCHEMA_PREFIX = "DMP_"

# Number of tables combined into a single impdp/imp invocation.  Batching
# reduces per-table process-startup overhead at the cost of coarser
# resumability granularity (a failed batch re-imports all its tables).
TABLE_IMPORT_BATCH_SIZE = 20


@dataclass(frozen=True)
class OracleAdminConnection:
    """Connection parameters for an Oracle administrative user.

    Used to create ``system``-level connections for staging schema management
    and to provide port/service information to runner helpers.

    Attributes:
        host: Hostname or IP address of the Oracle server.
        port: TCP port number (typically ``1521`` for the container).
        service: Oracle service name (e.g. ``"FREE"``).
        user: Administrative Oracle username (e.g. ``"system"``).
        password: Password for *user*.
    """

    host: str
    port: int
    service: str
    user: str
    password: str


@dataclass(frozen=True)
class ChunkConversionResult:
    """Result of converting a single chunk (one output file).

    Attributes:
        name: Chunk identifier, matching the :attr:`ChunkPlan.name` that
            produced this result.
        imported_rows: Rows counted in the staging schema after import.
        output_rows: Rows written to the output file.
        output_path: Absolute path to the written output file.
    """

    name: str
    imported_rows: int
    output_rows: int
    output_path: Path


@dataclass(frozen=True)
class TableConversionResult:
    """Aggregated result of converting all chunks for one table.

    Attributes:
        source_schema: Original Oracle schema name.
        table: Table name.
        chunks: Results for each converted chunk.
    """

    source_schema: str
    table: str
    chunks: tuple[ChunkConversionResult, ...] = field(default_factory=tuple)

    @property
    def rows(self) -> int:
        """Total output rows across all chunks for this table.

        Returns:
            Sum of :attr:`ChunkConversionResult.output_rows` for every chunk.
        """
        return sum(chunk.output_rows for chunk in self.chunks)


@dataclass(frozen=True)
class PlanConversionResult:
    """Aggregated result of converting all tables in a plan.

    Attributes:
        tables: Results for each successfully converted table.
        started_at: UTC datetime when :meth:`convert_plan` began.
        completed_at: UTC datetime when :meth:`convert_plan` finished.
    """

    tables: tuple[TableConversionResult, ...]
    started_at: datetime
    completed_at: datetime

    @property
    def rows(self) -> int:
        """Total output rows across all tables in the plan.

        Returns:
            Sum of :attr:`TableConversionResult.rows` for every table.
        """
        return sum(table.rows for table in self.tables)


def _chunk_output_path(
    *,
    source_schema: str,
    table: str,
    chunk_name: str,
    output_dir: Path,
    output_format: OutputFormat,
) -> Path:
    """Return the on-disk path for one converted chunk.

    Layout: ``<output_dir>/<schema>/<table>/<chunk>.<ext>`` with names run
    through :func:`~oracle_dmp_converter.oracle.identifiers.filesystem_safe_identifier`.

    Args:
        source_schema: Oracle schema name (case-preserved).
        table: Oracle table name (case-preserved).
        chunk_name: Chunk identifier, e.g. ``"whole"`` or
            ``"partition-00001-P_NORTH"``.
        output_dir: Root output directory.
        output_format: Target :class:`~oracle_dmp_converter.models.OutputFormat`
            which determines the file extension.

    Returns:
        Absolute :class:`~pathlib.Path` for the output file.
    """
    return (
        output_dir
        / filesystem_safe_identifier(source_schema)
        / filesystem_safe_identifier(table)
        / f"{filesystem_safe_identifier(chunk_name)}.{output_format.value}"
    )


class OracleDumpConverter:
    """Orchestrates inspect → convert against a running Oracle container.

    All dump-format-specific work (impdp vs imp, SQLFILE vs INDEXFILE,
    parfile syntax) is delegated to :attr:`_workflow`, which is either set
    by :meth:`inspect_dump` (auto-detected) or :meth:`use_format`
    (caller-known, e.g. for convert-only runs from a saved plan).
    """

    def __init__(
        self,
        *,
        container: DockerOracle,
        admin: OracleAdminConnection,
        work_dir: Path,
        dumpfiles: tuple[str, ...],
        directory: str = "DATA_PUMP_DIR",
        directory_path: str = "/opt/oracle/admin/FREE/dpdump",
        discovery_directory: str = "ORACLE_DMC_DISCOVERY",
        inspect_directory: str = "ORACLE_DMC_INSPECT",
        convert_directory: str = "ORACLE_DMC_CONVERT",
        stage_password: str = "StagePwd_123",
        output_format: OutputFormat = OutputFormat.PARQUET,
        config: ConverterConfig | None = None,
    ) -> None:
        """Initialise the converter.

        Args:
            container: Running Oracle Free Docker/Podman container.
            admin: Administrative connection parameters for the container.
            work_dir: Host-side working directory for intermediate artefacts
                such as generated parfiles and Data Pump logs.
            dumpfiles: Bare filenames of the ``.dmp`` files as seen inside the
                container's dump directory.
            directory: Oracle DIRECTORY object name mapping to the dump path
                inside the container.
            directory_path: Container-side path that *directory* points to.
            stage_password: Password for automatically created staging schemas.
            output_format: Target output format for converted data.
            config: Optional per-table and per-column overrides.  Defaults to
                an empty :class:`~oracle_dmp_converter.config.ConverterConfig`.
        """
        self.container = container
        self.admin = admin
        self.work_dir = work_dir
        self.dumpfiles = dumpfiles
        self.directory = directory
        self.directory_path = directory_path.rstrip("/")
        self.discovery_directory = discovery_directory
        self.inspect_directory = inspect_directory
        self.convert_directory = convert_directory
        self.stage_password = stage_password
        self.output_format = output_format
        self.config = config if config is not None else ConverterConfig()
        # Set lazily by inspect_dump() or use_format().
        self._workflow: DumpWorkflow | None = None

    # ------------------------------------------------------------------
    # Workflow lifecycle
    # ------------------------------------------------------------------

    @property
    def dump_format(self) -> DumpFormat:
        """Return the detected dump format. Raises if no workflow is set."""
        if self._workflow is None:
            raise RuntimeError(
                "dump_format is unavailable; call inspect_dump() or use_format() first"
            )
        return self._workflow.dump_format

    def _credentials(self) -> OracleCredentials:
        """Build an :class:`~oracle_dmp_converter.oracle.conn.OracleCredentials` from admin config.

        Returns:
            Credentials struct for the admin user.
        """
        return OracleCredentials(
            user=self.admin.user,
            password=self.admin.password,
            service=self.admin.service,
        )

    def _workflow_config(self) -> WorkflowConfig:
        """Assemble a :class:`~oracle_dmp_converter.datapump.workflow.WorkflowConfig`.

        Returns:
            Config struct wiring together container, credentials, directories,
            and dump filenames for workflow construction.
        """
        return WorkflowConfig(
            credentials=self._credentials(),
            directory=self.directory,
            directory_path=self.directory_path,
            dumpfiles=self.dumpfiles,
            container=self.container,
            work_dir=self.work_dir,
            discovery_directory=self.discovery_directory,
            inspect_directory=self.inspect_directory,
            convert_directory=self.convert_directory,
        )

    def use_format(self, dump_format: DumpFormat) -> None:
        """Initialise :attr:`_workflow` for a known dump format.

        Used by convert-only flows where the format is recorded in the
        saved plan and the auto-detection probe is unnecessary.
        """
        # Import here to avoid circular imports.
        from oracle_dmp_converter.datapump.legacy.workflow import (  # noqa: PLC0415
            LegacyDumpWorkflow,
            make_legacy_runners,
        )
        from oracle_dmp_converter.datapump.modern.workflow import (  # noqa: PLC0415
            DataPumpWorkflow,
            make_modern_runners,
        )

        cfg = self._workflow_config()
        if dump_format is DumpFormat.LEGACY:
            legacy_discovery, legacy_inspect, legacy_convert = make_legacy_runners(
                cfg.container, cfg.work_dir
            )
            self._workflow = LegacyDumpWorkflow(
                credentials=cfg.credentials,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                discovery_runner=legacy_discovery,
                discovery_dir=cfg.work_dir / "discovery",
                inspect_runner=legacy_inspect,
                convert_runner=legacy_convert,
            )
        else:
            discovery_runner, inspect_runner, convert_runner = make_modern_runners(
                cfg.container, cfg.work_dir
            )
            self._workflow = DataPumpWorkflow(
                credentials=cfg.credentials,
                directory=cfg.directory,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                discovery_runner=discovery_runner,
                discovery_dir=cfg.work_dir / "discovery",
                inspect_runner=inspect_runner,
                convert_runner=convert_runner,
                discovery_directory=cfg.discovery_directory,
                inspect_directory=cfg.inspect_directory,
                convert_directory=cfg.convert_directory,
            )

    def _require_workflow(self) -> DumpWorkflow:
        """Return the active workflow or raise if not yet initialised.

        Returns:
            The current :class:`~oracle_dmp_converter.datapump.workflow.DumpWorkflow`.

        Raises:
            RuntimeError: If neither :meth:`inspect_dump` nor
                :meth:`use_format` has been called.
        """
        if self._workflow is None:
            raise RuntimeError("No workflow active; call inspect_dump() or use_format() first")
        return self._workflow

    # ------------------------------------------------------------------
    # Staging schema / connection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _stage_schema_for(source_schema: str) -> str:
        """Return the staging schema name (``DMP_<source_schema>``)."""
        return f"{_STAGE_SCHEMA_PREFIX}{source_schema}"

    def _connect(self) -> AbstractContextManager[oracledb.Connection]:
        """Return a context manager that yields an admin Oracle connection.

        Returns:
            Context manager yielding a connected
            :class:`oracledb.Connection` for the container's admin user.
        """
        return oracle_connection(
            host=self.admin.host,
            port=self.admin.port,
            service=self.admin.service,
            user=self.admin.user,
            password=self.admin.password,
        )

    def _required_tablespaces(self) -> frozenset[str]:
        """Tablespaces that must exist before importing (legacy dumps only)."""
        if self._workflow is None:
            return frozenset()
        return self._workflow.required_tablespaces()

    def prepare_stage_schema(self, source_schema: str) -> None:
        """Create or verify the staging schema for *source_schema*.

        Creates the ``DMP_<source_schema>`` user/schema if it does not exist,
        and ensures all tablespaces required by the dump have been created with
        unlimited quota granted to the staging schema.

        Args:
            source_schema: Original Oracle schema name from the dump.
        """
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Ensuring staging schema %s for source %s", stage_schema, source_schema)
        with self._connect() as conn:
            ensure_schema(conn, stage_schema, self.stage_password)
            for tablespace in self._required_tablespaces():
                ensure_tablespace(conn, tablespace)
                grant_quota_unlimited(conn, stage_schema, tablespace)

    def drop_stage_schema(self, source_schema: str) -> None:
        """Drop the ``DMP_<source_schema>`` staging schema and all its objects.

        Args:
            source_schema: Original Oracle schema name from the dump.
        """
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Dropping staging schema %s", stage_schema)
        with self._connect() as conn:
            drop_schema(conn, stage_schema)

    # ------------------------------------------------------------------
    # Inspect
    # ------------------------------------------------------------------

    def _disable_triggers(self, source_schema: str) -> None:
        """Disable all triggers on tables in the staging schema for *source_schema*.

        Queries ``ALL_TRIGGERS`` and issues ``ALTER TRIGGER ... DISABLE`` for each.
        For modern dumps this is typically a no-op because triggers are excluded
        at import time; it runs unconditionally for correctness on legacy dumps.
        """
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Disabling triggers on staging schema %s", stage_schema)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT TRIGGER_NAME FROM ALL_TRIGGERS WHERE OWNER = :schema",
                    schema=stage_schema,
                )
                triggers = [row[0] for row in cursor.fetchall()]
            for trigger_name in triggers:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'ALTER TRIGGER "{stage_schema}"."{trigger_name}" DISABLE'
                    )
                LOGGER.debug("Disabled trigger %s.%s", stage_schema, trigger_name)

    def _drop_vpd_policies(self, source_schema: str) -> None:
        """Drop all VPD (DBMS_RLS) policies on tables in the staging schema.

        Queries ``ALL_POLICIES`` and calls ``DBMS_RLS.DROP_POLICY`` or
        ``DBMS_RLS.DROP_GROUPED_POLICY`` as appropriate.  Policies are dropped
        rather than disabled so they cannot interfere with data import or export.
        """
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Dropping VPD policies on staging schema %s", stage_schema)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT OBJECT_NAME, POLICY_NAME, POLICY_GROUP
                    FROM ALL_POLICIES
                    WHERE OBJECT_OWNER = :schema
                    """,
                    schema=stage_schema,
                )
                policies = cursor.fetchall()
            for object_name, policy_name, policy_group in policies:
                with conn.cursor() as cursor:
                    if policy_group and policy_group != "SYS_DEFAULT":
                        cursor.execute(
                            """
                            BEGIN
                                DBMS_RLS.DROP_GROUPED_POLICY(
                                    object_schema => :schema,
                                    object_name   => :obj,
                                    policy_group  => :grp,
                                    policy_name   => :pol
                                );
                            END;
                            """,
                            schema=stage_schema,
                            obj=object_name,
                            grp=policy_group,
                            pol=policy_name,
                        )
                    else:
                        cursor.execute(
                            """
                            BEGIN
                                DBMS_RLS.DROP_POLICY(
                                    object_schema => :schema,
                                    object_name   => :obj,
                                    policy_name   => :pol
                                );
                            END;
                            """,
                            schema=stage_schema,
                            obj=object_name,
                            pol=policy_name,
                        )
                LOGGER.debug(
                    "Dropped VPD policy %s on %s.%s", policy_name, stage_schema, object_name
                )

    def _dematerialize_mviews(self, source_schema: str) -> None:
        """Replace any materialized views in the staging schema with plain tables.

        When ``imp`` or ``impdp`` creates a materialized view in the staging
        schema, subsequent row imports fail because DML is not permitted on a
        materialized view.  This method detects such objects and converts them
        to ordinary heap tables by:

        1. Creating an empty ``CTAS`` copy under a temporary name.
        2. Dropping the materialized view (which also drops its underlying
           table segment).
        3. Renaming the empty table to the original name.
        """
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT MVIEW_NAME FROM ALL_MVIEWS WHERE OWNER = :schema",
                    schema=stage_schema,
                )
                mviews = [row[0] for row in cursor.fetchall()]
        if not mviews:
            return
        LOGGER.info(
            "Converting %d materialized view(s) to plain tables in staging schema %s: %s",
            len(mviews),
            stage_schema,
            ", ".join(mviews),
        )
        with self._connect() as conn:
            for mview_name in mviews:
                tmp_name = f"{mview_name[:120]}_$TMP"
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'CREATE TABLE "{stage_schema}"."{tmp_name}" AS '
                        f'SELECT * FROM "{stage_schema}"."{mview_name}" WHERE 1=0'
                    )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'DROP MATERIALIZED VIEW "{stage_schema}"."{mview_name}"'
                    )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'ALTER TABLE "{stage_schema}"."{tmp_name}" RENAME TO "{mview_name}"'
                    )
                LOGGER.debug(
                    "Converted materialized view %s.%s to plain table",
                    stage_schema,
                    mview_name,
                )

    def _apply_byte_to_char(self, source_schema: str) -> None:
        """Convert all ``BYTE``-length string columns to ``CHAR`` semantics.

        Queries ``ALL_TAB_COLUMNS`` for ``VARCHAR2`` and ``CHAR`` columns with
        ``CHAR_USED = 'B'`` and issues ``ALTER TABLE ... MODIFY`` for each.
        This prevents row-level truncation errors that arise when importing a
        single-byte-charset dump into an ``AL32UTF8`` staging database.

        Columns that cannot have their length semantics changed are excluded:

        * **Virtual columns** (excluded via ``ALL_TABLE_VIRTUAL_COLUMNS``) — modifying
          them raises ``ORA-54017``.
        * **Partition key columns** — modifying them raises ``ORA-14060``
          for both the modern Data Pump and legacy ``imp`` paths.  These
          columns are excluded, and the partition structure is left in place.
        * **Subpartition key columns** — same reasoning as partition keys.
        """
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Applying BYTE\u2192CHAR column adjustments on staging schema %s", stage_schema)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.CHAR_LENGTH
                    FROM ALL_TAB_COLUMNS tc
                    WHERE tc.OWNER = :schema
                      AND tc.DATA_TYPE IN ('VARCHAR2', 'CHAR')
                      AND tc.CHAR_USED = 'B'
                      AND NOT EXISTS (
                          SELECT 1 FROM ALL_TABLE_VIRTUAL_COLUMNS vc
                          WHERE vc.TABLE_OWNER = :schema
                            AND vc.TABLE_NAME = tc.TABLE_NAME
                            AND vc.VIRTUAL_COLUMN_NAME = tc.COLUMN_NAME
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM ALL_PART_KEY_COLUMNS pk
                          WHERE pk.OWNER = :schema
                            AND pk.NAME = tc.TABLE_NAME
                            AND pk.COLUMN_NAME = tc.COLUMN_NAME
                            AND pk.OBJECT_TYPE = 'TABLE'
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM ALL_SUBPART_KEY_COLUMNS sk
                          WHERE sk.OWNER = :schema
                            AND sk.NAME = tc.TABLE_NAME
                            AND sk.COLUMN_NAME = tc.COLUMN_NAME
                            AND sk.OBJECT_TYPE = 'TABLE'
                      )
                    """,
                    schema=stage_schema,
                )
                byte_columns = cursor.fetchall()
            for table_name, column_name, data_type, char_length in byte_columns:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'ALTER TABLE "{stage_schema}"."{table_name}" '
                        f'MODIFY "{column_name}" {data_type}({char_length} CHAR)'
                    )
                LOGGER.debug(
                    "Adjusted %s.%s.%s to %s(%d CHAR)",
                    stage_schema,
                    table_name,
                    column_name,
                    data_type,
                    char_length,
                )

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest."""
        self._workflow = create_workflow(self._workflow_config())
        schema_tables = self._workflow.discover_tables()

        total = len(schema_tables)
        LOGGER.info("Discovered %d tables in dump", total)

        # Bulk-import metadata for all tables per schema, then apply staging
        # adjustments before reading column metadata for the manifest.
        seen_schemas: set[str] = set()
        for source_schema, _ in schema_tables:
            if source_schema in seen_schemas:
                continue
            seen_schemas.add(source_schema)
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info(
                "Importing metadata for schema %s -> %s",
                source_schema,
                stage_schema,
            )
            self.prepare_stage_schema(source_schema)
            self._require_workflow().import_all_metadata(source_schema, stage_schema)
            self._dematerialize_mviews(source_schema)
            self._disable_triggers(source_schema)
            self._drop_vpd_policies(source_schema)
            self._apply_byte_to_char(source_schema)

        tables: list[TableMetadata] = []
        for i, (source_schema, table) in enumerate(schema_tables, start=1):
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info(
                "Inspecting table %d/%d: %s.%s (staging -> %s)",
                i,
                total,
                source_schema,
                table,
                stage_schema,
            )
            with self._connect() as conn:
                metadata = discover_table_metadata(conn, stage_schema, table)
            tables.append(
                TableMetadata(
                    schema=source_schema,
                    name=table,
                    columns=metadata.columns,
                    estimated_bytes=metadata.estimated_bytes,
                    row_count=metadata.row_count,
                    partitions=metadata.partitions,
                    primary_key=metadata.primary_key,
                    unique_keys=metadata.unique_keys,
                )
            )
        return DumpManifest(
            dump_paths=self.dumpfiles,
            tables=tuple(tables),
            dump_format=self._workflow.dump_format,
        )

    # ------------------------------------------------------------------
    # Convert
    # ------------------------------------------------------------------

    def import_table_chunk(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None = None,
    ) -> int:
        """Import one chunk of *table* into the staging schema.

        The staging schema is assumed to be pre-populated with DDL from the
        inspect phase.

        For modern Data Pump dumps the workflow issues
        ``TABLE_EXISTS_ACTION=TRUNCATE CONTENT=DATA_ONLY``, so the import
        tool itself clears the previous rows before loading the new chunk.

        For legacy ``imp`` dumps (which have no ``TABLE_EXISTS_ACTION``), this
        method issues a SQL ``TRUNCATE TABLE`` before delegating to the
        workflow, which then runs ``imp ROWS=Y IGNORE=Y``.

        Args:
            source_schema: Original Oracle schema name.
            table: Table name.
            chunk_name: Chunk identifier (used in log messages and parfile
                generation).
            partition_name: Partition name for ``PARTITION`` strategy chunks;
                ``None`` for whole-table chunks.

        Returns:
            Number of rows present in the staging table after the import.
        """
        stage_schema = self._stage_schema_for(source_schema)
        workflow = self._require_workflow()

        if workflow.dump_format == DumpFormat.LEGACY:
            with self._connect() as conn:
                truncate_table(conn, stage_schema, table)

        workflow.import_chunk(
            source_schema=source_schema,
            stage_schema=stage_schema,
            table=table,
            chunk_name=chunk_name,
            partition_name=partition_name,
        )

        with self._connect() as conn:
            return count_rows(conn, stage_schema, table)

    def export_stage_table(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        output_dir: Path,
        partition_name: str | None = None,
    ) -> ChunkConversionResult:
        """Export a staging table to the configured output format.

        Discovers column metadata from the staging schema, applies any
        per-column overrides from the config, then calls
        :func:`~oracle_dmp_converter.oracle.exporter.export_table` to stream
        rows to the output file.

        When *partition_name* is provided, both the row count and the data
        export are scoped to that partition via a ``PARTITION (name)`` clause.
        This is required for the batch-import path where a single ``impdp``
        call loads all partitions into the staging table at once; without the
        filter, every partition chunk would export the full table.

        Args:
            source_schema: Original Oracle schema name (used for output path
                and config override lookups).
            table: Table name.
            chunk_name: Chunk identifier used to construct the output filename.
            output_dir: Root output directory.
            partition_name: Oracle partition name to scope the export and row
                count to; ``None`` reads the entire staging table (used by the
                per-chunk import path which always has a single partition in
                staging).

        Returns:
            A :class:`ChunkConversionResult` with row counts and the output
            file path.
        """
        stage_schema = self._stage_schema_for(source_schema)
        output_path = _chunk_output_path(
            source_schema=source_schema,
            table=table,
            chunk_name=chunk_name,
            output_dir=output_dir,
            output_format=self.output_format,
        )
        with self._connect() as conn:
            metadata = discover_table_metadata(conn, stage_schema, table)
            imported_rows = count_rows(conn, stage_schema, table, partition_name)
            col_overrides = {
                col.name: ov
                for col in metadata.columns
                if (ov := column_override(self.config, source_schema, table, col.name))
            }
            export_result = export_table(
                conn,
                schema_name=stage_schema,
                table_name=table,
                columns=metadata.columns,
                output_path=output_path,
                output_format=self.output_format,
                column_overrides=col_overrides or None,
                partition_name=partition_name,
            )
        return ChunkConversionResult(
            name=chunk_name,
            imported_rows=imported_rows,
            output_rows=export_result.rows,
            output_path=export_result.path,
        )

    def convert_chunk_plan(
        self,
        *,
        table_plan: TablePlan,
        chunk: ChunkPlan,
        output_dir: Path,
    ) -> ChunkConversionResult:
        """Import and export one :class:`~oracle_dmp_converter.models.ChunkPlan`.

        Calls :meth:`import_table_chunk` followed by :meth:`export_stage_table`.
        The staging table is left in place after export so the next chunk can
        reuse the pre-loaded DDL via ``TRUNCATE_TABLE`` / ``DATA_ONLY`` import.

        Args:
            table_plan: Parent table plan.
            chunk: The individual chunk to process.
            output_dir: Root output directory.

        Returns:
            A :class:`ChunkConversionResult` for the processed chunk.
        """
        self.import_table_chunk(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunk_name=chunk.name,
            partition_name=chunk.partition_name,
        )
        return self.export_stage_table(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunk_name=chunk.name,
            output_dir=output_dir,
        )

    def convert_table_plan(
        self,
        table_plan: TablePlan,
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> TableConversionResult:
        """Convert all chunks for one table according to its plan.

        Skips already-completed chunks recorded in *state_store*.  Updates
        chunk state to ``"running"`` before import and ``"completed"`` (or
        ``"failed"``) afterwards.  Raises on row count mismatches.

        Args:
            table_plan: Conversion plan for the table.
            output_dir: Root output directory.
            state_store: Optional SQLite state store for resumability.

        Returns:
            A :class:`TableConversionResult` with results for each chunk.

        Raises:
            ValueError: If *table_plan* has ``UNSUPPORTED`` strategy, or if
                the imported and output row counts do not match.
        """
        if table_plan.strategy == TableStrategy.UNSUPPORTED:
            reason = table_plan.reason or "unsupported table conversion strategy"
            raise ValueError(f"{table_plan.qualified_name}: {reason}")

        self.prepare_stage_schema(table_plan.schema)
        chunk_results: list[ChunkConversionResult] = []
        for chunk in table_plan.chunks:
            state = state_store.get(table_plan.qualified_name, chunk.name) if state_store else None
            if state and state.status == "completed":
                output_path = _chunk_output_path(
                    source_schema=table_plan.schema,
                    table=table_plan.table,
                    chunk_name=chunk.name,
                    output_dir=output_dir,
                    output_format=self.output_format,
                )
                chunk_results.append(
                    ChunkConversionResult(
                        name=chunk.name,
                        imported_rows=state.imported_rows or 0,
                        output_rows=state.output_rows or 0,
                        output_path=output_path,
                    )
                )
                continue

            if state_store:
                state_store.upsert(ChunkState(table_plan.qualified_name, chunk.name, "running"))
            try:
                result = self.convert_chunk_plan(
                    table_plan=table_plan,
                    chunk=chunk,
                    output_dir=output_dir,
                )
                if result.imported_rows != result.output_rows:
                    msg = (
                        f"row count mismatch for {table_plan.qualified_name} {chunk.name}: "
                        f"imported={result.imported_rows}, output={result.output_rows}"
                    )
                    raise ValueError(msg)
                if state_store:
                    state_store.upsert(
                        ChunkState(
                            table_plan.qualified_name,
                            chunk.name,
                            "completed",
                            result.imported_rows,
                            result.output_rows,
                        )
                    )
                chunk_results.append(result)
            except Exception as exc:
                if state_store:
                    state_store.upsert(
                        ChunkState(table_plan.qualified_name, chunk.name, "failed", error=str(exc))
                    )
                raise
        return TableConversionResult(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunks=tuple(chunk_results),
        )

    def convert_table_batch(
        self,
        table_plans: list[TablePlan],
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> list[TableConversionResult]:
        """Convert a batch of table plans using a single Oracle import invocation.

        All pending chunks from every plan in *table_plans* are combined into
        one ``impdp``/``imp`` call via
        :meth:`~oracle_dmp_converter.datapump.workflow.DumpWorkflow.import_chunks_batch`,
        then each chunk is exported individually.  Chunks already recorded as
        ``"completed"`` in *state_store* are skipped.

        If the batch import itself raises, every pending chunk is marked
        ``"failed"`` before re-raising.  If an individual export raises, only
        that chunk is marked ``"failed"`` and the exception propagates.

        Args:
            table_plans: The batch of table plans to process together.
            output_dir: Root output directory.
            state_store: Optional SQLite state store for resumability.

        Returns:
            One :class:`TableConversionResult` per entry in *table_plans*,
            in the same order.
        """
        # Prepare staging schemas for every unique source schema in the batch.
        seen_schemas: set[str] = set()
        for tp in table_plans:
            if tp.schema not in seen_schemas:
                self.prepare_stage_schema(tp.schema)
                seen_schemas.add(tp.schema)

        # Partition chunks into already-completed and still-pending.
        # chunk_results accumulates results keyed by qualified_name so we can
        # reconstruct TableConversionResult in the original plan order.
        chunk_results: dict[str, list[ChunkConversionResult]] = {
            tp.qualified_name: [] for tp in table_plans
        }
        pending: list[tuple[TablePlan, ChunkPlan]] = []

        for tp in table_plans:
            for chunk in tp.chunks:
                state = (
                    state_store.get(tp.qualified_name, chunk.name) if state_store else None
                )
                if state and state.status == "completed":
                    output_path = _chunk_output_path(
                        source_schema=tp.schema,
                        table=tp.table,
                        chunk_name=chunk.name,
                        output_dir=output_dir,
                        output_format=self.output_format,
                    )
                    chunk_results[tp.qualified_name].append(
                        ChunkConversionResult(
                            name=chunk.name,
                            imported_rows=state.imported_rows or 0,
                            output_rows=state.output_rows or 0,
                            output_path=output_path,
                        )
                    )
                else:
                    pending.append((tp, chunk))

        if not pending:
            return [
                TableConversionResult(
                    source_schema=tp.schema,
                    table=tp.table,
                    chunks=tuple(chunk_results[tp.qualified_name]),
                )
                for tp in table_plans
            ]

        # Mark every pending chunk as running before touching Oracle.
        if state_store:
            for tp, chunk in pending:
                state_store.upsert(ChunkState(tp.qualified_name, chunk.name, "running"))

        # For legacy imp, truncate each staging table before the batch import
        # (impdp uses TABLE_EXISTS_ACTION=TRUNCATE internally; imp does not).
        workflow = self._require_workflow()
        if workflow.dump_format == DumpFormat.LEGACY:
            with self._connect() as conn:
                truncated: set[tuple[str, str]] = set()
                for tp, _ in pending:
                    stage_schema = self._stage_schema_for(tp.schema)
                    key = (stage_schema, tp.table)
                    if key not in truncated:
                        truncate_table(conn, stage_schema, tp.table)
                        truncated.add(key)

        import_specs: list[tuple[str, str, str, str, str | None]] = [
            (
                tp.schema,
                self._stage_schema_for(tp.schema),
                tp.table,
                chunk.name,
                chunk.partition_name,
            )
            for tp, chunk in pending
        ]

        try:
            workflow.import_chunks_batch(import_specs)
        except Exception as exc:
            if state_store:
                for tp, chunk in pending:
                    state_store.upsert(
                        ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc))
                    )
            raise

        # Export each chunk and finalise state.
        for tp, chunk in pending:
            try:
                result = self.export_stage_table(
                    source_schema=tp.schema,
                    table=tp.table,
                    chunk_name=chunk.name,
                    output_dir=output_dir,
                    partition_name=chunk.partition_name,
                )
                if result.imported_rows != result.output_rows:
                    msg = (
                        f"row count mismatch for {tp.qualified_name} {chunk.name}: "
                        f"imported={result.imported_rows}, output={result.output_rows}"
                    )
                    raise ValueError(msg)
                if state_store:
                    state_store.upsert(
                        ChunkState(
                            tp.qualified_name,
                            chunk.name,
                            "completed",
                            result.imported_rows,
                            result.output_rows,
                        )
                    )
                chunk_results[tp.qualified_name].append(result)
            except Exception as exc:
                if state_store:
                    state_store.upsert(
                        ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc))
                    )
                raise

        return [
            TableConversionResult(
                source_schema=tp.schema,
                table=tp.table,
                chunks=tuple(chunk_results[tp.qualified_name]),
            )
            for tp in table_plans
        ]

    def convert_plan(
        self,
        plan: ConversionPlan,
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> PlanConversionResult:
        """Convert all supported tables in a :class:`~oracle_dmp_converter.models.ConversionPlan`.

        Initialises the workflow from the plan's recorded format if no workflow
        is active (convert-only run).  Logs and skips ``UNSUPPORTED`` tables.

        Args:
            plan: The full conversion plan loaded from ``plan.yaml``.
            output_dir: Root output directory.
            state_store: Optional SQLite state store for resumability.

        Returns:
            A :class:`PlanConversionResult` summarising all converted tables.
        """
        # If we have no active workflow (convert-only run from a saved plan),
        # initialise one from the plan's recorded format.
        if self._workflow is None:
            self.use_format(plan.dump_format)

        supported: list[TablePlan] = []
        for table_plan in plan.tables:
            if table_plan.strategy == TableStrategy.UNSUPPORTED:
                LOGGER.warning(
                    "Skipping %s.%s: %s",
                    table_plan.schema,
                    table_plan.table,
                    table_plan.reason or "unsupported strategy",
                )
            else:
                supported.append(table_plan)

        plan_started_at = datetime.now(UTC)
        results: list[TableConversionResult] = []
        for batch_start in range(0, len(supported), TABLE_IMPORT_BATCH_SIZE):
            batch = supported[batch_start : batch_start + TABLE_IMPORT_BATCH_SIZE]
            LOGGER.info(
                "Importing batch of %d table(s) (%d–%d of %d)",
                len(batch),
                batch_start + 1,
                batch_start + len(batch),
                len(supported),
            )
            batch_results = self.convert_table_batch(batch, output_dir, state_store)
            for tp, table_result in zip(batch, batch_results, strict=True):
                LOGGER.info(
                    "Converted %s.%s: %d rows",
                    tp.schema,
                    tp.table,
                    table_result.rows,
                )
            results.extend(batch_results)
        plan_completed_at = datetime.now(UTC)
        return PlanConversionResult(
            tables=tuple(results),
            started_at=plan_started_at,
            completed_at=plan_completed_at,
        )
