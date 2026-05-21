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
    drop_table,
    ensure_schema,
    ensure_tablespace,
    grant_quota_unlimited,
    oracle_connection,
)
from oracle_dmp_converter.oracle.exporter import export_table
from oracle_dmp_converter.oracle.identifiers import filesystem_safe_identifier
from oracle_dmp_converter.oracle.metadata import discover_table_metadata

LOGGER = logging.getLogger(__name__)

_STAGE_SCHEMA_PREFIX = "DMP_"


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
    """

    tables: tuple[TableConversionResult, ...]

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
            raise RuntimeError(
                "No workflow active; call inspect_dump() or use_format() first"
            )
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

    def drop_stage_table(self, source_schema: str, table: str) -> None:
        """Drop a single table from the staging schema.

        Used to clean up after each chunk import so the next import starts
        with an empty target.

        Args:
            source_schema: Original Oracle schema name.
            table: Table name to drop from the ``DMP_<source_schema>`` schema.
        """
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

    # ------------------------------------------------------------------
    # Inspect
    # ------------------------------------------------------------------

    def _metadata_import_table(self, source_schema: str, table: str) -> None:
        """Import a single table's DDL into the staging schema for inspection.

        Ensures the staging schema exists, drops any existing copy of *table*,
        then delegates to the workflow's metadata-only import.

        Args:
            source_schema: Original Oracle schema name from the dump.
            table: Table to inspect.
        """
        stage_schema = self._stage_schema_for(source_schema)
        self.prepare_stage_schema(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)
        self._require_workflow().import_metadata(source_schema, stage_schema, table)

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest."""
        self._workflow = create_workflow(self._workflow_config())
        schema_tables = self._workflow.discover_tables()

        total = len(schema_tables)
        LOGGER.info("Discovered %d tables in dump", total)
        tables: list[TableMetadata] = []
        for i, (source_schema, table) in enumerate(schema_tables, start=1):
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info(
                "Inspecting table %d/%d: %s.%s (staging -> %s)",
                i, total, source_schema, table, stage_schema,
            )
            self._metadata_import_table(source_schema, table)
            try:
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
            finally:
                self.drop_stage_table(source_schema, table)
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

        Drops any existing copy of the table, delegates the import to the
        active workflow, then returns the row count.

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
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

        self._require_workflow().import_chunk(
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
    ) -> ChunkConversionResult:
        """Export a staging table to the configured output format.

        Discovers column metadata from the staging schema, applies any
        per-column overrides from the config, then calls
        :func:`~oracle_dmp_converter.oracle.exporter.export_table` to stream
        rows to the output file.

        Args:
            source_schema: Original Oracle schema name (used for output path
                and config override lookups).
            table: Table name.
            chunk_name: Chunk identifier used to construct the output filename.
            output_dir: Root output directory.

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
            imported_rows = count_rows(conn, stage_schema, table)
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

        Calls :meth:`import_table_chunk` followed by :meth:`export_stage_table`
        and always cleans up the staging table on completion.

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
        try:
            return self.export_stage_table(
                source_schema=table_plan.schema,
                table=table_plan.table,
                chunk_name=chunk.name,
                output_dir=output_dir,
            )
        finally:
            self.drop_stage_table(table_plan.schema, table_plan.table)

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

        results: list[TableConversionResult] = []
        for table_plan in plan.tables:
            if table_plan.strategy == TableStrategy.UNSUPPORTED:
                LOGGER.warning(
                    "Skipping %s.%s: %s",
                    table_plan.schema,
                    table_plan.table,
                    table_plan.reason or "unsupported strategy",
                )
                continue
            LOGGER.info(
                "Converting %s.%s (%s)",
                table_plan.schema,
                table_plan.table,
                table_plan.strategy,
            )
            table_result = self.convert_table_plan(table_plan, output_dir, state_store)
            LOGGER.info(
                "Converted %s.%s: %d rows",
                table_plan.schema,
                table_plan.table,
                table_result.rows,
            )
            results.append(table_result)
        return PlanConversionResult(tables=tuple(results))
