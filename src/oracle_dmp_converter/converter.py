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
    host: str
    port: int
    service: str
    user: str
    password: str


@dataclass(frozen=True)
class ChunkConversionResult:
    name: str
    imported_rows: int
    output_rows: int
    output_path: Path


@dataclass(frozen=True)
class TableConversionResult:
    source_schema: str
    table: str
    chunks: tuple[ChunkConversionResult, ...] = field(default_factory=tuple)

    @property
    def rows(self) -> int:
        return sum(chunk.output_rows for chunk in self.chunks)


@dataclass(frozen=True)
class PlanConversionResult:
    tables: tuple[TableConversionResult, ...]

    @property
    def rows(self) -> int:
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
    through :func:`filesystem_safe_identifier`.
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
        return OracleCredentials(
            user=self.admin.user,
            password=self.admin.password,
            service=self.admin.service,
        )

    def _workflow_config(self) -> WorkflowConfig:
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
            inspect_runner, convert_runner = make_legacy_runners(cfg.container, cfg.work_dir)
            self._workflow = LegacyDumpWorkflow(
                credentials=cfg.credentials,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                inspect_runner=inspect_runner,
                convert_runner=convert_runner,
            )
        else:
            inspect_runner, convert_runner = make_modern_runners(cfg.container, cfg.work_dir)
            self._workflow = DataPumpWorkflow(
                credentials=cfg.credentials,
                directory=cfg.directory,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                inspect_runner=inspect_runner,
                convert_runner=convert_runner,
            )

    def _require_workflow(self) -> DumpWorkflow:
        if self._workflow is None:
            raise RuntimeError("no active workflow; call inspect_dump() or use_format() first")
        return self._workflow

    # ------------------------------------------------------------------
    # Staging schema / connection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _stage_schema_for(source_schema: str) -> str:
        """Return the staging schema name (``DMP_<source_schema>``)."""
        return f"{_STAGE_SCHEMA_PREFIX}{source_schema}"

    def _connect(self) -> AbstractContextManager[oracledb.Connection]:
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
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.debug("Ensuring staging schema %s for source %s", stage_schema, source_schema)
        with self._connect() as conn:
            ensure_schema(conn, stage_schema, self.stage_password)
            for tablespace in self._required_tablespaces():
                ensure_tablespace(conn, tablespace)
                grant_quota_unlimited(conn, stage_schema, tablespace)

    def drop_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.debug("Dropping staging schema %s", stage_schema)
        with self._connect() as conn:
            drop_schema(conn, stage_schema)

    def drop_stage_table(self, source_schema: str, table: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

    # ------------------------------------------------------------------
    # Inspect
    # ------------------------------------------------------------------

    def _metadata_import_table(self, source_schema: str, table: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        self.prepare_stage_schema(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)
        self._require_workflow().import_metadata(source_schema, stage_schema, table)

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest."""
        self._workflow = create_workflow(self._workflow_config())
        schema_tables = self._workflow.discover_tables()

        LOGGER.info("Discovered %d tables in dump", len(schema_tables))
        tables: list[TableMetadata] = []
        for source_schema, table in schema_tables:
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.debug(
                "Inspecting %s.%s via staging schema %s", source_schema, table, stage_schema
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
            results.append(self.convert_table_plan(table_plan, output_dir, state_store))
        return PlanConversionResult(tables=tuple(results))
