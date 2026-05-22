"""Low-level Oracle staging executor.

Drives the discover → stage → import → export sequence against a running
Oracle container.  Format-specific branching (modern Data Pump vs legacy
exp/imp) is delegated to :class:`DumpWorkflow`.
"""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import ConverterConfig, column_override
from oracle_dmp_converter.core.results import (
    ChunkConversionResult,
    PlanConversionResult,
    TableConversionResult,
)
from oracle_dmp_converter.core.staging import (
    apply_byte_to_char,
    dematerialize_mviews,
    disable_triggers,
    drop_vpd_policies,
)
from oracle_dmp_converter.datapump._ddl_parser import parse_missing_tablespace_from_error
from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow, WorkflowConfig
from oracle_dmp_converter.datapump.legacy.workflow import (
    LegacyDumpWorkflow,
    make_legacy_runners,
)
from oracle_dmp_converter.datapump.modern.workflow import (
    DataPumpWorkflow,
    make_modern_runners,
)
from oracle_dmp_converter.datapump.workflow import create_workflow
from oracle_dmp_converter.errors import DataPumpError
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
    table_exists,
    truncate_table,
)
from oracle_dmp_converter.oracle.exporter import export_table
from oracle_dmp_converter.oracle.identifiers import filesystem_safe_identifier
from oracle_dmp_converter.oracle.metadata import discover_table_metadata
from oracle_dmp_converter.persistence.state import ChunkState, StateStore
from oracle_dmp_converter.runtime.admin import OracleAdminConnection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)

_STAGE_SCHEMA_PREFIX = "DMP_"

# Number of tables combined into a single impdp/imp invocation.
TABLE_IMPORT_BATCH_SIZE = 20


def chunk_output_path(
    *,
    source_schema: str,
    table: str,
    chunk_name: str,
    output_dir: Path,
    output_format: OutputFormat,
) -> Path:
    """Return ``<output_dir>/<schema>/<table>/<chunk>.<ext>`` for one chunk."""
    return (
        output_dir
        / filesystem_safe_identifier(source_schema)
        / filesystem_safe_identifier(table)
        / f"{filesystem_safe_identifier(chunk_name)}.{output_format.value}"
    )


class StagingExecutor:
    """Drive inspect → convert against a running Oracle container.

    All dump-format-specific work (impdp vs imp, SQLFILE vs INDEXFILE,
    parfile syntax) is delegated to :attr:`_workflow`, which is initialised
    by :meth:`inspect_dump` (auto-detect) or :meth:`use_format` (known).
    """

    def __init__(
        self,
        *,
        container: ContainerOracle,
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
        self._workflow: DumpWorkflow | None = None

    @property
    def dump_format(self) -> DumpFormat:
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
            discovery_directory=self.discovery_directory,
            inspect_directory=self.inspect_directory,
            convert_directory=self.convert_directory,
        )

    def use_format(self, dump_format: DumpFormat) -> None:
        """Initialise :attr:`_workflow` for a known dump format."""
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
        if self._workflow is None:
            raise RuntimeError("No workflow active; call inspect_dump() or use_format() first")
        return self._workflow

    @staticmethod
    def _stage_schema_for(source_schema: str) -> str:
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
        if self._workflow is None:
            return frozenset()
        return self._workflow.required_tablespaces()

    def _recover_missing_tablespaces(self, output: str, source_schema: str) -> bool:
        """Parse *output* for ``ORA-00959`` errors and create any missing tablespaces.

        Extracts every tablespace name from ``ORA-00959: tablespace '...' does
        not exist`` lines, creates each tablespace (using OMF), and grants
        ``QUOTA UNLIMITED`` on it to the staging schema.

        Args:
            output: Combined stdout+stderr text from a failed impdp/imp run
                (typically the message of a
                :class:`~oracle_dmp_converter.errors.DataPumpError`).
            source_schema: Source schema name; the corresponding staging schema
                receives ``QUOTA UNLIMITED`` on each newly created tablespace.

        Returns:
            ``True`` if one or more missing tablespaces were detected and
            created; ``False`` if *output* contained no ``ORA-00959`` lines.
        """
        missing = parse_missing_tablespace_from_error(output)
        if not missing:
            return False
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info(
            "Recovering %d missing tablespace(s) for schema %s: %s",
            len(missing),
            source_schema,
            ", ".join(sorted(missing)),
        )
        with self._connect() as conn:
            for tablespace in missing:
                ensure_tablespace(conn, tablespace)
                grant_quota_unlimited(conn, stage_schema, tablespace)
        return True

    def prepare_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Ensuring staging schema %s for source %s", stage_schema, source_schema)
        with self._connect() as conn:
            ensure_schema(conn, stage_schema, self.stage_password)
            for tablespace in self._required_tablespaces():
                ensure_tablespace(conn, tablespace)
                grant_quota_unlimited(conn, stage_schema, tablespace)

    def drop_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Dropping staging schema %s", stage_schema)
        with self._connect() as conn:
            drop_schema(conn, stage_schema)

    def validate_staging_tables(self, table_plans: list[TablePlan]) -> None:
        """Validate that staging tables imported during inspect still exist.

        Should be called before convert when the workflow was already
        initialised from a prior :meth:`inspect_dump` call.  Raises
        :exc:`ValueError` with a helpful message if any staging table is
        missing so the caller knows to re-run inspect rather than seeing a
        cryptic Oracle error mid-conversion.

        Args:
            table_plans: Supported (non-UNSUPPORTED) table plans whose
                staging tables should be present.

        Raises:
            ValueError: If one or more staging tables are absent.
        """
        missing: list[str] = []
        with self._connect() as conn:
            for tp in table_plans:
                stage_schema = self._stage_schema_for(tp.schema)
                if not table_exists(conn, stage_schema, tp.table):
                    missing.append(f"{stage_schema}.{tp.table}")
        if missing:
            raise ValueError(
                "Staging tables from the inspect phase are missing: "
                + ", ".join(missing)
                + ". Re-run inspect before convert."
            )

    def _apply_staging_fixups(self, source_schema: str) -> None:
        """Run all post-import staging fixups in a single connection."""
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            dematerialize_mviews(conn, stage_schema)
            disable_triggers(conn, stage_schema)
            drop_vpd_policies(conn, stage_schema)
            apply_byte_to_char(conn, stage_schema)

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest."""
        self._workflow = create_workflow(self._workflow_config())
        schema_tables = self._workflow.discover_tables()

        total = len(schema_tables)
        LOGGER.info("Discovered %d tables in dump", total)

        seen_schemas: set[str] = set()
        for source_schema, _ in schema_tables:
            if source_schema in seen_schemas:
                continue
            seen_schemas.add(source_schema)
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info("Importing metadata for schema %s -> %s", source_schema, stage_schema)
            self.prepare_stage_schema(source_schema)
            try:
                self._require_workflow().import_all_metadata(source_schema, stage_schema)
            except DataPumpError as exc:
                if not self._recover_missing_tablespaces(str(exc), source_schema):
                    raise
                LOGGER.info(
                    "Retrying bulk metadata import for %s after tablespace recovery",
                    source_schema,
                )
                self._require_workflow().import_all_metadata(source_schema, stage_schema)
            self._apply_staging_fixups(source_schema)

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

    def import_table_chunk(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None = None,
    ) -> int:
        stage_schema = self._stage_schema_for(source_schema)
        workflow = self._require_workflow()

        if workflow.dump_format == DumpFormat.LEGACY:
            with self._connect() as conn:
                truncate_table(conn, stage_schema, table)

        try:
            workflow.import_chunk(
                source_schema=source_schema,
                stage_schema=stage_schema,
                table=table,
                chunk_name=chunk_name,
                partition_name=partition_name,
            )
        except DataPumpError as exc:
            if not self._recover_missing_tablespaces(str(exc), source_schema):
                raise
            LOGGER.info(
                "Retrying chunk %s for %s.%s after tablespace recovery",
                chunk_name,
                source_schema,
                table,
            )
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
        stage_schema = self._stage_schema_for(source_schema)
        output_path = chunk_output_path(
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
        if table_plan.strategy == TableStrategy.UNSUPPORTED:
            reason = table_plan.reason or "unsupported table conversion strategy"
            raise ValueError(f"{table_plan.qualified_name}: {reason}")

        self.prepare_stage_schema(table_plan.schema)
        chunk_results: list[ChunkConversionResult] = []
        for chunk in table_plan.chunks:
            state = state_store.get(table_plan.qualified_name, chunk.name) if state_store else None
            if state and state.status == "completed":
                output_path = chunk_output_path(
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

    def _split_batch_pending(
        self,
        table_plans: list[TablePlan],
        output_dir: Path,
        state_store: StateStore | None,
    ) -> tuple[dict[str, list[ChunkConversionResult]], list[tuple[TablePlan, ChunkPlan]]]:
        """Separate already-completed chunks (rehydrated from state) from pending work."""
        chunk_results: dict[str, list[ChunkConversionResult]] = {
            tp.qualified_name: [] for tp in table_plans
        }
        pending: list[tuple[TablePlan, ChunkPlan]] = []
        for tp in table_plans:
            for chunk in tp.chunks:
                state = state_store.get(tp.qualified_name, chunk.name) if state_store else None
                if state and state.status == "completed":
                    chunk_results[tp.qualified_name].append(
                        ChunkConversionResult(
                            name=chunk.name,
                            imported_rows=state.imported_rows or 0,
                            output_rows=state.output_rows or 0,
                            output_path=chunk_output_path(
                                source_schema=tp.schema,
                                table=tp.table,
                                chunk_name=chunk.name,
                                output_dir=output_dir,
                                output_format=self.output_format,
                            ),
                        )
                    )
                else:
                    pending.append((tp, chunk))
        return chunk_results, pending

    def _truncate_legacy_stage_tables(self, pending: list[tuple[TablePlan, ChunkPlan]]) -> None:
        """Pre-truncate every unique staging table for a legacy batch import."""
        with self._connect() as conn:
            truncated: set[tuple[str, str]] = set()
            for tp, _ in pending:
                stage_schema = self._stage_schema_for(tp.schema)
                key = (stage_schema, tp.table)
                if key not in truncated:
                    truncate_table(conn, stage_schema, tp.table)
                    truncated.add(key)

    def _mark_pending_failed(
        self,
        pending: list[tuple[TablePlan, ChunkPlan]],
        state_store: StateStore | None,
        exc: BaseException,
    ) -> None:
        if state_store is None:
            return
        for tp, chunk in pending:
            state_store.upsert(ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc)))

    def _export_one_batched_chunk(
        self,
        tp: TablePlan,
        chunk: ChunkPlan,
        output_dir: Path,
        state_store: StateStore | None,
    ) -> ChunkConversionResult:
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
            return result
        except Exception as exc:
            if state_store:
                state_store.upsert(
                    ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc))
                )
            raise

    def convert_table_batch(
        self,
        table_plans: list[TablePlan],
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> list[TableConversionResult]:
        seen_schemas: set[str] = set()
        for tp in table_plans:
            if tp.schema not in seen_schemas:
                self.prepare_stage_schema(tp.schema)
                seen_schemas.add(tp.schema)

        chunk_results, pending = self._split_batch_pending(table_plans, output_dir, state_store)

        if not pending:
            return [
                TableConversionResult(
                    source_schema=tp.schema,
                    table=tp.table,
                    chunks=tuple(chunk_results[tp.qualified_name]),
                )
                for tp in table_plans
            ]

        if state_store:
            for tp, chunk in pending:
                state_store.upsert(ChunkState(tp.qualified_name, chunk.name, "running"))

        workflow = self._require_workflow()
        if workflow.dump_format == DumpFormat.LEGACY:
            self._truncate_legacy_stage_tables(pending)

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
            self._mark_pending_failed(pending, state_store, exc)
            raise

        for tp, chunk in pending:
            result = self._export_one_batched_chunk(tp, chunk, output_dir, state_store)
            chunk_results[tp.qualified_name].append(result)

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
        workflow_from_inspect = self._workflow is not None
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

        if workflow_from_inspect:
            self.validate_staging_tables(supported)

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
