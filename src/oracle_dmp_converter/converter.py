"""High-level conversion orchestration."""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import ConverterConfig, column_override
from oracle_dmp_converter.datapump.imp_show import parse_imp_indexfile_tables
from oracle_dmp_converter.datapump.legacy_parfile import (
    LegacyConnection,
    LegacyImportJob,
    LegacyIndexFileJob,
)
from oracle_dmp_converter.datapump.parfile import DataPumpConnection, ImportJob, SqlFileJob
from oracle_dmp_converter.datapump.runner import DataPumpRunner, is_legacy_format_error
from oracle_dmp_converter.datapump.sqlfile import parse_sqlfile_tables
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.errors import DataPumpError, LegacyDumpError
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
    count_rows,
    drop_schema,
    drop_table,
    ensure_schema,
    oracle_connection,
)
from oracle_dmp_converter.oracle.exporter import export_table
from oracle_dmp_converter.oracle.identifiers import filesystem_safe_identifier
from oracle_dmp_converter.oracle.metadata import discover_table_metadata
from oracle_dmp_converter.planner import hash_bucket_query, null_bucket_query

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


class OracleDumpConverter:
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
        self._inspect_runner = DataPumpRunner(container, work_dir / "inspect" / "parfiles")
        self._convert_runner = DataPumpRunner(container, work_dir / "convert" / "parfiles")
        # Set during discover_dump_tables(); defaults to DATAPUMP until
        # detection runs.
        self.dump_format: DumpFormat = DumpFormat.DATAPUMP

    @staticmethod
    def _stage_schema_for(source_schema: str) -> str:
        """Return the staging schema name for a given source schema.

        The convention is ``DMP_<source_schema>``.  For example, a dump
        containing schema ``APP`` is imported into ``DMP_APP``.
        """
        return f"{_STAGE_SCHEMA_PREFIX}{source_schema}"

    def _connect(self) -> AbstractContextManager[oracledb.Connection]:
        return oracle_connection(
            host=self.admin.host,
            port=self.admin.port,
            service=self.admin.service,
            user=self.admin.user,
            password=self.admin.password,
        )

    def _legacy_connection(self) -> LegacyConnection:
        return LegacyConnection(
            user=self.admin.user,
            password=self.admin.password,
            service=self.admin.service,
        )

    def _legacy_files(self) -> tuple[str, ...]:
        """Absolute paths to dump files inside the container."""
        return tuple(f"{self.directory_path}/{f}" for f in self.dumpfiles)

    def prepare_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.debug("Ensuring staging schema %s for source %s", stage_schema, source_schema)
        with self._connect() as conn:
            ensure_schema(conn, stage_schema, self.stage_password)

    def drop_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.debug("Dropping staging schema %s", stage_schema)
        with self._connect() as conn:
            drop_schema(conn, stage_schema)

    def _metadata_import_table(self, source_schema: str, table: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        self.prepare_stage_schema(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

        if self.dump_format == DumpFormat.LEGACY:
            job = LegacyImportJob(
                connection=self._legacy_connection(),
                files=self._legacy_files(),
                logfile=f"imp-meta-{source_schema}-{table}.log"[:120],
                fromuser=source_schema,
                touser=stage_schema,
                tables=(table,),
                rows=False,
                indexes=False,
                grants=False,
                constraints=False,
            )
            self._inspect_runner.run_imp(job)
        else:
            job = ImportJob(
                connection=DataPumpConnection(
                    self.admin.user, self.admin.password, self.admin.service
                ),
                directory=self.directory,
                dumpfiles=self.dumpfiles,
                logfile=f"metadata-{source_schema}-{table}.log"[:120],
                source_schema=source_schema,
                table=table,
                remap_schema=(source_schema, stage_schema),
                content="METADATA_ONLY",
                exclude=("INDEX", "REF_CONSTRAINT", "TRIGGER"),
            )
            self._inspect_runner.run_impdp(job)

    def _discover_legacy_dump_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover tables in a legacy ``exp`` dump via ``imp INDEXFILE=``."""
        indexfile = "dmp2parquet-legacy-discovery.sql"
        remote_indexfile = f"/tmp/{indexfile}"
        job = LegacyIndexFileJob(
            connection=self._legacy_connection(),
            files=self._legacy_files(),
            logfile="dmp2parquet-legacy-discovery.log",
            indexfile=remote_indexfile,
            full=True,
        )
        sql_text = self._inspect_runner.run_imp_indexfile(job)

        if not sql_text:
            # Fall back to reading from the directory path as well (some Oracle
            # versions write the indexfile there instead of /tmp).
            alt = self.container.exec(["cat", f"{self.directory_path}/{indexfile}"], check=False)
            sql_text = alt.stdout if alt.returncode == 0 else ""

        return parse_imp_indexfile_tables(sql_text)

    def _probe_dump_format(self) -> None:
        """Detect whether the dump is Data Pump (expdp) or legacy (exp) format.

        Runs ``impdp SQLFILE=`` against the dump.  On success the dump is a
        Data Pump file and :attr:`dump_format` is set to
        :attr:`~oracle_dmp_converter.models.DumpFormat.DATAPUMP`.

        If ``impdp`` fails with ``ORA-39142`` or ``ORA-39143`` the dump was
        produced by legacy ``exp``; :attr:`dump_format` is set to
        :attr:`~oracle_dmp_converter.models.DumpFormat.LEGACY` and
        :class:`~oracle_dmp_converter.errors.LegacyDumpError` is raised so the
        caller can switch to the ``imp``-based workflow.

        Any other ``impdp`` failure is re-raised as
        :class:`~oracle_dmp_converter.errors.DataPumpError`.
        """
        sqlfile = "dmp2parquet-discovery.sql"
        job = SqlFileJob(
            connection=DataPumpConnection(self.admin.user, self.admin.password, self.admin.service),
            directory=self.directory,
            dumpfiles=self.dumpfiles,
            logfile="dmp2parquet-discovery.log",
            sqlfile=sqlfile,
        )
        try:
            self._inspect_runner.run_sqlfile(job)
        except DataPumpError as exc:
            if not is_legacy_format_error(str(exc)):
                raise
            self.dump_format = DumpFormat.LEGACY
            raise LegacyDumpError(str(exc)) from exc
        self.dump_format = DumpFormat.DATAPUMP

    def discover_dump_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover tables in the dump, auto-detecting exp vs expdp format.

        Tries ``impdp SQLFILE=`` first.  If the output contains
        ``ORA-39142`` (incompatible dump-file version) or ``ORA-39143``
        ("The file may be an original export dump file" — emitted by 23ai
        Free), the dump was produced by legacy ``exp``; we set
        :attr:`dump_format` to
        :attr:`~oracle_dmp_converter.models.DumpFormat.LEGACY` and fall back to
        ``imp INDEXFILE=``.  Any other ``impdp`` failure is re-raised as-is.
        """
        try:
            self._probe_dump_format()
        except LegacyDumpError:
            return self._discover_legacy_dump_tables()

        # Data Pump succeeded — read the SQLFILE output.
        sqlfile = "dmp2parquet-discovery.sql"
        result = self.container.exec(["cat", f"{self.directory_path}/{sqlfile}"], check=False)
        sql_text = result.stdout if result.returncode == 0 else ""
        if not sql_text:
            result = self.container.exec(
                [
                    "bash",
                    "-lc",
                    (
                        "for path in "
                        f"{self.directory_path}/{sqlfile} "
                        f"{self.directory_path}/*/{sqlfile}; do "
                        '[ -f "$path" ] && cat "$path"; '
                        "done"
                    ),
                ],
                check=False,
            )
            sql_text = result.stdout if result.returncode == 0 else ""
        return parse_sqlfile_tables(sql_text)

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest.

        When the dump is a legacy ``exp`` file, :attr:`dump_format` is set
        to :attr:`~oracle_dmp_converter.models.DumpFormat.LEGACY` and ``imp``
        is used for all subsequent operations.
        """
        schema_tables = self.discover_dump_tables()

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
            dump_format=self.dump_format,
        )

    def import_table_chunk(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        query: str | None = None,
        partition_name: str | None = None,
    ) -> int:
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

        if self.dump_format == DumpFormat.LEGACY:
            # Legacy imp does not support QUERY= or partition-level imports.
            # query / partition_name are silently ignored here; the planner
            # must not produce HASH or PARTITION chunks for legacy dumps.
            job = LegacyImportJob(
                connection=self._legacy_connection(),
                files=self._legacy_files(),
                logfile=f"imp-{source_schema}-{table}-{chunk_name}.log"[:120],
                fromuser=source_schema,
                touser=stage_schema,
                tables=(table,),
                rows=True,
                indexes=False,
                grants=False,
                constraints=False,
            )
            self._convert_runner.run_imp(job)
        else:
            job = ImportJob(
                connection=DataPumpConnection(
                    self.admin.user, self.admin.password, self.admin.service
                ),
                directory=self.directory,
                dumpfiles=self.dumpfiles,
                logfile=f"impdp-{source_schema}-{table}-{chunk_name}.log"[:120],
                source_schema=source_schema,
                table=table,
                remap_schema=(source_schema, stage_schema),
                query=query,
                partition_name=partition_name,
            )
            self._convert_runner.run_impdp(job)

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
        table_dir = (
            output_dir
            / filesystem_safe_identifier(source_schema)
            / filesystem_safe_identifier(table)
        )
        ext = self.output_format.value
        output_path = table_dir / f"{filesystem_safe_identifier(chunk_name)}.{ext}"
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

    def drop_stage_table(self, source_schema: str, table: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            drop_table(conn, stage_schema, table)

    def convert_hash_table(
        self,
        *,
        source_schema: str,
        table: str,
        split_column: str,
        buckets: int,
        output_dir: Path,
        include_null_bucket: bool = True,
    ) -> TableConversionResult:
        # Detect the dump format before entering the hash loop.  Legacy exp
        # dumps cannot be hash-chunked because imp has no QUERY= support.
        try:
            self._probe_dump_format()
        except LegacyDumpError as exc:
            msg = (
                f"{source_schema}.{table}: hash chunking requires a Data Pump (expdp) dump; "
                "this dump was created with legacy exp which does not support QUERY= filtering. "
                "Re-export the data with expdp to use convert-hash-table."
            )
            raise LegacyDumpError(msg) from exc

        self.prepare_stage_schema(source_schema)
        chunks: list[ChunkConversionResult] = []
        for bucket_index in range(buckets):
            chunk_name = f"hash-{bucket_index:05d}-of-{buckets:05d}"
            query = hash_bucket_query(split_column, bucket_index, buckets)
            self.import_table_chunk(
                source_schema=source_schema,
                table=table,
                chunk_name=chunk_name,
                query=query,
            )
            try:
                chunks.append(
                    self.export_stage_table(
                        source_schema=source_schema,
                        table=table,
                        chunk_name=chunk_name,
                        output_dir=output_dir,
                    )
                )
            finally:
                self.drop_stage_table(source_schema, table)

        if include_null_bucket:
            chunk_name = "hash-null"
            self.import_table_chunk(
                source_schema=source_schema,
                table=table,
                chunk_name=chunk_name,
                query=null_bucket_query(split_column),
            )
            try:
                chunks.append(
                    self.export_stage_table(
                        source_schema=source_schema,
                        table=table,
                        chunk_name=chunk_name,
                        output_dir=output_dir,
                    )
                )
            finally:
                self.drop_stage_table(source_schema, table)

        return TableConversionResult(source_schema=source_schema, table=table, chunks=tuple(chunks))

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
            query=chunk.query,
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
                ext = self.output_format.value
                output_path = (
                    output_dir
                    / filesystem_safe_identifier(table_plan.schema)
                    / filesystem_safe_identifier(table_plan.table)
                    / f"{filesystem_safe_identifier(chunk.name)}.{ext}"
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
        results: list[TableConversionResult] = []
        for table_plan in plan.tables:
            LOGGER.info(
                "Converting %s.%s (%s)",
                table_plan.schema,
                table_plan.table,
                table_plan.strategy,
            )
            results.append(self.convert_table_plan(table_plan, output_dir, state_store))
        return PlanConversionResult(tables=tuple(results))
