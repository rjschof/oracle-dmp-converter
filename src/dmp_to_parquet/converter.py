"""High-level conversion orchestration."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path

import oracledb

from dmp_to_parquet.datapump import DataPumpRunner
from dmp_to_parquet.docker_oracle import DockerOracle
from dmp_to_parquet.exporter import export_table_to_parquet
from dmp_to_parquet.identifiers import filesystem_safe_identifier
from dmp_to_parquet.metadata import discover_table_metadata
from dmp_to_parquet.models import (
    ChunkPlan,
    ConversionPlan,
    DumpManifest,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from dmp_to_parquet.oracle_conn import (
    count_rows,
    drop_schema,
    drop_table,
    ensure_schema,
    oracle_connection,
)
from dmp_to_parquet.parfile import DataPumpConnection, ImportJob, SqlFileJob
from dmp_to_parquet.planner import hash_bucket_query, null_bucket_query
from dmp_to_parquet.sqlfile import parse_sqlfile_tables
from dmp_to_parquet.state import ChunkState, StateStore


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
    parquet_rows: int
    parquet_path: Path


@dataclass(frozen=True)
class TableConversionResult:
    source_schema: str
    table: str
    chunks: tuple[ChunkConversionResult, ...] = field(default_factory=tuple)

    @property
    def rows(self) -> int:
        return sum(chunk.parquet_rows for chunk in self.chunks)


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
        stage_schema: str = "DMP_STAGE",
        stage_password: str = "StagePwd_123",
    ) -> None:
        self.container = container
        self.admin = admin
        self.work_dir = work_dir
        self.dumpfiles = dumpfiles
        self.directory = directory
        self.directory_path = directory_path.rstrip("/")
        self.stage_schema = stage_schema
        self.stage_password = stage_password
        self.datapump = DataPumpRunner(container, work_dir / "parfiles")

    def _connect(self) -> AbstractContextManager[oracledb.Connection]:
        return oracle_connection(
            host=self.admin.host,
            port=self.admin.port,
            service=self.admin.service,
            user=self.admin.user,
            password=self.admin.password,
        )

    def prepare_stage_schema(self) -> None:
        with self._connect() as conn:
            ensure_schema(conn, self.stage_schema, self.stage_password)

    def drop_stage_schema(self) -> None:
        with self._connect() as conn:
            drop_schema(conn, self.stage_schema)

    def _metadata_import_table(self, source_schema: str, table: str) -> None:
        self.prepare_stage_schema()
        with self._connect() as conn:
            drop_table(conn, self.stage_schema, table)
        job = ImportJob(
            connection=DataPumpConnection(self.admin.user, self.admin.password, self.admin.service),
            directory=self.directory,
            dumpfiles=self.dumpfiles,
            logfile=f"metadata-{source_schema}-{table}.log"[:120],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, self.stage_schema),
            content="METADATA_ONLY",
            exclude=("INDEX", "REF_CONSTRAINT", "TRIGGER"),
        )
        self.datapump.run_impdp(job)

    def discover_dump_tables(self) -> tuple[tuple[str, str], ...]:
        sqlfile = "dmp2parquet-discovery.sql"
        job = SqlFileJob(
            connection=DataPumpConnection(self.admin.user, self.admin.password, self.admin.service),
            directory=self.directory,
            dumpfiles=self.dumpfiles,
            logfile="dmp2parquet-discovery.log",
            sqlfile=sqlfile,
        )
        self.datapump.run_sqlfile(job)
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
                        "[ -f \"$path\" ] && cat \"$path\"; "
                        "done"
                    ),
                ],
                check=False,
            )
            sql_text = result.stdout if result.returncode == 0 else ""
        return parse_sqlfile_tables(sql_text)

    def inspect_dump(self) -> DumpManifest:
        tables: list[TableMetadata] = []
        for source_schema, table in self.discover_dump_tables():
            self._metadata_import_table(source_schema, table)
            try:
                with self._connect() as conn:
                    metadata = discover_table_metadata(conn, self.stage_schema, table)
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
                self.drop_stage_table(table)
        return DumpManifest(dump_paths=self.dumpfiles, tables=tuple(tables))

    def import_table_chunk(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        query: str | None = None,
        partition_name: str | None = None,
    ) -> int:
        with self._connect() as conn:
            drop_table(conn, self.stage_schema, table)
        job = ImportJob(
            connection=DataPumpConnection(self.admin.user, self.admin.password, self.admin.service),
            directory=self.directory,
            dumpfiles=self.dumpfiles,
            logfile=f"impdp-{source_schema}-{table}-{chunk_name}.log"[:120],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, self.stage_schema),
            query=query,
            partition_name=partition_name,
        )
        self.datapump.run_impdp(job)
        with self._connect() as conn:
            return count_rows(conn, self.stage_schema, table)

    def export_stage_table(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        output_dir: Path,
    ) -> ChunkConversionResult:
        table_dir = (
            output_dir
            / filesystem_safe_identifier(source_schema)
            / filesystem_safe_identifier(table)
        )
        parquet_path = table_dir / f"{filesystem_safe_identifier(chunk_name)}.parquet"
        with self._connect() as conn:
            metadata = discover_table_metadata(conn, self.stage_schema, table)
            imported_rows = count_rows(conn, self.stage_schema, table)
            export_result = export_table_to_parquet(
                conn,
                schema_name=self.stage_schema,
                table_name=table,
                columns=metadata.columns,
                output_path=parquet_path,
            )
        return ChunkConversionResult(
            name=chunk_name,
            imported_rows=imported_rows,
            parquet_rows=export_result.rows,
            parquet_path=export_result.path,
        )

    def drop_stage_table(self, table: str) -> None:
        with self._connect() as conn:
            drop_table(conn, self.stage_schema, table)

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
        self.prepare_stage_schema()
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
                self.drop_stage_table(table)

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
                self.drop_stage_table(table)

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
            self.drop_stage_table(table_plan.table)

    def convert_table_plan(
        self,
        table_plan: TablePlan,
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> TableConversionResult:
        if table_plan.strategy == TableStrategy.UNSUPPORTED:
            reason = table_plan.reason or "unsupported table conversion strategy"
            raise ValueError(f"{table_plan.qualified_name}: {reason}")

        self.prepare_stage_schema()
        chunk_results: list[ChunkConversionResult] = []
        for chunk in table_plan.chunks:
            state = state_store.get(table_plan.qualified_name, chunk.name) if state_store else None
            if state and state.status == "completed":
                parquet_path = (
                    output_dir
                    / filesystem_safe_identifier(table_plan.schema)
                    / filesystem_safe_identifier(table_plan.table)
                    / f"{filesystem_safe_identifier(chunk.name)}.parquet"
                )
                chunk_results.append(
                    ChunkConversionResult(
                        name=chunk.name,
                        imported_rows=state.imported_rows or 0,
                        parquet_rows=state.parquet_rows or 0,
                        parquet_path=parquet_path,
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
                if result.imported_rows != result.parquet_rows:
                    msg = (
                        f"row count mismatch for {table_plan.qualified_name} {chunk.name}: "
                        f"imported={result.imported_rows}, parquet={result.parquet_rows}"
                    )
                    raise ValueError(msg)
                if state_store:
                    state_store.upsert(
                        ChunkState(
                            table_plan.qualified_name,
                            chunk.name,
                            "completed",
                            result.imported_rows,
                            result.parquet_rows,
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
            results.append(self.convert_table_plan(table_plan, output_dir, state_store))
        return PlanConversionResult(tables=tuple(results))
