"""Conversion planning for staged Oracle imports."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from oracle_dmp_converter.config import ConverterConfig, TableOverride, table_override
from oracle_dmp_converter.errors import PlanningError
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)


def hash_bucket_query(split_column: str, bucket_index: int, bucket_count: int) -> str:
    column = oracle_identifier(split_column)
    max_bucket = bucket_count - 1
    return f"{column} IS NOT NULL AND ORA_HASH({column}, {max_bucket}) = {bucket_index}"


def null_bucket_query(split_column: str) -> str:
    return f"{oracle_identifier(split_column)} IS NULL"


def _assert_hash_candidate(table: TableMetadata, col: ColumnMetadata) -> None:
    """Raise PlanningError if *col* cannot be used with ORA_HASH.

    Called when the user has explicitly named a split column in the config
    override so that an invalid choice is rejected at plan time rather than
    failing inside Oracle at runtime.
    """
    if not col.is_hash_candidate:
        raise PlanningError(
            f"{table.schema}.{table.name}: split_column {col.name!r} "
            f"is type {col.normalized_type}, which is not eligible for ORA_HASH "
            "(BFILE, BLOB, CLOB, LONG, LONG RAW, NCLOB, RAW, ROWID, UROWID, and "
            "XMLTYPE columns cannot be used as hash split columns)"
        )


def _scan_columns_for_split(
    columns: tuple[ColumnMetadata, ...],
    preferred_types: tuple[str, ...],
) -> ColumnMetadata | None:
    """Return the best available non-key split column, or None.

    Preference order: non-nullable preferred type → any preferred type →
    any hash-candidate type.
    """
    for col in columns:
        if col.is_hash_candidate and col.normalized_type in preferred_types and not col.nullable:
            return col
    for col in columns:
        if col.is_hash_candidate and col.normalized_type in preferred_types:
            return col
    for col in columns:
        if col.is_hash_candidate:
            return col
    return None


def choose_split_column(
    table: TableMetadata,
    override: TableOverride | None = None,
) -> ColumnMetadata | None:
    if override and override.split_column:
        col = table.column(override.split_column)
        if col is not None:
            _assert_hash_candidate(table, col)
        return col

    if len(table.primary_key) == 1:
        column = table.column(table.primary_key[0])
        if column and column.is_hash_candidate:
            return column

    for unique_key in table.unique_keys:
        if len(unique_key) == 1:
            column = table.column(unique_key[0])
            if column and column.is_hash_candidate:
                return column

    preferred_types = (
        "NUMBER",
        "INTEGER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
        "DATE",
        "TIMESTAMP",
        "VARCHAR2",
        "CHAR",
        "NVARCHAR2",
        "NCHAR",
    )
    return _scan_columns_for_split(table.columns, preferred_types)


def plan_table(
    table: TableMetadata,
    config: ConverterConfig,
    dump_format: DumpFormat = DumpFormat.DATAPUMP,
) -> TablePlan:
    override = table_override(config, table.schema, table.name)

    if override and override.strategy == "whole":
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )

    # Legacy exp dumps cannot be hash-chunked (no QUERY= support in imp) and
    # do not support reliable partition-level imports.  Restrict to
    # WHOLE_TABLE; tables that exceed the staging limit are UNSUPPORTED.
    if dump_format == DumpFormat.LEGACY:
        force_large = bool(override and override.force_large)
        if not force_large and (
            table.estimated_bytes is None or table.estimated_bytes <= config.max_stage_bytes
        ):
            warnings: tuple[str, ...] = ()
            if table.estimated_bytes is None:
                warnings = ("table size is unknown; planning whole-table import",)
            return TablePlan(
                schema=table.schema,
                table=table.name,
                strategy=TableStrategy.WHOLE_TABLE,
                chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
                warnings=warnings,
            )
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=(
                "table exceeds staging limit and legacy exp dumps do not support "
                "hash chunking; re-export with expdp to enable hash-chunked conversion"
            ),
        )

    if table.partitions:
        partition_chunks = tuple(
            ChunkPlan(
                name=f"partition-{partition.position:05d}-{partition.name}",
                strategy=TableStrategy.PARTITION,
                partition_name=partition.name,
            )
            for partition in table.partitions
        )
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.PARTITION,
            chunks=partition_chunks,
        )

    force_large = bool(override and override.force_large)
    if not force_large and (
        table.estimated_bytes is None or table.estimated_bytes <= config.max_stage_bytes
    ):
        warnings = ()
        if table.estimated_bytes is None:
            warnings = ("table size is unknown; planning whole-table import",)
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
            warnings=warnings,
        )

    split_column = choose_split_column(table, override)
    if split_column is None:
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason="table exceeds staging limit and no scalar split column is available",
        )

    bucket_count = (
        override.buckets if override and override.buckets else config.default_hash_buckets
    )
    if bucket_count < 1:
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason="hash bucket count must be at least 1",
        )

    hash_chunks: list[ChunkPlan] = []
    for bucket_index in range(bucket_count):
        hash_chunks.append(
            ChunkPlan(
                name=f"hash-{bucket_index:05d}-of-{bucket_count:05d}",
                strategy=TableStrategy.HASH,
                query=hash_bucket_query(split_column.name, bucket_index, bucket_count),
                bucket_index=bucket_index,
                bucket_count=bucket_count,
            )
        )
    if split_column.nullable:
        hash_chunks.append(
            ChunkPlan(
                name="hash-null",
                strategy=TableStrategy.HASH,
                query=null_bucket_query(split_column.name),
                bucket_count=bucket_count,
            )
        )

    return TablePlan(
        schema=table.schema,
        table=table.name,
        strategy=TableStrategy.HASH,
        chunks=tuple(hash_chunks),
        split_column=split_column.name,
    )


def plan_tables(
    tables: Iterable[TableMetadata],
    config: ConverterConfig,
    dump_format: DumpFormat = DumpFormat.DATAPUMP,
) -> tuple[TablePlan, ...]:
    return tuple(plan_table(table, config, dump_format) for table in tables)
