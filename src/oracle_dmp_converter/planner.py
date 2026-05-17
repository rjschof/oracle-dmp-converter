"""Conversion planning for staged Oracle imports."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from oracle_dmp_converter.config import ConverterConfig, TableOverride, table_override
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


def choose_split_column(
    table: TableMetadata,
    override: TableOverride | None = None,
) -> ColumnMetadata | None:
    if override and override.split_column:
        return table.column(override.split_column)

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
    for column in table.columns:
        if (
            column.is_hash_candidate
            and column.normalized_type in preferred_types
            and not column.nullable
        ):
            return column
    for column in table.columns:
        if column.is_hash_candidate and column.normalized_type in preferred_types:
            return column
    for column in table.columns:
        if column.is_hash_candidate:
            return column
    return None


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

    if override and override.strategy == "range":
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=(
                "range planning requires explicit range boundaries; only hash chunking is automated"
            ),
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
