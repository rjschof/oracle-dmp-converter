"""Conversion planning for staged Oracle imports."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from oracle_dmp_converter.config import ConverterConfig, table_override
from oracle_dmp_converter.models import (
    ChunkPlan,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)

LOGGER = logging.getLogger(__name__)


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

    if override and override.strategy is not None:
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=(
                f"strategy {override.strategy!r} is not supported; use 'whole' to override "
                "the default strategy for this table"
            ),
        )

    if dump_format == DumpFormat.LEGACY or not table.partitions:
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )

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


def plan_tables(
    tables: Iterable[TableMetadata],
    config: ConverterConfig,
    dump_format: DumpFormat = DumpFormat.DATAPUMP,
) -> tuple[TablePlan, ...]:
    return tuple(plan_table(table, config, dump_format) for table in tables)
