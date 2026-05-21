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
    """Build a :class:`TablePlan` for a single Oracle table.

    Strategy selection follows this priority order:

    1. If the config contains an explicit ``"whole"`` strategy override, return
       a single whole-table chunk regardless of partitions.
    2. If the config contains any other strategy override, return
       ``UNSUPPORTED`` with a descriptive reason (only ``"whole"`` is
       implemented; ``"range"`` and hash are handled elsewhere).
    3. If the dump is in legacy ``exp`` format, always return a whole-table
       chunk (legacy ``imp`` does not support ``QUERY=`` filtering).
    4. If the table has no partitions, return a whole-table chunk.
    5. Otherwise return one ``PARTITION`` chunk per partition.

    Args:
        table: Metadata for the table being planned.
        config: Active converter configuration, used to look up any per-table
            override.
        dump_format: Format of the source dump file.  Legacy dumps always
            produce whole-table plans.

    Returns:
        A :class:`TablePlan` with an appropriate strategy and chunk list.
    """
    override = table_override(config, table.schema, table.name)

    if override and override.strategy == "whole":
        LOGGER.debug("%s.%s: strategy=whole (config override)", table.schema, table.name)
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )

    if override and override.strategy is not None:
        LOGGER.debug(
            "%s.%s: strategy=unsupported (unrecognised config override %r)",
            table.schema, table.name, override.strategy,
        )
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
        LOGGER.debug(
            "%s.%s: strategy=whole (%s)",
            table.schema, table.name,
            "legacy dump format" if dump_format == DumpFormat.LEGACY else "no partitions",
        )
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )

    LOGGER.debug(
        "%s.%s: strategy=partition (%d partitions)", table.schema, table.name, len(table.partitions)
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
    """Build a :class:`TablePlan` for every table in an iterable.

    Delegates to :func:`plan_table` for each element and collects the results
    into an immutable tuple.

    Args:
        tables: Iterable of :class:`TableMetadata` instances to plan.
        config: Active converter configuration.
        dump_format: Format of the source dump file.

    Returns:
        Tuple of :class:`TablePlan` instances in the same order as *tables*.
    """
    return tuple(plan_table(table, config, dump_format) for table in tables)
