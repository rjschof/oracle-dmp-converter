"""Conversion planning for staged Oracle imports."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from oracle_dmp_converter.config import ConverterConfig, table_override
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.types import UNSUPPORTED_COLUMN_TYPES

LOGGER = logging.getLogger(__name__)


# Oracle reports owner-qualified type names for some built-in types that
# the converter handles natively (XMLTYPE is owned by PUBLIC, SDO_GEOMETRY
# by MDSYS).  These should *not* trigger the "user-defined type" path.
_BUILTIN_OWNED_TYPES = frozenset({"XMLTYPE", "SDO_GEOMETRY"})


def _unsupported_column_reason(column: ColumnMetadata) -> str | None:
    """Return a human-readable reason if *column* can't be safely exported.

    ``None`` means the column is fine.  Otherwise the returned string is
    suitable for use as ``TablePlan.reason``.
    """
    normalized = column.normalized_type
    if normalized in UNSUPPORTED_COLUMN_TYPES:
        return f"column {column.name!r} has unsupported type {normalized}"
    if normalized in _BUILTIN_OWNED_TYPES:
        # Owner-qualified built-ins (PUBLIC.XMLTYPE, MDSYS.SDO_GEOMETRY)
        # have ``data_type_owner`` set but are handled natively via
        # type-specific export expressions — let them through.
        return None
    # Genuine user-defined OBJECT / VARRAY / nested-table columns carry a
    # ``data_type_owner`` value pointing at the type's owning schema.
    # The converter cannot meaningfully serialise these via a normal
    # SELECT — oracledb returns DbObject handles that ``str()`` to repr
    # noise.  Mark the whole table UNSUPPORTED rather than emit garbage.
    if column.data_type_owner:
        return (
            f"column {column.name!r} has user-defined type "
            f"{column.data_type_owner}.{column.data_type}"
        )
    return None


def _table_unsupported_reason(table: TableMetadata) -> str | None:
    """Return a human-readable reason if *table* can't be exported at all.

    External tables: the LOCATION file isn't bind-mounted into the
    staging container, so the staged SELECT would raise KUP-04040.
    Global temporary tables: rows are session-scoped and never persist
    through Data Pump export/import, so the result would always be 0.
    """
    if table.table_type == "EXTERNAL":
        return "external table — LOCATION file not available in staging container"
    if table.table_type == "GTT":
        return (
            "global temporary table — data is session-scoped and does not "
            "round-trip through Data Pump export"
        )
    for column in table.columns:
        reason = _unsupported_column_reason(column)
        if reason is not None:
            return reason
    return None


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

    unsupported = _table_unsupported_reason(table)
    if unsupported is not None:
        LOGGER.debug("%s.%s: strategy=unsupported (%s)", table.schema, table.name, unsupported)
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=unsupported,
        )

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
            table.schema,
            table.name,
            override.strategy,
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
            table.schema,
            table.name,
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
