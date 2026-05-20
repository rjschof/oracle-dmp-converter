from oracle_dmp_converter.config import ConverterConfig, TableOverride
from oracle_dmp_converter.models import (
    ColumnMetadata,
    DumpFormat,
    PartitionMetadata,
    TableMetadata,
    TableStrategy,
)
from oracle_dmp_converter.planner import plan_table


def test_small_table_plans_whole_import() -> None:
    table = TableMetadata(
        schema="HR",
        name="EMP",
        columns=(ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),),
        estimated_bytes=1024,
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.WHOLE_TABLE
    assert len(plan.chunks) == 1


def test_partitioned_table_plans_partition_chunks() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=100 * 1024**3,
        partitions=(PartitionMetadata("P1", 1), PartitionMetadata("P2", 2)),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.PARTITION
    assert [chunk.partition_name for chunk in plan.chunks] == ["P1", "P2"]


def test_whole_strategy_override_forces_whole_table() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=100 * 1024**3,
        partitions=(PartitionMetadata("P1", 1), PartitionMetadata("P2", 2)),
    )
    plan = plan_table(
        table,
        ConverterConfig(tables={"SALES.FACT": TableOverride(strategy="whole")}),
    )
    assert plan.strategy == TableStrategy.WHOLE_TABLE
    assert len(plan.chunks) == 1


def test_legacy_format_table_plans_whole_table() -> None:
    """Legacy (exp) dumps always produce WHOLE_TABLE plans, even for partitioned tables."""
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=100 * 1024**3,
        partitions=(PartitionMetadata("P1", 1), PartitionMetadata("P2", 2)),
    )
    plan = plan_table(table, ConverterConfig(), dump_format=DumpFormat.LEGACY)
    assert plan.strategy == TableStrategy.WHOLE_TABLE
    assert len(plan.chunks) == 1


def test_unrecognized_strategy_returns_unsupported() -> None:
    """An unknown strategy override must produce an UNSUPPORTED plan with a clear reason."""
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=100 * 1024**3,
    )
    plan = plan_table(
        table,
        ConverterConfig(tables={"SALES.FACT": TableOverride(strategy="range")}),
    )
    assert plan.strategy == TableStrategy.UNSUPPORTED
    assert plan.reason is not None
    assert "range" in plan.reason


def test_chunk_names_are_zero_padded() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        partitions=(PartitionMetadata("P_NORTH", 1), PartitionMetadata("P_SOUTH", 2)),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.chunks[0].name == "partition-00001-P_NORTH"
    assert plan.chunks[1].name == "partition-00002-P_SOUTH"
