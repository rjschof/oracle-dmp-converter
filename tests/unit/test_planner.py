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


def test_external_table_is_unsupported() -> None:
    """External tables have no physical data in the staging container."""
    table = TableMetadata(
        schema="INVENTORY",
        name="EXT_FEED",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        table_type="EXTERNAL",
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.UNSUPPORTED
    assert plan.reason is not None
    assert "external table" in plan.reason.lower()


def test_gtt_is_unsupported() -> None:
    """Global temporary tables can't carry data through Data Pump."""
    table = TableMetadata(
        schema="AUDITLOG",
        name="GTT_STAGING",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        table_type="GTT",
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.UNSUPPORTED
    assert "temporary" in (plan.reason or "").lower()


def test_bfile_column_marks_table_unsupported() -> None:
    """A single BFILE column forces the whole table to UNSUPPORTED."""
    table = TableMetadata(
        schema="AUDITLOG",
        name="ATTACHMENTS",
        columns=(
            ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
            ColumnMetadata("FILE_REF", "BFILE", 2),
        ),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.UNSUPPORTED
    assert "BFILE" in (plan.reason or "")


def test_object_type_column_marks_table_unsupported() -> None:
    """User-defined OBJECT-typed columns force UNSUPPORTED (no string repr)."""
    table = TableMetadata(
        schema="FINANCE",
        name="CUSTOMER_PROFILE",
        columns=(
            ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
            ColumnMetadata("ADDR", "ADDRESS_T", 2, data_type_owner="FINANCE"),
        ),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.UNSUPPORTED
    assert "user-defined type" in (plan.reason or "")
    assert "FINANCE.ADDRESS_T" in (plan.reason or "")


def test_varray_column_marks_table_unsupported() -> None:
    """VARRAY columns (collection types) are treated the same as object types."""
    table = TableMetadata(
        schema="HRDATA",
        name="EMP_TAGS",
        columns=(
            ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
            ColumnMetadata("TAGS", "TAG_LIST", 2, data_type_owner="HRDATA"),
        ),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.UNSUPPORTED
