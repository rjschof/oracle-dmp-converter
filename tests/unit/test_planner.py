from oracle_dmp_converter.config import ConverterConfig, TableOverride
from oracle_dmp_converter.models import (
    ColumnMetadata,
    DumpFormat,
    PartitionMetadata,
    SubpartitionMetadata,
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


def test_legacy_format_partitioned_table_now_drills_down() -> None:
    """Legacy (exp) dumps now produce PARTITION plans for partitioned tables.

    Oracle's ``imp`` accepts partition (and subpartition) names directly in
    ``TABLES=schema.table:NAME``, so we no longer force WHOLE_TABLE for
    legacy dumps.  Empirically verified by
    ``scripts/verify_legacy_subpartition_import.py``.
    """
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=100 * 1024**3,
        partitions=(PartitionMetadata("P1", 1), PartitionMetadata("P2", 2)),
    )
    plan = plan_table(table, ConverterConfig(), dump_format=DumpFormat.LEGACY)
    assert plan.strategy == TableStrategy.PARTITION
    assert [c.partition_name for c in plan.chunks] == ["P1", "P2"]


def test_legacy_format_composite_table_drills_into_subpartitions() -> None:
    """Legacy + composite partitioning: one chunk per physical subpartition."""
    table = TableMetadata(
        schema="FINANCE",
        name="TXN_DETAILS",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        partitions=(
            PartitionMetadata(
                "P_2024",
                1,
                subpartitions=(
                    SubpartitionMetadata("SYS_SUBP001", 1, "P_2024"),
                    SubpartitionMetadata("SYS_SUBP002", 2, "P_2024"),
                ),
            ),
        ),
    )
    plan = plan_table(table, ConverterConfig(), dump_format=DumpFormat.LEGACY)
    assert plan.strategy == TableStrategy.PARTITION
    assert len(plan.chunks) == 2
    assert all(c.subpartition_name is not None for c in plan.chunks)


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


def test_composite_partitions_emit_per_subpartition_chunks() -> None:
    """Composite (RANGE-HASH etc.) partitioning emits one chunk per subpartition."""
    table = TableMetadata(
        schema="FINANCE",
        name="TXN_DETAILS",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        partitions=(
            PartitionMetadata(
                "P_2024",
                1,
                subpartitions=(
                    SubpartitionMetadata("P_2024_SP1", 1, "P_2024"),
                    SubpartitionMetadata("P_2024_SP2", 2, "P_2024"),
                ),
            ),
            PartitionMetadata(
                "P_2025",
                2,
                subpartitions=(
                    SubpartitionMetadata("P_2025_SP1", 1, "P_2025"),
                    SubpartitionMetadata("P_2025_SP2", 2, "P_2025"),
                ),
            ),
        ),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.PARTITION
    assert len(plan.chunks) == 4
    names = [chunk.name for chunk in plan.chunks]
    assert names == [
        "subpartition-00001-00001-P_2024-P_2024_SP1",
        "subpartition-00001-00002-P_2024-P_2024_SP2",
        "subpartition-00002-00001-P_2025-P_2025_SP1",
        "subpartition-00002-00002-P_2025-P_2025_SP2",
    ]
    for chunk in plan.chunks:
        assert chunk.partition_name is not None
        assert chunk.subpartition_name is not None


def test_partition_without_subpartitions_uses_partition_chunk() -> None:
    """A partition with an empty subpartitions tuple falls back to a partition chunk."""
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1),),
        partitions=(PartitionMetadata("P1", 1, subpartitions=()),),
    )
    plan = plan_table(table, ConverterConfig())
    assert len(plan.chunks) == 1
    assert plan.chunks[0].name == "partition-00001-P1"
    assert plan.chunks[0].subpartition_name is None


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


def test_builtin_owned_type_column_is_supported() -> None:
    """Owner-qualified built-ins (PUBLIC.XMLTYPE) are handled natively, not UNSUPPORTED."""
    table = TableMetadata(
        schema="DOCS",
        name="ARTICLES",
        columns=(
            ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
            ColumnMetadata("BODY", "XMLTYPE", 2, data_type_owner="PUBLIC"),
        ),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.WHOLE_TABLE


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
