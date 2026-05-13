from dmp_to_parquet.config import ConverterConfig, TableOverride
from dmp_to_parquet.models import ColumnMetadata, PartitionMetadata, TableMetadata, TableStrategy
from dmp_to_parquet.planner import hash_bucket_query, plan_table


def test_hash_bucket_query_uses_ora_hash_max_bucket() -> None:
    assert hash_bucket_query("ID", 2, 4) == "ID IS NOT NULL AND ORA_HASH(ID, 3) = 2"


def test_small_table_plans_whole_import() -> None:
    table = TableMetadata(
        schema="HR",
        name="EMP",
        columns=(ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),),
        estimated_bytes=1024,
    )
    plan = plan_table(table, ConverterConfig(max_stage_gb=8))
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
    plan = plan_table(table, ConverterConfig(max_stage_gb=8))
    assert plan.strategy == TableStrategy.PARTITION
    assert [chunk.partition_name for chunk in plan.chunks] == ["P1", "P2"]


def test_large_table_plans_hash_chunks_from_primary_key() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
        estimated_bytes=100 * 1024**3,
        primary_key=("ID",),
    )
    plan = plan_table(table, ConverterConfig(max_stage_gb=8, default_hash_buckets=4))
    assert plan.strategy == TableStrategy.HASH
    assert plan.split_column == "ID"
    assert len(plan.chunks) == 4


def test_large_nullable_table_adds_null_bucket() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("CUSTOMER_ID", "NUMBER", 1, nullable=True),),
        estimated_bytes=100 * 1024**3,
    )
    plan = plan_table(table, ConverterConfig(max_stage_gb=8, default_hash_buckets=4))
    assert len(plan.chunks) == 5
    assert plan.chunks[-1].query == "CUSTOMER_ID IS NULL"


def test_override_split_column_and_buckets() -> None:
    table = TableMetadata(
        schema="SALES",
        name="FACT",
        columns=(ColumnMetadata("TENANT_ID", "NUMBER", 1, nullable=False),),
        estimated_bytes=100 * 1024**3,
    )
    plan = plan_table(
        table,
        ConverterConfig(
            max_stage_gb=8,
            tables={
                "SALES.FACT": TableOverride(
                    strategy="hash",
                    split_column="TENANT_ID",
                    buckets=8,
                )
            },
        ),
    )
    assert len(plan.chunks) == 8
    assert plan.split_column == "TENANT_ID"
