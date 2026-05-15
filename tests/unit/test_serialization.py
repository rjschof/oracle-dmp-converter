from pathlib import Path

from dmp_to_parquet.io.serialization import load_manifest, load_plan, save_manifest, save_plan
from dmp_to_parquet.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpManifest,
    TableMetadata,
    TablePlan,
    TableStrategy,
)


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = DumpManifest(
        dump_paths=("/tmp/full.dmp",),
        tables=(
            TableMetadata(
                schema="SRC",
                name="EMP",
                columns=(ColumnMetadata("ID", "NUMBER", 1, False, 10, 0),),
                estimated_bytes=1024,
                row_count=2,
                primary_key=("ID",),
            ),
        ),
    )
    path = tmp_path / "manifest.json"
    save_manifest(path, manifest)
    loaded = load_manifest(path)
    assert loaded == manifest


def test_plan_round_trip(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/full.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        max_stage_gb=8,
        tables=(
            TablePlan(
                schema="SRC",
                table="EMP",
                strategy=TableStrategy.HASH,
                split_column="ID",
                chunks=(
                    ChunkPlan(
                        name="hash-00000-of-00004",
                        strategy=TableStrategy.HASH,
                        query="ID IS NOT NULL AND ORA_HASH(ID, 3) = 0",
                        bucket_index=0,
                        bucket_count=4,
                    ),
                ),
            ),
        ),
    )
    path = tmp_path / "plan.yaml"
    save_plan(path, plan)
    loaded = load_plan(path)
    assert loaded == plan
