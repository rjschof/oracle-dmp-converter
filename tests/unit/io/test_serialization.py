from pathlib import Path

from oracle_dmp_converter.io.serialization import load_manifest, load_plan, save_manifest, save_plan
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpFormat,
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


def test_manifest_round_trip_legacy_format(tmp_path: Path) -> None:
    manifest = DumpManifest(
        dump_paths=("/tmp/legacy.dmp",),
        dump_format=DumpFormat.LEGACY,
        tables=(
            TableMetadata(
                schema="SRC",
                name="EMP",
                columns=(ColumnMetadata("ID", "NUMBER", 1, False, 10, 0),),
                estimated_bytes=512,
                row_count=1,
            ),
        ),
    )
    path = tmp_path / "manifest_legacy.json"
    save_manifest(path, manifest)
    loaded = load_manifest(path)
    assert loaded == manifest
    assert loaded.dump_format == DumpFormat.LEGACY


def test_plan_round_trip(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/full.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(
            TablePlan(
                schema="SRC",
                table="EMP",
                strategy=TableStrategy.WHOLE_TABLE,
                chunks=(
                    ChunkPlan(
                        name="whole",
                        strategy=TableStrategy.WHOLE_TABLE,
                    ),
                ),
            ),
        ),
    )
    path = tmp_path / "plan.yaml"
    save_plan(path, plan)
    loaded = load_plan(path)
    assert loaded == plan


def test_plan_round_trip_with_partitions(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/full.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(
            TablePlan(
                schema="SRC",
                table="FACT",
                strategy=TableStrategy.PARTITION,
                chunks=(
                    ChunkPlan(
                        name="partition-00001-P1",
                        strategy=TableStrategy.PARTITION,
                        partition_name="P1",
                    ),
                    ChunkPlan(
                        name="partition-00002-P2",
                        strategy=TableStrategy.PARTITION,
                        partition_name="P2",
                    ),
                ),
            ),
        ),
    )
    path = tmp_path / "plan_parts.yaml"
    save_plan(path, plan)
    loaded = load_plan(path)
    assert loaded == plan


def test_plan_round_trip_legacy_format(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/legacy.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        dump_format=DumpFormat.LEGACY,
        tables=(
            TablePlan(
                schema="SRC",
                table="EMP",
                strategy=TableStrategy.WHOLE_TABLE,
                chunks=(
                    ChunkPlan(
                        name="whole",
                        strategy=TableStrategy.WHOLE_TABLE,
                    ),
                ),
            ),
        ),
    )
    path = tmp_path / "plan_legacy.yaml"
    save_plan(path, plan)
    loaded = load_plan(path)
    assert loaded == plan
    assert loaded.dump_format == DumpFormat.LEGACY
