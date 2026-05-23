import json
from pathlib import Path

import yaml

from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ContainerSession,
    ConversionPlan,
    DumpFormat,
    DumpManifest,
    PartitionMetadata,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.persistence.serialization import (
    column_to_dict,
    load_manifest,
    load_plan,
    load_session,
    save_manifest,
    save_plan,
    save_session,
    table_metadata_to_dict,
)


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = DumpManifest(
        dump_paths=("/tmp/full.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
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
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="podman",
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
    assert loaded.container_runtime == "podman"


def test_manifest_round_trip_missing_runtime_fields(tmp_path: Path) -> None:
    """Old manifest.json files without oracle_image/container_runtime load with empty defaults."""

    payload = {
        "version": 1,
        "dump_format": "datapump",
        "dump_paths": ["/tmp/old.dmp"],
        "tables": [],
    }
    path = tmp_path / "old_manifest.json"
    path.write_text(json.dumps(payload) + "\n")
    loaded = load_manifest(path)
    assert loaded.oracle_image == ""
    assert loaded.container_runtime == ""


def test_plan_round_trip(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/full.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
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
        container_runtime="podman",
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
    assert loaded.container_runtime == "podman"


def test_plan_round_trip_legacy_format(tmp_path: Path) -> None:
    plan = ConversionPlan(
        dump_paths=("/tmp/legacy.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        dump_format=DumpFormat.LEGACY,
        container_runtime="docker",
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


def test_plan_round_trip_missing_container_runtime(tmp_path: Path) -> None:
    """Old plan.yaml files without container_runtime load with 'docker' default."""

    payload = {
        "version": 1,
        "dump_format": "datapump",
        "dump_paths": ["/tmp/old.dmp"],
        "oracle_image": "gvenzl/oracle-free:23-faststart",
        "tables": [],
    }
    path = tmp_path / "old_plan.yaml"
    path.write_text(yaml.safe_dump(payload))
    loaded = load_plan(path)
    assert loaded.container_runtime == "docker"


def test_session_round_trip(tmp_path: Path) -> None:
    session = ContainerSession(
        container_name="oracle-dmp-converter-abc123def456",
        container_runtime="docker",
        oracle_image="gvenzl/oracle-free:23-faststart",
        oracle_service="FREEPDB1",
        work_dir="/tmp/work",
        dump_dir="/tmp/dumps",
        created_at="2026-05-20T14:30:00+00:00",
    )
    path = tmp_path / "session.json"
    save_session(path, session)
    loaded = load_session(path)
    assert loaded == session


def test_session_round_trip_podman(tmp_path: Path) -> None:
    session = ContainerSession(
        container_name="oracle-dmp-converter-deadbeef0000",
        container_runtime="podman",
        oracle_image="gvenzl/oracle-free:21-faststart",
        oracle_service="FREEPDB1",
        work_dir="/home/user/work",
        dump_dir="/home/user/dumps",
        created_at="2026-01-01T00:00:00+00:00",
    )
    path = tmp_path / "session_podman.json"
    save_session(path, session)
    loaded = load_session(path)
    assert loaded.container_runtime == "podman"
    assert loaded.oracle_image == "gvenzl/oracle-free:21-faststart"
    assert loaded == session


def test_session_creates_parent_dirs(tmp_path: Path) -> None:
    session = ContainerSession(
        container_name="oracle-dmp-converter-abc",
        container_runtime="docker",
        oracle_image="gvenzl/oracle-free:23-faststart",
        oracle_service="FREEPDB1",
        work_dir=str(tmp_path),
        dump_dir="/tmp/dumps",
        created_at="2026-05-20T12:00:00+00:00",
    )
    path = tmp_path / "nested" / "deep" / "session.json"
    save_session(path, session)
    assert path.exists()
    loaded = load_session(path)
    assert loaded == session


def test_session_missing_optional_fields(tmp_path: Path) -> None:
    """session.json files with missing optional fields use safe defaults."""

    payload = {
        "version": 1,
        "container_name": "oracle-dmp-converter-mintest",
        "container_runtime": "docker",
    }
    path = tmp_path / "minimal_session.json"
    path.write_text(json.dumps(payload) + "\n")
    loaded = load_session(path)
    assert loaded.oracle_image == ""
    assert loaded.oracle_service == "FREEPDB1"
    assert loaded.work_dir == ""
    assert loaded.dump_dir == ""
    assert loaded.created_at == ""
    assert loaded.container_name == "oracle-dmp-converter-mintest"


def test_manifest_round_trip_with_partitions(tmp_path: Path) -> None:
    """Manifests that include PartitionMetadata round-trip correctly."""
    manifest = DumpManifest(
        dump_paths=("/tmp/part.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
        tables=(
            TableMetadata(
                schema="SRC",
                name="FACT",
                columns=(ColumnMetadata("ID", "NUMBER", 1, False, 10, 0),),
                estimated_bytes=2048,
                row_count=100,
                partitions=(
                    PartitionMetadata(name="P_2024_01", position=1),
                    PartitionMetadata(name="P_2024_02", position=2),
                ),
            ),
        ),
    )
    path = tmp_path / "manifest_parts.json"
    save_manifest(path, manifest)
    loaded = load_manifest(path)
    assert loaded == manifest
    assert loaded.tables[0].partitions[0].name == "P_2024_01"
    assert loaded.tables[0].partitions[1].position == 2


def test_manifest_round_trip_carries_new_column_fields(tmp_path: Path) -> None:
    """data_type_owner / hidden / comment + table_type / comment must round-trip."""
    manifest = DumpManifest(
        dump_paths=("/tmp/full.dmp",),
        tables=(
            TableMetadata(
                schema="FINANCE",
                name="CUSTOMER_PROFILE",
                columns=(
                    ColumnMetadata(
                        name="ID",
                        data_type="NUMBER",
                        ordinal=1,
                        data_precision=10,
                        data_scale=0,
                        comment="primary key",
                    ),
                    ColumnMetadata(
                        name="ADDR",
                        data_type="ADDRESS_T",
                        ordinal=2,
                        data_type_owner="FINANCE",
                    ),
                    ColumnMetadata(
                        name="HIDDEN_COL",
                        data_type="VARCHAR2",
                        ordinal=3,
                        hidden=True,
                    ),
                ),
                table_type="GTT",
                comment="customer profile master",
            ),
        ),
    )
    path = tmp_path / "manifest.json"
    save_manifest(path, manifest)
    loaded = load_manifest(path)

    table = loaded.tables[0]
    assert table.table_type == "GTT"
    assert table.comment == "customer profile master"
    assert table.columns[0].comment == "primary key"
    assert table.columns[1].data_type_owner == "FINANCE"
    assert table.columns[2].hidden is True


def test_table_metadata_dict_omits_default_fields() -> None:
    """Plain tables should not bloat the manifest with default-valued fields."""
    plain_col = ColumnMetadata(name="ID", data_type="NUMBER", ordinal=1)
    payload = column_to_dict(plain_col)
    assert "data_type_owner" not in payload
    assert "hidden" not in payload
    assert "comment" not in payload

    plain_table = TableMetadata(
        schema="HR",
        name="EMPLOYEES",
        columns=(plain_col,),
    )
    tbl_payload = table_metadata_to_dict(plain_table)
    assert "table_type" not in tbl_payload
    assert "comment" not in tbl_payload
