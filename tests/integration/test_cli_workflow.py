from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from dmp_to_parquet.cli import main
from dmp_to_parquet.config import DEFAULT_ORACLE_IMAGE
from dmp_to_parquet.converter import OracleAdminConnection
from dmp_to_parquet.datapump import DataPumpRunner
from dmp_to_parquet.docker_oracle import DockerOracle, docker_available
from dmp_to_parquet.oracle_conn import create_directory, drop_schema, oracle_connection
from dmp_to_parquet.parfile import DataPumpConnection, ExportJob
from dmp_to_parquet.validation import count_parquet_rows

pytestmark = pytest.mark.integration


def _setup_cli_source(admin: OracleAdminConnection) -> None:
    with oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    ) as conn:
        drop_schema(conn, "CLISRC")
        with conn.cursor() as cursor:
            cursor.execute('CREATE USER CLISRC IDENTIFIED BY "CliSrcPwd_123"')
            cursor.execute("GRANT CONNECT, RESOURCE TO CLISRC")
            cursor.execute("ALTER USER CLISRC QUOTA UNLIMITED ON USERS")
            cursor.execute(
                """
                CREATE TABLE CLISRC.EVENTS (
                    ID NUMBER(10, 0) NOT NULL,
                    NAME VARCHAR2(40),
                    CONSTRAINT EVENTS_PK PRIMARY KEY (ID)
                )
                """
            )
            cursor.executemany(
                "INSERT INTO CLISRC.EVENTS(ID, NAME) VALUES (:1, :2)",
                [(idx, f"event-{idx}") for idx in range(1, 13)],
            )
        conn.commit()


def _export_cli_dump(tmp_path: Path, image: str, password: str) -> Path:
    dumpfile = "cli_full.dmp"
    with DockerOracle.start(
        image=image,
        password=password,
        mounts=((tmp_path, "/dumps", "rw"),),
    ) as container:
        container.wait_ready(timeout_seconds=900)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )
        _setup_cli_source(admin)
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            create_directory(conn, "CLI_DUMP", "/dumps")
        DataPumpRunner(container, tmp_path / "export-parfiles").run_expdp(
            ExportJob(
                connection=DataPumpConnection("system", password, container.service),
                directory="CLI_DUMP",
                dumpfile=dumpfile,
                logfile="cli_full_export.log",
                include_schemas=("CLISRC",),
            )
        )
    return tmp_path / dumpfile


def _read_event_ids(parquet_files: list[Path]) -> set[int]:
    ids: set[int] = set()
    for path in parquet_files:
        table = pq.read_table(path, columns=["ID"])
        ids.update(int(value.as_py()) for value in table.column("ID"))
    return ids


def test_cli_inspect_plan_convert_workflow(tmp_path: Path) -> None:
    if not docker_available():
        pytest.skip("Docker is not available")

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dump_path = _export_cli_dump(tmp_path, image, password)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
oracle:
  image: gvenzl/oracle-free:23-slim
  max_stage_gb: 8
default_hash_buckets: 4
tables:
  CLISRC.EVENTS:
    strategy: hash
    split_column: ID
    buckets: 4
    force_large: true
"""
    )

    runner = CliRunner()
    manifest_path = tmp_path / "work" / "manifest.json"
    plan_path = tmp_path / "work" / "plan.yaml"
    output_dir = tmp_path / "parquet"

    inspect_result = runner.invoke(
        main,
        [
            "inspect",
            "--dump",
            str(dump_path),
            "--work-dir",
            str(tmp_path / "work"),
            "--output",
            str(manifest_path),
            "--oracle-image",
            image,
            "--oracle-password",
            password,
        ],
    )
    assert inspect_result.exit_code == 0, inspect_result.output
    assert manifest_path.exists()

    plan_result = runner.invoke(
        main,
        [
            "plan",
            "--manifest",
            str(manifest_path),
            "--config",
            str(config_path),
            "--output",
            str(plan_path),
        ],
    )
    assert plan_result.exit_code == 0, plan_result.output
    assert plan_path.exists()

    convert_result = runner.invoke(
        main,
        [
            "convert",
            "--plan",
            str(plan_path),
            "--dump",
            str(dump_path),
            "--output",
            str(output_dir),
            "--work-dir",
            str(tmp_path / "work"),
            "--oracle-password",
            password,
        ],
    )
    assert convert_result.exit_code == 0, convert_result.output

    parquet_files = sorted((output_dir / "CLISRC" / "EVENTS").glob("*.parquet"))
    assert len(parquet_files) == 4
    assert count_parquet_rows(parquet_files) == 12
    assert _read_event_ids(parquet_files) == set(range(1, 13))
