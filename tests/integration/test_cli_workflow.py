from __future__ import annotations

import os
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from oracle_dmp_converter.cli import main
from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.converter import OracleAdminConnection
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    render_legacy_export_parfile,
)
from oracle_dmp_converter.datapump.modern.parfile import ExportJob
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.io.validation import count_parquet_rows
from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    create_directory,
    drop_schema,
    oracle_connection,
)

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
        container.wait_ready(timeout_seconds=120)
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
                connection=OracleCredentials("system", password, container.service),
                directory="CLI_DUMP",
                dumpfile=dumpfile,
                logfile="cli_full_export.log",
                include_schemas=("CLISRC",),
            )
        )
        container.exec(
            [
                "bash",
                "-lc",
                "chmod a+r /dumps/cli_full.dmp /dumps/cli_full_export.log",
            ],
            check=False,
        )
    return tmp_path / dumpfile


def _export_cli_legacy_dump(tmp_path: Path, image: str, password: str) -> Path:
    """Export CLISRC schema using legacy exp inside a temporary container."""
    dump_dir = tmp_path / "legacy-dumps"
    dump_dir.mkdir(exist_ok=True)
    dumpfile = "cli_legacy.dmp"

    with DockerOracle.start(
        image=image,
        password=password,
        mounts=((dump_dir, "/dumps", "rw"),),
    ) as container:
        container.wait_ready(timeout_seconds=120)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )
        _setup_cli_source(admin)

        conn_spec = OracleCredentials(user="system", password=password, service=container.service)
        job = LegacyExportJob(
            connection=conn_spec,
            files=(f"/dumps/{dumpfile}",),
            logfile="/dumps/cli_legacy_export.log",
            owner=("CLISRC",),
            rows=True,
            indexes=False,
            grants=False,
            compress=False,
        )
        parfile_text = render_legacy_export_parfile(job)
        par_name = f"exp-cli-{uuid.uuid4().hex}.par"
        local_par = tmp_path / par_name
        local_par.write_text(parfile_text)
        remote_par = f"/tmp/{par_name}"
        container.copy_to(local_par, remote_par)
        result = container.exec(["exp", f"parfile={remote_par}"], check=False)
        if result.returncode != 0:
            pytest.fail(f"Legacy exp failed:\n{result.stdout}\n{result.stderr}")
        container.exec(["bash", "-lc", f"chmod a+r /dumps/{dumpfile}"], check=False)

    return dump_dir / dumpfile


def _read_event_ids(parquet_files: list[Path]) -> set[int]:
    ids: set[int] = set()
    for path in parquet_files:
        table = pq.read_table(path, columns=["ID"])
        ids.update(int(value.as_py()) for value in table.column("ID"))
    return ids


def test_cli_inspect_plan_convert_workflow(tmp_path: Path) -> None:

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dump_path = _export_cli_dump(tmp_path, image, password)

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
    assert len(parquet_files) == 1
    assert count_parquet_rows(parquet_files) == 12
    assert _read_event_ids(parquet_files) == set(range(1, 13))


def test_cli_legacy_inspect_plan_convert_workflow(tmp_path: Path) -> None:
    """End-to-end CLI test using a legacy exp dump."""

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dump_path = _export_cli_legacy_dump(tmp_path, image, password)

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
    assert len(parquet_files) == 1
    assert count_parquet_rows(parquet_files) == 12
    assert _read_event_ids(parquet_files) == set(range(1, 13))
