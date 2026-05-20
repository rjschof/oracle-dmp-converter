from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.datapump.parfile import DataPumpConnection, ExportJob
from oracle_dmp_converter.datapump.runner import DataPumpRunner
from oracle_dmp_converter.docker_oracle import DockerOracle, docker_available
from oracle_dmp_converter.io.state import StateStore
from oracle_dmp_converter.io.validation import count_parquet_rows
from oracle_dmp_converter.models import ConversionPlan, TableStrategy
from oracle_dmp_converter.oracle.conn import drop_schema, oracle_connection, table_exists
from oracle_dmp_converter.planner import plan_tables

pytestmark = pytest.mark.integration


def _setup_source_schema(admin: OracleAdminConnection) -> None:
    with oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    ) as conn:
        drop_schema(conn, "SRC")
        with conn.cursor() as cursor:
            cursor.execute('CREATE USER SRC IDENTIFIED BY "SrcPwd_123"')
            cursor.execute("GRANT CONNECT, RESOURCE TO SRC")
            cursor.execute("ALTER USER SRC QUOTA UNLIMITED ON USERS")
            cursor.execute(
                """
                CREATE TABLE SRC.BIG_BUCKET_TABLE (
                    ID NUMBER(10, 0) NOT NULL,
                    PAYLOAD VARCHAR2(50),
                    CREATED_AT DATE,
                    CONSTRAINT BIG_BUCKET_TABLE_PK PRIMARY KEY (ID)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE SRC.SMALL_TABLE (
                    CODE VARCHAR2(10) NOT NULL,
                    DESCRIPTION VARCHAR2(50)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE SRC.PART_TABLE (
                    ID NUMBER(10, 0) NOT NULL,
                    REGION VARCHAR2(10)
                )
                PARTITION BY RANGE (ID) (
                    PARTITION P_LOW VALUES LESS THAN (10),
                    PARTITION P_HIGH VALUES LESS THAN (MAXVALUE)
                )
                """
            )
            rows = [(idx, f"payload-{idx}", idx) for idx in range(1, 33)]
            cursor.executemany(
                """
                INSERT INTO SRC.BIG_BUCKET_TABLE(ID, PAYLOAD, CREATED_AT)
                VALUES (:1, :2, DATE '2024-01-01' + :3)
                """,
                rows,
            )
            cursor.executemany(
                "INSERT INTO SRC.SMALL_TABLE(CODE, DESCRIPTION) VALUES (:1, :2)",
                [("A", "alpha"), ("B", "bravo"), ("C", "charlie")],
            )
            cursor.executemany(
                "INSERT INTO SRC.PART_TABLE(ID, REGION) VALUES (:1, :2)",
                [(idx, "low" if idx < 10 else "high") for idx in range(1, 15)],
            )
        conn.commit()


def _drop_source_schema(admin: OracleAdminConnection) -> None:
    with oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    ) as conn:
        drop_schema(conn, "SRC")
        assert not table_exists(conn, "SRC", "BIG_BUCKET_TABLE")


def _read_ids(parquet_files: list[Path]) -> set[int]:
    ids: set[int] = set()
    for path in parquet_files:
        table = pq.read_table(path, columns=["ID"])
        ids.update(int(value.as_py()) for value in table.column("ID"))
    return ids


def test_full_expdp_dump_whole_and_partition(tmp_path: Path) -> None:
    if not docker_available():
        pytest.skip("Docker is not available")

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dumpfile = "roundtrip_full.dmp"

    with DockerOracle.start(image=image, password=password) as container:
        container.wait_ready(timeout_seconds=120)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )
        _setup_source_schema(admin)

        datapump = DataPumpRunner(container, tmp_path / "parfiles")
        datapump.run_expdp(
            ExportJob(
                connection=DataPumpConnection("system", password, container.service),
                directory="DATA_PUMP_DIR",
                dumpfile=dumpfile,
                logfile="roundtrip_full_export.log",
                include_schemas=("SRC",),
            )
        )

        _drop_source_schema(admin)

        converter = OracleDumpConverter(
            container=container,
            admin=admin,
            work_dir=tmp_path / "work",
            dumpfiles=(dumpfile,),
            directory="DATA_PUMP_DIR",
        )
        manifest = converter.inspect_dump()
        assert {table.name for table in manifest.tables} == {
            "BIG_BUCKET_TABLE",
            "PART_TABLE",
            "SMALL_TABLE",
        }

        table_plans = plan_tables(manifest.tables, ConverterConfig())
        by_name = {table.table: table for table in table_plans}
        assert by_name["BIG_BUCKET_TABLE"].strategy == TableStrategy.WHOLE_TABLE
        assert by_name["PART_TABLE"].strategy == TableStrategy.PARTITION
        assert by_name["SMALL_TABLE"].strategy == TableStrategy.WHOLE_TABLE

        plan = ConversionPlan(
            dump_paths=(dumpfile,),
            tables=table_plans,
            oracle_image=image,
        )
        state = StateStore(tmp_path / "work" / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, tmp_path / "parquet", state)
        finally:
            state.close()

    big_files = sorted((tmp_path / "parquet" / "SRC" / "BIG_BUCKET_TABLE").glob("*.parquet"))
    part_files = sorted((tmp_path / "parquet" / "SRC" / "PART_TABLE").glob("*.parquet"))
    small_files = sorted((tmp_path / "parquet" / "SRC" / "SMALL_TABLE").glob("*.parquet"))
    assert len(big_files) == 1
    assert len(part_files) == 2
    assert len(small_files) == 1
    assert result.rows == 49
    assert count_parquet_rows(big_files) == 32
    assert count_parquet_rows(part_files) == 14
    assert count_parquet_rows(small_files) == 3
    assert _read_ids(big_files) == set(range(1, 33))
    assert all(
        chunk.imported_rows == chunk.output_rows
        for table_result in result.tables
        for chunk in table_result.chunks
    )
