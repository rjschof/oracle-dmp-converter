"""Integration test: legacy exp dump → imp fallback → Parquet.

Creates a source schema, exports it with the legacy ``exp`` utility,
then runs the full inspect → plan → convert pipeline to verify that:

1. ``discover_dump_tables()`` detects the legacy format via ``ORA-39142``
   or ``ORA-39143`` and sets ``dump_format = DumpFormat.LEGACY``.
2. The manifest returned by ``inspect_dump()`` carries
   ``dump_format = DumpFormat.LEGACY``.
3. ``plan_tables()`` enforces WHOLE_TABLE strategy for all tables
   (no HASH or PARTITION).
4. ``convert_plan()`` successfully imports via ``imp`` and produces
   correct Parquet output with matching row counts.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    render_legacy_export_parfile,
)
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.io.state import StateStore
from oracle_dmp_converter.io.validation import count_parquet_rows
from oracle_dmp_converter.models import ConversionPlan, DumpFormat, TableStrategy
from oracle_dmp_converter.oracle.conn import OracleCredentials, drop_schema, oracle_connection
from oracle_dmp_converter.planner import plan_tables

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_source_schema(admin: OracleAdminConnection) -> None:
    """Create SRC schema with two small tables and insert test rows."""
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
                CREATE TABLE SRC.EMPLOYEES (
                    EMP_ID   NUMBER(10, 0) NOT NULL,
                    EMP_NAME VARCHAR2(50),
                    CONSTRAINT EMPLOYEES_PK PRIMARY KEY (EMP_ID)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE SRC.DEPARTMENTS (
                    DEPT_ID   NUMBER(5, 0) NOT NULL,
                    DEPT_NAME VARCHAR2(30)
                )
                """
            )
            cursor.executemany(
                "INSERT INTO SRC.EMPLOYEES(EMP_ID, EMP_NAME) VALUES (:1, :2)",
                [(i, f"Employee {i}") for i in range(1, 11)],
            )
            cursor.executemany(
                "INSERT INTO SRC.DEPARTMENTS(DEPT_ID, DEPT_NAME) VALUES (:1, :2)",
                [(1, "Engineering"), (2, "Marketing"), (3, "Finance")],
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


def _run_legacy_exp(
    container: DockerOracle,
    work_dir: Path,
    password: str,
    service: str,
    dump_path: str,
    log_path: str,
) -> None:
    """Export SRC schema using the legacy ``exp`` utility inside the container."""
    conn = OracleCredentials(user="system", password=password, service=service)
    job = LegacyExportJob(
        connection=conn,
        files=(dump_path,),
        logfile=log_path,
        owner=("SRC",),
        rows=True,
        indexes=False,
        grants=False,
        compress=False,
    )
    parfile_text = render_legacy_export_parfile(job)
    parfile_name = f"exp-{uuid.uuid4().hex}.par"
    local_parfile = work_dir / parfile_name
    local_parfile.write_text(parfile_text)
    remote_parfile = f"/tmp/{parfile_name}"
    container.copy_to(local_parfile, remote_parfile)

    result = container.exec(["exp", f"parfile={remote_parfile}"], check=False)
    output = result.stdout + result.stderr
    if result.returncode != 0:
        pytest.fail(f"Legacy exp export failed:\n{output}")


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_legacy_exp_dump_to_parquet(tmp_path: Path) -> None:

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dump_filename = "legacy_exp.dmp"

    dump_dir = tmp_path / "dumps"
    dump_dir.mkdir()
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    parquet_dir = tmp_path / "parquet"

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

        # Step 1: create source schema + data.
        _setup_source_schema(admin)

        # Step 2: export with legacy exp into the mounted dump dir.
        _run_legacy_exp(
            container=container,
            work_dir=work_dir,
            password=password,
            service=container.service,
            dump_path=f"/dumps/{dump_filename}",
            log_path="/dumps/legacy_exp.log",
        )

        # Step 3: drop the source schema so the converter must re-create it.
        _drop_source_schema(admin)

        # Step 4: set up Oracle DIRECTORY object pointing at /dumps.
        # Note: no explicit GRANT needed — system already has DBA privileges
        # and cannot grant to itself (ORA-01749).
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE OR REPLACE DIRECTORY DMP2PARQUET_DUMP AS '/dumps'")

        # Step 5: run inspect — should detect legacy format.
        converter = OracleDumpConverter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=(dump_filename,),
            directory="DMP2PARQUET_DUMP",
            directory_path="/dumps",
        )
        manifest = converter.inspect_dump()

        assert manifest.dump_format == DumpFormat.LEGACY, (
            f"Expected DumpFormat.LEGACY, got {manifest.dump_format}"
        )
        table_names = {t.name for t in manifest.tables}
        assert "EMPLOYEES" in table_names
        assert "DEPARTMENTS" in table_names

        # Step 6: plan — all tables must be WHOLE_TABLE (no HASH, no PARTITION).
        table_plans = plan_tables(
            manifest.tables,
            ConverterConfig(),
            dump_format=manifest.dump_format,
        )
        by_name = {tp.table: tp for tp in table_plans}
        assert by_name["EMPLOYEES"].strategy == TableStrategy.WHOLE_TABLE
        assert by_name["DEPARTMENTS"].strategy == TableStrategy.WHOLE_TABLE

        # Step 7: convert.
        plan = ConversionPlan(
            dump_paths=(dump_filename,),
            tables=table_plans,
            oracle_image=image,
        )
        state = StateStore(work_dir / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, parquet_dir, state)
        finally:
            state.close()

    # Step 8: verify Parquet output.
    emp_files = sorted((parquet_dir / "SRC" / "EMPLOYEES").glob("*.parquet"))
    dept_files = sorted((parquet_dir / "SRC" / "DEPARTMENTS").glob("*.parquet"))

    assert len(emp_files) == 1, f"Expected 1 parquet file for EMPLOYEES, got {len(emp_files)}"
    assert len(dept_files) == 1, f"Expected 1 parquet file for DEPARTMENTS, got {len(dept_files)}"

    assert count_parquet_rows(emp_files) == 10
    assert count_parquet_rows(dept_files) == 3
    assert result.rows == 13

    # Verify all chunks match imported vs output counts.
    assert all(
        chunk.imported_rows == chunk.output_rows
        for table_result in result.tables
        for chunk in table_result.chunks
    )

    # Spot-check employee IDs.
    emp_table = pq.read_table(emp_files[0], columns=["EMP_ID"])
    emp_ids = {int(v.as_py()) for v in emp_table.column("EMP_ID")}
    assert emp_ids == set(range(1, 11))


# ---------------------------------------------------------------------------
# Faster variant: use the pre-built sample dump from sample-data/legacy/
# ---------------------------------------------------------------------------

_SAMPLE_DUMP_DIR = Path(__file__).resolve().parent.parent.parent / "sample-data" / "legacy"
_SAMPLE_DUMP_FILE = _SAMPLE_DUMP_DIR / "legacy_exp_sample.dmp"

# Schema and row counts expected in the pre-built sample dump.
_SAMPLE_EXPECTED = {
    "APP": {"PRODUCTS": 20, "ORDERS": 50},
    "REPORTING": {"REPORTS": 10, "METRICS": 15},
}


def test_prebuilt_legacy_sample_dump(tmp_path: Path) -> None:
    """Convert the pre-built legacy exp sample dump to Parquet.

    This test is skipped when:
    * Docker is unavailable, or
    * ``sample-data/legacy/legacy_exp_sample.dmp`` does not exist.

    It does NOT re-run ``exp``; it just mounts the existing dump file and
    exercises the full detect → inspect → plan → convert pipeline.
    """
    if not _SAMPLE_DUMP_FILE.exists():
        pytest.skip(
            f"Pre-built legacy sample dump not found at {_SAMPLE_DUMP_FILE}; "
            "generate it with a legacy exp export and place it there."
        )

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    dump_filename = _SAMPLE_DUMP_FILE.name
    work_dir = tmp_path / "work"
    parquet_dir = tmp_path / "parquet"

    with DockerOracle.start(
        image=image,
        password=password,
        mounts=((_SAMPLE_DUMP_DIR, "/dumps", "rw"),),
    ) as container:
        container.wait_ready(timeout_seconds=120)

        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )

        # Create the Oracle DIRECTORY object pointing at /dumps.
        # Note: no explicit GRANT needed — system already has DBA privileges
        # and cannot grant to itself (ORA-01749).
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE OR REPLACE DIRECTORY DMP2PARQUET_DUMP AS '/dumps'")

        converter = OracleDumpConverter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=(dump_filename,),
            directory="DMP2PARQUET_DUMP",
            directory_path="/dumps",
        )

        manifest = converter.inspect_dump()

        assert manifest.dump_format == DumpFormat.LEGACY
        table_names = {t.name for t in manifest.tables}
        for schema_tables in _SAMPLE_EXPECTED.values():
            for expected_table in schema_tables:
                assert expected_table in table_names, (
                    f"{expected_table} not found in manifest; got {table_names}"
                )

        table_plans = plan_tables(
            manifest.tables,
            ConverterConfig(),
            dump_format=manifest.dump_format,
        )
        for tp in table_plans:
            assert tp.strategy == TableStrategy.WHOLE_TABLE, (
                f"{tp.table} has strategy {tp.strategy}, expected WHOLE_TABLE"
            )

        plan = ConversionPlan(
            dump_paths=(dump_filename,),
            tables=table_plans,
            oracle_image=image,
        )
        state = StateStore(work_dir / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, parquet_dir, state)
        finally:
            state.close()

    total_expected = sum(
        rows for schema_tables in _SAMPLE_EXPECTED.values() for rows in schema_tables.values()
    )
    assert result.rows == total_expected
    for schema_name, schema_tables in _SAMPLE_EXPECTED.items():
        for table_name, expected_rows in schema_tables.items():
            parquet_files = sorted((parquet_dir / schema_name / table_name).glob("*.parquet"))
            assert len(parquet_files) == 1, (
                f"Expected 1 parquet file for {schema_name}.{table_name}, got {len(parquet_files)}"
            )
            assert count_parquet_rows(parquet_files) == expected_rows
    assert all(
        chunk.imported_rows == chunk.output_rows
        for table_result in result.tables
        for chunk in table_result.chunks
    )
