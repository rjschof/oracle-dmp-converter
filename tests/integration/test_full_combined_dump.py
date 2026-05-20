"""Integration test: pre-built full combined dump (modern + legacy).

Skipped automatically when ``sample-data/full-combined/`` does not contain the
expected dump files.  Generate them first with:

    uv run python scripts/create_full_combined_dump.py --force
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.docker_oracle import DockerOracle, docker_available
from oracle_dmp_converter.io.state import StateStore
from oracle_dmp_converter.io.validation import count_parquet_rows
from oracle_dmp_converter.models import ConversionPlan, DumpFormat, TableStrategy
from oracle_dmp_converter.oracle.conn import create_directory, oracle_connection
from oracle_dmp_converter.planner import plan_tables

pytestmark = pytest.mark.integration

_SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "sample-data" / "full-combined"
_MODERN_DUMP = _SAMPLE_DIR / "full_combined_modern.dmp"
_LEGACY_DUMP = _SAMPLE_DIR / "full_combined_legacy.dmp"

# Expected row counts per schema.table.
_EXPECTED_ROWS: dict[str, dict[str, int]] = {
    "HRDATA": {"DEPARTMENTS": 5, "JOBS": 10, "EMPLOYEES": 30},
    "INVENTORY": {"WAREHOUSES": 3, "PRODUCTS": 24, "STOCK_LEVELS": 45},
    "FINANCE": {"ACCOUNTS": 20, "TRANSACTIONS": 100},
    "AUDITLOG": {"CHANGE_LOG": 50},
}
_TOTAL_ROWS = sum(r for tbl in _EXPECTED_ROWS.values() for r in tbl.values())

# Tables that are partitioned → expect PARTITION strategy in modern dump.
_PARTITIONED = {"PRODUCTS", "TRANSACTIONS", "CHANGE_LOG"}


def _build_converter(
    *,
    container: DockerOracle,
    admin: OracleAdminConnection,
    work_dir: Path,
    dumpfiles: tuple[str, ...],
    dump_dir_path: str = "/dumps",
    dump_directory: str = "FULL_COMBINED_DUMP",
) -> OracleDumpConverter:
    return OracleDumpConverter(
        container=container,
        admin=admin,
        work_dir=work_dir,
        dumpfiles=dumpfiles,
        directory=dump_directory,
        directory_path=dump_dir_path,
    )


def test_prebuilt_modern_combined_dump(tmp_path: Path) -> None:
    """Convert the pre-built modern (expdp) combined dump to Parquet."""
    if not docker_available():
        pytest.skip("Docker is not available")
    if not _MODERN_DUMP.exists():
        pytest.skip(
            f"Pre-built modern dump not found at {_MODERN_DUMP}; "
            "run: uv run python scripts/create_full_combined_dump.py --force"
        )

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    parquet_dir = tmp_path / "parquet"

    with DockerOracle.start(
        image=image,
        password=password,
        mounts=((_SAMPLE_DIR, "/dumps", "rw"),),
    ) as container:
        container.wait_ready(timeout_seconds=300)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            create_directory(conn, "FULL_COMBINED_DUMP", "/dumps")

        converter = _build_converter(
            container=container,
            admin=admin,
            work_dir=tmp_path / "work",
            dumpfiles=(_MODERN_DUMP.name,),
        )
        manifest = converter.inspect_dump()

        assert manifest.dump_format == DumpFormat.DATAPUMP
        manifest_tables = {t.name for t in manifest.tables}
        for schema_tables in _EXPECTED_ROWS.values():
            for table_name in schema_tables:
                assert table_name in manifest_tables, (
                    f"{table_name} not found in manifest; got {manifest_tables}"
                )

        table_plans = plan_tables(
            manifest.tables, ConverterConfig(), dump_format=manifest.dump_format
        )
        by_name = {tp.table: tp for tp in table_plans}
        for table_name in _PARTITIONED:
            assert by_name[table_name].strategy == TableStrategy.PARTITION, (
                f"{table_name}: expected PARTITION, got {by_name[table_name].strategy}"
            )

        plan = ConversionPlan(
            dump_paths=(_MODERN_DUMP.name,),
            tables=table_plans,
            oracle_image=image,
        )
        state = StateStore(tmp_path / "work" / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, parquet_dir, state)
        finally:
            state.close()

    assert result.rows == _TOTAL_ROWS
    for schema_name, schema_tables in _EXPECTED_ROWS.items():
        for table_name, expected_rows in schema_tables.items():
            files = sorted((parquet_dir / schema_name / table_name).glob("*.parquet"))
            assert files, f"No parquet files for {schema_name}.{table_name}"
            assert count_parquet_rows(files) == expected_rows, (
                f"{schema_name}.{table_name}: expected {expected_rows} rows, "
                f"got {count_parquet_rows(files)}"
            )
    assert all(
        chunk.imported_rows == chunk.output_rows
        for table_result in result.tables
        for chunk in table_result.chunks
    )


def test_prebuilt_legacy_combined_dump(tmp_path: Path) -> None:
    """Convert the pre-built legacy (exp) combined dump to Parquet."""
    if not docker_available():
        pytest.skip("Docker is not available")
    if not _LEGACY_DUMP.exists():
        pytest.skip(
            f"Pre-built legacy dump not found at {_LEGACY_DUMP}; "
            "run: uv run python scripts/create_full_combined_dump.py --force"
        )

    image = os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)
    password = "OraclePwd_123"
    parquet_dir = tmp_path / "parquet"

    with DockerOracle.start(
        image=image,
        password=password,
        mounts=((_SAMPLE_DIR, "/dumps", "rw"),),
    ) as container:
        container.wait_ready(timeout_seconds=300)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=password,
        )
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            create_directory(conn, "FULL_COMBINED_DUMP", "/dumps")

        converter = _build_converter(
            container=container,
            admin=admin,
            work_dir=tmp_path / "work",
            dumpfiles=(_LEGACY_DUMP.name,),
        )
        manifest = converter.inspect_dump()

        assert manifest.dump_format == DumpFormat.LEGACY

        table_plans = plan_tables(
            manifest.tables, ConverterConfig(), dump_format=manifest.dump_format
        )
        # Legacy dumps must always produce WHOLE_TABLE plans.
        for tp in table_plans:
            assert tp.strategy == TableStrategy.WHOLE_TABLE, (
                f"{tp.table}: expected WHOLE_TABLE for legacy dump, got {tp.strategy}"
            )

        plan = ConversionPlan(
            dump_paths=(_LEGACY_DUMP.name,),
            tables=table_plans,
            oracle_image=image,
            dump_format=DumpFormat.LEGACY,
        )
        state = StateStore(tmp_path / "work" / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, parquet_dir, state)
        finally:
            state.close()

    assert result.rows == _TOTAL_ROWS
    for schema_name, schema_tables in _EXPECTED_ROWS.items():
        for table_name, expected_rows in schema_tables.items():
            files = sorted((parquet_dir / schema_name / table_name).glob("*.parquet"))
            assert files, f"No parquet files for {schema_name}.{table_name}"
            assert count_parquet_rows(files) == expected_rows, (
                f"{schema_name}.{table_name}: expected {expected_rows} rows, "
                f"got {count_parquet_rows(files)}"
            )
    assert all(
        chunk.imported_rows == chunk.output_rows
        for table_result in result.tables
        for chunk in table_result.chunks
    )
