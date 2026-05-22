"""Integration tests for tests/data/modern.dmp (Data Pump / expdp format).

The dump contains the full combined sample database:

    HRDATA:     DEPARTMENTS(5), JOBS(10), EMPLOYEES(30)
    INVENTORY:  WAREHOUSES(3), PRODUCTS(24), STOCK_LEVELS(45)
    FINANCE:    ACCOUNTS(20), TRANSACTIONS(100)
    AUDITLOG:   CHANGE_LOG(50)

Partitioned tables: PRODUCTS (LIST), TRANSACTIONS (RANGE), CHANGE_LOG (HASH).

Each test exercises one CLI subcommand in isolation:

  * test_modern_inspect          — ``inspect`` only
  * test_modern_plan             — ``inspect`` (prerequisite) then ``plan``
  * test_modern_convert          — ``inspect`` + ``plan`` (prerequisites) then ``convert``
  * test_modern_convert_oneshot  — ``convert`` in one-shot mode (no prior ``--plan``)
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from oracle_dmp_converter.cli import main
from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.models import DumpFormat, TableStrategy
from oracle_dmp_converter.persistence.serialization import load_manifest, load_plan
from oracle_dmp_converter.persistence.validation import count_parquet_rows

pytestmark = pytest.mark.integration

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_RUNS_DIR = Path(__file__).resolve().parent.parent / "runs"
_MODERN_DUMP = _DATA_DIR / "modern.dmp"


def _run_dir(name: str) -> Path:
    """Return a unique, empty directory under tests/runs/ for a test run."""
    path = _RUNS_DIR / f"{name}_{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True)
    return path


_EXPECTED_ROWS: dict[str, dict[str, int]] = {
    "HRDATA": {"DEPARTMENTS": 5, "JOBS": 10, "EMPLOYEES": 30},
    "INVENTORY": {"WAREHOUSES": 3, "PRODUCTS": 24, "STOCK_LEVELS": 45},
    "FINANCE": {"ACCOUNTS": 20, "TRANSACTIONS": 100},
    "AUDITLOG": {"CHANGE_LOG": 50},
}
_TOTAL_ROWS = sum(n for tbl in _EXPECTED_ROWS.values() for n in tbl.values())

# Tables that are partitioned in this dump — expect PARTITION strategy in the plan.
_PARTITIONED = {"PRODUCTS", "TRANSACTIONS", "CHANGE_LOG"}

_PASSWORD = "OraclePwd_123"


def _image() -> str:
    return os.environ.get("DMP_TO_PARQUET_ORACLE_IMAGE", DEFAULT_ORACLE_IMAGE)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _invoke_inspect(runner: CliRunner, work_dir: Path, manifest_path: Path) -> None:
    """Run the ``inspect`` subcommand and assert success."""
    result = runner.invoke(
        main,
        [
            "inspect",
            "--dump",
            str(_MODERN_DUMP),
            "--work-dir",
            str(work_dir),
            "--oracle-image",
            _image(),
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert result.exit_code == 0, f"inspect failed:\n{result.output}"
    assert manifest_path.exists(), "manifest.json was not created by inspect"


def _invoke_plan(runner: CliRunner, manifest_path: Path, plan_path: Path) -> None:
    """Run the ``plan`` subcommand and assert success."""
    result = runner.invoke(
        main,
        [
            "plan",
            "--manifest",
            str(manifest_path),
        ],
    )
    assert result.exit_code == 0, f"plan failed:\n{result.output}"
    assert plan_path.exists(), "plan.yaml was not created by plan"


def _assert_parquet_output(output_dir: Path) -> None:
    """Assert all expected tables have Parquet output with correct row counts."""
    for schema_name, schema_tables in _EXPECTED_ROWS.items():
        for table_name, expected_rows in schema_tables.items():
            files = sorted((output_dir / schema_name / table_name).glob("*.parquet"))
            assert files, f"No parquet files found for {schema_name}.{table_name}"
            actual_rows = count_parquet_rows(files)
            assert actual_rows == expected_rows, (
                f"{schema_name}.{table_name}: expected {expected_rows} rows, got {actual_rows}"
            )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_modern_inspect() -> None:
    """inspect subcommand: writes manifest.json with DATAPUMP format and all expected tables."""

    base = _run_dir("modern_inspect")
    work_dir = base / "work"
    manifest_path = work_dir / "manifest.json"
    _invoke_inspect(CliRunner(), work_dir, manifest_path)

    manifest = load_manifest(manifest_path)
    assert manifest.dump_format == DumpFormat.DATAPUMP, (
        f"Expected DumpFormat.DATAPUMP, got {manifest.dump_format}"
    )
    table_names = {t.name for t in manifest.tables}
    for schema_tables in _EXPECTED_ROWS.values():
        for table_name in schema_tables:
            assert table_name in table_names, (
                f"{table_name} not found in manifest tables; got {table_names}"
            )


def test_modern_plan() -> None:
    """plan subcommand: writes plan.yaml assigning PARTITION strategy to partitioned tables."""

    base = _run_dir("modern_plan")
    work_dir = base / "work"
    manifest_path = work_dir / "manifest.json"
    plan_path = work_dir / "plan.yaml"
    runner = CliRunner()

    _invoke_inspect(runner, work_dir, manifest_path)
    _invoke_plan(runner, manifest_path, plan_path)

    plan = load_plan(plan_path)
    assert plan.dump_format == DumpFormat.DATAPUMP, (
        f"Expected DumpFormat.DATAPUMP in plan, got {plan.dump_format}"
    )
    by_table = {tp.table: tp for tp in plan.tables}
    for table_name in _PARTITIONED:
        assert table_name in by_table, f"{table_name} not found in plan tables"
        assert by_table[table_name].strategy == TableStrategy.PARTITION, (
            f"{table_name}: expected PARTITION strategy, got {by_table[table_name].strategy}"
        )


def test_modern_convert() -> None:
    """convert subcommand: produces correct Parquet output for all tables."""

    base = _run_dir("modern_convert")
    work_dir = base / "work"
    manifest_path = work_dir / "manifest.json"
    plan_path = work_dir / "plan.yaml"
    output_dir = base / "parquet"
    runner = CliRunner()

    _invoke_inspect(runner, work_dir, manifest_path)
    _invoke_plan(runner, manifest_path, plan_path)

    convert_result = runner.invoke(
        main,
        [
            "convert",
            "--plan",
            str(plan_path),
            "--dump",
            str(_MODERN_DUMP),
            "--output",
            str(output_dir),
            "--work-dir",
            str(work_dir),
            "--oracle-image",
            _image(),
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert convert_result.exit_code == 0, f"convert failed:\n{convert_result.output}"

    _assert_parquet_output(output_dir)


def test_modern_convert_oneshot() -> None:
    """convert without --plan: inspects, plans, and converts in a single invocation."""

    base = _run_dir("modern_convert_oneshot")
    work_dir = base / "work"
    output_dir = base / "parquet"

    result = CliRunner().invoke(
        main,
        [
            "convert",
            "--dump",
            str(_MODERN_DUMP),
            "--output",
            str(output_dir),
            "--work-dir",
            str(work_dir),
            "--oracle-image",
            _image(),
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert result.exit_code == 0, f"convert (one-shot) failed:\n{result.output}"

    # One-shot mode must write intermediate artifacts into work-dir.
    assert (work_dir / "manifest.json").exists(), "one-shot convert did not write manifest.json"
    assert (work_dir / "plan.yaml").exists(), "one-shot convert did not write plan.yaml"

    _assert_parquet_output(output_dir)
