"""Integration tests for tests/data/legacy.dmp (legacy exp format).

The dump is produced by ``scripts/create_full_combined_dump.py``.  Legacy
``exp`` cannot serialise several audit-extension features (identity
columns, native JSON, virtual columns, RANGE-HASH composite partitions,
BFILE) so the dump is intentionally partial: tables with those features
are absent from the dump.  When the converter inspects + plans + converts
them, they appear in the conversion report's ``skipped`` list with a
"staging table absent" reason.  In addition, legacy exp lacks the column
infrastructure to round-trip object/VARRAY/nested-table types, so those
columns are also marked UNSUPPORTED by the planner.

Legacy exp format forces WHOLE_TABLE strategy for every successfully
exported table (no HASH or PARTITION).

A single Oracle container is shared across inspect, plan, convert, convert_avro,
and convert_csv via the ``shared_work`` module-scoped fixture.  The fixture runs
``inspect`` (which sets ``keep_alive=True`` internally) and ``plan`` once; all
convert tests reconnect to the same container through the ``session.json`` left
behind by inspect, and each passes ``--keep-alive`` so the container stays up
for the next test.  The fixture teardown calls ``cleanup_stale_session`` to stop
the container after the module finishes.

``test_legacy_convert_oneshot`` is intentionally standalone: it spins up its own
fresh container and tears it down in one invocation.

Each test exercises one CLI subcommand:

  * test_legacy_inspect          — ``inspect`` only
  * test_legacy_plan             — ``plan`` (inspect already done by fixture)
  * test_legacy_convert          — ``convert --plan`` (Parquet)
  * test_legacy_convert_oneshot  — ``convert`` in one-shot mode (no prior ``--plan``)
  * test_legacy_convert_avro     — ``convert --plan --format avro``
  * test_legacy_convert_csv      — ``convert --plan --format csv``
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from oracle_dmp_converter.cli import main
from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.models import DumpFormat, OutputFormat, TableStrategy
from oracle_dmp_converter.persistence.serialization import load_manifest, load_plan
from oracle_dmp_converter.persistence.validation import count_output_rows
from oracle_dmp_converter.runtime.session import cleanup_stale_session, session_path_for

pytestmark = pytest.mark.integration

# pytest fixtures intentionally reuse the fixture function name as the parameter name
# pylint: disable=redefined-outer-name

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_LEGACY_DUMP = _DATA_DIR / "legacy.dmp"


_EXPECTED_ROWS: dict[str, dict[str, int]] = {
    "AUDITLOG": {
        "CHANGE_LOG": 50,
    },
    "FINANCE": {
        "ACCOUNTS": 20,
        "CHECK_NOT_NULL": 5,
        "MV_ACCOUNT_SUMMARY": 20,
        "NUMERIC_EDGE": 3,
        "TRANSACTIONS": 100,
        "TRANSACTION_DETAILS": 80,
    },
    "HRDATA": {
        "DEPARTMENTS": 5,
        "DEPT_LOCATIONS": 4,
        "EMP_PREFERENCES": 30,
        "JOBS": 10,
        # Legacy exp mangles the mixed-case identifier so the staging
        # import succeeds but no data round-trips.
        "MixedCase_Table": 0,
    },
    "INVENTORY": {
        "PRODUCTS": 24,
        "STOCK_LEVELS": 45,
        "STORE_LOCATIONS": 3,
        "SUPPLIERS": 10,
        "SUPPLIER_NOTES": 15,
        "WAREHOUSES": 3,
    },
}
_TOTAL_ROWS = sum(n for tbl in _EXPECTED_ROWS.values() for n in tbl.values())

# Tables present in the legacy dump's manifest that don't make it to
# convert output, either because legacy exp produced incomplete
# metadata for them (IDENTITY / JSON columns) or because their column
# types are unexportable (BFILE, user-defined OBJECT / VARRAY).
_EXPECTED_SKIPPED: set[tuple[str, str]] = {
    ("AUDITLOG", "ATTACHMENTS"),
    ("FINANCE", "CUSTOMER_PROFILE"),
    ("HRDATA", "DEPT_LOCATION_PHONES"),
    ("HRDATA", "EMP_TAGS"),
    ("INVENTORY", "ORDERS"),
    ("INVENTORY", "ORDER_ITEMS"),
    ("INVENTORY", "PRODUCT_SPECS"),
}

_PASSWORD = "OraclePwd_123"


def _image() -> str:
    return os.environ.get("DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE)


# ---------------------------------------------------------------------------
# Module-scoped fixture: one container for inspect + plan + all convert tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def shared_work(tmp_path_factory):
    """Start one Oracle container, run inspect and plan, then yield shared paths.

    All convert tests reconnect to this container via ``session.json``.
    Teardown stops the container once the module finishes.
    """
    work_dir = tmp_path_factory.mktemp("legacy_shared")
    manifest_path = work_dir / "manifest.json"
    plan_path = work_dir / "plan.yaml"
    runner = CliRunner()

    # inspect: starts the container, writes session.json + manifest.json, keep_alive=True
    result = runner.invoke(
        main,
        [
            "inspect",
            "--dump",
            str(_LEGACY_DUMP),
            "--work-dir",
            str(work_dir),
            "--oracle-image",
            _image(),
            "--oracle-password",
            _PASSWORD,
        ],
    )
    if result.exit_code != 0:
        pytest.fail(f"shared_work fixture: inspect failed:\n{result.output}")

    # plan: offline — reads manifest.json, writes plan.yaml
    result = runner.invoke(
        main,
        [
            "plan",
            "--manifest",
            str(manifest_path),
        ],
    )
    if result.exit_code != 0:
        pytest.fail(f"shared_work fixture: plan failed:\n{result.output}")

    yield SimpleNamespace(work_dir=work_dir, manifest_path=manifest_path, plan_path=plan_path)

    # teardown: stop the container if still running
    sess_path = session_path_for(work_dir)
    if sess_path.exists():
        cleanup_stale_session(sess_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_state(work_dir: Path) -> None:
    """Delete the convert state.sqlite so each format gets a clean resumability slate."""
    state_file = work_dir / "convert" / "state.sqlite"
    if state_file.exists():
        state_file.unlink()


def _assert_output(output_dir: Path, output_format: OutputFormat, ext: str) -> None:
    """Assert all expected tables have output files with correct row counts."""
    for schema_name, schema_tables in _EXPECTED_ROWS.items():
        for table_name, expected_rows in schema_tables.items():
            files = sorted((output_dir / schema_name / table_name).glob(f"*.{ext}"))
            assert files, f"No {ext} files found for {schema_name}.{table_name}"
            actual_rows = count_output_rows(files, output_format)
            assert actual_rows == expected_rows, (
                f"{schema_name}.{table_name}: expected {expected_rows} rows, got {actual_rows}"
            )
    # Skipped tables must not have any parquet files left behind.
    for schema_name, table_name in _EXPECTED_SKIPPED:
        files = list((output_dir / schema_name / table_name).glob(f"*.{ext}"))
        assert not files, (
            f"Skipped table {schema_name}.{table_name} unexpectedly produced output files: {files}"
        )


def _assert_conversion_report(work_dir: Path, expected_total_rows: int) -> None:
    """Assert the conversion report carries the expected totals + skipped set."""
    yaml_report = work_dir / "conversion_report.yaml"
    json_report = work_dir / "conversion_report.json"
    assert yaml_report.exists(), "conversion_report.yaml was not written"
    assert json_report.exists(), "conversion_report.json was not written"
    report = json.loads(json_report.read_text())
    stats = report["statistics"]
    assert stats["total_output_rows"] == expected_total_rows, (
        f"Report total_output_rows: expected {expected_total_rows}, "
        f"got {stats['total_output_rows']}"
    )
    assert stats["tables_converted"] > 0, "Report shows no converted tables"
    actual_skipped = {(s["schema"], s["table"]) for s in report.get("skipped", [])}
    assert actual_skipped == _EXPECTED_SKIPPED, (
        f"Skipped-table set mismatch: expected {_EXPECTED_SKIPPED}, got {actual_skipped}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_legacy_inspect(shared_work: SimpleNamespace) -> None:
    """inspect subcommand: writes manifest.json with LEGACY format and all expected tables."""
    assert shared_work.manifest_path.exists(), "manifest.json was not created by inspect"

    manifest = load_manifest(shared_work.manifest_path)
    assert manifest.dump_format == DumpFormat.LEGACY, (
        f"Expected DumpFormat.LEGACY, got {manifest.dump_format}"
    )
    table_names = {t.name for t in manifest.tables}
    # Manifest should contain every table whose data we expect to
    # convert.  Tables in _EXPECTED_SKIPPED may or may not appear in the
    # manifest depending on whether legacy exp wrote any DDL for them.
    for schema_tables in _EXPECTED_ROWS.values():
        for table_name in schema_tables:
            assert table_name in table_names, (
                f"{table_name} not found in manifest tables; got {table_names}"
            )


def test_legacy_plan(shared_work: SimpleNamespace) -> None:
    """plan subcommand: writes plan.yaml assigning WHOLE_TABLE strategy to all tables."""
    assert shared_work.plan_path.exists(), "plan.yaml was not created by plan"

    plan = load_plan(shared_work.plan_path)
    assert plan.dump_format == DumpFormat.LEGACY, (
        f"Expected DumpFormat.LEGACY in plan, got {plan.dump_format}"
    )
    # Every supported table in a legacy dump uses WHOLE_TABLE — legacy
    # ``imp`` has no QUERY filter so per-partition imports aren't
    # possible.  Tables with unexportable column types (BFILE, object
    # types) may show up as UNSUPPORTED instead.
    for tp in plan.tables:
        assert tp.strategy in (TableStrategy.WHOLE_TABLE, TableStrategy.UNSUPPORTED), (
            f"{tp.table}: expected WHOLE_TABLE or UNSUPPORTED for legacy dump, got {tp.strategy}"
        )


def test_legacy_convert(shared_work: SimpleNamespace, tmp_path: Path) -> None:
    """convert subcommand: produces correct Parquet output and conversion report."""
    output_dir = tmp_path / "parquet"
    _reset_state(shared_work.work_dir)

    result = CliRunner().invoke(
        main,
        [
            "convert",
            "--plan",
            str(shared_work.plan_path),
            "--output",
            str(output_dir),
            "--keep-alive",
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert result.exit_code == 0, f"convert failed:\n{result.output}"

    _assert_output(output_dir, OutputFormat.PARQUET, "parquet")
    _assert_conversion_report(shared_work.work_dir, _TOTAL_ROWS)


def test_legacy_convert_oneshot(tmp_path: Path) -> None:
    """convert without --plan: inspects, plans, and converts in a single invocation."""
    work_dir = tmp_path / "work"
    output_dir = tmp_path / "parquet"

    result = CliRunner().invoke(
        main,
        [
            "convert",
            "--dump",
            str(_LEGACY_DUMP),
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

    _assert_output(output_dir, OutputFormat.PARQUET, "parquet")
    _assert_conversion_report(work_dir, _TOTAL_ROWS)


def test_legacy_convert_avro(shared_work: SimpleNamespace, tmp_path: Path) -> None:
    """convert with --plan --format avro: produces correct Avro output for all tables."""
    output_dir = tmp_path / "avro"
    _reset_state(shared_work.work_dir)

    result = CliRunner().invoke(
        main,
        [
            "convert",
            "--plan",
            str(shared_work.plan_path),
            "--output",
            str(output_dir),
            "--format",
            "avro",
            "--keep-alive",
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert result.exit_code == 0, f"convert (avro) failed:\n{result.output}"

    _assert_output(output_dir, OutputFormat.AVRO, "avro")
    _assert_conversion_report(shared_work.work_dir, _TOTAL_ROWS)


def test_legacy_convert_csv(shared_work: SimpleNamespace, tmp_path: Path) -> None:
    """convert with --plan --format csv: produces correct CSV output for all tables."""
    output_dir = tmp_path / "csv"
    _reset_state(shared_work.work_dir)

    result = CliRunner().invoke(
        main,
        [
            "convert",
            "--plan",
            str(shared_work.plan_path),
            "--output",
            str(output_dir),
            "--format",
            "csv",
            "--keep-alive",
            "--oracle-password",
            _PASSWORD,
        ],
    )
    assert result.exit_code == 0, f"convert (csv) failed:\n{result.output}"

    _assert_output(output_dir, OutputFormat.CSV, "csv")
    _assert_conversion_report(shared_work.work_dir, _TOTAL_ROWS)
