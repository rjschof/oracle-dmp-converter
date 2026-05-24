"""Unit tests for persistence/report.py."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from oracle_dmp_converter.core.results import (
    ChunkConversionResult,
    PlanConversionResult,
    TableConversionResult,
)
from oracle_dmp_converter.models import (
    ChunkPlan,
    ConversionPlan,
    DumpFormat,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.persistence.report import build_conversion_report, save_conversion_report

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _plan(tables: list[TablePlan]) -> ConversionPlan:
    return ConversionPlan(
        dump_paths=("/dumps/test.dmp",),
        tables=tuple(tables),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
        dump_format=DumpFormat.DATAPUMP,
    )


def _whole_table_plan(schema: str, table: str) -> TablePlan:
    return TablePlan(
        schema=schema,
        table=table,
        strategy=TableStrategy.WHOLE_TABLE,
        chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
    )


def _unsupported_plan(schema: str, table: str, reason: str = "unsupported type") -> TablePlan:
    return TablePlan(
        schema=schema,
        table=table,
        strategy=TableStrategy.UNSUPPORTED,
        chunks=(),
        reason=reason,
    )


def _result(tables: list[tuple[str, str, int]]) -> PlanConversionResult:
    tcr = tuple(
        TableConversionResult(
            source_schema=schema,
            table=table,
            chunks=(
                ChunkConversionResult(
                    name="whole",
                    imported_rows=rows,
                    output_rows=rows,
                    output_path=Path(f"/out/{schema}/{table}/whole.parquet"),
                ),
            ),
        )
        for schema, table, rows in tables
    )
    return PlanConversionResult(tables=tcr, started_at=_NOW, completed_at=_NOW)


# ---------------------------------------------------------------------------
# build_conversion_report
# ---------------------------------------------------------------------------


class TestBuildConversionReport:
    def test_successful_tables_only(self) -> None:
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 42)])
        report = build_conversion_report(plan, result, "parquet")
        assert report.statistics.total_output_rows == 42
        assert report.statistics.tables_converted == 1
        assert report.statistics.tables_skipped == 0
        assert report.statistics.tables_total == 1
        assert len(report.successful) == 1
        assert len(report.skipped) == 0

    def test_skipped_tables_only(self) -> None:
        plan = _plan([_unsupported_plan("S", "T", "no support")])
        result = PlanConversionResult(tables=(), started_at=_NOW, completed_at=_NOW)
        report = build_conversion_report(plan, result, "parquet")
        assert report.statistics.tables_converted == 0
        assert report.statistics.tables_skipped == 1
        assert report.statistics.total_output_rows == 0
        assert len(report.skipped) == 1
        assert report.skipped[0].reason == "no support"

    def test_mixed_tables(self) -> None:
        plan = _plan(
            [
                _whole_table_plan("S", "GOOD"),
                _unsupported_plan("S", "BAD"),
            ]
        )
        result = _result([("S", "GOOD", 10)])
        report = build_conversion_report(plan, result, "parquet")
        assert report.statistics.tables_converted == 1
        assert report.statistics.tables_skipped == 1
        assert report.statistics.tables_total == 2
        assert report.statistics.total_output_rows == 10

    def test_supported_table_without_result_is_skipped(self) -> None:
        """A supported plan table with no conversion result is recorded as skipped.

        Happens when the staging table was absent at convert time (e.g. a legacy
        exp dump with incomplete DDL) rather than crashing on a missing key.
        """
        plan = _plan([_whole_table_plan("S", "MISSING")])
        result = PlanConversionResult(tables=(), started_at=_NOW, completed_at=_NOW)
        report = build_conversion_report(plan, result, "parquet")
        assert report.statistics.tables_converted == 0
        assert report.statistics.tables_skipped == 1
        assert len(report.skipped) == 1
        assert "Staging table was absent" in report.skipped[0].reason

    def test_output_format_recorded(self) -> None:
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 5)])
        report = build_conversion_report(plan, result, "avro")
        assert report.output_format == "avro"

    def test_timestamps_recorded(self) -> None:
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 0)])
        report = build_conversion_report(plan, result, "parquet")
        assert "2024-01-01" in report.started_at
        assert "2024-01-01" in report.completed_at


# ---------------------------------------------------------------------------
# save_conversion_report
# ---------------------------------------------------------------------------


class TestSaveConversionReport:
    def test_writes_yaml_and_json(self, tmp_path: Path) -> None:
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 7)])
        report = build_conversion_report(plan, result, "parquet")
        save_conversion_report(tmp_path, report)

        yaml_file = tmp_path / "conversion_report.yaml"
        json_file = tmp_path / "conversion_report.json"
        assert yaml_file.exists()
        assert json_file.exists()

    def test_yaml_and_json_agree_on_total_rows(self, tmp_path: Path) -> None:
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 13)])
        report = build_conversion_report(plan, result, "parquet")
        save_conversion_report(tmp_path, report)

        loaded_json = json.loads((tmp_path / "conversion_report.json").read_text())
        loaded_yaml = yaml.safe_load((tmp_path / "conversion_report.yaml").read_text())

        assert loaded_json["statistics"]["total_output_rows"] == 13
        assert loaded_yaml["statistics"]["total_output_rows"] == 13

    def test_creates_work_dir_if_missing(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "nested" / "work"
        plan = _plan([_whole_table_plan("S", "T")])
        result = _result([("S", "T", 1)])
        report = build_conversion_report(plan, result, "csv")
        save_conversion_report(new_dir, report)
        assert (new_dir / "conversion_report.yaml").exists()
