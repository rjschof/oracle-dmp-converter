"""Unit tests for core/results.py."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from oracle_dmp_converter.core.results import (
    ChunkConversionResult,
    PlanConversionResult,
    TableConversionResult,
)

_NOW = datetime.now(UTC)


class TestChunkConversionResult:
    def test_fields(self) -> None:
        chunk = ChunkConversionResult(
            name="whole",
            imported_rows=10,
            output_rows=10,
            output_path=Path("/out/file.parquet"),
        )
        assert chunk.name == "whole"
        assert chunk.imported_rows == 10
        assert chunk.output_rows == 10
        assert chunk.output_path == Path("/out/file.parquet")


class TestTableConversionResult:
    def test_rows_sums_chunks(self) -> None:
        chunks = (
            ChunkConversionResult("c1", 5, 5, Path("/a")),
            ChunkConversionResult("c2", 3, 3, Path("/b")),
        )
        tcr = TableConversionResult(source_schema="S", table="T", chunks=chunks)
        assert tcr.rows == 8

    def test_rows_empty_chunks(self) -> None:
        tcr = TableConversionResult(source_schema="S", table="T")
        assert tcr.rows == 0


class TestPlanConversionResult:
    def test_rows_sums_tables(self) -> None:
        tables = (
            TableConversionResult(
                "S",
                "T1",
                (ChunkConversionResult("c1", 10, 10, Path("/a")),),
            ),
            TableConversionResult(
                "S",
                "T2",
                (ChunkConversionResult("c1", 5, 5, Path("/b")),),
            ),
        )
        result = PlanConversionResult(tables=tables, started_at=_NOW, completed_at=_NOW)
        assert result.rows == 15

    def test_rows_empty_tables(self) -> None:
        result = PlanConversionResult(tables=(), started_at=_NOW, completed_at=_NOW)
        assert result.rows == 0
