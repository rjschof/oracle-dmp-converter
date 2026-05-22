"""Unit tests for persistence/validation.py."""

from __future__ import annotations

import csv
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.models import OutputFormat
from oracle_dmp_converter.persistence.validation import count_output_rows, count_parquet_rows

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(path: Path, num_rows: int) -> None:
    table = pa.table({"id": list(range(num_rows))})
    pq.write_table(table, path)


def _write_avro(path: Path, num_rows: int) -> None:
    schema = fastavro.parse_schema(
        {"type": "record", "name": "R", "fields": [{"name": "id", "type": "int"}]}
    )
    with open(path, "wb") as fh:
        fastavro.writer(fh, schema, [{"id": i} for i in range(num_rows)])


def _write_csv(path: Path, num_rows: int) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id"])
        for i in range(num_rows):
            writer.writerow([i])


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


class TestCountParquet:
    def test_single_file(self, tmp_path: Path) -> None:
        p = tmp_path / "data.parquet"
        _write_parquet(p, 10)
        assert count_output_rows([p], OutputFormat.PARQUET) == 10

    def test_multiple_files(self, tmp_path: Path) -> None:
        p1 = tmp_path / "a.parquet"
        p2 = tmp_path / "b.parquet"
        _write_parquet(p1, 5)
        _write_parquet(p2, 7)
        assert count_output_rows([p1, p2], OutputFormat.PARQUET) == 12

    def test_empty_list(self) -> None:
        assert count_output_rows([], OutputFormat.PARQUET) == 0

    def test_alias_delegates_correctly(self, tmp_path: Path) -> None:
        p = tmp_path / "data.parquet"
        _write_parquet(p, 3)
        assert count_parquet_rows([p]) == 3


# ---------------------------------------------------------------------------
# Avro
# ---------------------------------------------------------------------------


class TestCountAvro:
    def test_single_file(self, tmp_path: Path) -> None:
        p = tmp_path / "data.avro"
        _write_avro(p, 8)
        assert count_output_rows([p], OutputFormat.AVRO) == 8

    def test_multiple_files(self, tmp_path: Path) -> None:
        p1 = tmp_path / "a.avro"
        p2 = tmp_path / "b.avro"
        _write_avro(p1, 3)
        _write_avro(p2, 4)
        assert count_output_rows([p1, p2], OutputFormat.AVRO) == 7

    def test_empty_list(self) -> None:
        assert count_output_rows([], OutputFormat.AVRO) == 0


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class TestCountCsv:
    def test_single_file(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, 5)
        assert count_output_rows([p], OutputFormat.CSV) == 5

    def test_multiple_files_accumulate(self, tmp_path: Path) -> None:
        p1 = tmp_path / "a.csv"
        p2 = tmp_path / "b.csv"
        _write_csv(p1, 3)
        _write_csv(p2, 4)
        assert count_output_rows([p1, p2], OutputFormat.CSV) == 7

    def test_empty_file_returns_zero(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.csv"
        p.write_text("")
        assert count_output_rows([p], OutputFormat.CSV) == 0

    def test_empty_list(self) -> None:
        assert count_output_rows([], OutputFormat.CSV) == 0


# ---------------------------------------------------------------------------
# Unsupported format
# ---------------------------------------------------------------------------


class TestUnsupportedFormat:
    def test_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unsupported output format"):
            # Pass an object that isn't one of the three handled formats
            count_output_rows([], "json")  # type: ignore[arg-type]
