"""Unit tests for the pluggable FormatWriter implementations."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.oracle.format_writer import (
    AvroFormatWriter,
    CsvFormatWriter,
    ParquetFormatWriter,
    make_writer,
)

# ---------------------------------------------------------------------------
# ParquetFormatWriter
# ---------------------------------------------------------------------------


class TestParquetFormatWriter:
    def test_write_single_batch(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        path = tmp_path / "out.parquet"
        writer = ParquetFormatWriter(path, simple_arrow_schema)
        writer.write_batch(
            make_arrow_table(simple_arrow_schema, [(1, "Alice", 1.5), (2, "Bob", 2.5)])
        )
        writer.close()

        result = pq.read_table(path)
        assert result.num_rows == 2
        assert result.schema.names == ["id", "name", "amount"]

    def test_write_multiple_batches(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        path = tmp_path / "out.parquet"
        writer = ParquetFormatWriter(path, simple_arrow_schema)
        for i in range(3):
            writer.write_batch(make_arrow_table(simple_arrow_schema, [(i, f"row{i}", float(i))]))
        writer.close()

        assert pq.read_table(path).num_rows == 3

    def test_write_empty(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
    ) -> None:
        path = tmp_path / "out.parquet"
        writer = ParquetFormatWriter(path, simple_arrow_schema)
        writer.write_empty(simple_arrow_schema)
        writer.close()

        result = pq.read_table(path)
        assert result.num_rows == 0
        assert result.schema.names == ["id", "name", "amount"]


# ---------------------------------------------------------------------------
# AvroFormatWriter
# ---------------------------------------------------------------------------


class TestAvroFormatWriter:
    def test_write_single_batch(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        path = tmp_path / "out.avro"
        writer = AvroFormatWriter(path, simple_arrow_schema)
        writer.write_batch(
            make_arrow_table(simple_arrow_schema, [(1, "Alice", 1.5), (2, "Bob", 2.5)])
        )
        writer.close()

        with open(path, "rb") as fh:
            records = list(fastavro.reader(fh))
        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["name"] == "Bob"

    def test_write_empty(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
    ) -> None:
        path = tmp_path / "out.avro"
        writer = AvroFormatWriter(path, simple_arrow_schema)
        writer.write_empty(simple_arrow_schema)
        writer.close()

        with open(path, "rb") as fh:
            records = list(fastavro.reader(fh))
        assert not records

    def test_decimal_field(self, tmp_path: Path) -> None:
        schema = pa.schema([pa.field("price", pa.decimal128(10, 2))])
        path = tmp_path / "dec.avro"
        values = [Decimal("12.34"), Decimal("99.99")]
        table = pa.Table.from_arrays([pa.array(values, type=pa.decimal128(10, 2))], schema=schema)
        writer = AvroFormatWriter(path, schema)
        writer.write_batch(table)
        writer.close()

        with open(path, "rb") as fh:
            records = list(fastavro.reader(fh))
        assert len(records) == 2
        assert records[0]["price"] == Decimal("12.34")


# ---------------------------------------------------------------------------
# CsvFormatWriter
# ---------------------------------------------------------------------------


class TestCsvFormatWriter:
    def test_write_single_batch(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        path = tmp_path / "out.csv"
        writer = CsvFormatWriter(path, simple_arrow_schema)
        writer.write_batch(
            make_arrow_table(simple_arrow_schema, [(1, "Alice", 1.5), (2, "Bob", 2.5)])
        )
        writer.close()

        lines = path.read_text().splitlines()
        assert "id" in lines[0] and "name" in lines[0] and "amount" in lines[0]
        assert len(lines) == 3  # header + 2 data rows

    def test_write_multiple_batches_single_header(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        path = tmp_path / "out.csv"
        writer = CsvFormatWriter(path, simple_arrow_schema)
        writer.write_batch(make_arrow_table(simple_arrow_schema, [(1, "A", 1.0)]))
        writer.write_batch(make_arrow_table(simple_arrow_schema, [(2, "B", 2.0)]))
        writer.write_batch(make_arrow_table(simple_arrow_schema, [(3, "C", 3.0)]))
        writer.close()

        lines = path.read_text().splitlines()
        header_count = sum(1 for ln in lines if "id" in ln and "name" in ln)
        assert header_count == 1
        assert len(lines) == 4  # header + 3 data rows

    def test_write_empty(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
    ) -> None:
        path = tmp_path / "out.csv"
        writer = CsvFormatWriter(path, simple_arrow_schema)
        writer.write_empty(simple_arrow_schema)
        writer.close()

        lines = path.read_text().splitlines()
        assert len(lines) == 1
        assert "id" in lines[0] and "name" in lines[0] and "amount" in lines[0]


# ---------------------------------------------------------------------------
# make_writer factory
# ---------------------------------------------------------------------------


def test_make_writer_returns_correct_types(tmp_path: Path, simple_arrow_schema: pa.Schema) -> None:
    assert isinstance(
        make_writer("parquet", tmp_path / "x.parquet", simple_arrow_schema), ParquetFormatWriter
    )
    assert isinstance(
        make_writer("avro", tmp_path / "x.avro", simple_arrow_schema), AvroFormatWriter
    )
    assert isinstance(make_writer("csv", tmp_path / "x.csv", simple_arrow_schema), CsvFormatWriter)


def test_make_writer_raises_on_unknown_format(
    tmp_path: Path, simple_arrow_schema: pa.Schema
) -> None:
    with pytest.raises(ValueError, match="Unknown output format"):
        make_writer("orc", tmp_path / "x.orc", simple_arrow_schema)
