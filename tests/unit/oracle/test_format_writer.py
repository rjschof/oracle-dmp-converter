"""Unit tests for the pluggable FormatWriter implementations."""

from __future__ import annotations

import datetime
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.oracle.format_writer import (
    AvroFormatWriter,
    CsvFormatWriter,
    ParquetFormatWriter,
    _arrow_to_avro_type,
    _table_to_records,
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


# ---------------------------------------------------------------------------
# _arrow_to_avro_type — direct unit tests for uncovered type branches
# ---------------------------------------------------------------------------


class TestArrowToAvroType:
    def test_int32_returns_int(self) -> None:
        assert _arrow_to_avro_type(pa.int32()) == "int"

    def test_int16_returns_int(self) -> None:
        assert _arrow_to_avro_type(pa.int16()) == "int"

    def test_float32_returns_float(self) -> None:
        assert _arrow_to_avro_type(pa.float32()) == "float"

    def test_bool_returns_boolean(self) -> None:
        assert _arrow_to_avro_type(pa.bool_()) == "boolean"

    def test_large_binary_returns_bytes(self) -> None:
        assert _arrow_to_avro_type(pa.large_binary()) == "bytes"

    def test_timestamp_returns_timestamp_micros(self) -> None:
        result = _arrow_to_avro_type(pa.timestamp("us"))
        assert result == {"type": "long", "logicalType": "timestamp-micros"}

    def test_date_returns_date_logical_type(self) -> None:
        result = _arrow_to_avro_type(pa.date32())
        assert result == {"type": "int", "logicalType": "date"}

    def test_string_falls_back_to_string(self) -> None:
        assert _arrow_to_avro_type(pa.string()) == "string"


# ---------------------------------------------------------------------------
# AvroFormatWriter — two-batch append path (lines 217-218)
# ---------------------------------------------------------------------------


class TestAvroTwoBatches:
    def test_write_two_batches_appends_records(
        self,
        tmp_path: Path,
        simple_arrow_schema: pa.Schema,
        make_arrow_table: Callable[[pa.Schema, list[tuple]], pa.Table],
    ) -> None:
        """Second batch uses the append (a+b) code path."""
        path = tmp_path / "two-batches.avro"
        writer = AvroFormatWriter(path, simple_arrow_schema)
        writer.write_batch(make_arrow_table(simple_arrow_schema, [(1, "A", 1.0)]))
        writer.write_batch(make_arrow_table(simple_arrow_schema, [(2, "B", 2.0)]))
        writer.close()

        with open(path, "rb") as fh:
            records = list(fastavro.reader(fh))
        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2


# ---------------------------------------------------------------------------
# _table_to_records — null and timestamp conversion paths
# ---------------------------------------------------------------------------


class TestTableToRecords:
    def test_null_value_produces_none_in_record(self) -> None:
        """Null values in any column type become None in the output dict."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        table = pa.Table.from_arrays(
            [
                pa.array([1, None], type=pa.int64()),
                pa.array(["A", None], type=pa.string()),
            ],
            schema=schema,
        )
        records = _table_to_records(table, schema)
        assert records[1]["id"] is None
        assert records[1]["name"] is None

    def test_datetime_in_timestamp_column_converted_to_microseconds(self) -> None:
        """datetime.datetime values are converted to epoch microseconds."""
        schema = pa.schema([pa.field("ts", pa.timestamp("us"))])
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        table = pa.Table.from_arrays(
            [pa.array([dt], type=pa.timestamp("us"))], schema=schema
        )
        records = _table_to_records(table, schema)
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
        aware = dt.replace(tzinfo=datetime.UTC)
        expected = int((aware - epoch).total_seconds() * 1_000_000)
        assert records[0]["ts"] == expected

    def test_raw_int_in_timestamp_column_cast_to_int(self) -> None:
        """Non-datetime integer values in timestamp columns are passed as int."""
        schema = pa.schema([pa.field("ts", pa.timestamp("us"))])
        mock_table = MagicMock()
        mock_table.num_rows = 1
        mock_table.to_pydict.return_value = {"ts": [1_000_000]}
        records = _table_to_records(mock_table, schema)
        assert records[0]["ts"] == 1_000_000
