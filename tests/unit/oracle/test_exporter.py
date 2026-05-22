"""Unit tests for oracle/exporter.py internals."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from oracle_dmp_converter.config import ColumnOverride
from oracle_dmp_converter.models import ColumnMetadata, OutputFormat
from oracle_dmp_converter.oracle.exporter import (
    _coerce_value,
    _rows_to_table,
    arrow_schema_for_columns,
    arrow_type_for_column,
    export_table,
)


def _col(
    data_type: str,
    precision: int | None = None,
    scale: int | None = None,
    *,
    name: str = "C",
) -> ColumnMetadata:
    return ColumnMetadata(
        name=name,
        data_type=data_type,
        ordinal=1,
        data_precision=precision,
        data_scale=scale,
    )


# ---------------------------------------------------------------------------
# arrow_type_for_column
# ---------------------------------------------------------------------------


class TestArrowTypeForColumn:
    def test_number_integer(self) -> None:
        assert arrow_type_for_column(_col("NUMBER", 10, 0)) == pa.int64()

    def test_number_decimal(self) -> None:
        assert arrow_type_for_column(_col("NUMBER", 20, 4)) == pa.decimal128(20, 4)

    def test_number_unconstrained(self) -> None:
        assert arrow_type_for_column(_col("NUMBER")) == pa.float64()

    def test_varchar2(self) -> None:
        assert arrow_type_for_column(_col("VARCHAR2")) == pa.string()

    def test_raw(self) -> None:
        assert arrow_type_for_column(_col("RAW")) == pa.binary()

    def test_date(self) -> None:
        assert arrow_type_for_column(_col("DATE")) == pa.timestamp("us")

    def test_timestamp(self) -> None:
        assert arrow_type_for_column(_col("TIMESTAMP")) == pa.timestamp("us")

    def test_override_takes_precedence(self) -> None:
        override = ColumnOverride(parquet_type="string")
        assert arrow_type_for_column(_col("NUMBER", 10, 0), override) == pa.string()


# ---------------------------------------------------------------------------
# arrow_schema_for_columns
# ---------------------------------------------------------------------------


def test_arrow_schema_for_columns_basic() -> None:
    columns = (
        ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
        ColumnMetadata("NAME", "VARCHAR2", 2),
    )
    schema = arrow_schema_for_columns(columns)
    assert schema.field("ID").type == pa.int64()
    assert schema.field("NAME").type == pa.string()


def test_arrow_schema_for_columns_with_override() -> None:
    columns = (ColumnMetadata("GEOM", "SDO_GEOMETRY", 1),)
    override = ColumnOverride(parquet_type="string")
    schema = arrow_schema_for_columns(columns, {"GEOM": override})
    assert schema.field("GEOM").type == pa.string()


# ---------------------------------------------------------------------------
# _coerce_value
# ---------------------------------------------------------------------------


class TestCoerceValue:
    def test_null_returns_none(self) -> None:
        assert _coerce_value(None, pa.string()) is None

    def test_bytes_to_string(self) -> None:
        assert _coerce_value(b"hello", pa.string()) == "hello"

    def test_non_string_to_string(self) -> None:
        assert _coerce_value(42, pa.string()) == "42"

    def test_string_to_binary(self) -> None:
        assert _coerce_value("hi", pa.binary()) == b"hi"

    def test_integer_coercion(self) -> None:
        assert _coerce_value(Decimal("7"), pa.int64()) == 7

    def test_float_coercion(self) -> None:
        assert _coerce_value(Decimal("3.14"), pa.float64()) == pytest.approx(3.14)

    def test_decimal_rescaling(self) -> None:
        assert _coerce_value(Decimal("25.9000000000"), pa.decimal128(12, 2)) == Decimal("25.90")

    def test_date_to_datetime(self) -> None:
        result = _coerce_value(date(2024, 1, 15), pa.timestamp("us"))
        assert result == datetime(2024, 1, 15, 0, 0, 0)

    def test_datetime_passthrough(self) -> None:
        dt = datetime(2024, 6, 1, 12, 0, 0)
        assert _coerce_value(dt, pa.timestamp("us")) == dt


# ---------------------------------------------------------------------------
# _rows_to_table
# ---------------------------------------------------------------------------


def test_rows_to_table_basic() -> None:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    rows = [(1, "Alice"), (2, "Bob")]
    table = _rows_to_table(rows, schema)
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("name").to_pylist() == ["Alice", "Bob"]


def test_rows_to_table_empty() -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    table = _rows_to_table([], schema)
    assert table.num_rows == 0


# ---------------------------------------------------------------------------
# export_table — SQL generation (partition_name)
# ---------------------------------------------------------------------------


def _make_mock_conn(executed_sqls: list[str]) -> MagicMock:
    """Return a mock oracledb.Connection that records executed SQL statements."""
    mock_cursor = MagicMock()
    mock_cursor.fetchmany.return_value = []  # empty table → no rows

    @contextmanager
    def _cursor_ctx():
        yield mock_cursor

    mock_conn = MagicMock()
    mock_conn.cursor = _cursor_ctx

    def _capture_execute(sql: str) -> None:
        executed_sqls.append(sql)

    mock_cursor.execute.side_effect = _capture_execute
    return mock_conn


def _simple_columns() -> tuple[ColumnMetadata, ...]:
    return (ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),)


def test_export_table_sql_no_partition(tmp_path: Path) -> None:
    """Without partition_name the FROM clause has no PARTITION token."""
    sqls: list[str] = []
    conn = _make_mock_conn(sqls)
    mock_writer = MagicMock()
    mock_writer.write_empty = MagicMock()
    mock_writer.close = MagicMock()

    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        export_table(
            conn,
            schema_name="DMP_FINANCE",
            table_name="TRANSACTIONS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
        )

    assert len(sqls) == 1
    assert "PARTITION" not in sqls[0].upper()
    assert "DMP_FINANCE" in sqls[0]
    assert "TRANSACTIONS" in sqls[0]


def test_export_table_sql_with_partition(tmp_path: Path) -> None:
    """With partition_name the FROM clause includes PARTITION (name)."""
    sqls: list[str] = []
    conn = _make_mock_conn(sqls)
    mock_writer = MagicMock()
    mock_writer.write_empty = MagicMock()
    mock_writer.close = MagicMock()

    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        export_table(
            conn,
            schema_name="DMP_FINANCE",
            table_name="TRANSACTIONS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
            partition_name="P_2024_Q1",
        )

    assert len(sqls) == 1
    sql_upper = sqls[0].upper()
    assert "PARTITION" in sql_upper
    assert "P_2024_Q1" in sqls[0]
    # Verify the PARTITION clause appears after the table reference
    from_idx = sql_upper.index("FROM")
    assert sql_upper.index("PARTITION") > from_idx
