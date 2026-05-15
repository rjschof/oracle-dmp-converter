"""Streaming Oracle-to-Parquet export."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from dmp_to_parquet.config import ColumnOverride
from dmp_to_parquet.models import ColumnMetadata
from dmp_to_parquet.oracle.identifiers import oracle_identifier, oracle_qualified_name
from dmp_to_parquet.oracle.types import export_expression, parquet_type_name


@dataclass(frozen=True)
class ExportResult:
    path: Path
    rows: int


def arrow_type_for_column(
    column: ColumnMetadata,
    override: ColumnOverride | None = None,
) -> pa.DataType:
    type_name = parquet_type_name(column, override)
    if type_name == "int64":
        return pa.int64()
    if type_name == "double":
        return pa.float64()
    if type_name == "string":
        return pa.string()
    if type_name == "binary":
        return pa.binary()
    if type_name == "timestamp_us":
        return pa.timestamp("us")
    if type_name.startswith("decimal128(") and type_name.endswith(")"):
        precision, scale = type_name[len("decimal128(") : -1].split(",", 1)
        return pa.decimal128(int(precision), int(scale))
    return pa.string()


def arrow_schema_for_columns(
    columns: tuple[ColumnMetadata, ...],
    overrides: dict[str, ColumnOverride] | None = None,
) -> pa.Schema:
    overrides = overrides or {}
    fields = [
        pa.field(column.name, arrow_type_for_column(column, overrides.get(column.name)))
        for column in columns
    ]
    return pa.schema(fields)


def _read_lob(value: Any) -> Any:
    if hasattr(value, "read") and callable(value.read):
        return value.read()
    return value


def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    value = _read_lob(value)
    if value is None:
        return None
    if pa.types.is_string(arrow_type):
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)
    if pa.types.is_binary(arrow_type):
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_decimal(arrow_type):
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        scale = arrow_type.scale
        if scale >= 0:
            quantizer = Decimal(1).scaleb(-scale)
            return decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
        return decimal_value
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
    return value


def _rows_to_table(rows: list[tuple[Any, ...]], schema: pa.Schema) -> pa.Table:
    arrays = []
    for index, field in enumerate(schema):
        values = [_coerce_value(row[index], field.type) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def export_table_to_parquet(
    conn: oracledb.Connection,
    *,
    schema_name: str,
    table_name: str,
    columns: tuple[ColumnMetadata, ...],
    output_path: Path,
    column_overrides: dict[str, ColumnOverride] | None = None,
    batch_size: int = 10_000,
) -> ExportResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_overrides = column_overrides or {}
    arrow_schema = arrow_schema_for_columns(columns, column_overrides)
    select_list = ", ".join(
        f"{export_expression(column, column_overrides.get(column.name))} "
        f"AS {oracle_identifier(column.name)}"
        for column in columns
    )
    sql = f"SELECT {select_list} FROM {oracle_qualified_name(schema_name, table_name)}"

    total_rows = 0
    writer: pq.ParquetWriter | None = None
    try:
        with conn.cursor() as cursor:
            cursor.arraysize = batch_size
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                table = _rows_to_table(rows, arrow_schema)
                if writer is None:
                    writer = pq.ParquetWriter(  # type: ignore[no-untyped-call]
                        output_path,
                        arrow_schema,
                    )
                writer.write_table(table)  # type: ignore[no-untyped-call]
                total_rows += len(rows)
        if writer is None:
            empty_arrays = [pa.array([], type=field.type) for field in arrow_schema]
            pq.write_table(  # type: ignore[no-untyped-call]
                pa.Table.from_arrays(empty_arrays, schema=arrow_schema),
                output_path,
            )
    finally:
        if writer is not None:
            writer.close()  # type: ignore[no-untyped-call]
    return ExportResult(path=output_path, rows=total_rows)
