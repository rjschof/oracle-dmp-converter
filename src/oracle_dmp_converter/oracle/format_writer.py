"""Pluggable output-format writers for Oracle row data."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class FormatWriter(ABC):
    """Write batches of Arrow tables to a single output file."""

    @abstractmethod
    def write_batch(self, table: pa.Table) -> None:
        """Append *table* to the output file."""

    @abstractmethod
    def write_empty(self, schema: pa.Schema) -> None:
        """Write an empty output file (zero rows) using *schema*."""

    @abstractmethod
    def close(self) -> None:
        """Flush and close the output file."""


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


class ParquetFormatWriter(FormatWriter):
    """Stream rows to a Parquet file via :class:`pyarrow.parquet.ParquetWriter`."""

    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self._path = path
        self._schema = schema
        self._writer: pq.ParquetWriter | None = None

    def write_batch(self, table: pa.Table) -> None:
        if self._writer is None:
            self._writer = pq.ParquetWriter(  # type: ignore[no-untyped-call]
                self._path, self._schema
            )
        self._writer.write_table(table)  # type: ignore[no-untyped-call]

    def write_empty(self, schema: pa.Schema) -> None:
        empty_arrays = [pa.array([], type=field.type) for field in schema]
        pq.write_table(  # type: ignore[no-untyped-call]
            pa.Table.from_arrays(empty_arrays, schema=schema),
            self._path,
        )

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()  # type: ignore[no-untyped-call]
            self._writer = None


# ---------------------------------------------------------------------------
# Avro
# ---------------------------------------------------------------------------


def _arrow_to_avro_type(arrow_type: pa.DataType) -> Any:
    """Map a PyArrow DataType to the corresponding Avro type descriptor."""
    if (
        pa.types.is_int8(arrow_type)
        or pa.types.is_int16(arrow_type)
        or pa.types.is_int32(arrow_type)
    ):
        return "int"
    if pa.types.is_int64(arrow_type):
        return "long"
    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return "float"
    if pa.types.is_float64(arrow_type):
        return "double"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "bytes"
    if pa.types.is_decimal(arrow_type):
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": arrow_type.precision,
            "scale": arrow_type.scale,
        }
    if pa.types.is_timestamp(arrow_type):
        # Avro timestamp-micros is a long (epoch microseconds, UTC)
        return {"type": "long", "logicalType": "timestamp-micros"}
    if pa.types.is_date(arrow_type):
        return {"type": "int", "logicalType": "date"}
    # string / large_string / anything else → string
    return "string"


def _arrow_schema_to_avro(schema: pa.Schema, record_name: str = "Row") -> dict[str, Any]:
    """Build a fastavro-compatible Avro schema from a PyArrow schema.

    All fields are emitted as a union of ``["null", <type>]`` with a
    ``null`` default, matching Oracle's general nullable nature.
    """
    fields = []
    for arrow_field in schema:
        avro_base = _arrow_to_avro_type(arrow_field.type)
        fields.append(
            {
                "name": arrow_field.name,
                "type": ["null", avro_base],
                "default": None,
            }
        )
    return {
        "type": "record",
        "name": record_name,
        "fields": fields,
    }


def _table_to_records(table: pa.Table, schema: pa.Schema) -> list[dict[str, Any]]:
    """Convert a PyArrow table to a list of row dicts suitable for fastavro."""
    from decimal import Decimal

    records: list[dict[str, Any]] = []
    column_names = [field.name for field in schema]
    pydict = table.to_pydict()
    n = table.num_rows
    for i in range(n):
        row: dict[str, Any] = {}
        for col_name, arrow_field in zip(column_names, schema, strict=True):
            value = pydict[col_name][i]
            arrow_type = arrow_field.type
            if value is None:
                row[col_name] = None
            elif pa.types.is_timestamp(arrow_type):
                # fastavro expects integer microseconds since epoch for timestamp-micros
                import datetime

                if isinstance(value, datetime.datetime):
                    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
                    aware = value.replace(tzinfo=datetime.UTC) if value.tzinfo is None else value
                    row[col_name] = int((aware - epoch).total_seconds() * 1_000_000)
                else:
                    row[col_name] = int(value)
            elif pa.types.is_decimal(arrow_type):
                # fastavro decimal-in-bytes expects a Python Decimal
                row[col_name] = value if isinstance(value, Decimal) else Decimal(str(value))
            else:
                row[col_name] = value
        records.append(row)
    return records


class AvroFormatWriter(FormatWriter):
    """Stream rows to an Avro container file via :mod:`fastavro`.

    The first batch creates the file (``wb`` mode, which writes the Avro
    container header).  Each subsequent batch is appended by reopening in
    ``a+b`` mode, which fastavro uses to detect and reuse the existing header.
    """

    def __init__(self, path: Path, schema: pa.Schema) -> None:
        import fastavro  # local import keeps it optional at module level

        self._path = path
        self._arrow_schema = schema
        self._avro_schema = fastavro.parse_schema(
            _arrow_schema_to_avro(schema, record_name=path.stem.replace("-", "_"))
        )
        self._fastavro = fastavro
        self._has_data = False

    def write_batch(self, table: pa.Table) -> None:
        records = _table_to_records(table, self._arrow_schema)
        if not self._has_data:
            with open(self._path, "wb") as fh:
                self._fastavro.writer(fh, self._avro_schema, records)
            self._has_data = True
        else:
            with open(self._path, "a+b") as fh:
                self._fastavro.writer(fh, self._avro_schema, records)

    def write_empty(self, schema: pa.Schema) -> None:
        with open(self._path, "wb") as fh:
            self._fastavro.writer(fh, self._avro_schema, [])

    def close(self) -> None:
        pass  # Each batch opens and closes its own file handle.


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class CsvFormatWriter(FormatWriter):
    """Stream rows to a CSV file using :func:`pyarrow.csv.write_csv`.

    The header is written once with the first batch; subsequent batches
    are appended without a header row.
    """

    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self._path = path
        self._schema = schema
        self._file: Any | None = None
        self._header_written = False

    def _ensure_open(self) -> Any:
        if self._file is None:
            self._file = open(self._path, "wb")  # noqa: WPS515 – deliberately kept open
        return self._file

    def write_batch(self, table: pa.Table) -> None:
        fh = self._ensure_open()
        write_options = pa_csv.WriteOptions(include_header=not self._header_written)
        pa_csv.write_csv(table, fh, write_options)
        self._header_written = True

    def write_empty(self, schema: pa.Schema) -> None:
        # Write just the header row for an empty table.
        empty_arrays = [pa.array([], type=field.type) for field in schema]
        empty_table = pa.Table.from_arrays(empty_arrays, schema=schema)
        with open(self._path, "wb") as fh:
            pa_csv.write_csv(empty_table, fh, pa_csv.WriteOptions(include_header=True))

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_writer(output_format: str, path: Path, schema: pa.Schema) -> FormatWriter:
    """Return a :class:`FormatWriter` for *output_format*.

    *output_format* must be one of ``"parquet"``, ``"avro"``, or ``"csv"``.
    """
    if output_format == "parquet":
        return ParquetFormatWriter(path, schema)
    if output_format == "avro":
        return AvroFormatWriter(path, schema)
    if output_format == "csv":
        return CsvFormatWriter(path, schema)
    raise ValueError(f"Unknown output format: {output_format!r}")
