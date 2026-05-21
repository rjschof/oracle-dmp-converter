"""Oracle-to-Arrow/Parquet type handling."""

from __future__ import annotations

import logging

from oracle_dmp_converter.config import ColumnOverride
from oracle_dmp_converter.models import ColumnMetadata
from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)

STRING_TYPES = {"CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG"}
BINARY_TYPES = {"RAW", "LONG RAW", "BLOB"}
FLOAT_TYPES = {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}
TIMESTAMP_TYPES = {"DATE", "TIMESTAMP"}
STRINGIFIED_TYPES = {
    "ANYDATA",
    "INTERVAL DAY TO SECOND",
    "INTERVAL YEAR TO MONTH",
    "SDO_GEOMETRY",
    "TIMESTAMP WITH LOCAL TIME ZONE",
    "TIMESTAMP WITH TIME ZONE",
    "UROWID",
    "XMLTYPE",
}


def oracle_to_arrow_token(column: ColumnMetadata, override: ColumnOverride | None = None) -> str:
    """Return the Arrow type token string for an Oracle column.

    The token is a short string used by
    :func:`~oracle_dmp_converter.oracle.exporter.arrow_type_for_column` to
    construct a concrete :class:`~pyarrow.DataType`.  Recognised tokens:

    * ``"int64"`` — 64-bit integer.
    * ``"double"`` — 64-bit floating point.
    * ``"string"`` — UTF-8 string.
    * ``"binary"`` — raw bytes.
    * ``"timestamp_us"`` — microsecond-resolution timestamp.
    * ``"decimal128(p,s)"`` — fixed-precision decimal with precision *p* and
      scale *s*.

    If *override* supplies a ``parquet_type``, it is returned verbatim so the
    caller can inject custom type mappings.

    Args:
        column: Column metadata from ``ALL_TAB_COLUMNS``.
        override: Optional per-column override; ``parquet_type`` takes
            unconditional priority when set.

    Returns:
        A type token string as described above.  Unknown Oracle types default
        to ``"string"`` with a warning log entry.
    """
    if override and override.parquet_type:
        return override.parquet_type
    data_type = column.normalized_type
    if data_type == "NUMBER":
        if (
            column.data_scale == 0
            and column.data_precision is not None
            and column.data_precision <= 18
        ):
            return "int64"
        if column.data_precision is not None and column.data_precision <= 38:
            return f"decimal128({column.data_precision},{column.data_scale or 0})"
        return "double"
    if data_type in FLOAT_TYPES:
        return "double"
    if data_type in STRING_TYPES or data_type in STRINGIFIED_TYPES:
        return "string"
    if data_type in BINARY_TYPES:
        return "binary"
    if data_type in TIMESTAMP_TYPES:
        return "timestamp_us"
    LOGGER.warning(
        "Unknown Oracle type %r for column %s; defaulting to string",
        data_type,
        column.name,
    )
    return "string"


def export_expression(column: ColumnMetadata, override: ColumnOverride | None = None) -> str:
    """Return the SQL expression used to SELECT *column* during export.

    For most columns this is just the quoted column identifier.  Exceptions:

    * If *override* provides an ``expression`` template, the ``{column}``
      placeholder is replaced with the quoted identifier and the result is
      returned — enabling user-defined transforms (e.g. geometry conversion).
    * Columns whose normalised type is in :data:`STRINGIFIED_TYPES` are
      wrapped in ``TO_CHAR(...)`` to produce a string representation that
      PyArrow can accept.

    Args:
        column: Column metadata describing the Oracle data type.
        override: Optional per-column override; ``expression`` takes
            unconditional priority when set.

    Returns:
        A SQL fragment suitable for inclusion in a ``SELECT`` list.
    """
    column_ref = oracle_identifier(column.name)
    if override and override.expression:
        return override.expression.replace("{column}", column_ref)
    if column.normalized_type in STRINGIFIED_TYPES:
        return f"TO_CHAR({column_ref})"
    return column_ref
