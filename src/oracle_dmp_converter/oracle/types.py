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
    column_ref = oracle_identifier(column.name)
    if override and override.expression:
        return override.expression.replace("{column}", column_ref)
    if column.normalized_type in STRINGIFIED_TYPES:
        return f"TO_CHAR({column_ref})"
    return column_ref
