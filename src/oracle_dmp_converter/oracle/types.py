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
    "JSON",
    "SDO_GEOMETRY",
    "TIMESTAMP WITH LOCAL TIME ZONE",
    "TIMESTAMP WITH TIME ZONE",
    "UROWID",
    "XMLTYPE",
}

# Types the converter cannot extract meaningful data for via a normal
# SELECT.  Tables containing these columns are marked UNSUPPORTED by the
# planner so they surface in the conversion report rather than producing
# garbage parquet.
UNSUPPORTED_COLUMN_TYPES = frozenset({"BFILE"})

# Heuristic detection: any normalized type the converter doesn't recognise
# AND that comes from a user-defined OBJECT / VARRAY / nested-table type
# is also UNSUPPORTED (e.g. ``ADDRESS_T``, ``TAG_LIST``).  Those are
# detected by ``planner.py`` via the column's ``data_type_owner`` field.

# Oracle's maximum NUMBER precision; used as a safe default when a column is
# declared as ``NUMBER`` (unbounded) or ``NUMBER(*,0)`` and the converter
# would otherwise silently drop to ``double`` (loses precision past 2^53).
_NUMBER_MAX_PRECISION = 38


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
        precision = column.data_precision
        scale = column.data_scale
        if scale == 0 and precision is not None and precision <= 18:
            return "int64"
        if precision is not None and precision <= _NUMBER_MAX_PRECISION:
            return f"decimal128({precision},{scale or 0})"
        # ``NUMBER`` (unbounded) and ``NUMBER(*,0)`` both report
        # ``data_precision IS NULL`` from ALL_TAB_COLUMNS.  Falling through
        # to ``double`` here silently loses precision for integers past
        # 2^53.  Default to the widest fixed-precision decimal Oracle
        # supports so values round-trip with full fidelity; the column
        # can still be overridden to ``double`` via per-column config.
        if precision is None:
            if scale is None or scale == 0:
                return f"decimal128({_NUMBER_MAX_PRECISION},0)"
            return f"decimal128({_NUMBER_MAX_PRECISION},{scale})"
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


_TYPE_SPECIFIC_EXPRESSIONS: dict[str, str] = {
    # XMLTYPE: TO_CHAR(xmltype) raises ORA-00932 against binary-storage
    # XMLTYPE columns in modern Oracle.  XMLSERIALIZE is the supported
    # conversion in 12c+ and works for both text- and binary-storage
    # XMLTYPE columns.
    "XMLTYPE": "XMLSERIALIZE(DOCUMENT {column} AS CLOB)",
    # SDO_GEOMETRY: TO_CHAR doesn't produce useful text for spatial
    # objects.  SDO_UTIL.TO_WKTGEOMETRY emits Well-Known Text which
    # round-trips through the converter as a string column.
    "SDO_GEOMETRY": "SDO_UTIL.TO_WKTGEOMETRY({column})",
    # Native JSON (Oracle 21c+): TO_CHAR works for VARCHAR2 IS JSON
    # storage but raises ORA-00932 against the binary-OSON storage used
    # by the native ``JSON`` type.  JSON_SERIALIZE renders the value to
    # text and works for both storages.
    "JSON": "JSON_SERIALIZE({column} RETURNING CLOB)",
}


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
    type_template = _TYPE_SPECIFIC_EXPRESSIONS.get(column.normalized_type)
    if type_template is not None:
        return type_template.replace("{column}", column_ref)
    if column.normalized_type in STRINGIFIED_TYPES:
        return f"TO_CHAR({column_ref})"
    return column_ref
