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
# It is also the widest precision ``decimal128`` (and the Parquet/Avro decimal
# logical types) can represent.
_NUMBER_MAX_PRECISION = 38
# The widest precision ``decimal256`` can represent (76 digits).  Columns
# whose effective precision exceeds ``_NUMBER_MAX_PRECISION`` but fits within
# this limit use ``decimal256`` instead of falling back to lossy ``double``.
_NUMBER_MAX_PRECISION_256 = 76
# A scale-0 ``NUMBER`` with this many digits or fewer fits losslessly in int64
# (2^63-1 has 19 digits, so 18 is the safe limit).
_INT64_MAX_DIGITS = 18


def _number_token(precision: int | None, scale: int | None) -> str:
    """Map an Oracle ``NUMBER(precision, scale)`` to an Arrow type token.

    Oracle permits scales outside ``0 <= scale <= precision`` — negative
    scales (``NUMBER(10,-2)``, rounded to the hundred) and scales that
    exceed the precision (``NUMBER(2,5)``, a pure fraction).  The
    Parquet/Avro decimal logical types reject both, so the raw values must
    be normalised into a valid ``(arrow_precision, arrow_scale)`` pair
    before they reach a writer.

    The conversions preserve the exact value:

    * negative scale ``s`` → a scale-0 decimal wide enough for the implied
      trailing zeros (``arrow_precision = precision + |s|``);
    * scale ``s`` greater than ``precision`` → ``decimal128(s, s)`` (the
      value is a fraction, so ``s`` significant digits suffice).

    Precisions up to :data:`_NUMBER_MAX_PRECISION` (38) use ``decimal128``;
    precisions up to :data:`_NUMBER_MAX_PRECISION_256` (76) use ``decimal256``.
    Anything wider falls back to ``double``.
    """
    scale = scale or 0
    if precision is None:
        # Unbounded ``NUMBER`` / ``NUMBER(*,s)``: default to the widest fixed
        # decimal Oracle supports so large integers round-trip exactly.
        if scale <= 0:
            return f"decimal128({_NUMBER_MAX_PRECISION},0)"
        arrow_scale = min(scale, _NUMBER_MAX_PRECISION)
        return f"decimal128({_NUMBER_MAX_PRECISION},{arrow_scale})"
    if scale < 0:
        arrow_precision = precision - scale  # precision + |scale|
        if arrow_precision > _NUMBER_MAX_PRECISION_256:
            return "double"
        if arrow_precision <= _INT64_MAX_DIGITS:
            return "int64"
        decimal_width = "128" if arrow_precision <= _NUMBER_MAX_PRECISION else "256"
        return f"decimal{decimal_width}({arrow_precision},0)"
    if scale == 0:
        if precision <= _INT64_MAX_DIGITS:
            return "int64"
        decimal_width = "128" if precision <= _NUMBER_MAX_PRECISION else "256"
        return f"decimal{decimal_width}({precision},0)"
    arrow_precision = max(precision, scale)
    if arrow_precision > _NUMBER_MAX_PRECISION_256:
        return "double"
    decimal_width = "128" if arrow_precision <= _NUMBER_MAX_PRECISION else "256"
    return f"decimal{decimal_width}({arrow_precision},{scale})"


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
      scale *s* (precision ≤ 38).
    * ``"decimal256(p,s)"`` — wide fixed-precision decimal for precisions
      39–76.

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
        return _number_token(column.data_precision, column.data_scale)
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
    # Time-zone-aware timestamps: a bare ``TO_CHAR`` uses the session's
    # ``NLS_TIMESTAMP_TZ_FORMAT`` / ``NLS_TIMESTAMP_FORMAT``, so the output
    # text varies with the environment the staging container happens to
    # inherit.  Pin an explicit ISO-8601 format model (with full
    # fractional seconds and, for TSTZ, the region/offset) so the string
    # representation is deterministic across runs.
    "TIMESTAMP WITH TIME ZONE": ("TO_CHAR({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9 TZR')"),
    "TIMESTAMP WITH LOCAL TIME ZONE": ("TO_CHAR({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')"),
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
