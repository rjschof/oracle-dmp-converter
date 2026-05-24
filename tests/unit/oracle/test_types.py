from oracle_dmp_converter.config import ColumnOverride
from oracle_dmp_converter.models import ColumnMetadata
from oracle_dmp_converter.oracle.types import (
    UNSUPPORTED_COLUMN_TYPES,
    export_expression,
    oracle_to_arrow_token,
)


def col(data_type: str, precision: int | None = None, scale: int | None = None) -> ColumnMetadata:
    return ColumnMetadata(
        name="C1",
        data_type=data_type,
        ordinal=1,
        data_precision=precision,
        data_scale=scale,
    )


def test_number_integer_maps_to_int64() -> None:
    assert oracle_to_arrow_token(col("NUMBER", 10, 0)) == "int64"


def test_large_number_maps_to_decimal() -> None:
    assert oracle_to_arrow_token(col("NUMBER", 30, 4)) == "decimal128(30,4)"


def test_unconstrained_number_maps_to_max_precision_decimal() -> None:
    """``NUMBER`` (no precision/scale) must not silently truncate to double.

    Falling through to ``double`` loses precision for integers past 2^53.
    Map to the widest decimal128 Oracle supports so large values like
    ``NUMBER(*,0)`` columns round-trip with full fidelity.
    """
    assert oracle_to_arrow_token(col("NUMBER")) == "decimal128(38,0)"


def test_number_star_zero_maps_to_max_precision_decimal() -> None:
    """``NUMBER(*,0)`` reports ``data_precision=None`` and ``data_scale=0``."""
    assert oracle_to_arrow_token(col("NUMBER", None, 0)) == "decimal128(38,0)"


def test_unbounded_number_with_scale_keeps_scale() -> None:
    """``NUMBER`` columns with a declared scale but no precision keep the scale."""
    assert oracle_to_arrow_token(col("NUMBER", None, 4)) == "decimal128(38,4)"


def test_json_uses_json_serialize() -> None:
    column = col("JSON")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "JSON_SERIALIZE(C1 RETURNING CLOB)"


def test_sdo_geometry_uses_wkt_helper() -> None:
    column = col("SDO_GEOMETRY")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "SDO_UTIL.TO_WKTGEOMETRY(C1)"


def test_bfile_remains_unmapped_so_planner_marks_unsupported() -> None:
    """BFILE is intentionally NOT in STRINGIFIED — planner marks the table UNSUPPORTED."""
    assert "BFILE" in UNSUPPORTED_COLUMN_TYPES


def test_xmltype_is_stringified() -> None:
    column = col("XMLTYPE")
    assert oracle_to_arrow_token(column) == "string"
    # XMLSERIALIZE is required for binary-storage XMLTYPE in modern Oracle;
    # TO_CHAR raises ORA-00932 against the default binary representation.
    assert export_expression(column) == "XMLSERIALIZE(DOCUMENT C1 AS CLOB)"


def test_oversized_number_precision_falls_back_to_double() -> None:
    """A NUMBER whose precision exceeds Oracle's max (38) can't fit decimal128 → double."""
    assert oracle_to_arrow_token(col("NUMBER", 40, 2)) == "double"


def test_timestamp_with_time_zone_uses_to_char() -> None:
    """Stringified types without a type-specific template fall back to TO_CHAR()."""
    column = col("TIMESTAMP WITH TIME ZONE")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "TO_CHAR(C1)"


def test_float_type_maps_to_double() -> None:
    assert oracle_to_arrow_token(col("FLOAT", 126)) == "double"


def test_string_type_maps_to_string() -> None:
    assert oracle_to_arrow_token(col("VARCHAR2")) == "string"


def test_binary_type_maps_to_binary() -> None:
    assert oracle_to_arrow_token(col("RAW")) == "binary"


def test_timestamp_maps_to_timestamp_us() -> None:
    assert oracle_to_arrow_token(col("TIMESTAMP")) == "timestamp_us"


def test_unknown_type_defaults_to_string() -> None:
    assert oracle_to_arrow_token(col("CUSTOM_TYPE")) == "string"


def test_export_expression_with_override() -> None:
    override = ColumnOverride(expression="MY_FUNC({column})")
    result = export_expression(col("NUMBER"), override)
    assert result == "MY_FUNC(C1)"


def test_export_expression_plain_column() -> None:
    result = export_expression(col("VARCHAR2"))
    assert result == "C1"
