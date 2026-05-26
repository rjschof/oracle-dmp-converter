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


def test_oversized_number_precision_uses_decimal256() -> None:
    """A NUMBER whose precision exceeds 38 but fits 76 uses decimal256."""
    assert oracle_to_arrow_token(col("NUMBER", 40, 2)) == "decimal256(40,2)"


def test_oversized_number_precision_falls_back_to_double() -> None:
    """A NUMBER whose precision exceeds decimal256's max (76) → double."""
    assert oracle_to_arrow_token(col("NUMBER", 80, 2)) == "double"


def test_timestamp_with_time_zone_uses_explicit_format() -> None:
    """TSTZ pins an explicit ISO format with TZR so output is NLS-independent."""
    column = col("TIMESTAMP WITH TIME ZONE")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "TO_CHAR(C1, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9 TZR')"


def test_timestamp_with_local_time_zone_uses_explicit_format() -> None:
    """TSLTZ pins an explicit ISO format (no region) so output is NLS-independent."""
    column = col("TIMESTAMP WITH LOCAL TIME ZONE")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "TO_CHAR(C1, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')"


def test_interval_still_falls_back_to_bare_to_char() -> None:
    """Interval types have no explicit template and use bare TO_CHAR()."""
    column = col("INTERVAL DAY TO SECOND")
    assert export_expression(column) == "TO_CHAR(C1)"


# ---------------------------------------------------------------------------
# NUMBER scale normalisation — scales Oracle allows but Parquet/Avro reject
# ---------------------------------------------------------------------------


def test_negative_scale_small_maps_to_int64() -> None:
    """NUMBER(10,-2) is an integer multiple of 100; it fits in int64."""
    assert oracle_to_arrow_token(col("NUMBER", 10, -2)) == "int64"


def test_negative_scale_wide_maps_to_scale0_decimal() -> None:
    """NUMBER(20,-2) needs 22 digits; widen precision and pin scale 0."""
    assert oracle_to_arrow_token(col("NUMBER", 20, -2)) == "decimal128(22,0)"


def test_negative_scale_overflowing_128_uses_decimal256() -> None:
    """NUMBER(38,-10) needs 48 digits > decimal128 max (38) → decimal256."""
    assert oracle_to_arrow_token(col("NUMBER", 38, -10)) == "decimal256(48,0)"


def test_negative_scale_overflowing_256_falls_back_to_double() -> None:
    """NUMBER(70,-10) needs 80 digits > decimal256 max (76) → double."""
    assert oracle_to_arrow_token(col("NUMBER", 70, -10)) == "double"


def test_scale_greater_than_precision_widens_precision() -> None:
    """NUMBER(2,5) is a pure fraction; precision must be >= scale for decimal128."""
    assert oracle_to_arrow_token(col("NUMBER", 2, 5)) == "decimal128(5,5)"


def test_large_scale_greater_than_38_uses_decimal256() -> None:
    """NUMBER(2,40) needs 40-digit precision → decimal256."""
    assert oracle_to_arrow_token(col("NUMBER", 2, 40)) == "decimal256(40,40)"


def test_unbounded_number_with_huge_scale_clamps_scale() -> None:
    """NUMBER(*, 50) cannot exceed decimal128's 38-digit precision."""
    assert oracle_to_arrow_token(col("NUMBER", None, 50)) == "decimal128(38,38)"


def test_unbounded_number_negative_scale_stays_wide_integer() -> None:
    """NUMBER(*, -2) keeps full integer fidelity rather than dropping to double."""
    assert oracle_to_arrow_token(col("NUMBER", None, -2)) == "decimal128(38,0)"


def test_scale_equals_precision_is_valid() -> None:
    """NUMBER(2,2) (e.g. COMMISSION_PCT) is already valid: decimal128(2,2)."""
    assert oracle_to_arrow_token(col("NUMBER", 2, 2)) == "decimal128(2,2)"


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
