from oracle_dmp_converter.models import ColumnMetadata
from oracle_dmp_converter.oracle.types import export_expression, oracle_to_arrow_token


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


def test_unconstrained_number_maps_to_double() -> None:
    assert oracle_to_arrow_token(col("NUMBER")) == "double"


def test_xmltype_is_stringified() -> None:
    column = col("XMLTYPE")
    assert oracle_to_arrow_token(column) == "string"
    assert export_expression(column) == "TO_CHAR(C1)"
