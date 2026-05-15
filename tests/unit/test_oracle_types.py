from dmp_to_parquet.models import ColumnMetadata
from dmp_to_parquet.oracle.types import export_expression, parquet_type_name


def col(data_type: str, precision: int | None = None, scale: int | None = None) -> ColumnMetadata:
    return ColumnMetadata(
        name="C1",
        data_type=data_type,
        ordinal=1,
        data_precision=precision,
        data_scale=scale,
    )


def test_number_integer_maps_to_int64() -> None:
    assert parquet_type_name(col("NUMBER", 10, 0)) == "int64"


def test_large_number_maps_to_decimal() -> None:
    assert parquet_type_name(col("NUMBER", 30, 4)) == "decimal128(30,4)"


def test_unconstrained_number_maps_to_double() -> None:
    assert parquet_type_name(col("NUMBER")) == "double"


def test_xmltype_is_stringified() -> None:
    column = col("XMLTYPE")
    assert parquet_type_name(column) == "string"
    assert export_expression(column) == "TO_CHAR(C1)"
