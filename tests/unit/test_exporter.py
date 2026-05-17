from decimal import Decimal

import pyarrow as pa

from oracle_dmp_converter.oracle.exporter import _coerce_value


def test_decimal_coercion_rescales_to_arrow_scale() -> None:
    assert _coerce_value(Decimal("25.9000000000"), pa.decimal128(12, 2)) == Decimal("25.90")
