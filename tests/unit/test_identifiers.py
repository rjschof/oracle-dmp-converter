from dmp_to_parquet.identifiers import (
    filesystem_safe_identifier,
    oracle_identifier,
    oracle_qualified_name,
    quote_oracle_identifier,
)


def test_oracle_identifier_leaves_simple_uppercase_names_unquoted() -> None:
    assert oracle_identifier("CUSTOMER_ID") == "CUSTOMER_ID"


def test_oracle_identifier_quotes_mixed_case_names() -> None:
    assert oracle_identifier("Customer Id") == '"Customer Id"'


def test_quote_oracle_identifier_escapes_quotes() -> None:
    assert quote_oracle_identifier('A"B') == '"A""B"'


def test_oracle_qualified_name() -> None:
    assert oracle_qualified_name("HR", "EMPLOYEES") == "HR.EMPLOYEES"


def test_filesystem_safe_identifier_percent_encodes() -> None:
    assert filesystem_safe_identifier("A B/C") == "A%20B%2FC"
