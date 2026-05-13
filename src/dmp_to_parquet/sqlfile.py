"""Parse table names from Data Pump SQLFILE output."""

from __future__ import annotations

import re

from dmp_to_parquet.metadata import ORACLE_MAINTAINED_SCHEMAS

_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+|PRIVATE\s+TEMPORARY\s+)?TABLE\s+"
    r'"(?P<schema>(?:[^"]|"")+)"\."(?P<table>(?:[^"]|"")+)"',
    re.IGNORECASE,
)


def _unescape_quoted_identifier(value: str) -> str:
    return value.replace('""', '"')


def parse_sqlfile_tables(sql_text: str) -> tuple[tuple[str, str], ...]:
    """Return schema/table pairs from Data Pump SQLFILE DDL."""

    tables: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in _CREATE_TABLE_RE.finditer(sql_text):
        schema = _unescape_quoted_identifier(match.group("schema"))
        table = _unescape_quoted_identifier(match.group("table"))
        if schema.upper() in ORACLE_MAINTAINED_SCHEMAS:
            continue
        key = (schema, table)
        if key not in seen:
            seen.add(key)
            tables.append(key)
    return tuple(tables)
