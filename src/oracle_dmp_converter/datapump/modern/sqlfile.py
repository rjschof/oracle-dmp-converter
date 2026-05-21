"""Parse table names from Data Pump SQLFILE output."""

from __future__ import annotations

import logging

from oracle_dmp_converter.datapump._ddl_parser import (
    CREATE_TABLE_QUOTED_RE,
    unescape_quoted_identifier,
)
from oracle_dmp_converter.oracle.constants import ORACLE_MAINTAINED_SCHEMAS

LOGGER = logging.getLogger(__name__)


def parse_sqlfile_tables(sql_text: str) -> tuple[tuple[str, str], ...]:
    """Return schema/table pairs from Data Pump SQLFILE DDL."""
    tables: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in CREATE_TABLE_QUOTED_RE.finditer(sql_text):
        schema = unescape_quoted_identifier(match.group("schema"))
        table = unescape_quoted_identifier(match.group("table"))
        if schema.upper() in ORACLE_MAINTAINED_SCHEMAS:
            continue
        key = (schema, table)
        if key not in seen:
            seen.add(key)
            tables.append(key)
    return tuple(tables)
