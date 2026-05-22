"""Parse table names and tablespace references from Data Pump SQLFILE output."""

from __future__ import annotations

import logging

from oracle_dmp_converter.datapump._ddl_parser import (
    CREATE_TABLE_QUOTED_RE,
    parse_tablespace_names,
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


def parse_sqlfile_tablespaces(sql_text: str) -> frozenset[str]:
    """Return non-system tablespace names referenced in Data Pump SQLFILE DDL.

    Scans for ``TABLESPACE "NAME"`` clauses and returns names not in the
    built-in Oracle Free tablespace set.  Used by
    :class:`~oracle_dmp_converter.datapump.modern.workflow.DataPumpWorkflow`
    to discover tablespaces that must be pre-created before import begins.

    Args:
        sql_text: DDL text produced by ``impdp SQLFILE=``.

    Returns:
        A :class:`frozenset` of uppercase tablespace names.
    """
    return parse_tablespace_names(sql_text)
