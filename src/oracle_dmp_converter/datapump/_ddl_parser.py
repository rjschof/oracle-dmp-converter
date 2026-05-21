"""Shared DDL parsing primitives for Data Pump and legacy imp output.

Both the modern Data Pump SQLFILE path and the legacy imp INDEXFILE path
produce CREATE TABLE DDL that must be parsed to discover schema/table names.
This module holds the regex and helpers shared by both parsers so the pattern
is defined exactly once.
"""

from __future__ import annotations

import re

# Matches schema-qualified quoted identifiers in CREATE TABLE statements:
#   CREATE TABLE "SCHEMA"."TABLE" (...)
#   CREATE GLOBAL TEMPORARY TABLE "SCHEMA"."TABLE" (...)
#   CREATE PRIVATE TEMPORARY TABLE "SCHEMA"."TABLE" (...)
CREATE_TABLE_QUOTED_RE = re.compile(
    r"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+|PRIVATE\s+TEMPORARY\s+)?TABLE\s+"
    r'"(?P<schema>(?:[^"]|"")+)"\."(?P<table>(?:[^"]|"")+)"',
    re.IGNORECASE,
)


def unescape_quoted_identifier(value: str) -> str:
    """Remove Oracle double-quote escaping from a quoted identifier value.

    Oracle encodes a literal double-quote inside a quoted identifier as ``""``.
    For example, ``"FOO""BAR"`` refers to the identifier ``FOO"BAR``.
    """
    return value.replace('""', '"')

