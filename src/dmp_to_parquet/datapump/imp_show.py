"""Parse table names from legacy ``imp`` output.

Two output formats are handled:

1. **INDEXFILE output** – produced by ``imp INDEXFILE=/path/file.sql``.
   The file contains ``CREATE TABLE`` DDL, which may be schema-qualified
   (``"SCHEMA"."TABLE"``) when a full import is performed.  The regex is
   intentionally identical to the one in ``sqlfile.py`` so that both
   Data Pump and legacy paths share the same parser where possible.

2. **SHOW=Y log output** – produced by running ``imp SHOW=Y``.  The log
   contains lines like::

       . importing SRC's objects into SRC
       . . importing table          "EMPLOYEES"        107 rows

   This is used as a fallback when the INDEXFILE does not yield
   schema-qualified ``CREATE TABLE`` statements (which can happen with
   older export files or single-schema exports).
"""

from __future__ import annotations

import logging
import re

from dmp_to_parquet.oracle.metadata import ORACLE_MAINTAINED_SCHEMAS

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# INDEXFILE parser (primary)
# ---------------------------------------------------------------------------

# Matches both quoted-and-schema-qualified and quoted-but-not-qualified forms:
#   CREATE TABLE "SCHEMA"."TABLE" (...)
#   CREATE TABLE "TABLE" (...)  <- no schema prefix
_CREATE_TABLE_QUALIFIED_RE = re.compile(
    r"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+|PRIVATE\s+TEMPORARY\s+)?TABLE\s+"
    r'"(?P<schema>(?:[^"]|"")+)"\."(?P<table>(?:[^"]|"")+)"',
    re.IGNORECASE,
)

# Also match unquoted schema.table in case the INDEXFILE omits quotes.
_CREATE_TABLE_UNQUOTED_RE = re.compile(
    r"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+|PRIVATE\s+TEMPORARY\s+)?TABLE\s+"
    r"(?P<schema>[A-Z_$#][A-Z0-9_$#]*)\.(?P<table>[A-Z_$#][A-Z0-9_$#]*)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# SHOW=Y log parser (fallback)
# ---------------------------------------------------------------------------

# Matches: ". importing SCHEMANAME's objects into ..."
_IMPORTING_SCHEMA_RE = re.compile(
    r"^\.\s+importing\s+(?P<schema>\S+)'s\s+objects\s+into",
    re.IGNORECASE,
)

# Matches: ". . importing table   "TABLENAME"   N rows"
# Also handles optional row count at the end.
_IMPORTING_TABLE_RE = re.compile(
    r'^\.\s+\.\s+importing\s+table\s+"(?P<table>[^"]+)"',
    re.IGNORECASE,
)


def _unescape_quoted_identifier(value: str) -> str:
    return value.replace('""', '"')


def parse_imp_indexfile_tables(sql_text: str) -> tuple[tuple[str, str], ...]:
    """Return ``(schema, table)`` pairs from ``imp INDEXFILE=`` DDL output.

    Tries schema-qualified patterns first (both quoted and unquoted).
    Falls through to the SHOW=Y log parser if nothing is found that way,
    which allows the same function to handle log output captured from
    ``imp SHOW=Y``.
    """
    tables: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    # --- attempt 1: quoted "SCHEMA"."TABLE" (same format as Data Pump SQLFILE)
    for match in _CREATE_TABLE_QUALIFIED_RE.finditer(sql_text):
        schema = _unescape_quoted_identifier(match.group("schema"))
        table = _unescape_quoted_identifier(match.group("table"))
        if schema.upper() in ORACLE_MAINTAINED_SCHEMAS:
            continue
        key = (schema, table)
        if key not in seen:
            seen.add(key)
            tables.append(key)

    if tables:
        return tuple(tables)

    # --- attempt 2: unquoted SCHEMA.TABLE
    for match in _CREATE_TABLE_UNQUOTED_RE.finditer(sql_text):
        schema = match.group("schema").upper()
        table = match.group("table").upper()
        if schema in ORACLE_MAINTAINED_SCHEMAS:
            continue
        key = (schema, table)
        if key not in seen:
            seen.add(key)
            tables.append(key)

    if tables:
        return tuple(tables)

    # --- attempt 3: fall back to SHOW=Y log format (stateful)
    return _parse_show_output(sql_text)


def _parse_show_output(log_text: str) -> tuple[tuple[str, str], ...]:
    """Parse schema/table pairs from ``imp SHOW=Y`` log output."""
    tables: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    current_schema: str | None = None

    for line in log_text.splitlines():
        schema_match = _IMPORTING_SCHEMA_RE.match(line.strip())
        if schema_match:
            current_schema = schema_match.group("schema")
            continue

        table_match = _IMPORTING_TABLE_RE.match(line.strip())
        if table_match and current_schema is not None:
            schema = current_schema
            table = table_match.group("table")
            if schema.upper() in ORACLE_MAINTAINED_SCHEMAS:
                continue
            key = (schema, table)
            if key not in seen:
                seen.add(key)
                tables.append(key)

    return tuple(tables)
