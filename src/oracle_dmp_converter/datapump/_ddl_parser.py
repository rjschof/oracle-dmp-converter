"""Shared DDL parsing primitives for Data Pump and legacy imp output.

Both the modern Data Pump SQLFILE path and the legacy imp INDEXFILE path
produce CREATE TABLE DDL that must be parsed to discover schema/table names
and tablespace references.  This module holds the regexes and helpers shared
by both parsers so each pattern is defined exactly once.
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

# Matches a quoted tablespace name following the TABLESPACE keyword in DDL:
#   TABLESPACE "MY_TS"
_TABLESPACE_NAME_RE = re.compile(r'\bTABLESPACE\s+"([^"]+)"', re.IGNORECASE)

# Tablespaces that always exist in Oracle Free and never need pre-creation.
_SYSTEM_TABLESPACES: frozenset[str] = frozenset(
    {"SYSTEM", "SYSAUX", "USERS", "TEMP", "UNDOTBS1"}
)

# Matches the Oracle error that fires when a tablespace is referenced but missing:
#   ORA-00959: tablespace 'MY_TS' does not exist
_MISSING_TABLESPACE_RE = re.compile(
    r"ORA-00959:\s+tablespace\s+'([^']+)'\s+does\s+not\s+exist",
    re.IGNORECASE,
)


def unescape_quoted_identifier(value: str) -> str:
    """Remove Oracle double-quote escaping from a quoted identifier value.

    Oracle encodes a literal double-quote inside a quoted identifier as ``""``.
    For example, ``"FOO""BAR"`` refers to the identifier ``FOO"BAR``.
    """
    return value.replace('""', '"')


def parse_tablespace_names(sql_text: str) -> frozenset[str]:
    """Return non-system tablespace names referenced in DDL text.

    Scans for ``TABLESPACE "NAME"`` clauses and returns the set of names that
    are not in the built-in Oracle Free tablespace set.  Used by both the
    legacy INDEXFILE path and the modern SQLFILE path to discover tablespaces
    that must be pre-created in the staging instance.

    Args:
        sql_text: DDL text produced by ``imp INDEXFILE=`` or ``impdp SQLFILE=``.

    Returns:
        A :class:`frozenset` of uppercase tablespace names.
    """
    return frozenset(
        m.group(1).upper()
        for m in _TABLESPACE_NAME_RE.finditer(sql_text)
        if m.group(1).upper() not in _SYSTEM_TABLESPACES
    )


def parse_missing_tablespace_from_error(output: str) -> frozenset[str]:
    """Extract tablespace names from ``ORA-00959`` error messages.

    Parses lines of the form::

        ORA-00959: tablespace 'MY_TS' does not exist

    from impdp / imp output (typically captured as the body of a
    :class:`~oracle_dmp_converter.errors.DataPumpError`).

    Args:
        output: Combined stdout+stderr text from an impdp or imp invocation.

    Returns:
        A :class:`frozenset` of uppercase tablespace names that are reported
        missing.  Empty if no ``ORA-00959`` lines are found.
    """
    return frozenset(m.group(1).upper() for m in _MISSING_TABLESPACE_RE.finditer(output))
