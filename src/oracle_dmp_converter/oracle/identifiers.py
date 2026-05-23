"""Oracle and filesystem identifier utilities."""

from __future__ import annotations

import logging
import re
from urllib.parse import quote

LOGGER = logging.getLogger(__name__)

_SIMPLE_ORACLE_IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$#]*$")

# Oracle SQL reserved words.  A column / table name that *looks* like a
# simple identifier (all uppercase, alpha-numeric) still needs double-quoting
# if it matches one of these — otherwise emitting it bare in a SELECT list
# produces a parse error.  Source: Oracle SQL Language Reference, Appendix D
# "Oracle Reserved Words" (kept conservative — additions are cheap, omissions
# cause ORA-00936 / ORA-00904).
_ORACLE_RESERVED_WORDS = frozenset(
    {
        "ACCESS",
        "ADD",
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "AUDIT",
        "BETWEEN",
        "BY",
        "CHAR",
        "CHECK",
        "CLUSTER",
        "COLUMN",
        "COMMENT",
        "COMPRESS",
        "CONNECT",
        "CREATE",
        "CURRENT",
        "DATE",
        "DECIMAL",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXCLUSIVE",
        "EXISTS",
        "FILE",
        "FLOAT",
        "FOR",
        "FROM",
        "GRANT",
        "GROUP",
        "HAVING",
        "IDENTIFIED",
        "IMMEDIATE",
        "IN",
        "INCREMENT",
        "INDEX",
        "INITIAL",
        "INSERT",
        "INTEGER",
        "INTERSECT",
        "INTO",
        "IS",
        "LEVEL",
        "LIKE",
        "LOCK",
        "LONG",
        "MAXEXTENTS",
        "MINUS",
        "MLSLABEL",
        "MODE",
        "MODIFY",
        "NOAUDIT",
        "NOCOMPRESS",
        "NOT",
        "NOWAIT",
        "NULL",
        "NUMBER",
        "OF",
        "OFFLINE",
        "ON",
        "ONLINE",
        "OPTION",
        "OR",
        "ORDER",
        "PCTFREE",
        "PRIOR",
        "PUBLIC",
        "RAW",
        "RENAME",
        "RESOURCE",
        "REVOKE",
        "ROW",
        "ROWID",
        "ROWNUM",
        "ROWS",
        "SELECT",
        "SESSION",
        "SET",
        "SHARE",
        "SIZE",
        "SMALLINT",
        "START",
        "SUCCESSFUL",
        "SYNONYM",
        "SYSDATE",
        "TABLE",
        "THEN",
        "TO",
        "TRIGGER",
        "UID",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USER",
        "VALIDATE",
        "VALUES",
        "VARCHAR",
        "VARCHAR2",
        "VIEW",
        "WHENEVER",
        "WHERE",
        "WITH",
    }
)


def quote_oracle_identifier(name: str) -> str:
    """Return a double-quoted Oracle identifier."""

    return '"' + name.replace('"', '""') + '"'


def oracle_identifier(name: str) -> str:
    """Return an Oracle identifier, quoted only when quoting is required.

    Quoting is required when the name:
    - contains characters outside the simple-identifier set (lower-case
      letters, leading digit, spaces, punctuation), OR
    - matches an Oracle reserved word like ``SELECT`` / ``ORDER`` — these
      look like simple identifiers but are not valid as bare references.
    """

    if _SIMPLE_ORACLE_IDENTIFIER.fullmatch(name) and name not in _ORACLE_RESERVED_WORDS:
        return name
    return quote_oracle_identifier(name)


def oracle_qualified_name(schema: str, name: str) -> str:
    """Return a schema-qualified Oracle object reference."""

    return f"{oracle_identifier(schema)}.{oracle_identifier(name)}"


def filesystem_safe_identifier(name: str) -> str:
    """Return a reversible path segment for an Oracle identifier."""

    return quote(name, safe="")


def parse_qualified_table(value: str) -> tuple[str, str]:
    """Parse SCHEMA.TABLE from CLI input."""

    parts = value.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        msg = f"Expected SCHEMA.TABLE, got {value!r}"
        raise ValueError(msg)
    return parts[0], parts[1]
