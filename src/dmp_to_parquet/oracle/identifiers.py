"""Oracle and filesystem identifier utilities."""

from __future__ import annotations

import re
from urllib.parse import quote

_SIMPLE_ORACLE_IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def quote_oracle_identifier(name: str) -> str:
    """Return a double-quoted Oracle identifier."""

    return '"' + name.replace('"', '""') + '"'


def oracle_identifier(name: str) -> str:
    """Return an Oracle identifier, quoted only when quoting is required."""

    if _SIMPLE_ORACLE_IDENTIFIER.fullmatch(name):
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
