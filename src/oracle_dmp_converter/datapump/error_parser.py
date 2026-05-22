"""Parsers that pull missing-tablespace info out of impdp / legacy imp output.

Both modern Data Pump (``impdp``) and legacy ``imp`` will, when a target
tablespace is absent, surface the failure as an ``ORA-00959`` line in the
combined stdout+stderr stream.  Modern impdp prefixes the failure with an
``ORA-39083`` "Object type ... failed to create" line that carries the
``"SCHEMA"."TABLE"`` identifier; legacy ``imp`` instead emits the
``CREATE TABLE "SCHEMA"."TABLE" ...`` statement followed by one or more
``IMP-00003: ORACLE error ... ORA-00959`` lines.

This module groups the missing tablespaces by source object so the executor's
chunk-recovery wrapper can report which tables required which tablespaces, and
exposes a single dispatcher (:func:`extract_missing_tablespaces`) that returns
the union as a flat set.
"""

from __future__ import annotations

import re

# Modern impdp: ``ORA-39083: Object type TABLE:"OWNER"."TABLE" failed to create``.
_IMPDP_OBJECT_HEADER_RE = re.compile(
    r'ORA-39083:\s+Object\s+type\s+\S+\s*:\s*"(?P<schema>[^"]+)"\."(?P<table>[^"]+)"',
    re.IGNORECASE,
)

# Generic ORA-00959 ``tablespace 'NAME' does not exist`` line shared by both formats.
_MISSING_TABLESPACE_RE = re.compile(
    r"ORA-00959:\s+tablespace\s+'([^']+)'\s+does\s+not\s+exist",
    re.IGNORECASE,
)

# Legacy imp prints the CREATE TABLE statement before its ORA-00959 lines.
_LEGACY_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+|PRIVATE\s+TEMPORARY\s+)?TABLE\s+"
    r'"(?P<schema>[^"]+)"\."(?P<table>[^"]+)"',
    re.IGNORECASE,
)


def parse_impdp_tablespace_failures(log_text: str) -> dict[str, set[str]]:
    """Group ``ORA-00959`` tablespaces by the impdp object that caused them.

    Walks *log_text* in order, tracking the most recent ``ORA-39083`` object
    header.  Every subsequent ``ORA-00959`` is attributed to that object until
    another header is encountered.  ``ORA-00959`` lines that appear before any
    header are bucketed under the literal key ``"<unknown>"`` so callers can
    still recover the tablespace names.

    Args:
        log_text: Combined stdout+stderr from an ``impdp`` invocation
            (typically the body of a
            :class:`~oracle_dmp_converter.errors.DataPumpError`).

    Returns:
        Mapping of ``"SCHEMA.TABLE"`` keys (or ``"<unknown>"``) to the set of
        uppercase tablespace names reported missing for that object.
    """
    failures: dict[str, set[str]] = {}
    current_key = "<unknown>"
    for line in log_text.splitlines():
        header = _IMPDP_OBJECT_HEADER_RE.search(line)
        if header:
            current_key = f"{header.group('schema')}.{header.group('table')}"
            continue
        match = _MISSING_TABLESPACE_RE.search(line)
        if match:
            failures.setdefault(current_key, set()).add(match.group(1).upper())
    return failures


def parse_legacy_tablespace_failures(log_text: str) -> dict[str, set[str]]:
    """Group ``ORA-00959`` tablespaces by the legacy ``imp`` CREATE TABLE that
    introduced them.

    Legacy ``imp`` prints each CREATE TABLE statement before the
    ``ORA-00959`` line(s) it triggers.  This parser tracks the most recent
    ``CREATE TABLE "OWNER"."TABLE"`` and attributes every following
    ``ORA-00959`` to it until the next CREATE TABLE.  ``ORA-00959`` lines
    that appear before any CREATE TABLE are bucketed under ``"<unknown>"``.

    Args:
        log_text: Combined stdout+stderr from a legacy ``imp`` invocation.

    Returns:
        Mapping of ``"SCHEMA.TABLE"`` keys (or ``"<unknown>"``) to the set of
        uppercase tablespace names reported missing for that table.
    """
    failures: dict[str, set[str]] = {}
    current_key = "<unknown>"
    for line in log_text.splitlines():
        create = _LEGACY_CREATE_TABLE_RE.search(line)
        if create:
            current_key = f"{create.group('schema')}.{create.group('table')}"
            continue
        match = _MISSING_TABLESPACE_RE.search(line)
        if match:
            failures.setdefault(current_key, set()).add(match.group(1).upper())
    return failures


def extract_missing_tablespaces(output: str, *, is_legacy: bool = False) -> set[str]:
    """Return the union of all missing tablespace names found in *output*.

    Dispatches to :func:`parse_legacy_tablespace_failures` when *is_legacy* is
    True, otherwise to :func:`parse_impdp_tablespace_failures`, then flattens
    all values into a single set.

    Args:
        output: Combined stdout+stderr from an impdp or imp invocation.
        is_legacy: ``True`` to use the legacy ``imp`` parser, ``False`` (the
            default) to use the modern impdp parser.

    Returns:
        Set of uppercase tablespace names reported missing.  Empty when no
        ``ORA-00959`` lines are present.
    """
    parser = parse_legacy_tablespace_failures if is_legacy else parse_impdp_tablespace_failures
    result: set[str] = set()
    for names in parser(output).values():
        result.update(names)
    return result
