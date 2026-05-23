"""Unit tests for datapump/error_parser.py."""

from __future__ import annotations

from oracle_dmp_converter.datapump.error_parser import (
    extract_missing_tablespaces,
    parse_impdp_tablespace_failures,
    parse_legacy_tablespace_failures,
)

_IMPDP_LOG = """
ORA-39083: Object type TABLE:"HR"."EMPLOYEES" failed to create
ORA-00959: tablespace 'HR_DATA' does not exist
ORA-39083: Object type TABLE:"FINANCE"."ACCOUNTS" failed to create
ORA-00959: tablespace 'FIN_DATA' does not exist
ORA-00959: tablespace 'FIN_IDX' does not exist
""".strip()

_LEGACY_LOG = """
. about to import HR's tables ...
"CREATE TABLE "HR"."EMPLOYEES" (...)"
IMP-00003: ORACLE error 959 encountered
ORA-00959: tablespace 'HR_DATA' does not exist
"CREATE TABLE "FINANCE"."ACCOUNTS" (...)"
ORA-00959: tablespace 'FIN_DATA' does not exist
""".strip()


class TestParseImpdpTablespaceFailures:
    def test_groups_by_object(self) -> None:
        result = parse_impdp_tablespace_failures(_IMPDP_LOG)
        assert result["HR.EMPLOYEES"] == {"HR_DATA"}
        assert result["FINANCE.ACCOUNTS"] == {"FIN_DATA", "FIN_IDX"}

    def test_orphan_lines_bucketed_under_unknown(self) -> None:
        log = "ORA-00959: tablespace 'X' does not exist"
        result = parse_impdp_tablespace_failures(log)
        assert result == {"<unknown>": {"X"}}

    def test_empty_log_returns_empty(self) -> None:
        assert not parse_impdp_tablespace_failures("")


class TestParseLegacyTablespaceFailures:
    def test_groups_by_create_table(self) -> None:
        result = parse_legacy_tablespace_failures(_LEGACY_LOG)
        assert result["HR.EMPLOYEES"] == {"HR_DATA"}
        assert result["FINANCE.ACCOUNTS"] == {"FIN_DATA"}

    def test_orphan_lines_bucketed_under_unknown(self) -> None:
        log = "ORA-00959: tablespace 'X' does not exist"
        result = parse_legacy_tablespace_failures(log)
        assert result == {"<unknown>": {"X"}}

    def test_empty_log_returns_empty(self) -> None:
        assert not parse_legacy_tablespace_failures("")


class TestExtractMissingTablespaces:
    def test_modern_path(self) -> None:
        assert extract_missing_tablespaces(_IMPDP_LOG) == {"HR_DATA", "FIN_DATA", "FIN_IDX"}

    def test_legacy_path(self) -> None:
        assert extract_missing_tablespaces(_LEGACY_LOG, is_legacy=True) == {"HR_DATA", "FIN_DATA"}

    def test_empty_returns_empty_set(self) -> None:
        assert extract_missing_tablespaces("") == set()
