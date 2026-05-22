"""Unit tests for datapump/_ddl_parser.py shared parsing primitives."""

from __future__ import annotations

from oracle_dmp_converter.datapump._ddl_parser import (
    parse_missing_tablespace_from_error,
    parse_tablespace_names,
)

# ---------------------------------------------------------------------------
# parse_tablespace_names
# ---------------------------------------------------------------------------


class TestParseTablespaceNames:
    def test_extracts_single_custom_tablespace(self) -> None:
        ddl = 'CREATE TABLE "S"."T" (ID NUMBER)\nTABLESPACE "MY_DATA" STORAGE (INITIAL 8192);\n'
        assert parse_tablespace_names(ddl) == frozenset({"MY_DATA"})

    def test_extracts_multiple_tablespaces(self) -> None:
        ddl = (
            'CREATE TABLE "A"."T1" (ID NUMBER) TABLESPACE "DATA_TS";\n'
            'CREATE TABLE "A"."T2" (ID NUMBER) TABLESPACE "IDX_TS";\n'
        )
        result = parse_tablespace_names(ddl)
        assert result == frozenset({"DATA_TS", "IDX_TS"})

    def test_deduplicates_same_tablespace(self) -> None:
        ddl = (
            'CREATE TABLE "A"."T1" (ID NUMBER) TABLESPACE "CUSTOM";\n'
            'CREATE TABLE "A"."T2" (ID NUMBER) TABLESPACE "CUSTOM";\n'
        )
        result = parse_tablespace_names(ddl)
        assert result == frozenset({"CUSTOM"})

    def test_filters_system_tablespaces(self) -> None:
        ddl = (
            'CREATE TABLE "A"."T" (ID NUMBER) TABLESPACE "SYSTEM";\n'
            'CREATE TABLE "A"."U" (ID NUMBER) TABLESPACE "SYSAUX";\n'
            'CREATE TABLE "A"."V" (ID NUMBER) TABLESPACE "USERS";\n'
            'CREATE TABLE "A"."W" (ID NUMBER) TABLESPACE "TEMP";\n'
            'CREATE TABLE "A"."X" (ID NUMBER) TABLESPACE "UNDOTBS1";\n'
        )
        assert parse_tablespace_names(ddl) == frozenset()

    def test_mixed_system_and_custom(self) -> None:
        ddl = (
            'CREATE TABLE "A"."T" (ID NUMBER) TABLESPACE "USERS";\n'
            'CREATE TABLE "A"."U" (ID NUMBER) TABLESPACE "APP_TS";\n'
        )
        result = parse_tablespace_names(ddl)
        assert result == frozenset({"APP_TS"})

    def test_returns_uppercase(self) -> None:
        ddl = 'CREATE TABLE "A"."T" (ID NUMBER) TABLESPACE "lowercase_ts";\n'
        result = parse_tablespace_names(ddl)
        assert "LOWERCASE_TS" in result

    def test_empty_input_returns_empty(self) -> None:
        assert parse_tablespace_names("") == frozenset()

    def test_no_tablespace_clause_returns_empty(self) -> None:
        ddl = 'CREATE TABLE "A"."T" (ID NUMBER);\n'
        assert parse_tablespace_names(ddl) == frozenset()


# ---------------------------------------------------------------------------
# parse_missing_tablespace_from_error
# ---------------------------------------------------------------------------


class TestParseMissingTablespaceFromError:
    def test_extracts_single_tablespace(self) -> None:
        output = "ORA-00959: tablespace 'CUSTOM_TS' does not exist\n"
        result = parse_missing_tablespace_from_error(output)
        assert result == frozenset({"CUSTOM_TS"})

    def test_extracts_multiple_tablespaces(self) -> None:
        output = (
            "ORA-00959: tablespace 'TS_ONE' does not exist\n"
            "ORA-00031: session marked for kill\n"
            "ORA-00959: tablespace 'TS_TWO' does not exist\n"
        )
        result = parse_missing_tablespace_from_error(output)
        assert result == frozenset({"TS_ONE", "TS_TWO"})

    def test_deduplicates_same_tablespace(self) -> None:
        output = (
            "ORA-00959: tablespace 'SAME_TS' does not exist\n"
            "ORA-00959: tablespace 'SAME_TS' does not exist\n"
        )
        result = parse_missing_tablespace_from_error(output)
        assert result == frozenset({"SAME_TS"})

    def test_returns_uppercase(self) -> None:
        output = "ORA-00959: tablespace 'mixed_case_ts' does not exist\n"
        result = parse_missing_tablespace_from_error(output)
        assert "MIXED_CASE_TS" in result

    def test_case_insensitive_match(self) -> None:
        output = "ora-00959: TABLESPACE 'MY_TS' does not exist\n"
        result = parse_missing_tablespace_from_error(output)
        assert "MY_TS" in result

    def test_no_ora_00959_returns_empty(self) -> None:
        output = (
            "IMP-00003: ORACLE error 942 encountered\nORA-00942: table or view does not exist\n"
        )
        assert parse_missing_tablespace_from_error(output) == frozenset()

    def test_empty_output_returns_empty(self) -> None:
        assert parse_missing_tablespace_from_error("") == frozenset()

    def test_impdp_style_output(self) -> None:
        output = (
            "Processing object type SCHEMA_EXPORT/TABLE/TABLE\n"
            "ORA-39083: Object type TABLE failed to create with error:\n"
            "ORA-00959: tablespace 'CUSTOM_DATA' does not exist\n"
            "Failing sql is:\n"
            'CREATE TABLE "SRC"."ORDERS" ...\n'
        )
        result = parse_missing_tablespace_from_error(output)
        assert result == frozenset({"CUSTOM_DATA"})
