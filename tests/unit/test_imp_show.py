"""Unit tests for imp INDEXFILE / SHOW=Y output parsing."""

from __future__ import annotations

from oracle_dmp_converter.datapump.legacy.imp_show import (
    _parse_show_output,
    parse_imp_indexfile_tables,
)

# ---------------------------------------------------------------------------
# Sample INDEXFILE output (as produced by imp FULL=Y INDEXFILE=...)
# ---------------------------------------------------------------------------

_INDEXFILE_QUOTED_SCHEMA = """\
REM  Exporting table EMPLOYEES
CREATE TABLE "HR"."EMPLOYEES" (
    "EMPLOYEE_ID" NUMBER(6, 0) NOT NULL ENABLE,
    "FIRST_NAME" VARCHAR2(20),
    "LAST_NAME" VARCHAR2(25) NOT NULL ENABLE,
    CONSTRAINT "EMP_PK" PRIMARY KEY ("EMPLOYEE_ID")
);

CREATE UNIQUE INDEX "HR"."EMP_EMAIL_UK" ON "HR"."EMPLOYEES" ("EMAIL");

REM  Exporting table DEPARTMENTS
CREATE TABLE "HR"."DEPARTMENTS" (
    "DEPARTMENT_ID" NUMBER(4, 0) NOT NULL ENABLE,
    "DEPARTMENT_NAME" VARCHAR2(30) NOT NULL ENABLE
);
"""

_INDEXFILE_UNQUOTED_SCHEMA = """\
CREATE TABLE SRC.ORDERS (
    ORDER_ID NUMBER NOT NULL,
    CUSTOMER_ID NUMBER
);
CREATE TABLE SRC.CUSTOMERS (
    CUSTOMER_ID NUMBER NOT NULL
);
"""

_INDEXFILE_MIXED = """\
CREATE TABLE "SRC"."PRODUCTS" (
    PRODUCT_ID NUMBER NOT NULL
);
CREATE TABLE SRC.LEGACY_TABLE (
    ID NUMBER
);
"""

_INDEXFILE_SYSTEM_FILTERED = """\
CREATE TABLE "SYS"."HIDDEN_TABLE" (
    ID NUMBER
);
CREATE TABLE "MYAPP"."VISIBLE_TABLE" (
    ID NUMBER
);
"""

_INDEXFILE_EMPTY = ""


# ---------------------------------------------------------------------------
# Sample SHOW=Y log output
# ---------------------------------------------------------------------------

_SHOW_OUTPUT = """\
Import: Release 19.0.0.0.0 - Production on Wed Jan 1 00:00:00 2025

Connected to: Oracle Database 19c Enterprise Edition

Export file created by EXPORT:V12.01.00 via conventional path

. importing SRC's objects into SRC
. . importing table                    "EMPLOYEES"         107 rows
. . importing table                   "DEPARTMENTS"          27 rows

. importing OPS's objects into OPS
. . importing table                    "ORDERS"           1024 rows
"""

_SHOW_OUTPUT_NO_SCHEMA_LINE = """\
. . importing table  "ORPHAN_TABLE"  5 rows
"""

_SHOW_OUTPUT_SYSTEM_SCHEMA = """\
. importing SYS's objects into SYS
. . importing table  "BOOTSTRAP$"   0 rows

. importing MYAPP's objects into MYAPP
. . importing table  "EVENTS"   42 rows
"""


# ---------------------------------------------------------------------------
# Tests for INDEXFILE parser
# ---------------------------------------------------------------------------


class TestParseImpIndexfileTables:
    def test_quoted_schema_qualified(self) -> None:
        result = parse_imp_indexfile_tables(_INDEXFILE_QUOTED_SCHEMA)
        assert set(result) == {("HR", "EMPLOYEES"), ("HR", "DEPARTMENTS")}

    def test_unquoted_schema_qualified(self) -> None:
        result = parse_imp_indexfile_tables(_INDEXFILE_UNQUOTED_SCHEMA)
        assert set(result) == {("SRC", "ORDERS"), ("SRC", "CUSTOMERS")}

    def test_mixed_formats_prefers_quoted(self) -> None:
        # Quoted match is found first; once found, unquoted pass is skipped.
        result = parse_imp_indexfile_tables(_INDEXFILE_MIXED)
        assert ("SRC", "PRODUCTS") in result
        # LEGACY_TABLE is unquoted but should not appear because
        # the quoted pass already yielded results.
        assert ("SRC", "LEGACY_TABLE") not in result

    def test_filters_oracle_system_schemas(self) -> None:
        result = parse_imp_indexfile_tables(_INDEXFILE_SYSTEM_FILTERED)
        names = {r[1] for r in result}
        assert "HIDDEN_TABLE" not in names
        assert "VISIBLE_TABLE" in names

    def test_empty_text_falls_back_to_show_parser(self) -> None:
        # Empty input — no tables found.
        result = parse_imp_indexfile_tables(_INDEXFILE_EMPTY)
        assert not result

    def test_deduplication(self) -> None:
        # Duplicate CREATE TABLE in the file.
        ddl = """\
CREATE TABLE "SRC"."ORDERS" (ID NUMBER);
CREATE TABLE "SRC"."ORDERS" (ID NUMBER);
"""
        result = parse_imp_indexfile_tables(ddl)
        assert result.count(("SRC", "ORDERS")) == 1

    def test_falls_back_to_show_output_when_no_create_table(self) -> None:
        # When the text contains no CREATE TABLE but does contain SHOW=Y output.
        result = parse_imp_indexfile_tables(_SHOW_OUTPUT)
        assert ("SRC", "EMPLOYEES") in result
        assert ("SRC", "DEPARTMENTS") in result
        assert ("OPS", "ORDERS") in result

    def test_preserves_insertion_order(self) -> None:
        result = parse_imp_indexfile_tables(_INDEXFILE_QUOTED_SCHEMA)
        # EMPLOYEES appears before DEPARTMENTS in the input.
        assert result[0] == ("HR", "EMPLOYEES")
        assert result[1] == ("HR", "DEPARTMENTS")

    def test_global_temporary_table(self) -> None:
        ddl = 'CREATE GLOBAL TEMPORARY TABLE "SRC"."TMP_WORK" (ID NUMBER);\n'
        result = parse_imp_indexfile_tables(ddl)
        assert ("SRC", "TMP_WORK") in result


# ---------------------------------------------------------------------------
# Tests for the SHOW=Y log parser
# ---------------------------------------------------------------------------


class TestParseShowOutput:
    def test_basic_show_output(self) -> None:
        result = _parse_show_output(_SHOW_OUTPUT)
        assert ("SRC", "EMPLOYEES") in result
        assert ("SRC", "DEPARTMENTS") in result
        assert ("OPS", "ORDERS") in result

    def test_filters_oracle_system_schemas(self) -> None:
        result = _parse_show_output(_SHOW_OUTPUT_SYSTEM_SCHEMA)
        tables = {r[1] for r in result}
        assert "BOOTSTRAP$" not in tables
        assert "EVENTS" in tables

    def test_table_without_schema_context_skipped(self) -> None:
        # No `. importing SCHEMA's objects` line; table line should be ignored.
        result = _parse_show_output(_SHOW_OUTPUT_NO_SCHEMA_LINE)
        assert not result

    def test_deduplication(self) -> None:
        log = """\
. importing SRC's objects into SRC
. . importing table  "ORDERS"  100 rows
. . importing table  "ORDERS"  100 rows
"""
        result = _parse_show_output(log)
        assert result.count(("SRC", "ORDERS")) == 1

    def test_schema_switches(self) -> None:
        log = """\
. importing SCHEMA1's objects into SCHEMA1
. . importing table  "TABLE_A"  10 rows

. importing SCHEMA2's objects into SCHEMA2
. . importing table  "TABLE_B"  20 rows
"""
        result = _parse_show_output(log)
        assert ("SCHEMA1", "TABLE_A") in result
        assert ("SCHEMA2", "TABLE_B") in result
        # TABLE_A should NOT be attributed to SCHEMA2
        assert ("SCHEMA2", "TABLE_A") not in result
