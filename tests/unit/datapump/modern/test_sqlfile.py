from oracle_dmp_converter.datapump.modern.sqlfile import (
    parse_sqlfile_tables,
    parse_sqlfile_tablespaces,
)


def test_parse_sqlfile_tables_extracts_user_tables() -> None:
    sql = """
CREATE TABLE "SRC"."EMP" ("ID" NUMBER);
CREATE TABLE "SYSTEM"."IGNORED" ("ID" NUMBER);
CREATE TABLE "Mixed Schema"."Odd Table" ("ID" NUMBER);
"""
    assert parse_sqlfile_tables(sql) == (("SRC", "EMP"), ("Mixed Schema", "Odd Table"))


def test_parse_sqlfile_tables_deduplicates() -> None:
    sql = """
CREATE TABLE "SRC"."EMP" ("ID" NUMBER);
CREATE TABLE "SRC"."EMP" ("ID" NUMBER);
"""
    assert parse_sqlfile_tables(sql) == (("SRC", "EMP"),)


# ---------------------------------------------------------------------------
# parse_sqlfile_tablespaces
# ---------------------------------------------------------------------------


class TestParseSqlfileTablespaces:
    def test_extracts_custom_tablespace(self) -> None:
        sql = 'CREATE TABLE "SRC"."T" (ID NUMBER) TABLESPACE "CUSTOM_TS";\n'
        assert parse_sqlfile_tablespaces(sql) == frozenset({"CUSTOM_TS"})

    def test_filters_system_tablespaces(self) -> None:
        sql = (
            'CREATE TABLE "SRC"."T" (ID NUMBER) TABLESPACE "USERS";\n'
            'CREATE TABLE "SRC"."U" (ID NUMBER) TABLESPACE "SYSTEM";\n'
        )
        assert parse_sqlfile_tablespaces(sql) == frozenset()

    def test_mixed_system_and_custom(self) -> None:
        sql = (
            'CREATE TABLE "SRC"."T" (ID NUMBER) TABLESPACE "USERS";\n'
            'CREATE TABLE "SRC"."U" (ID NUMBER) TABLESPACE "APP_DATA";\n'
        )
        assert parse_sqlfile_tablespaces(sql) == frozenset({"APP_DATA"})

    def test_empty_sql_returns_empty(self) -> None:
        assert parse_sqlfile_tablespaces("") == frozenset()
