from oracle_dmp_converter.datapump.sqlfile import parse_sqlfile_tables


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
