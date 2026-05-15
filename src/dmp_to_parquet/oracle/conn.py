"""Oracle connection and DDL helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import oracledb

from dmp_to_parquet.oracle.identifiers import oracle_identifier, oracle_qualified_name


@contextmanager
def oracle_connection(
    *,
    host: str,
    port: int,
    service: str,
    user: str,
    password: str,
) -> Iterator[oracledb.Connection]:
    conn = oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service}")
    try:
        yield conn
    finally:
        conn.close()


def _oracle_error_code(exc: Exception) -> int | None:
    if not isinstance(exc, oracledb.DatabaseError):
        return None
    error = exc.args[0]
    return getattr(error, "code", None)


def execute_ignore(conn: oracledb.Connection, sql: str, ignored_codes: set[int]) -> None:
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    except Exception as exc:  # noqa: BLE001 - Oracle driver wraps errors by code.
        if _oracle_error_code(exc) not in ignored_codes:
            raise


def drop_schema(conn: oracledb.Connection, schema: str) -> None:
    execute_ignore(conn, f"DROP USER {oracle_identifier(schema)} CASCADE", {1918})


def ensure_schema(conn: oracledb.Connection, schema: str, password: str) -> None:
    ident = oracle_identifier(schema)
    quoted_password = '"' + password.replace('"', '""') + '"'
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE USER {ident} IDENTIFIED BY {quoted_password}")
    except Exception as exc:  # noqa: BLE001 - Oracle driver wraps errors by code.
        if _oracle_error_code(exc) != 1920:
            raise
        with conn.cursor() as cursor:
            cursor.execute(f"ALTER USER {ident} IDENTIFIED BY {quoted_password} ACCOUNT UNLOCK")

    with conn.cursor() as cursor:
        cursor.execute(f"GRANT CONNECT, RESOURCE TO {ident}")
        cursor.execute(f"ALTER USER {ident} QUOTA UNLIMITED ON USERS")
    conn.commit()


def create_directory(conn: oracledb.Connection, name: str, path: str) -> None:
    escaped_path = path.replace("'", "''")
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE OR REPLACE DIRECTORY {oracle_identifier(name)} AS '{escaped_path}'")
    conn.commit()


def drop_table(conn: oracledb.Connection, schema: str, table: str) -> None:
    execute_ignore(conn, f"DROP TABLE {oracle_qualified_name(schema, table)} PURGE", {942})


def count_rows(conn: oracledb.Connection, schema: str, table: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {oracle_qualified_name(schema, table)}")
        value = cursor.fetchone()[0]
    return int(value)


def table_exists(conn: oracledb.Connection, schema: str, table: str) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM ALL_TABLES
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            """,
            owner=schema,
            table_name=table,
        )
        return int(cursor.fetchone()[0]) > 0
