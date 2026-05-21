"""Oracle connection and DDL helpers."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import oracledb

from oracle_dmp_converter.oracle.identifiers import oracle_identifier, oracle_qualified_name

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class OracleCredentials:
    """Connection credentials shared by all Oracle utilities (impdp, expdp, imp, exp).

    The ``userid`` property produces the ``user/password@service`` connection
    string accepted by both Data Pump and legacy exp/imp parameter files.
    """

    user: str
    password: str
    service: str = "FREEPDB1"

    @property
    def userid(self) -> str:
        return f"{self.user}/{self.password}@{self.service}"


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


def ensure_tablespace(
    conn: oracledb.Connection,
    tablespace: str,
    *,
    datafile_dir: str = "/opt/oracle/oradata/FREE/FREEPDB1",
) -> None:
    """Create *tablespace* if it does not already exist (ORA-01543 is ignored)."""
    datafile = f"{datafile_dir}/{tablespace.lower()}01.dbf"
    execute_ignore(
        conn,
        f"CREATE TABLESPACE {oracle_identifier(tablespace)}"
        f" DATAFILE '{datafile}' SIZE 10M AUTOEXTEND ON NEXT 10M",
        {1543},  # ORA-01543: tablespace already exists
    )
    conn.commit()


def grant_quota_unlimited(conn: oracledb.Connection, schema: str, tablespace: str) -> None:
    """Grant QUOTA UNLIMITED on *tablespace* to *schema*."""
    with conn.cursor() as cursor:
        cursor.execute(
            f"ALTER USER {oracle_identifier(schema)}"
            f" QUOTA UNLIMITED ON {oracle_identifier(tablespace)}"
        )
    conn.commit()


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
