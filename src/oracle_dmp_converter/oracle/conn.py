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
        """Return the ``user/password@service`` connection string.

        This format is accepted by both Data Pump (``expdp``/``impdp``) and
        legacy (``exp``/``imp``) parfiles as the ``USERID`` parameter.

        Returns:
            Connection string of the form ``"user/password@service"``.
        """
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
    """Context manager that yields a connected :class:`oracledb.Connection`.

    Closes the connection when the ``with`` block exits, whether normally or
    due to an exception.

    Args:
        host: Database hostname or IP address.
        port: TNS listener port.
        service: Oracle service name (e.g. ``"FREEPDB1"``).
        user: Oracle username.
        password: Oracle password.

    Yields:
        An open :class:`oracledb.Connection`.
    """
    conn = oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service}")
    try:
        yield conn
    finally:
        conn.close()


def _oracle_error_code(exc: Exception) -> int | None:
    """Extract the ORA- error code from an :class:`oracledb.DatabaseError`.

    Args:
        exc: The exception to inspect.

    Returns:
        The integer ORA- code if *exc* is a database error with a parseable
        code attribute, otherwise ``None``.
    """
    if not isinstance(exc, oracledb.DatabaseError):
        return None
    error = exc.args[0]
    return getattr(error, "code", None)


def execute_ignore(conn: oracledb.Connection, sql: str, ignored_codes: set[int]) -> None:
    """Execute *sql*, suppressing specified ORA- error codes.

    Useful for idempotent DDL (``DROP … IF EXISTS`` semantics) where Oracle
    does not provide a native ``IF EXISTS`` clause.

    Args:
        conn: Active Oracle connection.
        sql: SQL statement to execute.
        ignored_codes: Set of ORA- error codes (e.g. ``{942}`` for
            ``ORA-00942: table or view does not exist``) that should be
            silently swallowed.

    Raises:
        Exception: Any database error whose code is not in *ignored_codes*.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    except Exception as exc:  # noqa: BLE001 - Oracle driver wraps errors by code.
        if _oracle_error_code(exc) not in ignored_codes:
            raise


def drop_schema(conn: oracledb.Connection, schema: str) -> None:
    """Drop an Oracle schema and all its objects (``CASCADE``).

    ``ORA-01918`` (user does not exist) is silently ignored, making this
    operation idempotent.

    Args:
        conn: Active Oracle connection with ``DROP USER`` privilege.
        schema: Schema name to drop.
    """
    LOGGER.info("Dropping schema %s", schema)
    execute_ignore(conn, f"DROP USER {oracle_identifier(schema)} CASCADE", {1918})


def ensure_schema(conn: oracledb.Connection, schema: str, password: str) -> None:
    """Create the schema user, or unlock and reset the password if it already exists.

    On success the schema is granted ``CONNECT`` and ``RESOURCE`` roles and
    unlimited quota on the ``USERS`` tablespace.

    Args:
        conn: Active Oracle connection with ``CREATE USER`` privilege.
        schema: Schema (user) name to create or reset.
        password: Password to assign to the schema user.
    """
    ident = oracle_identifier(schema)
    quoted_password = '"' + password.replace('"', '""') + '"'
    LOGGER.info("Creating/unlocking schema %s", schema)
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
    """Create or replace an Oracle DIRECTORY object.

    Args:
        conn: Active Oracle connection with ``CREATE ANY DIRECTORY`` privilege.
        name: Directory object name.
        path: OS path inside the Oracle server (container) to map.
    """
    LOGGER.info("Creating Oracle DIRECTORY %s -> %s", name, path)
    escaped_path = path.replace("'", "''")
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE OR REPLACE DIRECTORY {oracle_identifier(name)} AS '{escaped_path}'")
    conn.commit()


def drop_table(conn: oracledb.Connection, schema: str, table: str) -> None:
    """Drop a table, purging it from the recycle bin.

    ``ORA-00942`` (table or view does not exist) is silently ignored, making
    this operation idempotent.

    Args:
        conn: Active Oracle connection.
        schema: Table owner.
        table: Table name to drop.
    """
    LOGGER.debug("Dropping table %s.%s", schema, table)
    execute_ignore(conn, f"DROP TABLE {oracle_qualified_name(schema, table)} PURGE", {942})


def truncate_table(conn: oracledb.Connection, schema: str, table: str) -> None:
    """Truncate all rows from a table, leaving the structure intact.

    Unlike :func:`drop_table`, this preserves the table definition so that
    subsequent data-only imports (``CONTENT=DATA_ONLY`` / ``ROWS=Y``) can
    load directly without re-creating DDL.

    Args:
        conn: Active Oracle connection.
        schema: Table owner.
        table: Table name to truncate.
    """
    LOGGER.debug("Truncating table %s.%s", schema, table)
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {oracle_qualified_name(schema, table)}")
    conn.commit()


def count_rows(
    conn: oracledb.Connection,
    schema: str,
    table: str,
    partition_name: str | None = None,
) -> int:
    """Return the exact row count of a table (or partition) via ``SELECT COUNT(*)``.

    When *partition_name* is provided the count is scoped to that single
    partition via a ``PARTITION (name)`` clause.  This is used by the
    batch-import export path where the staging table holds all partitions'
    data and only the target partition's row count should be compared against
    the exported output.

    Args:
        conn: Active Oracle connection.
        schema: Table owner.
        table: Table name.
        partition_name: Optional Oracle partition name; when given, only rows
            in that partition are counted.

    Returns:
        Number of rows in ``schema.table`` (or the specified partition).
    """
    table_ref = oracle_qualified_name(schema, table)
    if partition_name:
        table_ref = f"{table_ref} PARTITION ({oracle_identifier(partition_name)})"
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_ref}")
        value = cursor.fetchone()[0]
    return int(value)


def configure_omf_destination(
    conn: oracledb.Connection,
    path: str = "/opt/oracle/oradata/FREE/FREEPDB1",
) -> None:
    """Set ``DB_CREATE_FILE_DEST`` so Oracle manages datafile paths automatically.

    Must be called once per PDB session before any ``CREATE TABLESPACE`` that
    omits an explicit ``DATAFILE`` clause.  ``SCOPE=MEMORY`` avoids requiring a
    server-parameter file (SPFILE) and is sufficient for the lifetime of the
    container instance.

    Args:
        conn: Active Oracle connection with ``ALTER SYSTEM`` privilege.
        path: Absolute directory path inside the container where Oracle will
            place new OMF datafiles.  Defaults to the standard Oracle Free
            PDB data directory.
    """
    LOGGER.info("Configuring OMF destination: DB_CREATE_FILE_DEST = %s", path)
    with conn.cursor() as cursor:
        cursor.execute(
            f"ALTER SYSTEM SET DB_CREATE_FILE_DEST = '{path}' SCOPE = MEMORY"
        )
    conn.commit()


def ensure_tablespace(conn: oracledb.Connection, tablespace: str) -> None:
    """Create *tablespace* using Oracle Managed Files if it does not already exist.

    Requires ``DB_CREATE_FILE_DEST`` to be configured (see
    :func:`configure_omf_destination`).  Oracle determines the datafile path
    automatically; no explicit ``DATAFILE`` clause is used.

    ``ORA-01543`` (tablespace already exists) is silently ignored so this
    function is idempotent.

    Args:
        conn: Active Oracle connection with ``CREATE TABLESPACE`` privilege.
        tablespace: Name of the tablespace to create.
    """
    LOGGER.info("Creating tablespace %s (OMF)", tablespace)
    execute_ignore(
        conn,
        f"CREATE TABLESPACE {oracle_identifier(tablespace)}"
        " DATAFILE SIZE 10M AUTOEXTEND ON NEXT 10M",
        {1543},  # ORA-01543: tablespace already exists
    )
    conn.commit()


def grant_quota_unlimited(conn: oracledb.Connection, schema: str, tablespace: str) -> None:
    """Grant QUOTA UNLIMITED on *tablespace* to *schema*."""
    LOGGER.debug("Granting QUOTA UNLIMITED on %s to %s", tablespace, schema)
    with conn.cursor() as cursor:
        cursor.execute(
            f"ALTER USER {oracle_identifier(schema)}"
            f" QUOTA UNLIMITED ON {oracle_identifier(tablespace)}"
        )
    conn.commit()


def table_exists(conn: oracledb.Connection, schema: str, table: str) -> bool:
    """Check whether a table exists in ``ALL_TABLES``.

    Args:
        conn: Active Oracle connection.
        schema: Table owner.
        table: Table name.

    Returns:
        ``True`` if the table exists and is visible to the connected user,
        ``False`` otherwise.
    """
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
