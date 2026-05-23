"""Oracle connection and DDL helpers."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import oracledb

from oracle_dmp_converter.datapump._exit_policy import RETRYABLE_ORA_CODES
from oracle_dmp_converter.oracle.identifiers import oracle_identifier, oracle_qualified_name

LOGGER = logging.getLogger(__name__)

# Bounded exponential backoff for transient Oracle errors (listener
# unreachable, timeouts, lock contention).  See ``with_oracle_retry``.
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_BASE_SECONDS = 0.5
DEFAULT_RETRY_MAX_SECONDS = 8.0


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
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
) -> Iterator[oracledb.Connection]:
    """Context manager that yields a connected :class:`oracledb.Connection`.

    Closes the connection when the ``with`` block exits, whether normally or
    due to an exception.  Connection acquisition is retried via
    :func:`with_oracle_retry` for transient errors (listener unreachable,
    timeouts) so flaky networks or a still-warming-up container don't
    abort the whole conversion.

    Args:
        host: Database hostname or IP address.
        port: TNS listener port.
        service: Oracle service name (e.g. ``"FREEPDB1"``).
        user: Oracle username.
        password: Oracle password.
        retry_attempts: Maximum number of connection attempts before giving
            up.  Set to ``1`` to disable retries.

    Yields:
        An open :class:`oracledb.Connection`.
    """
    dsn = f"{host}:{port}/{service}"
    conn = with_oracle_retry(
        lambda: oracledb.connect(user=user, password=password, dsn=dsn),
        attempts=retry_attempts,
        what=f"oracledb.connect({dsn})",
    )
    try:
        yield conn
    finally:
        conn.close()


def with_oracle_retry[T](
    operation: Callable[[], T],
    *,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
    max_seconds: float = DEFAULT_RETRY_MAX_SECONDS,
    what: str = "oracle operation",
) -> T:
    """Invoke *operation* with bounded exponential-backoff retry.

    Re-runs *operation* when it raises an :class:`oracledb.DatabaseError`
    whose ORA code is in :data:`RETRYABLE_ORA_CODES` (listener unreachable,
    TNS timeouts, row-lock contention, etc.).  Any other exception type, or
    an Oracle error code not classified as retryable, propagates unchanged
    so genuine misconfiguration (bad credentials, missing privileges) fails
    fast.

    Args:
        operation: Zero-arg callable to execute.
        attempts: Total number of attempts (including the first).  ``1``
            disables retry.
        base_seconds: Initial backoff delay between attempts; doubles each
            time until capped at *max_seconds*.
        max_seconds: Upper bound on the backoff delay.
        what: Short description used in log messages so a retry storm in
            the log can be traced to its caller.

    Returns:
        Whatever *operation* returns on its first successful invocation.

    Raises:
        Exception: The last exception raised by *operation* once all
            attempts are exhausted, or any non-retryable exception
            encountered along the way.
    """
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except oracledb.DatabaseError as exc:
            code = _oracle_error_code(exc)
            last_exc = exc
            if code not in RETRYABLE_ORA_CODES or attempt == attempts:
                raise
            delay = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.1)  # noqa: S311 — non-crypto jitter
            LOGGER.warning(
                "%s failed with ORA-%05d (attempt %d/%d); retrying in %.2fs",
                what,
                code or 0,
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)
    # Unreachable: the loop either returns or re-raises.
    assert last_exc is not None
    raise last_exc


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
    except oracledb.DatabaseError as exc:
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
    except oracledb.DatabaseError as exc:
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
    subpartition_name: str | None = None,
) -> int:
    """Return the exact row count of a table (or partition) via ``SELECT COUNT(*)``.

    When *partition_name* is provided the count is scoped to that single
    partition via a ``PARTITION (name)`` clause.  This is used by the
    batch-import export path where the staging table holds all partitions'
    data and only the target partition's row count should be compared against
    the exported output.

    When *subpartition_name* is provided a bare ``SUBPARTITION (name)``
    clause is used instead — subpartition names are unique within a table,
    so the parent ``PARTITION (...)`` qualifier is unnecessary.

    Args:
        conn: Active Oracle connection.
        schema: Table owner.
        table: Table name.
        partition_name: Optional Oracle partition name; when given, only rows
            in that partition are counted.
        subpartition_name: Optional Oracle subpartition name; takes
            precedence over *partition_name* when both are set.

    Returns:
        Number of rows in ``schema.table`` (or the specified partition).
    """
    table_ref = oracle_qualified_name(schema, table)
    if subpartition_name:
        table_ref = f"{table_ref} SUBPARTITION ({oracle_identifier(subpartition_name)})"
    elif partition_name:
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

    Thin shim that delegates to
    :func:`oracle_dmp_converter.oracle.omf.ensure_db_create_file_dest`.  Kept
    for back-compat with call sites that import this symbol from
    :mod:`oracle_dmp_converter.oracle.conn`.

    Args:
        conn: Active Oracle connection with ``ALTER SYSTEM`` privilege.
        path: Absolute directory path inside the container where Oracle will
            place new OMF datafiles.  Defaults to the standard Oracle Free
            PDB data directory.
    """
    # Local import avoids a hard module-load dependency cycle if any code
    # in oracle/ ever imports conn.py at import time.
    # pylint: disable=import-outside-toplevel
    from oracle_dmp_converter.oracle.omf import (  # noqa: PLC0415
        ensure_db_create_file_dest,
    )

    ensure_db_create_file_dest(conn, path)


def ensure_tablespace(conn: oracledb.Connection, tablespace: str) -> None:
    """Create *tablespace* using Oracle Managed Files if it does not already exist.

    Thin shim that delegates to
    :func:`oracle_dmp_converter.oracle.omf.create_tablespace_if_missing` while
    preserving the historical ``ORA-01543 -> ignore`` idempotency.

    Args:
        conn: Active Oracle connection with ``CREATE TABLESPACE`` privilege.
        tablespace: Name of the tablespace to create.
    """
    # pylint: disable=import-outside-toplevel
    from oracle_dmp_converter.oracle.omf import (  # noqa: PLC0415
        create_tablespace_if_missing,
    )

    create_tablespace_if_missing(conn, tablespace)


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
