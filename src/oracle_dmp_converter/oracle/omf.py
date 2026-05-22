"""Oracle Managed Files (OMF) helpers.

Centralises the two pieces of OMF logic the converter relies on:

* :func:`ensure_db_create_file_dest` configures ``DB_CREATE_FILE_DEST`` so
  Oracle picks the datafile path automatically when ``CREATE TABLESPACE``
  omits a ``DATAFILE`` clause.
* :func:`create_tablespace_if_missing` issues a DATAFILE-less
  ``CREATE TABLESPACE`` and translates the two Oracle errors that matter into
  a clean boolean / :class:`ValueError`.

The connection-level helpers in :mod:`oracle_dmp_converter.oracle.conn` are
thin shims that delegate here so older call sites keep working.
"""

from __future__ import annotations

import logging

import oracledb

from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)

# ORA-01543: tablespace 'X' already exists.
_ORA_TABLESPACE_EXISTS = 1543
# ORA-02199: missing DATAFILE clause (raised when DB_CREATE_FILE_DEST is unset).
_ORA_MISSING_DATAFILE = 2199


def ensure_db_create_file_dest(
    conn: oracledb.Connection,
    path: str = "/opt/oracle/oradata",
) -> bool:
    """Ensure ``DB_CREATE_FILE_DEST`` is set to *path*.

    Reads ``v$parameter`` to check the current value; if it already matches
    (or any non-empty value is set), returns ``False`` without issuing an
    ``ALTER SYSTEM``.  Otherwise runs
    ``ALTER SYSTEM SET DB_CREATE_FILE_DEST='<path>' SCOPE=BOTH``, commits, and
    returns ``True``.

    Args:
        conn: Active Oracle connection with ``ALTER SYSTEM`` privilege.
        path: Absolute directory inside the Oracle server where OMF datafiles
            should be placed.

    Returns:
        ``True`` if the parameter was changed, ``False`` if it was already set.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT value FROM v$parameter WHERE name = 'db_create_file_dest'")
        row = cursor.fetchone()
    current = (row[0] if row else None) or ""
    if current:
        LOGGER.debug("DB_CREATE_FILE_DEST already set to %s; not changing", current)
        return False
    LOGGER.info("Setting DB_CREATE_FILE_DEST = %s", path)
    escaped = path.replace("'", "''")
    with conn.cursor() as cursor:
        cursor.execute(f"ALTER SYSTEM SET DB_CREATE_FILE_DEST = '{escaped}' SCOPE = BOTH")
    conn.commit()
    return True


def create_tablespace_if_missing(conn: oracledb.Connection, name: str) -> bool:
    """Create *name* as an OMF tablespace (no DATAFILE clause).

    Args:
        conn: Active Oracle connection with ``CREATE TABLESPACE`` privilege.
        name: Tablespace name to create.

    Returns:
        ``True`` if the tablespace was created by this call; ``False`` if it
        already existed (``ORA-01543``).

    Raises:
        ValueError: If Oracle reports ``ORA-02199`` ("missing DATAFILE
            clause"), meaning ``DB_CREATE_FILE_DEST`` has not been
            configured.  Callers should invoke
            :func:`ensure_db_create_file_dest` first.
    """
    LOGGER.info("Creating tablespace %s (OMF)", name)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLESPACE {oracle_identifier(name)}")
        conn.commit()
        return True
    except oracledb.DatabaseError as exc:
        code = getattr(exc.args[0], "code", None) if exc.args else None
        if code == _ORA_TABLESPACE_EXISTS:
            return False
        if code == _ORA_MISSING_DATAFILE:
            raise ValueError(
                f"Cannot create tablespace {name!r}: DB_CREATE_FILE_DEST is not "
                "configured (ORA-02199). Call ensure_db_create_file_dest() first."
            ) from exc
        raise
