"""Unit tests for oracle/omf.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import oracledb
import pytest

from oracle_dmp_converter.oracle.omf import (
    create_tablespace_if_missing,
    ensure_db_create_file_dest,
)


def _make_cursor(fetchone_value=None) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = fetchone_value
    return cursor


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    conn.cursor.side_effect = cursors
    return conn


def _db_error(code: int) -> oracledb.DatabaseError:
    info = MagicMock()
    info.code = code
    return oracledb.DatabaseError(info)


class TestEnsureDbCreateFileDest:
    def test_skips_when_already_set(self) -> None:
        cursor = _make_cursor(fetchone_value=("/opt/oracle/oradata/FREE/FREEPDB1",))
        conn = _make_conn([cursor])
        result = ensure_db_create_file_dest(conn, "/somewhere/else")
        assert result is False
        # only the SELECT ran — no ALTER SYSTEM
        assert cursor.execute.call_count == 1

    def test_sets_when_unset(self) -> None:
        select_cursor = _make_cursor(fetchone_value=(None,))
        alter_cursor = _make_cursor()
        conn = _make_conn([select_cursor, alter_cursor])
        result = ensure_db_create_file_dest(conn, "/opt/oracle/oradata/FREE/FREEPDB1")
        assert result is True
        sql = alter_cursor.execute.call_args[0][0]
        assert "ALTER SYSTEM" in sql
        assert "/opt/oracle/oradata/FREE/FREEPDB1" in sql

    def test_escapes_quotes_in_path(self) -> None:
        select_cursor = _make_cursor(fetchone_value=(None,))
        alter_cursor = _make_cursor()
        conn = _make_conn([select_cursor, alter_cursor])
        ensure_db_create_file_dest(conn, "/path'with/quote")
        sql = alter_cursor.execute.call_args[0][0]
        assert "/path''with/quote" in sql


class TestCreateTablespaceIfMissing:
    def test_creates_new_tablespace(self) -> None:
        cursor = _make_cursor()
        conn = _make_conn([cursor])
        assert create_tablespace_if_missing(conn, "COMBINED_DATA") is True
        sql = cursor.execute.call_args[0][0]
        assert "CREATE TABLESPACE" in sql
        assert "COMBINED_DATA" in sql

    def test_returns_false_when_already_exists(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(1543)
        conn = _make_conn([cursor])
        assert create_tablespace_if_missing(conn, "EXISTING") is False

    def test_raises_helpful_error_when_db_create_file_dest_unset(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(2199)
        conn = _make_conn([cursor])
        with pytest.raises(ValueError, match="DB_CREATE_FILE_DEST"):
            create_tablespace_if_missing(conn, "NEW_TS")

    def test_reraises_other_oracle_errors(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(942)
        conn = _make_conn([cursor])
        with pytest.raises(oracledb.DatabaseError):
            create_tablespace_if_missing(conn, "NEW_TS")
