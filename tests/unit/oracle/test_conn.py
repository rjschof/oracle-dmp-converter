"""Unit tests for oracle/conn.py."""
# pylint: disable=protected-access,unused-argument

from __future__ import annotations

from unittest.mock import MagicMock, patch

import oracledb
import pytest

from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    _oracle_error_code,
    count_rows,
    create_directory,
    drop_schema,
    drop_table,
    ensure_schema,
    ensure_tablespace,
    execute_ignore,
    grant_quota_unlimited,
    oracle_connection,
    table_exists,
    truncate_table,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _db_error(code: int) -> oracledb.DatabaseError:
    """Construct an oracledb.DatabaseError with a given ORA- code."""
    info = MagicMock()
    info.code = code
    return oracledb.DatabaseError(info)


def _make_cursor(fetchall=(), fetchone=None) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = list(fetchall)
    cursor.fetchone.return_value = fetchone
    return cursor


def _make_conn(cursor: MagicMock | None = None) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor or _make_cursor()
    return conn


# ---------------------------------------------------------------------------
# OracleCredentials
# ---------------------------------------------------------------------------


class TestOracleCredentials:
    def test_userid_format(self) -> None:
        creds = OracleCredentials(user="system", password="secret", service="FREEPDB1")
        assert creds.userid == "system/secret@FREEPDB1"

    def test_userid_default_service(self) -> None:
        creds = OracleCredentials(user="admin", password="pw")
        assert creds.userid == "admin/pw@FREEPDB1"


# ---------------------------------------------------------------------------
# oracle_connection
# ---------------------------------------------------------------------------


class TestOracleConnection:
    def test_yields_connection_and_closes(self) -> None:
        mock_conn = MagicMock()
        with patch("oracle_dmp_converter.oracle.conn.oracledb.connect", return_value=mock_conn):
            with oracle_connection(
                host="localhost", port=1521, service="FREE", user="u", password="p"
            ) as conn:
                assert conn is mock_conn
        mock_conn.close.assert_called_once()

    def test_closes_on_exception(self) -> None:
        mock_conn = MagicMock()
        with patch("oracle_dmp_converter.oracle.conn.oracledb.connect", return_value=mock_conn):
            with pytest.raises(RuntimeError):
                with oracle_connection(
                    host="localhost", port=1521, service="FREE", user="u", password="p"
                ):
                    raise RuntimeError("boom")
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# _oracle_error_code
# ---------------------------------------------------------------------------


class TestOracleErrorCode:
    def test_returns_code_for_database_error(self) -> None:
        exc = _db_error(942)
        assert _oracle_error_code(exc) == 942

    def test_returns_none_for_non_database_error(self) -> None:
        assert _oracle_error_code(ValueError("nope")) is None

    def test_returns_none_when_no_code_attr(self) -> None:
        exc = oracledb.DatabaseError(object())
        assert _oracle_error_code(exc) is None


# ---------------------------------------------------------------------------
# execute_ignore
# ---------------------------------------------------------------------------


class TestExecuteIgnore:
    def test_executes_successfully(self) -> None:
        conn = _make_conn()
        execute_ignore(conn, "DROP TABLE T", {942})
        conn.cursor.return_value.execute.assert_called_once_with("DROP TABLE T")

    def test_suppresses_ignored_code(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(942)
        conn = _make_conn(cursor)
        execute_ignore(conn, "DROP TABLE T", {942})  # must not raise

    def test_reraises_other_db_error(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(1)
        conn = _make_conn(cursor)
        with pytest.raises(oracledb.DatabaseError):
            execute_ignore(conn, "DROP TABLE T", {942})

    def test_reraises_non_db_error(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("unexpected")
        conn = _make_conn(cursor)
        with pytest.raises(RuntimeError):
            execute_ignore(conn, "SELECT 1", {942})


# ---------------------------------------------------------------------------
# drop_schema
# ---------------------------------------------------------------------------


class TestDropSchema:
    def test_executes_drop_user(self) -> None:
        conn = _make_conn()
        drop_schema(conn, "MYSCHEMA")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "DROP USER" in sql
        assert "CASCADE" in sql

    def test_ignores_user_not_exists(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(1918)
        conn = _make_conn(cursor)
        drop_schema(conn, "GONE")  # must not raise


# ---------------------------------------------------------------------------
# ensure_schema
# ---------------------------------------------------------------------------


class TestEnsureSchema:
    def test_creates_user_and_grants(self) -> None:
        conn = _make_conn()
        ensure_schema(conn, "NEWSCHEMA", "pass123")
        sqls = [c[0][0] for c in conn.cursor.return_value.execute.call_args_list]
        assert any("CREATE USER" in s for s in sqls)
        assert any("GRANT" in s for s in sqls)
        conn.commit.assert_called()

    def test_alters_existing_user_on_duplicate(self) -> None:
        call_count = 0

        def side_effect(sql, *a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _db_error(1920)

        cursor = _make_cursor()
        cursor.execute.side_effect = side_effect
        conn = _make_conn(cursor)
        ensure_schema(conn, "EXISTS", "newpass")
        sqls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("ALTER USER" in s for s in sqls)

    def test_reraises_unexpected_create_error(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(28)
        conn = _make_conn(cursor)
        with pytest.raises(oracledb.DatabaseError):
            ensure_schema(conn, "BAD", "pw")


# ---------------------------------------------------------------------------
# create_directory
# ---------------------------------------------------------------------------


class TestCreateDirectory:
    def test_executes_create_directory(self) -> None:
        conn = _make_conn()
        create_directory(conn, "DUMP_DIR", "/dumps")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "CREATE OR REPLACE DIRECTORY" in sql
        assert "/dumps" in sql
        conn.commit.assert_called_once()

    def test_escapes_single_quotes_in_path(self) -> None:
        conn = _make_conn()
        create_directory(conn, "D", "/it's/here")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "it''s" in sql


# ---------------------------------------------------------------------------
# drop_table / truncate_table
# ---------------------------------------------------------------------------


class TestDropTable:
    def test_executes_drop_purge(self) -> None:
        conn = _make_conn()
        drop_table(conn, "MYSCHEMA", "ORDERS")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "DROP TABLE" in sql
        assert "PURGE" in sql

    def test_ignores_table_not_exists(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(942)
        conn = _make_conn(cursor)
        drop_table(conn, "S", "T")  # must not raise


class TestTruncateTable:
    def test_executes_truncate_and_commits(self) -> None:
        conn = _make_conn()
        truncate_table(conn, "MYSCHEMA", "ORDERS")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "TRUNCATE TABLE" in sql
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# count_rows
# ---------------------------------------------------------------------------


class TestCountRows:
    def test_plain_count(self) -> None:
        cursor = _make_cursor(fetchone=(42,))
        conn = _make_conn(cursor)
        result = count_rows(conn, "S", "T")
        assert result == 42
        sql = cursor.execute.call_args[0][0]
        assert "PARTITION" not in sql

    def test_count_with_partition(self) -> None:
        cursor = _make_cursor(fetchone=(7,))
        conn = _make_conn(cursor)
        result = count_rows(conn, "S", "T", partition_name="P1")
        assert result == 7
        sql = cursor.execute.call_args[0][0]
        assert "PARTITION" in sql


# ---------------------------------------------------------------------------
# ensure_tablespace
# ---------------------------------------------------------------------------


class TestEnsureTablespace:
    def test_creates_tablespace(self) -> None:
        conn = _make_conn()
        ensure_tablespace(conn, "MY_TS")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "CREATE TABLESPACE" in sql
        assert "my_ts" in sql
        conn.commit.assert_called()

    def test_ignores_already_exists(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(1543)
        conn = _make_conn(cursor)
        ensure_tablespace(conn, "EXISTS_TS")  # must not raise


# ---------------------------------------------------------------------------
# grant_quota_unlimited
# ---------------------------------------------------------------------------


class TestGrantQuotaUnlimited:
    def test_executes_alter_user(self) -> None:
        conn = _make_conn()
        grant_quota_unlimited(conn, "MYUSER", "MY_TS")
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "ALTER USER" in sql
        assert "QUOTA UNLIMITED" in sql
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# table_exists
# ---------------------------------------------------------------------------


class TestTableExists:
    def test_returns_true_when_found(self) -> None:
        cursor = _make_cursor(fetchone=(1,))
        conn = _make_conn(cursor)
        assert table_exists(conn, "S", "T") is True

    def test_returns_false_when_not_found(self) -> None:
        cursor = _make_cursor(fetchone=(0,))
        conn = _make_conn(cursor)
        assert table_exists(conn, "S", "GONE") is False
