"""Unit tests for core/staging.py."""
# pylint: disable=protected-access

from __future__ import annotations

from unittest.mock import MagicMock

import oracledb

from oracle_dmp_converter.core.staging import (
    apply_byte_to_char,
    dematerialize_mviews,
    disable_foreign_keys,
    disable_triggers,
    drop_vpd_policies,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(fetchall=()) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = list(fetchall)
    return cursor


def _make_conn(fetchall_values: list | None = None) -> MagicMock:
    """Build a connection mock where successive cursor() calls return cursors
    with successive fetchall results from *fetchall_values*."""
    conn = MagicMock()
    if fetchall_values is None:
        conn.cursor.return_value = _make_cursor()
    else:
        cursors = []
        for rows in fetchall_values:
            cursors.append(_make_cursor(rows))
        conn.cursor.side_effect = cursors
    return conn


def _make_ora_error(code: int) -> oracledb.DatabaseError:
    """Build a minimal oracledb.DatabaseError whose first arg has a .code attribute."""
    err_info = MagicMock()
    err_info.code = code
    exc = oracledb.DatabaseError(err_info)
    return exc


# ---------------------------------------------------------------------------
# disable_triggers
# ---------------------------------------------------------------------------


class TestDisableTriggers:
    def test_disables_each_trigger(self) -> None:
        discovery_cursor = _make_cursor([("TRG_A",), ("TRG_B",)])
        action_cursor_a = _make_cursor()
        action_cursor_b = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor_a, action_cursor_b]

        count = disable_triggers(conn, "MYSCHEMA")

        sqls_a = action_cursor_a.execute.call_args[0][0]
        sqls_b = action_cursor_b.execute.call_args[0][0]
        assert "DISABLE" in sqls_a
        assert "TRG_A" in sqls_a
        assert "DISABLE" in sqls_b
        assert "TRG_B" in sqls_b
        assert count == 2

    def test_no_triggers_no_alter(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        count = disable_triggers(conn, "EMPTY_SCHEMA")

        # Only the SELECT call; no ALTER calls issued
        assert cursor.execute.call_count == 1
        assert count == 0

    def test_missing_trigger_ora_04080_skipped_silently(self) -> None:
        """ORA-04080 (trigger not found) should be skipped with no warning."""
        discovery_cursor = _make_cursor([("TRG_GONE",)])
        missing_cursor = _make_cursor()
        missing_cursor.execute.side_effect = _make_ora_error(4080)
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, missing_cursor]

        count = disable_triggers(conn, "MYSCHEMA")
        assert count == 0

    def test_other_error_warns_and_continues(self) -> None:
        """Non-404080 DatabaseError should log a warning and not stop processing."""
        discovery_cursor = _make_cursor([("TRG_BAD",), ("TRG_OK",)])
        bad_cursor = _make_cursor()
        bad_cursor.execute.side_effect = _make_ora_error(600)
        good_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, bad_cursor, good_cursor]

        count = disable_triggers(conn, "MYSCHEMA")
        assert count == 1  # only TRG_OK succeeded

    def test_one_missing_one_success_returns_one(self) -> None:
        discovery_cursor = _make_cursor([("TRG_GONE",), ("TRG_OK",)])
        missing_cursor = _make_cursor()
        missing_cursor.execute.side_effect = _make_ora_error(4080)
        good_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, missing_cursor, good_cursor]

        count = disable_triggers(conn, "MYSCHEMA")
        assert count == 1


# ---------------------------------------------------------------------------
# drop_vpd_policies
# ---------------------------------------------------------------------------


class TestDropVpdPolicies:
    def test_drops_non_grouped_policy(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "POL1", "SYS_DEFAULT")])
        action_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]

        count = drop_vpd_policies(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "DROP_POLICY" in sql
        assert "DROP_GROUPED_POLICY" not in sql
        assert count == 1

    def test_drops_grouped_policy(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "POL1", "MYGROUP")])
        action_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]

        count = drop_vpd_policies(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "DROP_GROUPED_POLICY" in sql
        assert count == 1

    def test_no_policies_no_drop(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        count = drop_vpd_policies(conn, "EMPTY")

        assert cursor.execute.call_count == 1  # Only the SELECT
        assert count == 0

    def test_missing_policy_ora_28102_skipped_silently(self) -> None:
        """ORA-28102 (policy does not exist) should be skipped with no warning."""
        discovery_cursor = _make_cursor([("ORDERS", "POL_GONE", "SYS_DEFAULT")])
        missing_cursor = _make_cursor()
        missing_cursor.execute.side_effect = _make_ora_error(28102)
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, missing_cursor]

        count = drop_vpd_policies(conn, "MYSCHEMA")
        assert count == 0

    def test_other_error_warns_and_continues(self) -> None:
        discovery_cursor = _make_cursor(
            [("ORDERS", "POL_BAD", "SYS_DEFAULT"), ("ORDERS", "POL_OK", "SYS_DEFAULT")]
        )
        bad_cursor = _make_cursor()
        bad_cursor.execute.side_effect = _make_ora_error(600)
        good_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, bad_cursor, good_cursor]

        count = drop_vpd_policies(conn, "MYSCHEMA")
        assert count == 1


# ---------------------------------------------------------------------------
# dematerialize_mviews
# ---------------------------------------------------------------------------


class TestDematerializeMviews:
    def test_no_mviews_returns_early(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        dematerialize_mviews(conn, "MYSCHEMA")

        assert cursor.execute.call_count == 1  # Only the SELECT

    def test_creates_tmp_drops_mview_and_renames(self) -> None:
        discovery_cursor = _make_cursor([("MV_ORDERS",)])
        create_cursor = _make_cursor()
        drop_cursor = _make_cursor()
        rename_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [
            discovery_cursor,
            create_cursor,
            drop_cursor,
            rename_cursor,
        ]

        dematerialize_mviews(conn, "MYSCHEMA")

        create_sql = create_cursor.execute.call_args[0][0]
        drop_sql = drop_cursor.execute.call_args[0][0]
        rename_sql = rename_cursor.execute.call_args[0][0]

        assert "CREATE TABLE" in create_sql
        assert "_$TMP" in create_sql
        assert "DROP MATERIALIZED VIEW" in drop_sql
        assert "MV_ORDERS" in drop_sql
        assert "RENAME TO" in rename_sql
        assert "MV_ORDERS" in rename_sql


# ---------------------------------------------------------------------------
# apply_byte_to_char
# ---------------------------------------------------------------------------


class TestApplyByteToChar:
    def test_modifies_byte_columns(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "NAME", "VARCHAR2", 40)])
        action_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]

        count = apply_byte_to_char(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "ALTER TABLE" in sql
        assert "MODIFY" in sql
        assert "CHAR" in sql
        assert "40" in sql
        assert count == 1

    def test_no_byte_columns_no_alter(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        count = apply_byte_to_char(conn, "ALLCHAR")

        assert cursor.execute.call_count == 1  # Only the SELECT
        assert count == 0

    def test_failed_alter_warns_and_continues(self) -> None:
        discovery_cursor = _make_cursor(
            [("ORDERS", "COL_BAD", "VARCHAR2", 50), ("ORDERS", "COL_OK", "VARCHAR2", 30)]
        )
        bad_cursor = _make_cursor()
        bad_cursor.execute.side_effect = _make_ora_error(1439)
        good_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, bad_cursor, good_cursor]

        count = apply_byte_to_char(conn, "MYSCHEMA")
        assert count == 1  # only COL_OK succeeded

    def test_all_columns_fail_returns_zero(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "COL_A", "VARCHAR2", 20)])
        bad_cursor = _make_cursor()
        bad_cursor.execute.side_effect = _make_ora_error(1439)
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, bad_cursor]

        count = apply_byte_to_char(conn, "MYSCHEMA")
        assert count == 0


# ---------------------------------------------------------------------------
# disable_foreign_keys
# ---------------------------------------------------------------------------


class TestDisableForeignKeys:
    def test_disables_each_constraint(self) -> None:
        discovery_cursor = _make_cursor(
            [("ORDERS", "FK_ORDERS_CUSTOMER"), ("LINES", "FK_LINES_ORDERS")]
        )
        action_cursor_a = _make_cursor()
        action_cursor_b = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor_a, action_cursor_b]

        count = disable_foreign_keys(conn, "DMP_HR")
        assert count == 2
        action_cursor_a.execute.assert_called_once()
        action_cursor_b.execute.assert_called_once()
        # Statements should reference the schema and constraint name
        sql_a = action_cursor_a.execute.call_args[0][0]
        assert "DMP_HR" in sql_a
        assert "FK_ORDERS_CUSTOMER" in sql_a
        assert "DISABLE CONSTRAINT" in sql_a

    def test_no_constraints_returns_zero(self) -> None:
        conn = _make_conn(fetchall_values=[[]])
        count = disable_foreign_keys(conn, "EMPTY")
        assert count == 0

    def test_missing_constraint_is_skipped_silently(self) -> None:
        # ORA-02430/2431 means the constraint doesn't exist anymore; not fatal.
        discovery_cursor = _make_cursor([("T", "FK_GONE")])
        action_cursor = _make_cursor()
        action_cursor.execute.side_effect = _make_ora_error(2431)
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]
        count = disable_foreign_keys(conn, "S")
        assert count == 0

    def test_other_oracle_error_logged_and_continues(self) -> None:
        # Two constraints: first errors with non-skip code, second succeeds.
        discovery_cursor = _make_cursor([("T1", "FK1"), ("T2", "FK2")])
        bad_cursor = _make_cursor()
        bad_cursor.execute.side_effect = _make_ora_error(1)
        good_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, bad_cursor, good_cursor]
        count = disable_foreign_keys(conn, "S")
        assert count == 1  # only the second succeeded
