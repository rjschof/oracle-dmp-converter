"""Unit tests for core/staging.py."""
# pylint: disable=protected-access

from __future__ import annotations

from unittest.mock import MagicMock

from oracle_dmp_converter.core.staging import (
    apply_byte_to_char,
    dematerialize_mviews,
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

        disable_triggers(conn, "MYSCHEMA")

        sqls_a = action_cursor_a.execute.call_args[0][0]
        sqls_b = action_cursor_b.execute.call_args[0][0]
        assert "DISABLE" in sqls_a
        assert "TRG_A" in sqls_a
        assert "DISABLE" in sqls_b
        assert "TRG_B" in sqls_b

    def test_no_triggers_no_alter(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        disable_triggers(conn, "EMPTY_SCHEMA")

        # Only the SELECT call; no ALTER calls issued
        assert cursor.execute.call_count == 1


# ---------------------------------------------------------------------------
# drop_vpd_policies
# ---------------------------------------------------------------------------


class TestDropVpdPolicies:
    def test_drops_non_grouped_policy(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "POL1", "SYS_DEFAULT")])
        action_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]

        drop_vpd_policies(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "DROP_POLICY" in sql
        assert "DROP_GROUPED_POLICY" not in sql

    def test_drops_grouped_policy(self) -> None:
        discovery_cursor = _make_cursor([("ORDERS", "POL1", "MYGROUP")])
        action_cursor = _make_cursor()
        conn = MagicMock()
        conn.cursor.side_effect = [discovery_cursor, action_cursor]

        drop_vpd_policies(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "DROP_GROUPED_POLICY" in sql

    def test_no_policies_no_drop(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        drop_vpd_policies(conn, "EMPTY")

        assert cursor.execute.call_count == 1  # Only the SELECT


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

        apply_byte_to_char(conn, "MYSCHEMA")

        sql = action_cursor.execute.call_args[0][0]
        assert "ALTER TABLE" in sql
        assert "MODIFY" in sql
        assert "CHAR" in sql
        assert "40" in sql

    def test_no_byte_columns_no_alter(self) -> None:
        cursor = _make_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cursor

        apply_byte_to_char(conn, "ALLCHAR")

        assert cursor.execute.call_count == 1  # Only the SELECT
