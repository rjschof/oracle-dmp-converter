"""Unit tests for oracle/metadata.py."""
# pylint: disable=protected-access

from __future__ import annotations

from unittest.mock import MagicMock

import oracledb
import pytest

from oracle_dmp_converter.oracle.metadata import _estimated_segment_bytes, discover_table_metadata

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor() -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _db_error(code: int) -> oracledb.DatabaseError:
    info = MagicMock()
    info.code = code
    return oracledb.DatabaseError(info)


# ---------------------------------------------------------------------------
# _estimated_segment_bytes
# ---------------------------------------------------------------------------


class TestEstimatedSegmentBytes:
    def test_returns_bytes_when_available(self) -> None:
        cursor = _make_cursor()
        cursor.fetchone.return_value = (1024,)
        result = _estimated_segment_bytes(cursor, schema="S", table="T")
        assert result == 1024

    def test_returns_zero_when_null(self) -> None:
        cursor = _make_cursor()
        cursor.fetchone.return_value = (None,)
        result = _estimated_segment_bytes(cursor, schema="S", table="T")
        assert result == 0

    def test_returns_none_on_ora_942(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(942)
        result = _estimated_segment_bytes(cursor, schema="S", table="T")
        assert result is None

    def test_reraises_other_db_errors(self) -> None:
        cursor = _make_cursor()
        cursor.execute.side_effect = _db_error(1)
        with pytest.raises(oracledb.DatabaseError):
            _estimated_segment_bytes(cursor, schema="S", table="T")


# ---------------------------------------------------------------------------
# discover_table_metadata — full integration
# ---------------------------------------------------------------------------


class TestDiscoverTableMetadata:
    def _setup_cursor(
        self,
        *,
        col_rows=None,
        seg_bytes=1024,
        table_stats=(100, 50),
        partition_rows=None,
        pk_rows=None,
        unique_rows=None,
    ) -> MagicMock:
        col_rows = col_rows or [
            ("ID", "NUMBER", 1, "N", 10, 0, 0, "B"),
            ("NAME", "VARCHAR2", 2, "Y", None, None, 40, "C"),
        ]
        cursor = _make_cursor()
        cursor.fetchall.side_effect = [
            col_rows,
            partition_rows or [],
            pk_rows or [("ID",)],
            unique_rows or [],
        ]
        cursor.fetchone.side_effect = [
            (seg_bytes,),
            table_stats,
        ]
        return cursor

    def test_basic_metadata(self) -> None:
        cursor = self._setup_cursor()
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "MYSCHEMA", "ORDERS")
        assert meta.schema == "MYSCHEMA"
        assert meta.name == "ORDERS"
        assert len(meta.columns) == 2
        assert meta.columns[0].name == "ID"
        assert meta.columns[1].name == "NAME"
        assert meta.row_count == 100
        assert meta.estimated_bytes == 1024

    def test_primary_key_populated(self) -> None:
        cursor = self._setup_cursor(pk_rows=[("ID",)])
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.primary_key == ("ID",)

    def test_unique_keys_populated(self) -> None:
        cursor = self._setup_cursor(unique_rows=[("UK1", "EMAIL"), ("UK1", "PHONE")])
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert len(meta.unique_keys) == 1
        assert set(meta.unique_keys[0]) == {"EMAIL", "PHONE"}

    def test_partitions_populated(self) -> None:
        cursor = self._setup_cursor(partition_rows=[("P1", 1), ("P2", 2)])
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert len(meta.partitions) == 2
        assert meta.partitions[0].name == "P1"
        assert meta.partitions[1].name == "P2"

    def test_no_row_count_when_stats_none(self) -> None:
        cursor = _make_cursor()
        cursor.fetchall.side_effect = [
            [("ID", "NUMBER", 1, "N", 10, 0, 0, "B")],
            [],
            [],
            [],
        ]
        cursor.fetchone.side_effect = [
            (512,),
            None,
        ]
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.row_count is None

    def test_estimated_bytes_from_stats_when_segment_unavailable(self) -> None:
        cursor = _make_cursor()
        cursor.fetchall.side_effect = [
            [("ID", "NUMBER", 1, "N", 10, 0, 0, "B")],
            [],
            [],
            [],
        ]
        # _estimated_segment_bytes raises ORA-942 → returns None
        cursor.execute.side_effect = [
            None,  # ALL_TAB_COLUMNS
            _db_error(942),  # ALL_SEGMENTS → returns None
            None,  # ALL_TABLES
            None,  # ALL_TAB_PARTITIONS
            None,  # ALL_CONSTRAINTS PK
            None,  # ALL_CONSTRAINTS UK
        ]
        cursor.fetchone.side_effect = [
            (100, 50),  # ALL_TABLES: 100 rows * 50 avg_row_len = 5000
        ]
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.estimated_bytes == 5000
