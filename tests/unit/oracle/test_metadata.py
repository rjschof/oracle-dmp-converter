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


def _col_row(
    name: str,
    data_type: str,
    ordinal: int,
    *,
    nullable: str = "Y",
    precision: int | None = None,
    scale: int | None = None,
    char_length: int | None = None,
    char_used: str | None = None,
    data_type_owner: str | None = None,
    hidden: str = "NO",
    comment: str | None = None,
) -> tuple:
    """Build an ALL_TAB_COLS row matching the discover_table_metadata projection."""
    return (
        name,
        data_type,
        ordinal,
        nullable,
        precision,
        scale,
        char_length,
        char_used,
        data_type_owner,
        hidden,
        comment,
    )


# Tuple shape returned for the ALL_TABLES + ALL_TAB_COMMENTS join:
# (NUM_ROWS, AVG_ROW_LEN, IOT_TYPE, TEMPORARY, COMMENTS).
def _table_row(
    num_rows: int | None,
    avg_row_len: int | None,
    *,
    iot_type: str | None = None,
    temporary: str = "N",
    comment: str | None = None,
) -> tuple:
    return (num_rows, avg_row_len, iot_type, temporary, comment)


class TestDiscoverTableMetadata:
    def _setup_cursor(
        self,
        *,
        col_rows=None,
        seg_bytes=1024,
        table_stats=None,
        external_row=None,
        partition_rows=None,
        pk_rows=None,
        unique_rows=None,
    ) -> MagicMock:
        col_rows = col_rows or [
            _col_row(
                "ID", "NUMBER", 1, nullable="N", precision=10, scale=0, char_length=0, char_used="B"
            ),
            _col_row("NAME", "VARCHAR2", 2, char_length=40, char_used="C"),
        ]
        if table_stats is None:
            table_stats = _table_row(100, 50)
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
            external_row,  # ALL_EXTERNAL_TABLES probe
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
        assert meta.table_type == "TABLE"

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
            [_col_row("ID", "NUMBER", 1, nullable="N", precision=10, scale=0)],
            [],
            [],
            [],
        ]
        cursor.fetchone.side_effect = [
            (512,),
            None,
            None,
        ]
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.row_count is None

    def test_estimated_bytes_from_stats_when_segment_unavailable(self) -> None:
        cursor = _make_cursor()
        cursor.fetchall.side_effect = [
            [_col_row("ID", "NUMBER", 1, nullable="N", precision=10, scale=0)],
            [],
            [],
            [],
        ]
        # _estimated_segment_bytes raises ORA-942 → returns None
        cursor.execute.side_effect = [
            None,  # ALL_TAB_COLS
            _db_error(942),  # ALL_SEGMENTS → returns None
            None,  # ALL_TABLES
            None,  # ALL_EXTERNAL_TABLES probe
            None,  # ALL_TAB_PARTITIONS
            None,  # ALL_CONSTRAINTS PK
            None,  # ALL_CONSTRAINTS UK
        ]
        cursor.fetchone.side_effect = [
            _table_row(100, 50),  # ALL_TABLES: 100 rows * 50 avg_row_len = 5000
            None,  # ALL_EXTERNAL_TABLES probe
        ]
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.estimated_bytes == 5000

    def test_external_table_flagged(self) -> None:
        cursor = self._setup_cursor(external_row=(1,))
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.table_type == "EXTERNAL"

    def test_global_temporary_table_flagged(self) -> None:
        cursor = self._setup_cursor(
            table_stats=_table_row(0, None, temporary="Y"),
        )
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "T")
        assert meta.table_type == "GTT"

    def test_object_type_column_captured(self) -> None:
        cursor = self._setup_cursor(
            col_rows=[
                _col_row("ADDR", "ADDRESS_T", 1, data_type_owner="FINANCE"),
            ],
        )
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "FINANCE", "CUSTOMER_PROFILE")
        assert meta.columns[0].data_type_owner == "FINANCE"

    def test_column_and_table_comments_propagated(self) -> None:
        cursor = self._setup_cursor(
            col_rows=[
                _col_row("ID", "NUMBER", 1, precision=10, scale=0, comment="primary key"),
            ],
            table_stats=_table_row(0, None, comment="orders master table"),
        )
        conn = _make_conn(cursor)
        meta = discover_table_metadata(conn, "S", "ORDERS")
        assert meta.columns[0].comment == "primary key"
        assert meta.comment == "orders master table"
