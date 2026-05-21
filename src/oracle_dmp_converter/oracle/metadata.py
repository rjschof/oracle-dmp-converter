"""Oracle metadata discovery."""

from __future__ import annotations

import logging

import oracledb

from oracle_dmp_converter.models import ColumnMetadata, PartitionMetadata, TableMetadata
from oracle_dmp_converter.oracle.conn import _oracle_error_code

LOGGER = logging.getLogger(__name__)


def _estimated_segment_bytes(
    cursor: oracledb.Cursor,
    *,
    schema: str,
    table: str,
) -> int | None:
    """Query ``ALL_SEGMENTS`` for the total allocated bytes of *table*.

    Returns ``None`` when the dictionary view is unavailable (``ORA-00942``),
    which can happen in restricted staging schemas.  Any other database error
    is re-raised.

    Args:
        cursor: Open Oracle cursor from an active connection.
        schema: Table owner.
        table: Table name.

    Returns:
        Total allocated segment bytes, or ``None`` if unavailable.
    """
    try:
        cursor.execute(
            """
            SELECT NVL(SUM(BYTES), 0)
            FROM ALL_SEGMENTS
            WHERE OWNER = :owner AND SEGMENT_NAME = :table_name
            """,
            owner=schema,
            table_name=table,
        )
        return int(cursor.fetchone()[0] or 0)
    except Exception as exc:  # noqa: BLE001 - dictionary view availability varies by image/user.
        if _oracle_error_code(exc) != 942:
            raise
        return None


def discover_table_metadata(conn: oracledb.Connection, schema: str, table: str) -> TableMetadata:
    """Collect full structural metadata for *schema*.*table* from Oracle catalogs.

    Queries the following dictionary views in a single cursor session:

    * ``ALL_TAB_COLUMNS`` — column names, types, precision/scale, nullability.
    * ``ALL_SEGMENTS`` — estimated table size in bytes.
    * ``ALL_TABLES`` — statistics-based row count and average row length.
    * ``ALL_TAB_PARTITIONS`` — partition names and positions.
    * ``ALL_CONSTRAINTS`` + ``ALL_CONS_COLUMNS`` — primary key columns.
    * ``ALL_CONSTRAINTS`` + ``ALL_CONS_COLUMNS`` — unique key columns.

    Args:
        conn: Active Oracle connection with ``SELECT`` privilege on the
            ``ALL_*`` dictionary views.
        schema: Table owner (exact case, as stored in the dictionary).
        table: Table name (exact case, as stored in the dictionary).

    Returns:
        A fully populated :class:`~oracle_dmp_converter.models.TableMetadata`
        instance.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   COLUMN_ID,
                   NULLABLE,
                   DATA_PRECISION,
                   DATA_SCALE,
                   CHAR_LENGTH,
                   CHAR_USED
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
            """,
            owner=schema,
            table_name=table,
        )
        columns = tuple(
            ColumnMetadata(
                name=row[0],
                data_type=row[1],
                ordinal=int(row[2]),
                nullable=row[3] == "Y",
                data_precision=row[4],
                data_scale=row[5],
                char_length=row[6],
                char_used=row[7],
            )
            for row in cursor.fetchall()
        )

        estimated_bytes = _estimated_segment_bytes(cursor, schema=schema, table=table)

        cursor.execute(
            """
            SELECT NUM_ROWS, AVG_ROW_LEN
            FROM ALL_TABLES
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            """,
            owner=schema,
            table_name=table,
        )
        table_stats = cursor.fetchone()
        row_count = None
        if table_stats and table_stats[0] is not None:
            row_count = int(table_stats[0])
            if (estimated_bytes is None or estimated_bytes == 0) and table_stats[1] is not None:
                estimated_bytes = row_count * int(table_stats[1])

        cursor.execute(
            """
            SELECT PARTITION_NAME, PARTITION_POSITION
            FROM ALL_TAB_PARTITIONS
            WHERE TABLE_OWNER = :owner AND TABLE_NAME = :table_name
            ORDER BY PARTITION_POSITION
            """,
            owner=schema,
            table_name=table,
        )
        partitions = tuple(
            PartitionMetadata(name=row[0], position=int(row[1])) for row in cursor.fetchall()
        )

        cursor.execute(
            """
            SELECT c.COLUMN_NAME
            FROM ALL_CONSTRAINTS k
            JOIN ALL_CONS_COLUMNS c
              ON c.OWNER = k.OWNER
             AND c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE k.OWNER = :owner
              AND k.TABLE_NAME = :table_name
              AND k.CONSTRAINT_TYPE = 'P'
            ORDER BY c.POSITION
            """,
            owner=schema,
            table_name=table,
        )
        primary_key = tuple(row[0] for row in cursor.fetchall())

        cursor.execute(
            """
            SELECT k.CONSTRAINT_NAME, c.COLUMN_NAME
            FROM ALL_CONSTRAINTS k
            JOIN ALL_CONS_COLUMNS c
              ON c.OWNER = k.OWNER
             AND c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE k.OWNER = :owner
              AND k.TABLE_NAME = :table_name
              AND k.CONSTRAINT_TYPE = 'U'
            ORDER BY k.CONSTRAINT_NAME, c.POSITION
            """,
            owner=schema,
            table_name=table,
        )
        unique_by_name: dict[str, list[str]] = {}
        for constraint_name, column_name in cursor.fetchall():
            unique_by_name.setdefault(constraint_name, []).append(column_name)

    return TableMetadata(
        schema=schema,
        name=table,
        columns=columns,
        estimated_bytes=estimated_bytes,
        row_count=row_count,
        partitions=partitions,
        primary_key=primary_key,
        unique_keys=tuple(tuple(value) for value in unique_by_name.values()),
    )
