"""Oracle metadata discovery."""

from __future__ import annotations

import logging

import oracledb

from oracle_dmp_converter.models import ColumnMetadata, PartitionMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)

ORACLE_MAINTAINED_SCHEMAS = frozenset(
    {
        "ANONYMOUS",
        "APPQOSSYS",
        "AUDSYS",
        "CTXSYS",
        "DBSFWUSER",
        "DBSNMP",
        "DIP",
        "DVF",
        "DVSYS",
        "GGSYS",
        "GSMADMIN_INTERNAL",
        "GSMCATUSER",
        "GSMUSER",
        "LBACSYS",
        "MDSYS",
        "OJVMSYS",
        "OLAPSYS",
        "ORDDATA",
        "ORDPLUGINS",
        "ORDSYS",
        "OUTLN",
        "REMOTE_SCHEDULER_AGENT",
        "SYS",
        "SYS$UMF",
        "SYSBACKUP",
        "SYSDG",
        "SYSKM",
        "SYSRAC",
        "SYSTEM",
        "WMSYS",
        "XDB",
        "XS$NULL",
    }
)


def _oracle_error_code(exc: Exception) -> int | None:
    if not isinstance(exc, oracledb.DatabaseError):
        return None
    error = exc.args[0]
    return getattr(error, "code", None)


def _estimated_segment_bytes(
    cursor: oracledb.Cursor,
    *,
    schema: str,
    table: str,
) -> int | None:
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
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   COLUMN_ID,
                   NULLABLE,
                   DATA_PRECISION,
                   DATA_SCALE,
                   CHAR_LENGTH
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
