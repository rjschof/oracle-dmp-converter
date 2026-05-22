"""Post-import staging-schema fixups.

These helpers operate on a single Oracle connection already opened against the
staging database and a resolved stage-schema name.  They handle the housekeeping
that Data Pump imports leave behind: disabling triggers, dropping VPD policies,
dematerialising materialised views, and widening BYTE-length string columns to
CHAR semantics.
"""

from __future__ import annotations

import logging

import oracledb

LOGGER = logging.getLogger(__name__)


def disable_triggers(conn: oracledb.Connection, stage_schema: str) -> int:
    """Disable every trigger on tables in *stage_schema*.

    Returns the number of triggers successfully disabled.  Missing triggers
    (ORA-04080) are skipped silently; any other error is logged as a warning
    and processing continues.
    """
    LOGGER.info("Disabling triggers on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT TRIGGER_NAME FROM ALL_TRIGGERS WHERE OWNER = :schema",
            schema=stage_schema,
        )
        triggers = [row[0] for row in cursor.fetchall()]
    disabled = 0
    for trigger_name in triggers:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f'ALTER TRIGGER "{stage_schema}"."{trigger_name}" DISABLE')
        except oracledb.DatabaseError as exc:
            ora_code = exc.args[0].code if exc.args else None
            if ora_code == 4080:  # ORA-04080: trigger does not exist
                LOGGER.debug("Trigger %s.%s no longer exists, skipping", stage_schema, trigger_name)
            else:
                LOGGER.warning(
                    "Failed to disable trigger %s.%s: %s", stage_schema, trigger_name, exc
                )
            continue
        disabled += 1
        LOGGER.debug("Disabled trigger %s.%s", stage_schema, trigger_name)
    return disabled


def drop_vpd_policies(conn: oracledb.Connection, stage_schema: str) -> int:
    """Drop every DBMS_RLS policy on tables in *stage_schema*.

    Returns the number of policies successfully dropped.  Missing policies
    (ORA-28102) are skipped silently; any other error is logged as a warning
    and processing continues.
    """
    LOGGER.info("Dropping VPD policies on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT OBJECT_NAME, POLICY_NAME, POLICY_GROUP
            FROM ALL_POLICIES
            WHERE OBJECT_OWNER = :schema
            """,
            schema=stage_schema,
        )
        policies = cursor.fetchall()
    dropped = 0
    for object_name, policy_name, policy_group in policies:
        try:
            with conn.cursor() as cursor:
                if policy_group and policy_group != "SYS_DEFAULT":
                    cursor.execute(
                        """
                    BEGIN
                        DBMS_RLS.DROP_GROUPED_POLICY(
                            object_schema => :schema,
                            object_name   => :obj,
                            policy_group  => :grp,
                            policy_name   => :pol
                        );
                    END;
                    """,
                        schema=stage_schema,
                        obj=object_name,
                        grp=policy_group,
                        pol=policy_name,
                    )
                else:
                    cursor.execute(
                        """
                    BEGIN
                        DBMS_RLS.DROP_POLICY(
                            object_schema => :schema,
                            object_name   => :obj,
                            policy_name   => :pol
                        );
                    END;
                    """,
                        schema=stage_schema,
                        obj=object_name,
                        pol=policy_name,
                    )
        except oracledb.DatabaseError as exc:
            ora_code = exc.args[0].code if exc.args else None
            if ora_code == 28102:  # ORA-28102: policy does not exist
                LOGGER.debug(
                    "VPD policy %s on %s.%s no longer exists, skipping",
                    policy_name,
                    stage_schema,
                    object_name,
                )
            else:
                LOGGER.warning(
                    "Failed to drop VPD policy %s on %s.%s: %s",
                    policy_name,
                    stage_schema,
                    object_name,
                    exc,
                )
            continue
        dropped += 1
        LOGGER.debug("Dropped VPD policy %s on %s.%s", policy_name, stage_schema, object_name)
    return dropped


def dematerialize_mviews(conn: oracledb.Connection, stage_schema: str) -> None:
    """Replace materialised views in *stage_schema* with plain heap tables."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT MVIEW_NAME FROM ALL_MVIEWS WHERE OWNER = :schema",
            schema=stage_schema,
        )
        mviews = [row[0] for row in cursor.fetchall()]
    if not mviews:
        return
    LOGGER.info(
        "Converting %d materialized view(s) to plain tables in staging schema %s: %s",
        len(mviews),
        stage_schema,
        ", ".join(mviews),
    )
    for mview_name in mviews:
        tmp_name = f"{mview_name[:120]}_$TMP"
        with conn.cursor() as cursor:
            cursor.execute(
                f'CREATE TABLE "{stage_schema}"."{tmp_name}" AS '
                f'SELECT * FROM "{stage_schema}"."{mview_name}" WHERE 1=0'
            )
        with conn.cursor() as cursor:
            cursor.execute(f'DROP MATERIALIZED VIEW "{stage_schema}"."{mview_name}"')
        with conn.cursor() as cursor:
            cursor.execute(f'ALTER TABLE "{stage_schema}"."{tmp_name}" RENAME TO "{mview_name}"')
        LOGGER.debug("Converted materialized view %s.%s to plain table", stage_schema, mview_name)


def apply_byte_to_char(conn: oracledb.Connection, stage_schema: str) -> int:
    """Widen ``BYTE``-length string columns in *stage_schema* to ``CHAR`` semantics.

    Skips virtual columns and partition/subpartition key columns, which Oracle
    refuses to modify in place.

    Returns the number of columns successfully modified.  Any failure on an
    individual column is logged as a warning and processing continues.
    """
    LOGGER.info("Applying BYTE→CHAR column adjustments on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.CHAR_LENGTH
            FROM ALL_TAB_COLUMNS tc
            WHERE tc.OWNER = :schema
              AND tc.DATA_TYPE IN ('VARCHAR2', 'CHAR')
              AND tc.CHAR_USED = 'B'
              AND NOT EXISTS (
                  SELECT 1 FROM ALL_TABLE_VIRTUAL_COLUMNS vc
                  WHERE vc.TABLE_OWNER = :schema
                    AND vc.TABLE_NAME = tc.TABLE_NAME
                    AND vc.VIRTUAL_COLUMN_NAME = tc.COLUMN_NAME
              )
              AND NOT EXISTS (
                  SELECT 1 FROM ALL_PART_KEY_COLUMNS pk
                  WHERE pk.OWNER = :schema
                    AND pk.NAME = tc.TABLE_NAME
                    AND pk.COLUMN_NAME = tc.COLUMN_NAME
                    AND pk.OBJECT_TYPE = 'TABLE'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM ALL_SUBPART_KEY_COLUMNS sk
                  WHERE sk.OWNER = :schema
                    AND sk.NAME = tc.TABLE_NAME
                    AND sk.COLUMN_NAME = tc.COLUMN_NAME
                    AND sk.OBJECT_TYPE = 'TABLE'
              )
            """,
            schema=stage_schema,
        )
        byte_columns = cursor.fetchall()
    modified = 0
    for table_name, column_name, data_type, char_length in byte_columns:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'ALTER TABLE "{stage_schema}"."{table_name}" '
                    f'MODIFY "{column_name}" {data_type}({char_length} CHAR)'
                )
        except oracledb.DatabaseError as exc:
            LOGGER.warning(
                "Failed to apply BYTE→CHAR on %s.%s.%s: %s",
                stage_schema,
                table_name,
                column_name,
                exc,
            )
            continue
        modified += 1
        LOGGER.debug(
            "Adjusted %s.%s.%s to %s(%d CHAR)",
            stage_schema,
            table_name,
            column_name,
            data_type,
            char_length,
        )
    return modified
