"""Post-import staging-schema fixups.

These helpers operate on a single Oracle connection already opened against the
staging database and a resolved stage-schema name.  They handle the housekeeping
that Data Pump imports leave behind: disabling triggers, dropping VPD policies,
dematerialising materialised views, and widening BYTE-length string columns to
CHAR semantics.

External tables and global temporary tables are excluded from the table
listings these helpers operate over — modifying them in place either fails
(``ALTER TABLE`` is not valid against an external table) or is meaningless
(GTT rows are session-scoped and not present at fixup time).
"""

from __future__ import annotations

import logging

import oracledb

LOGGER = logging.getLogger(__name__)

# Subquery used by table-listing fixups to exclude external + temporary
# tables.  Kept here so every fixup applies the same filter.
_TABLE_FILTER_EXCLUDING_EXTERNAL_AND_GTT = """
    NOT EXISTS (
        SELECT 1 FROM ALL_EXTERNAL_TABLES x
        WHERE x.OWNER = :schema AND x.TABLE_NAME = t.TABLE_NAME
    )
    AND (t.TEMPORARY IS NULL OR t.TEMPORARY = 'N')
"""


def disable_foreign_keys(conn: oracledb.Connection, stage_schema: str) -> int:
    """Disable every enabled FOREIGN KEY constraint on tables in *stage_schema*.

    Why this is needed: when ``impdp`` re-imports data with
    ``TABLE_EXISTS_ACTION=TRUNCATE`` it tries to truncate each target table
    before loading.  Oracle refuses to truncate a table whose primary/unique
    key is referenced by an *enabled* foreign key on another table, raising
    ``ORA-02266``.  That triggers ``ORA-39120`` from Data Pump and the row
    data is skipped.  Reference-partitioned tables and multi-column-FK pairs
    in particular always hit this.  Disabling FKs in staging before convert
    lets impdp truncate and reload cleanly; the staging schema is not a
    referential-integrity authority, so this is safe.

    Returns the number of constraints successfully disabled.  Missing /
    already-disabled constraints are skipped silently.
    """
    LOGGER.info("Disabling FOREIGN KEY constraints on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        # Join through ALL_TABLES so we can apply the external/GTT filter
        # — Oracle's ALL_CONSTRAINTS doesn't expose TEMPORARY directly.
        cursor.execute(
            f"""
            SELECT c.TABLE_NAME, c.CONSTRAINT_NAME
            FROM ALL_CONSTRAINTS c
            JOIN ALL_TABLES t
                ON t.OWNER = c.OWNER AND t.TABLE_NAME = c.TABLE_NAME
            WHERE c.OWNER = :schema
              AND c.CONSTRAINT_TYPE = 'R'
              AND c.STATUS = 'ENABLED'
              AND {_TABLE_FILTER_EXCLUDING_EXTERNAL_AND_GTT}
            """,
            schema=stage_schema,
        )
        constraints = cursor.fetchall()
    disabled = 0
    for table_name, constraint_name in constraints:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'ALTER TABLE "{stage_schema}"."{table_name}"'
                    f' DISABLE CONSTRAINT "{constraint_name}"'
                )
        except oracledb.DatabaseError as exc:
            ora_code = exc.args[0].code if exc.args else None
            # ORA-02430: cannot enable / disable constraint - does not exist
            # ORA-02431: cannot disable constraint - does not exist
            if ora_code in {2430, 2431}:
                LOGGER.debug(
                    "Constraint %s.%s.%s no longer exists, skipping",
                    stage_schema,
                    table_name,
                    constraint_name,
                )
            else:
                LOGGER.warning(
                    "Failed to disable FK %s.%s.%s: %s",
                    stage_schema,
                    table_name,
                    constraint_name,
                    exc,
                )
            continue
        disabled += 1
        LOGGER.debug("Disabled FK %s.%s.%s", stage_schema, table_name, constraint_name)
    return disabled


def disable_triggers(conn: oracledb.Connection, stage_schema: str) -> int:
    """Disable every trigger on tables in *stage_schema*.

    Returns the number of triggers successfully disabled.  Missing triggers
    (ORA-04080) are skipped silently; any other error is logged as a warning
    and processing continues.
    """
    LOGGER.info("Disabling triggers on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        # Only walk triggers whose target table is a real heap/IOT table —
        # external tables can't have data triggers anyway, and GTT
        # triggers fire at session scope only, so dropping them gains
        # nothing.  The join also keeps us from hitting MV-log triggers.
        cursor.execute(
            f"""
            SELECT g.TRIGGER_NAME
            FROM ALL_TRIGGERS g
            JOIN ALL_TABLES t
                ON t.OWNER = g.TABLE_OWNER AND t.TABLE_NAME = g.TABLE_NAME
            WHERE g.OWNER = :schema
              AND {_TABLE_FILTER_EXCLUDING_EXTERNAL_AND_GTT}
            """,
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


def drop_vpd_policy_functions(conn: oracledb.Connection, stage_schema: str) -> int:
    """Drop PL/SQL functions referenced by VPD policies in *stage_schema*.

    VPD policies reference a PL/SQL function that Oracle invokes to produce a
    predicate.  When a legacy ``imp`` re-imports metadata, it tries to
    re-attach the policy — and if the function still exists in the staging
    schema the ``ADD_POLICY`` call succeeds, but the function may reference
    objects that no longer resolve correctly, leading to ``ORA-28100`` at
    query time.  Dropping the functions prevents this.

    This must run **before** :func:`drop_vpd_policies`: function discovery
    reads ``ALL_POLICIES`` to learn which functions a policy references, so the
    policies must still exist when this runs.  (Dropping a function while its
    policy is still attached is permitted; the caller drops the policies
    immediately afterwards, and no query touches the protected table in
    between.)

    Only standalone functions owned by *stage_schema* are dropped (i.e.
    ``PF_OWNER = stage_schema`` and ``PACKAGE IS NULL``).  Packaged policy
    functions are left alone — dropping an entire package for a single policy
    function could break other code.

    Returns the number of functions successfully dropped.  ``ORA-04043``
    (object does not exist) is silently skipped; any other error is logged
    as a warning and processing continues.
    """
    LOGGER.info("Dropping VPD policy functions on staging schema %s", stage_schema)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT FUNCTION
            FROM ALL_POLICIES
            WHERE OBJECT_OWNER = :schema
              AND PF_OWNER = :schema
              AND PACKAGE IS NULL
              AND FUNCTION IS NOT NULL
            """,
            schema=stage_schema,
        )
        functions = [row[0] for row in cursor.fetchall()]
    dropped = 0
    for func_name in functions:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f'DROP FUNCTION "{stage_schema}"."{func_name}"')
        except oracledb.DatabaseError as exc:
            ora_code = exc.args[0].code if exc.args else None
            if ora_code == 4043:  # ORA-04043: object does not exist
                LOGGER.debug(
                    "VPD policy function %s.%s no longer exists, skipping",
                    stage_schema,
                    func_name,
                )
            else:
                LOGGER.warning(
                    "Failed to drop VPD policy function %s.%s: %s",
                    stage_schema,
                    func_name,
                    exc,
                )
            continue
        dropped += 1
        LOGGER.debug("Dropped VPD policy function %s.%s", stage_schema, func_name)
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
        # External tables don't accept ``ALTER TABLE … MODIFY`` and GTTs
        # don't benefit from semantic widening, so join through
        # ALL_TABLES with the shared external/GTT filter.  Virtual-column
        # and partition-key exclusions are preserved.
        cursor.execute(
            f"""
            SELECT tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.CHAR_LENGTH
            FROM ALL_TAB_COLUMNS tc
            JOIN ALL_TABLES t
                ON t.OWNER = tc.OWNER AND t.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.OWNER = :schema
              AND tc.DATA_TYPE IN ('VARCHAR2', 'CHAR')
              AND tc.CHAR_USED = 'B'
              AND {_TABLE_FILTER_EXCLUDING_EXTERNAL_AND_GTT}
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
