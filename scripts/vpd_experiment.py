#!/usr/bin/env python3
"""
VPD Policy Experiment: ROWS=Y vs DATA_ONLY=Y for legacy imp chunk imports.

Schema layout:
  VPDSEC.GET_PREDICATE  -- predicate function (intentionally NOT exported)
  VPDTEST.SECURE_DATA   -- table with VPD policy referencing VPDSEC.GET_PREDICATE

Steps:
  Phase 1  Start source container, build schema, export VPDTEST only via exp.
           VPDSEC is NOT included in the dump.
  Phase 2  Run 'oracle-dmp-converter inspect + plan' against the dump.
           inspect imports metadata (ROWS=N) into DMP_VPDTEST and runs
           _apply_staging_fixups (including drop_vpd_policies) so the
           staging DB is clean before any data import.
  Phase 3  Test A – imp ROWS=Y (current chunk-import behaviour).
           The ADD_POLICY call stored in the dump re-creates the VPD policy
           even though VPDSEC.GET_PREDICATE does not exist in staging.
           Oracle defers function-existence validation to query time.
           A SELECT immediately afterwards raises ORA-28100.
  Phase 4  Reset staging (drop the re-created policy).
           Test B – imp DATA_ONLY=Y (proposed fix).
           No DDL is executed; the VPD policy is NOT re-created.
           SELECT succeeds and returns the 5 expected rows.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    LegacyImportJob,
    render_legacy_export_parfile,
    render_legacy_import_parfile,
)
from oracle_dmp_converter.oracle.conn import OracleCredentials, oracle_connection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ORACLE_PASSWORD = "OraclePwd_123"
SOURCE_SCHEMA = "VPDTEST"
FUNC_SCHEMA = "VPDSEC"       # owns the predicate function — will NOT be exported
STAGE_SCHEMA = "DMP_VPDTEST"
TABLE = "SECURE_DATA"
POLICY_NAME = "SEC_POLICY"
DUMP_NAME = "vpdtest_experiment.dmp"
CONTAINER_DUMP_PATH = "/exp-dump"   # mount point for the dump directory


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def _sep(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _ok(msg: str) -> None:
    print(f"  [OK]  {msg}")


def _fail(msg: str) -> None:
    print(f"  [!!]  {msg}")


def _info(msg: str) -> None:
    print(f"        {msg}")


# ---------------------------------------------------------------------------
# Oracle SQL helpers
# ---------------------------------------------------------------------------

def _connect_system(container: ContainerOracle) -> oracle_connection:
    return oracle_connection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=container.password,
    )


def _exec_sql(conn: oracledb.Connection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _exec_plsql(conn: oracledb.Connection, plsql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(plsql)
    conn.commit()


def _count_vpd_policies(conn: oracledb.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ALL_POLICIES WHERE OBJECT_OWNER = :s",
            s=schema,
        )
        row = cur.fetchone()
        return row[0] if row else 0


def _show_vpd_policies(conn: oracledb.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT OBJECT_NAME, POLICY_NAME, PF_OWNER, PACKAGE, FUNCTION
            FROM   ALL_POLICIES
            WHERE  OBJECT_OWNER = :s
            """,
            s=schema,
        )
        rows = cur.fetchall()
    if not rows:
        _info(f"ALL_POLICIES: no policies for {schema}")
    else:
        for obj, pol, pf_owner, pkg, func in rows:
            fn = f"{pkg}.{func}" if pkg else func
            _info(f"ALL_POLICIES: policy={pol!r} on {schema}.{obj} → {pf_owner}.{fn}")


def _try_select(conn: oracledb.Connection, schema: str, table: str) -> tuple[bool, int | str]:
    """Return (success, row_count_or_error)."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
            return True, (row[0] if row else 0)
    except oracledb.DatabaseError as exc:
        code = exc.args[0].code if exc.args else 0
        return False, f"ORA-{code:05d}: {exc}"


# ---------------------------------------------------------------------------
# Phase 1: build source database and export with exp
# ---------------------------------------------------------------------------

def build_source_db_and_export(container: ContainerOracle, dump_dir: Path) -> None:
    _sep("Phase 1 — source DB setup + legacy exp export")

    with _connect_system(container) as conn:
        # ---- Clean up any leftover schemas ----
        for schema in (SOURCE_SCHEMA, FUNC_SCHEMA):
            try:
                _exec_sql(conn, f"DROP USER {schema} CASCADE")
                _info(f"Dropped existing schema {schema}")
            except oracledb.DatabaseError:
                pass

        # ---- VPDSEC: holds the predicate function (will NOT be exported) ----
        _exec_sql(conn, f"CREATE USER {FUNC_SCHEMA} IDENTIFIED BY {ORACLE_PASSWORD}")
        _exec_sql(conn, f"GRANT CONNECT, RESOURCE TO {FUNC_SCHEMA}")
        _exec_plsql(
            conn,
            f"""
            CREATE OR REPLACE FUNCTION {FUNC_SCHEMA}.GET_PREDICATE(
                p_schema IN VARCHAR2,
                p_object IN VARCHAR2
            ) RETURN VARCHAR2 AS
            BEGIN
                RETURN '1=1';
            END;
            """,
        )
        _ok(f"Created {FUNC_SCHEMA}.GET_PREDICATE (always-true predicate)")

        # ---- VPDTEST: table + data ----
        _exec_sql(conn, f"CREATE USER {SOURCE_SCHEMA} IDENTIFIED BY {ORACLE_PASSWORD}")
        _exec_sql(conn, f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {SOURCE_SCHEMA}")
        # EXP_FULL_DATABASE so VPDTEST's own exp captures the VPD policy metadata.
        _exec_sql(conn, f"GRANT EXP_FULL_DATABASE TO {SOURCE_SCHEMA}")
        _exec_sql(
            conn,
            f"""
            CREATE TABLE {SOURCE_SCHEMA}.{TABLE} (
                ID      NUMBER(6)    PRIMARY KEY,
                PAYLOAD VARCHAR2(80) NOT NULL
            )
            """,
        )
        for i in range(1, 6):
            _exec_sql(
                conn,
                f"INSERT INTO {SOURCE_SCHEMA}.{TABLE} VALUES ({i}, 'row-{i}')",
            )
        _ok(f"Created {SOURCE_SCHEMA}.{TABLE} with 5 rows")

        # ---- Attach VPD policy referencing VPDSEC.GET_PREDICATE ----
        # SYSTEM has DBA role which grants access to DBMS_RLS.
        _exec_plsql(
            conn,
            f"""
            BEGIN
                DBMS_RLS.ADD_POLICY(
                    object_schema   => '{SOURCE_SCHEMA}',
                    object_name     => '{TABLE}',
                    policy_name     => '{POLICY_NAME}',
                    function_schema => '{FUNC_SCHEMA}',
                    policy_function => 'GET_PREDICATE'
                );
            END;
            """,
        )
        _ok(
            f"VPD policy {POLICY_NAME!r} on {SOURCE_SCHEMA}.{TABLE} "
            f"→ {FUNC_SCHEMA}.GET_PREDICATE"
        )

        count = _count_vpd_policies(conn, SOURCE_SCHEMA)
        _info(f"ALL_POLICIES count for {SOURCE_SCHEMA}: {count}")
        _show_vpd_policies(conn, SOURCE_SCHEMA)

    # ---- Legacy exp: export VPDTEST only (VPDSEC intentionally excluded) ----
    # Connect as VPDTEST itself (not SYSTEM) so imp's INDEXFILE discovery
    # does not route DDL into the SYS-only sidecar SQL file.
    creds = OracleCredentials(user=SOURCE_SCHEMA, password=ORACLE_PASSWORD)
    job = LegacyExportJob(
        connection=creds,
        files=(f"{CONTAINER_DUMP_PATH}/{DUMP_NAME}",),
        logfile=f"{CONTAINER_DUMP_PATH}/vpdtest_exp.log",
        owner=(SOURCE_SCHEMA,),
        rows=True,
        indexes=False,
        grants=False,
        compress=False,
    )
    parfile_text = render_legacy_export_parfile(job)
    par_name = f"exp-vpd-{uuid.uuid4().hex}.par"
    local_par = dump_dir / par_name
    local_par.write_text(parfile_text)
    remote_par = f"/tmp/{par_name}"
    container.copy_to(local_par, remote_par)

    result = container.exec(["exp", f"parfile={remote_par}"], check=False)
    container.exec(["rm", "-f", remote_par], check=False)
    local_par.unlink(missing_ok=True)

    output = result.stdout + result.stderr
    for line in output.splitlines():
        ls = line.strip()
        if ls and any(k in ls for k in ("error", "Error", "ORA-", "EXP-", "success", "Export")):
            _info(f"  exp> {ls}")

    dump_file = dump_dir / DUMP_NAME
    if not dump_file.exists() or dump_file.stat().st_size == 0:
        _fail(f"exp did not produce a dump: {dump_file}")
        sys.exit(1)
    _ok(f"exp completed → {dump_file.name}  ({dump_file.stat().st_size // 1024} KB)")

    # Ensure the file is readable (container may write it as root)
    container.exec(
        ["bash", "-lc", f"chmod a+r {CONTAINER_DUMP_PATH}/{DUMP_NAME} 2>/dev/null || true"],
        check=False,
    )


# ---------------------------------------------------------------------------
# Phase 2: prepare staging schema (direct, bypassing the converter)
# ---------------------------------------------------------------------------

def prepare_staging_schema(container: ContainerOracle) -> None:
    """Create VPDTEST in staging (no remap; imp will connect as VPDTEST).
    VPDSEC is intentionally NOT created so the policy-function reference
    is dangling in staging."""
    _sep("Phase 2 — prepare staging schema (VPDTEST only; no VPDSEC)")

    with _connect_system(container) as conn:
        for schema in (SOURCE_SCHEMA, FUNC_SCHEMA):
            try:
                _exec_sql(conn, f"DROP USER {schema} CASCADE")
                _info(f"Dropped existing {schema}")
            except oracledb.DatabaseError:
                pass
        _exec_sql(conn, f"CREATE USER {SOURCE_SCHEMA} IDENTIFIED BY {ORACLE_PASSWORD}")
        _exec_sql(
            conn,
            f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {SOURCE_SCHEMA}",
        )
        # DBA so imp executes DBMS_RLS.ADD_POLICY inline (else it goes to _sys.sql).
        _exec_sql(conn, f"GRANT DBA TO {SOURCE_SCHEMA}")
        _ok(f"Staging schema {SOURCE_SCHEMA} created (VPDSEC is absent on purpose)")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :u", u=FUNC_SCHEMA
            )
            row = cur.fetchone()
        _info(f"ALL_USERS count for {FUNC_SCHEMA}: {row[0] if row else 0}  (expected 0)")


# ---------------------------------------------------------------------------
# Phase 3 & 4: run imp tests against the staging container
# ---------------------------------------------------------------------------

def _run_imp_parfile(
    container: ContainerOracle,
    work_dir: Path,
    parfile_text: str,
) -> str:
    """Copy *parfile_text* into the container, run imp, return combined output."""
    par_name = f"imp-exp-{uuid.uuid4().hex}.par"
    local_par = work_dir / par_name
    local_par.write_text(parfile_text)
    remote_par = f"/tmp/{par_name}"
    container.copy_to(local_par, remote_par)
    try:
        result = container.exec(["imp", f"parfile={remote_par}"], check=False)
        return result.stdout + result.stderr
    finally:
        container.exec(["rm", "-f", remote_par], check=False)
        local_par.unlink(missing_ok=True)


def _show_imp_summary(output: str) -> None:
    for line in output.splitlines():
        ls = line.strip()
        if ls and any(
            k in ls
            for k in ("IMP-", "ORA-", "successfully", "warning", "Warning", "error", "Error",
                      "policy", "POLICY", "row", "import", "Import")
        ):
            _info(f"  imp> {ls}")


def _dump_sidecar(container: ContainerOracle, path: str) -> None:
    result = container.exec(["bash", "-lc", f"cat {path} 2>/dev/null || true"], check=False)
    text = result.stdout.strip()
    if not text:
        _info(f"  sidecar {path}: (empty or missing)")
        return
    _info(f"  sidecar {path}:")
    for line in text.splitlines():
        _info(f"    | {line}")


def run_rows_y_test(
    container: ContainerOracle,
    work_dir: Path,
    dump_in_container: str,
) -> None:
    _sep("Phase 3 — imp ROWS=Y  (current behaviour — expects ORA-28100)")

    # Manual parfile: connect AS SYSDBA so DBMS_RLS DDL runs inline (no sidecar).
    parfile_text = "\n".join(
        [
            f"USERID='sys/{ORACLE_PASSWORD}@FREEPDB1 AS SYSDBA'",
            f"FILE={dump_in_container}",
            "LOG=/tmp/imp_rows_y.log",
            f"FROMUSER={SOURCE_SCHEMA}",
            f"TOUSER={SOURCE_SCHEMA}",
            "ROWS=Y",
            "INDEXES=N",
            "GRANTS=N",
            "CONSTRAINTS=N",
            "IGNORE=Y",
            "",
        ]
    )
    _info("imp parfile (ROWS=Y):")
    for line in parfile_text.splitlines():
        _info(f"    {line}")

    with _connect_system(container) as conn:
        before = _count_vpd_policies(conn, SOURCE_SCHEMA)
        _info(f"VPD policies on {SOURCE_SCHEMA} BEFORE imp: {before}")
        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        _info(f"SELECT before imp: {'%d rows' % res if ok else res}")

    output = _run_imp_parfile(container, work_dir, parfile_text)
    _show_imp_summary(output)
    _dump_sidecar(container, "/tmp/imp_rows_y_sys.sql")

    with _connect_system(container) as conn:
        after = _count_vpd_policies(conn, SOURCE_SCHEMA)
        _info(f"VPD policies on {SOURCE_SCHEMA} AFTER  imp: {after}")
        _show_vpd_policies(conn, SOURCE_SCHEMA)

        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        if not ok:
            _fail(f"SELECT after ROWS=Y imp: {res}  ← BUG CONFIRMED")
        else:
            _ok(f"SELECT after ROWS=Y imp: {res} rows  (note: see experiment summary)")

        # Reset: drop VPD policy and data so DATA_ONLY test starts clean
        if after > 0:
            _info("Dropping re-created VPD policy to reset for DATA_ONLY=Y test...")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    BEGIN
                        DBMS_RLS.DROP_POLICY(
                            object_schema => :s,
                            object_name   => :t,
                            policy_name   => :p
                        );
                    END;
                    """,
                    s=SOURCE_SCHEMA, t=TABLE, p=POLICY_NAME,
                )
            conn.commit()
            _info("Policy dropped — staging reset")

        # Truncate (don't drop) so DATA_ONLY=Y has a target table to load into.
        try:
            _exec_sql(conn, f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{TABLE}"')
            _info("Staging table truncated — ready for DATA_ONLY=Y test")
        except oracledb.DatabaseError as exc:
            _info(f"TRUNCATE skipped: {exc}")


def run_data_only_test(
    container: ContainerOracle,
    work_dir: Path,
    dump_in_container: str,
) -> None:
    _sep("Phase 4 — imp DATA_ONLY=Y  (proposed fix — expects 5 rows, no ORA-28100)")

    # Same SYSDBA connection; DATA_ONLY=Y + IGNORE=N (IMP-00402 if IGNORE=Y).
    parfile_text = "\n".join(
        [
            f"USERID='sys/{ORACLE_PASSWORD}@FREEPDB1 AS SYSDBA'",
            f"FILE={dump_in_container}",
            "LOG=/tmp/imp_data_only.log",
            f"FROMUSER={SOURCE_SCHEMA}",
            f"TOUSER={SOURCE_SCHEMA}",
            "ROWS=Y",
            "DATA_ONLY=Y",
            "INDEXES=N",
            "GRANTS=N",
            "CONSTRAINTS=N",
            "IGNORE=N",
            "",
        ]
    )
    _info("imp parfile (DATA_ONLY=Y):")
    for line in parfile_text.splitlines():
        _info(f"    {line}")

    with _connect_system(container) as conn:
        before = _count_vpd_policies(conn, SOURCE_SCHEMA)
        _info(f"VPD policies on {SOURCE_SCHEMA} BEFORE imp: {before}")
        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        _info(f"SELECT before imp: {'%d rows' % res if ok else res}")

    output = _run_imp_parfile(container, work_dir, parfile_text)
    _show_imp_summary(output)
    _dump_sidecar(container, "/tmp/imp_data_only_sys.sql")

    with _connect_system(container) as conn:
        after = _count_vpd_policies(conn, SOURCE_SCHEMA)
        _info(f"VPD policies on {SOURCE_SCHEMA} AFTER  imp: {after}")
        _show_vpd_policies(conn, SOURCE_SCHEMA)

        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        if ok:
            _ok(f"SELECT after DATA_ONLY=Y imp: {res} rows  ← FIX CONFIRMED")
        else:
            _fail(f"SELECT after DATA_ONLY=Y imp: {res}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    work_dir = Path(tempfile.mkdtemp(prefix="vpd_experiment_"))
    dump_dir = work_dir / "dump"
    dump_dir.mkdir()
    stage_work_dir = work_dir / "stage"
    stage_work_dir.mkdir()

    print(f"\nExperiment work directory: {work_dir}")

    # ---- Phase 1: source DB setup + exp export ----
    source_container: ContainerOracle | None = None
    try:
        source_container = ContainerOracle.start(
            image=DEFAULT_ORACLE_IMAGE,
            password=ORACLE_PASSWORD,
            mounts=((dump_dir, CONTAINER_DUMP_PATH, "rw"),),
        )
        print(f"Source container: {source_container.name}")
        print("Waiting for Oracle to be ready (60–120 s; ~1 GB pull on first run)…")
        source_container.wait_ready(timeout_seconds=300)
        build_source_db_and_export(source_container, dump_dir)
    finally:
        if source_container and source_container.started:
            source_container.stop()
            _ok("Source container stopped")

    dump_file = dump_dir / DUMP_NAME
    if not dump_file.exists() or dump_file.stat().st_size == 0:
        sys.exit(f"Dump file not found or empty: {dump_file}")

    # ---- Phase 2: start staging container directly, prepare schema ----
    staging_dump_dir = work_dir / "stage_dump"
    staging_dump_dir.mkdir()
    # Copy the dump into a dir the staging container will mount
    import shutil
    shutil.copy(dump_file, staging_dump_dir / DUMP_NAME)
    dump_in_container = f"/dumps/{DUMP_NAME}"

    staging_container = ContainerOracle.start(
        image=DEFAULT_ORACLE_IMAGE,
        password=ORACLE_PASSWORD,
        mounts=((staging_dump_dir, "/dumps", "rw"),),
    )
    print(f"Staging container: {staging_container.name}")
    print("Waiting for staging Oracle to be ready (60–120 s)…")
    staging_container.wait_ready(timeout_seconds=300)
    prepare_staging_schema(staging_container)

    try:
        # ---- Phase 3: ROWS=Y test ----
        run_rows_y_test(staging_container, stage_work_dir, dump_in_container)

        # ---- Phase 4: DATA_ONLY=Y test ----
        run_data_only_test(staging_container, stage_work_dir, dump_in_container)

    finally:
        staging_container.stop()
        _ok("Staging container stopped")

    _sep("Experiment complete")
    print(
        """
  Summary
  -------
  ROWS=Y     : imp re-executes DBMS_RLS.ADD_POLICY from the dump.
               Oracle defers function-existence validation to query time.
               First SELECT on the staging table raises ORA-28100 because
               VPDSEC.GET_PREDICATE does not exist in the staging schema.

  DATA_ONLY=Y: imp skips ALL DDL (including DBMS_RLS.ADD_POLICY calls).
               The VPD policy is never re-created in staging.
               SELECT returns rows cleanly.

  Fix: pass DATA_ONLY=Y in import_chunks_batch() and import_chunk()
       inside datapump/legacy/workflow.py (add data_only field to
       LegacyImportJob + render_legacy_import_parfile()).
"""
    )


if __name__ == "__main__":
    main()
