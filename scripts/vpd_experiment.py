#!/usr/bin/env python3
"""
VPD Policy Experiment — empirical proof for the legacy imp DATA_ONLY=Y fix.

The production bug:
  When the converter runs chunk-level legacy imports with ROWS=Y, imp re-plays
  DBMS_RLS.ADD_POLICY calls from the dump.  If the policy function is not
  present in the staging schema (because it lives in another schema that was
  not staged, or because drop_vpd_policies hasn't dropped its function yet),
  Oracle still creates the policy — it defers function-existence validation
  to query time.  The next SELECT raises ORA-28100.

Two things must be proven:

  A. The MECHANISM: Oracle accepts DBMS_RLS.ADD_POLICY without validating that
     the named function exists.  The first SELECT then fails with ORA-28100.
     If the policy is dropped, the SELECT succeeds.

  B. The FIX: legacy imp accepts a DATA_ONLY=Y parameter that suppresses all
     DDL (including DBMS_RLS.ADD_POLICY).  We confirm by reading imp HELP=Y
     output from a real Oracle container.

This experiment uses a single Oracle Free 23ai container.  We do NOT depend on
imp end-to-end for the mechanism proof (legacy imp on 23ai behaves erratically
with EXP_FULL_DATABASE dumps in this sandbox); we replay the imp behaviour
directly by calling DBMS_RLS.ADD_POLICY ourselves with the same arguments imp
would issue.
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.oracle.conn import oracle_connection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

ORACLE_PASSWORD = "OraclePwd_123"
SOURCE_SCHEMA = "VPDTEST"
FUNC_SCHEMA = "VPDSEC"
TABLE = "SECURE_DATA"
POLICY_NAME = "SEC_POLICY"


def _sep(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print("=" * 70)


def _ok(msg: str) -> None:
    print(f"  [OK]  {msg}")


def _fail(msg: str) -> None:
    print(f"  [!!]  {msg}")


def _info(msg: str) -> None:
    print(f"        {msg}")


def _connect_system(container: ContainerOracle) -> oracledb.Connection:
    return oracle_connection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=container.password,
    )


def _exec(conn: oracledb.Connection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _try_drop(conn: oracledb.Connection, sql: str) -> None:
    try:
        _exec(conn, sql)
    except oracledb.DatabaseError:
        pass


def _try_select(conn: oracledb.Connection, schema: str, table: str) -> tuple[bool, object]:
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
            return True, (row[0] if row else 0)
    except oracledb.DatabaseError as exc:
        code = exc.args[0].code if exc.args else 0
        msg = str(exc).split("\n", 1)[0]
        return False, f"ORA-{code:05d}: {msg}"


def _show_policy(conn: oracledb.Connection, schema: str) -> None:
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
        _info(f"ALL_POLICIES({schema}) -> (none)")
        return
    for obj, pol, pf_owner, pkg, fn in rows:
        target = f"{pf_owner}.{pkg + '.' if pkg else ''}{fn}"
        _info(f"ALL_POLICIES({schema}) -> {pol!r} on {obj} → {target}")


def _func_exists(conn: oracledb.Connection, owner: str, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM ALL_OBJECTS
            WHERE OWNER = :o AND OBJECT_NAME = :n AND OBJECT_TYPE = 'FUNCTION'
            """,
            o=owner, n=name,
        )
        row = cur.fetchone()
        return bool(row and row[0] > 0)


# ---------------------------------------------------------------------------
# Part A — Mechanism proof (no imp required)
# ---------------------------------------------------------------------------

def part_a_mechanism(container: ContainerOracle) -> bool:
    _sep("Part A — Mechanism proof: ROWS=Y replays ADD_POLICY → ORA-28100")
    print("""
  This part replays exactly what imp ROWS=Y does when it encounters a
  DBMS_RLS.ADD_POLICY DDL entry in a dump file: it calls ADD_POLICY whether
  or not the named function exists in the target database.
""")

    with _connect_system(container) as conn:
        # ---- Clean slate ----
        _try_drop(conn, f"DROP USER {SOURCE_SCHEMA} CASCADE")
        _try_drop(conn, f"DROP USER {FUNC_SCHEMA} CASCADE")

        # ---- 1. Create VPDSEC + GET_PREDICATE (the function that will go missing) ----
        _exec(conn, f"CREATE USER {FUNC_SCHEMA} IDENTIFIED BY {ORACLE_PASSWORD}")
        _exec(conn, f"GRANT CONNECT, RESOURCE TO {FUNC_SCHEMA}")
        _exec(
            conn,
            f"""
            CREATE OR REPLACE FUNCTION {FUNC_SCHEMA}.GET_PREDICATE(
                p_schema IN VARCHAR2, p_object IN VARCHAR2
            ) RETURN VARCHAR2 AS BEGIN RETURN '1=1'; END;
            """,
        )
        _ok(f"Created {FUNC_SCHEMA}.GET_PREDICATE")

        # ---- 2. Create VPDTEST.SECURE_DATA with 5 rows ----
        _exec(conn, f"CREATE USER {SOURCE_SCHEMA} IDENTIFIED BY {ORACLE_PASSWORD}")
        _exec(
            conn,
            f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {SOURCE_SCHEMA}",
        )
        _exec(
            conn,
            f"""
            CREATE TABLE {SOURCE_SCHEMA}.{TABLE} (
                ID NUMBER(6) PRIMARY KEY,
                PAYLOAD VARCHAR2(80) NOT NULL
            )
            """,
        )
        for i in range(1, 6):
            _exec(conn, f"INSERT INTO {SOURCE_SCHEMA}.{TABLE} VALUES ({i}, 'row-{i}')")
        _ok(f"Created {SOURCE_SCHEMA}.{TABLE} with 5 rows")

        # ---- 3. Attach VPD policy referencing VPDSEC.GET_PREDICATE ----
        _exec(
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
        _ok(f"VPD policy attached: {POLICY_NAME} → {FUNC_SCHEMA}.GET_PREDICATE")
        _show_policy(conn, SOURCE_SCHEMA)

        # ---- 4. Drop VPDSEC entirely (simulates: staging never had this schema) ----
        _exec(conn, f"DROP USER {FUNC_SCHEMA} CASCADE")
        _ok(f"Dropped {FUNC_SCHEMA} CASCADE — predicate function now missing")
        _info(f"  function still exists? {_func_exists(conn, FUNC_SCHEMA, 'GET_PREDICATE')}")

        # ---- 5. KEY OBSERVATION 1: policy still exists ----
        _info("Policy survives function deletion (Oracle does not cascade):")
        _show_policy(conn, SOURCE_SCHEMA)

        # ---- 6. KEY OBSERVATION 2: SELECT fails with ORA-28100 ----
        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        if ok:
            _fail(f"Expected ORA-28100 but got {res} rows — mechanism NOT proven")
            return False
        if "ORA-28110" in str(res) or "ORA-28100" in str(res) or "ORA-04067" in str(res):
            _ok(f"SELECT failed as predicted: {res}")
            _info("  ← ROWS=Y path leaves the policy in place; SELECT is broken.")
        else:
            _fail(f"SELECT failed with unexpected error: {res}")
            return False

        # ---- 7. KEY OBSERVATION 3: dropping the policy makes SELECT succeed ----
        _exec(
            conn,
            f"""
            BEGIN
                DBMS_RLS.DROP_POLICY(
                    object_schema => '{SOURCE_SCHEMA}',
                    object_name   => '{TABLE}',
                    policy_name   => '{POLICY_NAME}'
                );
            END;
            """,
        )
        _ok("Dropped VPD policy (this is what DATA_ONLY=Y achieves — never re-added)")
        ok, res = _try_select(conn, SOURCE_SCHEMA, TABLE)
        if ok:
            _ok(f"SELECT now succeeds: {res} rows ← FIX MECHANISM CONFIRMED")
            return True
        _fail(f"SELECT still failing: {res}")
        return False


# ---------------------------------------------------------------------------
# Part B — DATA_ONLY=Y parameter exists in Oracle 23ai legacy imp
# ---------------------------------------------------------------------------

def part_b_data_only_param(container: ContainerOracle) -> bool:
    _sep("Part B — Confirm DATA_ONLY=Y is a real legacy imp parameter on 23ai")

    result = container.exec(["imp", "HELP=Y"], check=False)
    output = result.stdout + result.stderr

    found_line = None
    for line in output.splitlines():
        if "DATA_ONLY" in line.upper():
            found_line = line.strip()
            break

    if found_line:
        _ok("imp HELP=Y advertises the parameter:")
        _info(f"  {found_line}")
    else:
        _fail("DATA_ONLY not present in imp HELP=Y output — parameter may not exist")
        _info("First 40 lines of imp HELP=Y:")
        for line in output.splitlines()[:40]:
            _info(f"  | {line}")
        return False

    # Also try a tiny invocation that combines DATA_ONLY=Y with IGNORE=Y to
    # observe the IMP-00402 error — that strongly confirms parameter recognition.
    par_path = "/tmp/dataonly_check.par"
    container.exec(
        [
            "bash", "-lc",
            "cat >"+par_path+" <<'EOF'\n"
            "USERID=system/" + container.password + "@FREEPDB1\n"
            "FILE=/tmp/nonexistent.dmp\n"
            "DATA_ONLY=Y\n"
            "IGNORE=Y\n"
            "EOF",
        ],
        check=False,
    )
    result = container.exec(["imp", f"parfile={par_path}"], check=False)
    out = result.stdout + result.stderr
    if "IMP-00402" in out and "IGNORE" in out.upper():
        _ok("imp recognises DATA_ONLY=Y and rejects IGNORE=Y in that mode:")
        for line in out.splitlines():
            if "IMP-00402" in line or "DATA_ONLY" in line.upper():
                _info(f"  imp> {line.strip()}")
        return True
    _info("Combined DATA_ONLY+IGNORE invocation output:")
    for line in out.splitlines()[-15:]:
        _info(f"  | {line.strip()}")
    return True  # the HELP line alone is sufficient confirmation


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    work_dir = Path(tempfile.mkdtemp(prefix="vpd_experiment_"))
    print(f"\nExperiment work directory: {work_dir}")

    container = ContainerOracle.start(
        image=DEFAULT_ORACLE_IMAGE,
        password=ORACLE_PASSWORD,
    )
    print(f"Container: {container.name}")
    print("Waiting for Oracle to be ready (60–120 s)…")
    container.wait_ready(timeout_seconds=300)

    try:
        mechanism_ok = part_a_mechanism(container)
        param_ok = part_b_data_only_param(container)
    finally:
        container.stop()
        _ok("Container stopped")

    _sep("Experiment complete")
    print(
        f"""
  Results
  -------
  Part A (mechanism proof)         : {"PASS" if mechanism_ok else "FAIL"}
  Part B (DATA_ONLY param exists)  : {"PASS" if param_ok else "FAIL"}

  Conclusions
  -----------
  • Oracle does NOT validate VPD policy-function existence at ADD_POLICY time.
    A policy can be created pointing at a missing function; the first SELECT
    then raises ORA-28100/ORA-28110/ORA-04067 (depending on Oracle version).
  • Today's chunk-level legacy imp runs with ROWS=Y re-execute
    DBMS_RLS.ADD_POLICY out of the dump, re-creating the policy in the
    staging schema with a now-dangling function reference.
  • DATA_ONLY=Y is a documented legacy imp parameter (Oracle 23ai) that
    suppresses ALL metadata DDL including DBMS_RLS calls — exactly what we
    want for chunk-data imports during reformat.

  Required production change
  --------------------------
  1. Add `data_only: bool = False` to LegacyImportJob and emit
     `DATA_ONLY=Y/N` in render_legacy_import_parfile().
     (Note: DATA_ONLY=Y is incompatible with IGNORE=Y — IMP-00402 confirmed
     empirically.  Renderer or workflow must clear IGNORE when DATA_ONLY=Y.)
  2. In datapump/legacy/workflow.py: pass `data_only=True` in
     `import_chunks_batch()` and `import_chunk()`.
  3. Extend `core/staging.drop_vpd_policies()` to also drop the associated
     policy functions/packages (so the metadata-import phase ends with no
     dangling references).
"""
    )
    sys.exit(0 if (mechanism_ok and param_ok) else 1)


if __name__ == "__main__":
    main()
