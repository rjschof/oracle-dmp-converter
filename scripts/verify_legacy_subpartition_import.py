#!/usr/bin/env python3
"""Empirically verify legacy ``exp``/``imp`` round-trip on a composite-partitioned table.

Phase A of the legacy subpartition drill-down plan. Documentation says
legacy ``imp`` supports ``TABLES=schema.table:partition_name`` and
``TABLES=schema.table:subpartition_name``. This script proves it works
against the actual gvenzl Oracle Free image we use everywhere else, so
the production code changes that follow have a real validation point.

What it does, against a single Oracle Free container (using TRUNCATE +
re-import in the same schema, which is functionally equivalent to
two containers for proving the ``imp`` TABLES syntax):

  1. Create a composite-partitioned table ``VERIFY.TXN_DETAILS``
     (3 range partitions x 4 hash subpartitions = 12 subpartitions),
     insert 120 rows.
  2. Record the per-partition and per-subpartition row counts from
     ``ALL_TAB_SUBPARTITIONS`` so we know the ground truth.
  3. ``exp`` the table to a dump file inside the container.
  4. ``TRUNCATE`` the staging table (keeps DDL, drops rows).
  5. ``imp TABLES=(TXN_DETAILS:p_2024)`` — partition-level import.
     Assert only that partition's rows came back.
  6. ``TRUNCATE``; ``imp TABLES=(TXN_DETAILS:<SUB_NAME>)`` for one
     subpartition (auto-generated name read from the catalog).
     Assert only that subpartition's rows came back.
  7. ``TRUNCATE``; ``imp TABLES=(TXN_DETAILS)`` for whole-table sanity.
     Assert all 120 rows came back.

Each step prints what it's doing and what it observed. Exit code 0 means
all three filtering modes work; non-zero means stop and rethink.

Run with:
    uv run python scripts/verify_legacy_subpartition_import.py
"""

# pylint: disable=too-many-locals,too-many-statements  # standalone diagnostic — readability beats decomposition.

from __future__ import annotations

import logging
import sys
import tempfile
import uuid
from pathlib import Path

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    LegacyImportJob,
    render_legacy_export_parfile,
    render_legacy_import_parfile,
)
from oracle_dmp_converter.oracle.conn import OracleCredentials, oracle_connection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle, docker_available

LOGGER = logging.getLogger("verify_legacy_subpartition")

SCHEMA = "VERIFY"
SCHEMA_PASSWORD = "VerifyPwd_123"
ADMIN_PASSWORD = "OraclePwd_123"
TABLE = "TXN_DETAILS"
CONTAINER_DUMP_PATH = "/dumps"
DUMP_FILE = "verify_legacy.dmp"
EXP_LOG = "verify_legacy_exp.log"


def _run_sql(conn, statement: str) -> None:
    LOGGER.debug("SQL: %s", statement.strip().splitlines()[0])
    with conn.cursor() as cursor:
        cursor.execute(statement)


def _count(conn, table_ref: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_ref}")
        return int(cursor.fetchone()[0])


def _subpartition_names(conn) -> list[tuple[str, str, int]]:
    """Return ``(parent_partition, subpartition_name, row_count)`` per subpartition."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT partition_name, subpartition_name
            FROM ALL_TAB_SUBPARTITIONS
            WHERE table_owner = :owner AND table_name = :table_name
            ORDER BY partition_name, subpartition_position
            """,
            owner=SCHEMA,
            table_name=TABLE,
        )
        pairs = list(cursor.fetchall())
    rows = []
    for parent, sub in pairs:
        count = _count(conn, f'{SCHEMA}.{TABLE} SUBPARTITION ("{sub}")')
        rows.append((parent, sub, count))
    return rows


def _create_source_table(conn) -> None:
    _run_sql(conn, f"CREATE USER {SCHEMA} IDENTIFIED BY {SCHEMA_PASSWORD}")
    _run_sql(conn, f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {SCHEMA}")
    _run_sql(
        conn,
        f"""
        CREATE TABLE {SCHEMA}.{TABLE} (
            id NUMBER PRIMARY KEY,
            txn_date DATE NOT NULL,
            account_id NUMBER NOT NULL,
            amount NUMBER
        )
        PARTITION BY RANGE (txn_date)
        SUBPARTITION BY HASH (account_id) SUBPARTITIONS 4
        (
            PARTITION p_2024 VALUES LESS THAN (DATE '2025-01-01'),
            PARTITION p_2025 VALUES LESS THAN (DATE '2026-01-01'),
            PARTITION p_max  VALUES LESS THAN (MAXVALUE)
        )
        """,
    )
    # 40 rows per range partition, varying account_id to spread across hash subpartitions.
    with conn.cursor() as cursor:
        rows = []
        for i in range(120):
            if i < 40:
                txn_date = "DATE '2024-06-15'"
            elif i < 80:
                txn_date = "DATE '2025-06-15'"
            else:
                txn_date = "DATE '2027-06-15'"
            rows.append(
                f"INTO {SCHEMA}.{TABLE} (id, txn_date, account_id, amount) "
                f"VALUES ({i}, {txn_date}, {i}, {i * 10})"
            )
        cursor.execute("INSERT ALL " + " ".join(rows) + " SELECT 1 FROM DUAL")
    conn.commit()


def _truncate(conn) -> None:
    _run_sql(conn, f"TRUNCATE TABLE {SCHEMA}.{TABLE}")


def _exp(container: ContainerOracle) -> None:
    job = LegacyExportJob(
        connection=OracleCredentials(
            user="system", password=ADMIN_PASSWORD, service=container.service
        ),
        files=(f"{CONTAINER_DUMP_PATH}/{DUMP_FILE}",),
        logfile=f"{CONTAINER_DUMP_PATH}/{EXP_LOG}",
        owner=(SCHEMA,),
        rows=True,
        indexes=False,
        grants=False,
    )
    parfile_text = render_legacy_export_parfile(job)
    par_name = f"verify-exp-{uuid.uuid4().hex}.par"
    with tempfile.TemporaryDirectory() as tmp:
        local_par = Path(tmp) / par_name
        local_par.write_text(parfile_text)
        container.copy_to(local_par, f"/tmp/{par_name}")
        result = container.exec(["exp", f"parfile=/tmp/{par_name}"], check=False)
        LOGGER.info("exp stdout:\n%s", result.stdout)
        LOGGER.info("exp stderr:\n%s", result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"exp failed (returncode={result.returncode}); see logs above")


def _imp(container: ContainerOracle, tables_arg: str) -> None:
    """Render a parfile by hand because LegacyImportJob.tables is bare-name today."""
    creds = OracleCredentials(user="system", password=ADMIN_PASSWORD, service=container.service)
    job = LegacyImportJob(
        connection=creds,
        files=(f"{CONTAINER_DUMP_PATH}/{DUMP_FILE}",),
        logfile=f"{CONTAINER_DUMP_PATH}/verify_legacy_imp.log",
        fromuser=SCHEMA,
        touser=SCHEMA,
        rows=True,
        indexes=False,
        grants=False,
        constraints=False,
        ignore=True,
    )
    parfile_text = render_legacy_import_parfile(job)
    # The current renderer skips TABLES= when job.tables is empty; inject our
    # own filter line so we can test partition/subpartition syntax directly.
    parfile_text = parfile_text.rstrip("\n") + f"\nTABLES=({tables_arg})\n"
    par_name = f"verify-imp-{uuid.uuid4().hex}.par"
    LOGGER.info("imp parfile:\n%s", parfile_text)
    with tempfile.TemporaryDirectory() as tmp:
        local_par = Path(tmp) / par_name
        local_par.write_text(parfile_text)
        container.copy_to(local_par, f"/tmp/{par_name}")
        result = container.exec(["imp", f"parfile=/tmp/{par_name}"], check=False)
        LOGGER.info("imp stdout:\n%s", result.stdout)
        LOGGER.info("imp stderr:\n%s", result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"imp failed for TABLES=({tables_arg}) — returncode={result.returncode}"
            )


def _connect(container: ContainerOracle):
    return oracle_connection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=ADMIN_PASSWORD,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    if not docker_available():
        LOGGER.error("Docker is not available; start Docker Desktop first.")
        return 2

    with tempfile.TemporaryDirectory(prefix="oracle-verify-legacy-") as dump_host_dir:
        dump_host_path = Path(dump_host_dir)
        with ContainerOracle.start(
            image=DEFAULT_ORACLE_IMAGE,
            password=ADMIN_PASSWORD,
            mounts=((dump_host_path, CONTAINER_DUMP_PATH, "rw"),),
        ) as container:
            container.wait_ready(timeout_seconds=300)

            LOGGER.info("=== Step 1/7: create source composite-partitioned table ===")
            with _connect(container) as conn:
                _create_source_table(conn)

            LOGGER.info("=== Step 2/7: read ground-truth subpartition row counts ===")
            with _connect(container) as conn:
                catalog = _subpartition_names(conn)
                total = _count(conn, f"{SCHEMA}.{TABLE}")
            LOGGER.info("Total rows in source: %d", total)
            for parent, sub, count in catalog:
                LOGGER.info("  partition=%s subpartition=%s rows=%d", parent, sub, count)
            if total != 120:
                LOGGER.error("Expected 120 source rows, got %d", total)
                return 1

            LOGGER.info("=== Step 3/7: exp the table ===")
            _exp(container)

            LOGGER.info("=== Step 4/7: TRUNCATE + imp partition p_2024 ===")
            with _connect(container) as conn:
                _truncate(conn)
            _imp(container, f"{TABLE}:p_2024")
            with _connect(container) as conn:
                got = _count(conn, f"{SCHEMA}.{TABLE}")
                p_2024_expected = sum(c for parent, _, c in catalog if parent.upper() == "P_2024")
            LOGGER.info("After imp partition p_2024: got=%d expected=%d", got, p_2024_expected)
            if got != p_2024_expected:
                LOGGER.error("PARTITION import did NOT match ground truth")
                return 1

            target_parent, target_sub, target_count = catalog[0]
            LOGGER.info(
                "=== Step 5/7: TRUNCATE + imp subpartition %s (parent=%s, expected=%d) ===",
                target_sub,
                target_parent,
                target_count,
            )
            with _connect(container) as conn:
                _truncate(conn)
            _imp(container, f"{TABLE}:{target_sub}")
            with _connect(container) as conn:
                got = _count(conn, f"{SCHEMA}.{TABLE}")
            LOGGER.info(
                "After imp subpartition %s: got=%d expected=%d", target_sub, got, target_count
            )
            if got != target_count:
                LOGGER.error("SUBPARTITION import did NOT match ground truth")
                return 1

            LOGGER.info("=== Step 6/7: TRUNCATE + imp whole table ===")
            with _connect(container) as conn:
                _truncate(conn)
            _imp(container, TABLE)
            with _connect(container) as conn:
                got = _count(conn, f"{SCHEMA}.{TABLE}")
            LOGGER.info("After imp whole table: got=%d expected=120", got)
            if got != 120:
                LOGGER.error("WHOLE-TABLE import did NOT match ground truth")
                return 1

            LOGGER.info("=== Step 7/7: all three modes verified ===")
            LOGGER.info("PASS: legacy imp supports partition AND subpartition TABLES= syntax.")
            return 0


if __name__ == "__main__":
    sys.exit(main())
