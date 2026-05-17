#!/usr/bin/env python3
"""Create a sample Oracle DMP file using the legacy ``exp`` utility.

This script:
1. Starts an Oracle Free container.
2. Creates two source schemas (APP and REPORTING) with tables and test data.
3. Exports both schemas with ``exp`` (legacy format, NOT Data Pump).
4. Saves the resulting .dmp and .log files to ``sample-data/legacy/``.

The generated files are intentionally ignored by git (see .gitignore).
Run with:
    uv run python scripts/create_legacy_exp_sample.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import uuid
from pathlib import Path

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.datapump.legacy_parfile import (
    LegacyConnection,
    LegacyExportJob,
    render_legacy_export_parfile,
)
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.oracle.conn import drop_schema, oracle_connection

LOGGER = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "sample-data" / "legacy"
DUMP_FILENAME = "legacy_exp_sample.dmp"
LOG_FILENAME = "legacy_exp_sample.log"
APP_SCHEMA = "APP"
REPORTING_SCHEMA = "REPORTING"
ORACLE_PASSWORD = "OraclePwd_123"
SCHEMA_PASSWORD = "LegacyPwd_123"


def setup_app_schema(host: str, port: int, service: str) -> None:
    LOGGER.info("Creating schema %s ...", APP_SCHEMA)
    with oracle_connection(
        host=host,
        port=port,
        service=service,
        user="system",
        password=ORACLE_PASSWORD,
    ) as conn:
        drop_schema(conn, APP_SCHEMA)
        with conn.cursor() as cur:
            cur.execute(f'CREATE USER {APP_SCHEMA} IDENTIFIED BY "{SCHEMA_PASSWORD}"')
            cur.execute(f"GRANT CONNECT, RESOURCE TO {APP_SCHEMA}")
            cur.execute(f"ALTER USER {APP_SCHEMA} QUOTA UNLIMITED ON USERS")

            cur.execute(
                f"""
                CREATE TABLE {APP_SCHEMA}.PRODUCTS (
                    PRODUCT_ID   NUMBER(10,0) NOT NULL,
                    PRODUCT_NAME VARCHAR2(100),
                    PRICE        NUMBER(10,2),
                    CONSTRAINT PRODUCTS_PK PRIMARY KEY (PRODUCT_ID)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {APP_SCHEMA}.ORDERS (
                    ORDER_ID    NUMBER(10,0) NOT NULL,
                    PRODUCT_ID  NUMBER(10,0),
                    QUANTITY    NUMBER(5,0),
                    ORDER_DATE  DATE DEFAULT SYSDATE,
                    CONSTRAINT ORDERS_PK PRIMARY KEY (ORDER_ID)
                )
                """
            )

            products = [(i, f"Product {i}", round(9.99 + i * 1.5, 2)) for i in range(1, 21)]
            cur.executemany(f"INSERT INTO {APP_SCHEMA}.PRODUCTS VALUES (:1, :2, :3)", products)

            orders = [(i, (i % 20) + 1, (i % 5) + 1) for i in range(1, 51)]
            cur.executemany(
                f"INSERT INTO {APP_SCHEMA}.ORDERS(ORDER_ID, PRODUCT_ID, QUANTITY)"
                " VALUES (:1, :2, :3)",
                orders,
            )
        conn.commit()
    LOGGER.info("  %s.PRODUCTS  — 20 rows", APP_SCHEMA)
    LOGGER.info("  %s.ORDERS    — 50 rows", APP_SCHEMA)


def setup_reporting_schema(host: str, port: int, service: str) -> None:
    LOGGER.info("Creating schema %s ...", REPORTING_SCHEMA)
    with oracle_connection(
        host=host,
        port=port,
        service=service,
        user="system",
        password=ORACLE_PASSWORD,
    ) as conn:
        drop_schema(conn, REPORTING_SCHEMA)
        with conn.cursor() as cur:
            cur.execute(f'CREATE USER {REPORTING_SCHEMA} IDENTIFIED BY "{SCHEMA_PASSWORD}"')
            cur.execute(f"GRANT CONNECT, RESOURCE TO {REPORTING_SCHEMA}")
            cur.execute(f"ALTER USER {REPORTING_SCHEMA} QUOTA UNLIMITED ON USERS")

            cur.execute(
                f"""
                CREATE TABLE {REPORTING_SCHEMA}.REPORTS (
                    REPORT_ID    NUMBER(10,0) NOT NULL,
                    REPORT_NAME  VARCHAR2(100),
                    CREATED_DATE DATE DEFAULT SYSDATE,
                    CONSTRAINT REPORTS_PK PRIMARY KEY (REPORT_ID)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {REPORTING_SCHEMA}.METRICS (
                    METRIC_ID   NUMBER(10,0) NOT NULL,
                    METRIC_NAME VARCHAR2(100),
                    VALUE       NUMBER(12,4),
                    CONSTRAINT METRICS_PK PRIMARY KEY (METRIC_ID)
                )
                """
            )

            reports = [(i, f"Report {i}") for i in range(1, 11)]
            cur.executemany(
                f"INSERT INTO {REPORTING_SCHEMA}.REPORTS(REPORT_ID, REPORT_NAME) VALUES (:1, :2)",
                reports,
            )

            metrics = [(i, f"Metric {i}", round(i * 3.14, 4)) for i in range(1, 16)]
            cur.executemany(
                f"INSERT INTO {REPORTING_SCHEMA}.METRICS(METRIC_ID, METRIC_NAME, VALUE)"
                " VALUES (:1, :2, :3)",
                metrics,
            )
        conn.commit()
    LOGGER.info("  %s.REPORTS  — 10 rows", REPORTING_SCHEMA)
    LOGGER.info("  %s.METRICS  — 15 rows", REPORTING_SCHEMA)


def run_legacy_exp(
    container: DockerOracle,
    work_dir: Path,
    service: str,
    dump_container_path: str,
    log_container_path: str,
) -> None:
    LOGGER.info("Running legacy exp ...")
    conn = LegacyConnection(user="system", password=ORACLE_PASSWORD, service=service)
    job = LegacyExportJob(
        connection=conn,
        files=(dump_container_path,),
        logfile=log_container_path,
        owner=(APP_SCHEMA, REPORTING_SCHEMA),
        rows=True,
        indexes=False,
        grants=False,
        compress=False,
    )
    parfile_text = render_legacy_export_parfile(job)
    par_name = f"exp-sample-{uuid.uuid4().hex}.par"
    local_par = work_dir / par_name
    local_par.write_text(parfile_text)
    remote_par = f"/tmp/{par_name}"
    container.copy_to(local_par, remote_par)

    result = container.exec(["exp", f"parfile={remote_par}"], check=False)
    output = result.stdout + result.stderr
    LOGGER.info(output)
    if result.returncode != 0:
        LOGGER.error("exp failed — see output above")
        sys.exit(1)


def copy_out(container: DockerOracle, remote_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["docker", "cp", f"{container.name}:{remote_path}", str(local_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        LOGGER.error("ERROR copying %s: %s", remote_path, result.stderr)
        sys.exit(1)
    LOGGER.info("Saved -> %s", local_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--oracle-image",
        default=DEFAULT_ORACLE_IMAGE,
        help="Docker image for Oracle Free (default: %(default)s)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing sample dump if present",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    dump_out = OUTPUT_DIR / DUMP_FILENAME
    log_out = OUTPUT_DIR / LOG_FILENAME

    if dump_out.exists() and not args.force:
        LOGGER.info("Sample dump already exists: %s", dump_out)
        LOGGER.info("Pass --force to regenerate.")
        return

    work_dir = OUTPUT_DIR / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)

    container_dumps = "/dumps"
    dump_container = f"{container_dumps}/{DUMP_FILENAME}"
    log_container = f"{container_dumps}/{LOG_FILENAME}"

    image = args.oracle_image
    LOGGER.info("Starting Oracle container (%s) ...", image)

    with DockerOracle.start(
        image=image,
        password=ORACLE_PASSWORD,
        mounts=((OUTPUT_DIR, container_dumps, "rw"),),
    ) as container:
        LOGGER.info("Container: %s", container.name)
        LOGGER.info("Waiting for Oracle to be ready (may take a few minutes) ...")
        container.wait_ready(timeout_seconds=600)
        LOGGER.info("Oracle is ready.")

        port = container.mapped_port()
        service = container.service

        setup_app_schema("localhost", port, service)
        setup_reporting_schema("localhost", port, service)
        run_legacy_exp(container, work_dir, service, dump_container, log_container)

        # Files are written into the mounted OUTPUT_DIR on the host.
        # Fall back to docker cp only if the mount didn't surface them.
        if not dump_out.exists():
            copy_out(container, dump_container, dump_out)
            copy_out(container, log_container, log_out)

    if dump_out.exists():
        size_kb = dump_out.stat().st_size // 1024
        LOGGER.info("Legacy exp dump created successfully:")
        LOGGER.info("  %s  (%d KB)", dump_out, size_kb)
        if log_out.exists():
            LOGGER.info("  %s", log_out)
    else:
        LOGGER.error("dump file not found after container exit.")
        sys.exit(1)


if __name__ == "__main__":
    main()
