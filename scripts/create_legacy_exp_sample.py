#!/usr/bin/env python3
"""Create a sample Oracle DMP file using the legacy ``exp`` utility.

This script:
1. Starts an Oracle Free container.
2. Creates a source schema (LEGACYSRC) with two tables and test data.
3. Exports the schema with ``exp`` (legacy format, NOT Data Pump).
4. Saves the resulting .dmp and .log files to ``sample-data/legacy/``.

The generated files are intentionally ignored by git (see .gitignore).
Run with:
    uv run python scripts/create_legacy_exp_sample.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

from dmp_to_parquet.config import DEFAULT_ORACLE_IMAGE
from dmp_to_parquet.datapump.legacy_parfile import (
    LegacyConnection,
    LegacyExportJob,
    render_legacy_export_parfile,
)
from dmp_to_parquet.docker_oracle import DockerOracle
from dmp_to_parquet.oracle.conn import drop_schema, oracle_connection

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "sample-data" / "legacy"
DUMP_FILENAME = "legacy_exp_sample.dmp"
LOG_FILENAME = "legacy_exp_sample.log"
SCHEMA = "LEGACYSRC"
PASSWORD = "OraclePwd_123"
SCHEMA_PASSWORD = "LegacySrcPwd_123"


def setup_schema(host: str, port: int, service: str) -> None:
    print(f"  Creating schema {SCHEMA} …")
    with oracle_connection(
        host=host,
        port=port,
        service=service,
        user="system",
        password=PASSWORD,
    ) as conn:
        drop_schema(conn, SCHEMA)
        with conn.cursor() as cur:
            cur.execute(f'CREATE USER {SCHEMA} IDENTIFIED BY "{SCHEMA_PASSWORD}"')
            cur.execute(f"GRANT CONNECT, RESOURCE TO {SCHEMA}")
            cur.execute(f"ALTER USER {SCHEMA} QUOTA UNLIMITED ON USERS")

            cur.execute(
                f"""
                CREATE TABLE {SCHEMA}.PRODUCTS (
                    PRODUCT_ID   NUMBER(10,0) NOT NULL,
                    PRODUCT_NAME VARCHAR2(100),
                    PRICE        NUMBER(10,2),
                    CONSTRAINT PRODUCTS_PK PRIMARY KEY (PRODUCT_ID)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {SCHEMA}.ORDERS (
                    ORDER_ID    NUMBER(10,0) NOT NULL,
                    PRODUCT_ID  NUMBER(10,0),
                    QUANTITY    NUMBER(5,0),
                    ORDER_DATE  DATE DEFAULT SYSDATE,
                    CONSTRAINT ORDERS_PK PRIMARY KEY (ORDER_ID)
                )
                """
            )

            products = [(i, f"Product {i}", round(9.99 + i * 1.5, 2)) for i in range(1, 21)]
            cur.executemany(f"INSERT INTO {SCHEMA}.PRODUCTS VALUES (:1, :2, :3)", products)

            orders = [(i, (i % 20) + 1, (i % 5) + 1) for i in range(1, 51)]
            cur.executemany(
                f"INSERT INTO {SCHEMA}.ORDERS(ORDER_ID, PRODUCT_ID, QUANTITY) VALUES (:1, :2, :3)",
                orders,
            )
        conn.commit()
    print(f"    {SCHEMA}.PRODUCTS  — 20 rows")
    print(f"    {SCHEMA}.ORDERS    — 50 rows")


def run_legacy_exp(
    container: DockerOracle,
    work_dir: Path,
    service: str,
    dump_container_path: str,
    log_container_path: str,
) -> None:
    print("  Running legacy exp …")
    conn = LegacyConnection(user="system", password=PASSWORD, service=service)
    job = LegacyExportJob(
        connection=conn,
        files=(dump_container_path,),
        logfile=log_container_path,
        owner=(SCHEMA,),
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
    print(output)
    if result.returncode != 0:
        print("ERROR: exp failed — see output above", file=sys.stderr)
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
        print(f"ERROR copying {remote_path}: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"    Saved → {local_path}")


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

    dump_out = OUTPUT_DIR / DUMP_FILENAME
    log_out = OUTPUT_DIR / LOG_FILENAME

    if dump_out.exists() and not args.force:
        print(f"Sample dump already exists: {dump_out}")
        print("Pass --force to regenerate.")
        return

    work_dir = OUTPUT_DIR / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)

    container_dumps = "/dumps"
    dump_container = f"{container_dumps}/{DUMP_FILENAME}"
    log_container = f"{container_dumps}/{LOG_FILENAME}"

    image = args.oracle_image
    print(f"Starting Oracle container ({image}) …")

    with DockerOracle.start(
        image=image,
        password=PASSWORD,
        mounts=((OUTPUT_DIR, container_dumps, "rw"),),
    ) as container:
        print(f"  Container: {container.name}")
        print("  Waiting for Oracle to be ready (may take a few minutes) …")
        container.wait_ready(timeout_seconds=600)
        print("  Oracle is ready.")

        port = container.mapped_port()
        service = container.service

        setup_schema("localhost", port, service)
        run_legacy_exp(container, work_dir, service, dump_container, log_container)

        # Files are written into the mounted OUTPUT_DIR on the host.
        # Fall back to docker cp only if the mount didn't surface them.
        if not dump_out.exists():
            copy_out(container, dump_container, dump_out)
            copy_out(container, log_container, log_out)

    print()
    if dump_out.exists():
        size_kb = dump_out.stat().st_size // 1024
        print("Legacy exp dump created successfully:")
        print(f"  {dump_out}  ({size_kb} KB)")
        if log_out.exists():
            print(f"  {log_out}")
    else:
        print("ERROR: dump file not found after container exit.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
