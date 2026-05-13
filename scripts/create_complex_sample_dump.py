#!/usr/bin/env python3
"""Create a complex local Oracle Data Pump sample dump.

This is intentionally outside the dmp-to-parquet CLI. Run it with:

    uv run python scripts/create_complex_sample_dump.py --force
"""

from __future__ import annotations

import argparse
import tempfile
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path

import oracledb
import yaml

from dmp_to_parquet.config import DEFAULT_ORACLE_IMAGE
from dmp_to_parquet.converter import OracleAdminConnection
from dmp_to_parquet.datapump import DataPumpRunner
from dmp_to_parquet.docker_oracle import DockerOracle, docker_available
from dmp_to_parquet.oracle_conn import (
    create_directory,
    drop_schema,
    ensure_schema,
    oracle_connection,
)
from dmp_to_parquet.parfile import DataPumpConnection, ExportJob

SCHEMAS = ("D2P_APP", "D2P_SALES", "D2P_DOCS")
PASSWORD = "SamplePwd_123"
DUMP_DIRECTORY = "D2P_SAMPLE_DUMP"
CONTAINER_DUMP_PATH = "/sample-dump"


@dataclass(frozen=True)
class SampleCounts:
    customers: int
    orders: int
    order_items: int
    events: int
    documents: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a complex Oracle expdp dump for local converter testing."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("sample-data/complex"),
        help="Directory where the dump, config, and notes are written.",
    )
    parser.add_argument(
        "--dumpfile",
        default="complex_full.dmp",
        help="Data Pump dump filename to create inside --output-dir.",
    )
    parser.add_argument(
        "--oracle-image",
        default=DEFAULT_ORACLE_IMAGE,
        help="Oracle Free Docker image to use.",
    )
    parser.add_argument(
        "--oracle-password",
        default="OraclePwd_123",
        help="SYSTEM password for the temporary Oracle container.",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="Scale factor for generated rows. Keep this small for fast local tests.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing generated dump/config files in --output-dir.",
    )
    return parser.parse_args()


def sample_counts(scale: int) -> SampleCounts:
    if scale < 1:
        msg = "--scale must be at least 1"
        raise ValueError(msg)
    return SampleCounts(
        customers=50 * scale,
        orders=240 * scale,
        order_items=720 * scale,
        events=500 * scale,
        documents=16 * scale,
    )


def execute_many_statements(conn: oracledb.Connection, statements: Iterable[str]) -> None:
    with conn.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    conn.commit()


def prepare_output_dir(output_dir: Path, dumpfile: str, force: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_files = (
        output_dir / dumpfile,
        output_dir / "complex_full_export.log",
        output_dir / "config.yaml",
        output_dir / "README.md",
    )
    existing = [path for path in generated_files if path.exists()]
    if existing and not force:
        names = ", ".join(str(path) for path in existing)
        msg = f"Generated files already exist: {names}. Re-run with --force to overwrite."
        raise FileExistsError(msg)
    for path in existing:
        path.unlink()


def connect_admin(admin: OracleAdminConnection) -> AbstractContextManager[oracledb.Connection]:
    return oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    )


def reset_schemas(conn: oracledb.Connection) -> None:
    for schema in SCHEMAS:
        drop_schema(conn, schema)
    for schema in SCHEMAS:
        ensure_schema(conn, schema, PASSWORD)


def create_tables(conn: oracledb.Connection) -> None:
    execute_many_statements(
        conn,
        (
            """
            CREATE TABLE D2P_APP.CUSTOMERS (
                CUSTOMER_ID NUMBER(10, 0) NOT NULL,
                CUSTOMER_CODE VARCHAR2(20) NOT NULL,
                FULL_NAME NVARCHAR2(120),
                EMAIL VARCHAR2(160),
                STATUS CHAR(1),
                CREDIT_LIMIT NUMBER(12, 2),
                SIGNUP_DATE DATE,
                UPDATED_AT TIMESTAMP,
                PROFILE CLOB,
                CONSTRAINT CUSTOMERS_PK PRIMARY KEY (CUSTOMER_ID),
                CONSTRAINT CUSTOMERS_CODE_UK UNIQUE (CUSTOMER_CODE)
            )
            """,
            """
            CREATE TABLE D2P_APP.ACCOUNT_SETTINGS (
                SETTING_ID NUMBER(10, 0) NOT NULL,
                CUSTOMER_ID NUMBER(10, 0) NOT NULL,
                SETTING_KEY VARCHAR2(40) NOT NULL,
                SETTING_VALUE VARCHAR2(200),
                ENABLED_FLAG CHAR(1),
                CONSTRAINT ACCOUNT_SETTINGS_PK PRIMARY KEY (SETTING_ID)
            )
            """,
            """
            CREATE TABLE D2P_SALES.ORDERS (
                ORDER_ID NUMBER(12, 0) NOT NULL,
                CUSTOMER_ID NUMBER(10, 0) NOT NULL,
                ORDER_DATE DATE NOT NULL,
                ORDER_TS TIMESTAMP,
                CHANNEL VARCHAR2(20),
                STATUS VARCHAR2(20),
                ORDER_TOTAL NUMBER(12, 2),
                NOTES CLOB,
                CONSTRAINT ORDERS_PK PRIMARY KEY (ORDER_ID)
            )
            PARTITION BY RANGE (ORDER_DATE) (
                PARTITION P2023 VALUES LESS THAN (DATE '2024-01-01'),
                PARTITION P2024 VALUES LESS THAN (DATE '2025-01-01'),
                PARTITION PMAX VALUES LESS THAN (MAXVALUE)
            )
            """,
            """
            CREATE TABLE D2P_SALES.ORDER_ITEMS (
                ITEM_ID NUMBER(12, 0) NOT NULL,
                ORDER_ID NUMBER(12, 0) NOT NULL,
                SKU VARCHAR2(40) NOT NULL,
                QUANTITY NUMBER(8, 0),
                UNIT_PRICE NUMBER(10, 2),
                DISCOUNT_PCT NUMBER(5, 2),
                CONSTRAINT ORDER_ITEMS_PK PRIMARY KEY (ITEM_ID)
            )
            """,
            """
            CREATE TABLE D2P_SALES.FACT_EVENTS (
                EVENT_ID NUMBER(12, 0) NOT NULL,
                CUSTOMER_ID NUMBER(10, 0),
                EVENT_DATE DATE,
                EVENT_TYPE VARCHAR2(30),
                SESSION_TOKEN RAW(16),
                PAYLOAD VARCHAR2(200),
                CONSTRAINT FACT_EVENTS_PK PRIMARY KEY (EVENT_ID)
            )
            """,
            """
            CREATE TABLE D2P_DOCS.DOCUMENTS (
                DOC_ID NUMBER(10, 0) NOT NULL,
                CUSTOMER_ID NUMBER(10, 0),
                DOC_NAME VARCHAR2(120),
                DOC_TEXT CLOB,
                DOC_BYTES BLOB,
                CREATED_AT DATE,
                CONSTRAINT DOCUMENTS_PK PRIMARY KEY (DOC_ID)
            )
            """,
        ),
    )


def insert_customers(conn: oracledb.Connection, count: int) -> None:
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            (
                idx,
                f"CUST-{idx:05d}",
                f"Customer {idx}",
                f"customer{idx}@example.test",
                "A" if idx % 7 else "H",
                round(500 + (idx * 13.37), 2),
                idx % 365,
                idx % 365,
                f"Profile text for customer {idx}. Segment={idx % 5}. " * 3,
            )
        )
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO D2P_APP.CUSTOMERS(
                CUSTOMER_ID, CUSTOMER_CODE, FULL_NAME, EMAIL, STATUS, CREDIT_LIMIT,
                SIGNUP_DATE, UPDATED_AT, PROFILE
            ) VALUES (
                :1, :2, :3, :4, :5, :6,
                DATE '2022-01-01' + :7,
                TIMESTAMP '2024-01-01 08:00:00' + NUMTODSINTERVAL(:8, 'DAY'),
                :9
            )
            """,
            rows,
        )
        settings = []
        setting_id = 1
        for customer_id in range(1, count + 1):
            for key in ("email_opt_in", "risk_band", "preferred_channel"):
                settings.append(
                    (
                        setting_id,
                        customer_id,
                        key,
                        f"value-{customer_id % 11}-{key}",
                        "Y" if customer_id % 4 else "N",
                    )
                )
                setting_id += 1
        cursor.executemany(
            """
            INSERT INTO D2P_APP.ACCOUNT_SETTINGS(
                SETTING_ID, CUSTOMER_ID, SETTING_KEY, SETTING_VALUE, ENABLED_FLAG
            ) VALUES (:1, :2, :3, :4, :5)
            """,
            settings,
        )
    conn.commit()


def insert_sales(conn: oracledb.Connection, counts: SampleCounts) -> None:
    order_rows = []
    for idx in range(1, counts.orders + 1):
        customer_id = ((idx - 1) % counts.customers) + 1
        order_rows.append(
            (
                idx,
                customer_id,
                idx % 730,
                idx % 730,
                ("web", "store", "partner", "phone")[idx % 4],
                ("new", "paid", "shipped", "returned")[idx % 4],
                round(25 + (idx % 200) * 4.15, 2),
                f"Order {idx} notes for customer {customer_id}.",
            )
        )

    item_rows = []
    item_id = 1
    for order_id in range(1, counts.orders + 1):
        for item_number in range(1, 4):
            item_rows.append(
                (
                    item_id,
                    order_id,
                    f"SKU-{(order_id + item_number) % 37:04d}",
                    (item_number % 5) + 1,
                    round(7.5 + ((order_id + item_number) % 29) * 2.75, 2),
                    0 if order_id % 5 else 10,
                )
            )
            item_id += 1

    event_rows = []
    for idx in range(1, counts.events + 1):
        customer_id = None if idx % 17 == 0 else ((idx - 1) % counts.customers) + 1
        token = idx.to_bytes(16, "big")
        event_rows.append(
            (
                idx,
                customer_id,
                idx % 365,
                ("page_view", "cart", "checkout", "support", "email")[idx % 5],
                token,
                f"event={idx};customer={customer_id};campaign={idx % 9}",
            )
        )

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO D2P_SALES.ORDERS(
                ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_TS, CHANNEL, STATUS, ORDER_TOTAL, NOTES
            ) VALUES (
                :1, :2,
                DATE '2023-01-01' + :3,
                TIMESTAMP '2023-01-01 12:00:00' + NUMTODSINTERVAL(:4, 'DAY'),
                :5, :6, :7, :8
            )
            """,
            order_rows,
        )
        cursor.executemany(
            """
            INSERT INTO D2P_SALES.ORDER_ITEMS(
                ITEM_ID, ORDER_ID, SKU, QUANTITY, UNIT_PRICE, DISCOUNT_PCT
            ) VALUES (:1, :2, :3, :4, :5, :6)
            """,
            item_rows,
        )
        cursor.executemany(
            """
            INSERT INTO D2P_SALES.FACT_EVENTS(
                EVENT_ID, CUSTOMER_ID, EVENT_DATE, EVENT_TYPE, SESSION_TOKEN, PAYLOAD
            ) VALUES (:1, :2, DATE '2024-01-01' + :3, :4, :5, :6)
            """,
            event_rows,
        )
    conn.commit()


def insert_documents(conn: oracledb.Connection, counts: SampleCounts) -> None:
    rows = []
    for idx in range(1, counts.documents + 1):
        body = f"Document {idx}\n" + ("Lorem ipsum dolor sit amet. " * (idx % 9 + 1))
        blob = bytes((idx + offset) % 256 for offset in range(64))
        rows.append(
            (
                idx,
                ((idx - 1) % counts.customers) + 1,
                f"contract-{idx:04d}.txt",
                body,
                blob,
                idx % 365,
            )
        )
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO D2P_DOCS.DOCUMENTS(
                DOC_ID, CUSTOMER_ID, DOC_NAME, DOC_TEXT, DOC_BYTES, CREATED_AT
            ) VALUES (:1, :2, :3, :4, :5, DATE '2024-06-01' + :6)
            """,
            rows,
        )
    conn.commit()


def gather_stats(conn: oracledb.Connection) -> None:
    with conn.cursor() as cursor:
        for schema in SCHEMAS:
            cursor.callproc("DBMS_STATS.GATHER_SCHEMA_STATS", [schema])
    conn.commit()


def write_config(output_dir: Path, oracle_image: str) -> None:
    config = {
        "oracle": {"image": oracle_image, "max_stage_gb": 8},
        "default_hash_buckets": 8,
        "tables": {
            "D2P_SALES.FACT_EVENTS": {
                "strategy": "hash",
                "split_column": "CUSTOMER_ID",
                "buckets": 8,
                "force_large": True,
            },
            "D2P_SALES.ORDER_ITEMS": {
                "strategy": "hash",
                "split_column": "ORDER_ID",
                "buckets": 4,
                "force_large": True,
            },
        },
    }
    (output_dir / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))


def write_notes(output_dir: Path, dumpfile: str, counts: SampleCounts) -> None:
    (output_dir / "README.md").write_text(
        f"""# Complex Sample Dump

Generated Oracle Data Pump file: `{dumpfile}`

Schemas:

- `D2P_APP`: customers and account settings.
- `D2P_SALES`: partitioned orders, order items, and hash-chunk event facts.
- `D2P_DOCS`: documents with CLOB and BLOB columns.

Approximate row counts:

- Customers: {counts.customers}
- Account settings: {counts.customers * 3}
- Orders: {counts.orders}
- Order items: {counts.order_items}
- Fact events: {counts.events}
- Documents: {counts.documents}

Try it:

```bash
uv run dmp-to-parquet inspect \\
  --dump {output_dir / dumpfile} \\
  --work-dir {output_dir / 'work'} \\
  --output {output_dir / 'work' / 'manifest.json'}

uv run dmp-to-parquet plan \\
  --manifest {output_dir / 'work' / 'manifest.json'} \\
  --config {output_dir / 'config.yaml'} \\
  --output {output_dir / 'work' / 'plan.yaml'}

uv run dmp-to-parquet convert \\
  --plan {output_dir / 'work' / 'plan.yaml'} \\
  --output {output_dir / 'parquet'}
```
"""
    )


def export_dump(
    *,
    container: DockerOracle,
    admin: OracleAdminConnection,
    output_dir: Path,
    dumpfile: str,
) -> None:
    with connect_admin(admin) as conn:
        create_directory(conn, DUMP_DIRECTORY, CONTAINER_DUMP_PATH)
    with tempfile.TemporaryDirectory(prefix="dmp-to-parquet-sample-") as tmp:
        runner = DataPumpRunner(container, Path(tmp))
        runner.run_expdp(
            ExportJob(
                connection=DataPumpConnection(admin.user, admin.password, admin.service),
                directory=DUMP_DIRECTORY,
                dumpfile=dumpfile,
                logfile="complex_full_export.log",
                include_schemas=SCHEMAS,
            )
        )
    if not (output_dir / dumpfile).exists():
        msg = f"Data Pump export completed but {output_dir / dumpfile} was not created"
        raise FileNotFoundError(msg)


def create_sample_database(admin: OracleAdminConnection, counts: SampleCounts) -> None:
    with connect_admin(admin) as conn:
        reset_schemas(conn)
        create_tables(conn)
        insert_customers(conn, counts.customers)
        insert_sales(conn, counts)
        insert_documents(conn, counts)
        gather_stats(conn)


def print_summary(output_dir: Path, dumpfile: str, counts: SampleCounts) -> None:
    print(f"Created {output_dir / dumpfile}")
    print(f"Wrote {output_dir / 'config.yaml'}")
    print(f"Wrote {output_dir / 'README.md'}")
    print(
        "Rows: "
        f"customers={counts.customers}, "
        f"orders={counts.orders}, "
        f"order_items={counts.order_items}, "
        f"events={counts.events}, "
        f"documents={counts.documents}"
    )


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    counts = sample_counts(args.scale)

    if not docker_available():
        msg = "Docker is not available. Start Docker Desktop or your Docker daemon first."
        raise RuntimeError(msg)

    prepare_output_dir(output_dir, args.dumpfile, args.force)
    with DockerOracle.start(
        image=args.oracle_image,
        password=args.oracle_password,
        mounts=((output_dir, CONTAINER_DUMP_PATH, "rw"),),
    ) as container:
        print(f"Started Oracle container {container.name}; waiting for readiness...")
        container.wait_ready(timeout_seconds=900)
        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=args.oracle_password,
        )
        print("Creating sample schemas and data...")
        create_sample_database(admin, counts)
        print("Exporting full Data Pump dump...")
        export_dump(container=container, admin=admin, output_dir=output_dir, dumpfile=args.dumpfile)

    write_config(output_dir, args.oracle_image)
    write_notes(output_dir, args.dumpfile, counts)
    print_summary(output_dir, args.dumpfile, counts)


if __name__ == "__main__":
    main()
