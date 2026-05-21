#!/usr/bin/env python3
"""Create a full Oracle dump in both modern (expdp) and legacy (exp) formats.

This script exercises a broad set of Oracle database features:

- Custom tablespaces (COMBINED_DATA for tables, COMBINED_IDX for indexes)
- Four schemas: HRDATA, INVENTORY, FINANCE, AUDITLOG
- Column types: TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE,
  INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, NCLOB
- All three partition types: LIST (INVENTORY.PRODUCTS),
  RANGE (FINANCE.TRANSACTIONS), HASH (AUDITLOG.CHANGE_LOG)
- Intra- and cross-schema foreign keys and check constraints
- Sequences, BEFORE INSERT triggers, views, a materialized view,
  a stored function, procedure, and package (spec + body)
- Function-based and composite indexes in a dedicated tablespace
- Cross-schema grants and synonyms (public and private)

Two dump files are produced from the same database:

- ``full_combined_modern.dmp``  — Oracle Data Pump (expdp) format
- ``full_combined_legacy.dmp``  — legacy exp format

The legacy dump will log a warning for the cross-schema FK
(FINANCE.ACCOUNTS → HRDATA.EMPLOYEES); this is expected and documented in
the generated README.

Run with:
    uv run python scripts/create_full_combined_dump.py [--force]
"""

# pylint: disable=too-many-lines  # standalone script with extensive DDL + README content

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import uuid
from collections.abc import Iterable
from contextlib import AbstractContextManager
from pathlib import Path

import oracledb
import yaml

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.converter import OracleAdminConnection
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    render_legacy_export_parfile,
)
from oracle_dmp_converter.datapump.modern.parfile import ExportJob
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner
from oracle_dmp_converter.docker_oracle import DockerOracle, docker_available
from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    create_directory,
    drop_schema,
    ensure_schema,
    execute_ignore,
    oracle_connection,
)

LOGGER = logging.getLogger(__name__)

SCHEMAS = ("HRDATA", "INVENTORY", "FINANCE", "AUDITLOG")
PASSWORD = "CombinedPwd_123"
DUMP_DIRECTORY = "D2P_COMBINED_DUMP"
CONTAINER_DUMP_PATH = "/combined-dump"
MODERN_DUMPFILE = "full_combined_modern.dmp"
MODERN_LOGFILE = "full_combined_modern_export.log"
LEGACY_DUMPFILE = "full_combined_legacy.dmp"
LEGACY_LOGFILE = "full_combined_legacy_export.log"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate full Oracle dumps in both Data Pump and legacy exp formats."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("sample-data/full-combined"),
        help="Directory where all dumps, configs, and notes are written.",
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
        "--force",
        action="store_true",
        help="Overwrite existing generated files in --output-dir.",
    )
    return parser.parse_args()


def prepare_output_dir(output_dir: Path, force: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_files = (
        output_dir / MODERN_DUMPFILE,
        output_dir / MODERN_LOGFILE,
        output_dir / LEGACY_DUMPFILE,
        output_dir / LEGACY_LOGFILE,
        output_dir / "config.yaml",
        output_dir / "README.md",
    )
    existing = [p for p in generated_files if p.exists()]
    if existing and not force:
        names = ", ".join(str(p) for p in existing)
        msg = f"Generated files already exist: {names}. Re-run with --force to overwrite."
        raise FileExistsError(msg)
    for p in existing:
        p.unlink()


def connect_admin(admin: OracleAdminConnection) -> AbstractContextManager[oracledb.Connection]:
    return oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    )


def execute_stmts(conn: oracledb.Connection, statements: Iterable[str]) -> None:
    """Execute multiple DDL statements on a single cursor and commit once."""
    with conn.cursor() as cursor:
        for stmt in statements:
            cursor.execute(stmt)
    conn.commit()


# ---------------------------------------------------------------------------
# Schema / tablespace setup
# ---------------------------------------------------------------------------


def create_tablespaces(conn: oracledb.Connection) -> None:
    """Create COMBINED_DATA (tables) and COMBINED_IDX (indexes) tablespaces."""
    execute_ignore(
        conn,
        """
        CREATE TABLESPACE COMBINED_DATA
        DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/combined_data01.dbf'
        SIZE 50M AUTOEXTEND ON NEXT 10M
        """,
        {1543},  # ORA-01543: tablespace already exists
    )
    execute_ignore(
        conn,
        """
        CREATE TABLESPACE COMBINED_IDX
        DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/combined_idx01.dbf'
        SIZE 20M AUTOEXTEND ON NEXT 5M
        """,
        {1543},
    )
    conn.commit()


def reset_schemas(conn: oracledb.Connection) -> None:
    for schema in SCHEMAS:
        drop_schema(conn, schema)
    for schema in SCHEMAS:
        ensure_schema(conn, schema, PASSWORD)
    with conn.cursor() as cursor:
        for schema in SCHEMAS:
            cursor.execute(f"ALTER USER {schema} QUOTA UNLIMITED ON COMBINED_DATA")
            cursor.execute(f"ALTER USER {schema} QUOTA UNLIMITED ON COMBINED_IDX")
        # FINANCE needs CREATE MATERIALIZED VIEW (not included in RESOURCE role)
        # and CREATE TABLE granted DIRECTLY (the RESOURCE role grant is not
        # honoured during MV creation in another schema).
        cursor.execute("GRANT CREATE MATERIALIZED VIEW TO FINANCE")
        cursor.execute("GRANT CREATE TABLE TO FINANCE")
    conn.commit()


# ---------------------------------------------------------------------------
# DDL: sequences
# ---------------------------------------------------------------------------


def create_sequences(conn: oracledb.Connection) -> None:
    """One sequence per schema. START WITH 100 so they don't collide with
    the explicitly-inserted test IDs (all < 100)."""
    execute_stmts(
        conn,
        (
            "CREATE SEQUENCE HRDATA.HR_SEQ          START WITH 100 INCREMENT BY 1 NOCACHE",
            "CREATE SEQUENCE INVENTORY.INV_SEQ  START WITH 100 INCREMENT BY 1 NOCACHE",
            "CREATE SEQUENCE FINANCE.FIN_SEQ    START WITH 100 INCREMENT BY 1 NOCACHE",
            "CREATE SEQUENCE AUDITLOG.AUD_SEQ      START WITH 100 INCREMENT BY 1 NOCACHE",
        ),
    )


# ---------------------------------------------------------------------------
# DDL: tables  (dependency order: HRDATA → INVENTORY → FINANCE → AUDITLOG)
# ---------------------------------------------------------------------------


def create_tables(conn: oracledb.Connection) -> None:
    # Phase 1: HRDATA + INVENTORY (no cross-schema FKs).
    execute_stmts(
        conn,
        (
            # ------------------------------------------------------------------
            # HRDATA.DEPARTMENTS — simple reference table, no FK dependencies
            # ------------------------------------------------------------------
            """
            CREATE TABLE HRDATA.DEPARTMENTS (
                DEPT_ID     NUMBER(6)     NOT NULL,
                DEPT_NAME   VARCHAR2(60)  NOT NULL,
                LOCATION    VARCHAR2(100),
                MANAGER_ID  NUMBER(6),
                CONSTRAINT DEPARTMENTS_PK PRIMARY KEY (DEPT_ID)
            ) TABLESPACE COMBINED_DATA
            """,
            # ------------------------------------------------------------------
            # HRDATA.JOBS — simple reference table, VARCHAR2 primary key
            # ------------------------------------------------------------------
            """
            CREATE TABLE HRDATA.JOBS (
                JOB_ID     VARCHAR2(10) NOT NULL,
                JOB_TITLE  VARCHAR2(50) NOT NULL,
                MIN_SALARY NUMBER(8, 2),
                MAX_SALARY NUMBER(8, 2),
                CONSTRAINT JOBS_PK PRIMARY KEY (JOB_ID)
            ) TABLESPACE COMBINED_DATA
            """,
            # ------------------------------------------------------------------
            # HRDATA.EMPLOYEES — INTERVAL YEAR TO MONTH, NCLOB,
            #                intra-schema FKs, check constraint
            # ------------------------------------------------------------------
            """
            CREATE TABLE HRDATA.EMPLOYEES (
                EMP_ID         NUMBER(6)           NOT NULL,
                FIRST_NAME     VARCHAR2(40)        NOT NULL,
                LAST_NAME      VARCHAR2(40)        NOT NULL,
                EMAIL          VARCHAR2(80)        NOT NULL,
                PHONE          NVARCHAR2(20),
                HIRE_DATE      DATE                NOT NULL,
                JOB_ID         VARCHAR2(10),
                SALARY         NUMBER(8, 2),
                COMMISSION_PCT NUMBER(2, 2),
                DEPT_ID        NUMBER(6),
                STATUS         CHAR(1)             NOT NULL,
                TENURE         INTERVAL YEAR(2) TO MONTH,
                BIO            NCLOB,
                CONSTRAINT EMPLOYEES_PK   PRIMARY KEY (EMP_ID),
                CONSTRAINT EMP_JOB_FK     FOREIGN KEY (JOB_ID)
                    REFERENCES HRDATA.JOBS(JOB_ID),
                CONSTRAINT EMP_DEPT_FK    FOREIGN KEY (DEPT_ID)
                    REFERENCES HRDATA.DEPARTMENTS(DEPT_ID),
                CONSTRAINT EMP_STATUS_CK  CHECK (STATUS IN ('A', 'I', 'P'))
            ) TABLESPACE COMBINED_DATA
            """,
            # ------------------------------------------------------------------
            # INVENTORY.WAREHOUSES
            # ------------------------------------------------------------------
            """
            CREATE TABLE INVENTORY.WAREHOUSES (
                WAREHOUSE_ID   NUMBER(4)    NOT NULL,
                WAREHOUSE_NAME VARCHAR2(60) NOT NULL,
                CITY           VARCHAR2(60),
                COUNTRY_CODE   CHAR(2),
                CAPACITY       NUMBER(10),
                CONSTRAINT WAREHOUSES_PK PRIMARY KEY (WAREHOUSE_ID)
            ) TABLESPACE COMBINED_DATA
            """,
            # ------------------------------------------------------------------
            # INVENTORY.PRODUCTS — LIST-partitioned by REGION,
            #                      BINARY_DOUBLE and BINARY_FLOAT columns
            # ------------------------------------------------------------------
            """
            CREATE TABLE INVENTORY.PRODUCTS (
                PRODUCT_ID   NUMBER(8)      NOT NULL,
                PRODUCT_NAME VARCHAR2(80)   NOT NULL,
                CATEGORY     VARCHAR2(30),
                REGION       VARCHAR2(10)   NOT NULL,
                UNIT_PRICE   NUMBER(12, 4)  NOT NULL,
                WEIGHT_KG    NUMBER(8, 4),
                ACTIVE_FLAG  CHAR(1),
                CREATED_DATE DATE,
                CONSTRAINT PRODUCTS_PK PRIMARY KEY (PRODUCT_ID)
            ) TABLESPACE COMBINED_DATA
            PARTITION BY LIST (REGION) (
                PARTITION P_NORTH VALUES ('NORTH'),
                PARTITION P_SOUTH VALUES ('SOUTH'),
                PARTITION P_INTL  VALUES ('INTL')
            )
            """,
            # ------------------------------------------------------------------
            # INVENTORY.STOCK_LEVELS — intra-schema FKs to PRODUCTS + WAREHOUSES
            # ------------------------------------------------------------------
            """
            CREATE TABLE INVENTORY.STOCK_LEVELS (
                STOCK_ID      NUMBER(10) NOT NULL,
                PRODUCT_ID    NUMBER(8)  NOT NULL,
                WAREHOUSE_ID  NUMBER(4)  NOT NULL,
                QUANTITY      NUMBER(10) NOT NULL,
                LAST_UPDATED  TIMESTAMP  NOT NULL,
                REORDER_LEVEL NUMBER(10),
                CONSTRAINT STOCK_LEVELS_PK    PRIMARY KEY (STOCK_ID),
                CONSTRAINT STOCK_PRODUCT_FK   FOREIGN KEY (PRODUCT_ID)
                    REFERENCES INVENTORY.PRODUCTS(PRODUCT_ID),
                CONSTRAINT STOCK_WAREHOUSE_FK FOREIGN KEY (WAREHOUSE_ID)
                    REFERENCES INVENTORY.WAREHOUSES(WAREHOUSE_ID)
            ) TABLESPACE COMBINED_DATA
            """,
        ),
    )

    # Cross-schema FK from FINANCE.ACCOUNTS to HRDATA.EMPLOYEES requires the
    # FINANCE owner to hold the REFERENCES privilege on HRDATA.EMPLOYEES.
    # (Oracle checks REFERENCES against the table OWNER, not the executor —
    # SYSTEM being a DBA is not sufficient.)
    execute_stmts(
        conn,
        ("GRANT REFERENCES ON HRDATA.EMPLOYEES TO FINANCE",),
    )

    # Phase 2: FINANCE + AUDITLOG (cross-schema FK now permitted).
    execute_stmts(
        conn,
        (
            # ------------------------------------------------------------------
            # FINANCE.ACCOUNTS — cross-schema FK to HRDATA.EMPLOYEES, check constraint
            # ------------------------------------------------------------------
            """
            CREATE TABLE FINANCE.ACCOUNTS (
                ACCOUNT_ID   NUMBER(8)     NOT NULL,
                EMP_ID       NUMBER(6)     NOT NULL,
                ACCOUNT_TYPE VARCHAR2(20)  NOT NULL,
                BALANCE      NUMBER(14, 2) NOT NULL,
                OPENED_DATE  DATE          NOT NULL,
                STATUS       VARCHAR2(10)  NOT NULL,
                NOTES        CLOB,
                CONSTRAINT ACCOUNTS_PK        PRIMARY KEY (ACCOUNT_ID),
                CONSTRAINT ACCOUNTS_EMP_FK    FOREIGN KEY (EMP_ID)
                    REFERENCES HRDATA.EMPLOYEES(EMP_ID),
                CONSTRAINT ACCOUNTS_STATUS_CK CHECK (STATUS IN ('ACTIVE', 'CLOSED', 'FROZEN'))
            ) TABLESPACE COMBINED_DATA
            """,
            # ------------------------------------------------------------------
            # FINANCE.TRANSACTIONS — RANGE-partitioned by TXN_DATE (5 partitions),
            #                        TIMESTAMP WITH TIME ZONE, INTERVAL DAY TO SECOND,
            #                        check constraint
            # ------------------------------------------------------------------
            """
            CREATE TABLE FINANCE.TRANSACTIONS (
                TXN_ID     NUMBER(10)              NOT NULL,
                ACCOUNT_ID NUMBER(8)               NOT NULL,
                TXN_DATE   DATE                    NOT NULL,
                TXN_TS     TIMESTAMP WITH TIME ZONE,
                AMOUNT     NUMBER(12, 2)           NOT NULL,
                DURATION   INTERVAL DAY(2) TO SECOND(0),
                TXN_TYPE   VARCHAR2(20),
                REFERENCE  VARCHAR2(40),
                STATUS     VARCHAR2(10),
                CONSTRAINT TRANSACTIONS_PK   PRIMARY KEY (TXN_ID),
                CONSTRAINT TXN_ACCOUNT_FK    FOREIGN KEY (ACCOUNT_ID)
                    REFERENCES FINANCE.ACCOUNTS(ACCOUNT_ID),
                CONSTRAINT TXN_AMOUNT_CK     CHECK (AMOUNT > 0)
            ) TABLESPACE COMBINED_DATA
            PARTITION BY RANGE (TXN_DATE) (
                PARTITION P_2024_Q1 VALUES LESS THAN (DATE '2024-04-01'),
                PARTITION P_2024_Q2 VALUES LESS THAN (DATE '2024-07-01'),
                PARTITION P_2024_Q3 VALUES LESS THAN (DATE '2024-10-01'),
                PARTITION P_2024_Q4 VALUES LESS THAN (DATE '2025-01-01'),
                PARTITION P_MAX     VALUES LESS THAN (MAXVALUE)
            )
            """,
            # ------------------------------------------------------------------
            # AUDITLOG.CHANGE_LOG — HASH-partitioned by USER_ID (4 buckets),
            #                    TIMESTAMP WITH LOCAL TIME ZONE
            # ------------------------------------------------------------------
            """
            CREATE TABLE AUDITLOG.CHANGE_LOG (
                LOG_ID      NUMBER(10)                     NOT NULL,
                USER_ID     NUMBER(6),
                SCHEMA_NAME VARCHAR2(30)                   NOT NULL,
                TABLE_NAME  VARCHAR2(60)                   NOT NULL,
                ROW_ID      VARCHAR2(20),
                ACTION      VARCHAR2(10)                   NOT NULL,
                CHANGED_AT  TIMESTAMP WITH LOCAL TIME ZONE NOT NULL,
                OLD_VALUES  CLOB,
                NEW_VALUES  CLOB,
                CONSTRAINT CHANGE_LOG_PK PRIMARY KEY (LOG_ID)
            ) TABLESPACE COMBINED_DATA
            PARTITION BY HASH (USER_ID) PARTITIONS 4
            """,
        ),
    )


# ---------------------------------------------------------------------------
# DDL: grants and synonyms  (must precede cross-schema views)
# ---------------------------------------------------------------------------


def create_grants_and_synonyms(conn: oracledb.Connection) -> None:
    execute_stmts(
        conn,
        (
            # FINANCE needs SELECT on HR tables for cross-schema views and FK-based queries
            "GRANT SELECT ON HRDATA.EMPLOYEES    TO FINANCE",
            "GRANT SELECT ON HRDATA.DEPARTMENTS  TO FINANCE",
            # INVENTORY needs SELECT on HR for reporting views
            "GRANT SELECT ON HRDATA.EMPLOYEES    TO INVENTORY",
            # Private synonym in FINANCE for transparent access to HRDATA.EMPLOYEES
            "CREATE SYNONYM FINANCE.EMPLOYEES FOR HRDATA.EMPLOYEES",
            # Public synonym for AUDITLOG.CHANGE_LOG
            "CREATE PUBLIC SYNONYM AUDIT_LOG FOR AUDITLOG.CHANGE_LOG",
        ),
    )


# ---------------------------------------------------------------------------
# DDL: views  (grants must be in place first)
# ---------------------------------------------------------------------------


def create_views(conn: oracledb.Connection) -> None:
    execute_stmts(
        conn,
        (
            # Simple filtered view — active employees only
            """
            CREATE VIEW HRDATA.V_ACTIVE_EMPLOYEES AS
            SELECT EMP_ID, FIRST_NAME, LAST_NAME, EMAIL, JOB_ID, DEPT_ID, HIRE_DATE
            FROM   HRDATA.EMPLOYEES
            WHERE  STATUS = 'A'
            """,
            # Reference join — employee details with job and department
            """
            CREATE VIEW HRDATA.V_EMPLOYEE_DETAILS AS
            SELECT
                e.EMP_ID,
                e.FIRST_NAME || ' ' || e.LAST_NAME AS FULL_NAME,
                e.EMAIL,
                j.JOB_TITLE,
                d.DEPT_NAME,
                e.SALARY,
                e.STATUS
            FROM       HRDATA.EMPLOYEES   e
            LEFT JOIN  HRDATA.JOBS        j  ON j.JOB_ID  = e.JOB_ID
            LEFT JOIN  HRDATA.DEPARTMENTS d  ON d.DEPT_ID = e.DEPT_ID
            """,
            # Cross-schema view in FINANCE (uses GRANT SELECT on HRDATA.EMPLOYEES)
            """
            CREATE VIEW FINANCE.V_EMPLOYEE_ACCOUNTS AS
            SELECT
                e.EMP_ID,
                e.FIRST_NAME || ' ' || e.LAST_NAME AS FULL_NAME,
                a.ACCOUNT_ID,
                a.ACCOUNT_TYPE,
                a.BALANCE,
                a.STATUS       AS ACCOUNT_STATUS,
                a.OPENED_DATE
            FROM  HRDATA.EMPLOYEES   e
            JOIN  FINANCE.ACCOUNTS a  ON a.EMP_ID = e.EMP_ID
            """,
        ),
    )


# ---------------------------------------------------------------------------
# DDL: indexes  (all land in COMBINED_IDX tablespace)
# ---------------------------------------------------------------------------


def create_indexes(conn: oracledb.Connection) -> None:
    execute_stmts(
        conn,
        (
            # Function-based unique index — exercises non-default index type
            "CREATE UNIQUE INDEX HRDATA.IDX_EMP_EMAIL_UPPER"
            "    ON HRDATA.EMPLOYEES (UPPER(EMAIL))"
            "    TABLESPACE COMBINED_IDX",
            # FK indexes in HRDATA
            "CREATE INDEX HRDATA.IDX_EMP_DEPT ON HRDATA.EMPLOYEES (DEPT_ID)"
            " TABLESPACE COMBINED_IDX",
            "CREATE INDEX HRDATA.IDX_EMP_JOB  ON HRDATA.EMPLOYEES (JOB_ID) TABLESPACE COMBINED_IDX",
            # Cross-schema FK index in FINANCE
            "CREATE INDEX FINANCE.IDX_ACCOUNTS_EMP"
            "    ON FINANCE.ACCOUNTS (EMP_ID)"
            "    TABLESPACE COMBINED_IDX",
            # Composite index for transaction range scans
            "CREATE INDEX FINANCE.IDX_TXN_ACCOUNT_DATE"
            "    ON FINANCE.TRANSACTIONS (ACCOUNT_ID, TXN_DATE)"
            "    TABLESPACE COMBINED_IDX",
            # FK indexes on INVENTORY.STOCK_LEVELS
            "CREATE INDEX INVENTORY.IDX_STOCK_PRODUCT"
            "    ON INVENTORY.STOCK_LEVELS (PRODUCT_ID)"
            "    TABLESPACE COMBINED_IDX",
            "CREATE INDEX INVENTORY.IDX_STOCK_WAREHOUSE"
            "    ON INVENTORY.STOCK_LEVELS (WAREHOUSE_ID)"
            "    TABLESPACE COMBINED_IDX",
        ),
    )


# ---------------------------------------------------------------------------
# DDL: PL/SQL objects — function, procedure, package spec + body
# ---------------------------------------------------------------------------


def create_plsql_objects(conn: oracledb.Connection) -> None:
    with conn.cursor() as cursor:
        # Deterministic helper function in HR
        cursor.execute(
            """
            CREATE OR REPLACE FUNCTION HRDATA.FULL_NAME(
                p_first IN VARCHAR2,
                p_last  IN VARCHAR2
            ) RETURN VARCHAR2 DETERMINISTIC IS
            BEGIN
                RETURN TRIM(p_first || ' ' || p_last);
            END FULL_NAME;
            """
        )

        # Audit-trail recording procedure
        cursor.execute(
            """
            CREATE OR REPLACE PROCEDURE AUDITLOG.LOG_CHANGE(
                p_schema  IN VARCHAR2,
                p_table   IN VARCHAR2,
                p_row_id  IN VARCHAR2,
                p_action  IN VARCHAR2
            ) AS
                l_log_id NUMBER;
            BEGIN
                SELECT AUDITLOG.AUD_SEQ.NEXTVAL INTO l_log_id FROM DUAL;
                INSERT INTO AUDITLOG.CHANGE_LOG (
                    LOG_ID, USER_ID, SCHEMA_NAME, TABLE_NAME,
                    ROW_ID, ACTION, CHANGED_AT
                ) VALUES (
                    l_log_id, NULL, p_schema, p_table,
                    p_row_id, p_action, SYSTIMESTAMP
                );
                COMMIT;
            END LOG_CHANGE;
            """
        )

        # Package specification
        cursor.execute(
            """
            CREATE OR REPLACE PACKAGE FINANCE.PKG_REPORTING AS
                FUNCTION  ACCOUNT_BALANCE(p_account_id IN NUMBER) RETURN NUMBER;
                PROCEDURE PRINT_SUMMARY  (p_emp_id     IN NUMBER);
            END PKG_REPORTING;
            """
        )

        # Package body
        cursor.execute(
            """
            CREATE OR REPLACE PACKAGE BODY FINANCE.PKG_REPORTING AS
                FUNCTION ACCOUNT_BALANCE(p_account_id IN NUMBER) RETURN NUMBER IS
                    l_balance NUMBER;
                BEGIN
                    SELECT BALANCE INTO l_balance
                    FROM   FINANCE.ACCOUNTS
                    WHERE  ACCOUNT_ID = p_account_id;
                    RETURN l_balance;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN RETURN NULL;
                END ACCOUNT_BALANCE;

                PROCEDURE PRINT_SUMMARY(p_emp_id IN NUMBER) IS
                    l_count NUMBER;
                    l_total NUMBER;
                BEGIN
                    SELECT COUNT(*), SUM(a.BALANCE)
                    INTO   l_count, l_total
                    FROM   FINANCE.ACCOUNTS a
                    WHERE  a.EMP_ID = p_emp_id;
                    DBMS_OUTPUT.PUT_LINE(
                        'Emp ' || p_emp_id
                        || ': accounts='   || l_count
                        || ', total_bal='  || l_total
                    );
                END PRINT_SUMMARY;
            END PKG_REPORTING;
            """
        )
    conn.commit()


# ---------------------------------------------------------------------------
# DDL: BEFORE INSERT triggers — fire sequences when PK is NULL
# ---------------------------------------------------------------------------


def create_triggers(conn: oracledb.Connection) -> None:
    # (schema, table, pk_column, sequence_expr)
    # HRDATA.JOBS uses a VARCHAR2 PK so no sequence trigger is applicable.
    trigger_specs = (
        ("HRDATA", "DEPARTMENTS", "DEPT_ID", "HRDATA.HR_SEQ"),
        ("HRDATA", "EMPLOYEES", "EMP_ID", "HRDATA.HR_SEQ"),
        ("INVENTORY", "WAREHOUSES", "WAREHOUSE_ID", "INVENTORY.INV_SEQ"),
        ("INVENTORY", "PRODUCTS", "PRODUCT_ID", "INVENTORY.INV_SEQ"),
        ("INVENTORY", "STOCK_LEVELS", "STOCK_ID", "INVENTORY.INV_SEQ"),
        ("FINANCE", "ACCOUNTS", "ACCOUNT_ID", "FINANCE.FIN_SEQ"),
        ("FINANCE", "TRANSACTIONS", "TXN_ID", "FINANCE.FIN_SEQ"),
        ("AUDITLOG", "CHANGE_LOG", "LOG_ID", "AUDITLOG.AUD_SEQ"),
    )
    with conn.cursor() as cursor:
        for schema, table, pk_col, seq in trigger_specs:
            trigger_name = f"TRG_{table}_BI"
            cursor.execute(
                f"""
                CREATE OR REPLACE TRIGGER {schema}.{trigger_name}
                BEFORE INSERT ON {schema}.{table}
                FOR EACH ROW
                WHEN (NEW.{pk_col} IS NULL)
                BEGIN
                    SELECT {seq}.NEXTVAL INTO :NEW.{pk_col} FROM DUAL;
                END {trigger_name};
                """
            )
    conn.commit()


# ---------------------------------------------------------------------------
# DML: data insertion
# ---------------------------------------------------------------------------


def insert_hr_data(conn: oracledb.Connection) -> None:
    dept_rows = [
        (10, "Engineering", "San Francisco", None),
        (20, "Sales", "New York", None),
        (30, "Finance", "Chicago", None),
        (40, "Operations", "Austin", None),
        (50, "Human Resources", "Boston", None),
    ]
    job_rows = [
        ("SE", "Software Engineer", 70000.00, 150000.00),
        ("SSE", "Senior Software Engineer", 100000.00, 200000.00),
        ("EM", "Engineering Manager", 130000.00, 220000.00),
        ("SA", "Sales Associate", 50000.00, 100000.00),
        ("SM", "Sales Manager", 80000.00, 150000.00),
        ("FA", "Financial Analyst", 65000.00, 120000.00),
        ("FM", "Finance Manager", 95000.00, 170000.00),
        ("OA", "Operations Analyst", 60000.00, 110000.00),
        ("HRA", "HR Analyst", 55000.00, 100000.00),
        ("DIR", "Director", 150000.00, 250000.00),
    ]

    job_cycle = ["SE", "SSE", "EM", "SA", "SM", "FA", "FM", "OA", "HRA", "DIR"]
    dept_cycle = [10, 20, 30, 40, 50]
    status_cycle = ["A", "A", "A", "A", "I", "A", "A", "A", "P", "A"]

    emp_rows = []
    for idx in range(1, 31):
        dept_id = dept_cycle[(idx - 1) % len(dept_cycle)]
        job_id = job_cycle[(idx - 1) % len(job_cycle)]
        status = status_cycle[(idx - 1) % len(status_cycle)]
        salary = round(50000.0 + (idx * 2500) % 100000, 2)
        commission = round(0.05 + (idx % 10) * 0.01, 2) if idx % 3 == 0 else None
        tenure_mo = 12 + ((idx - 1) * 4) % 84  # 12–95 months
        dept_name = ["Engineering", "Sales", "Finance", "Operations", "HR"][(idx - 1) % 5]
        bio = (f"Employee {idx} works in {dept_name}. " * 3).strip()
        hire_offset = (idx * 11) % 365 + 1
        emp_rows.append(
            (
                idx,
                f"First{idx}",
                f"Last{idx}",
                f"emp{idx:03d}@company.example",
                f"+1-555-{idx:04d}",
                hire_offset,
                job_id,
                salary,
                commission,
                dept_id,
                status,
                tenure_mo,
                bio,
            )
        )

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO HRDATA.DEPARTMENTS (DEPT_ID, DEPT_NAME, LOCATION, MANAGER_ID)
            VALUES (:1, :2, :3, :4)
            """,
            dept_rows,
        )
        cursor.executemany(
            """
            INSERT INTO HRDATA.JOBS (JOB_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY)
            VALUES (:1, :2, :3, :4)
            """,
            job_rows,
        )
        cursor.executemany(
            """
            INSERT INTO HRDATA.EMPLOYEES (
                EMP_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
                HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT,
                DEPT_ID, STATUS, TENURE, BIO
            ) VALUES (
                :1, :2, :3, :4, :5,
                DATE '2020-01-01' + :6, :7, :8, :9,
                :10, :11, NUMTOYMINTERVAL(:12, 'MONTH'), :13
            )
            """,
            emp_rows,
        )
    conn.commit()


def insert_inventory_data(conn: oracledb.Connection) -> None:
    warehouse_rows = [
        (1, "North Warehouse", "Minneapolis", "US", 50000),
        (2, "South Warehouse", "Atlanta", "US", 40000),
        (3, "International Hub", "London", "GB", 30000),
    ]

    categories = [
        "ELECTRONICS",
        "CLOTHING",
        "FOOD",
        "FURNITURE",
        "TOOLS",
        "BOOKS",
        "SPORTS",
        "TOYS",
    ]
    # 8 products per LIST partition: NORTH / SOUTH / INTL
    regions = ["NORTH"] * 8 + ["SOUTH"] * 8 + ["INTL"] * 8

    product_rows = []
    for idx in range(1, 25):
        category = categories[(idx - 1) % len(categories)]
        region = regions[idx - 1]
        unit_price = round(9.99 + idx * 7.13, 2)  # stored as BINARY_DOUBLE
        weight_kg = round(0.1 + (idx * 0.45) % 25.0, 2)  # stored as BINARY_FLOAT
        active = "Y" if idx % 7 != 0 else "N"
        product_rows.append(
            (
                idx,
                f"Product-{idx:03d} {category.title()}",
                category,
                region,
                unit_price,
                weight_kg,
                active,
                (idx * 11) % 365,  # created_date offset from 2023-01-01
            )
        )

    # 45 stock-level rows; product_id and warehouse_id cycle independently
    stock_rows = []
    for idx in range(1, 46):
        product_id = (idx - 1) % 24 + 1
        warehouse_id = (idx - 1) % 3 + 1
        quantity = 50 + (idx * 13) % 500
        reorder_lvl = 20 + idx % 30
        day_offset = idx % 30 + 1
        stock_rows.append((idx, product_id, warehouse_id, quantity, day_offset, reorder_lvl))

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO INVENTORY.WAREHOUSES (
                WAREHOUSE_ID, WAREHOUSE_NAME, CITY, COUNTRY_CODE, CAPACITY
            ) VALUES (:1, :2, :3, :4, :5)
            """,
            warehouse_rows,
        )
        cursor.executemany(
            """
            INSERT INTO INVENTORY.PRODUCTS (
                PRODUCT_ID, PRODUCT_NAME, CATEGORY, REGION,
                UNIT_PRICE, WEIGHT_KG, ACTIVE_FLAG, CREATED_DATE
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, DATE '2023-01-01' + :8
            )
            """,
            product_rows,
        )
        cursor.executemany(
            """
            INSERT INTO INVENTORY.STOCK_LEVELS (
                STOCK_ID, PRODUCT_ID, WAREHOUSE_ID, QUANTITY, LAST_UPDATED, REORDER_LEVEL
            ) VALUES (
                :1, :2, :3, :4,
                TIMESTAMP '2024-06-01 00:00:00' + NUMTODSINTERVAL(:5, 'DAY'),
                :6
            )
            """,
            stock_rows,
        )
    conn.commit()


def insert_finance_data(conn: oracledb.Connection) -> None:
    account_types = ["CHECKING", "SAVINGS", "CREDIT"]
    account_status = [
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "FROZEN",
    ]

    account_rows = []
    for idx in range(1, 21):
        acct_type = account_types[(idx - 1) % len(account_types)]
        balance = round(1000.0 + idx * 247.53, 2)
        status = account_status[(idx - 1) % len(account_status)]
        note = f"Account {idx}: type={acct_type}, opened for employee {idx}."
        account_rows.append(
            (
                idx,
                idx,  # EMP_ID 1-20 (all map to existing employees)
                acct_type,
                balance,
                (idx * 17) % 365 + 1,  # opened_date offset from 2022-01-01
                status,
                note,
            )
        )

    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "FEE", "INTEREST"]
    txn_status = ["SETTLED", "SETTLED", "SETTLED", "PENDING", "SETTLED"]

    txn_rows = []
    for idx in range(1, 101):
        account_id = (idx - 1) % 20 + 1
        # Spread 100 rows across ~400 days → hits all 5 RANGE partitions
        days_offset = (idx - 1) * 4
        hours_offset = days_offset * 24 + idx % 24
        amount = round(10.0 + (idx * 37.41) % 2000.0, 2)
        duration_secs = 30 + (idx * 7) % 300  # 30–329 seconds of "processing time"
        txn_type = txn_types[(idx - 1) % len(txn_types)]
        status = txn_status[(idx - 1) % len(txn_status)]
        reference = f"REF-{idx:06d}-{txn_type[:3]}"
        txn_rows.append(
            (
                idx,
                account_id,
                days_offset,  # TXN_DATE  = DATE '2024-01-01' + days_offset
                hours_offset,  # TXN_TS    = FROM_TZ(... + hours, 'UTC')
                amount,
                duration_secs,  # DURATION  = NUMTODSINTERVAL(n, 'SECOND')
                txn_type,
                reference,
                status,
            )
        )

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO FINANCE.ACCOUNTS (
                ACCOUNT_ID, EMP_ID, ACCOUNT_TYPE, BALANCE,
                OPENED_DATE, STATUS, NOTES
            ) VALUES (
                :1, :2, :3, :4, DATE '2022-01-01' + :5, :6, :7
            )
            """,
            account_rows,
        )
        cursor.executemany(
            """
            INSERT INTO FINANCE.TRANSACTIONS (
                TXN_ID, ACCOUNT_ID, TXN_DATE, TXN_TS,
                AMOUNT, DURATION, TXN_TYPE, REFERENCE, STATUS
            ) VALUES (
                :1, :2,
                DATE '2024-01-01' + :3,
                FROM_TZ(TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(:4, 'HOUR'), 'UTC'),
                :5,
                NUMTODSINTERVAL(:6, 'SECOND'),
                :7, :8, :9
            )
            """,
            txn_rows,
        )
    conn.commit()


def insert_audit_data(conn: oracledb.Connection) -> None:
    schema_names = ["HRDATA", "INVENTORY", "FINANCE", "AUDITLOG"]
    table_names = ["EMPLOYEES", "PRODUCTS", "TRANSACTIONS", "CHANGE_LOG"]
    actions = ["INSERT", "UPDATE", "DELETE"]

    log_rows = []
    for idx in range(1, 51):
        user_id = ((idx - 1) % 30) + 1  # cycles through employee IDs 1–30
        schema_name = schema_names[(idx - 1) % len(schema_names)]
        table_name = table_names[(idx - 1) % len(table_names)]
        action = actions[(idx - 1) % len(actions)]
        row_id = f"{schema_name[:2]}{idx:06d}"
        min_offset = idx * 30  # 30-minute intervals → TIMESTAMP WITH LOCAL TIME ZONE
        old_vals = f'{{"id": {idx}, "status": "OLD"}}' if action != "INSERT" else None
        new_vals = f'{{"id": {idx}, "status": "NEW"}}' if action != "DELETE" else None
        log_rows.append(
            (
                idx,
                user_id,
                schema_name,
                table_name,
                row_id,
                action,
                min_offset,
                old_vals,
                new_vals,
            )
        )

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO AUDITLOG.CHANGE_LOG (
                LOG_ID, USER_ID, SCHEMA_NAME, TABLE_NAME,
                ROW_ID, ACTION, CHANGED_AT, OLD_VALUES, NEW_VALUES
            ) VALUES (
                :1, :2, :3, :4, :5, :6,
                CAST(
                    TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(:7, 'MINUTE')
                    AS TIMESTAMP WITH LOCAL TIME ZONE
                ),
                :8, :9
            )
            """,
            log_rows,
        )
    conn.commit()


# ---------------------------------------------------------------------------
# DDL: materialized view  (created after data so BUILD IMMEDIATE has rows)
# ---------------------------------------------------------------------------


def create_materialized_view(conn: oracledb.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE MATERIALIZED VIEW FINANCE.MV_ACCOUNT_SUMMARY
            BUILD IMMEDIATE
            REFRESH COMPLETE ON DEMAND
            AS
            SELECT
                a.ACCOUNT_ID,
                a.EMP_ID,
                a.ACCOUNT_TYPE,
                COUNT(t.TXN_ID)  AS TXN_COUNT,
                SUM(t.AMOUNT)    AS TOTAL_AMOUNT,
                MAX(t.TXN_DATE)  AS LAST_TXN_DATE
            FROM       FINANCE.ACCOUNTS     a
            LEFT JOIN  FINANCE.TRANSACTIONS t  ON t.ACCOUNT_ID = a.ACCOUNT_ID
            GROUP BY   a.ACCOUNT_ID, a.EMP_ID, a.ACCOUNT_TYPE
            """
        )
    conn.commit()


def gather_stats(conn: oracledb.Connection) -> None:
    with conn.cursor() as cursor:
        for schema in SCHEMAS:
            cursor.callproc("DBMS_STATS.GATHER_SCHEMA_STATS", [schema])
    conn.commit()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def create_sample_database(admin: OracleAdminConnection) -> None:
    with connect_admin(admin) as conn:
        LOGGER.info("  Creating tablespaces...")
        create_tablespaces(conn)
        LOGGER.info("  Resetting schemas...")
        reset_schemas(conn)
        LOGGER.info("  Creating sequences...")
        create_sequences(conn)
        LOGGER.info("  Creating tables...")
        create_tables(conn)
        LOGGER.info("  Creating grants and synonyms...")
        create_grants_and_synonyms(conn)
        LOGGER.info("  Creating views...")
        create_views(conn)
        LOGGER.info("  Creating indexes...")
        create_indexes(conn)
        LOGGER.info("  Creating PL/SQL objects...")
        create_plsql_objects(conn)
        LOGGER.info("  Creating triggers...")
        create_triggers(conn)
        LOGGER.info("  Inserting HR data...")
        insert_hr_data(conn)
        LOGGER.info("  Inserting INVENTORY data...")
        insert_inventory_data(conn)
        LOGGER.info("  Inserting FINANCE data...")
        insert_finance_data(conn)
        LOGGER.info("  Inserting AUDIT data...")
        insert_audit_data(conn)
        LOGGER.info("  Creating materialized view...")
        create_materialized_view(conn)
        LOGGER.info("  Gathering statistics...")
        gather_stats(conn)


# ---------------------------------------------------------------------------
# Export: modern (expdp)
# ---------------------------------------------------------------------------


def export_modern_dump(
    *,
    container: DockerOracle,
    admin: OracleAdminConnection,
    output_dir: Path,
) -> None:
    with connect_admin(admin) as conn:
        create_directory(conn, DUMP_DIRECTORY, CONTAINER_DUMP_PATH)

    with tempfile.TemporaryDirectory(prefix="oracle-combined-modern-") as tmp:
        runner = DataPumpRunner(container, Path(tmp))
        runner.run_expdp(
            ExportJob(
                connection=OracleCredentials(admin.user, admin.password, admin.service),
                directory=DUMP_DIRECTORY,
                dumpfile=MODERN_DUMPFILE,
                logfile=MODERN_LOGFILE,
                include_schemas=SCHEMAS,
            )
        )
        container.exec(
            [
                "bash",
                "-lc",
                f"chmod a+r {CONTAINER_DUMP_PATH}/{MODERN_DUMPFILE} "
                f"{CONTAINER_DUMP_PATH}/{MODERN_LOGFILE}",
            ],
            check=False,
        )

    if not (output_dir / MODERN_DUMPFILE).exists():
        msg = f"expdp finished but {output_dir / MODERN_DUMPFILE} was not created"
        raise FileNotFoundError(msg)


# ---------------------------------------------------------------------------
# Export: legacy (exp)
# ---------------------------------------------------------------------------


def export_legacy_dump(
    *,
    container: DockerOracle,
    admin: OracleAdminConnection,
    output_dir: Path,
) -> None:
    conn_spec = OracleCredentials(user=admin.user, password=admin.password, service=admin.service)
    job = LegacyExportJob(
        connection=conn_spec,
        files=(f"{CONTAINER_DUMP_PATH}/{LEGACY_DUMPFILE}",),
        logfile=f"{CONTAINER_DUMP_PATH}/{LEGACY_LOGFILE}",
        owner=SCHEMAS,
        rows=True,
        indexes=True,
        grants=True,
        compress=False,
    )
    parfile_text = render_legacy_export_parfile(job)
    par_name = f"exp-combined-{uuid.uuid4().hex}.par"

    with tempfile.TemporaryDirectory(prefix="oracle-combined-legacy-") as tmp:
        local_par = Path(tmp) / par_name
        local_par.write_text(parfile_text)
        remote_par = f"/tmp/{par_name}"
        container.copy_to(local_par, remote_par)

        result = container.exec(["exp", f"parfile={remote_par}"], check=False)
        output = result.stdout + result.stderr
        LOGGER.info(output)
        if result.returncode != 0:
            LOGGER.error("exp failed — see log output above")
            sys.exit(1)

        container.exec(
            [
                "bash",
                "-lc",
                f"chmod a+r {CONTAINER_DUMP_PATH}/{LEGACY_DUMPFILE} "
                f"{CONTAINER_DUMP_PATH}/{LEGACY_LOGFILE}",
            ],
            check=False,
        )

    if not (output_dir / LEGACY_DUMPFILE).exists():
        msg = f"exp finished but {output_dir / LEGACY_DUMPFILE} was not created"
        raise FileNotFoundError(msg)


# ---------------------------------------------------------------------------
# Post-export artefacts
# ---------------------------------------------------------------------------


def write_config(output_dir: Path, oracle_image: str) -> None:
    config = {
        "oracle": {"image": oracle_image},
    }
    (output_dir / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))


def write_notes(output_dir: Path) -> None:
    (output_dir / "README.md").write_text(
        f"""\
# Full Combined Oracle Dump

Two dump files produced from a single Oracle database instance.

## Dump Files

| File | Format |
|------|--------|
| `{MODERN_DUMPFILE}` | Oracle Data Pump (`expdp`) |
| `{LEGACY_DUMPFILE}` | Legacy `exp` |

> **Note (legacy dump):** `exp` cannot export cross-schema referential
> constraints. It will log a warning for the FK
> `FINANCE.ACCOUNTS → HRDATA.EMPLOYEES` — this is expected behaviour.

---

## Custom Tablespaces

| Tablespace | Purpose |
|------------|---------|
| `COMBINED_DATA` | All application tables |
| `COMBINED_IDX`  | All user-created indexes |

---

## Schemas and Tables

| Schema | Table | Partition type | Notable column types / features |
|--------|-------|----------------|----------------------------------|
| HRDATA | DEPARTMENTS | — | Simple reference |
| HRDATA | JOBS | — | VARCHAR2 primary key |
| HRDATA | EMPLOYEES | — | `INTERVAL YEAR TO MONTH`, `NCLOB`, check + FK constraints |
| INVENTORY | WAREHOUSES | — | |
| INVENTORY | PRODUCTS | **LIST** (REGION) | `NUMBER(12,4)` unit price, `NUMBER(8,4)` weight |
| INVENTORY | STOCK_LEVELS | — | FK to PRODUCTS and WAREHOUSES |
| FINANCE | ACCOUNTS | — | Cross-schema FK to HRDATA.EMPLOYEES |
| FINANCE | TRANSACTIONS | **RANGE** (TXN_DATE, 5 parts) | `TIMESTAMP WITH TZ`, `INTERVAL DS` |
| AUDITLOG | CHANGE_LOG | **HASH** (USER_ID, 4 buckets) | `TIMESTAMP WITH LOCAL TIME ZONE` |

---

## Row Counts

| Table | Rows |
|-------|------|
| HRDATA.DEPARTMENTS | 5 |
| HRDATA.JOBS | 10 |
| HRDATA.EMPLOYEES | 30 |
| INVENTORY.WAREHOUSES | 3 |
| INVENTORY.PRODUCTS | 24 (8 per partition) |
| INVENTORY.STOCK_LEVELS | 45 |
| FINANCE.ACCOUNTS | 20 |
| FINANCE.TRANSACTIONS | 100 (across 5 partitions) |
| AUDITLOG.CHANGE_LOG | 50 |

---

## PL/SQL Objects

- **Sequences:** `HRDATA.HR_SEQ`, `INVENTORY.INV_SEQ`, `FINANCE.FIN_SEQ`, `AUDITLOG.AUD_SEQ`
- **Triggers:** `BEFORE INSERT` on every table (fires sequence when PK is NULL)
- **Views:** `HRDATA.V_ACTIVE_EMPLOYEES`, `HRDATA.V_EMPLOYEE_DETAILS`,
  `FINANCE.V_EMPLOYEE_ACCOUNTS` (cross-schema join)
- **Materialized View:** `FINANCE.MV_ACCOUNT_SUMMARY` (complete refresh, built on creation)
- **Function:** `HRDATA.FULL_NAME(p_first, p_last) RETURN VARCHAR2 DETERMINISTIC`
- **Procedure:** `AUDITLOG.LOG_CHANGE(p_schema, p_table, p_row_id, p_action)`
- **Package:** `FINANCE.PKG_REPORTING` — spec + body (`ACCOUNT_BALANCE`, `PRINT_SUMMARY`)

---

## Indexes  (all in COMBINED_IDX tablespace)

| Index | Type | Columns |
|-------|------|---------|
| `HRDATA.IDX_EMP_EMAIL_UPPER` | Function-based unique | `UPPER(EMAIL)` |
| `HRDATA.IDX_EMP_DEPT` | B-tree | `DEPT_ID` |
| `HRDATA.IDX_EMP_JOB` | B-tree | `JOB_ID` |
| `FINANCE.IDX_ACCOUNTS_EMP` | B-tree | `EMP_ID` |
| `FINANCE.IDX_TXN_ACCOUNT_DATE` | Composite B-tree | `(ACCOUNT_ID, TXN_DATE)` |
| `INVENTORY.IDX_STOCK_PRODUCT` | B-tree | `PRODUCT_ID` |
| `INVENTORY.IDX_STOCK_WAREHOUSE` | B-tree | `WAREHOUSE_ID` |

---

## Grants and Synonyms

- `FINANCE` granted `SELECT` on `HRDATA.EMPLOYEES` and `HRDATA.DEPARTMENTS`
- `INVENTORY` granted `SELECT` on `HRDATA.EMPLOYEES`
- `FINANCE.EMPLOYEES` — private synonym for `HRDATA.EMPLOYEES`
- `PUBLIC.AUDIT_LOG` — public synonym for `AUDITLOG.CHANGE_LOG`

---

## Convert the Modern Dump

```bash
uv run oracle-dmp-converter inspect \\
  --dump {output_dir}/{MODERN_DUMPFILE} \\
  --work-dir {output_dir}/work \\
  --output {output_dir}/work/manifest.json

uv run oracle-dmp-converter plan \\
  --manifest {output_dir}/work/manifest.json \\
  --config {output_dir}/config.yaml \\
  --output {output_dir}/work/plan.yaml

uv run oracle-dmp-converter convert \\
  --plan {output_dir}/work/plan.yaml \\
  --output {output_dir}/parquet
```
"""
    )


def print_summary(output_dir: Path) -> None:
    LOGGER.info("Modern dump : %s", output_dir / MODERN_DUMPFILE)
    LOGGER.info("Legacy dump : %s", output_dir / LEGACY_DUMPFILE)
    LOGGER.info("Config      : %s", output_dir / "config.yaml")
    LOGGER.info("Notes       : %s", output_dir / "README.md")
    LOGGER.info(
        "Rows: departments=5, jobs=10, employees=30, warehouses=3, "
        "products=24, stock_levels=45, accounts=20, transactions=100, change_log=50"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    output_dir = args.output_dir.resolve()

    if not docker_available():
        msg = "Docker is not available. Start Docker Desktop or the Docker daemon first."
        raise RuntimeError(msg)

    prepare_output_dir(output_dir, args.force)

    with DockerOracle.start(
        image=args.oracle_image,
        password=args.oracle_password,
        mounts=((output_dir, CONTAINER_DUMP_PATH, "rw"),),
    ) as container:
        LOGGER.info("Started Oracle container %s; waiting for readiness...", container.name)
        container.wait_ready(timeout_seconds=300)

        admin = OracleAdminConnection(
            host="localhost",
            port=container.mapped_port(),
            service=container.service,
            user="system",
            password=args.oracle_password,
        )

        LOGGER.info("Building sample database...")
        create_sample_database(admin)

        LOGGER.info("Exporting Data Pump (modern) dump...")
        export_modern_dump(container=container, admin=admin, output_dir=output_dir)

        LOGGER.info("Exporting legacy exp dump...")
        export_legacy_dump(container=container, admin=admin, output_dir=output_dir)

    write_config(output_dir, args.oracle_image)
    write_notes(output_dir)
    print_summary(output_dir)


if __name__ == "__main__":
    main()
