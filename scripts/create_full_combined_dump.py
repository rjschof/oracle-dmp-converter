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
- Non-fatal legacy imp code fixtures (see notes below)

Legacy imp non-fatal code fixtures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Three intentional objects ensure that a fresh ``imp`` run of the legacy dump
produces specific non-fatal IMP/ORA codes, validating the permissive error-
handling path in ``datapump/legacy/workflow.py``:

``FINANCE.AUDIT_HOOKS`` + ``FINANCE.TRG_AUDIT_HOOKS_LOG``
  An AFTER INSERT trigger that calls ``AUDITLOG.LOG_CHANGE``.  In the
  source DB the reference resolves.  During ``imp FROMUSER=FINANCE
  TOUSER=DMP_FINANCE`` the staging instance has no ``AUDITLOG`` schema, so
  the trigger compiles INVALID and ``imp`` emits:

  * ``IMP-00403`` (object created with compilation warnings)
  * ``IMP-00041`` (object altered with compilation warnings, on the
    recompile sweep at the end of import)
  * ``ORA-04043`` (object AUDITLOG.LOG_CHANGE does not exist)

  ``AUDIT_HOOKS`` has no rows, so the invalid trigger never fires during
  data import — ``IMP-00098`` is *not* produced via this path.

``FINANCE.MV_ACCOUNT_SNAPSHOT`` + materialized view log on ``FINANCE.ACCOUNTS``
  A fast-refresh (``REFRESH FAST ON DEMAND``) row-level copy of
  ``FINANCE.ACCOUNTS``, backed by a materialized view log.
  ``exp`` captures both the ``MLOG$_ACCOUNTS`` log-table and the MV DDL,
  but does **not** export the internal replication-catalog rows
  (``SYS.MLOG$``, etc.).  When ``imp`` creates the fast-refresh MV and
  tries to register it against the imported MV log, the catalog lookup
  fails and ``imp`` emits:

  * ``ORA-23308`` (object FINANCE.MLOG$_ACCOUNTS of type MATERIALIZED VIEW
    LOG does not exist or is invalid)

  The internal MV-log management triggers that Oracle auto-creates
  (``DML_MV_*``) may also be absent after the incomplete catalog setup,
  producing:

  * ``ORA-04080`` (trigger DML_MV_xxx does not exist)

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
import tempfile
import uuid
from collections.abc import Iterable
from contextlib import AbstractContextManager
from pathlib import Path

import oracledb
import yaml

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    render_legacy_export_parfile,
)
from oracle_dmp_converter.datapump.modern.parfile import ExportJob
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner
from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    create_directory,
    drop_schema,
    ensure_schema,
    execute_ignore,
    oracle_connection,
)
from oracle_dmp_converter.runtime.admin import OracleAdminConnection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle, docker_available

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
        tenure_mo = 12 + ((idx - 1) * 4) % 84  # 12-95 months
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
        duration_secs = 30 + (idx * 7) % 300  # 30-329 seconds of "processing time"
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
        user_id = ((idx - 1) % 30) + 1  # cycles through employee IDs 1-30
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

    # -----------------------------------------------------------------------
    # MV log + fast-refresh snapshot MV — non-fatal imp code fixtures
    #
    # During legacy imp FROMUSER=FINANCE TOUSER=DMP_FINANCE the internal
    # replication-catalog rows (SYS.MLOG$ etc.) are not carried over by exp.
    # imp therefore fails to complete the fast-refresh registration and emits
    # ORA-23308 (and potentially ORA-04080 for the auto-created DML_MV_*
    # trigger).  Both are handled as known-non-fatal by the converter.
    # -----------------------------------------------------------------------
    _try_ddl(
        conn,
        "FINANCE MV log + fast-refresh snapshot (ORA-23308 / ORA-04080 fixture)",
        [
            # MV log with rowid + PK columns — required for fast refresh.
            """
            CREATE MATERIALIZED VIEW LOG ON FINANCE.ACCOUNTS
            WITH PRIMARY KEY, ROWID
                 (EMP_ID, ACCOUNT_TYPE, BALANCE, STATUS, OPENED_DATE)
            INCLUDING NEW VALUES
            """,
            # Simple row-level fast-refresh copy: no aggregation so the
            # only requirement is the MV log above.
            """
            CREATE MATERIALIZED VIEW FINANCE.MV_ACCOUNT_SNAPSHOT
            BUILD IMMEDIATE
            REFRESH FAST ON DEMAND
            AS
            SELECT ACCOUNT_ID, EMP_ID, ACCOUNT_TYPE, BALANCE, STATUS, OPENED_DATE
            FROM   FINANCE.ACCOUNTS
            """,
        ],
    )


def gather_stats(conn: oracledb.Connection) -> None:
    with conn.cursor() as cursor:
        for schema in SCHEMAS:
            cursor.callproc("DBMS_STATS.GATHER_SCHEMA_STATS", [schema])
    conn.commit()


# ---------------------------------------------------------------------------
# Audit-coverage extensions
# ---------------------------------------------------------------------------
#
# Objects in this section exercise Oracle features that are common in
# production databases but were not part of the original combined fixture.
# Each block is wrapped in _try_ddl() so a missing privilege or an Oracle
# image without an optional feature (e.g. MDSYS for SDO_GEOMETRY) logs a
# warning and continues rather than failing the whole fixture build.

# Tables created here whose materialization depends on optional Oracle
# features.  Integration tests should tolerate any of these missing from
# the resulting dump rather than asserting hard-coded row counts.
OPTIONAL_AUDIT_TABLES: tuple[tuple[str, str], ...] = (
    ("INVENTORY", "EXT_PRICE_FEED"),
    ("INVENTORY", "STORE_LOCATIONS"),
    ("AUDITLOG", "ATTACHMENTS"),
    ("AUDITLOG", "LONG_NOTES"),
    ("AUDITLOG", "LONG_BLOBS"),
    ("FINANCE", "CUSTOMER_PROFILE"),
    ("HRDATA", "EMP_TAGS"),
    ("FINANCE", "TRANSACTION_DOCS"),
    ("INVENTORY", "PRODUCT_SPECS"),
    ("FINANCE", "TRANSACTION_DETAILS"),
    ("FINANCE", "TRANSACTION_LINES"),
    ("AUDITLOG", "EVENT_STREAM"),
    # Non-fatal imp code fixtures (IMP-00041/IMP-00403/ORA-04043 via cross-schema
    # trigger; ORA-23308/ORA-04080 via fast-refresh MV + MV log).
    ("FINANCE", "AUDIT_HOOKS"),
    ("FINANCE", "MV_ACCOUNT_SNAPSHOT"),
)


def _try_ddl(
    conn: oracledb.Connection,
    label: str,
    statements: Iterable[str],
    *,
    ignored_codes: set[int] | None = None,
) -> bool:
    """Execute DDL, logging a warning and continuing on failure.

    Returns True if all statements ran clean, False if any was skipped.
    Each statement runs in its own try/except so a clean-up DROP that fails
    does not block the following CREATE.  ``ignored_codes`` matches
    :func:`execute_ignore`: those error codes are silently swallowed.
    """
    ok = True
    for sql in statements:
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
        except oracledb.DatabaseError as exc:
            error = exc.args[0]
            code = getattr(error, "code", None)
            if ignored_codes and code in ignored_codes:
                continue
            ok = False
            LOGGER.warning(
                "Audit-extension skip [%s] ORA-%05d: %s",
                label,
                code or 0,
                str(error).strip().splitlines()[0],
            )
            break
    return ok


def alter_existing_tables_for_audit(conn: oracledb.Connection) -> None:
    """Add virtual / invisible / unicode columns to existing tables.

    Done as ALTER TABLE (not in the base CREATE TABLE) so the original
    DDL in create_tables() stays stable for any tests that parse it.
    """
    _try_ddl(
        conn,
        "EMPLOYEES virtual + unicode columns",
        [
            "ALTER TABLE HRDATA.EMPLOYEES ADD ("
            "  FULL_NAME GENERATED ALWAYS AS (TRIM(FIRST_NAME || ' ' || LAST_NAME)) VIRTUAL,"
            "  KANJI_NAME NVARCHAR2(50)"
            ")",
        ],
        ignored_codes={1430},  # column already exists (re-run)
    )
    _try_ddl(
        conn,
        "ACCOUNTS invisible column",
        [
            "ALTER TABLE FINANCE.ACCOUNTS ADD (INTERNAL_NOTE VARCHAR2(200) INVISIBLE)",
        ],
        ignored_codes={1430},
    )


def create_audit_tier1_tables(conn: oracledb.Connection) -> None:
    """Tier 1: IOT, GTT, identity-column table.

    External table is created separately in create_audit_external_table()
    because it needs a CSV file staged into the container.
    """
    _try_ddl(
        conn,
        "HRDATA.EMP_PREFERENCES (index-organized)",
        [
            "DROP TABLE HRDATA.EMP_PREFERENCES PURGE",
            "CREATE TABLE HRDATA.EMP_PREFERENCES ("
            "  EMP_ID NUMBER(6) NOT NULL,"
            "  PREF_KEY VARCHAR2(40) NOT NULL,"
            "  PREF_VALUE VARCHAR2(200),"
            "  CONSTRAINT EMP_PREF_PK PRIMARY KEY (EMP_ID, PREF_KEY)"
            ") ORGANIZATION INDEX TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )
    _try_ddl(
        conn,
        "AUDITLOG.GTT_STAGING (global temporary)",
        [
            "DROP TABLE AUDITLOG.GTT_STAGING PURGE",
            "CREATE GLOBAL TEMPORARY TABLE AUDITLOG.GTT_STAGING ("
            "  STAGE_ID NUMBER(10),"
            "  STAGE_KEY VARCHAR2(60),"
            "  STAGE_VALUE VARCHAR2(200)"
            ") ON COMMIT PRESERVE ROWS",
        ],
        ignored_codes={942},
    )
    _try_ddl(
        conn,
        "INVENTORY.ORDERS (identity column)",
        [
            "DROP TABLE INVENTORY.ORDERS CASCADE CONSTRAINTS PURGE",
            "CREATE TABLE INVENTORY.ORDERS ("
            "  ORDER_ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
            "  CUSTOMER_NAME VARCHAR2(80) NOT NULL,"
            "  ORDER_DATE DATE NOT NULL,"
            "  ORDER_TOTAL NUMBER(12,2)"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )


def create_audit_external_table(
    conn: oracledb.Connection,
    container: ContainerOracle,
) -> None:
    """Stage a CSV inside the container and define an external table over it.

    Uses the existing combined-dump directory as the LOCATION so we don't
    need an extra mount.  If anything fails (no privilege, no DIRECTORY
    object, no ORACLE_LOADER access) we log and continue.
    """
    csv_content = (
        "PRICE_ID,PRODUCT_CODE,UNIT_PRICE,EFFECTIVE_DATE\n"
        "1,SKU-001,19.99,2024-01-01\n"
        "2,SKU-002,29.99,2024-01-15\n"
        "3,SKU-003,9.49,2024-02-01\n"
        "4,SKU-004,149.00,2024-02-10\n"
        "5,SKU-005,2.39,2024-03-05\n"
    )
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, prefix="price_feed_"
        ) as fh:
            fh.write(csv_content)
            local_csv = Path(fh.name)
        container.copy_to(local_csv, f"{CONTAINER_DUMP_PATH}/price_feed.csv")
        local_csv.unlink(missing_ok=True)
        container.exec(
            [
                "bash",
                "-lc",
                f"chmod a+r {CONTAINER_DUMP_PATH}/price_feed.csv",
            ],
            check=False,
        )
    except (OSError, RuntimeError) as exc:
        LOGGER.warning("Audit-extension skip [external table CSV staging]: %s", exc)
        return

    _try_ddl(
        conn,
        "INVENTORY.EXT_PRICE_FEED (external table)",
        [
            "DROP TABLE INVENTORY.EXT_PRICE_FEED PURGE",
            "CREATE TABLE INVENTORY.EXT_PRICE_FEED ("
            "  PRICE_ID NUMBER(8),"
            "  PRODUCT_CODE VARCHAR2(20),"
            "  UNIT_PRICE NUMBER(12,4),"
            "  EFFECTIVE_DATE DATE"
            ") ORGANIZATION EXTERNAL ("
            f"  TYPE ORACLE_LOADER DEFAULT DIRECTORY {DUMP_DIRECTORY}"
            "  ACCESS PARAMETERS ("
            "    RECORDS DELIMITED BY NEWLINE"
            "    SKIP 1"
            "    FIELDS TERMINATED BY ',' "
            "    MISSING FIELD VALUES ARE NULL"
            "    (PRICE_ID, PRODUCT_CODE, UNIT_PRICE,"
            "     EFFECTIVE_DATE DATE 'YYYY-MM-DD')"
            "  )"
            "  LOCATION ('price_feed.csv')"
            ") REJECT LIMIT UNLIMITED",
        ],
        ignored_codes={942},
    )


def create_audit_tier2_tables(conn: oracledb.Connection) -> None:
    """Tier 2: type-handling coverage.

    Each block is independent; failure of one does not block the rest.
    JSON column falls back to ``VARCHAR2 CHECK (IS JSON)`` when the
    native ``JSON`` type isn't recognised (pre-21c images).
    """
    # JSON — try native type first, fall back to constrained VARCHAR2.
    if not _try_ddl(
        conn,
        "INVENTORY.PRODUCT_SPECS (native JSON)",
        [
            "DROP TABLE INVENTORY.PRODUCT_SPECS PURGE",
            "CREATE TABLE INVENTORY.PRODUCT_SPECS ("
            "  SPEC_ID NUMBER(8) PRIMARY KEY,"
            "  PRODUCT_ID NUMBER(8),"
            "  SPEC JSON"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    ):
        _try_ddl(
            conn,
            "INVENTORY.PRODUCT_SPECS (VARCHAR2 IS JSON fallback)",
            [
                "DROP TABLE INVENTORY.PRODUCT_SPECS PURGE",
                "CREATE TABLE INVENTORY.PRODUCT_SPECS ("
                "  SPEC_ID NUMBER(8) PRIMARY KEY,"
                "  PRODUCT_ID NUMBER(8),"
                "  SPEC VARCHAR2(4000) CHECK (SPEC IS JSON)"
                ") TABLESPACE COMBINED_DATA",
            ],
            ignored_codes={942},
        )

    _try_ddl(
        conn,
        "FINANCE.TRANSACTION_DOCS (XMLTYPE)",
        [
            "DROP TABLE FINANCE.TRANSACTION_DOCS PURGE",
            "CREATE TABLE FINANCE.TRANSACTION_DOCS ("
            "  DOC_ID NUMBER(8) PRIMARY KEY,"
            "  TXN_ID NUMBER(10),"
            "  RAW_XML XMLTYPE"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    _try_ddl(
        conn,
        "FINANCE.ADDRESS_T + CUSTOMER_PROFILE (object type)",
        [
            "DROP TABLE FINANCE.CUSTOMER_PROFILE PURGE",
            "DROP TYPE FINANCE.ADDRESS_T FORCE",
            "CREATE TYPE FINANCE.ADDRESS_T AS OBJECT ("
            "  STREET VARCHAR2(80), CITY VARCHAR2(60),"
            "  COUNTRY VARCHAR2(40), POSTAL VARCHAR2(20)"
            ")",
            "CREATE TABLE FINANCE.CUSTOMER_PROFILE ("
            "  PROFILE_ID NUMBER(8) PRIMARY KEY,"
            "  EMP_ID NUMBER(6),"
            "  ADDR FINANCE.ADDRESS_T"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942, 4043},
    )

    _try_ddl(
        conn,
        "HRDATA.EMP_TAGS (VARRAY + nested table)",
        [
            "DROP TABLE HRDATA.EMP_TAGS PURGE",
            "DROP TYPE HRDATA.TAG_LIST FORCE",
            "DROP TYPE HRDATA.HISTORY_T FORCE",
            "CREATE TYPE HRDATA.TAG_LIST AS VARRAY(10) OF VARCHAR2(50)",
            "CREATE TYPE HRDATA.HISTORY_T AS TABLE OF VARCHAR2(200)",
            "CREATE TABLE HRDATA.EMP_TAGS ("
            "  EMP_ID NUMBER(6) PRIMARY KEY,"
            "  TAGS HRDATA.TAG_LIST,"
            "  HISTORY HRDATA.HISTORY_T"
            ") NESTED TABLE HISTORY STORE AS EMP_TAGS_HISTORY_NT"
            "  TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942, 4043},
    )

    # SDO_GEOMETRY only works on images with MDSYS installed.
    _try_ddl(
        conn,
        "INVENTORY.STORE_LOCATIONS (SDO_GEOMETRY)",
        [
            "DROP TABLE INVENTORY.STORE_LOCATIONS PURGE",
            "CREATE TABLE INVENTORY.STORE_LOCATIONS ("
            "  STORE_ID NUMBER(8) PRIMARY KEY,"
            "  STORE_NAME VARCHAR2(80),"
            "  LOCATION MDSYS.SDO_GEOMETRY"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # BFILE — declare even if the underlying file is absent; row inserts
    # use BFILENAME() which Oracle accepts without verifying the file.
    _try_ddl(
        conn,
        "AUDITLOG.ATTACHMENTS (BFILE)",
        [
            "DROP TABLE AUDITLOG.ATTACHMENTS PURGE",
            "CREATE TABLE AUDITLOG.ATTACHMENTS ("
            "  ATTACH_ID NUMBER(8) PRIMARY KEY,"
            "  FILE_REF BFILE"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # LONG / LONG RAW each in their own table (Oracle restriction: only
    # one LONG column per table, and no LONG with LOBs in the same table).
    _try_ddl(
        conn,
        "AUDITLOG.LONG_NOTES (LONG)",
        [
            "DROP TABLE AUDITLOG.LONG_NOTES PURGE",
            "CREATE TABLE AUDITLOG.LONG_NOTES ("
            "  NOTE_ID NUMBER(8) PRIMARY KEY,"
            "  NOTE LONG"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )
    _try_ddl(
        conn,
        "AUDITLOG.LONG_BLOBS (LONG RAW)",
        [
            "DROP TABLE AUDITLOG.LONG_BLOBS PURGE",
            "CREATE TABLE AUDITLOG.LONG_BLOBS ("
            "  BLOB_ID NUMBER(8) PRIMARY KEY,"
            "  PAYLOAD LONG RAW"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    _try_ddl(
        conn,
        "FINANCE.NUMERIC_EDGE (NUMBER precision boundaries)",
        [
            "DROP TABLE FINANCE.NUMERIC_EDGE PURGE",
            "CREATE TABLE FINANCE.NUMERIC_EDGE ("
            "  ROW_ID NUMBER(8) PRIMARY KEY,"
            "  BIG38 NUMBER(38),"
            "  STAR_ZERO NUMBER(*,0),"
            "  UNBOUNDED NUMBER,"
            "  SMALL_DEC NUMBER(5,4),"
            # Scales Oracle permits but the Parquet/Avro decimal logical types
            # reject without normalisation: negative scale (rounded integers)
            # and scale greater than precision (pure fractions).
            "  NEG_SCALE NUMBER(10,-2),"
            "  NEG_SCALE_WIDE NUMBER(20,-2),"
            "  FRAC_SCALE NUMBER(2,5),"
            # FLOAT exercises the converter's FLOAT_TYPES -> double path, which
            # previously had no real column (the PRODUCTS "BINARY_*" comments
            # are misleading: those columns are NUMBER).  BINARY_FLOAT /
            # BINARY_DOUBLE are intentionally NOT used here because legacy
            # ``exp`` raises EXP-00104 and drops the whole table.
            "  LEGACY_FLOAT FLOAT(126)"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )


def create_audit_tier3_tables(conn: oracledb.Connection) -> None:
    """Tier 3: partitioning + constraint coverage."""
    _try_ddl(
        conn,
        "FINANCE.TRANSACTION_DETAILS (RANGE-HASH composite partitioning)",
        [
            "DROP TABLE FINANCE.TRANSACTION_DETAILS CASCADE CONSTRAINTS PURGE",
            "CREATE TABLE FINANCE.TRANSACTION_DETAILS ("
            "  DETAIL_ID NUMBER(10) NOT NULL,"
            "  ACCOUNT_ID NUMBER(8) NOT NULL,"
            "  TXN_DATE DATE NOT NULL,"
            "  AMOUNT NUMBER(12,2) NOT NULL,"
            "  CONSTRAINT TXN_DETAILS_PK PRIMARY KEY (DETAIL_ID, TXN_DATE)"
            ") TABLESPACE COMBINED_DATA"
            " PARTITION BY RANGE (TXN_DATE)"
            " SUBPARTITION BY HASH (ACCOUNT_ID) SUBPARTITIONS 4"
            " ("
            "  PARTITION P_2024 VALUES LESS THAN (DATE '2025-01-01'),"
            "  PARTITION P_2025 VALUES LESS THAN (DATE '2026-01-01'),"
            "  PARTITION P_MAX  VALUES LESS THAN (MAXVALUE)"
            " )",
        ],
        ignored_codes={942},
    )

    _try_ddl(
        conn,
        "FINANCE.TRANSACTION_LINES (reference partitioning)",
        [
            "DROP TABLE FINANCE.TRANSACTION_LINES PURGE",
            "CREATE TABLE FINANCE.TRANSACTION_LINES ("
            "  LINE_ID NUMBER(10) NOT NULL,"
            "  DETAIL_ID NUMBER(10) NOT NULL,"
            "  TXN_DATE DATE NOT NULL,"
            "  DESCRIPTION VARCHAR2(200),"
            "  CONSTRAINT TXN_LINES_PK PRIMARY KEY (LINE_ID, TXN_DATE),"
            "  CONSTRAINT TXN_LINES_FK FOREIGN KEY (DETAIL_ID, TXN_DATE)"
            "    REFERENCES FINANCE.TRANSACTION_DETAILS(DETAIL_ID, TXN_DATE)"
            ") PARTITION BY REFERENCE (TXN_LINES_FK)",
        ],
        ignored_codes={942},
    )

    _try_ddl(
        conn,
        "AUDITLOG.EVENT_STREAM (interval partitioning)",
        [
            "DROP TABLE AUDITLOG.EVENT_STREAM PURGE",
            "CREATE TABLE AUDITLOG.EVENT_STREAM ("
            "  EVENT_ID NUMBER(10) NOT NULL,"
            "  EVENT_TIME DATE NOT NULL,"
            "  EVENT_TYPE VARCHAR2(40),"
            "  PAYLOAD VARCHAR2(200)"
            ") TABLESPACE COMBINED_DATA"
            " PARTITION BY RANGE (EVENT_TIME)"
            " INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))"
            " ("
            "  PARTITION P_INITIAL VALUES LESS THAN (DATE '2024-01-01')"
            " )",
        ],
        ignored_codes={942},
    )

    # Deferred FK between two new INVENTORY tables.
    _try_ddl(
        conn,
        "INVENTORY.SUPPLIERS + SUPPLIER_NOTES (deferred FK)",
        [
            "DROP TABLE INVENTORY.SUPPLIER_NOTES PURGE",
            "DROP TABLE INVENTORY.SUPPLIERS PURGE",
            "CREATE TABLE INVENTORY.SUPPLIERS ("
            "  SUPPLIER_ID NUMBER(8) PRIMARY KEY,"
            "  SUPPLIER_NAME VARCHAR2(80) NOT NULL"
            ") TABLESPACE COMBINED_DATA",
            "CREATE TABLE INVENTORY.SUPPLIER_NOTES ("
            "  NOTE_ID NUMBER(8) PRIMARY KEY,"
            "  SUPPLIER_ID NUMBER(8) NOT NULL,"
            "  NOTE VARCHAR2(200),"
            "  CONSTRAINT SUPPLIER_NOTES_FK FOREIGN KEY (SUPPLIER_ID)"
            "    REFERENCES INVENTORY.SUPPLIERS(SUPPLIER_ID)"
            "    DEFERRABLE INITIALLY DEFERRED"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # ON DELETE CASCADE child of INVENTORY.ORDERS.
    _try_ddl(
        conn,
        "INVENTORY.ORDER_ITEMS (ON DELETE CASCADE)",
        [
            "DROP TABLE INVENTORY.ORDER_ITEMS PURGE",
            "CREATE TABLE INVENTORY.ORDER_ITEMS ("
            "  ITEM_ID NUMBER(8) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
            "  ORDER_ID NUMBER NOT NULL,"
            "  PRODUCT_ID NUMBER(8),"
            "  QUANTITY NUMBER(8) NOT NULL,"
            "  CONSTRAINT ORDER_ITEMS_FK FOREIGN KEY (ORDER_ID)"
            "    REFERENCES INVENTORY.ORDERS(ORDER_ID) ON DELETE CASCADE"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # Multi-column FK in HRDATA.
    _try_ddl(
        conn,
        "HRDATA.DEPT_LOCATIONS + DEPT_LOCATION_PHONES (multi-column FK)",
        [
            "DROP TABLE HRDATA.DEPT_LOCATION_PHONES PURGE",
            "DROP TABLE HRDATA.DEPT_LOCATIONS PURGE",
            "CREATE TABLE HRDATA.DEPT_LOCATIONS ("
            "  DEPT_ID NUMBER(6) NOT NULL,"
            "  LOC_CODE VARCHAR2(10) NOT NULL,"
            "  LOC_NAME VARCHAR2(60),"
            "  CONSTRAINT DEPT_LOC_PK PRIMARY KEY (DEPT_ID, LOC_CODE)"
            ") TABLESPACE COMBINED_DATA",
            "CREATE TABLE HRDATA.DEPT_LOCATION_PHONES ("
            "  PHONE_ID NUMBER(8) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
            "  DEPT_ID NUMBER(6) NOT NULL,"
            "  LOC_CODE VARCHAR2(10) NOT NULL,"
            "  PHONE VARCHAR2(20),"
            "  CONSTRAINT DEPT_LOC_PHONES_FK FOREIGN KEY (DEPT_ID, LOC_CODE)"
            "    REFERENCES HRDATA.DEPT_LOCATIONS(DEPT_ID, LOC_CODE)"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )


def create_audit_tier56_tables(conn: oracledb.Connection) -> None:
    """Tier 5/6: metadata + identifier-edge coverage."""
    # Mixed-case quoted identifier with a reserved-word column.
    _try_ddl(
        conn,
        'HRDATA."MixedCase_Table" (quoted identifiers)',
        [
            'DROP TABLE HRDATA."MixedCase_Table" PURGE',
            'CREATE TABLE HRDATA."MixedCase_Table" ('
            '  "CamelCase_Col" VARCHAR2(50),'
            '  "SELECT" VARCHAR2(50),'
            '  "ORDER" VARCHAR2(50)'
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # Long identifier (close to Oracle 12.2+ 128-char limit).
    long_name = "LONG_TABLE_NAME_" + "X" * 100
    _try_ddl(
        conn,
        "HRDATA long-identifier table",
        [
            f"DROP TABLE HRDATA.{long_name} PURGE",
            f"CREATE TABLE HRDATA.{long_name} ("
            "  ID NUMBER(8) PRIMARY KEY,"
            "  PAYLOAD VARCHAR2(80)"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942, 972},  # 972 = identifier too long (pre-12.2)
    )

    # NOT NULL via CHECK constraint vs declarative NOT NULL.
    _try_ddl(
        conn,
        "FINANCE.CHECK_NOT_NULL (check-based NOT NULL)",
        [
            "DROP TABLE FINANCE.CHECK_NOT_NULL PURGE",
            "CREATE TABLE FINANCE.CHECK_NOT_NULL ("
            "  ID NUMBER PRIMARY KEY,"
            "  VAL NUMBER CHECK (VAL IS NOT NULL),"
            "  NOTE VARCHAR2(80) NOT NULL"
            ") TABLESPACE COMBINED_DATA",
        ],
        ignored_codes={942},
    )

    # -----------------------------------------------------------------------
    # Non-fatal imp code fixture: cross-schema trigger
    #
    # FINANCE.AUDIT_HOOKS is a minimal hook-registry table whose AFTER INSERT
    # trigger calls AUDITLOG.LOG_CHANGE.  In the source DB the reference
    # resolves; during legacy imp FROMUSER=FINANCE TOUSER=DMP_FINANCE the
    # staging instance has no AUDITLOG schema, so Oracle compiles the trigger
    # as INVALID and imp emits:
    #
    #   IMP-00403  object created with compilation warnings
    #   IMP-00041  object altered with compilation warnings (recompile sweep)
    #   ORA-04043  object AUDITLOG.LOG_CHANGE does not exist
    #
    # The table intentionally has no data rows so the invalid trigger never
    # fires during data import (avoiding IMP-00098 / ORA-04098 at that layer).
    # -----------------------------------------------------------------------
    _try_ddl(
        conn,
        "FINANCE.AUDIT_HOOKS + cross-schema trigger (IMP-00041/IMP-00403/ORA-04043 fixture)",
        [
            "DROP TABLE FINANCE.AUDIT_HOOKS PURGE",
            "CREATE TABLE FINANCE.AUDIT_HOOKS ("
            "  HOOK_ID   NUMBER(8)    NOT NULL,"
            "  HOOK_NAME VARCHAR2(60) NOT NULL,"
            "  CREATED   DATE         DEFAULT SYSDATE,"
            "  CONSTRAINT AUDIT_HOOKS_PK PRIMARY KEY (HOOK_ID)"
            ") TABLESPACE COMBINED_DATA",
            # The trigger body is intentionally NOT rewritten by imp's
            # FROMUSER/TOUSER translation — AUDITLOG.LOG_CHANGE remains
            # unqualified in the staging schema where only DMP_AUDITLOG
            # exists.  This forces the compilation failure that produces the
            # desired non-fatal error codes.
            """
            CREATE OR REPLACE TRIGGER FINANCE.TRG_AUDIT_HOOKS_LOG
            AFTER INSERT ON FINANCE.AUDIT_HOOKS
            FOR EACH ROW
            BEGIN
                AUDITLOG.LOG_CHANGE(
                    'FINANCE', 'AUDIT_HOOKS',
                    TO_CHAR(:NEW.HOOK_ID),
                    'INSERT'
                );
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END TRG_AUDIT_HOOKS_LOG;
            """,
        ],
        ignored_codes={942},
    )


def create_audit_comments(conn: oracledb.Connection) -> None:
    """Add COMMENT ON statements so converter metadata round-trip is testable."""
    comments = (
        "COMMENT ON TABLE HRDATA.EMPLOYEES IS 'Employee master records'",
        "COMMENT ON COLUMN HRDATA.EMPLOYEES.EMAIL IS 'Unique work email address'",
        "COMMENT ON COLUMN HRDATA.EMPLOYEES.SALARY IS 'Annual salary in USD'",
        "COMMENT ON TABLE FINANCE.ACCOUNTS IS 'Customer financial accounts'",
        "COMMENT ON COLUMN FINANCE.ACCOUNTS.BALANCE IS 'Current balance, USD'",
        "COMMENT ON TABLE INVENTORY.PRODUCTS IS 'Product catalog, LIST-partitioned by region'",
    )
    _try_ddl(conn, "audit comments", comments)


# Each insert block is intentionally independent so a missing optional
# feature in one block doesn't skip later blocks — the function naturally
# has many branches and statements.  Splitting it into helpers buys
# nothing because each helper would have its own try/except dance.
# pylint: disable-next=too-many-branches,too-many-statements
def insert_audit_extension_data(conn: oracledb.Connection) -> None:
    """Populate the audit-coverage tables with a small amount of data.

    Each block is independent: a missing table (e.g. SDO_GEOMETRY skipped
    on a non-MDSYS image) does not block the rest.
    """
    # Update existing rows with Unicode data.
    _try_ddl(
        conn,
        "EMPLOYEES KANJI_NAME unicode samples",
        [
            "UPDATE HRDATA.EMPLOYEES SET KANJI_NAME = '山田' WHERE EMP_ID = 1",
            "UPDATE HRDATA.EMPLOYEES SET KANJI_NAME = 'Иван' WHERE EMP_ID = 2",
            "UPDATE HRDATA.EMPLOYEES SET KANJI_NAME = '\U0001f600 test' WHERE EMP_ID = 3",
        ],
    )
    _try_ddl(
        conn,
        "ACCOUNTS INTERNAL_NOTE population",
        ["UPDATE FINANCE.ACCOUNTS SET INTERNAL_NOTE = 'internal:' || ACCOUNT_ID"],
    )

    # IOT data
    iot_rows = []
    for emp_id in range(1, 11):
        for key, val in (("theme", "dark"), ("lang", "en"), ("tz", "UTC")):
            iot_rows.append((emp_id, key, val))
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO HRDATA.EMP_PREFERENCES (EMP_ID, PREF_KEY, PREF_VALUE)"
                " VALUES (:1, :2, :3)",
                iot_rows,
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [EMP_PREFERENCES rows]: %s", exc)

    # GTT — populate inside the same session/transaction. The rows will
    # not survive export, but creating them exercises the DDL path.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO AUDITLOG.GTT_STAGING (STAGE_ID, STAGE_KEY, STAGE_VALUE)"
                " VALUES (:1, :2, :3)",
                [(i, f"key{i}", f"val{i}") for i in range(1, 6)],
            )
        # Intentionally no commit — GTT ON COMMIT PRESERVE ROWS will keep
        # them session-bound regardless, and uncommitted is fine for the
        # export-time visibility check.
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [GTT_STAGING rows]: %s", exc)

    # ORDERS — identity column generates ORDER_ID.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO INVENTORY.ORDERS (CUSTOMER_NAME, ORDER_DATE, ORDER_TOTAL)"
                " VALUES (:1, DATE '2024-01-01' + :2, :3)",
                [(f"Customer-{i:03d}", i, round(50.0 + i * 3.14, 2)) for i in range(1, 21)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [ORDERS rows]: %s", exc)

    # ORDER_ITEMS — depends on ORDERS being populated.
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ORDER_ID FROM INVENTORY.ORDERS")
            order_ids = [row[0] for row in cursor.fetchall()]
        if order_ids:
            items = []
            for idx, order_id in enumerate(order_ids, start=1):
                items.append((order_id, (idx % 24) + 1, 1 + idx % 5))
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO INVENTORY.ORDER_ITEMS (ORDER_ID, PRODUCT_ID, QUANTITY)"
                    " VALUES (:1, :2, :3)",
                    items,
                )
            conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [ORDER_ITEMS rows]: %s", exc)

    # PRODUCT_SPECS (JSON or VARCHAR2 IS JSON).
    json_rows = [
        (i, i, f'{{"weight_kg": {0.5 + i * 0.1:.2f}, "color": "blue", "tags": ["a", "b"]}}')
        for i in range(1, 11)
    ]
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO INVENTORY.PRODUCT_SPECS (SPEC_ID, PRODUCT_ID, SPEC)"
                " VALUES (:1, :2, :3)",
                json_rows,
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [PRODUCT_SPECS rows]: %s", exc)

    # TRANSACTION_DOCS (XMLTYPE).
    try:
        with conn.cursor() as cursor:
            for i in range(1, 6):
                cursor.execute(
                    "INSERT INTO FINANCE.TRANSACTION_DOCS (DOC_ID, TXN_ID, RAW_XML)"
                    " VALUES (:1, :2, XMLTYPE(:3))",
                    [i, i, f"<txn id='{i}'><amount>{i * 100}</amount></txn>"],
                )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [TRANSACTION_DOCS rows]: %s", exc)

    # CUSTOMER_PROFILE (object type).
    try:
        with conn.cursor() as cursor:
            for i in range(1, 6):
                cursor.execute(
                    "INSERT INTO FINANCE.CUSTOMER_PROFILE (PROFILE_ID, EMP_ID, ADDR)"
                    " VALUES (:1, :2,"
                    "   FINANCE.ADDRESS_T(:3, :4, :5, :6))",
                    [i, i, f"{i} Main St", "Springfield", "US", f"0000{i}"],
                )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [CUSTOMER_PROFILE rows]: %s", exc)

    # SDO_GEOMETRY points (only if MDSYS present).
    try:
        with conn.cursor() as cursor:
            for i in range(1, 4):
                cursor.execute(
                    "INSERT INTO INVENTORY.STORE_LOCATIONS (STORE_ID, STORE_NAME, LOCATION)"
                    " VALUES (:1, :2,"
                    "   MDSYS.SDO_GEOMETRY(2001, 4326,"
                    "     MDSYS.SDO_POINT_TYPE(:3, :4, NULL), NULL, NULL))",
                    [i, f"Store-{i}", -122.0 + i * 0.1, 37.5 + i * 0.1],
                )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [STORE_LOCATIONS rows]: %s", exc)

    # LONG / LONG RAW.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO AUDITLOG.LONG_NOTES (NOTE_ID, NOTE) VALUES (:1, :2)",
                [(i, "Long note content " + ("x" * (50 * i))) for i in range(1, 6)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [LONG_NOTES rows]: %s", exc)
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO AUDITLOG.LONG_BLOBS (BLOB_ID, PAYLOAD) VALUES (:1, :2)",
                [(i, b"binary-payload-" + b"y" * (10 * i)) for i in range(1, 6)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [LONG_BLOBS rows]: %s", exc)

    # NUMERIC_EDGE boundary values.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO FINANCE.NUMERIC_EDGE"
                " (ROW_ID, BIG38, STAR_ZERO, UNBOUNDED, SMALL_DEC,"
                "  NEG_SCALE, NEG_SCALE_WIDE, FRAC_SCALE, LEGACY_FLOAT)"
                " VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)",
                [
                    # NEG_SCALE/NEG_SCALE_WIDE values are multiples of 100;
                    # FRAC_SCALE values are < 0.001 (NUMBER(2,5) is a fraction).
                    # FLOAT values stay within Oracle NUMBER range (~1e125) so
                    # oracledb's default NUMBER bind does not overflow.
                    (
                        1,
                        10**37,
                        12345678901234567890,
                        1.5,
                        0.0001,
                        12300,
                        1234500,
                        0.00012,
                        1.2345678901234,
                    ),
                    (2, -(10**37), -42, 0, 0.9999, -4200, -100, 0.00099, 987654321.5),
                    (3, 1, 0, 1.0e20, 0.5, 0, 500, 0.00001, 0.0009765625),
                ],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [NUMERIC_EDGE rows]: %s", exc)

    # TRANSACTION_DETAILS (subpartitioned) + LINES (reference partition).
    try:
        with conn.cursor() as cursor:
            detail_rows = []
            for i in range(1, 81):
                day_offset = (i - 1) * 5  # spreads across 2024-2025
                detail_rows.append((i, (i % 20) + 1, day_offset, round(10.0 + i * 1.5, 2)))
            cursor.executemany(
                "INSERT INTO FINANCE.TRANSACTION_DETAILS"
                " (DETAIL_ID, ACCOUNT_ID, TXN_DATE, AMOUNT)"
                " VALUES (:1, :2, DATE '2024-01-01' + :3, :4)",
                detail_rows,
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [TRANSACTION_DETAILS rows]: %s", exc)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DETAIL_ID, TXN_DATE FROM FINANCE.TRANSACTION_DETAILS")
            parents = cursor.fetchall()
        if parents:
            line_rows = []
            for idx, (detail_id, txn_date) in enumerate(parents[:60], start=1):
                line_rows.append((idx, detail_id, txn_date, f"line {idx}"))
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO FINANCE.TRANSACTION_LINES"
                    " (LINE_ID, DETAIL_ID, TXN_DATE, DESCRIPTION)"
                    " VALUES (:1, :2, :3, :4)",
                    line_rows,
                )
            conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [TRANSACTION_LINES rows]: %s", exc)

    # EVENT_STREAM (interval partitioned) — spread rows across 6 months.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO AUDITLOG.EVENT_STREAM"
                " (EVENT_ID, EVENT_TIME, EVENT_TYPE, PAYLOAD)"
                " VALUES (:1, DATE '2024-01-01' + :2, :3, :4)",
                [(i, (i - 1) * 6, f"TYPE_{i % 5}", f"payload-{i}") for i in range(1, 61)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [EVENT_STREAM rows]: %s", exc)

    # SUPPLIERS + SUPPLIER_NOTES (deferred FK).
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO INVENTORY.SUPPLIERS (SUPPLIER_ID, SUPPLIER_NAME) VALUES (:1, :2)",
                [(i, f"Supplier-{i:02d}") for i in range(1, 11)],
            )
            cursor.executemany(
                "INSERT INTO INVENTORY.SUPPLIER_NOTES"
                " (NOTE_ID, SUPPLIER_ID, NOTE) VALUES (:1, :2, :3)",
                [(i, ((i - 1) % 10) + 1, f"Note {i}") for i in range(1, 16)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [SUPPLIERS/SUPPLIER_NOTES rows]: %s", exc)

    # DEPT_LOCATIONS + child (multi-column FK).
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO HRDATA.DEPT_LOCATIONS (DEPT_ID, LOC_CODE, LOC_NAME)"
                " VALUES (:1, :2, :3)",
                [
                    (10, "HQ", "Engineering HQ"),
                    (10, "REM", "Engineering Remote"),
                    (20, "HQ", "Sales HQ"),
                    (30, "HQ", "Finance HQ"),
                ],
            )
            cursor.executemany(
                "INSERT INTO HRDATA.DEPT_LOCATION_PHONES"
                " (DEPT_ID, LOC_CODE, PHONE) VALUES (:1, :2, :3)",
                [
                    (10, "HQ", "+1-555-0100"),
                    (10, "REM", "+1-555-0101"),
                    (20, "HQ", "+1-555-0200"),
                    (30, "HQ", "+1-555-0300"),
                ],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [DEPT_LOCATIONS rows]: %s", exc)

    # Mixed-case table rows + CHECK_NOT_NULL + long-identifier table.
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO HRDATA."MixedCase_Table"'
                ' ("CamelCase_Col", "SELECT", "ORDER") VALUES (:1, :2, :3)',
                [(f"row-{i}", f"sel-{i}", f"ord-{i}") for i in range(1, 6)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [MixedCase_Table rows]: %s", exc)
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO FINANCE.CHECK_NOT_NULL (ID, VAL, NOTE) VALUES (:1, :2, :3)",
                [(i, i * 10, f"row-{i}") for i in range(1, 6)],
            )
        conn.commit()
    except oracledb.DatabaseError as exc:
        LOGGER.warning("Audit-extension skip [CHECK_NOT_NULL rows]: %s", exc)


def build_audit_extensions(
    conn: oracledb.Connection,
    container: ContainerOracle,
) -> None:
    """Top-level entry point that wires all audit-coverage steps together."""
    LOGGER.info("  Altering existing tables for audit coverage...")
    alter_existing_tables_for_audit(conn)
    LOGGER.info("  Creating Tier 1 (core DDL) audit objects...")
    create_audit_tier1_tables(conn)
    LOGGER.info("  Creating external-table audit object...")
    create_audit_external_table(conn, container)
    LOGGER.info("  Creating Tier 2 (type-handling) audit objects...")
    create_audit_tier2_tables(conn)
    LOGGER.info("  Creating Tier 3 (partitioning/constraints) audit objects...")
    create_audit_tier3_tables(conn)
    LOGGER.info("  Creating Tier 5/6 (metadata/identifier) audit objects...")
    create_audit_tier56_tables(conn)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def create_sample_database(
    admin: OracleAdminConnection,
    container: ContainerOracle,
) -> None:
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
        LOGGER.info("  Building audit-coverage extensions (DDL)...")
        build_audit_extensions(conn, container)
        LOGGER.info("  Inserting HR data...")
        insert_hr_data(conn)
        LOGGER.info("  Inserting INVENTORY data...")
        insert_inventory_data(conn)
        LOGGER.info("  Inserting FINANCE data...")
        insert_finance_data(conn)
        LOGGER.info("  Inserting AUDIT data...")
        insert_audit_data(conn)
        LOGGER.info("  Inserting audit-extension data...")
        insert_audit_extension_data(conn)
        LOGGER.info("  Adding audit COMMENT ON metadata...")
        create_audit_comments(conn)
        LOGGER.info("  Creating materialized view...")
        create_materialized_view(conn)
        LOGGER.info("  Gathering statistics...")
        gather_stats(conn)


# ---------------------------------------------------------------------------
# Export: modern (expdp)
# ---------------------------------------------------------------------------


def export_modern_dump(
    *,
    container: ContainerOracle,
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
    container: ContainerOracle,
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

        container.exec(
            [
                "bash",
                "-lc",
                f"chmod a+r {CONTAINER_DUMP_PATH}/{LEGACY_DUMPFILE} "
                f"{CONTAINER_DUMP_PATH}/{LEGACY_LOGFILE} 2>/dev/null || true",
            ],
            check=False,
        )

    # Legacy ``exp`` will return non-zero whenever it encounters any feature
    # it cannot serialise — and the audit-coverage extensions deliberately
    # include features the original tool predates (virtual columns, native
    # JSON, BFILE, RANGE-HASH composite partitions, etc.).  We therefore
    # treat the failure as expected so long as a non-empty dump file was
    # produced.  The modern Data Pump dump is the primary fixture; the
    # legacy dump is best-effort and documented as such in the README.
    legacy_dump = output_dir / LEGACY_DUMPFILE
    if result.returncode != 0:
        if legacy_dump.exists() and legacy_dump.stat().st_size > 0:
            LOGGER.warning(
                "exp returned %d (audit-extension features unsupported by legacy "
                "exp); legacy dump is partial. Modern dump remains authoritative.",
                result.returncode,
            )
        else:
            LOGGER.error("exp failed and produced no dump file — see log above")
            raise RuntimeError(f"exp exited {result.returncode} with no dump file produced")

    if not legacy_dump.exists():
        msg = f"exp finished but {legacy_dump} was not created"
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
>
> **Note (legacy dump + audit extensions):** Legacy `exp` cannot serialise
> several features intentionally added by the audit-coverage extensions:
> virtual columns (EMPLOYEES.FULL_NAME), native JSON (PRODUCT_SPECS.SPEC),
> binary XMLTYPE (TRANSACTION_DOCS.RAW_XML), RANGE-HASH composite
> partitions (TRANSACTION_DETAILS, TRANSACTION_LINES), identity columns
> on tables that exist (ORDERS, ORDER_ITEMS), BFILE (ATTACHMENTS), and
> very long quoted identifiers.  The legacy dump is therefore **partial**
> by design; `exp` exits non-zero and the script logs a warning.  Use the
> modern dump as the authoritative test artefact for these features.

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

## Audit-Coverage Extensions

Objects added to surface converter bugs around features common in production
databases but absent from the original combined fixture. Each is built via
`_try_ddl()` and logged as a warning if unavailable in the running image, so
this list is best-effort: check the export log to confirm which materialized.

### Tier 1 — Core DDL

| Object | Feature |
|--------|---------|
| `HRDATA.EMP_PREFERENCES` | Index-organized table (IOT, composite PK) |
| `INVENTORY.EXT_PRICE_FEED` | External table over `price_feed.csv` (ORACLE_LOADER) |
| `AUDITLOG.GTT_STAGING` | Global temporary table (`ON COMMIT PRESERVE ROWS`) |
| `INVENTORY.ORDERS` | `NUMBER GENERATED ALWAYS AS IDENTITY` primary key |
| `HRDATA.EMPLOYEES.FULL_NAME` | Virtual column (`GENERATED ALWAYS AS … VIRTUAL`) |
| `FINANCE.ACCOUNTS.INTERNAL_NOTE` | Invisible column |

### Tier 2 — Type handling

| Object | Feature |
|--------|---------|
| `INVENTORY.PRODUCT_SPECS` | Native `JSON` (21c+) or `VARCHAR2 CHECK IS JSON` fallback |
| `FINANCE.TRANSACTION_DOCS` | `XMLTYPE` column with populated XML |
| `FINANCE.CUSTOMER_PROFILE` | `FINANCE.ADDRESS_T` object-type column |
| `HRDATA.EMP_TAGS` | `VARRAY` + nested table column |
| `INVENTORY.STORE_LOCATIONS` | `MDSYS.SDO_GEOMETRY` (skipped on images without MDSYS) |
| `AUDITLOG.ATTACHMENTS` | `BFILE` column |
| `AUDITLOG.LONG_NOTES` | `LONG` column |
| `AUDITLOG.LONG_BLOBS` | `LONG RAW` column |
| `FINANCE.NUMERIC_EDGE` | NUMBER scale edges (negative scale, scale>precision) + FLOAT(126) |
| `HRDATA.EMPLOYEES.KANJI_NAME` | `NVARCHAR2` with kanji / cyrillic / emoji samples |

### Tier 3 — Partitioning & constraints

| Object | Feature |
|--------|---------|
| `FINANCE.TRANSACTION_DETAILS` | RANGE-HASH composite subpartitioning |
| `FINANCE.TRANSACTION_LINES` | `PARTITION BY REFERENCE` child of TRANSACTION_DETAILS |
| `AUDITLOG.EVENT_STREAM` | Interval partitioning (`INTERVAL NUMTOYMINTERVAL(1,'MONTH')`) |
| `INVENTORY.SUPPLIERS` + `SUPPLIER_NOTES` | `DEFERRABLE INITIALLY DEFERRED` FK |
| `INVENTORY.ORDER_ITEMS` | `ON DELETE CASCADE` FK to ORDERS |
| `HRDATA.DEPT_LOCATIONS` + `DEPT_LOCATION_PHONES` | Multi-column composite FK |

### Tier 5/6 — Metadata & identifiers

| Object | Feature |
|--------|---------|
| `HRDATA."MixedCase_Table"` | Quoted mixed-case + reserved-word columns (`"SELECT"`, `"ORDER"`) |
| `HRDATA.LONG_TABLE_NAME_X…` | Long identifier (close to 128-char limit) |
| `FINANCE.CHECK_NOT_NULL` | NOT NULL enforced via CHECK constraint (vs declarative) |
| `COMMENT ON TABLE / COLUMN` | Added to EMPLOYEES, ACCOUNTS, PRODUCTS for metadata round-trip |

> Tables marked as **optional** (`EXT_PRICE_FEED`, `STORE_LOCATIONS`,
> `ATTACHMENTS`, `LONG_NOTES`, `LONG_BLOBS`, `CUSTOMER_PROFILE`, `EMP_TAGS`,
> `TRANSACTION_DOCS`, `PRODUCT_SPECS`, `TRANSACTION_DETAILS`,
> `TRANSACTION_LINES`, `EVENT_STREAM`) may not materialize on every Oracle
> image. Inspect the build log if any are absent from the dump.

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

    with ContainerOracle.start(
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
        create_sample_database(admin, container)

        LOGGER.info("Exporting Data Pump (modern) dump...")
        export_modern_dump(container=container, admin=admin, output_dir=output_dir)

        LOGGER.info("Exporting legacy exp dump...")
        export_legacy_dump(container=container, admin=admin, output_dir=output_dir)

    write_config(output_dir, args.oracle_image)
    write_notes(output_dir)
    print_summary(output_dir)


if __name__ == "__main__":
    main()
