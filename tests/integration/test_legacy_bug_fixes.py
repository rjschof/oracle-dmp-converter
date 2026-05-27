"""Construct-precondition integration tests for the two legacy-path fixes that
a synthetic dump can't reproduce on a modern Oracle image.

These build the *failing state* directly in a live container and assert the
real fix code resolves it — so they fail if the fix is reverted, without
depending on an old-Oracle ``exp`` dump.

  * #10 (``aa09a0c`` + ordering fix) — a standalone VPD policy function on a
    staging table.  ``StagingExecutor._apply_staging_fixups`` must drop both
    the policy *and* its function.  (The function-drop only works because the
    executor runs it before ``drop_vpd_policies`` empties ``ALL_POLICIES`` —
    see the ordering regression guard in ``tests/unit/test_executor_extras.py``.)
  * #6  (``1b24e95``) — a legacy ``imp`` that exits fatally with an IMP/ORA
    code outside the historical non-fatal whitelist must be swallowed by
    ``import_all_metadata`` instead of aborting the inspect phase.
"""

from __future__ import annotations

import logging
import os
from types import SimpleNamespace

import pytest

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.core.executor import StagingExecutor
from oracle_dmp_converter.datapump.legacy.workflow import LegacyDumpWorkflow, make_legacy_runners
from oracle_dmp_converter.oracle.conn import OracleCredentials, oracle_connection
from oracle_dmp_converter.runtime.admin import admin_for_container
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

pytestmark = pytest.mark.integration

# the test deliberately drives the executor's protected fixup entry point
# pylint: disable=redefined-outer-name,protected-access

_PASSWORD = "OraclePwd_123"


def _image() -> str:
    return os.environ.get("DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE)


@pytest.fixture(scope="module")
def oracle(tmp_path_factory):
    """Start one Oracle container shared by all reproduction tests."""
    work_dir = tmp_path_factory.mktemp("legacy_fix_work")
    with ContainerOracle.start(image=_image(), password=_PASSWORD) as container:
        container.wait_ready(timeout_seconds=300)
        admin = admin_for_container(container, _PASSWORD)
        yield SimpleNamespace(container=container, admin=admin, work_dir=work_dir)


def _connect(admin):
    return oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    )


def _exec(conn, statements: list[str]) -> None:
    with conn.cursor() as cursor:
        for stmt in statements:
            cursor.execute(stmt)
    conn.commit()


def _count(conn, sql: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# #10 — VPD policy + function cleanup
# ---------------------------------------------------------------------------


def test_staging_fixups_drop_vpd_policy_and_function(oracle: SimpleNamespace) -> None:
    """#10: _apply_staging_fixups removes the VPD policy AND its standalone function.

    Reverting the executor's function-before-policy ordering leaves the
    function behind (drop_vpd_policies empties ALL_POLICIES first), so the
    function-count assertion fails.
    """
    stage = "DMP_FIXSRC"  # _apply_staging_fixups("FIXSRC") targets DMP_FIXSRC
    with _connect(oracle.admin) as conn:
        _exec(
            conn,
            [
                f"BEGIN EXECUTE IMMEDIATE 'DROP USER {stage} CASCADE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                f"CREATE USER {stage} IDENTIFIED BY x",
                f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {stage}",
                f"CREATE TABLE {stage}.LEDGER (ID NUMBER PRIMARY KEY, AMT NUMBER(10,2))",
                f"""CREATE OR REPLACE FUNCTION {stage}.LEDGER_POLICY(s VARCHAR2, o VARCHAR2)
                    RETURN VARCHAR2 IS BEGIN RETURN '1=1'; END;""",
                f"""BEGIN DBMS_RLS.ADD_POLICY(object_schema=>'{stage}', object_name=>'LEDGER',
                    policy_name=>'LEDGER_VPD_POL', function_schema=>'{stage}',
                    policy_function=>'LEDGER_POLICY', statement_types=>'SELECT', enable=>TRUE);
                    END;""",
            ],
        )
        assert _count(conn, f"SELECT COUNT(*) FROM all_policies WHERE object_owner='{stage}'") == 1
        assert (
            _count(
                conn,
                f"SELECT COUNT(*) FROM all_objects WHERE owner='{stage}' "
                f"AND object_type='FUNCTION' AND object_name='LEDGER_POLICY'",
            )
            == 1
        )

    executor = StagingExecutor(
        container=oracle.container,
        admin=oracle.admin,
        work_dir=oracle.work_dir,
        dumpfiles=("unused.dmp",),
    )
    executor._apply_staging_fixups("FIXSRC")  # noqa: SLF001 — exercising the real fixup path

    with _connect(oracle.admin) as conn:
        policies = _count(conn, f"SELECT COUNT(*) FROM all_policies WHERE object_owner='{stage}'")
        functions = _count(
            conn,
            f"SELECT COUNT(*) FROM all_objects WHERE owner='{stage}' "
            f"AND object_type='FUNCTION' AND object_name='LEDGER_POLICY'",
        )
    assert policies == 0, "VPD policy was not dropped"
    assert functions == 0, (
        "VPD policy function survived — drop_vpd_policy_functions found nothing because "
        "drop_vpd_policies ran first and emptied ALL_POLICIES (ordering regression)"
    )


# ---------------------------------------------------------------------------
# #6 — permissive legacy imp error handling
# ---------------------------------------------------------------------------


def test_metadata_import_swallows_fatal_imp(oracle: SimpleNamespace, caplog) -> None:
    """#6: a fatal legacy imp with an out-of-whitelist code is swallowed, not raised.

    A garbage dump file makes ``imp`` exit fatally with ``IMP-00010`` (not a
    valid export file) — a code outside the historical whitelist.  The
    permissive handler swallows it; the pre-fix strict handler re-raised, which
    is what killed the inspect phase.
    """
    # Stage a non-dump file so imp fails its header verification.
    oracle.container.exec(
        ["bash", "-lc", "echo 'THIS IS NOT AN ORACLE EXPORT FILE' > /tmp/garbage.dmp"]
    )

    discovery, inspect, convert = make_legacy_runners(oracle.container, oracle.work_dir)
    workflow = LegacyDumpWorkflow(
        credentials=OracleCredentials(
            user=oracle.admin.user, password=oracle.admin.password, service=oracle.admin.service
        ),
        directory_path="/tmp",
        dumpfiles=("garbage.dmp",),
        discovery_runner=discovery,
        discovery_dir=oracle.work_dir / "discovery",
        inspect_runner=inspect,
        convert_runner=convert,
    )

    with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
        # Must NOT raise: the permissive handler swallows the fatal imp.
        workflow.import_all_metadata("SRC", "DMP_SRC")

    swallow_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.name == "oracle_dmp_converter.datapump.legacy.workflow"
        and (
            "completed with non-fatal errors" in r.getMessage()
            or "unknown IMP/ORA" in r.getMessage()
        )
    ]
    assert swallow_msgs, (
        "Expected import_all_metadata to log a swallow message for the fatal imp; "
        f"records: {[r.getMessage() for r in caplog.records]}"
    )
    assert "IMP-00010" in "\n".join(swallow_msgs) or any(
        "IMP-" in m or "ORA-" in m for m in swallow_msgs
    ), "Swallowed output should reference the imp/ora code that the old whitelist rejected"
