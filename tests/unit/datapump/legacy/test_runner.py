"""Unit tests for datapump/legacy/runner.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from oracle_dmp_converter.datapump.legacy.parfile import LegacyImportJob, LegacyIndexFileJob
from oracle_dmp_converter.datapump.legacy.runner import LegacyRunner
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _creds() -> OracleCredentials:
    return OracleCredentials(user="system", password="pw", service="FREE")


def _make_container(returncode: int = 0, stdout: str = "done", stderr: str = "") -> MagicMock:
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    container = MagicMock()
    container.exec.return_value = result
    return container


class TestLegacyRunner:
    def test_run_imp_succeeds(self, tmp_path: Path) -> None:
        runner = LegacyRunner(_make_container(), tmp_path)
        job = LegacyImportJob(
            connection=_creds(),
            files=("/dumps/legacy.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="STAGE",
            tables=("ORDERS",),
            rows=True,
            indexes=False,
            grants=False,
            constraints=False,
        )
        output = runner.run_imp(job)
        assert output is not None

    def test_run_imp_indexfile_returns_sql_and_log(self, tmp_path: Path) -> None:
        cat_result = MagicMock()
        cat_result.returncode = 0
        cat_result.stdout = "CREATE TABLE ..."
        cat_result.stderr = ""

        imp_result = MagicMock()
        imp_result.returncode = 0
        imp_result.stdout = "Import complete"
        imp_result.stderr = ""

        container = MagicMock()
        # imp, rm -f parfile cleanup, cat indexfile
        rm_result = MagicMock(returncode=0, stdout="", stderr="")
        container.exec.side_effect = [imp_result, rm_result, cat_result]

        runner = LegacyRunner(container, tmp_path)
        job = LegacyIndexFileJob(
            connection=_creds(),
            files=("/dumps/legacy.dmp",),
            logfile="disc.log",
            indexfile="/tmp/indexfile.sql",
            full=True,
        )
        sql_content, log_output = runner.run_imp_indexfile(job)
        assert sql_content == "CREATE TABLE ..."
        assert "Import complete" in log_output

    def test_run_imp_indexfile_returns_empty_sql_on_cat_failure(self, tmp_path: Path) -> None:
        imp_result = MagicMock()
        imp_result.returncode = 0
        imp_result.stdout = "Import complete"
        imp_result.stderr = ""

        cat_result = MagicMock()
        cat_result.returncode = 1
        cat_result.stdout = ""
        cat_result.stderr = "No such file"

        container = MagicMock()
        # imp, rm -f parfile cleanup, cat indexfile
        rm_result = MagicMock(returncode=0, stdout="", stderr="")
        container.exec.side_effect = [imp_result, rm_result, cat_result]

        runner = LegacyRunner(container, tmp_path)
        job = LegacyIndexFileJob(
            connection=_creds(),
            files=("/dumps/legacy.dmp",),
            logfile="disc.log",
            indexfile="/tmp/missing.sql",
            full=True,
        )
        sql_content, _ = runner.run_imp_indexfile(job)
        assert sql_content == ""
