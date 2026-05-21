"""Unit tests for LegacyDumpWorkflow.import_chunks_batch."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from oracle_dmp_converter.datapump.legacy.parfile import LegacyImportJob
from oracle_dmp_converter.datapump.legacy.runner import LegacyRunner
from oracle_dmp_converter.datapump.legacy.workflow import LegacyDumpWorkflow
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _credentials() -> OracleCredentials:
    return OracleCredentials(user="system", password="OraclePwd_123", service="FREEPDB1")


def _make_workflow(tmp_path: Path) -> tuple[LegacyDumpWorkflow, MagicMock]:
    """Return a ``LegacyDumpWorkflow`` with a mocked convert_runner.

    The mocked runner's ``run_imp`` method does nothing and records calls.
    Returns ``(workflow, mock_convert_runner)``.
    """
    mock_convert_runner = MagicMock(spec=LegacyRunner)
    mock_convert_runner.run_imp.return_value = "imp completed successfully"

    workflow = LegacyDumpWorkflow(
        credentials=_credentials(),
        directory_path="/container/dumps",
        dumpfiles=("export.dmp",),
        discovery_runner=MagicMock(spec=LegacyRunner),
        discovery_dir=tmp_path / "discovery",
        inspect_runner=MagicMock(spec=LegacyRunner),
        convert_runner=mock_convert_runner,
    )
    return workflow, mock_convert_runner


class TestImportChunksBatchEmpty:
    def test_empty_chunks_does_not_call_run_imp(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        workflow.import_chunks_batch([])
        mock_runner.run_imp.assert_not_called()


class TestImportChunksBatchSingleSchema:
    def test_single_schema_produces_one_imp_call(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
            ("HRDATA", "DMP_HRDATA", "JOBS", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        assert mock_runner.run_imp.call_count == 1

    def test_single_schema_job_has_correct_fromuser_touser(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        job: LegacyImportJob = mock_runner.run_imp.call_args[0][0]
        assert job.fromuser == "HRDATA"
        assert job.touser == "DMP_HRDATA"

    def test_single_schema_job_includes_all_tables(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
            ("HRDATA", "DMP_HRDATA", "JOBS", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        job: LegacyImportJob = mock_runner.run_imp.call_args[0][0]
        assert set(job.tables) == {"EMPLOYEES", "DEPARTMENTS", "JOBS"}

    def test_duplicate_table_names_deduplicated(self, tmp_path: Path) -> None:
        """Two chunks for the same table (e.g. re-queued) must not duplicate TABLES=."""
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        assert mock_runner.run_imp.call_count == 1
        job: LegacyImportJob = mock_runner.run_imp.call_args[0][0]
        assert job.tables.count("EMPLOYEES") == 1  # type: ignore[attr-defined]

    def test_single_schema_job_rows_true(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        workflow.import_chunks_batch([("SRC", "DMP_SRC", "T1", "whole", None)])
        job: LegacyImportJob = mock_runner.run_imp.call_args[0][0]
        assert job.rows is True

    def test_single_schema_job_indexes_grants_constraints_false(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        workflow.import_chunks_batch([("SRC", "DMP_SRC", "T1", "whole", None)])
        job: LegacyImportJob = mock_runner.run_imp.call_args[0][0]
        assert job.indexes is False
        assert job.grants is False
        assert job.constraints is False


class TestImportChunksBatchMultiSchema:
    def test_two_schemas_produce_two_imp_calls(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "PRODUCTS", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "WAREHOUSES", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        assert mock_runner.run_imp.call_count == 2

    def test_four_schemas_produce_four_imp_calls(self, tmp_path: Path) -> None:
        """Mirrors the real dump: HRDATA, INVENTORY, FINANCE, AUDITLOG."""
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
            ("HRDATA", "DMP_HRDATA", "JOBS", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "PRODUCTS", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "WAREHOUSES", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "STOCK_LEVELS", "whole", None),
            ("FINANCE", "DMP_FINANCE", "ACCOUNTS", "whole", None),
            ("FINANCE", "DMP_FINANCE", "TRANSACTIONS", "whole", None),
            ("FINANCE", "DMP_FINANCE", "MV_ACCOUNT_SUMMARY", "whole", None),
            ("AUDITLOG", "DMP_AUDITLOG", "CHANGE_LOG", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        assert mock_runner.run_imp.call_count == 4

    def test_each_schema_group_gets_correct_fromuser(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "PRODUCTS", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        calls = mock_runner.run_imp.call_args_list
        fromusers = {c[0][0].fromuser for c in calls}
        tousers = {c[0][0].touser for c in calls}
        assert fromusers == {"HRDATA", "INVENTORY"}
        assert tousers == {"DMP_HRDATA", "DMP_INVENTORY"}

    def test_each_schema_group_gets_only_its_tables(self, tmp_path: Path) -> None:
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("HRDATA", "DMP_HRDATA", "EMPLOYEES", "whole", None),
            ("HRDATA", "DMP_HRDATA", "DEPARTMENTS", "whole", None),
            ("INVENTORY", "DMP_INVENTORY", "PRODUCTS", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        calls = mock_runner.run_imp.call_args_list
        jobs: dict[str, LegacyImportJob] = {c[0][0].fromuser: c[0][0] for c in calls}

        assert set(jobs["HRDATA"].tables) == {"EMPLOYEES", "DEPARTMENTS"}
        assert set(jobs["INVENTORY"].tables) == {"PRODUCTS"}

    def test_cross_schema_no_bleed_between_groups(self, tmp_path: Path) -> None:
        """No table from one schema must appear in another schema's TABLES= list."""
        workflow, mock_runner = _make_workflow(tmp_path)
        chunks = [
            ("FINANCE", "DMP_FINANCE", "ACCOUNTS", "whole", None),
            ("AUDITLOG", "DMP_AUDITLOG", "CHANGE_LOG", "whole", None),
        ]
        workflow.import_chunks_batch(chunks)
        calls = mock_runner.run_imp.call_args_list
        jobs: dict[str, LegacyImportJob] = {c[0][0].fromuser: c[0][0] for c in calls}

        assert "CHANGE_LOG" not in jobs["FINANCE"].tables
        assert "ACCOUNTS" not in jobs["AUDITLOG"].tables
