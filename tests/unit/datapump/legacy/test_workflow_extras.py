"""Additional unit tests for datapump/legacy/workflow.py (uncovered code paths)."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_dmp_converter.datapump.legacy.workflow import LegacyDumpWorkflow, make_legacy_runners
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _creds() -> OracleCredentials:
    return OracleCredentials(user="system", password="pw", service="FREE")


def _make_workflow(
    tmp_path: Path,
    indexfile_sql: str = "",
    cat_fallback: str = "",
) -> LegacyDumpWorkflow:
    discovery_dir = tmp_path / "discovery"
    discovery_dir.mkdir(parents=True, exist_ok=True)

    imp_result = MagicMock()
    imp_result.returncode = 0
    imp_result.stdout = "Import complete"
    imp_result.stderr = ""

    cat_result = MagicMock()
    cat_result.returncode = 0 if indexfile_sql else 1
    cat_result.stdout = indexfile_sql
    cat_result.stderr = ""

    fallback_result = MagicMock()
    fallback_result.returncode = 0 if cat_fallback else 1
    fallback_result.stdout = cat_fallback

    container = MagicMock()
    container.exec.side_effect = [imp_result, cat_result, fallback_result]

    discovery_runner = MagicMock()
    discovery_runner.container = container
    discovery_runner.run_imp_indexfile.return_value = (indexfile_sql, "Import complete")

    return LegacyDumpWorkflow(
        credentials=_creds(),
        directory_path="/dumps",
        dumpfiles=("legacy.dmp",),
        discovery_runner=discovery_runner,
        discovery_dir=discovery_dir,
        inspect_runner=MagicMock(),
        convert_runner=MagicMock(),
    )


class TestLegacyDumpWorkflowDumpFormat:
    def test_format_is_legacy(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        assert wf.dump_format == DumpFormat.LEGACY


class TestDiscoverTables:
    def test_discovers_tables_from_indexfile_sql(self, tmp_path: Path) -> None:
        sql = 'REM CREATE TABLE "HRDATA"."DEPARTMENTS" (\nREM CREATE TABLE "HRDATA"."EMPLOYEES" (\n'
        wf = _make_workflow(tmp_path, indexfile_sql=sql)
        tables = wf.discover_tables()
        assert ("HRDATA", "DEPARTMENTS") in tables
        assert ("HRDATA", "EMPLOYEES") in tables

    def test_caches_indexfile_result(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path, indexfile_sql='REM CREATE TABLE "S"."T" (\n')
        wf.discover_tables()
        wf.discover_tables()
        wf._discovery_runner.run_imp_indexfile.assert_called_once()

    def test_empty_indexfile_raises_data_pump_error(self, tmp_path: Path) -> None:
        """An empty indexfile is always a real failure and must be surfaced.

        Earlier versions of the workflow attempted a ``cat`` fallback against
        the dump directory.  That was removed in favour of fail-fast
        behaviour (the indexfile is now written into a rw-mounted work-dir
        path, so the only reason for an empty result is a genuine imp
        failure that the caller needs to see).
        """
        discovery_dir = tmp_path / "discovery"
        discovery_dir.mkdir(parents=True)

        container = MagicMock()
        discovery_runner = MagicMock()
        discovery_runner.container = container
        discovery_runner.run_imp_indexfile.return_value = ("", "Import complete")

        wf = LegacyDumpWorkflow(
            credentials=_creds(),
            directory_path="/dumps",
            dumpfiles=("legacy.dmp",),
            discovery_runner=discovery_runner,
            discovery_dir=discovery_dir,
            inspect_runner=MagicMock(),
            convert_runner=MagicMock(),
        )
        with pytest.raises(DataPumpError, match="produced no SQL output"):
            wf.discover_tables()
        # The container fallback must no longer be invoked.
        container.exec.assert_not_called()
        # The log is still persisted so operators can diagnose the failure.
        assert (discovery_dir / "discovery-imp-indexfile.log").read_text() == "Import complete"


class TestRequiredTablespaces:
    def test_returns_tablespaces_from_ddl(self, tmp_path: Path) -> None:
        sql = (
            'CREATE TABLE "S"."T" (ID NUMBER)\nTABLESPACE "MY_CUSTOM_TS" STORAGE (INITIAL 8192);\n'
        )
        wf = _make_workflow(tmp_path, indexfile_sql=sql)
        ts = wf.required_tablespaces()
        assert "MY_CUSTOM_TS" in ts


class TestImportAllMetadata:
    def test_calls_inspect_runner_run_imp(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_all_metadata("SRC", "STAGE")
        wf._inspect_runner.run_imp.assert_called_once()

    def test_job_uses_rows_false(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_all_metadata("SRC", "STAGE")
        job = wf._inspect_runner.run_imp.call_args[0][0]
        assert job.rows is False

    def test_non_fatal_error_codes_are_swallowed(self, tmp_path: Path) -> None:
        """A DataPumpError whose message contains only known non-fatal codes must not propagate."""
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00003: ORACLE error 942 encountered\n"
            "ORA-00942: table or view does not exist\n"
            "IMP-00017: following statement failed with ORACLE error 1435\n"
            "ORA-01435: user does not exist\n"
        )
        # Should not raise
        wf.import_all_metadata("SRC", "STAGE")

    def test_fatal_error_code_is_re_raised(self, tmp_path: Path) -> None:
        """A DataPumpError that contains an unknown code must propagate."""
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00009: abnormal end of export file\n"
        )
        with pytest.raises(DataPumpError):
            wf.import_all_metadata("SRC", "STAGE")

    def test_error_with_no_recognisable_codes_is_re_raised(self, tmp_path: Path) -> None:
        """A DataPumpError with no IMP/ORA codes must propagate."""
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError("Connection refused")
        with pytest.raises(DataPumpError):
            wf.import_all_metadata("SRC", "STAGE")

    def test_mixed_fatal_and_non_fatal_codes_are_re_raised(self, tmp_path: Path) -> None:
        """A mix of known and unknown codes must propagate."""
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "ORA-00942: table or view does not exist\nIMP-00009: abnormal end of export file\n"
        )
        with pytest.raises(DataPumpError):
            wf.import_all_metadata("SRC", "STAGE")


class TestImportMetadata:
    def test_calls_inspect_runner_run_imp(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_metadata("SRC", "STAGE", "ORDERS")
        wf._inspect_runner.run_imp.assert_called_once()

    def test_job_includes_table_name(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_metadata("SRC", "STAGE", "ORDERS")
        job = wf._inspect_runner.run_imp.call_args[0][0]
        assert "ORDERS" in job.tables

    def test_non_fatal_error_codes_are_swallowed(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00017: following statement failed with ORACLE error 942\n"
            "ORA-00942: table or view does not exist\n"
        )
        # Should not raise
        wf.import_metadata("SRC", "STAGE", "ORDERS")

    def test_fatal_error_code_is_re_raised(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00009: abnormal end of export file\n"
        )
        with pytest.raises(DataPumpError):
            wf.import_metadata("SRC", "STAGE", "ORDERS")

    def test_error_with_no_recognisable_codes_is_re_raised(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError("Connection refused")
        with pytest.raises(DataPumpError):
            wf.import_metadata("SRC", "STAGE", "ORDERS")


class TestImportChunk:
    def test_calls_convert_runner_with_rows_true(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunk("SRC", "STAGE", "ORDERS", "whole", None)
        wf._convert_runner.run_imp.assert_called_once()
        job = wf._convert_runner.run_imp.call_args[0][0]
        assert job.rows is True


class TestImportChunksBatch:
    def test_empty_batch_is_noop(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch([])
        wf._convert_runner.run_imp.assert_not_called()

    def test_groups_by_schema_pair(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch(
            [
                ("SRC1", "STAGE1", "T1", "whole", None, None),
                ("SRC1", "STAGE1", "T2", "whole", None, None),
                ("SRC2", "STAGE2", "T3", "whole", None, None),
            ]
        )
        assert wf._convert_runner.run_imp.call_count == 2

    def test_deduplicates_tables_within_schema_group(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch(
            [
                ("SRC", "STAGE", "T1", "c1", None, None),
                ("SRC", "STAGE", "T1", "c2", None, None),
            ]
        )
        job = wf._convert_runner.run_imp.call_args[0][0]
        assert job.tables.count("T1") == 1


class TestMakeLegacyRunners:
    def test_creates_three_runners(self, tmp_path: Path) -> None:
        container = MagicMock()
        disc, insp, conv = make_legacy_runners(container, tmp_path)
        assert disc.container is container
        assert insp.container is container
        assert conv.container is container
