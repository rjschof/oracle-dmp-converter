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

    inspect_runner = MagicMock()
    inspect_runner.run_imp.return_value = ""  # default: clean success, no IMP/ORA codes
    convert_runner = MagicMock()
    convert_runner.run_imp.return_value = ""
    return LegacyDumpWorkflow(
        credentials=_creds(),
        directory_path="/dumps",
        dumpfiles=("legacy.dmp",),
        discovery_runner=discovery_runner,
        discovery_dir=discovery_dir,
        inspect_runner=inspect_runner,
        convert_runner=convert_runner,
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

    def test_known_codes_are_swallowed_and_logged_at_info(self, tmp_path: Path, caplog) -> None:
        """A DataPumpError whose message contains only known non-fatal codes must not propagate
        and must be logged at INFO."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00003: ORACLE error 942 encountered\n"
            "ORA-00942: table or view does not exist\n"
            "IMP-00017: following statement failed with ORACLE error 1435\n"
            "ORA-01435: user does not exist\n"
        )
        with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_all_metadata("SRC", "STAGE")  # must not raise
        assert any(r.levelno == logging.INFO for r in caplog.records)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_unknown_code_is_swallowed_and_logged_at_warning(self, tmp_path: Path, caplog) -> None:
        """A DataPumpError containing an unknown IMP/ORA code must be swallowed and logged at WARNING."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00009: abnormal end of export file\n"
        )
        with caplog.at_level(logging.WARNING, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_all_metadata("SRC", "STAGE")  # must not raise
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_error_with_no_recognisable_codes_is_re_raised(self, tmp_path: Path) -> None:
        """A DataPumpError with no IMP/ORA codes must propagate."""
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError("Connection refused")
        with pytest.raises(DataPumpError):
            wf.import_all_metadata("SRC", "STAGE")

    def test_mixed_known_and_unknown_codes_are_swallowed_and_logged_at_warning(
        self, tmp_path: Path, caplog
    ) -> None:
        """A mix of known and unknown codes must be swallowed and logged at WARNING."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "ORA-00942: table or view does not exist\nIMP-00009: abnormal end of export file\n"
        )
        with caplog.at_level(logging.WARNING, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_all_metadata("SRC", "STAGE")  # must not raise
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_exit_code2_known_codes_logged_at_info(self, tmp_path: Path, caplog) -> None:
        """When run_imp returns (exit-code-2/EX_WARN) with known codes, log at INFO."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.return_value = (
            "imp completed with warnings (returncode=2):\n"
            "IMP-00403: warning: object created with compilation errors\n"
            "ORA-04043: object AUDITLOG.LOG_CHANGE does not exist\n"
        )
        with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_all_metadata("SRC", "STAGE")  # must not raise
        msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("IMP-00403" in m for m in msgs)
        assert any("ORA-04043" in m for m in msgs)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_exit_code2_clean_output_logs_nothing(self, tmp_path: Path, caplog) -> None:
        """When run_imp returns with no IMP/ORA codes, nothing extra is logged."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.return_value = "Import: Release 23.0 ...\nImport complete.\n"
        with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_all_metadata("SRC", "STAGE")
        assert not caplog.records


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

    def test_known_codes_are_swallowed_and_logged_at_info(self, tmp_path: Path, caplog) -> None:
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00017: following statement failed with ORACLE error 942\n"
            "ORA-00942: table or view does not exist\n"
        )
        with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_metadata("SRC", "STAGE", "ORDERS")  # must not raise
        assert any(r.levelno == logging.INFO for r in caplog.records)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_unknown_code_is_swallowed_and_logged_at_warning(self, tmp_path: Path, caplog) -> None:
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError(
            "IMP-00009: abnormal end of export file\n"
        )
        with caplog.at_level(logging.WARNING, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_metadata("SRC", "STAGE", "ORDERS")  # must not raise
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_error_with_no_recognisable_codes_is_re_raised(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.side_effect = DataPumpError("Connection refused")
        with pytest.raises(DataPumpError):
            wf.import_metadata("SRC", "STAGE", "ORDERS")

    def test_exit_code2_known_codes_logged_at_info(self, tmp_path: Path, caplog) -> None:
        """When run_imp returns (exit-code-2/EX_WARN) with known codes, log at INFO."""
        import logging

        wf = _make_workflow(tmp_path)
        wf._inspect_runner.run_imp.return_value = (
            "imp completed with warnings (returncode=2):\n"
            "IMP-00041: warning: object altered with compilation warnings\n"
            "ORA-04043: object AUDITLOG.LOG_CHANGE does not exist\n"
        )
        with caplog.at_level(logging.INFO, logger="oracle_dmp_converter.datapump.legacy.workflow"):
            wf.import_metadata("SRC", "STAGE", "ORDERS")  # must not raise
        msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("IMP-00041" in m for m in msgs)
        assert any("ORA-04043" in m for m in msgs)


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
