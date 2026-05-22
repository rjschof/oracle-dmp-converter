"""Additional unit tests for datapump/legacy/workflow.py (uncovered code paths)."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from oracle_dmp_converter.datapump.legacy.workflow import LegacyDumpWorkflow, make_legacy_runners
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

    def test_falls_back_to_directory_path_when_indexfile_empty(self, tmp_path: Path) -> None:
        discovery_dir = tmp_path / "discovery"
        discovery_dir.mkdir(parents=True)
        fallback_sql = 'REM CREATE TABLE "S"."FALLBACK" (\n'

        fallback_result = MagicMock()
        fallback_result.returncode = 0
        fallback_result.stdout = fallback_sql

        container = MagicMock()
        container.exec.return_value = fallback_result

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
        tables = wf.discover_tables()
        assert ("S", "FALLBACK") in tables


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
                ("SRC1", "STAGE1", "T1", "whole", None),
                ("SRC1", "STAGE1", "T2", "whole", None),
                ("SRC2", "STAGE2", "T3", "whole", None),
            ]
        )
        assert wf._convert_runner.run_imp.call_count == 2

    def test_deduplicates_tables_within_schema_group(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch(
            [
                ("SRC", "STAGE", "T1", "c1", None),
                ("SRC", "STAGE", "T1", "c2", None),
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
