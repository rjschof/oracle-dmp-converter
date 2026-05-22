"""Unit tests for datapump/modern/runner.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from oracle_dmp_converter.datapump.modern.parfile import (
    BatchImportJob,
    BulkMetadataImportJob,
    ExportJob,
    ImportJob,
    SqlFileJob,
)
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner, is_legacy_format_error
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


class TestIsLegacyFormatError:
    def test_detects_ora_39142(self) -> None:
        assert is_legacy_format_error("ORA-39142: incompatible version") is True

    def test_detects_ora_39143(self) -> None:
        assert is_legacy_format_error("ORA-39143: original export dump file") is True

    def test_returns_false_for_other_errors(self) -> None:
        assert is_legacy_format_error("ORA-01234: some other error") is False

    def test_returns_false_for_success_output(self) -> None:
        assert is_legacy_format_error("Export completed successfully") is False


class TestDataPumpRunner:
    def _runner(self, tmp_path: Path, returncode: int = 0) -> DataPumpRunner:
        return DataPumpRunner(_make_container(returncode=returncode), tmp_path)

    def test_run_expdp_succeeds(self, tmp_path: Path) -> None:
        runner = self._runner(tmp_path)
        job = ExportJob(
            connection=_creds(),
            directory="DUMP_DIR",
            dumpfile="test.dmp",
            logfile="test.log",
            include_schemas=("MYSCHEMA",),
        )
        output = runner.run_expdp(job)
        assert output is not None

    def test_run_sqlfile_succeeds(self, tmp_path: Path) -> None:
        runner = self._runner(tmp_path)
        job = SqlFileJob(
            connection=_creds(),
            directory="DUMP_DIR",
            dumpfiles=("test.dmp",),
            logfile="DISC_DIR:disc.log",
            sqlfile="DISC_DIR:disc.sql",
        )
        output = runner.run_sqlfile(job)
        assert output is not None

    def test_read_remote_file_returns_stdout_on_success(self, tmp_path: Path) -> None:
        container = _make_container(returncode=0, stdout="file contents")
        runner = DataPumpRunner(container, tmp_path)
        result = runner.read_remote_file("/tmp/test.sql")
        assert result == "file contents"

    def test_read_remote_file_returns_empty_on_failure(self, tmp_path: Path) -> None:
        container = _make_container(returncode=1, stdout="", stderr="not found")
        runner = DataPumpRunner(container, tmp_path)
        result = runner.read_remote_file("/tmp/missing.sql")
        assert result == ""

    def test_run_impdp_succeeds(self, tmp_path: Path) -> None:
        runner = self._runner(tmp_path)
        job = ImportJob(
            connection=_creds(),
            directory="DUMP_DIR",
            dumpfiles=("test.dmp",),
            logfile="INSP:meta.log",
            source_schema="SRC",
            table="ORDERS",
            remap_schema=("SRC", "STAGE"),
            content="METADATA_ONLY",
            table_exists_action="REPLACE",
            exclude=(),
        )
        output = runner.run_impdp(job)
        assert output is not None

    def test_run_batch_impdp_succeeds(self, tmp_path: Path) -> None:
        runner = self._runner(tmp_path)
        job = BatchImportJob(
            connection=_creds(),
            directory="DUMP_DIR",
            dumpfiles=("test.dmp",),
            logfile="CONV:batch.log",
            table_specs=(("SRC", "ORDERS", None),),
            remap_schemas=(("SRC", "STAGE"),),
            content="DATA_ONLY",
        )
        output = runner.run_batch_impdp(job)
        assert output is not None

    def test_run_bulk_metadata_impdp_succeeds(self, tmp_path: Path) -> None:
        runner = self._runner(tmp_path)
        job = BulkMetadataImportJob(
            connection=_creds(),
            directory="DUMP_DIR",
            dumpfiles=("test.dmp",),
            logfile="INSP:bulk.log",
            remap_schema=("SRC", "STAGE"),
            schemas=("SRC",),
        )
        output = runner.run_bulk_metadata_impdp(job)
        assert output is not None
