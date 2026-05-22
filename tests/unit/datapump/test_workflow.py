"""Unit tests for datapump/workflow.py (create_workflow factory)."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_dmp_converter.datapump._workflow_base import WorkflowConfig
from oracle_dmp_converter.datapump.workflow import _ProbedModernWorkflow, create_workflow
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _cfg(tmp_path: Path) -> WorkflowConfig:
    container = MagicMock()
    return WorkflowConfig(
        container=container,
        credentials=OracleCredentials("system", "pw", "FREE"),
        directory="DUMP_DIR",
        directory_path="/dumps",
        dumpfiles=("test.dmp",),
        work_dir=tmp_path / "work",
        discovery_directory="DISC",
        inspect_directory="INSP",
        convert_directory="CONV",
    )


class TestCreateWorkflowModern:
    def test_returns_probed_modern_workflow_when_tables_found(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        mock_tables = (("SCHEMA", "TABLE"),)

        with (
            patch(
                "oracle_dmp_converter.datapump.workflow.make_modern_runners",
                return_value=(MagicMock(), MagicMock(), MagicMock()),
            ),
            patch("oracle_dmp_converter.datapump.workflow.DataPumpWorkflow") as mock_workflow_cls,
        ):
            instance = mock_workflow_cls.return_value
            instance.discover_tables.return_value = mock_tables
            instance.dump_format = DumpFormat.DATAPUMP
            instance.required_tablespaces.return_value = frozenset()

            result = create_workflow(cfg)

        assert isinstance(result, _ProbedModernWorkflow)

    def test_returns_legacy_workflow_on_legacy_format_error(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)

        with (
            patch(
                "oracle_dmp_converter.datapump.workflow.make_modern_runners",
                return_value=(MagicMock(), MagicMock(), MagicMock()),
            ),
            patch("oracle_dmp_converter.datapump.workflow.DataPumpWorkflow") as mock_modern_cls,
            patch(
                "oracle_dmp_converter.datapump.workflow.make_legacy_runners",
                return_value=(MagicMock(), MagicMock(), MagicMock()),
            ),
            patch("oracle_dmp_converter.datapump.workflow.LegacyDumpWorkflow") as mock_legacy_cls,
        ):
            modern_instance = mock_modern_cls.return_value
            modern_instance.discover_tables.side_effect = DataPumpError("ORA-39143: legacy dump")
            legacy_instance = mock_legacy_cls.return_value

            result = create_workflow(cfg)

        assert result is legacy_instance

    def test_reraises_non_legacy_datapump_error(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)

        with (
            patch(
                "oracle_dmp_converter.datapump.workflow.make_modern_runners",
                return_value=(MagicMock(), MagicMock(), MagicMock()),
            ),
            patch("oracle_dmp_converter.datapump.workflow.DataPumpWorkflow") as mock_workflow_cls,
        ):
            instance = mock_workflow_cls.return_value
            instance.discover_tables.side_effect = DataPumpError("ORA-01234: other error")
            with pytest.raises(DataPumpError, match="ORA-01234"):
                create_workflow(cfg)


class TestProbedModernWorkflow:
    def _make(self) -> _ProbedModernWorkflow:
        inner = MagicMock()
        inner.dump_format = DumpFormat.DATAPUMP
        inner.required_tablespaces.return_value = frozenset({"USERS"})
        cached = (("S", "T"),)
        return _ProbedModernWorkflow(inner, discovered_tables=cached)

    def test_discover_tables_returns_cached(self) -> None:
        wf = self._make()
        assert wf.discover_tables() == (("S", "T"),)
        wf._inner.discover_tables.assert_not_called()

    def test_dump_format_delegates(self) -> None:
        wf = self._make()
        assert wf.dump_format == DumpFormat.DATAPUMP

    def test_required_tablespaces_delegates(self) -> None:
        wf = self._make()
        assert wf.required_tablespaces() == frozenset({"USERS"})

    def test_import_all_metadata_delegates(self) -> None:
        wf = self._make()
        wf.import_all_metadata("S", "STAGE")
        wf._inner.import_all_metadata.assert_called_once_with("S", "STAGE")

    def test_import_metadata_delegates(self) -> None:
        wf = self._make()
        wf.import_metadata("S", "STAGE", "T")
        wf._inner.import_metadata.assert_called_once_with("S", "STAGE", "T")

    def test_import_chunk_delegates(self) -> None:
        wf = self._make()
        wf.import_chunk("S", "STAGE", "T", "c", None)
        wf._inner.import_chunk.assert_called_once()

    def test_import_chunks_batch_delegates(self) -> None:
        wf = self._make()
        wf.import_chunks_batch([])
        wf._inner.import_chunks_batch.assert_called_once()
