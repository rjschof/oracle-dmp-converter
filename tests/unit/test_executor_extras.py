"""Additional unit tests for StagingExecutor uncovered paths."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_dmp_converter.config import ConverterConfig
from oracle_dmp_converter.core.executor import StagingExecutor
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import (
    ColumnMetadata,
    DumpFormat,
    TableMetadata,
)
from oracle_dmp_converter.runtime.admin import OracleAdminConnection


def _make_executor(
    config: ConverterConfig | None = None, tmp_path: Path | None = None
) -> StagingExecutor:
    return StagingExecutor(
        container=MagicMock(),
        admin=OracleAdminConnection("localhost", 1521, "FREE", "system", "pwd"),
        work_dir=tmp_path or Path("/tmp/work"),
        dumpfiles=("test.dmp",),
        config=config or ConverterConfig(),
    )


def _mock_conn_ctx():
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_ctx


# ---------------------------------------------------------------------------
# dump_format property
# ---------------------------------------------------------------------------


class TestDumpFormatProperty:
    def test_raises_when_no_workflow(self) -> None:
        executor = _make_executor()
        with pytest.raises(RuntimeError, match="dump_format is unavailable"):
            _ = executor.dump_format

    def test_returns_workflow_format(self) -> None:
        executor = _make_executor()
        mock_workflow = MagicMock()
        mock_workflow.dump_format = DumpFormat.DATAPUMP
        executor._workflow = mock_workflow
        assert executor.dump_format == DumpFormat.DATAPUMP


# ---------------------------------------------------------------------------
# use_format
# ---------------------------------------------------------------------------


class TestUseFormat:
    def test_use_format_legacy(self, tmp_path: Path) -> None:
        executor = _make_executor(tmp_path=tmp_path)
        with patch(
            "oracle_dmp_converter.core.executor.make_legacy_runners",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ):
            executor.use_format(DumpFormat.LEGACY)
        assert executor._workflow is not None
        assert executor._workflow.dump_format == DumpFormat.LEGACY

    def test_use_format_datapump(self, tmp_path: Path) -> None:
        executor = _make_executor(tmp_path=tmp_path)
        with patch(
            "oracle_dmp_converter.core.executor.make_modern_runners",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ):
            executor.use_format(DumpFormat.DATAPUMP)
        assert executor._workflow is not None


# ---------------------------------------------------------------------------
# _require_workflow
# ---------------------------------------------------------------------------


class TestRequireWorkflow:
    def test_raises_when_no_workflow(self) -> None:
        executor = _make_executor()
        with pytest.raises(RuntimeError, match="No workflow active"):
            executor._require_workflow()

    def test_returns_workflow(self) -> None:
        executor = _make_executor()
        mock_workflow = MagicMock()
        executor._workflow = mock_workflow
        assert executor._require_workflow() is mock_workflow


# ---------------------------------------------------------------------------
# _required_tablespaces
# ---------------------------------------------------------------------------


class TestRequiredTablespaces:
    def test_returns_empty_when_no_workflow(self) -> None:
        executor = _make_executor()
        assert executor._required_tablespaces() == frozenset()

    def test_delegates_to_workflow(self) -> None:
        executor = _make_executor()
        mock_workflow = MagicMock()
        mock_workflow.required_tablespaces.return_value = frozenset({"MYTS"})
        executor._workflow = mock_workflow
        assert executor._required_tablespaces() == frozenset({"MYTS"})


# ---------------------------------------------------------------------------
# drop_stage_schema
# ---------------------------------------------------------------------------


class TestDropStageSchema:
    def test_calls_drop_schema(self) -> None:
        executor = _make_executor()
        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.drop_schema") as mock_drop,
        ):
            executor.drop_stage_schema("MYSCHEMA")
        mock_drop.assert_called_once()


# ---------------------------------------------------------------------------
# _apply_staging_fixups
# ---------------------------------------------------------------------------


class TestApplyStagingFixups:
    def test_calls_all_fixups(self) -> None:
        executor = _make_executor()
        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.dematerialize_mviews") as m1,
            patch("oracle_dmp_converter.core.executor.disable_triggers") as m2,
            patch("oracle_dmp_converter.core.executor.drop_vpd_policies") as m3,
            patch("oracle_dmp_converter.core.executor.apply_byte_to_char") as m4,
        ):
            executor._apply_staging_fixups("MYSCHEMA")
        m1.assert_called_once()
        m2.assert_called_once()
        m3.assert_called_once()
        m4.assert_called_once()


# ---------------------------------------------------------------------------
# prepare_stage_schema
# ---------------------------------------------------------------------------


class TestPrepareStageSchema:
    def test_calls_ensure_schema(self) -> None:
        executor = _make_executor()
        _, mock_ctx = _mock_conn_ctx()
        executor._workflow = MagicMock()
        executor._workflow.required_tablespaces.return_value = frozenset()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_schema") as mock_ensure,
        ):
            executor.prepare_stage_schema("MYSCHEMA")
        mock_ensure.assert_called_once()

    def test_creates_tablespaces_when_required(self) -> None:
        executor = _make_executor()
        _, mock_ctx = _mock_conn_ctx()
        executor._workflow = MagicMock()
        executor._workflow.required_tablespaces.return_value = frozenset({"MYTS"})
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_schema"),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace") as mock_ts,
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited"),
        ):
            executor.prepare_stage_schema("MYSCHEMA")
        mock_ts.assert_called_once()


# ---------------------------------------------------------------------------
# import_table_chunk with legacy format truncation
# ---------------------------------------------------------------------------


class TestImportTableChunkLegacy:
    def test_legacy_truncates_before_import(self) -> None:
        executor = _make_executor()
        mock_workflow = MagicMock()
        mock_workflow.dump_format = DumpFormat.LEGACY
        executor._workflow = mock_workflow

        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.truncate_table") as mock_trunc,
            patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
        ):
            result = executor.import_table_chunk(
                source_schema="SRC", table="ORDERS", chunk_name="whole"
            )

        mock_trunc.assert_called_once()
        assert result == 5


# ---------------------------------------------------------------------------
# inspect_dump
# ---------------------------------------------------------------------------


class TestInspectDump:
    def test_returns_manifest_with_discovered_tables(self, tmp_path: Path) -> None:
        executor = _make_executor(tmp_path=tmp_path)

        mock_workflow = MagicMock()
        mock_workflow.dump_format = DumpFormat.DATAPUMP
        mock_workflow.discover_tables.return_value = (("MYSCHEMA", "ORDERS"),)
        mock_workflow.required_tablespaces.return_value = frozenset()

        mock_metadata = TableMetadata(
            schema="DMC_STAGE_MYSCHEMA",
            name="ORDERS",
            columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
        )

        _, mock_ctx = _mock_conn_ctx()

        with (
            patch("oracle_dmp_converter.core.executor.create_workflow", return_value=mock_workflow),
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_schema"),
            patch("oracle_dmp_converter.core.executor.dematerialize_mviews"),
            patch("oracle_dmp_converter.core.executor.disable_triggers"),
            patch("oracle_dmp_converter.core.executor.drop_vpd_policies"),
            patch("oracle_dmp_converter.core.executor.apply_byte_to_char"),
            patch(
                "oracle_dmp_converter.core.executor.discover_table_metadata",
                return_value=mock_metadata,
            ),
        ):
            manifest = executor.inspect_dump()

        assert len(manifest.tables) == 1
        assert manifest.tables[0].name == "ORDERS"
        assert manifest.tables[0].schema == "MYSCHEMA"
        assert manifest.dump_format == DumpFormat.DATAPUMP


# ---------------------------------------------------------------------------
# _recover_missing_tablespaces
# ---------------------------------------------------------------------------


class TestRecoverMissingTablespaces:
    def test_returns_false_when_no_ora_00959(self) -> None:
        executor = _make_executor()
        output = "ORA-00942: table or view does not exist\n"
        assert executor._recover_missing_tablespaces(output, "SRC") is False

    def test_returns_false_for_empty_output(self) -> None:
        executor = _make_executor()
        assert executor._recover_missing_tablespaces("", "SRC") is False

    def test_creates_tablespace_and_grants_quota(self) -> None:
        executor = _make_executor()
        output = "ORA-00959: tablespace 'MISSING_TS' does not exist\n"
        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace") as mock_ts,
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited") as mock_quota,
        ):
            result = executor._recover_missing_tablespaces(output, "SRC")
        assert result is True
        mock_ts.assert_called_once()
        mock_quota.assert_called_once()

    def test_creates_multiple_tablespaces(self) -> None:
        executor = _make_executor()
        output = (
            "ORA-00959: tablespace 'TS_ONE' does not exist\n"
            "ORA-00959: tablespace 'TS_TWO' does not exist\n"
        )
        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace") as mock_ts,
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited") as mock_quota,
        ):
            result = executor._recover_missing_tablespaces(output, "SRC")
        assert result is True
        assert mock_ts.call_count == 2
        assert mock_quota.call_count == 2

    def test_grants_quota_to_stage_schema(self) -> None:
        executor = _make_executor()
        output = "ORA-00959: tablespace 'APP_TS' does not exist\n"
        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace"),
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited") as mock_quota,
        ):
            executor._recover_missing_tablespaces(output, "MYSCHEMA")
        quota_call_args = mock_quota.call_args[0]
        # Second positional arg is schema name; should be the staging schema
        assert quota_call_args[1] == "DMP_MYSCHEMA"


# ---------------------------------------------------------------------------
# import_table_chunk mid-import ORA-00959 recovery
# ---------------------------------------------------------------------------


class TestImportTableChunkTablespaceRecovery:
    def _setup_workflow(self, dump_format: DumpFormat = DumpFormat.DATAPUMP) -> MagicMock:
        mock_workflow = MagicMock()
        mock_workflow.dump_format = dump_format
        return mock_workflow

    def test_recovers_and_retries_on_ora_00959(self) -> None:
        executor = _make_executor()
        mock_workflow = self._setup_workflow()
        # First call raises ORA-00959, second succeeds
        mock_workflow.import_chunk.side_effect = [
            DataPumpError("ORA-00959: tablespace 'CUSTOM_TS' does not exist\n"),
            None,
        ]
        executor._workflow = mock_workflow

        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace"),
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited"),
            patch("oracle_dmp_converter.core.executor.count_rows", return_value=10),
        ):
            result = executor.import_table_chunk(
                source_schema="SRC", table="ORDERS", chunk_name="whole"
            )
        assert mock_workflow.import_chunk.call_count == 2
        assert result == 10

    def test_reraises_when_no_ora_00959(self) -> None:
        executor = _make_executor()
        mock_workflow = self._setup_workflow()
        mock_workflow.import_chunk.side_effect = DataPumpError("IMP-00009: abnormal end of export")
        executor._workflow = mock_workflow

        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.count_rows", return_value=0),
        ):
            with pytest.raises(DataPumpError, match="IMP-00009"):
                executor.import_table_chunk(
                    source_schema="SRC", table="ORDERS", chunk_name="whole"
                )

    def test_reraises_on_second_failure(self) -> None:
        executor = _make_executor()
        mock_workflow = self._setup_workflow()
        mock_workflow.import_chunk.side_effect = DataPumpError(
            "ORA-00959: tablespace 'CUSTOM_TS' does not exist\n"
        )
        executor._workflow = mock_workflow

        _, mock_ctx = _mock_conn_ctx()
        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.ensure_tablespace"),
            patch("oracle_dmp_converter.core.executor.grant_quota_unlimited"),
        ):
            with pytest.raises(DataPumpError):
                executor.import_table_chunk(
                    source_schema="SRC", table="ORDERS", chunk_name="whole"
                )
