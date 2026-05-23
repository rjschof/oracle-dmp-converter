"""Additional unit tests for StagingExecutor uncovered paths."""
# pylint: disable=protected-access

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_dmp_converter.config import ConverterConfig
from oracle_dmp_converter.core.executor import (
    StagingExecutor,
    _emit_phase_timing,
    _phase_timer,
)
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
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
                executor.import_table_chunk(source_schema="SRC", table="ORDERS", chunk_name="whole")

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
                executor.import_table_chunk(source_schema="SRC", table="ORDERS", chunk_name="whole")


class TestPhaseTimingHelpers:
    """Cover the gated phase-timing instrumentation helpers."""

    def test_emit_is_noop_when_env_var_unset(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("DMP_CONVERTER_PHASE_TIMING_FILE", raising=False)
        target = tmp_path / "should_not_exist.jsonl"
        _emit_phase_timing("preclear_modern_plan", tables=5)
        assert not target.exists()

    def test_emit_appends_jsonl_when_env_var_set(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        target = tmp_path / "phases.jsonl"
        monkeypatch.setenv("DMP_CONVERTER_PHASE_TIMING_FILE", str(target))
        _emit_phase_timing("import_batch", dump_format="datapump", chunks=12)
        _emit_phase_timing("export_chunk", table="HR.EMP", chunk="whole")

        lines = target.read_text().splitlines()
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first == {"phase": "import_batch", "dump_format": "datapump", "chunks": 12}
        second = json.loads(lines[1])
        assert second == {"phase": "export_chunk", "table": "HR.EMP", "chunk": "whole"}

    def test_emit_swallows_oserror(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Point at a path that can never be opened (directory that doesn't exist).
        monkeypatch.setenv(
            "DMP_CONVERTER_PHASE_TIMING_FILE", "/nonexistent/dir/phases.jsonl"
        )
        # Must not raise.
        _emit_phase_timing("preclear_legacy_batch", chunks=3)

    def test_phase_timer_records_elapsed_ms(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        target = tmp_path / "phases.jsonl"
        monkeypatch.setenv("DMP_CONVERTER_PHASE_TIMING_FILE", str(target))

        with _phase_timer("preclear_modern_plan", dump_format="datapump", tables=7):
            pass  # zero-work block; just verifying the record shape

        lines = target.read_text().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["phase"] == "preclear_modern_plan"
        assert record["dump_format"] == "datapump"
        assert record["tables"] == 7
        assert isinstance(record["took_ms"], (int, float))
        assert record["took_ms"] >= 0


def _whole_table_plan(schema: str, table: str) -> TablePlan:
    return TablePlan(
        schema=schema,
        table=table,
        strategy=TableStrategy.WHOLE_TABLE,
        chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
    )


def _fake_metadata(schema: str, table: str) -> TableMetadata:
    return TableMetadata(
        schema=schema,
        name=table,
        columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
    )


def _fake_export(tmp_path: Path, schema: str, table: str) -> MagicMock:
    m = MagicMock()
    m.rows = 5
    m.path = tmp_path / schema / table / "whole.parquet"
    return m


def _cursor_returning(fetchone_value: object) -> MagicMock:
    """Context-managed mock cursor whose fetchone() returns a fixed value."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = fetchone_value
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__ = lambda s: mock_cursor
    cursor_ctx.__exit__ = MagicMock(return_value=False)
    return cursor_ctx


class TestStageMetadataCache:
    """T1.1 — staging metadata is discovered once per (stage_schema, table)."""

    def test_caches_metadata_across_chunks_of_same_table(self, tmp_path: Path) -> None:
        executor = _make_executor(tmp_path=tmp_path)
        _, mock_ctx = _mock_conn_ctx()

        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch(
                "oracle_dmp_converter.core.executor.discover_table_metadata",
                return_value=_fake_metadata("DMP_SRC", "ORDERS"),
            ) as mock_discover,
            patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
            patch(
                "oracle_dmp_converter.core.executor.export_table",
                return_value=_fake_export(tmp_path, "src", "orders"),
            ),
        ):
            for chunk_name in ("partition-1", "partition-2"):
                executor.export_stage_table(
                    source_schema="SRC",
                    table="ORDERS",
                    chunk_name=chunk_name,
                    output_dir=tmp_path,
                )
            assert mock_discover.call_count == 1  # same table → one discovery

            executor.export_stage_table(
                source_schema="SRC",
                table="INVOICES",
                chunk_name="whole",
                output_dir=tmp_path,
            )
            assert mock_discover.call_count == 2  # different table → cache miss


class TestValidateMetadataState:
    """T1.4 — validate_metadata_state only fails-fast on missing users."""

    def _plan(self) -> ConversionPlan:
        return ConversionPlan(
            dump_paths=("test.dmp",),
            oracle_image="gvenzl/oracle-free:23-faststart",
            tables=(_whole_table_plan("SRC", "ORDERS"),),
        )

    def test_raises_on_missing_user(self) -> None:
        executor = _make_executor()
        executor.metadata_imported = True
        mock_conn, mock_ctx = _mock_conn_ctx()
        mock_conn.cursor.return_value = _cursor_returning(None)  # user not found

        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.table_exists") as mock_table_exists,
        ):
            with pytest.raises(ValueError, match="missing staging users"):
                executor.validate_metadata_state(self._plan())
        mock_table_exists.assert_not_called()

    def test_passes_without_per_table_existence_query(self) -> None:
        executor = _make_executor()
        executor.metadata_imported = True
        mock_conn, mock_ctx = _mock_conn_ctx()
        mock_conn.cursor.return_value = _cursor_returning((1,))  # user found

        with (
            patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
            patch("oracle_dmp_converter.core.executor.table_exists") as mock_table_exists,
        ):
            executor.validate_metadata_state(self._plan())  # no raise
        mock_table_exists.assert_not_called()

    def test_noop_when_metadata_not_imported(self) -> None:
        executor = _make_executor()
        executor.metadata_imported = False
        # No connection patched: must return before touching Oracle.
        executor.validate_metadata_state(self._plan())
