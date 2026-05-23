"""Unit tests for StagingExecutor internals."""
# pylint: disable=protected-access,unused-argument,unused-variable,missing-function-docstring

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_dmp_converter.config import ColumnOverride, ConverterConfig
from oracle_dmp_converter.core.executor import StagingExecutor
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.persistence.state import ChunkState, StateStore
from oracle_dmp_converter.runtime.admin import OracleAdminConnection


def _make_converter(config: ConverterConfig) -> StagingExecutor:
    return StagingExecutor(
        container=MagicMock(),
        admin=OracleAdminConnection("localhost", 1521, "FREE", "system", "pwd"),
        work_dir=Path("/tmp/work"),
        dumpfiles=("test.dmp",),
        config=config,
    )


def _whole_table_plan(schema: str, table: str) -> TablePlan:
    return TablePlan(
        schema=schema,
        table=table,
        strategy=TableStrategy.WHOLE_TABLE,
        chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
    )


def _fake_export(tmp_path: Path, schema: str, table: str, rows: int = 5) -> MagicMock:
    m = MagicMock()
    m.rows = rows
    m.path = tmp_path / schema / table / "whole.parquet"
    return m


def _mock_conn_ctx() -> tuple[MagicMock, MagicMock]:
    """Return (mock_conn, context_manager) where context_manager yields mock_conn."""
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_ctx


def _fake_metadata(schema: str, table: str) -> TableMetadata:
    return TableMetadata(
        schema=schema,
        name=table,
        columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
    )


def test_export_stage_table_passes_column_overrides(tmp_path: Path) -> None:
    """Column overrides from ConverterConfig are forwarded to export_table()."""
    config = ConverterConfig(
        columns={
            "APP.ORDERS.GEOM": ColumnOverride(
                expression="SDO_UTIL.TO_WKTGEOMETRY({column})",
                parquet_type="string",
            )
        }
    )
    converter = _make_converter(config)

    columns = (
        ColumnMetadata("ID", "NUMBER", 1, nullable=False),
        ColumnMetadata("GEOM", "SDO_GEOMETRY", 2),
    )
    fake_metadata = TableMetadata(schema="DMP_APP", name="ORDERS", columns=columns)

    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    fake_export = MagicMock()
    fake_export.rows = 0
    fake_export.path = tmp_path / "app" / "orders" / "whole.parquet"

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=0),
        patch(
            "oracle_dmp_converter.core.executor.export_table", return_value=fake_export
        ) as mock_export,
    ):
        converter.export_stage_table(
            source_schema="APP",
            table="ORDERS",
            chunk_name="whole",
            output_dir=tmp_path,
        )

    mock_export.assert_called_once()
    call_kwargs = mock_export.call_args.kwargs
    overrides = call_kwargs.get("column_overrides")
    assert overrides is not None, "column_overrides should be passed to export_table"
    assert "GEOM" in overrides
    assert overrides["GEOM"].expression == "SDO_UTIL.TO_WKTGEOMETRY({column})"
    assert overrides["GEOM"].parquet_type == "string"
    assert "ID" not in overrides, "columns without overrides should not appear in the dict"


def test_export_stage_table_no_overrides_passes_none(tmp_path: Path) -> None:
    """When no column overrides are configured, export_table receives column_overrides=None."""
    converter = _make_converter(ConverterConfig())

    columns = (ColumnMetadata("ID", "NUMBER", 1, nullable=False),)
    fake_metadata = TableMetadata(schema="DMP_APP", name="ORDERS", columns=columns)

    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    fake_export = MagicMock()
    fake_export.rows = 0
    fake_export.path = tmp_path / "app" / "orders" / "whole.parquet"

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=0),
        patch(
            "oracle_dmp_converter.core.executor.export_table", return_value=fake_export
        ) as mock_export,
    ):
        converter.export_stage_table(
            source_schema="APP",
            table="ORDERS",
            chunk_name="whole",
            output_dir=tmp_path,
        )

    call_kwargs = mock_export.call_args.kwargs
    assert call_kwargs.get("column_overrides") is None


def test_convert_plan_skips_unsupported_tables(tmp_path: Path) -> None:
    """convert_plan() must skip UNSUPPORTED tables and only batch supported ones."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    unsupported_plan = TablePlan(
        schema="SRC",
        table="BROKEN",
        strategy=TableStrategy.UNSUPPORTED,
        reason="strategy 'range' is not supported",
    )
    good_plan = _whole_table_plan("SRC", "SMALL")
    plan = ConversionPlan(
        dump_paths=("test.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(unsupported_plan, good_plan),
    )

    _, mock_ctx = _mock_conn_ctx()
    fake_export = _fake_export(tmp_path, "src", "small", rows=5)

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=_fake_metadata("DMP_SRC", "SMALL"),
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
        patch("oracle_dmp_converter.core.executor.export_table", return_value=fake_export),
    ):
        result = converter.convert_plan(plan, tmp_path)

    # Only the good table appears in results; the UNSUPPORTED one is silently skipped.
    assert len(result.tables) == 1
    assert result.tables[0].table == "SMALL"
    # The workflow import was called exactly once for the single good table's chunk.
    mock_workflow.import_chunks_batch.assert_called_once()
    specs = mock_workflow.import_chunks_batch.call_args[0][0]
    assert len(specs) == 1
    assert specs[0][2] == "SMALL"  # table name is the third element


def test_convert_table_batch_single_impdp_call(tmp_path: Path) -> None:
    """A batch of N tables triggers exactly one import_chunks_batch() call with N specs."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    plans = [_whole_table_plan("SRC", t) for t in ("ORDERS", "PRODUCTS", "CUSTOMERS")]

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            side_effect=lambda conn, schema, table: _fake_metadata(schema, table),
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
        patch(
            "oracle_dmp_converter.core.executor.export_table",
            side_effect=lambda conn, **kw: _fake_export(tmp_path, "src", kw["table_name"].lower()),
        ),
    ):
        results = converter.convert_table_batch(plans, tmp_path)

    # One import_chunks_batch call containing all three chunk specs.
    mock_workflow.import_chunks_batch.assert_called_once()
    specs = mock_workflow.import_chunks_batch.call_args[0][0]
    assert len(specs) == 3
    imported_tables = {s[2] for s in specs}
    assert imported_tables == {"ORDERS", "PRODUCTS", "CUSTOMERS"}

    # Each table gets its own result entry.
    assert len(results) == 3
    assert {r.table for r in results} == {"ORDERS", "PRODUCTS", "CUSTOMERS"}


def test_convert_table_batch_skips_completed_chunks(tmp_path: Path) -> None:
    """Chunks already marked 'completed' in state_store are not re-imported."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    done_plan = _whole_table_plan("SRC", "DONE")
    todo_plan = _whole_table_plan("SRC", "TODO")

    state_store = MagicMock(spec=StateStore)

    def fake_state_get(table_name: str, chunk_name: str) -> ChunkState | None:
        if "DONE" in table_name:
            return ChunkState(table_name, chunk_name, "completed", imported_rows=7, output_rows=7)
        return None

    state_store.get.side_effect = fake_state_get

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            side_effect=lambda conn, schema, table: _fake_metadata(schema, table),
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=4),
        patch(
            "oracle_dmp_converter.core.executor.export_table",
            return_value=_fake_export(tmp_path, "src", "todo", rows=4),
        ),
    ):
        results = converter.convert_table_batch([done_plan, todo_plan], tmp_path, state_store)

    # Only the pending chunk appears in the batch import.
    mock_workflow.import_chunks_batch.assert_called_once()
    specs = mock_workflow.import_chunks_batch.call_args[0][0]
    assert len(specs) == 1
    assert specs[0][2] == "TODO"

    # Both tables appear in results, DONE reuses its stored row counts.
    assert len(results) == 2
    done_result = next(r for r in results if r.table == "DONE")
    assert done_result.rows == 7


def test_convert_table_batch_marks_all_failed_on_import_error(tmp_path: Path) -> None:
    """If import_chunks_batch() raises, every pending chunk is marked 'failed'."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    mock_workflow.import_chunks_batch.side_effect = RuntimeError("impdp boom")
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    plans = [_whole_table_plan("SRC", t) for t in ("A", "B")]
    state_store = MagicMock(spec=StateStore)
    state_store.get.return_value = None

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        pytest.raises(RuntimeError, match="impdp boom"),
    ):
        converter.convert_table_batch(plans, tmp_path, state_store)

    # Both pending chunks marked failed.
    failed_calls = [c for c in state_store.upsert.call_args_list if c.args[0].status == "failed"]
    assert len(failed_calls) == 2
    failed_tables = {c.args[0].table_name for c in failed_calls}
    assert "SRC.A" in failed_tables
    assert "SRC.B" in failed_tables


# ---------------------------------------------------------------------------
# Partition-name forwarding through export_stage_table and convert_table_batch
# ---------------------------------------------------------------------------


def _partition_table_plan(schema: str, table: str, partitions: list[str]) -> TablePlan:
    """Build a TablePlan with PARTITION strategy chunks for each partition name."""
    chunks = tuple(
        ChunkPlan(
            name=f"partition-{i + 1:05d}-{p}",
            strategy=TableStrategy.PARTITION,
            partition_name=p,
        )
        for i, p in enumerate(partitions)
    )
    return TablePlan(schema=schema, table=table, strategy=TableStrategy.PARTITION, chunks=chunks)


def test_export_stage_table_passes_partition_name_to_export_table(tmp_path: Path) -> None:
    """partition_name is forwarded to export_table when supplied."""
    converter = _make_converter(ConverterConfig())

    columns = (ColumnMetadata("ID", "NUMBER", 1, nullable=False),)
    fake_metadata = TableMetadata(schema="DMP_FINANCE", name="TRANSACTIONS", columns=columns)

    fake_export = MagicMock()
    fake_export.rows = 20
    fake_export.path = tmp_path / "finance" / "transactions" / "partition-00001-P_Q1.parquet"

    mock_conn, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=20) as mock_count,
        patch(
            "oracle_dmp_converter.core.executor.export_table", return_value=fake_export
        ) as mock_export,
    ):
        result = converter.export_stage_table(
            source_schema="FINANCE",
            table="TRANSACTIONS",
            chunk_name="partition-00001-P_Q1",
            output_dir=tmp_path,
            partition_name="P_Q1",
        )

    # count_rows must receive the partition name so the row count is scoped.
    mock_count.assert_called_once()
    count_args = mock_count.call_args
    # count_rows(conn, schema, table, partition_name) — partition_name is args[3]
    assert count_args.args[3] == "P_Q1" or count_args.kwargs.get("partition_name") == "P_Q1"

    # export_table must receive partition_name so the SELECT is partition-filtered.
    mock_export.assert_called_once()
    export_kwargs = mock_export.call_args.kwargs
    assert export_kwargs.get("partition_name") == "P_Q1"

    assert result.imported_rows == 20
    assert result.output_rows == 20


def test_export_stage_table_no_partition_name_omits_filter(tmp_path: Path) -> None:
    """Without a partition_name, export_table receives partition_name=None."""
    converter = _make_converter(ConverterConfig())

    columns = (ColumnMetadata("ID", "NUMBER", 1, nullable=False),)
    fake_metadata = TableMetadata(schema="DMP_SRC", name="ORDERS", columns=columns)

    fake_export = MagicMock()
    fake_export.rows = 10
    fake_export.path = tmp_path / "src" / "orders" / "whole.parquet"

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=10),
        patch(
            "oracle_dmp_converter.core.executor.export_table", return_value=fake_export
        ) as mock_export,
    ):
        converter.export_stage_table(
            source_schema="SRC",
            table="ORDERS",
            chunk_name="whole",
            output_dir=tmp_path,
        )

    export_kwargs = mock_export.call_args.kwargs
    assert export_kwargs.get("partition_name") is None


def test_convert_table_batch_passes_partition_name_for_partition_chunks(tmp_path: Path) -> None:
    """Partition chunks in a batch export pass their partition_name to export_stage_table."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    plan = _partition_table_plan("FINANCE", "TRANSACTIONS", ["P_Q1", "P_Q2", "P_Q3"])
    _, mock_ctx = _mock_conn_ctx()

    captured_partition_names: list[str | None] = []

    def fake_export_stage(
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        output_dir: Path,
        partition_name: str | None = None,
    ):
        captured_partition_names.append(partition_name)
        return MagicMock(
            name=chunk_name,
            imported_rows=20,
            output_rows=20,
            output_path=tmp_path / "finance" / "transactions" / f"{chunk_name}.parquet",
        )

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch.object(converter, "export_stage_table", side_effect=fake_export_stage),
    ):
        converter.convert_table_batch([plan], tmp_path)

    # All three partition chunks should have forwarded their partition names.
    assert captured_partition_names == ["P_Q1", "P_Q2", "P_Q3"]


def test_convert_table_batch_whole_table_chunk_passes_none_partition_name(
    tmp_path: Path,
) -> None:
    """Whole-table chunks pass partition_name=None to export_stage_table."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # type: ignore[attr-defined]

    plan = _whole_table_plan("SRC", "ORDERS")
    _, mock_ctx = _mock_conn_ctx()

    captured_partition_names: list[str | None] = []

    def fake_export_stage(
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        output_dir: Path,
        partition_name: str | None = None,
    ):
        captured_partition_names.append(partition_name)
        return MagicMock(
            name=chunk_name,
            imported_rows=5,
            output_rows=5,
            output_path=tmp_path / "src" / "orders" / "whole.parquet",
        )

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch.object(converter, "export_stage_table", side_effect=fake_export_stage),
    ):
        converter.convert_table_batch([plan], tmp_path)

    assert captured_partition_names == [None]


# ---------------------------------------------------------------------------
# validate_staging_tables / convert_plan metadata reuse
# ---------------------------------------------------------------------------


def test_convert_calls_validate_staging_tables(tmp_path: Path) -> None:
    """When a workflow already exists (from inspect), convert_plan calls validate_staging_tables."""
    converter = _make_converter(ConverterConfig())
    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP
    converter._workflow = mock_workflow  # Already set — simulates prior inspect call

    plan = ConversionPlan(
        dump_paths=("test.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(_whole_table_plan("SRC", "ORDERS"),),
    )

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=_fake_metadata("DMP_SRC", "ORDERS"),
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
        patch(
            "oracle_dmp_converter.core.executor.export_table",
            return_value=_fake_export(tmp_path, "src", "orders"),
        ),
        patch.object(converter, "validate_staging_tables") as mock_validate,
    ):
        converter.convert_plan(plan, tmp_path)

    mock_validate.assert_called_once()
    called_plans = mock_validate.call_args[0][0]
    assert len(called_plans) == 1
    assert called_plans[0].table == "ORDERS"


def test_convert_does_not_validate_when_no_prior_inspect(tmp_path: Path) -> None:
    """When no workflow exists yet (no prior inspect), convert_plan skips validation."""
    converter = _make_converter(ConverterConfig())
    # _workflow is None — simulate standalone convert (no prior inspect)
    assert converter._workflow is None  # type: ignore[attr-defined]

    mock_workflow = MagicMock()
    mock_workflow.dump_format = DumpFormat.DATAPUMP

    plan = ConversionPlan(
        dump_paths=("test.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(_whole_table_plan("SRC", "ORDERS"),),
    )

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch(
            "oracle_dmp_converter.core.executor.discover_table_metadata",
            return_value=_fake_metadata("DMP_SRC", "ORDERS"),
        ),
        patch("oracle_dmp_converter.core.executor.count_rows", return_value=5),
        patch(
            "oracle_dmp_converter.core.executor.export_table",
            return_value=_fake_export(tmp_path, "src", "orders"),
        ),
        patch.object(
            StagingExecutor,
            "use_format",
            lambda self, fmt: setattr(self, "_workflow", mock_workflow),
        ),
        patch.object(converter, "validate_staging_tables") as mock_validate,
    ):
        converter.convert_plan(plan, tmp_path)

    mock_validate.assert_not_called()


def test_validate_staging_tables_filters_out_missing(tmp_path: Path) -> None:
    """validate_staging_tables drops missing staging tables and logs a warning."""
    converter = _make_converter(ConverterConfig())
    plan_a = _whole_table_plan("SRC", "ORDERS")
    plan_b = _whole_table_plan("SRC", "INVOICES")

    _, mock_ctx = _mock_conn_ctx()

    # Only ORDERS exists in staging; INVOICES does not.
    def fake_exists(_conn: object, _schema: str, table: str) -> bool:
        return table == "ORDERS"

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.core.executor.table_exists",
            side_effect=fake_exists,
        ),
    ):
        result = converter.validate_staging_tables([plan_a, plan_b])

    assert [p.table for p in result] == ["ORDERS"]


def test_validate_staging_tables_passes_when_all_present(tmp_path: Path) -> None:
    """validate_staging_tables returns the input unchanged when all tables exist."""
    converter = _make_converter(ConverterConfig())
    plan = _whole_table_plan("SRC", "ORDERS")

    _, mock_ctx = _mock_conn_ctx()

    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.core.executor.table_exists", return_value=True),
    ):
        result = converter.validate_staging_tables([plan])
        assert result == [plan]
