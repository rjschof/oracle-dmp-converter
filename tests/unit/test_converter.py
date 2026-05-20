"""Unit tests for OracleDumpConverter internals."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from oracle_dmp_converter.config import ColumnOverride, ConverterConfig
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    TableMetadata,
    TablePlan,
    TableStrategy,
)


def _make_converter(config: ConverterConfig) -> OracleDumpConverter:
    return OracleDumpConverter(
        container=MagicMock(),
        admin=OracleAdminConnection("localhost", 1521, "FREE", "system", "pwd"),
        work_dir=Path("/tmp/work"),
        dumpfiles=("test.dmp",),
        config=config,
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
        patch("oracle_dmp_converter.converter.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.converter.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.converter.count_rows", return_value=0),
        patch(
            "oracle_dmp_converter.converter.export_table", return_value=fake_export
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
        patch("oracle_dmp_converter.converter.oracle_connection", return_value=mock_ctx),
        patch(
            "oracle_dmp_converter.converter.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.converter.count_rows", return_value=0),
        patch(
            "oracle_dmp_converter.converter.export_table", return_value=fake_export
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
    """convert_plan() must skip UNSUPPORTED tables with a warning, not raise."""
    converter = _make_converter(ConverterConfig())

    unsupported_plan = TablePlan(
        schema="SRC",
        table="BROKEN",
        strategy=TableStrategy.UNSUPPORTED,
        reason="strategy 'range' is not supported",
    )
    good_plan = TablePlan(
        schema="SRC",
        table="SMALL",
        strategy=TableStrategy.WHOLE_TABLE,
        chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
    )
    plan = ConversionPlan(
        dump_paths=("test.dmp",),
        oracle_image="gvenzl/oracle-free:23-faststart",
        tables=(unsupported_plan, good_plan),
    )

    fake_export = MagicMock()
    fake_export.rows = 5
    fake_export.path = tmp_path / "SRC" / "SMALL" / "whole.parquet"

    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    fake_metadata = TableMetadata(
        schema="DMP_SRC",
        name="SMALL",
        columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
    )

    with (
        patch("oracle_dmp_converter.converter.oracle_connection", return_value=mock_ctx),
        patch("oracle_dmp_converter.converter.ensure_schema"),
        patch("oracle_dmp_converter.converter.drop_table"),
        patch(
            "oracle_dmp_converter.converter.discover_table_metadata",
            return_value=fake_metadata,
        ),
        patch("oracle_dmp_converter.converter.count_rows", return_value=5),
        patch("oracle_dmp_converter.converter.export_table", return_value=fake_export),
        patch.object(converter, "import_table_chunk"),
    ):
        result = converter.convert_plan(plan, tmp_path)

    # Only the good table appears in results; the UNSUPPORTED one is silently skipped.
    assert len(result.tables) == 1
    assert result.tables[0].table == "SMALL"
