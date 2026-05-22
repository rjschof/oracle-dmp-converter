"""Unit tests for the :class:`OracleDMPConverter` facade and :class:`ConverterSettings`."""
# pylint: disable=protected-access,missing-function-docstring,no-value-for-parameter

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_dmp_converter import ConverterSettings, OracleDMPConverter
from oracle_dmp_converter.config import ConverterConfig
from oracle_dmp_converter.models import (
    ColumnMetadata,
    DumpFormat,
    DumpManifest,
    TableMetadata,
)

# ---------------------------------------------------------------------------
# ConverterSettings
# ---------------------------------------------------------------------------


def _settings(tmp_path: Path, **overrides) -> ConverterSettings:
    dump = tmp_path / "dumps" / "a.dmp"
    dump.parent.mkdir(parents=True, exist_ok=True)
    dump.touch()
    defaults = {
        "dump_paths": (dump,),
        "oracle_password": "pw",
        "work_dir": tmp_path / "work",
    }
    defaults.update(overrides)
    return ConverterSettings(**defaults)


def test_settings_rejects_empty_dump_paths() -> None:
    with pytest.raises(ValueError, match="dump_paths must not be empty"):
        ConverterSettings(dump_paths=(), oracle_password="pw")


def test_settings_rejects_dump_paths_from_different_dirs(tmp_path: Path) -> None:
    d1 = tmp_path / "a"
    d2 = tmp_path / "b"
    d1.mkdir()
    d2.mkdir()
    (d1 / "x.dmp").touch()
    (d2 / "y.dmp").touch()
    with pytest.raises(ValueError, match="same parent|single parent"):
        ConverterSettings(
            dump_paths=(d1 / "x.dmp", d2 / "y.dmp"),
            oracle_password="pw",
        )


def test_settings_oracle_password_is_required() -> None:
    """Missing oracle_password raises TypeError at construction (no default)."""
    with pytest.raises(TypeError):
        ConverterSettings(dump_paths=(Path("x"),))  # type: ignore[call-arg]


def test_settings_oracle_image_reads_env_at_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Proves default_factory semantics — env is read when instance is built."""
    monkeypatch.setenv("ORACLE_DMP_CONVERTER_IMAGE", "envimage:latest")
    s = _settings(tmp_path)
    assert s.oracle_image == "envimage:latest"
    monkeypatch.setenv("ORACLE_DMP_CONVERTER_IMAGE", "second:latest")
    s2 = _settings(tmp_path)
    assert s2.oracle_image == "second:latest"


def test_settings_container_runtime_reads_env_at_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME", "podman")
    s = _settings(tmp_path)
    assert s.container_runtime == "podman"


def test_settings_dump_helpers(tmp_path: Path) -> None:
    d = tmp_path / "dumps"
    d.mkdir()
    (d / "a.dmp").touch()
    (d / "b.dmp").touch()
    s = ConverterSettings(
        dump_paths=(d / "a.dmp", d / "b.dmp"),
        oracle_password="pw",
    )
    assert s.dump_dir == d.resolve()
    assert s.dump_filenames == ("a.dmp", "b.dmp")


# ---------------------------------------------------------------------------
# OracleDMPConverter facade
# ---------------------------------------------------------------------------


def _patch_container_seam():
    """Patch the single mock seam used during start()."""
    mock_container = MagicMock()
    mock_container.name = "oracle-test"
    mock_container.service = "FREEPDB1"
    mock_container.mapped_port.return_value = 1521
    return patch(
        "oracle_dmp_converter.runtime.container_manager.start_or_reconnect",
        return_value=mock_container,
    ), mock_container


def test_convert_requires_output_dir(tmp_path: Path) -> None:
    settings = _settings(tmp_path)  # no output_dir
    converter = OracleDMPConverter(settings)
    converter._container = MagicMock()  # bypass start()
    converter._executor = MagicMock()
    plan = MagicMock()
    with pytest.raises(ValueError, match="output_dir is required"):
        converter.convert(plan)


def test_context_manager_uses_single_seam(tmp_path: Path) -> None:
    """OracleDMPConverter works as a context manager when start_or_reconnect is patched."""
    settings = _settings(tmp_path)
    seam, mock_container = _patch_container_seam()

    with (
        seam,
        patch("oracle_dmp_converter.converter.admin_for_container") as mock_admin,
        patch("oracle_dmp_converter.converter.create_dump_directory"),
        patch("oracle_dmp_converter.converter.create_work_dir_directories"),
    ):
        mock_admin.return_value = MagicMock()
        with OracleDMPConverter(settings) as converter:
            assert converter.container is mock_container
            assert converter.executor is not None
        mock_container.wait_ready.assert_called_once()
        # Without keep_alive, stop() should have called container.stop()
        mock_container.stop.assert_called_once()


def test_inspect_writes_manifest_json(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    converter = OracleDMPConverter(settings)

    fake_manifest = DumpManifest(
        dump_paths=("ignored",),
        tables=(
            TableMetadata(
                schema="APP",
                name="ORDERS",
                columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
            ),
        ),
        dump_format=DumpFormat.DATAPUMP,
    )
    executor = MagicMock()
    executor.inspect_dump.return_value = fake_manifest
    converter._executor = executor
    converter._container = MagicMock()

    result = converter.inspect()

    manifest_path = settings.work_dir / "manifest.json"
    assert manifest_path.exists()
    # Manifest is re-stamped with settings' dump paths + image + runtime.
    assert result.oracle_image == settings.oracle_image
    assert result.container_runtime == settings.container_runtime


def test_plan_writes_plan_yaml(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    converter = OracleDMPConverter(settings)
    manifest = DumpManifest(
        dump_paths=("/dumps/a.dmp",),
        tables=(
            TableMetadata(
                schema="APP",
                name="ORDERS",
                columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
            ),
        ),
        dump_format=DumpFormat.DATAPUMP,
    )

    plan = converter.plan(manifest)

    plan_path = settings.work_dir / "plan.yaml"
    assert plan_path.exists()
    assert plan.dump_format == DumpFormat.DATAPUMP
    assert plan.oracle_image  # populated from manifest/config/default cascade


def test_plan_image_mismatch_raises_value_error(tmp_path: Path) -> None:
    settings = _settings(tmp_path, config=ConverterConfig(oracle_image="cfg:img"))
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    converter = OracleDMPConverter(settings)
    manifest = DumpManifest(
        dump_paths=("/dumps/a.dmp",),
        tables=(),
        dump_format=DumpFormat.DATAPUMP,
        oracle_image="manifest:img",
    )
    with pytest.raises(ValueError, match="oracle_image mismatch"):
        converter.plan(manifest)


def test_plan_image_mismatch_resolved_by_override(tmp_path: Path) -> None:
    settings = _settings(tmp_path, config=ConverterConfig(oracle_image="cfg:img"))
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    converter = OracleDMPConverter(settings)
    manifest = DumpManifest(
        dump_paths=("/dumps/a.dmp",),
        tables=(),
        dump_format=DumpFormat.DATAPUMP,
        oracle_image="manifest:img",
    )
    plan = converter.plan(manifest, oracle_image="override:img")
    assert plan.oracle_image == "override:img"


def test_facade_has_all_advertised_methods() -> None:
    """Mirrors the snippet in the plan's acceptance criteria."""
    assert hasattr(OracleDMPConverter, "__enter__")
    for name in ("start", "stop", "inspect", "plan", "convert", "run"):
        assert hasattr(OracleDMPConverter, name), name


def test_load_manifest_round_trip(tmp_path: Path) -> None:
    """save_manifest + load_manifest round-trips a manifest through the facade."""
    settings = _settings(tmp_path)
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    converter = OracleDMPConverter(settings)

    original = DumpManifest(
        dump_paths=("/dumps/a.dmp",),
        tables=(
            TableMetadata(
                schema="APP",
                name="ORDERS",
                columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
            ),
        ),
        dump_format=DumpFormat.DATAPUMP,
    )
    target = converter.save_manifest(original)
    loaded = OracleDMPConverter.load_manifest(target)
    assert loaded.dump_format == DumpFormat.DATAPUMP
    assert loaded.tables[0].name == "ORDERS"
