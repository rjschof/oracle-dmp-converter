"""Unit tests for runtime/container_manager.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from oracle_dmp_converter.runtime.container_manager import (
    build_work_subdir_mounts,
    start_or_reconnect,
)


class TestBuildWorkSubdirMounts:
    def test_creates_subdirs_and_returns_mounts(self, tmp_path: Path) -> None:
        mounts = build_work_subdir_mounts(tmp_path)
        assert len(mounts) == 3
        for host_path, _container_path, mode in mounts:
            assert host_path.exists()
            assert mode == "rw"
        host_paths = {m[0].name for m in mounts}
        assert "discovery" in host_paths
        assert "inspect" in host_paths
        assert "convert" in host_paths


class TestStartOrReconnect:
    def _make_settings(self, tmp_path: Path) -> MagicMock:
        settings = MagicMock()
        settings.work_dir = tmp_path / "work"
        settings.dump_dir = tmp_path / "dumps"
        settings.oracle_image = "gvenzl/oracle-free:23-faststart"
        settings.oracle_password = "OraclePwd_123"
        settings.container_runtime = "docker"
        return settings

    def test_reconnects_when_session_exists(self, tmp_path: Path) -> None:
        settings = self._make_settings(tmp_path)
        settings.work_dir.mkdir(parents=True)
        mock_session = MagicMock()
        mock_session.container_name = "oracle-abc"
        mock_session.oracle_image = "gvenzl/oracle-free:23-faststart"
        mock_session.oracle_service = "FREEPDB1"
        mock_container = MagicMock()

        with (
            patch(
                "oracle_dmp_converter.runtime.container_manager.load_session_if_exists",
                return_value=mock_session,
            ),
            patch(
                "oracle_dmp_converter.runtime.container_manager.ContainerOracle.reconnect",
                return_value=mock_container,
            ) as mock_reconnect,
        ):
            result = start_or_reconnect(settings)

        assert result is mock_container
        mock_reconnect.assert_called_once()

    def test_starts_new_container_when_no_session(self, tmp_path: Path) -> None:
        settings = self._make_settings(tmp_path)
        (tmp_path / "dumps").mkdir(parents=True)
        mock_container = MagicMock()

        with (
            patch(
                "oracle_dmp_converter.runtime.container_manager.load_session_if_exists",
                return_value=None,
            ),
            patch(
                "oracle_dmp_converter.runtime.container_manager.ContainerOracle.start",
                return_value=mock_container,
            ) as mock_start,
        ):
            result = start_or_reconnect(settings)

        assert result is mock_container
        mock_start.assert_called_once()
