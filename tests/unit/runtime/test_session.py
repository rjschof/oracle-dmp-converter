"""Unit tests for runtime/session.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from oracle_dmp_converter.models import ContainerSession
from oracle_dmp_converter.persistence.serialization import save_session
from oracle_dmp_converter.runtime.session import (
    SESSION_FILENAME,
    cleanup_stale_session,
    load_session_if_exists,
    session_path_for,
    write_session,
)


def _sample_session() -> ContainerSession:
    return ContainerSession(
        container_name="oracle-dmp-converter-abc123",
        container_runtime="docker",
        oracle_image="gvenzl/oracle-free:23-faststart",
        oracle_service="FREEPDB1",
        work_dir="/tmp/work",
        dump_dir="/tmp/dumps",
        created_at="2024-01-01T12:00:00+00:00",
    )


class TestSessionPathFor:
    def test_returns_session_json_inside_work_dir(self, tmp_path: Path) -> None:
        result = session_path_for(tmp_path)
        assert result == tmp_path / SESSION_FILENAME


class TestLoadSessionIfExists:
    def test_returns_none_when_file_absent(self, tmp_path: Path) -> None:
        path = tmp_path / "session.json"
        assert load_session_if_exists(path) is None

    def test_loads_session_when_file_present(self, tmp_path: Path) -> None:
        path = tmp_path / "session.json"
        session = _sample_session()
        save_session(path, session)
        loaded = load_session_if_exists(path)
        assert loaded is not None
        assert loaded.container_name == session.container_name
        assert loaded.oracle_image == session.oracle_image


class TestCleanupStaleSession:
    def test_stops_container_and_deletes_file(self, tmp_path: Path) -> None:
        path = tmp_path / "session.json"
        session = _sample_session()
        save_session(path, session)

        mock_container = MagicMock()
        with patch(
            "oracle_dmp_converter.runtime.session.ContainerOracle.reconnect",
            return_value=mock_container,
        ):
            cleanup_stale_session(path)

        mock_container.stop.assert_called_once()
        assert not path.exists()

    def test_deletes_file_even_when_reconnect_fails(self, tmp_path: Path) -> None:
        path = tmp_path / "session.json"
        session = _sample_session()
        save_session(path, session)

        with patch(
            "oracle_dmp_converter.runtime.session.ContainerOracle.reconnect",
            side_effect=Exception("container gone"),
        ):
            cleanup_stale_session(path)

        assert not path.exists()

    def test_suppresses_oserror_on_unlink(self, tmp_path: Path) -> None:
        """cleanup_stale_session does not raise even when path.unlink() fails."""
        path = tmp_path / "session.json"
        session = _sample_session()
        save_session(path, session)

        with (
            patch(
                "oracle_dmp_converter.runtime.session.ContainerOracle.reconnect",
                side_effect=Exception("container gone"),
            ),
            patch.object(Path, "unlink", side_effect=OSError("permission denied")),
        ):
            cleanup_stale_session(path)  # must not raise


class TestWriteSession:
    def test_writes_session_file(self, tmp_path: Path) -> None:
        path = tmp_path / "session.json"
        mock_container = MagicMock()
        mock_container.name = "oracle-dmp-converter-abc"
        mock_container.service = "FREEPDB1"

        write_session(
            path,
            container=mock_container,
            container_runtime="docker",
            oracle_image="gvenzl/oracle-free:23-faststart",
            work_dir=tmp_path / "work",
            dump_dir=tmp_path / "dumps",
        )

        assert path.exists()
        loaded = load_session_if_exists(path)
        assert loaded is not None
        assert loaded.container_name == "oracle-dmp-converter-abc"
        assert loaded.container_runtime == "docker"
