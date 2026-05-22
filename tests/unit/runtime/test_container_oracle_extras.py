"""Additional unit tests for ContainerOracle methods not covered by reconnect tests."""
# pylint: disable=protected-access

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from docker.errors import APIError, DockerException, ImageNotFound, NotFound

from oracle_dmp_converter.errors import (
    DockerContainerError,
    DockerError,
    DockerExecError,
    DockerImageError,
    DockerPortError,
)
from oracle_dmp_converter.runtime.container_oracle import (
    ContainerOracle,
    _docker_client,
    _ensure_mount_path_permissions,
    _podman_socket_url,
    _run_docker_cp,
    docker_available,
)

# ---------------------------------------------------------------------------
# _podman_socket_url
# ---------------------------------------------------------------------------


class TestPodmanSocketUrl:
    def test_returns_none_when_podman_fails(self) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            assert _podman_socket_url() is None

    def test_returns_none_when_no_running_machine(self) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            '[{"State": "stopped", "ConnectionInfo": {"PodmanSocket": {"Path": "/tmp/sock"}}}]'
        )
        with patch("subprocess.run", return_value=mock_result):
            assert _podman_socket_url() is None

    def test_returns_socket_url_when_machine_running(self, tmp_path: Path) -> None:
        sock = tmp_path / "podman.sock"
        sock.write_bytes(b"")
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            f'[{{"State": "running", "ConnectionInfo": {{"PodmanSocket": {{"Path": "{sock}"}}}}}}]'
        )
        with patch("subprocess.run", return_value=mock_result):
            url = _podman_socket_url()
        assert url is not None
        assert url.startswith("unix://")


# ---------------------------------------------------------------------------
# _docker_client
# ---------------------------------------------------------------------------


class TestDockerClientFactory:
    def test_returns_client_for_docker(self) -> None:
        mock_client = MagicMock()
        with patch("docker.from_env", return_value=mock_client):
            client = _docker_client("docker")
        assert client is mock_client

    def test_raises_docker_error_when_daemon_unreachable(self) -> None:
        with patch("docker.from_env", side_effect=DockerException("no daemon")):
            with pytest.raises(DockerError):
                _docker_client("docker")


# ---------------------------------------------------------------------------
# _ensure_mount_path_permissions
# ---------------------------------------------------------------------------


class TestEnsureMountPathPermissions:
    def test_returns_resolved_path(self, tmp_path: Path) -> None:
        resolved = _ensure_mount_path_permissions(tmp_path, "ro")
        assert resolved == tmp_path.resolve()

    def test_raises_when_path_does_not_exist(self, tmp_path: Path) -> None:
        with pytest.raises(DockerContainerError, match="does not exist"):
            _ensure_mount_path_permissions(tmp_path / "nonexistent", "rw")


# ---------------------------------------------------------------------------
# docker_available
# ---------------------------------------------------------------------------


class TestDockerAvailable:
    def test_returns_true_on_zero_exit(self) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch("subprocess.run", return_value=mock_result):
            assert docker_available("docker") is True

    def test_returns_false_on_nonzero_exit(self) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 1
        with patch("subprocess.run", return_value=mock_result):
            assert docker_available("docker") is False


# ---------------------------------------------------------------------------
# _run_docker_cp
# ---------------------------------------------------------------------------


class TestRunDockerCp:
    def test_raises_on_nonzero_exit(self, tmp_path: Path) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "no such file"
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(DockerExecError, match="no such file"):
                _run_docker_cp(tmp_path / "f.par", "container-name", "/tmp/f.par")

    def test_succeeds_on_zero_exit(self, tmp_path: Path) -> None:
        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch("subprocess.run", return_value=mock_result):
            _run_docker_cp(tmp_path / "f.par", "container-name", "/tmp/f.par")  # no exception


# ---------------------------------------------------------------------------
# ContainerOracle._require_container
# ---------------------------------------------------------------------------


class TestRequireContainer:
    def test_raises_when_not_started(self) -> None:
        co = ContainerOracle()
        with pytest.raises(DockerContainerError, match="not been started"):
            co._require_container()

    def test_returns_container_when_started(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        co._container = mock_container
        assert co._require_container() is mock_container


# ---------------------------------------------------------------------------
# ContainerOracle.exec
# ---------------------------------------------------------------------------


class TestContainerOracleExec:
    def _make_container(self) -> ContainerOracle:
        co = ContainerOracle(name="test-container", runtime="docker")
        co._container = MagicMock()
        return co

    def test_returns_completed_process_on_success(self) -> None:
        co = self._make_container()
        mock_result = subprocess.CompletedProcess(
            args=["docker", "exec", "test-container", "ls"],
            returncode=0,
            stdout="file.txt\n",
            stderr="",
        )
        with patch("subprocess.run", return_value=mock_result):
            result = co.exec(["ls"], check=False)
        assert result.returncode == 0
        assert result.stdout == "file.txt\n"

    def test_raises_on_nonzero_with_check(self) -> None:
        co = self._make_container()
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="command not found"
        )
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(DockerExecError, match="command not found"):
                co.exec(["bad-cmd"], check=True)

    def test_no_raise_on_nonzero_without_check(self) -> None:
        co = self._make_container()
        mock_result = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="err")
        with patch("subprocess.run", return_value=mock_result):
            result = co.exec(["bad"], check=False)
        assert result.returncode == 1


# ---------------------------------------------------------------------------
# ContainerOracle.copy_to
# ---------------------------------------------------------------------------


class TestContainerOracleCopyTo:
    def test_delegates_to_run_docker_cp(self, tmp_path: Path) -> None:
        co = ContainerOracle(name="mycontainer", runtime="docker")
        co._container = MagicMock()
        with patch("oracle_dmp_converter.runtime.container_oracle._run_docker_cp") as mock_cp:
            co.copy_to(tmp_path / "file.par", "/tmp/file.par")
        mock_cp.assert_called_once_with(
            tmp_path / "file.par", "mycontainer", "/tmp/file.par", "docker"
        )


# ---------------------------------------------------------------------------
# ContainerOracle.mapped_port
# ---------------------------------------------------------------------------


class TestMappedPort:
    def test_returns_mapped_port(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.attrs = {"NetworkSettings": {"Ports": {"1521/tcp": [{"HostPort": "51521"}]}}}
        co._container = mock_container
        assert co.mapped_port() == 51521

    def test_raises_when_no_mapping(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.attrs = {"NetworkSettings": {"Ports": {}}}
        co._container = mock_container
        with pytest.raises(DockerPortError):
            co.mapped_port()


# ---------------------------------------------------------------------------
# ContainerOracle.logs
# ---------------------------------------------------------------------------


class TestContainerOracleLogs:
    def test_returns_decoded_logs(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.logs.return_value = b"Starting Oracle...\n"
        co._container = mock_container
        assert "Starting Oracle" in co.logs()

    def test_returns_string_logs_directly(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.logs.return_value = "already a string"
        co._container = mock_container
        assert co.logs() == "already a string"


# ---------------------------------------------------------------------------
# ContainerOracle.stop
# ---------------------------------------------------------------------------


class TestContainerOracleStop:
    def test_stops_and_clears_state(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_client = MagicMock()
        co._container = mock_container
        co._client = mock_client
        co.started = True

        co.stop()

        mock_container.stop.assert_called_once_with(timeout=30)
        mock_client.close.assert_called_once()
        assert co.started is False
        assert co._container is None
        assert co._client is None

    def test_stop_is_noop_when_not_started(self) -> None:
        co = ContainerOracle()
        co.started = False
        co.stop()  # must not raise

    def test_stop_tolerates_not_found(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.stop.side_effect = NotFound("gone")
        co._container = mock_container
        co._client = MagicMock()
        co.started = True
        co.stop()  # must not raise
        assert co.started is False

    def test_stop_kills_on_api_error(self) -> None:
        co = ContainerOracle()
        mock_container = MagicMock()
        mock_container.stop.side_effect = APIError("timeout")
        co._container = mock_container
        co._client = MagicMock()
        co.started = True
        co.stop()  # must not raise; fallback to kill
        mock_container.kill.assert_called_once()


# ---------------------------------------------------------------------------
# ContainerOracle context manager
# ---------------------------------------------------------------------------


class TestContainerOracleContextManager:
    def test_enter_returns_self(self) -> None:
        co = ContainerOracle()
        with co as ctx:
            assert ctx is co

    def test_exit_calls_stop(self) -> None:
        co = ContainerOracle()
        co.started = False
        co.__exit__(None, None, None)  # stop is safe when not started


# ---------------------------------------------------------------------------
# ContainerOracle.start
# ---------------------------------------------------------------------------


class TestContainerOracleStart:
    def test_start_returns_instance_with_started_true(self, tmp_path: Path) -> None:
        mock_docker_container = MagicMock()
        mock_client = MagicMock()
        mock_client.containers.run.return_value = mock_docker_container

        with (
            patch(
                "oracle_dmp_converter.runtime.container_oracle._docker_client",
                return_value=mock_client,
            ),
            patch(
                "oracle_dmp_converter.runtime.container_oracle._ensure_mount_path_permissions",
                side_effect=lambda path, mode: path.resolve(),
            ),
        ):
            co = ContainerOracle.start(
                image="gvenzl/oracle-free:23-faststart",
                password="pw",
                mounts=((tmp_path, "/dumps", "rw"),),
            )

        assert co.started is True
        assert co._container is mock_docker_container

    def test_start_raises_image_error_on_image_not_found(self) -> None:
        mock_client = MagicMock()
        mock_client.containers.run.side_effect = ImageNotFound("no image")

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client",
            return_value=mock_client,
        ):
            with pytest.raises(DockerImageError):
                ContainerOracle.start(image="nonexistent:latest", password="pw")

    def test_start_raises_image_error_on_pull_denied(self) -> None:
        mock_client = MagicMock()
        mock_client.containers.run.side_effect = APIError("pull access denied")

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client",
            return_value=mock_client,
        ):
            with pytest.raises(DockerImageError):
                ContainerOracle.start(image="private:latest", password="pw")

    def test_start_raises_container_error_on_other_api_error(self) -> None:
        mock_client = MagicMock()
        mock_client.containers.run.side_effect = APIError("some other error")

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client",
            return_value=mock_client,
        ):
            with pytest.raises(DockerContainerError):
                ContainerOracle.start(image="ok:latest", password="pw")
