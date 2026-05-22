"""Unit tests for ContainerOracle.reconnect()."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from docker.errors import NotFound

from oracle_dmp_converter.errors import DockerContainerError
from oracle_dmp_converter.runtime.container_oracle import DEFAULT_CONTAINER_RUNTIME, ContainerOracle


def _make_mock_container(
    *,
    running: bool = True,
    status: str = "running",
    oracle_password: str = "Secret_99",
    extra_env: list[str] | None = None,
) -> MagicMock:
    """Build a minimal mock Docker container object."""
    env = [f"ORACLE_PASSWORD={oracle_password}", "PATH=/usr/bin"] + (extra_env or [])
    container = MagicMock()
    container.attrs = {
        "State": {"Running": running, "Status": status},
        "Config": {"Env": env},
    }
    container.reload.return_value = None
    return container


def _make_mock_client(container: MagicMock) -> MagicMock:
    client = MagicMock()
    client.containers.get.return_value = container
    return client


class TestReconnectSuccess:
    def test_returns_docker_oracle_instance(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(
                name="oracle-dmp-converter-abc123",
                image="gvenzl/oracle-free:23-faststart",
                service="FREEPDB1",
                runtime="docker",
            )

        assert isinstance(result, ContainerOracle)

    def test_started_is_true(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(
                name="oracle-dmp-converter-abc123",
                image="gvenzl/oracle-free:23-faststart",
            )

        assert result.started is True

    def test_name_matches(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        assert result.name == "oracle-dmp-converter-abc123"

    def test_password_read_from_container_env(self) -> None:
        mock_container = _make_mock_container(oracle_password="TopSecret_42")
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        assert result.password == "TopSecret_42"

    def test_password_empty_when_env_missing(self) -> None:
        """If ORACLE_PASSWORD is absent from the container env, password is empty string."""
        container = MagicMock()
        container.attrs = {
            "State": {"Running": True, "Status": "running"},
            "Config": {"Env": ["PATH=/usr/bin", "HOME=/root"]},
        }
        container.reload.return_value = None
        mock_client = _make_mock_client(container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        assert result.password == ""

    def test_password_empty_when_env_is_none(self) -> None:
        """Container with Config.Env = None does not raise."""
        container = MagicMock()
        container.attrs = {
            "State": {"Running": True, "Status": "running"},
            "Config": {"Env": None},
        }
        container.reload.return_value = None
        mock_client = _make_mock_client(container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        assert result.password == ""

    def test_image_stored_on_instance(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(
                name="oracle-dmp-converter-abc123",
                image="gvenzl/oracle-free:21-faststart",
            )

        assert result.image == "gvenzl/oracle-free:21-faststart"

    def test_service_stored_on_instance(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            result = ContainerOracle.reconnect(
                name="oracle-dmp-converter-abc123",
                service="XEPDB1",
            )

        assert result.service == "XEPDB1"

    def test_runtime_defaults_to_docker(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ) as mock_dc:
            ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        mock_dc.assert_called_once_with(DEFAULT_CONTAINER_RUNTIME)

    def test_runtime_explicit_podman(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ) as mock_dc:
            result = ContainerOracle.reconnect(
                name="oracle-dmp-converter-abc123",
                runtime="podman",
            )

        mock_dc.assert_called_once_with("podman")
        assert result.runtime == "podman"

    def test_client_looks_up_container_by_name(self) -> None:
        mock_container = _make_mock_container()
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            ContainerOracle.reconnect(name="oracle-dmp-converter-abc123")

        mock_client.containers.get.assert_called_once_with("oracle-dmp-converter-abc123")


class TestReconnectFailures:
    def test_raises_when_container_not_found(self) -> None:
        mock_client = MagicMock()
        mock_client.containers.get.side_effect = NotFound("not found")

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            with pytest.raises(DockerContainerError, match="not found"):
                ContainerOracle.reconnect(name="oracle-dmp-converter-gone")

    def test_raises_when_container_not_running(self) -> None:
        mock_container = _make_mock_container(running=False, status="exited")
        mock_client = _make_mock_client(mock_container)

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            with pytest.raises(DockerContainerError, match="not running"):
                ContainerOracle.reconnect(name="oracle-dmp-converter-stopped")

    def test_error_message_includes_container_name(self) -> None:
        mock_client = MagicMock()
        mock_client.containers.get.side_effect = NotFound("not found")

        with patch(
            "oracle_dmp_converter.runtime.container_oracle._docker_client", return_value=mock_client
        ):
            with pytest.raises(DockerContainerError, match="oracle-dmp-converter-gone"):
                ContainerOracle.reconnect(name="oracle-dmp-converter-gone")
