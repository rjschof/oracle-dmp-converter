"""Container lifecycle for Oracle Database Free (Docker and Podman)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from stat import S_IMODE
from typing import Any

import docker
import oracledb
from docker.errors import APIError, DockerException, ImageNotFound, NotFound

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE
from oracle_dmp_converter.errors import (
    DockerContainerError,
    DockerError,
    DockerExecError,
    DockerImageError,
    DockerPortError,
    DockerReadinessError,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_CONTAINER_RUNTIME = "docker"


def docker_available(runtime: str = DEFAULT_CONTAINER_RUNTIME) -> bool:
    result = subprocess.run(
        [runtime, "version", "--format", "{{json .Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _podman_socket_url() -> str | None:
    """Return a ``unix://`` URL for the first running Podman machine, or ``None``."""
    try:
        result = subprocess.run(
            ["podman", "machine", "inspect"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        machines = json.loads(result.stdout)
        for machine in machines:
            if machine.get("State", "").lower() != "running":
                continue
            path = machine.get("ConnectionInfo", {}).get("PodmanSocket", {}).get("Path")
            if path and Path(path).exists():
                return f"unix://{path}"
    except (OSError, json.JSONDecodeError, KeyError, TypeError):
        pass
    return None


def _docker_client(runtime: str = DEFAULT_CONTAINER_RUNTIME) -> docker.DockerClient:
    try:
        if runtime == "podman" and not os.environ.get("DOCKER_HOST"):
            socket_url = _podman_socket_url()
            if socket_url:
                LOGGER.debug("Connecting to Podman socket at %s", socket_url)
                return docker.DockerClient(base_url=socket_url)
        return docker.from_env()
    except DockerException as exc:
        raise DockerError(str(exc)) from exc


def _run_docker_cp(
    host_path: Path,
    container_name: str,
    container_path: str,
    runtime: str = DEFAULT_CONTAINER_RUNTIME,
) -> None:
    result = subprocess.run(
        [runtime, "cp", str(host_path), f"{container_name}:{container_path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        msg = (
            result.stderr.strip()
            or result.stdout.strip()
            or f"{runtime} cp failed: {host_path} -> {container_name}:{container_path}"
        )
        raise DockerExecError(msg)


def _ensure_mount_path_permissions(host_path: Path, mode: str) -> Path:
    resolved = host_path.resolve()
    if not resolved.exists():
        raise DockerContainerError(f"Mount path does not exist: {resolved}")

    try:
        current_mode = S_IMODE(resolved.stat().st_mode)
    except OSError as exc:
        raise DockerContainerError(f"Unable to inspect mount path permissions: {resolved}") from exc

    if resolved.is_dir():
        required_mode = 0o005
        if "w" in mode:
            required_mode |= 0o002
    else:
        required_mode = 0o004
        if "w" in mode:
            required_mode |= 0o002

    target_mode = current_mode | required_mode
    if target_mode != current_mode:
        try:
            resolved.chmod(target_mode)
        except OSError as exc:
            raise DockerContainerError(
                f"Mount path is not accessible for Oracle container user: {resolved}"
            ) from exc
    return resolved


@dataclass
class DockerOracle:
    image: str = DEFAULT_ORACLE_IMAGE
    password: str = "OraclePwd_123"
    service: str = "FREEPDB1"
    name: str = field(default_factory=lambda: f"oracle-dmp-converter-{uuid.uuid4().hex[:12]}")
    platform: str | None = None
    mounts: tuple[tuple[Path, str, str], ...] = ()
    runtime: str = DEFAULT_CONTAINER_RUNTIME
    started: bool = False
    _client: docker.DockerClient | None = field(default=None, init=False, repr=False)
    _container: Any | None = field(default=None, init=False, repr=False)

    @classmethod
    def start(
        cls,
        *,
        image: str | None = None,
        password: str = "OraclePwd_123",
        service: str = "FREEPDB1",
        name: str | None = None,
        platform: str | None = None,
        mounts: tuple[tuple[Path, str, str], ...] = (),
        runtime: str | None = None,
    ) -> DockerOracle:
        container = cls(
            image=image or DEFAULT_ORACLE_IMAGE,
            password=password,
            service=service,
            name=name or f"oracle-dmp-converter-{uuid.uuid4().hex[:12]}",
            platform=platform or os.environ.get("ORACLE_DMP_CONVERTER_DOCKER_PLATFORM"),
            mounts=mounts,
            runtime=runtime
            or os.environ.get("ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME),
        )
        container._start_container()
        return container

    def _start_container(self) -> None:
        volumes: dict[str, dict[str, str]] = {}
        for host_path, container_path, mode in self.mounts:
            prepared = _ensure_mount_path_permissions(host_path, mode)
            volumes[str(prepared)] = {"bind": container_path, "mode": mode}

        ports = {"1521/tcp": ("127.0.0.1", None)}
        self._client = _docker_client(self.runtime)
        try:
            self._container = self._client.containers.run(
                self.image,
                detach=True,
                remove=True,
                name=self.name,
                shm_size="1g",
                environment={"ORACLE_PASSWORD": self.password},
                ports=ports,
                platform=self.platform,
                volumes=volumes or None,
            )
        except ImageNotFound as exc:
            raise DockerImageError(f"Docker image not found: {self.image}") from exc
        except APIError as exc:
            message = str(exc)
            if "pull access denied" in message.lower() or "not found" in message.lower():
                raise DockerImageError(message) from exc
            raise DockerContainerError(message) from exc
        except DockerException as exc:
            raise DockerContainerError(str(exc)) from exc
        self.started = True

    def _require_container(self) -> Any:
        if self._container is None:
            raise DockerContainerError("Oracle container is not started")
        return self._container

    def mapped_port(self) -> int:
        container = self._require_container()
        try:
            container.reload()
            ports = container.attrs.get("NetworkSettings", {}).get("Ports", {})
            mappings = ports.get("1521/tcp")
            if not mappings:
                raise DockerPortError("Oracle container does not expose port 1521")
            return int(mappings[0]["HostPort"])
        except (APIError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, DockerError):
                raise
            raise DockerPortError(str(exc)) from exc

    def logs(self) -> str:
        container = self._require_container()
        try:
            output = container.logs(stdout=True, stderr=True)
        except (APIError, DockerException) as exc:
            raise DockerContainerError(str(exc)) from exc
        if isinstance(output, bytes):
            return output.decode("utf-8", errors="replace")
        return str(output)

    def exec(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        # Use subprocess rather than the Docker SDK's exec_run: exec_run blocks
        # indefinitely when the container stops mid-exec because the HTTP chunked
        # stream never sends EOF. The docker/podman exec CLI exits cleanly in that scenario.
        result = subprocess.run(
            [self.runtime, "exec", self.name, *args],
            capture_output=True,
            text=True,
            check=False,
        )
        if check and result.returncode != 0:
            msg = (
                result.stderr.strip()
                or result.stdout.strip()
                or f"{self.runtime} exec failed: {args}"
            )
            raise DockerExecError(msg)
        return result

    def copy_to(self, host_path: Path, container_path: str) -> None:
        _run_docker_cp(host_path, self.name, container_path, self.runtime)

    def wait_ready(self, timeout_seconds: int = 600) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        container = self._require_container()
        while time.monotonic() < deadline:
            try:
                container.reload()
                state = container.attrs.get("State", {})
                if not state.get("Running"):
                    raise DockerReadinessError("Oracle container disappeared")
                conn = oracledb.connect(
                    user="system",
                    password=self.password,
                    dsn=f"localhost:{self.mapped_port()}/{self.service}",
                )
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("select 1 from dual")
                        cursor.fetchone()
                    return
                finally:
                    conn.close()
            except DockerReadinessError:
                raise
            except Exception as exc:  # noqa: BLE001 - readiness polling preserves the last error.
                last_error = exc
                time.sleep(5)
        logs = self.logs()[-4000:]
        msg = f"Oracle did not become ready within {timeout_seconds}s"
        if last_error:
            msg += f"; last connection error: {last_error}"
        msg += f"\nContainer logs:\n{logs}"
        raise DockerReadinessError(msg)

    def stop(self) -> None:
        if self.started:
            try:
                container = self._require_container()
                container.stop(timeout=30)
            except NotFound:
                pass
            except (APIError, DockerException):
                try:
                    container = self._require_container()
                    container.kill()
                except (NotFound, APIError, DockerException):
                    pass
            self.started = False
            self._container = None
            if self._client is not None:
                self._client.close()
                self._client = None

    def __enter__(self) -> DockerOracle:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
