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
    """Check whether a container runtime CLI is available and responding.

    Args:
        runtime: Container runtime executable name (e.g. ``"docker"`` or
            ``"podman"``).  For rootless Podman there is no separate daemon
            process, so the check uses ``version`` without a format string to
            avoid Go-template fields that only exist in Docker.

    Returns:
        ``True`` if the runtime's ``version`` command exits with status 0,
        ``False`` otherwise.
    """
    result = subprocess.run(
        [runtime, "version"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _podman_socket_url() -> str | None:
    """Return a ``unix://`` URL for a running Podman socket, or ``None``.

    Probes candidate locations in priority order:

    1. **Linux rootless** — ``$XDG_RUNTIME_DIR/podman/podman.sock`` (set by
       ``podman system service`` on Linux when running as a non-root user).
    2. **Explicit temp socket** — ``/tmp/podman.sock`` (common when the service
       is started with ``podman system service unix:///tmp/podman.sock``).
    3. **Podman Machine** (macOS / Windows) — ``podman machine inspect`` output.
    """
    # 1. Linux rootless socket via XDG_RUNTIME_DIR
    xdg_runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    for candidate in (
        Path(xdg_runtime) / "podman" / "podman.sock",
        Path("/tmp/podman.sock"),
    ):
        if candidate.exists():
            return f"unix://{candidate}"

    # 2. Podman Machine (macOS / Windows)
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
    """Create a :class:`docker.DockerClient`, handling Podman socket discovery.

    For Podman runtimes without ``DOCKER_HOST`` set the function attempts to
    auto-discover the running Podman machine socket via
    :func:`_podman_socket_url`.

    Args:
        runtime: Container runtime name (``"docker"`` or ``"podman"``).

    Returns:
        A connected :class:`docker.DockerClient`.

    Raises:
        :class:`~oracle_dmp_converter.errors.DockerError`: If the Docker/Podman
            daemon cannot be reached.
    """
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
    """Copy a file from the host into a running container via ``docker cp``.

    Args:
        host_path: Absolute path of the file on the host.
        container_name: Name or ID of the target container.
        container_path: Destination path inside the container.
        runtime: Container runtime CLI name.

    Raises:
        :class:`~oracle_dmp_converter.errors.DockerExecError`: If the copy
            command exits with a non-zero status.
    """
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
    """Verify and widen permissions on a host path before mounting into a container.

    Oracle's container user needs read access (and write access for ``"rw"``
    mounts).  If the current permissions are insufficient, the function
    attempts to ``chmod`` the path to add the missing bits.

    Args:
        host_path: The host-side path to check and possibly fix.
        mode: Mount mode string; ``"rw"`` requires write permission in
            addition to read.

    Returns:
        The resolved absolute :class:`~pathlib.Path`.

    Raises:
        :class:`~oracle_dmp_converter.errors.DockerContainerError`: If the
            path does not exist or permissions cannot be adjusted.
    """
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
class ContainerOracle:
    """Managed Oracle Database Free container.

    Wraps the Docker/Podman SDK to start, probe, and stop a
    ``gvenzl/oracle-free`` container (or any compatible image) and exposes
    helpers for executing commands and copying files.

    Use the :meth:`start` class method as the primary constructor.  The
    instance may be used as a context manager; :meth:`stop` is called
    automatically on exit.

    Attributes:
        image: Docker image tag for the Oracle container.
        password: ``ORACLE_PASSWORD`` value set at container startup.
        service: Oracle PDB service name (default ``"FREEPDB1"``).
        name: Container name; auto-generated if not supplied.
        platform: Docker ``--platform`` override (e.g. ``"linux/amd64"``).
            Read from ``DMP_CONVERTER_DOCKER_PLATFORM`` if unset.
        mounts: Sequence of ``(host_path, container_path, mode)`` triples
            describing bind mounts added at startup.
        runtime: Container runtime CLI (``"docker"`` or ``"podman"``).
        userns_mode: User-namespace mode passed to the container runtime
            (e.g. ``"keep-id"`` for rootless Podman).  ``None`` disables the
            option.
        started: ``True`` once :meth:`_start_container` has succeeded.
    """

    image: str = DEFAULT_ORACLE_IMAGE
    password: str = "OraclePwd_123"
    service: str = "FREEPDB1"
    name: str = field(default_factory=lambda: f"oracle-dmp-converter-{uuid.uuid4().hex[:12]}")
    platform: str | None = None
    mounts: tuple[tuple[Path, str, str], ...] = ()
    runtime: str = DEFAULT_CONTAINER_RUNTIME
    userns_mode: str | None = None
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
        userns_mode: str | None = None,
    ) -> ContainerOracle:
        """Create and start a new Oracle Free container.

        Constructs a :class:`ContainerOracle` instance and immediately starts the
        container.  Callers should use this as a context manager to ensure the
        container is stopped on exit.

        Args:
            image: Docker image tag.  Defaults to
                :data:`~oracle_dmp_converter.config.DEFAULT_ORACLE_IMAGE` or
                ``DMP_CONVERTER_IMAGE`` environment variable.
            password: Oracle SYS/SYSTEM password set via ``ORACLE_PASSWORD``.
            service: Oracle PDB service name.
            name: Container name; auto-generated if omitted.
            platform: Docker ``--platform`` string; falls back to
                ``DMP_CONVERTER_DOCKER_PLATFORM`` environment variable.
            mounts: Bind mounts as ``(host_path, container_path, mode)``
                triples, e.g. ``((Path("/dumps"), "/dumps", "rw"),)``.
            runtime: Container runtime override; falls back to
                ``DMP_CONVERTER_CONTAINER_RUNTIME`` or ``"docker"``.
            userns_mode: User-namespace mode (e.g. ``"keep-id"`` for rootless
                Podman).  ``None`` leaves the runtime default.

        Returns:
            A :class:`ContainerOracle` instance with :attr:`started` set to
            ``True``.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerImageError`: If the
                image cannot be found or pulled.
            :class:`~oracle_dmp_converter.errors.DockerContainerError`: If the
                container fails to start.
        """
        container = cls(
            image=image or DEFAULT_ORACLE_IMAGE,
            password=password,
            service=service,
            name=name or f"oracle-dmp-converter-{uuid.uuid4().hex[:12]}",
            platform=platform or os.environ.get("DMP_CONVERTER_DOCKER_PLATFORM"),
            mounts=mounts,
            runtime=runtime
            or os.environ.get("DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME),
            userns_mode=userns_mode,
        )
        container._start_container()
        return container

    @classmethod
    def reconnect(
        cls,
        *,
        name: str,
        image: str = "",
        service: str = "FREEPDB1",
        runtime: str | None = None,
    ) -> ContainerOracle:
        """Reconnect to an already-running Oracle container by name.

        Looks up the container in the Docker/Podman daemon, verifies it is
        still running, and reads the ``ORACLE_PASSWORD`` back from the
        container's environment so the caller does not need to supply it.

        Use this after loading a :class:`~oracle_dmp_converter.models.ContainerSession`
        written by a previous ``inspect`` run.  The returned instance behaves
        exactly like one returned by :meth:`start` — in particular,
        :meth:`stop` will stop (and auto-remove) the container.

        Args:
            name: Container name as recorded in ``session.json``.
            image: Docker image tag recorded in the session.  Used to populate
                :attr:`image` on the returned instance; not used to pull or
                create anything.
            service: Oracle PDB service name (default ``"FREEPDB1"``).
            runtime: Container runtime CLI override; falls back to
                ``DMP_CONVERTER_CONTAINER_RUNTIME`` or ``"docker"``.

        Returns:
            A :class:`ContainerOracle` instance with :attr:`started` set to
            ``True`` and :attr:`password` populated from the container env.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerContainerError`: If the
                container cannot be found or is not currently running.
            :class:`~oracle_dmp_converter.errors.DockerError`: If the
                Docker/Podman daemon cannot be reached.
        """
        effective_runtime = runtime or os.environ.get(
            "DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME
        )
        client = _docker_client(effective_runtime)
        try:
            existing = client.containers.get(name)
        except NotFound as exc:
            raise DockerContainerError(
                f"Session container {name!r} not found — "
                "the container may have exited since inspect was run"
            ) from exc
        except (APIError, DockerException) as exc:
            raise DockerContainerError(str(exc)) from exc

        try:
            existing.reload()
            state = existing.attrs.get("State", {})
        except (APIError, DockerException) as exc:
            raise DockerContainerError(str(exc)) from exc

        if not state.get("Running"):
            raise DockerContainerError(
                f"Session container {name!r} exists but is not running "
                f"(status={state.get('Status', 'unknown')!r})"
            )

        # Recover the Oracle password from the container's own environment so
        # callers do not need to pass it separately.
        password = ""
        env_list = existing.attrs.get("Config", {}).get("Env") or []
        for item in env_list:
            if item.startswith("ORACLE_PASSWORD="):
                password = item[len("ORACLE_PASSWORD=") :]
                break

        instance = cls(
            image=image,
            password=password,
            service=service,
            name=name,
            runtime=effective_runtime,
        )
        instance._client = client
        instance._container = existing
        instance.started = True
        LOGGER.info("Reconnected to Oracle container %s", name)
        return instance

    def _start_container(self) -> None:
        """Pull the image and start the container.

        Populates :attr:`_client` and :attr:`_container` and sets
        :attr:`started` to ``True``.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerImageError`: If the
                image is not available.
            :class:`~oracle_dmp_converter.errors.DockerContainerError`: On
                any other Docker/Podman error.
        """
        volumes: dict[str, dict[str, str]] = {}
        for host_path, container_path, mode in self.mounts:
            prepared = _ensure_mount_path_permissions(host_path, mode)
            # Podman on SELinux-enforcing systems requires the :z relabel option so
            # the container process can access host-mounted directories.  Appending
            # ",z" to the mode string (e.g. "rw,z") instructs Podman to apply a
            # shared SELinux relabel at mount time.  This is a no-op on systems
            # without SELinux and is ignored by Docker.
            effective_mode = f"{mode},z" if self.runtime == "podman" else mode
            volumes[str(prepared)] = {"bind": container_path, "mode": effective_mode}

        ports = {"1521/tcp": ("127.0.0.1", None)}
        self._client = _docker_client(self.runtime)
        LOGGER.info("Starting Oracle container %s (image=%s)", self.name, self.image)
        extra_kwargs: dict[str, object] = {}
        if self.userns_mode is not None:
            extra_kwargs["userns_mode"] = self.userns_mode
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
                **extra_kwargs,
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
        LOGGER.info("Oracle container %s is running", self.name)

    def _require_container(self) -> Any:
        """Return the underlying container object, raising if not started.

        Returns:
            The Docker SDK container object.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerContainerError`: If the
                container has not been started.
        """
        if self._container is None:
            raise DockerContainerError("Oracle container has not been started")
        return self._container

    def mapped_port(self) -> int:
        """Return the host port mapped to the container's Oracle listener.

        Reloads container metadata from the daemon to get the current port
        assignment (which is determined at runtime for ``None``-mapped ports).

        Returns:
            Host-side TCP port number mapped to container port ``1521``.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerPortError`: If the port
                mapping cannot be determined.
        """
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
        """Return the combined stdout/stderr logs from the container.

        Returns:
            Container log output decoded as a UTF-8 string (with replacement
            for undecodable bytes).

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerContainerError`: If the
                logs cannot be retrieved.
        """
        container = self._require_container()
        try:
            output = container.logs(stdout=True, stderr=True)
        except (APIError, DockerException) as exc:
            raise DockerContainerError(str(exc)) from exc
        if isinstance(output, bytes):
            return output.decode("utf-8", errors="replace")
        return str(output)

    def exec(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Execute a command inside the running container.

        Uses ``subprocess.run(["docker/podman", "exec", ...])`` rather than
        the Docker SDK's ``exec_run()`` because the SDK's chunked HTTP stream
        never sends EOF when the container stops mid-exec.

        Args:
            args: Command and arguments to run inside the container.
            check: If ``True`` (default), raise
                :class:`~oracle_dmp_converter.errors.DockerExecError` when the
                command exits with a non-zero status.

        Returns:
            A :class:`subprocess.CompletedProcess` with ``stdout`` and
            ``stderr`` captured as strings.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerExecError`: If *check*
                is ``True`` and the command exits with a non-zero status.
        """
        result = subprocess.run(
            [self.runtime, "exec", self.name, *args],
            capture_output=True,
            text=True,
            check=False,
        )
        LOGGER.debug("exec %s -> returncode=%d", args[0], result.returncode)
        if check and result.returncode != 0:
            msg = (
                result.stderr.strip()
                or result.stdout.strip()
                or f"{self.runtime} exec failed: {args}"
            )
            raise DockerExecError(msg)
        return result

    def copy_to(self, host_path: Path, container_path: str) -> None:
        """Copy a file from the host into the container.

        Delegates to :func:`_run_docker_cp`.

        Args:
            host_path: Absolute path of the file to copy on the host.
            container_path: Destination path inside the container.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerExecError`: If the copy
                fails.
        """
        LOGGER.debug("copy_to %s -> %s", host_path, container_path)
        _run_docker_cp(host_path, self.name, container_path, self.runtime)

    def wait_ready(self, timeout_seconds: int = 600) -> None:
        """Block until the Oracle database is accepting connections.

        Polls every 5 seconds by attempting a ``SELECT 1 FROM DUAL`` via the
        ``system`` user.  Returns as soon as the query succeeds.

        Args:
            timeout_seconds: Maximum time to wait before raising.

        Raises:
            :class:`~oracle_dmp_converter.errors.DockerReadinessError`: If the
                container disappears or the timeout is reached.
        """
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        container = self._require_container()
        LOGGER.info(
            "Waiting for Oracle container %s to be ready (timeout=%ds)",
            self.name,
            timeout_seconds,
        )
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
                    LOGGER.info("Oracle container %s is ready", self.name)
                    return
                finally:
                    conn.close()
            except DockerReadinessError:
                raise
            except Exception as exc:  # noqa: BLE001 - readiness polling preserves the last error.
                last_error = exc
                LOGGER.debug("Oracle container %s not yet ready: %s", self.name, exc)
                time.sleep(5)
        logs = self.logs()[-4000:]
        msg = f"Oracle did not become ready within {timeout_seconds}s"
        if last_error:
            msg += f"; last connection error: {last_error}"
        msg += f"\nContainer logs:\n{logs}"
        raise DockerReadinessError(msg)

    def stop(self) -> None:
        """Stop and remove the container.

        Attempts a graceful 30-second stop first; falls back to ``kill`` if
        the stop times out or fails.  Closes the Docker client connection and
        resets :attr:`started` to ``False``.  Safe to call multiple times.
        """
        if self.started:
            LOGGER.info("Stopping Oracle container %s", self.name)
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

    def __enter__(self) -> ContainerOracle:
        """Support use as a context manager; returns ``self``.

        Returns:
            This :class:`ContainerOracle` instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Stop the container on context manager exit.

        Args:
            exc_type: Exception type, or ``None`` if no exception occurred.
            exc: Exception instance, or ``None``.
            tb: Traceback, or ``None``.
        """
        self.stop()
