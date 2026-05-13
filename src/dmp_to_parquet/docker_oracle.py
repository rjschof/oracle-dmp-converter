"""Docker lifecycle for Oracle Database Free."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import oracledb

from dmp_to_parquet.config import DEFAULT_ORACLE_IMAGE
from dmp_to_parquet.errors import DockerError


def docker_available() -> bool:
    result = subprocess.run(
        ["docker", "version", "--format", "{{json .Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _run_docker(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or f"docker {' '.join(args)} failed"
        raise DockerError(msg)
    return result


@dataclass
class DockerOracle:
    image: str = DEFAULT_ORACLE_IMAGE
    password: str = "OraclePwd_123"
    service: str = "FREEPDB1"
    name: str = field(default_factory=lambda: f"dmp2parquet-{uuid.uuid4().hex[:12]}")
    platform: str | None = None
    mounts: tuple[tuple[Path, str, str], ...] = ()
    started: bool = False

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
    ) -> DockerOracle:
        container = cls(
            image=image or DEFAULT_ORACLE_IMAGE,
            password=password,
            service=service,
            name=name or f"dmp2parquet-{uuid.uuid4().hex[:12]}",
            platform=platform or os.environ.get("DMP_TO_PARQUET_DOCKER_PLATFORM"),
            mounts=mounts,
        )
        container._start_container()
        return container

    def _start_container(self) -> None:
        args = [
            "run",
            "--detach",
            "--rm",
            "--name",
            self.name,
            "--shm-size",
            "1g",
            "-e",
            f"ORACLE_PASSWORD={self.password}",
            "-p",
            "127.0.0.1::1521",
        ]
        if self.platform:
            args.extend(["--platform", self.platform])
        for host_path, container_path, mode in self.mounts:
            args.extend(["-v", f"{host_path.resolve()}:{container_path}:{mode}"])
        args.append(self.image)
        _run_docker(args)
        self.started = True

    def mapped_port(self) -> int:
        result = _run_docker(["inspect", self.name])
        data = json.loads(result.stdout)[0]
        ports = data["NetworkSettings"]["Ports"].get("1521/tcp")
        if not ports:
            raise DockerError("Oracle container does not expose port 1521")
        return int(ports[0]["HostPort"])

    def logs(self) -> str:
        result = _run_docker(["logs", self.name], check=False)
        return result.stdout + result.stderr

    def exec(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = _run_docker(["exec", self.name, *args], check=False)
        if check and result.returncode != 0:
            msg = result.stderr.strip() or result.stdout.strip() or f"docker exec failed: {args}"
            raise DockerError(msg)
        return result

    def copy_to(self, host_path: Path, container_path: str) -> None:
        _run_docker(["cp", str(host_path), f"{self.name}:{container_path}"])

    def wait_ready(self, timeout_seconds: int = 600) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            state = _run_docker(["inspect", self.name], check=False)
            if state.returncode != 0:
                raise DockerError(state.stderr.strip() or "Oracle container disappeared")
            try:
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
            except Exception as exc:  # noqa: BLE001 - readiness polling preserves the last error.
                last_error = exc
                time.sleep(5)
        logs = self.logs()[-4000:]
        msg = f"Oracle did not become ready within {timeout_seconds}s"
        if last_error:
            msg += f"; last connection error: {last_error}"
        msg += f"\nContainer logs:\n{logs}"
        raise DockerError(msg)

    def stop(self) -> None:
        if self.started:
            _run_docker(["stop", "--timeout", "30", self.name], check=False)
            self.started = False

    def __enter__(self) -> DockerOracle:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
