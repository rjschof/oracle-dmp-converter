"""Configuration container for :class:`OracleDMPConverter`."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig
from oracle_dmp_converter.models import OutputFormat

DEFAULT_CONTAINER_RUNTIME = "docker"


def _default_oracle_image() -> str:
    return os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE)


def _default_container_runtime() -> str:
    return os.environ.get("ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME)


@dataclass(frozen=True)
class ConverterSettings:
    """User-facing configuration for :class:`OracleDMPConverter`.

    Attributes:
        dump_paths: Resolved absolute paths to one or more ``.dmp`` files.  All
            paths must share a common parent directory because Oracle Data Pump
            mounts a single host directory into the container.
        work_dir: Host-side directory for intermediate artefacts, the session
            file, manifest, plan, and conversion report.
        output_dir: Where converted output files are written.  Required for
            :meth:`OracleDMPConverter.convert`; leave ``None`` for inspect- or
            plan-only workflows.
        output_format: Target output format for converted data.
        oracle_image: Docker/Podman image tag for the Oracle Free container.
            Reads ``ORACLE_DMP_CONVERTER_IMAGE`` at instantiation when not
            given explicitly.
        oracle_password: ``ORACLE_PASSWORD`` set on the running container.  No
            default — callers must specify a value.
        container_runtime: Container runtime CLI (``"docker"`` or ``"podman"``).
            Reads ``ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME`` at instantiation
            when not given explicitly.
        config: Optional per-table and per-column overrides.
        keep_alive: When ``True``, leave the Oracle container running and
            preserve ``session.json`` on :meth:`OracleDMPConverter.stop`.
    """

    dump_paths: tuple[Path, ...]
    oracle_password: str
    work_dir: Path = Path("work")
    output_dir: Path | None = None
    output_format: OutputFormat = OutputFormat.PARQUET
    oracle_image: str = field(default_factory=_default_oracle_image)
    container_runtime: str = field(default_factory=_default_container_runtime)
    config: ConverterConfig | None = None
    keep_alive: bool = False

    def __post_init__(self) -> None:
        if not self.dump_paths:
            raise ValueError("dump_paths must not be empty")
        parents = {path.parent.resolve() for path in self.dump_paths}
        if len(parents) != 1:
            raise ValueError("All dump_paths must share a single parent directory")

    @property
    def dump_dir(self) -> Path:
        """Common parent directory of every entry in :attr:`dump_paths`."""
        return next(iter({path.parent.resolve() for path in self.dump_paths}))

    @property
    def dump_filenames(self) -> tuple[str, ...]:
        """Bare filenames (no directory component) for each dump path."""
        return tuple(path.name for path in self.dump_paths)
