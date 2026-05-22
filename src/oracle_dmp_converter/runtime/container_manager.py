"""Single entry point for starting or reconnecting the Oracle container."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.runtime.admin import (
    DEFAULT_CONTAINER_CONVERT_PATH,
    DEFAULT_CONTAINER_DISCOVERY_PATH,
    DEFAULT_CONTAINER_DUMP_PATH,
    DEFAULT_CONTAINER_INSPECT_PATH,
)
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle
from oracle_dmp_converter.runtime.session import load_session_if_exists, session_path_for
from oracle_dmp_converter.settings import ConverterSettings

LOGGER = logging.getLogger(__name__)


def build_work_subdir_mounts(work_dir: Path) -> tuple[tuple[Path, str, str], ...]:
    """Create discovery/inspect/convert subdirs and return bind-mount specs."""
    subdirs = (
        (work_dir / "discovery", DEFAULT_CONTAINER_DISCOVERY_PATH),
        (work_dir / "inspect", DEFAULT_CONTAINER_INSPECT_PATH),
        (work_dir / "convert", DEFAULT_CONTAINER_CONVERT_PATH),
    )
    for host_path, _ in subdirs:
        host_path.mkdir(parents=True, exist_ok=True)
    return tuple((host_path, container_path, "rw") for host_path, container_path in subdirs)


def start_or_reconnect(settings: ConverterSettings) -> ContainerOracle:
    """Reconnect to a saved session if present, otherwise start a fresh container.

    This is the single seam tests patch out to avoid requiring Docker.
    """
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    session_path = session_path_for(settings.work_dir)
    session = load_session_if_exists(session_path)
    if session is not None:
        LOGGER.info(
            "Found session at %s — reconnecting to container %s",
            session_path,
            session.container_name,
        )
        return ContainerOracle.reconnect(
            name=session.container_name,
            image=session.oracle_image or settings.oracle_image,
            service=session.oracle_service,
            runtime=settings.container_runtime,
        )
    LOGGER.info(
        "Starting Oracle container (image=%s, dump_dir=%s)",
        settings.oracle_image,
        settings.dump_dir,
    )
    work_subdir_mounts = build_work_subdir_mounts(settings.work_dir)
    return ContainerOracle.start(
        image=settings.oracle_image,
        password=settings.oracle_password,
        mounts=(
            (settings.dump_dir, DEFAULT_CONTAINER_DUMP_PATH, "rw"),
            *work_subdir_mounts,
        ),
        runtime=settings.container_runtime,
        userns_mode=settings.userns_mode,
    )
