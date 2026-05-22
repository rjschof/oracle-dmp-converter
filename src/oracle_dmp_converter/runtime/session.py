"""Persistent ``session.json`` handling and stale-session cleanup."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from oracle_dmp_converter.models import ContainerSession
from oracle_dmp_converter.persistence.serialization import (
    load_session,
    save_session,
)
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)

SESSION_FILENAME = "session.json"


def session_path_for(work_dir: Path) -> Path:
    """Return the canonical session.json path inside *work_dir*."""
    return work_dir / SESSION_FILENAME


def load_session_if_exists(path: Path) -> ContainerSession | None:
    """Return the loaded session if *path* exists, else ``None``."""
    if not path.exists():
        return None
    return load_session(path)


def write_session(
    path: Path,
    *,
    container: ContainerOracle,
    container_runtime: str,
    oracle_image: str,
    work_dir: Path,
    dump_dir: Path,
) -> None:
    """Serialise *container*'s identity to *path* as JSON."""
    session = ContainerSession(
        container_name=container.name,
        container_runtime=container_runtime,
        oracle_image=oracle_image,
        oracle_service=container.service,
        work_dir=str(work_dir.resolve()),
        dump_dir=str(dump_dir),
        created_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    save_session(path, session)


def cleanup_stale_session(path: Path) -> None:
    """Stop the container recorded in *path* (best-effort) and delete the file."""
    try:
        session = load_session(path)
        stale = ContainerOracle.reconnect(
            name=session.container_name,
            image=session.oracle_image,
            service=session.oracle_service,
            runtime=session.container_runtime,
        )
        stale.stop()
        LOGGER.info("Stopped stale session container %s", session.container_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Could not stop stale session container: %s", exc)
    try:
        path.unlink()
    except OSError:
        pass
