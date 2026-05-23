"""Persistent ``session.json`` handling and stale-session cleanup."""

from __future__ import annotations

import hashlib
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


def compute_session_fingerprint(
    *,
    oracle_image: str,
    container_runtime: str,
    container_name: str,
    prepared_schemas: frozenset[str] | None,
) -> str:
    """Return a stable SHA-256 fingerprint of the session-identifying inputs.

    Used by :func:`verify_session_fingerprint` to detect a ``session.json``
    that still references a container which has since been restarted,
    recreated, or had its prepared-schemas state diverge from what the
    session recorded.
    """
    inputs = [
        oracle_image,
        container_runtime,
        container_name,
        ",".join(sorted(prepared_schemas or ())),
    ]
    blob = "\x1f".join(inputs).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def verify_session_fingerprint(
    session: ContainerSession,
    *,
    container_name: str,
    container_runtime: str,
    oracle_image: str,
    prepared_schemas: frozenset[str] | None,
) -> tuple[bool, str]:
    """Compare the recorded fingerprint with one computed from current inputs.

    Returns ``(ok, reason)``.  ``ok`` is ``True`` for clean match.
    Sessions written by older versions (``fingerprint=""``) return
    ``(True, "unverified: session predates fingerprinting")`` so callers
    can choose to warn rather than fail.
    """
    if not session.fingerprint:
        return True, "unverified: session predates fingerprinting"
    expected = compute_session_fingerprint(
        oracle_image=oracle_image,
        container_runtime=container_runtime,
        container_name=container_name,
        prepared_schemas=prepared_schemas,
    )
    if expected == session.fingerprint:
        return True, "match"
    return False, (
        "fingerprint mismatch: recorded fingerprint does not match the "
        "current container/image/schemas. The container was likely "
        "restarted or recreated since the session was written."
    )


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
    metadata_imported: bool = False,
    prepared_schemas: frozenset[str] | None = None,
) -> None:
    """Serialise *container*'s identity to *path* as JSON.

    When *metadata_imported* is ``True`` the current UTC time is stamped into
    ``metadata_import_time`` so a later ``convert`` invocation can tell how
    fresh the imported staging state is.  *prepared_schemas* is normalised to
    a frozen set (defaulting to empty) so consumers always see a stable type.
    """
    now = datetime.now(UTC).isoformat(timespec="seconds")
    schemas = prepared_schemas or frozenset()
    fingerprint = compute_session_fingerprint(
        oracle_image=oracle_image,
        container_runtime=container_runtime,
        container_name=container.name,
        prepared_schemas=schemas,
    )
    session = ContainerSession(
        container_name=container.name,
        container_runtime=container_runtime,
        oracle_image=oracle_image,
        oracle_service=container.service,
        work_dir=str(work_dir.resolve()),
        dump_dir=str(dump_dir),
        created_at=now,
        metadata_imported=metadata_imported,
        metadata_import_time=now if metadata_imported else "",
        prepared_schemas=schemas,
        fingerprint=fingerprint,
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
