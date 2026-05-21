"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_dmp_converter.cli import SESSION_FILENAME, _cleanup_stale_session
from oracle_dmp_converter.docker_oracle import docker_available

_RUNS_DIR = Path(__file__).resolve().parent.parent / "runs"


@pytest.fixture(autouse=True)
def skip_if_no_docker() -> None:
    """Skip every integration test when Docker is unavailable."""
    if not docker_available():
        pytest.skip("Docker is not available")


@pytest.fixture(autouse=True)
def cleanup_integration_sessions() -> None:
    """Always stop inspect sessions created by integration tests."""
    yield
    for session_path in _RUNS_DIR.rglob(SESSION_FILENAME):
        _cleanup_stale_session(session_path)
