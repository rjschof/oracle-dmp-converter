"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

import pytest

from oracle_dmp_converter.docker_oracle import docker_available


@pytest.fixture(autouse=True)
def skip_if_no_docker() -> None:
    """Skip every integration test when Docker is unavailable."""
    if not docker_available():
        pytest.skip("Docker is not available")
