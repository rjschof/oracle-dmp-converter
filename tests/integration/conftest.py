"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

import os

import pytest

from oracle_dmp_converter.runtime.container_oracle import docker_available


@pytest.fixture(autouse=True)
def skip_if_no_docker() -> None:
    """Skip every integration test when the configured container runtime is unavailable."""
    runtime = os.environ.get("ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME", "docker")
    if not docker_available(runtime):
        pytest.skip(f"{runtime} is not available")
