"""Unit tests for the converter.py ``_safe_stop_container`` helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_dmp_converter.converter import _safe_stop_container


def _make_container(name: str = "oracle-test") -> MagicMock:
    c = MagicMock()
    c.name = name
    return c


def test_stops_container_cleanly() -> None:
    container = _make_container()
    _safe_stop_container(container, reason="normal shutdown")
    container.stop.assert_called_once()


def test_swallows_docker_error_by_default(caplog: pytest.LogCaptureFixture) -> None:
    container = _make_container()
    container.stop.side_effect = RuntimeError("docker daemon went away")
    # Should NOT raise; the original exception path (e.g. start failure)
    # is more important than secondary cleanup failures.
    _safe_stop_container(container, reason="start failure")
    assert any("Container stop failed" in r.message for r in caplog.records)


def test_reraises_when_flag_set() -> None:
    container = _make_container()
    container.stop.side_effect = RuntimeError("docker daemon died")
    with pytest.raises(RuntimeError, match="docker daemon died"):
        _safe_stop_container(container, reason="normal shutdown", reraise=True)


def test_log_includes_container_name_and_reason(caplog: pytest.LogCaptureFixture) -> None:
    container = _make_container(name="oracle-XYZ")
    container.stop.side_effect = RuntimeError("kaboom")
    _safe_stop_container(container, reason="weird state")
    log_text = " ".join(r.getMessage() for r in caplog.records)
    assert "oracle-XYZ" in log_text
    assert "weird state" in log_text
