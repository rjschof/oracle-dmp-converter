"""Unit tests for session fingerprint computation and verification.

The session fingerprint binds together the inputs that, taken together,
identify the running container that a ``session.json`` describes.  If any
of those inputs diverge between when the session was written and when it's
reused, the converter should treat the recorded staging state as stale
rather than blindly trusting it.
"""

from __future__ import annotations

from oracle_dmp_converter.models import ContainerSession
from oracle_dmp_converter.runtime.session import (
    compute_session_fingerprint,
    verify_session_fingerprint,
)


def _make_session(**overrides) -> ContainerSession:
    defaults = {
        "container_name": "oracle-staging",
        "container_runtime": "docker",
        "oracle_image": "oracle/database:free-23ai",
        "oracle_service": "FREEPDB1",
        "work_dir": "/tmp/work",
        "dump_dir": "/tmp/dump",
        "prepared_schemas": frozenset({"DMP_HRDATA", "DMP_FINANCE"}),
    }
    defaults.update(overrides)
    defaults["fingerprint"] = compute_session_fingerprint(
        oracle_image=defaults["oracle_image"],
        container_runtime=defaults["container_runtime"],
        container_name=defaults["container_name"],
        prepared_schemas=defaults["prepared_schemas"],
    )
    return ContainerSession(**defaults)


def test_matching_inputs_verify_clean() -> None:
    session = _make_session()
    ok, reason = verify_session_fingerprint(
        session,
        container_name=session.container_name,
        container_runtime=session.container_runtime,
        oracle_image=session.oracle_image,
        prepared_schemas=session.prepared_schemas,
    )
    assert ok
    assert reason == "match"


def test_different_image_fails_verification() -> None:
    session = _make_session()
    ok, reason = verify_session_fingerprint(
        session,
        container_name=session.container_name,
        container_runtime=session.container_runtime,
        oracle_image="oracle/database:free-21c",  # different
        prepared_schemas=session.prepared_schemas,
    )
    assert not ok
    assert "mismatch" in reason


def test_different_prepared_schemas_fails_verification() -> None:
    session = _make_session()
    ok, _ = verify_session_fingerprint(
        session,
        container_name=session.container_name,
        container_runtime=session.container_runtime,
        oracle_image=session.oracle_image,
        prepared_schemas=frozenset({"DMP_HRDATA"}),  # missing DMP_FINANCE
    )
    assert not ok


def test_different_container_name_fails_verification() -> None:
    session = _make_session()
    ok, _ = verify_session_fingerprint(
        session,
        container_name="other-container",
        container_runtime=session.container_runtime,
        oracle_image=session.oracle_image,
        prepared_schemas=session.prepared_schemas,
    )
    assert not ok


def test_legacy_session_without_fingerprint_is_unverified_not_failed() -> None:
    """Sessions written by older versions have fingerprint='' — preserve compat."""
    session = ContainerSession(
        container_name="oracle-staging",
        container_runtime="docker",
        oracle_image="oracle/database:free-23ai",
        oracle_service="FREEPDB1",
        work_dir="/tmp/work",
        dump_dir="/tmp/dump",
        prepared_schemas=frozenset(),
        fingerprint="",
    )
    ok, reason = verify_session_fingerprint(
        session,
        container_name=session.container_name,
        container_runtime=session.container_runtime,
        oracle_image=session.oracle_image,
        prepared_schemas=session.prepared_schemas,
    )
    assert ok
    assert "unverified" in reason


def test_fingerprint_stable_across_set_ordering() -> None:
    """Same schemas in different iteration order must produce the same fingerprint."""
    fp_a = compute_session_fingerprint(
        oracle_image="img",
        container_runtime="docker",
        container_name="c",
        prepared_schemas=frozenset({"A", "B", "C"}),
    )
    fp_b = compute_session_fingerprint(
        oracle_image="img",
        container_runtime="docker",
        container_name="c",
        prepared_schemas=frozenset({"C", "B", "A"}),
    )
    assert fp_a == fp_b
