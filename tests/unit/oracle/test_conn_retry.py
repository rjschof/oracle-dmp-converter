"""Unit tests for the ``with_oracle_retry`` helper in oracle/conn.py.

Covers:
- Retryable ORA codes (listener-down, TNS timeouts) trigger retry.
- Non-retryable ORA codes (bad credentials) raise immediately.
- Non-DatabaseError exceptions propagate unchanged.
- Retries are bounded; the last exception bubbles up on exhaustion.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import oracledb
import pytest

from oracle_dmp_converter.oracle.conn import with_oracle_retry


def _db_error(code: int) -> oracledb.DatabaseError:
    """Construct an oracledb.DatabaseError that reports ORA-<code>.

    Uses a MagicMock to mimic the internal `_Error` shape (which has
    ``.code`` and ``.message`` attributes) without poking at the
    private class itself.
    """
    info = MagicMock()
    info.code = code
    info.message = f"ORA-{code:05d}: simulated"
    return oracledb.DatabaseError(info)


def test_retries_then_succeeds_on_transient_listener_error() -> None:
    calls = {"n": 0}

    def operation() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _db_error(12541)  # TNS:no listener
        return "ok"

    with patch("time.sleep"):  # do not actually wait
        assert with_oracle_retry(operation, attempts=5) == "ok"
    assert calls["n"] == 3


def test_non_retryable_ora_code_raises_immediately() -> None:
    calls = {"n": 0}

    def operation() -> str:
        calls["n"] += 1
        raise _db_error(1017)  # invalid username/password

    with patch("time.sleep"), pytest.raises(oracledb.DatabaseError):
        with_oracle_retry(operation, attempts=5)
    assert calls["n"] == 1


def test_non_database_error_propagates() -> None:
    def operation() -> str:
        raise RuntimeError("not an Oracle error")

    with pytest.raises(RuntimeError):
        with_oracle_retry(operation, attempts=5)


def test_attempts_exhausted_reraises_last_exception() -> None:
    calls = {"n": 0}

    def operation() -> str:
        calls["n"] += 1
        raise _db_error(12541)

    with patch("time.sleep"), pytest.raises(oracledb.DatabaseError):
        with_oracle_retry(operation, attempts=3)
    assert calls["n"] == 3


def test_attempts_one_disables_retry() -> None:
    calls = {"n": 0}

    def operation() -> str:
        calls["n"] += 1
        raise _db_error(12541)

    with pytest.raises(oracledb.DatabaseError):
        with_oracle_retry(operation, attempts=1)
    assert calls["n"] == 1
