"""Unit tests for datapump/_base_runner.py."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
from oracle_dmp_converter.datapump._exit_policy import (
    LEGACY_IMP_POLICY,
    STRICT_POLICY,
)
from oracle_dmp_converter.errors import DataPumpError


def _make_container(returncode: int = 0, stdout: str = "ok", stderr: str = "") -> MagicMock:
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    container = MagicMock()
    container.exec.return_value = result
    return container


class TestBaseRunnerInit:
    def test_creates_work_dir(self, tmp_path: Path) -> None:
        work_dir = tmp_path / "parfiles"
        assert not work_dir.exists()
        _BaseRunner(_make_container(), work_dir)
        assert work_dir.exists()


class TestParfileContextManager:
    def test_writes_parfile_locally_and_copies_to_container(self, tmp_path: Path) -> None:
        container = _make_container()
        runner = _BaseRunner(container, tmp_path)
        with runner._parfile("USERID=sys/pw\n", "expdp") as remote:
            assert remote.startswith("/tmp/")
            assert remote.endswith(".par")
        container.copy_to.assert_called_once()

    def test_cleans_up_local_and_remote_parfile_by_default(self, tmp_path: Path) -> None:
        container = _make_container()
        runner = _BaseRunner(container, tmp_path)
        with runner._parfile("c", "impdp") as remote:
            local_files = list(tmp_path.glob("*.par"))
            assert local_files, "parfile should exist while inside the context"
        # On exit the local parfile is removed and a `rm -f <remote>` exec ran.
        assert not list(tmp_path.glob("*.par"))
        rm_call = next(
            (c for c in container.exec.call_args_list if c.args[0][:2] == ["rm", "-f"]),
            None,
        )
        assert rm_call is not None
        assert rm_call.args[0][2] == remote

    def test_keep_parfiles_skips_cleanup(self, tmp_path: Path) -> None:
        container = _make_container()
        runner = _BaseRunner(container, tmp_path, keep_parfiles=True)
        with runner._parfile("c", "impdp"):
            pass
        assert list(tmp_path.glob("*.par")), "parfile should be preserved"
        # No `rm -f` exec should have been issued.
        assert not any(c.args[0][:2] == ["rm", "-f"] for c in container.exec.call_args_list)

    def test_cleanup_runs_even_when_body_raises(self, tmp_path: Path) -> None:
        container = _make_container()
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(RuntimeError):
            with runner._parfile("c", "impdp"):
                raise RuntimeError("boom")
        assert not list(tmp_path.glob("*.par"))


class TestRunTool:
    def test_returns_output_on_success(self, tmp_path: Path) -> None:
        container = _make_container(returncode=0, stdout="Export completed", stderr="")
        runner = _BaseRunner(container, tmp_path)
        output = runner._run_tool(["expdp"], "USERID=u/p\n", "expdp")
        assert "Export completed" in output

    def test_raises_datapump_error_on_nonzero_exit_strict_policy(self, tmp_path: Path) -> None:
        container = _make_container(returncode=1, stdout="", stderr="ORA-39001: invalid argument")
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(DataPumpError, match="ORA-39001"):
            runner._run_tool(["impdp"], "USERID=u/p\n", "impdp", policy=STRICT_POLICY)

    def test_combines_stdout_and_stderr(self, tmp_path: Path) -> None:
        container = _make_container(returncode=1, stdout="stdout-msg", stderr="stderr-msg")
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(DataPumpError) as exc_info:
            runner._run_tool(["impdp"], "x", "impdp")
        assert "stdout-msg" in str(exc_info.value)
        assert "stderr-msg" in str(exc_info.value)

    def test_legacy_imp_exit_2_with_warnings_does_not_raise(self, tmp_path: Path) -> None:
        # Legacy imp exit 2 = EX_OKWARN.  Without any fatal ORA codes in the
        # output the warning is logged but the call returns normally.
        container = _make_container(
            returncode=2,
            stdout="IMP-00041: Warnings encountered during import\n",
            stderr="",
        )
        runner = _BaseRunner(container, tmp_path)
        output = runner._run_tool(["imp"], "USERID=u/p\n", "imp", policy=LEGACY_IMP_POLICY)
        assert "IMP-00041" in output

    def test_legacy_imp_exit_2_with_ora_39126_promotes_to_fatal(self, tmp_path: Path) -> None:
        # Even when imp exits 2 (warning), an ORA-39126 in the output
        # promotes the result to fatal: the worker died mid-import.
        container = _make_container(
            returncode=2,
            stdout="ORA-39126: Worker unexpected fatal error\n",
            stderr="",
        )
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(DataPumpError, match="ORA-39126"):
            runner._run_tool(["imp"], "USERID=u/p\n", "imp", policy=LEGACY_IMP_POLICY)
