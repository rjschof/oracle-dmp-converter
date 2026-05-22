"""Unit tests for datapump/_base_runner.py."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
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


class TestWriteAndCopy:
    def test_writes_parfile_locally_and_copies_to_container(self, tmp_path: Path) -> None:
        container = _make_container()
        runner = _BaseRunner(container, tmp_path)
        remote = runner._write_and_copy("USERID=sys/pw\n", "expdp")
        assert remote.startswith("/tmp/")
        assert remote.endswith(".par")
        container.copy_to.assert_called_once()


class TestRunTool:
    def test_returns_output_on_success(self, tmp_path: Path) -> None:
        container = _make_container(returncode=0, stdout="Export completed", stderr="")
        runner = _BaseRunner(container, tmp_path)
        output = runner._run_tool(["expdp"], "USERID=u/p\n", "expdp")
        assert "Export completed" in output

    def test_raises_datapump_error_on_nonzero_exit(self, tmp_path: Path) -> None:
        container = _make_container(returncode=1, stdout="", stderr="ORA-39001: invalid argument")
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(DataPumpError, match="ORA-39001"):
            runner._run_tool(["impdp"], "USERID=u/p\n", "impdp")

    def test_combines_stdout_and_stderr(self, tmp_path: Path) -> None:
        container = _make_container(returncode=1, stdout="stdout-msg", stderr="stderr-msg")
        runner = _BaseRunner(container, tmp_path)
        with pytest.raises(DataPumpError) as exc_info:
            runner._run_tool(["impdp"], "x", "impdp")
        assert "stdout-msg" in str(exc_info.value)
        assert "stderr-msg" in str(exc_info.value)
