"""Data Pump command execution for modern expdp/impdp operations."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
from oracle_dmp_converter.datapump.modern.parfile import (
    ExportJob,
    ImportJob,
    SqlFileJob,
    render_export_parfile,
    render_import_parfile,
    render_sqlfile_parfile,
)
from oracle_dmp_converter.docker_oracle import DockerOracle

LOGGER = logging.getLogger(__name__)

# ORA-39142: incompatible version number in dump file.
# ORA-39143: "The file may be an original export dump file."
# Either code is a definitive signal the dump was created by legacy exp, not expdp.
# The exact code varies by Oracle version; 23ai Free emits ORA-39143.
_LEGACY_FORMAT_ERRORS = ("ORA-39142", "ORA-39143")


def is_legacy_format_error(output: str) -> bool:
    """Return True if *output* contains an error identifying a legacy ``exp`` dump."""
    return any(code in output for code in _LEGACY_FORMAT_ERRORS)


class DataPumpRunner(_BaseRunner):
    """Executes modern Data Pump (expdp/impdp) jobs inside a Docker container."""

    def __init__(self, container: DockerOracle, work_dir: Path) -> None:
        super().__init__(container, work_dir)

    def run_expdp(self, job: ExportJob) -> str:
        return self._run_tool(["expdp"], render_export_parfile(job), "expdp")

    def run_impdp(self, job: ImportJob) -> str:
        return self._run_tool(["impdp"], render_import_parfile(job), "impdp")

    def run_sqlfile(self, job: SqlFileJob) -> str:
        return self._run_tool(["impdp"], render_sqlfile_parfile(job), "sqlfile")

    def read_remote_file(self, path: str) -> str:
        """Read a file from inside the container, returning its contents or ''."""
        result = self.container.exec(["cat", path], check=False)
        return result.stdout if result.returncode == 0 else ""
