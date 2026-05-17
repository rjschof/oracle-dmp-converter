"""Data Pump command execution through Docker."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from oracle_dmp_converter.datapump.legacy_parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
    render_legacy_import_parfile,
    render_legacy_indexfile_parfile,
)
from oracle_dmp_converter.datapump.parfile import (
    ExportJob,
    ImportJob,
    SqlFileJob,
    render_export_parfile,
    render_import_parfile,
    render_sqlfile_parfile,
)
from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.errors import DataPumpError

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error-code helpers
# ---------------------------------------------------------------------------

# ORA-39142: incompatible version number in dump file.
# ORA-39143: "The file may be an original export dump file."
# Either code is a definitive signal the dump was created by legacy exp, not expdp.
# The exact code varies by Oracle version; 23ai Free emits ORA-39143.
# ORA-39000 / ORA-39001 often accompany these in the same impdp output.
_LEGACY_FORMAT_ERRORS = ("ORA-39142", "ORA-39143")


def is_legacy_format_error(output: str) -> bool:
    """Return True if *output* contains an error code that identifies a legacy
    ``exp`` dump file being fed to ``impdp``."""
    return any(code in output for code in _LEGACY_FORMAT_ERRORS)


class DataPumpRunner:
    def __init__(self, container: DockerOracle, work_dir: Path) -> None:
        self.container = container
        self.work_dir = work_dir
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def _write_and_copy(self, content: str, prefix: str) -> str:
        local_path = self.work_dir / f"{prefix}-{uuid.uuid4().hex}.par"
        local_path.write_text(content)
        remote_path = f"/tmp/{local_path.name}"
        self.container.copy_to(local_path, remote_path)
        return remote_path

    def run_expdp(self, job: ExportJob) -> str:
        remote_path = self._write_and_copy(render_export_parfile(job), "expdp")
        result = self.container.exec(["expdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    def run_impdp(self, job: ImportJob) -> str:
        remote_path = self._write_and_copy(render_import_parfile(job), "impdp")
        result = self.container.exec(["impdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    def run_sqlfile(self, job: SqlFileJob) -> str:
        remote_path = self._write_and_copy(render_sqlfile_parfile(job), "sqlfile")
        result = self.container.exec(["impdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    # ------------------------------------------------------------------
    # Legacy exp/imp support
    # ------------------------------------------------------------------

    def run_imp(self, job: LegacyImportJob) -> str:
        """Run a legacy ``imp`` import job inside the container.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit, preserving the full combined stdout+stderr as the message.
        """
        remote_path = self._write_and_copy(render_legacy_import_parfile(job), "imp")
        result = self.container.exec(["imp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    def run_imp_indexfile(self, job: LegacyIndexFileJob) -> str:
        """Run ``imp INDEXFILE=`` to discover tables in a legacy dump.

        Writes CREATE TABLE / CREATE INDEX DDL to *job.indexfile* inside
        the container, then reads and returns that file's contents.
        Returns an empty string if the file cannot be read (e.g. the dump
        is empty or contains no table objects).

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``imp``.
        """
        remote_path = self._write_and_copy(render_legacy_indexfile_parfile(job), "imp-indexfile")
        result = self.container.exec(["imp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)

        # Read the INDEXFILE content back out of the container.
        cat_result = self.container.exec(["cat", job.indexfile], check=False)
        return cat_result.stdout if cat_result.returncode == 0 else ""
