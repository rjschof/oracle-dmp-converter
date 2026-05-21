"""Legacy imp/exp command execution inside a Docker container."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
    render_legacy_import_parfile,
    render_legacy_indexfile_parfile,
)
from oracle_dmp_converter.docker_oracle import DockerOracle

LOGGER = logging.getLogger(__name__)


class LegacyRunner(_BaseRunner):
    """Executes legacy Oracle imp/exp jobs inside a Docker container."""

    def __init__(self, container: DockerOracle, work_dir: Path) -> None:
        """Initialise the runner with a target container and local work directory.

        Args:
            container: Running :class:`~oracle_dmp_converter.docker_oracle.DockerOracle`
                instance in which ``imp`` commands will be executed.
            work_dir: Local directory for temporary parfiles; passed directly
                to :class:`~oracle_dmp_converter.datapump._base_runner._BaseRunner`.
        """
        super().__init__(container, work_dir)

    def run_imp(self, job: LegacyImportJob) -> str:
        """Run a legacy ``imp`` import job.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit, preserving the full combined stdout+stderr as the message.
        """
        return self._run_tool(["imp"], render_legacy_import_parfile(job), "imp")

    def run_imp_indexfile(self, job: LegacyIndexFileJob) -> tuple[str, str]:
        """Run ``imp INDEXFILE=`` to discover tables in a legacy dump.

        Writes CREATE TABLE / CREATE INDEX DDL to *job.indexfile* inside
        the container, then reads and returns that file's contents alongside
        the combined stdout+stderr from the ``imp`` invocation.

        Returns a ``(sql_content, log_output)`` tuple.  *sql_content* is the
        indexfile DDL text (empty string if the file cannot be read).
        *log_output* is the combined stdout+stderr from the ``imp`` process.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``imp``.
        """
        log_output = self._run_tool(["imp"], render_legacy_indexfile_parfile(job), "imp-indexfile")

        cat_result = self.container.exec(["cat", job.indexfile], check=False)
        sql_content = cat_result.stdout if cat_result.returncode == 0 else ""
        return sql_content, log_output
