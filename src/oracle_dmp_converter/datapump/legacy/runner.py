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
        super().__init__(container, work_dir)

    def run_imp(self, job: LegacyImportJob) -> str:
        """Run a legacy ``imp`` import job.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit, preserving the full combined stdout+stderr as the message.
        """
        return self._run_tool(["imp"], render_legacy_import_parfile(job), "imp")

    def run_imp_indexfile(self, job: LegacyIndexFileJob) -> str:
        """Run ``imp INDEXFILE=`` to discover tables in a legacy dump.

        Writes CREATE TABLE / CREATE INDEX DDL to *job.indexfile* inside
        the container, then reads and returns that file's contents.
        Returns an empty string if the file cannot be read (e.g. the dump
        is empty or contains no table objects).

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``imp``.
        """
        self._run_tool(["imp"], render_legacy_indexfile_parfile(job), "imp-indexfile")

        cat_result = self.container.exec(["cat", job.indexfile], check=False)
        return cat_result.stdout if cat_result.returncode == 0 else ""
