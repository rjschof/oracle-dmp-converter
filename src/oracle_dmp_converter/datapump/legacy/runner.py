"""Legacy imp/exp command execution inside a Docker container."""

from __future__ import annotations

import logging

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
from oracle_dmp_converter.datapump._exit_policy import LEGACY_IMP_POLICY
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
    render_legacy_import_parfile,
    render_legacy_indexfile_parfile,
)

LOGGER = logging.getLogger(__name__)


class LegacyRunner(_BaseRunner):
    """Executes legacy Oracle imp/exp jobs inside a Docker container.

    Legacy ``imp`` returns ``2`` for completed-with-warnings.  We pass
    :data:`LEGACY_IMP_POLICY` so that exit code is *logged* as a warning
    but does not raise unless the output also contains a fatal ORA code
    (e.g. ``ORA-39126``, ``ORA-31693``).
    """

    def run_imp(self, job: LegacyImportJob) -> str:
        """Run a legacy ``imp`` import job."""
        return self._run_tool(
            ["imp"],
            render_legacy_import_parfile(job),
            "imp",
            policy=LEGACY_IMP_POLICY,
        )

    def run_imp_indexfile(self, job: LegacyIndexFileJob) -> tuple[str, str]:
        """Run ``imp INDEXFILE=`` to discover tables in a legacy dump.

        Writes CREATE TABLE / CREATE INDEX DDL to *job.indexfile* inside
        the container, then reads and returns that file's contents alongside
        the combined stdout+stderr from the ``imp`` invocation.

        Returns a ``(sql_content, log_output)`` tuple.  *sql_content* is the
        indexfile DDL text (empty string if the file cannot be read).
        """
        log_output = self._run_tool(
            ["imp"],
            render_legacy_indexfile_parfile(job),
            "imp-indexfile",
            policy=LEGACY_IMP_POLICY,
        )

        cat_result = self.container.exec(["cat", job.indexfile], check=False)
        sql_content = cat_result.stdout if cat_result.returncode == 0 else ""
        return sql_content, log_output
