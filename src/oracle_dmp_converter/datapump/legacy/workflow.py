"""Workflow implementation for legacy Oracle exp/imp dump files."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.datapump.legacy.imp_show import (
    parse_imp_indexfile_tables,
    parse_imp_indexfile_tablespaces,
)
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
)
from oracle_dmp_converter.datapump.legacy.runner import LegacyRunner
from oracle_dmp_converter.datapump.workflow import DumpWorkflow
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)

_INDEXFILE_NAME = "dmp2parquet-legacy-discovery.sql"
_INDEXFILE_REMOTE = f"/tmp/{_INDEXFILE_NAME}"
_DISCOVERY_LOG = "dmp2parquet-legacy-discovery.log"


class LegacyDumpWorkflow(DumpWorkflow):
    """Workflow for legacy Oracle exp/imp dump files.

    ``imp INDEXFILE=`` is run lazily on first access and the resulting SQL
    text is cached so that :meth:`discover_tables` and
    :meth:`required_tablespaces` share a single round-trip to the container.
    """

    def __init__(
        self,
        *,
        credentials: OracleCredentials,
        directory_path: str,
        dumpfiles: tuple[str, ...],
        inspect_runner: LegacyRunner,
        convert_runner: LegacyRunner,
    ) -> None:
        self._credentials = credentials
        self._directory_path = directory_path.rstrip("/")
        self._dumpfiles = dumpfiles
        self._inspect_runner = inspect_runner
        self._convert_runner = convert_runner
        # Cached after the first call to _indexfile_sql().
        self._cached_sql: str | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _legacy_files(self) -> tuple[str, ...]:
        """Absolute paths to dump files inside the container."""
        return tuple(f"{self._directory_path}/{f}" for f in self._dumpfiles)

    def _indexfile_sql(self) -> str:
        """Run ``imp INDEXFILE=`` once and cache the DDL text.

        Falls back to reading from the directory path if ``/tmp`` does not
        contain the indexfile (some Oracle versions write it there instead).
        """
        if self._cached_sql is not None:
            return self._cached_sql

        job = LegacyIndexFileJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=_DISCOVERY_LOG,
            indexfile=_INDEXFILE_REMOTE,
            full=True,
        )
        sql_text = self._inspect_runner.run_imp_indexfile(job)

        if not sql_text:
            alt = self._inspect_runner.container.exec(
                ["cat", f"{self._directory_path}/{_INDEXFILE_NAME}"], check=False
            )
            sql_text = alt.stdout if alt.returncode == 0 else ""

        self._cached_sql = sql_text
        return self._cached_sql

    # ------------------------------------------------------------------
    # DumpWorkflow interface
    # ------------------------------------------------------------------

    @property
    def dump_format(self) -> DumpFormat:
        return DumpFormat.LEGACY

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover schema/table pairs via ``imp INDEXFILE=``."""
        return parse_imp_indexfile_tables(self._indexfile_sql())

    def required_tablespaces(self) -> frozenset[str]:
        """Return custom tablespaces referenced in the dump DDL.

        These must be pre-created in the staging Oracle instance before
        ``imp`` can land tables that reference them.
        """
        return parse_imp_indexfile_tablespaces(self._indexfile_sql())

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import table DDL only (``rows=False``) into the staging schema."""
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-meta-{source_schema}-{table}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(table,),
            rows=False,
            indexes=False,
            grants=False,
            constraints=False,
        )
        self._inspect_runner.run_imp(job)

    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
    ) -> None:
        """Import one chunk of table data into the staging schema.

        Legacy ``imp`` does not support ``QUERY=`` or partition-level imports;
        ``chunk_name`` and ``partition_name`` are ignored and the whole table
        is always imported.  The planner must not produce ``HASH`` or
        ``PARTITION`` chunks for legacy dumps.
        """
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-{source_schema}-{table}-{chunk_name}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(table,),
            rows=True,
            indexes=False,
            grants=False,
            constraints=False,
        )
        self._convert_runner.run_imp(job)


def make_legacy_runners(
    container: object,
    work_dir: Path,
) -> tuple[LegacyRunner, LegacyRunner]:
    """Create the inspect and convert ``LegacyRunner`` pair for a given container."""
    inspect_runner = LegacyRunner(container, work_dir / "inspect" / "parfiles")  # type: ignore[arg-type]
    convert_runner = LegacyRunner(container, work_dir / "convert" / "parfiles")  # type: ignore[arg-type]
    return inspect_runner, convert_runner
