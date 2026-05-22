"""Workflow implementation for legacy Oracle exp/imp dump files."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow
from oracle_dmp_converter.datapump.legacy.imp_show import (
    parse_imp_indexfile_tables,
    parse_imp_indexfile_tablespaces,
)
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
)
from oracle_dmp_converter.datapump.legacy.runner import LegacyRunner
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)

_INDEXFILE_NAME = "dmpconverter-legacy-discovery.sql"
_INDEXFILE_REMOTE = f"/tmp/{_INDEXFILE_NAME}"
_DISCOVERY_LOG = "dmpconverter-legacy-discovery.log"


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
        discovery_runner: LegacyRunner,
        discovery_dir: Path,
        inspect_runner: LegacyRunner,
        convert_runner: LegacyRunner,
    ) -> None:
        """Store all configuration needed to drive legacy ``imp`` operations.

        Args:
            credentials: Oracle credentials written to the parfile ``USERID``
                field for every ``imp`` invocation.
            directory_path: Absolute OS path inside the container where the
                dump files reside.
            dumpfiles: Tuple of dump file base-names (without the directory
                path prefix).
            discovery_runner: :class:`LegacyRunner` used exclusively for the
                ``INDEXFILE=`` discovery invocation; its parfiles are written
                to ``discovery_dir / "parfiles"``.
            discovery_dir: Local directory where discovery artifacts
                (parfiles, ``.log``, ``.sql``) are written.  Created
                automatically if it does not already exist.
            inspect_runner: :class:`LegacyRunner` used for read-only
                ``ROWS=N`` metadata imports during the inspect phase.
            convert_runner: :class:`LegacyRunner` used for data-importing
                operations (row imports).
        """
        self._credentials = credentials
        self._directory_path = directory_path.rstrip("/")
        self._dumpfiles = dumpfiles
        self._discovery_runner = discovery_runner
        self._discovery_dir = discovery_dir
        discovery_dir.mkdir(parents=True, exist_ok=True)
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

        Saves ``discovery-imp-indexfile.log`` (subprocess stdout+stderr) and
        ``discovery-imp-indexfile.sql`` (the indexfile DDL) into
        :attr:`_discovery_dir`.
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
        LOGGER.info(
            "Running imp INDEXFILE discovery (files: %s, indexfile=%s)",
            ", ".join(self._dumpfiles),
            _INDEXFILE_REMOTE,
        )
        sql_text, log_output = self._discovery_runner.run_imp_indexfile(job)

        if not sql_text:
            LOGGER.debug(
                "INDEXFILE not found at %s; trying dump directory fallback", _INDEXFILE_REMOTE
            )
            alt = self._discovery_runner.container.exec(
                ["cat", f"{self._directory_path}/{_INDEXFILE_NAME}"], check=False
            )
            sql_text = alt.stdout if alt.returncode == 0 else ""

        LOGGER.info("imp INDEXFILE discovery complete (%d chars of DDL)", len(sql_text))

        (self._discovery_dir / "discovery-imp-indexfile.log").write_text(log_output)
        (self._discovery_dir / "discovery-imp-indexfile.sql").write_text(sql_text)

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

    def import_all_metadata(self, source_schema: str, stage_schema: str) -> None:
        """Import DDL for all tables in *source_schema* via a single ``imp ROWS=N`` call.

        Omitting ``tables`` from the job causes the parfile renderer to emit no
        ``TABLES=`` line, so ``imp`` imports metadata for every table in the schema
        at once.
        """
        LOGGER.debug("Bulk importing legacy metadata for %s -> %s", source_schema, stage_schema)
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-bulk-meta-{source_schema}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(),
            rows=False,
            indexes=False,
            grants=False,
            constraints=False,
        )
        self._inspect_runner.run_imp(job)

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import table DDL only (``rows=False``) into the staging schema."""
        LOGGER.debug(
            "Importing legacy metadata for %s.%s -> %s", source_schema, table, stage_schema
        )
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
        LOGGER.debug(
            "Importing legacy chunk %s for %s.%s -> %s",
            chunk_name,
            source_schema,
            table,
            stage_schema,
        )
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

    def import_chunks_batch(
        self,
        chunks: list[tuple[str, str, str, str, str | None]],
    ) -> None:
        """Import multiple tables using as few ``imp`` invocations as possible.

        Legacy ``imp`` only supports a single ``FROMUSER``/``TOUSER`` pair per
        invocation, so cross-schema batching is not possible.  Chunks are
        grouped by ``(source_schema, stage_schema)`` and one ``imp`` call is
        issued per distinct schema pair, with all table names for that pair
        combined into the ``TABLES=`` list.

        ``chunk_name`` and ``partition_name`` are ignored — legacy ``imp`` has
        no ``QUERY=`` support.
        """
        if not chunks:
            return
        # Group tables by (source_schema, stage_schema), preserving order.
        schema_groups: dict[tuple[str, str], list[str]] = {}
        for source_schema, stage_schema, table, _chunk_name, _partition_name in chunks:
            key = (source_schema, stage_schema)
            schema_groups.setdefault(key, []).append(table)

        for (source_schema, stage_schema), tables in schema_groups.items():
            unique_tables = tuple(dict.fromkeys(tables))
            LOGGER.debug(
                "Batch-importing %d legacy table(s) for %s -> %s via single imp call",
                len(unique_tables),
                source_schema,
                stage_schema,
            )
            logfile = f"imp-batch-{source_schema}-{'-'.join(unique_tables[:3])}.log"[:120]
            job = LegacyImportJob(
                connection=self._credentials,
                files=self._legacy_files(),
                logfile=logfile,
                fromuser=source_schema,
                touser=stage_schema,
                tables=unique_tables,
                rows=True,
                indexes=False,
                grants=False,
                constraints=False,
            )
            self._convert_runner.run_imp(job)


def make_legacy_runners(
    container: object,
    work_dir: Path,
) -> tuple[LegacyRunner, LegacyRunner, LegacyRunner]:
    """Create the discovery, inspect, and convert ``LegacyRunner`` triple.

    Returns ``(discovery_runner, inspect_runner, convert_runner)``.

    * *discovery_runner* writes parfiles to ``work_dir/discovery/parfiles/``
      and is used exclusively for the ``INDEXFILE=`` discovery invocation.
    * *inspect_runner* writes parfiles to ``work_dir/inspect/parfiles/``
      and handles ``ROWS=N`` metadata imports during the inspect phase.
    * *convert_runner* writes parfiles to ``work_dir/convert/parfiles/``
      and handles data-importing operations.
    """
    discovery_runner = LegacyRunner(container, work_dir / "discovery" / "parfiles")  # type: ignore[arg-type]
    inspect_runner = LegacyRunner(container, work_dir / "inspect" / "parfiles")  # type: ignore[arg-type]
    convert_runner = LegacyRunner(container, work_dir / "convert" / "parfiles")  # type: ignore[arg-type]
    return discovery_runner, inspect_runner, convert_runner
