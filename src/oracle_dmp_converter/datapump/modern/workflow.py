"""Data Pump workflow implementation for modern expdp/impdp dumps."""

from __future__ import annotations

import logging
from pathlib import Path

from oracle_dmp_converter.datapump.modern.parfile import ImportJob, SqlFileJob
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner
from oracle_dmp_converter.datapump.modern.sqlfile import parse_sqlfile_tables
from oracle_dmp_converter.datapump.workflow import DumpWorkflow
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)


class DataPumpWorkflow(DumpWorkflow):
    """Workflow for modern Oracle Data Pump (expdp/impdp) dump files."""

    def __init__(
        self,
        *,
        credentials: OracleCredentials,
        directory: str,
        directory_path: str,
        dumpfiles: tuple[str, ...],
        inspect_runner: DataPumpRunner,
        convert_runner: DataPumpRunner,
    ) -> None:
        """Store all configuration needed to drive modern Data Pump operations.

        Args:
            credentials: Oracle credentials written to the parfile ``USERID``
                field for every ``impdp`` invocation.
            directory: Oracle DIRECTORY object name (e.g. ``"DUMP_DIR"``).
            directory_path: Absolute OS path inside the container that
                *directory* maps to; used when reading files produced by
                ``impdp SQLFILE=``.
            dumpfiles: Tuple of dump file base-names (without directory path).
            inspect_runner: :class:`DataPumpRunner` used for read-only
                discovery operations (``SQLFILE=``, ``CONTENT=METADATA_ONLY``).
            convert_runner: :class:`DataPumpRunner` used for data-importing
                operations.
        """
        self._credentials = credentials
        self._directory = directory
        self._directory_path = directory_path.rstrip("/")
        self._dumpfiles = dumpfiles
        self._inspect_runner = inspect_runner
        self._convert_runner = convert_runner

    # ------------------------------------------------------------------
    # DumpWorkflow interface
    # ------------------------------------------------------------------

    @property
    def dump_format(self) -> DumpFormat:
        return DumpFormat.DATAPUMP

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover schema/table pairs via ``impdp SQLFILE=``.

        Runs a ``SQLFILE=`` job so that Data Pump writes CREATE TABLE DDL
        to a file inside the container, then reads and parses that file.
        """
        sqlfile = "dmp2parquet-discovery.sql"
        job = SqlFileJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile="dmp2parquet-discovery.log",
            sqlfile=sqlfile,
        )
        LOGGER.info("Running impdp SQLFILE discovery (sqlfile=%s)", sqlfile)
        self._inspect_runner.run_sqlfile(job)

        # Try the canonical directory path first, then a glob for sub-dirs.
        sql_text = self._inspect_runner.read_remote_file(f"{self._directory_path}/{sqlfile}")
        if not sql_text:
            result = self._inspect_runner.container.exec(
                [
                    "bash",
                    "-lc",
                    (
                        "for path in "
                        f"{self._directory_path}/{sqlfile} "
                        f"{self._directory_path}/*/{sqlfile}; do "
                        '[ -f "$path" ] && cat "$path"; '
                        "done"
                    ),
                ],
                check=False,
            )
            sql_text = result.stdout if result.returncode == 0 else ""

        tables = parse_sqlfile_tables(sql_text)
        LOGGER.info("SQLFILE discovery found %d tables", len(tables))
        return tables

    def required_tablespaces(self) -> frozenset[str]:
        """Modern Data Pump imports never require pre-created tablespaces."""
        return frozenset()

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import table DDL only (``CONTENT=METADATA_ONLY``) into the staging schema."""
        LOGGER.debug("Importing metadata for %s.%s -> %s", source_schema, table, stage_schema)
        job = ImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"metadata-{source_schema}-{table}.log"[:120],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, stage_schema),
            content="METADATA_ONLY",
            exclude=("INDEX", "REF_CONSTRAINT", "TRIGGER", "GRANT"),
        )
        self._inspect_runner.run_impdp(job)

    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
    ) -> None:
        """Import one chunk of table data into the staging schema."""
        LOGGER.debug(
            "Importing chunk %s for %s.%s -> %s", chunk_name, source_schema, table, stage_schema
        )
        job = ImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"impdp-{source_schema}-{table}-{chunk_name}.log"[:120],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, stage_schema),
            partition_name=partition_name,
        )
        self._convert_runner.run_impdp(job)


def make_modern_runners(
    container: object,
    work_dir: Path,
) -> tuple[DataPumpRunner, DataPumpRunner]:
    """Create the inspect and convert ``DataPumpRunner`` pair for a given container."""
    inspect_runner = DataPumpRunner(container, work_dir / "inspect" / "parfiles")  # type: ignore[arg-type]
    convert_runner = DataPumpRunner(container, work_dir / "convert" / "parfiles")  # type: ignore[arg-type]
    return inspect_runner, convert_runner
