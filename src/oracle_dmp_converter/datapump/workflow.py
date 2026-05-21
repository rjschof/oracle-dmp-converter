"""Dump workflow abstraction: the seam between the converter and format-specific code.

:class:`DumpWorkflow` is the single interface that
:class:`~oracle_dmp_converter.converter.OracleDumpConverter` uses to interact
with the underlying dump format.  Concrete implementations live in the
``modern`` and ``legacy`` sub-packages:

* :class:`~oracle_dmp_converter.datapump.modern.workflow.DataPumpWorkflow` -
  for dumps produced by ``expdp`` (modern Data Pump).
* :class:`~oracle_dmp_converter.datapump.legacy.workflow.LegacyDumpWorkflow` -
  for dumps produced by legacy ``exp``.

The :func:`create_workflow` factory auto-detects the format: it first
attempts a ``DataPumpWorkflow``, and if the underlying ``impdp`` invocation
signals a legacy-format error it transparently falls back to
``LegacyDumpWorkflow``.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from oracle_dmp_converter.docker_oracle import DockerOracle
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)


class DumpWorkflow(ABC):
    """Format-agnostic interface for interacting with an Oracle dump file.

    All format-specific branching (``impdp`` vs ``imp``, ``SQLFILE`` vs
    ``INDEXFILE``, modern parfile syntax vs legacy parfile syntax) is
    encapsulated here so the orchestrator can call a single, consistent API
    regardless of whether the dump was produced by modern Data Pump or
    legacy ``exp``.
    """

    @property
    @abstractmethod
    def dump_format(self) -> DumpFormat:
        """Return the :class:`DumpFormat` this workflow handles."""

    @abstractmethod
    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Return ``(schema, table)`` pairs found in the dump.

        For modern dumps this runs ``impdp SQLFILE=`` and parses the DDL.
        For legacy dumps this runs ``imp INDEXFILE=`` and parses the DDL.
        """

    @abstractmethod
    def required_tablespaces(self) -> frozenset[str]:
        """Return custom tablespaces that must exist before import begins.

        Modern dumps always return an empty set.  Legacy dumps return any
        non-system tablespaces referenced in their ``INDEXFILE=`` DDL.
        """

    @abstractmethod
    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import the DDL for *table* (no row data) into *stage_schema*.

        Modern: ``impdp CONTENT=METADATA_ONLY``
        Legacy: ``imp ROWS=N``
        """

    @abstractmethod
    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
    ) -> None:
        """Import one logical chunk of *table* data into *stage_schema*.

        Modern: ``impdp TABLES=schema.table:partition`` (partition optional).
        Legacy: ``imp TABLES=(table)`` — partition and chunk names are ignored
        because legacy ``imp`` has no ``QUERY=`` support.
        """


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowConfig:
    """All parameters required to build either workflow implementation.

    Attributes:
        credentials: Oracle credentials used in the parfile ``USERID`` field.
        directory: Oracle DIRECTORY object name (e.g. ``"DUMP_DIR"``).
        directory_path: OS path inside the container that *directory* maps to.
        dumpfiles: Tuple of dump file base-names (without directory path).
        container: Running Oracle Docker container.
        work_dir: Local directory for temporary parfiles.
    """

    credentials: OracleCredentials
    directory: str
    directory_path: str
    dumpfiles: tuple[str, ...]
    container: DockerOracle
    work_dir: Path


def create_workflow(cfg: WorkflowConfig) -> DumpWorkflow:
    """Detect the dump format and return the appropriate :class:`DumpWorkflow`.

    Tries :class:`~oracle_dmp_converter.datapump.modern.workflow.DataPumpWorkflow`
    first.  If ``impdp`` signals a legacy-format error (``ORA-39142`` /
    ``ORA-39143``) the function transparently constructs and returns a
    :class:`~oracle_dmp_converter.datapump.legacy.workflow.LegacyDumpWorkflow`
    instead.

    Any other ``impdp`` failure is re-raised as-is.
    """
    # Import here to avoid circular imports between workflow.py and its
    # sub-package implementations.
    from oracle_dmp_converter.datapump.legacy.workflow import (  # noqa: PLC0415
        LegacyDumpWorkflow,
        make_legacy_runners,
    )
    from oracle_dmp_converter.datapump.modern.runner import is_legacy_format_error  # noqa: PLC0415
    from oracle_dmp_converter.datapump.modern.workflow import (  # noqa: PLC0415
        DataPumpWorkflow,
        make_modern_runners,
    )

    inspect_runner, convert_runner = make_modern_runners(cfg.container, cfg.work_dir)
    modern_workflow = DataPumpWorkflow(
        credentials=cfg.credentials,
        directory=cfg.directory,
        directory_path=cfg.directory_path,
        dumpfiles=cfg.dumpfiles,
        inspect_runner=inspect_runner,
        convert_runner=convert_runner,
    )

    # Probe the format by attempting table discovery with the modern workflow.
    # If it raises DataPumpError containing a legacy-format ORA code, fall back.
    LOGGER.info("Probing dump format via impdp SQLFILE (files: %s)", ", ".join(cfg.dumpfiles))
    try:
        tables = modern_workflow.discover_tables()
    except DataPumpError as exc:
        if not is_legacy_format_error(str(exc)):
            raise
        LOGGER.info("Legacy exp format detected; switching to imp-based workflow")
        legacy_inspect, legacy_convert = make_legacy_runners(cfg.container, cfg.work_dir)
        return LegacyDumpWorkflow(
            credentials=cfg.credentials,
            directory_path=cfg.directory_path,
            dumpfiles=cfg.dumpfiles,
            inspect_runner=legacy_inspect,
            convert_runner=legacy_convert,
        )

    LOGGER.info("Modern Data Pump format confirmed; discovered %d tables", len(tables))

    # Modern probe succeeded — but we already consumed the discovery result
    # by calling discover_tables() above.  Wrap the workflow so the converter
    # gets the cached result without re-running the SQLFILE= job.
    return _ProbedModernWorkflow(modern_workflow, discovered_tables=tables)


class _ProbedModernWorkflow(DumpWorkflow):
    """Wraps a ``DataPumpWorkflow`` after format detection has already run.

    :func:`create_workflow` calls ``discover_tables()`` internally to probe
    the format.  This wrapper holds the result from that probe so the
    converter does not re-run the ``SQLFILE=`` job when it calls
    ``discover_tables()`` later.
    """

    def __init__(
        self,
        inner: DumpWorkflow,
        discovered_tables: tuple[tuple[str, str], ...],
    ) -> None:
        """Wrap *inner* and cache the already-computed table list.

        Args:
            inner: The probed
                :class:`~oracle_dmp_converter.datapump.modern.workflow.DataPumpWorkflow`
                whose ``discover_tables()`` has already been called.
            discovered_tables: The ``(schema, table)`` pairs returned by
                the probe run; returned verbatim by :meth:`discover_tables`
                without re-running the ``SQLFILE=`` job.
        """
        self._inner = inner
        self._cached_tables = discovered_tables

    @property
    def dump_format(self) -> DumpFormat:
        return self._inner.dump_format

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        return self._cached_tables

    def required_tablespaces(self) -> frozenset[str]:
        return self._inner.required_tablespaces()

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        self._inner.import_metadata(source_schema, stage_schema, table)

    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
    ) -> None:
        self._inner.import_chunk(source_schema, stage_schema, table, chunk_name, partition_name)
