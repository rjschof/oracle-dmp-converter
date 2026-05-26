"""Legacy exp/imp parameter file rendering.

The legacy ``exp``/``imp`` utilities pre-date Data Pump and use a
different parameter format.  Key differences from Data Pump parfiles:

* ``FILE=`` takes the full file-system path to the dump file; there is
  no Oracle directory object abstraction.
* Schema remapping uses ``FROMUSER=`` / ``TOUSER=`` instead of
  ``REMAP_SCHEMA=``.
* Row-data inclusion is controlled by ``ROWS=Y/N`` instead of
  ``CONTENT=``.
* Individual object-type exclusions (``INDEXES=N``, ``GRANTS=N``, …)
  replace the generic ``EXCLUDE=`` directive.
* There is no ``QUERY=`` support, so arbitrary WHERE-filter chunking
  is impossible.  Partition- and subpartition-level imports *are*
  supported via the ``TABLES=schema.table:NAME`` syntax (Oracle's
  Original Import docs accept both partition and subpartition names in
  the ``:NAME`` slot since subpartition names are unique within a
  table).  ``LegacyImportJob.tables`` entries may therefore include
  ``:NAME`` qualifiers.
* The ``INDEXFILE=`` parameter writes CREATE TABLE / CREATE INDEX DDL
  to a file without executing any import - the equivalent of Data
  Pump's ``SQLFILE=``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LegacyExportJob:
    """Parameter specification for a legacy ``exp`` export job."""

    connection: OracleCredentials
    # Absolute path(s) inside the container for the output dump file(s).
    files: tuple[str, ...]
    logfile: str
    owner: tuple[str, ...] = ()
    full: bool = False
    rows: bool = True
    indexes: bool = False
    grants: bool = False
    compress: bool = False


@dataclass(frozen=True)
class LegacyImportJob:
    """Parameter specification for a legacy ``imp`` import job.

    ``files`` must be absolute paths inside the container (e.g.
    ``/dumps/export.dmp``).  ``fromuser`` / ``touser`` handle schema
    remapping.  Set ``rows=False`` for a metadata-only import
    (equivalent to ``CONTENT=METADATA_ONLY`` in Data Pump).
    """

    connection: OracleCredentials
    # Absolute path(s) inside the container.
    files: tuple[str, ...]
    logfile: str
    fromuser: str
    touser: str
    tables: tuple[str, ...] = ()
    rows: bool = True
    indexes: bool = False
    grants: bool = False
    constraints: bool = False
    # When True the import replaces an existing table; when False it
    # appends to (or skips) an existing table.
    ignore: bool = True
    # When True emit ``DATA_ONLY=Y``, which suppresses ALL metadata DDL
    # (table creates, indexes, triggers, DBMS_RLS policy calls, …) and
    # loads only row data into already-existing tables.  Used by the
    # chunk-import workflow: the metadata-import phase has already
    # created all objects, and re-running ``DBMS_RLS.ADD_POLICY`` per
    # chunk would re-establish stale VPD policies pointing at functions
    # the staging schema does not have, raising ORA-28100 on the next
    # SELECT.  Note: ``DATA_ONLY=Y`` is incompatible with ``IGNORE=Y``
    # (IMP-00402); when ``data_only`` is True the renderer omits the
    # ``IGNORE`` line entirely.
    data_only: bool = False


@dataclass(frozen=True)
class LegacyIndexFileJob:
    """Parameter specification for ``imp INDEXFILE=`` discovery.

    Passing ``INDEXFILE=`` to ``imp`` causes it to write CREATE TABLE
    and CREATE INDEX SQL to the named file without importing any data.
    The resulting SQL is similar to Data Pump's ``SQLFILE=`` output and
    can be parsed to discover which schemas and tables are present in
    the dump.
    """

    connection: OracleCredentials
    # Absolute path(s) inside the container.
    files: tuple[str, ...]
    logfile: str
    # Absolute path inside the container where DDL will be written.
    indexfile: str
    full: bool = True
    # Optional owner filter - limits discovery to specific schemas.
    owner: tuple[str, ...] = field(default_factory=tuple)


def render_legacy_export_parfile(job: LegacyExportJob) -> str:
    """Render a parameter file for ``exp``."""
    lines = [
        f"USERID={job.connection.userid}",
        f"FILE={','.join(job.files)}",
        f"LOG={job.logfile}",
        f"ROWS={'Y' if job.rows else 'N'}",
        f"INDEXES={'Y' if job.indexes else 'N'}",
        f"GRANTS={'Y' if job.grants else 'N'}",
        f"COMPRESS={'Y' if job.compress else 'N'}",
    ]
    if job.full:
        lines.append("FULL=Y")
    elif job.owner:
        owner_list = ", ".join(job.owner)
        lines.append(f"OWNER=({owner_list})")
    return "\n".join(lines) + "\n"


def render_legacy_import_parfile(job: LegacyImportJob) -> str:
    """Render a parameter file for ``imp``."""
    lines = [
        f"USERID={job.connection.userid}",
        f"FILE={','.join(job.files)}",
        f"LOG={job.logfile}",
        f"FROMUSER={job.fromuser}",
        f"TOUSER={job.touser}",
        f"ROWS={'Y' if job.rows else 'N'}",
        f"INDEXES={'Y' if job.indexes else 'N'}",
        f"GRANTS={'Y' if job.grants else 'N'}",
        f"CONSTRAINTS={'Y' if job.constraints else 'N'}",
    ]
    # IMP-00402: combining DATA_ONLY=Y with IGNORE=Y aborts the import
    # before any rows are loaded, so omit IGNORE entirely in DATA_ONLY mode.
    if not job.data_only:
        lines.append(f"IGNORE={'Y' if job.ignore else 'N'}")
    if job.data_only:
        lines.append("DATA_ONLY=Y")
    if job.tables:
        tables_list = ", ".join(job.tables)
        lines.append(f"TABLES=({tables_list})")
    return "\n".join(lines) + "\n"


def render_legacy_indexfile_parfile(job: LegacyIndexFileJob) -> str:
    """Render a parameter file for ``imp INDEXFILE=`` discovery."""
    lines = [
        f"USERID={job.connection.userid}",
        f"FILE={','.join(job.files)}",
        f"LOG={job.logfile}",
        f"INDEXFILE={job.indexfile}",
        f"FULL={'Y' if job.full else 'N'}",
    ]
    if job.owner:
        owner_list = ", ".join(job.owner)
        lines.append(f"OWNER=({owner_list})")
    return "\n".join(lines) + "\n"
