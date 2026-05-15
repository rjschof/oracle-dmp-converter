"""Legacy Oracle exp/imp parameter file rendering.

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
* There is no ``QUERY=`` support, so hash/partition chunking is not
  available.
* The ``INDEXFILE=`` parameter writes CREATE TABLE / CREATE INDEX DDL
  to a file without executing any import – the equivalent of Data
  Pump's ``SQLFILE=``.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LegacyConnection:
    """Credentials for legacy exp/imp utilities."""

    user: str
    password: str
    service: str = "FREEPDB1"

    @property
    def userid(self) -> str:
        return f"{self.user}/{self.password}@{self.service}"


@dataclass(frozen=True)
class LegacyExportJob:
    """Parameter specification for a legacy ``exp`` export job."""

    connection: LegacyConnection
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

    connection: LegacyConnection
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


@dataclass(frozen=True)
class LegacyIndexFileJob:
    """Parameter specification for ``imp INDEXFILE=`` discovery.

    Passing ``INDEXFILE=`` to ``imp`` causes it to write CREATE TABLE
    and CREATE INDEX SQL to the named file without importing any data.
    The resulting SQL is similar to Data Pump's ``SQLFILE=`` output and
    can be parsed to discover which schemas and tables are present in
    the dump.
    """

    connection: LegacyConnection
    # Absolute path(s) inside the container.
    files: tuple[str, ...]
    logfile: str
    # Absolute path inside the container where DDL will be written.
    indexfile: str
    full: bool = True
    # Optional owner filter – limits discovery to specific schemas.
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
        f"IGNORE={'Y' if job.ignore else 'N'}",
    ]
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
