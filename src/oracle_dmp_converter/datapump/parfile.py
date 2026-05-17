"""Data Pump parameter file rendering."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataPumpConnection:
    user: str
    password: str
    service: str = "FREEPDB1"

    @property
    def userid(self) -> str:
        return f"{self.user}/{self.password}@{self.service}"


@dataclass(frozen=True)
class ExportJob:
    connection: DataPumpConnection
    directory: str
    dumpfile: str
    logfile: str
    full: bool = True
    include_schemas: tuple[str, ...] = ()


@dataclass(frozen=True)
class ImportJob:
    connection: DataPumpConnection
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    source_schema: str
    table: str
    remap_schema: tuple[str, str] | None = None
    content: str | None = None
    query: str | None = None
    partition_name: str | None = None
    table_exists_action: str = "REPLACE"
    exclude: tuple[str, ...] = field(
        default=("INDEX", "CONSTRAINT", "REF_CONSTRAINT", "TRIGGER", "STATISTICS")
    )
    transform: tuple[str, ...] = field(
        default=("DISABLE_ARCHIVE_LOGGING:Y", "SEGMENT_ATTRIBUTES:N")
    )


@dataclass(frozen=True)
class SqlFileJob:
    connection: DataPumpConnection
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    sqlfile: str
    full: bool = True
    include: tuple[str, ...] = ("TABLE",)


def _schema_include_expression(schemas: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{schema.replace("'", "''")}'" for schema in schemas)
    return f"IN ({quoted})"


def _table_spec(schema: str, table: str, partition_name: str | None = None) -> str:
    spec = f"{oracle_identifier(schema)}.{oracle_identifier(table)}"
    if partition_name:
        spec += f":{oracle_identifier(partition_name)}"
    return spec


def _query_prefix(schema: str, table: str) -> str:
    return f"{oracle_identifier(schema)}.{oracle_identifier(table)}"


def render_export_parfile(job: ExportJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={job.dumpfile}",
        f"LOGFILE={job.logfile}",
    ]
    if job.full:
        lines.append("FULL=Y")
    if job.include_schemas:
        lines.append(f'INCLUDE=SCHEMA:"{_schema_include_expression(job.include_schemas)}"')
    return "\n".join(lines) + "\n"


def render_import_parfile(job: ImportJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"TABLES={_table_spec(job.source_schema, job.table, job.partition_name)}",
        f"TABLE_EXISTS_ACTION={job.table_exists_action}",
    ]
    for transform in job.transform:
        lines.append(f"TRANSFORM={transform}")
    if job.content:
        lines.append(f"CONTENT={job.content}")
    if job.remap_schema:
        source, target = job.remap_schema
        lines.append(f"REMAP_SCHEMA={oracle_identifier(source)}:{oracle_identifier(target)}")
    if job.query:
        compact_query = " ".join(job.query.split())
        lines.append(f'QUERY={_query_prefix(job.source_schema, job.table)}:"WHERE {compact_query}"')
    for object_type in job.exclude:
        lines.append(f"EXCLUDE={object_type}")
    return "\n".join(lines) + "\n"


def render_sqlfile_parfile(job: SqlFileJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"SQLFILE={job.sqlfile}",
    ]
    if job.full:
        lines.append("FULL=Y")
    for object_type in job.include:
        lines.append(f"INCLUDE={object_type}")
    return "\n".join(lines) + "\n"
