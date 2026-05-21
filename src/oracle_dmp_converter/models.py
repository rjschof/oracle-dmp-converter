"""Shared metadata and planning models."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# Oracle ALL_TAB_COLUMNS embeds precision inside the type name, e.g.
# "TIMESTAMP(6) WITH TIME ZONE" or "INTERVAL DAY(2) TO SECOND(6)".
# Strip those so normalized lookups against STRINGIFIED_TYPES / TIMESTAMP_TYPES
# work regardless of the declared precision.
_PRECISION_RE = re.compile(r"\(\d+\)")

LOGGER = logging.getLogger(__name__)


class TableStrategy(StrEnum):
    """Strategy used to export and convert an Oracle table.

    Attributes:
        WHOLE_TABLE: Export the entire table in a single operation.
        PARTITION: Export each partition as an independent chunk.
        UNSUPPORTED: The requested strategy cannot be fulfilled; the table is
            skipped with an explanatory reason attached to the plan.
    """

    WHOLE_TABLE = "whole_table"
    PARTITION = "partition"
    UNSUPPORTED = "unsupported"


class DumpFormat(StrEnum):
    """Oracle dump file format.

    Attributes:
        DATAPUMP: Modern Oracle Data Pump format produced by ``expdp``.
        LEGACY: Classic export format produced by legacy ``exp``.
    """

    DATAPUMP = "datapump"
    LEGACY = "legacy"


class OutputFormat(StrEnum):
    """Target output format for converted table data.

    Attributes:
        PARQUET: Apache Parquet columnar format (default).
        AVRO: Apache Avro row-based format.
        CSV: Comma-separated values with a single header row.
    """

    PARQUET = "parquet"
    AVRO = "avro"
    CSV = "csv"


@dataclass(frozen=True)
class ColumnMetadata:
    """Metadata for a single Oracle table column.

    Attributes:
        name: Oracle column name, case-preserved.
        data_type: Oracle data type string as reported by ``ALL_TAB_COLUMNS``,
            e.g. ``"VARCHAR2"`` or ``"TIMESTAMP(6) WITH TIME ZONE"``.
        ordinal: 1-based column position in the table.
        nullable: Whether the column allows NULL values.
        data_precision: Total number of significant digits for NUMBER types.
        data_scale: Number of digits to the right of the decimal point.
        char_length: Maximum character length for character types.
        char_used: Length semantics for character types: ``'B'`` for byte,
            ``'C'`` for character, or ``None`` for non-character types.
    """

    name: str
    data_type: str
    ordinal: int
    nullable: bool = True
    data_precision: int | None = None
    data_scale: int | None = None
    char_length: int | None = None
    char_used: str | None = None

    @property
    def normalized_type(self) -> str:
        """Return the data type with embedded precision specifiers removed.

        Oracle embeds numeric precision inside the type name, e.g.
        ``"TIMESTAMP(6) WITH TIME ZONE"``.  This property strips those
        parenthesised digits so that type lookups work regardless of the
        declared precision.

        Returns:
            Upper-cased data type string with all ``(N)`` tokens removed.
        """
        return _PRECISION_RE.sub("", self.data_type.upper())


@dataclass(frozen=True)
class PartitionMetadata:
    """Metadata for a single Oracle table partition.

    Attributes:
        name: Partition name as reported by ``ALL_TAB_PARTITIONS``.
        position: 1-based ordinal position of the partition within the table.
    """

    name: str
    position: int


@dataclass(frozen=True)
class TableMetadata:
    """Metadata for an Oracle table discovered during the inspect phase.

    Attributes:
        schema: Oracle schema (owner) name, case-preserved.
        name: Table name, case-preserved.
        columns: Ordered tuple of column metadata.
        estimated_bytes: Estimated on-disk segment size in bytes, or ``None``
            if unavailable.
        row_count: Approximate row count from ``ALL_TABLES.NUM_ROWS``, or
            ``None`` if statistics have not been gathered.
        partitions: Ordered tuple of partition metadata; empty for
            non-partitioned tables.
        primary_key: Tuple of column names forming the single-column or
            multi-column primary key; empty if none exists.
        unique_keys: Tuple of tuples, each representing the column names of
            one unique constraint on the table.
    """

    schema: str
    name: str
    columns: tuple[ColumnMetadata, ...]
    estimated_bytes: int | None = None
    row_count: int | None = None
    partitions: tuple[PartitionMetadata, ...] = ()
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()

    @property
    def qualified_name(self) -> str:
        """Return the fully-qualified ``SCHEMA.TABLE`` identifier.

        Returns:
            A string of the form ``"SCHEMA.TABLE"``.
        """
        return f"{self.schema}.{self.name}"

    def column(self, name: str) -> ColumnMetadata | None:
        """Look up a column by name, case-insensitively.

        Args:
            name: Column name to search for.  An exact match is attempted
                first; if that fails the comparison is repeated in upper case.

        Returns:
            The matching :class:`ColumnMetadata` instance, or ``None`` if no
            column with that name exists.
        """
        for column in self.columns:
            if column.name == name or column.name.upper() == name.upper():
                return column
        return None


@dataclass(frozen=True)
class ChunkPlan:
    """Plan for a single export/import chunk within a table.

    A chunk corresponds to one output file and one import operation.  For
    whole-table strategies there is exactly one chunk named ``"whole"``; for
    partition strategies there is one chunk per partition.

    Attributes:
        name: Unique chunk identifier used as the output filename stem, e.g.
            ``"whole"`` or ``"partition-00001-P_NORTH"``.
        strategy: The :class:`TableStrategy` that governs how this chunk is
            imported.
        partition_name: Oracle partition name for ``PARTITION`` strategy
            chunks; ``None`` for ``WHOLE_TABLE`` chunks.
    """

    name: str
    strategy: TableStrategy
    partition_name: str | None = None


@dataclass(frozen=True)
class TablePlan:
    """Conversion plan for a single Oracle table.

    Attributes:
        schema: Oracle schema name, case-preserved.
        table: Table name, case-preserved.
        strategy: Top-level :class:`TableStrategy` for the table.
        chunks: Ordered tuple of :class:`ChunkPlan` instances describing each
            individual import/export operation.
        reason: Human-readable explanation for ``UNSUPPORTED`` tables, or
            ``None`` for supported strategies.
        warnings: Tuple of non-fatal warning messages generated during
            planning (e.g. nullable split column, missing statistics).
        extra: Arbitrary key/value data attached by the planner for use by
            the converter (e.g. ``split_column``, ``buckets``).
    """

    schema: str
    table: str
    strategy: TableStrategy
    chunks: tuple[ChunkPlan, ...] = ()
    reason: str | None = None
    warnings: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        """Return the fully-qualified ``SCHEMA.TABLE`` identifier.

        Returns:
            A string of the form ``"SCHEMA.TABLE"``.
        """
        return f"{self.schema}.{self.table}"


@dataclass(frozen=True)
class DumpManifest:
    """Inspection manifest produced by the ``inspect`` phase.

    Serialised to ``manifest.json`` and consumed by the ``plan`` phase.

    Attributes:
        dump_paths: Absolute paths to the source ``.dmp`` files inside the
            container's dump directory.
        tables: Metadata for every discoverable table in the dump.
        version: Manifest schema version; currently always ``1``.
        dump_format: Whether the dump is a modern Data Pump or legacy exp dump.
        oracle_image: Docker image tag used for the Oracle Free staging
            container during inspect.  Empty string when not recorded (e.g.
            manifests produced by older versions of the tool).
        container_runtime: Container runtime (``"docker"`` or ``"podman"``)
            used during inspect.  Empty string when not recorded.
    """

    dump_paths: tuple[str, ...]
    tables: tuple[TableMetadata, ...]
    version: int = 1
    dump_format: DumpFormat = DumpFormat.DATAPUMP
    oracle_image: str = ""
    container_runtime: str = ""


@dataclass(frozen=True)
class ContainerSession:
    """Active container session written by ``inspect`` for reuse by ``convert``.

    Serialised to ``session.json`` in the work directory.  The Oracle password
    is intentionally omitted — it is read back from the running container's
    environment via ``docker inspect`` when reconnecting.

    Attributes:
        container_name: Docker/Podman container name used to reconnect.
        container_runtime: Container runtime CLI (``"docker"`` or ``"podman"``).
        oracle_image: Docker image tag that was used to start the container.
        oracle_service: Oracle PDB service name (e.g. ``"FREEPDB1"``).
        work_dir: Absolute path to the host-side working directory.
        dump_dir: Absolute path to the host-side dump directory that is
            bind-mounted at :data:`~oracle_dmp_converter.cli.DEFAULT_CONTAINER_DUMP_PATH`
            inside the container.
        version: Session schema version; currently always ``1``.
        created_at: ISO 8601 timestamp of when the session was created.
    """

    container_name: str
    container_runtime: str
    oracle_image: str
    oracle_service: str
    work_dir: str
    dump_dir: str
    version: int = 1
    created_at: str = ""


@dataclass(frozen=True)
class ConversionPlan:
    """Conversion plan produced by the ``plan`` phase.

    Serialised to ``plan.yaml`` and consumed by the ``convert`` phase.

    Attributes:
        dump_paths: Absolute paths to the source ``.dmp`` files inside the
            container's dump directory.
        tables: Per-table conversion plans including strategy and chunk list.
        oracle_image: Docker image tag used for the Oracle Free staging
            container.
        version: Plan schema version; currently always ``1``.
        dump_format: Whether the dump is a modern Data Pump or legacy exp dump.
        container_runtime: Container runtime (``"docker"`` or ``"podman"``)
            recorded at plan time.  Defaults to ``"docker"`` for plans
            produced by older versions of the tool.
    """

    dump_paths: tuple[str, ...]
    tables: tuple[TablePlan, ...]
    oracle_image: str
    version: int = 1
    dump_format: DumpFormat = DumpFormat.DATAPUMP
    container_runtime: str = "docker"
