"""Shared metadata and planning models."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

LOGGER = logging.getLogger(__name__)


class TableStrategy(StrEnum):
    WHOLE_TABLE = "whole_table"
    PARTITION = "partition"
    HASH = "hash"
    RANGE = "range"
    UNSUPPORTED = "unsupported"


class DumpFormat(StrEnum):
    DATAPUMP = "datapump"
    LEGACY = "legacy"


@dataclass(frozen=True)
class ColumnMetadata:
    name: str
    data_type: str
    ordinal: int
    nullable: bool = True
    data_precision: int | None = None
    data_scale: int | None = None
    char_length: int | None = None

    @property
    def normalized_type(self) -> str:
        return self.data_type.upper()

    @property
    def is_hash_candidate(self) -> bool:
        unsupported = {
            "BFILE",
            "BLOB",
            "CLOB",
            "LONG",
            "LONG RAW",
            "NCLOB",
            "RAW",
            "ROWID",
            "UROWID",
            "XMLTYPE",
        }
        if self.normalized_type in unsupported:
            return False
        return True


@dataclass(frozen=True)
class PartitionMetadata:
    name: str
    position: int


@dataclass(frozen=True)
class TableMetadata:
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
        return f"{self.schema}.{self.name}"

    def column(self, name: str) -> ColumnMetadata | None:
        for column in self.columns:
            if column.name == name or column.name.upper() == name.upper():
                return column
        return None


@dataclass(frozen=True)
class ChunkPlan:
    name: str
    strategy: TableStrategy
    query: str | None = None
    partition_name: str | None = None
    bucket_index: int | None = None
    bucket_count: int | None = None


@dataclass(frozen=True)
class TablePlan:
    schema: str
    table: str
    strategy: TableStrategy
    chunks: tuple[ChunkPlan, ...] = ()
    split_column: str | None = None
    reason: str | None = None
    warnings: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass(frozen=True)
class DumpManifest:
    dump_paths: tuple[str, ...]
    tables: tuple[TableMetadata, ...]
    version: int = 1
    dump_format: DumpFormat = DumpFormat.DATAPUMP


@dataclass(frozen=True)
class ConversionPlan:
    dump_paths: tuple[str, ...]
    tables: tuple[TablePlan, ...]
    oracle_image: str
    max_stage_gb: int
    version: int = 1
    dump_format: DumpFormat = DumpFormat.DATAPUMP
