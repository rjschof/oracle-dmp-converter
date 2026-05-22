"""Result dataclasses produced by :class:`StagingExecutor`."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class ChunkConversionResult:
    """Result of converting a single chunk (one output file)."""

    name: str
    imported_rows: int
    output_rows: int
    output_path: Path


@dataclass(frozen=True)
class TableConversionResult:
    """Aggregated result of converting all chunks for one table."""

    source_schema: str
    table: str
    chunks: tuple[ChunkConversionResult, ...] = field(default_factory=tuple)

    @property
    def rows(self) -> int:
        return sum(chunk.output_rows for chunk in self.chunks)


@dataclass(frozen=True)
class PlanConversionResult:
    """Aggregated result of converting all tables in a plan."""

    tables: tuple[TableConversionResult, ...]
    started_at: datetime
    completed_at: datetime

    @property
    def rows(self) -> int:
        return sum(table.rows for table in self.tables)
