"""Small SQLite-backed resumability state."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkState:
    table_name: str
    chunk_name: str
    status: str
    imported_rows: int | None = None
    output_rows: int | None = None
    error: str | None = None


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self._migrate()
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                table_name TEXT NOT NULL,
                chunk_name TEXT NOT NULL,
                status TEXT NOT NULL,
                imported_rows INTEGER,
                output_rows INTEGER,
                error TEXT,
                PRIMARY KEY (table_name, chunk_name)
            )
            """
        )
        self.conn.commit()

    def _migrate(self) -> None:
        """Rename legacy ``parquet_rows`` column to ``output_rows`` if present."""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'"
        )
        if cursor.fetchone() is None:
            return  # table doesn't exist yet; nothing to migrate
        col_names = {row[1] for row in self.conn.execute("PRAGMA table_info(chunks)").fetchall()}
        if "parquet_rows" in col_names and "output_rows" not in col_names:
            LOGGER.info("Migrating state.sqlite: renaming parquet_rows → output_rows")
            self.conn.execute("ALTER TABLE chunks RENAME COLUMN parquet_rows TO output_rows")
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def upsert(self, state: ChunkState) -> None:
        self.conn.execute(
            """
            INSERT INTO chunks(table_name, chunk_name, status, imported_rows, output_rows, error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name, chunk_name) DO UPDATE SET
                status = excluded.status,
                imported_rows = excluded.imported_rows,
                output_rows = excluded.output_rows,
                error = excluded.error
            """,
            (
                state.table_name,
                state.chunk_name,
                state.status,
                state.imported_rows,
                state.output_rows,
                state.error,
            ),
        )
        self.conn.commit()

    def get(self, table_name: str, chunk_name: str) -> ChunkState | None:
        row = self.conn.execute(
            """
            SELECT table_name, chunk_name, status, imported_rows, output_rows, error
            FROM chunks
            WHERE table_name = ? AND chunk_name = ?
            """,
            (table_name, chunk_name),
        ).fetchone()
        if row is None:
            return None
        return ChunkState(*row)
