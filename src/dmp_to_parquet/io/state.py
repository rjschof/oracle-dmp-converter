"""Small SQLite-backed resumability state."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ChunkState:
    table_name: str
    chunk_name: str
    status: str
    imported_rows: int | None = None
    parquet_rows: int | None = None
    error: str | None = None


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                table_name TEXT NOT NULL,
                chunk_name TEXT NOT NULL,
                status TEXT NOT NULL,
                imported_rows INTEGER,
                parquet_rows INTEGER,
                error TEXT,
                PRIMARY KEY (table_name, chunk_name)
            )
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def upsert(self, state: ChunkState) -> None:
        self.conn.execute(
            """
            INSERT INTO chunks(table_name, chunk_name, status, imported_rows, parquet_rows, error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name, chunk_name) DO UPDATE SET
                status = excluded.status,
                imported_rows = excluded.imported_rows,
                parquet_rows = excluded.parquet_rows,
                error = excluded.error
            """,
            (
                state.table_name,
                state.chunk_name,
                state.status,
                state.imported_rows,
                state.parquet_rows,
                state.error,
            ),
        )
        self.conn.commit()

    def get(self, table_name: str, chunk_name: str) -> ChunkState | None:
        row = self.conn.execute(
            """
            SELECT table_name, chunk_name, status, imported_rows, parquet_rows, error
            FROM chunks
            WHERE table_name = ? AND chunk_name = ?
            """,
            (table_name, chunk_name),
        ).fetchone()
        if row is None:
            return None
        return ChunkState(*row)
