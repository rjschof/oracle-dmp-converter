"""Small SQLite-backed resumability state."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkState:
    """Persisted state record for a single conversion chunk.

    Written to SQLite by :class:`StateStore` so that interrupted conversions
    can resume from where they left off.

    Attributes:
        table_name: Fully-qualified ``SCHEMA.TABLE`` identifier.
        chunk_name: Chunk identifier, e.g. ``"whole"`` or
            ``"partition-00001-P_NORTH"``.
        status: Current processing status: ``"running"``, ``"completed"``, or
            ``"failed"``.
        imported_rows: Row count from the staging table after import; ``None``
            until the import is complete.
        output_rows: Row count written to the output file; ``None`` until the
            export is complete.
        error: Error message if *status* is ``"failed"``; ``None`` otherwise.
    """

    table_name: str
    chunk_name: str
    status: str
    imported_rows: int | None = None
    output_rows: int | None = None
    error: str | None = None


class StateStore:
    """SQLite-backed store for chunk conversion state.

    Enables resumable conversions: before processing a chunk its state is set
    to ``"running"``; on success it transitions to ``"completed"``; on failure
    to ``"failed"``.  A subsequent run can detect ``"completed"`` chunks and
    skip them.

    The database file is created at *path* (with parent directories) on
    construction.  A schema migration from the legacy ``parquet_rows`` column
    name to ``output_rows`` is applied automatically.

    Can be used as a context manager; :meth:`close` is called on exit.
    """

    def __init__(self, path: Path) -> None:
        """Open (or create) the SQLite database at *path*.

        Creates the ``chunks`` table if it does not exist and runs any pending
        migrations.

        Args:
            path: Filesystem path for the SQLite database file.  Parent
                directories are created as needed.
        """
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
        """Close the underlying SQLite connection."""
        self.conn.close()

    def __enter__(self) -> StateStore:
        """Support use as a context manager; returns ``self``.

        Returns:
            This :class:`StateStore` instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the store on context manager exit.

        Args:
            exc_type: Exception type, or ``None``.
            exc: Exception instance, or ``None``.
            tb: Traceback, or ``None``.
        """
        self.close()

    def upsert(self, state: ChunkState) -> None:
        """Insert or update a :class:`ChunkState` record.

        Uses ``INSERT … ON CONFLICT … DO UPDATE`` so that repeated calls for
        the same ``(table_name, chunk_name)`` pair replace the previous row.
        Changes are committed immediately.

        Args:
            state: The chunk state to persist.
        """
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
        """Retrieve the stored state for a specific chunk.

        Args:
            table_name: Fully-qualified ``SCHEMA.TABLE`` identifier.
            chunk_name: Chunk identifier.

        Returns:
            The persisted :class:`ChunkState`, or ``None`` if no record
            exists for the given pair.
        """
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
