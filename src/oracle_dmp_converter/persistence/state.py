"""Small SQLite-backed resumability state."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
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

    def __init__(self, path: Path, *, sidecar: bool = True) -> None:
        """Open (or create) the SQLite database at *path*.

        Creates the ``chunks`` table if it does not exist and runs any pending
        migrations.  When *sidecar* is True (the default), every successful
        ``COMPLETED`` upsert also writes a small ``<chunk>.rowcount`` JSON
        sidecar in the same directory as the SQLite file, so a corrupted
        database can be reconciled by replaying the sidecar files.

        Args:
            path: Filesystem path for the SQLite database file.  Parent
                directories are created as needed.
            sidecar: Whether to emit per-chunk rowcount sidecar files.
                Disable in tests that don't need them; the converter
                default is enabled.
        """
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sidecar = sidecar
        self.conn = sqlite3.connect(path)
        # When False, ``upsert`` skips its per-call commit and sidecar write
        # so a caller can batch many upserts into one commit via
        # ``deferred_commit``.  Defaults to per-call commit for safety.
        self._autocommit = True
        # Completed states accumulated during a ``deferred_commit`` block;
        # their sidecars are written together when the block commits.
        self._deferred_completed: list[ChunkState] = []
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
        Outside a :meth:`deferred_commit` block, changes are committed
        immediately and a ``completed`` transition writes its JSON sidecar
        right away.  Inside a ``deferred_commit`` block the commit and
        sidecar writes are batched to the end of the block.

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
        if self._autocommit:
            self.conn.commit()
            if self.sidecar and state.status == "completed":
                self._write_sidecar(state)
        elif self.sidecar and state.status == "completed":
            self._deferred_completed.append(state)

    @contextmanager
    def deferred_commit(self) -> Iterator[None]:
        """Batch every :meth:`upsert` in the block into a single commit.

        Suspends per-upsert commits and sidecar writes; on clean exit
        commits once and flushes the accumulated ``completed`` sidecars,
        rolling back (and discarding the pending sidecars) on exception.
        Resumability granularity becomes per-block rather than per-chunk:
        a crash inside the block re-does the whole block's work on resume,
        which is safe because staging re-import is idempotent.
        """
        self._autocommit = False
        self._deferred_completed = []
        try:
            yield
            self.conn.commit()
            for state in self._deferred_completed:
                self._write_sidecar(state)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self._autocommit = True
            self._deferred_completed = []

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Yield the underlying connection inside a SQLite transaction.

        Commits on clean exit; rolls back on exception so a mid-write
        crash cannot leave the database half-updated.  The default
        :meth:`upsert` API does not need this — it is provided for callers
        that need to write multiple rows atomically (e.g. bulk
        reconciliation from sidecar files).
        """
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _sidecar_path(self, table_name: str, chunk_name: str) -> Path:
        """Return the on-disk sidecar path for a specific chunk."""
        sidecar_dir = self.path.parent / "sidecars"
        safe_table = table_name.replace("/", "_")
        safe_chunk = chunk_name.replace("/", "_")
        return sidecar_dir / f"{safe_table}__{safe_chunk}.rowcount"

    def _write_sidecar(self, state: ChunkState) -> None:
        """Write ``<chunks-dir>/sidecars/<table>__<chunk>.rowcount`` atomically.

        The file is written via a temp + rename so a partial write cannot
        leave behind an unreadable sidecar.  Failures here are logged at
        WARNING and never raised — the SQLite write has already succeeded,
        and the sidecar is only a belt-and-braces reconciliation aid.
        """
        path = self._sidecar_path(state.table_name, state.chunk_name)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            payload = {
                "table_name": state.table_name,
                "chunk_name": state.chunk_name,
                "status": state.status,
                "imported_rows": state.imported_rows,
                "output_rows": state.output_rows,
            }
            tmp.write_text(json.dumps(payload, sort_keys=True) + "\n")
            tmp.replace(path)
        except OSError as exc:
            LOGGER.warning(
                "Failed to write rowcount sidecar for %s.%s: %s",
                state.table_name,
                state.chunk_name,
                exc,
            )

    def reconcile_from_sidecars(self) -> int:
        """Re-import completed-chunk records from sidecar files.

        Walks the sidecar directory and inserts a ``completed`` row into
        the ``chunks`` table for any chunk that has a sidecar but no
        matching row.  Used to recover from a corrupted ``state.sqlite``.

        Returns:
            The number of records reconciled.
        """
        sidecar_dir = self.path.parent / "sidecars"
        if not sidecar_dir.exists():
            return 0
        recovered = 0
        with self.transaction() as conn:
            for sidecar_path in sidecar_dir.glob("*.rowcount"):
                try:
                    data = json.loads(sidecar_path.read_text())
                except (OSError, json.JSONDecodeError) as exc:
                    LOGGER.warning("Skipping malformed sidecar %s: %s", sidecar_path, exc)
                    continue
                existing = conn.execute(
                    "SELECT 1 FROM chunks WHERE table_name = ? AND chunk_name = ?",
                    (data["table_name"], data["chunk_name"]),
                ).fetchone()
                if existing is not None:
                    continue
                conn.execute(
                    """
                    INSERT INTO chunks(table_name, chunk_name, status,
                                       imported_rows, output_rows, error)
                    VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        data["table_name"],
                        data["chunk_name"],
                        data.get("status", "completed"),
                        data.get("imported_rows"),
                        data.get("output_rows"),
                    ),
                )
                recovered += 1
        if recovered:
            LOGGER.info("Reconciled %d chunks from sidecar files", recovered)
        return recovered

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

    def snapshot(self) -> dict[tuple[str, str], ChunkState]:
        """Return every stored chunk state in one query, keyed by chunk.

        Lets a caller fetch all states once at the start of a convert run
        instead of issuing a per-chunk :meth:`get` inside nested loops.
        """
        rows = self.conn.execute(
            """
            SELECT table_name, chunk_name, status, imported_rows, output_rows, error
            FROM chunks
            """
        ).fetchall()
        return {(row[0], row[1]): ChunkState(*row) for row in rows}
