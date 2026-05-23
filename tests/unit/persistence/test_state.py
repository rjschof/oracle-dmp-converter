import sqlite3
from pathlib import Path

import pytest

from oracle_dmp_converter.persistence.state import ChunkState, StateStore


def test_state_store_upserts_and_reads_chunk_state(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite") as store:
        store.upsert(ChunkState("SRC.EMP", "whole", "running"))
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 10, 10))
        state = store.get("SRC.EMP", "whole")
    assert state == ChunkState("SRC.EMP", "whole", "completed", 10, 10)


def test_state_store_get_returns_none_for_missing_chunk(tmp_path: Path) -> None:
    """get() returns None when no record exists for the given (table, chunk) pair."""
    with StateStore(tmp_path / "state.sqlite") as store:
        result = store.get("SRC.EMP", "nonexistent")
    assert result is None


def test_state_store_reopen_skips_migration_for_new_schema(tmp_path: Path) -> None:
    """Re-opening an already-migrated DB leaves the schema unchanged (branch 93→exit)."""
    db_path = tmp_path / "state.sqlite"
    # First open: creates the new-schema DB
    with StateStore(db_path) as store:
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 5, 5))
    # Second open: migration code runs but finds no parquet_rows column → no-op
    with StateStore(db_path) as store:
        state = store.get("SRC.EMP", "whole")
    assert state is not None
    assert state.output_rows == 5


def test_state_store_migrates_legacy_parquet_rows_column(tmp_path: Path) -> None:
    """A legacy state.sqlite with parquet_rows should be transparently migrated."""
    db_path = tmp_path / "state.sqlite"
    # Create an old-style DB with parquet_rows column.
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE chunks (
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
    conn.execute(
        "INSERT INTO chunks VALUES (?, ?, ?, ?, ?, ?)",
        ("SRC.EMP", "whole", "completed", 5, 5, None),
    )
    conn.commit()
    conn.close()

    # Opening via StateStore should migrate transparently.
    with StateStore(db_path) as store:
        state = store.get("SRC.EMP", "whole")
    assert state is not None
    assert state.output_rows == 5


# ---------------------------------------------------------------------------
# T1.3 — snapshot() returns all states in one query
# ---------------------------------------------------------------------------


def test_snapshot_returns_all_states_keyed_by_chunk(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite", sidecar=False) as store:
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 10, 10))
        store.upsert(ChunkState("SRC.DEPT", "partition-1", "running"))
        snap = store.snapshot()

    assert snap == {
        ("SRC.EMP", "whole"): ChunkState("SRC.EMP", "whole", "completed", 10, 10),
        ("SRC.DEPT", "partition-1"): ChunkState("SRC.DEPT", "partition-1", "running"),
    }


def test_snapshot_empty_when_no_rows(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite", sidecar=False) as store:
        assert store.snapshot() == {}


# ---------------------------------------------------------------------------
# T1.2 — deferred_commit batches commits and sidecar writes
# ---------------------------------------------------------------------------


def test_deferred_commit_defers_until_block_exit(tmp_path: Path) -> None:
    """Rows upserted inside the block are not visible to another connection until exit."""
    db_path = tmp_path / "state.sqlite"
    with StateStore(db_path, sidecar=False) as store:
        with store.deferred_commit():
            store.upsert(ChunkState("SRC.EMP", "whole", "completed", 5, 5))
            # A separate connection should NOT see the uncommitted row yet.
            other = sqlite3.connect(db_path)
            try:
                pre = other.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
            finally:
                other.close()
            assert pre == 0
        # After the block exits the commit has happened.
        other = sqlite3.connect(db_path)
        try:
            post = other.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        finally:
            other.close()
    assert post == 1


def test_deferred_commit_rolls_back_on_exception(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    with StateStore(db_path, sidecar=False) as store:
        with pytest.raises(RuntimeError):
            with store.deferred_commit():
                store.upsert(ChunkState("SRC.EMP", "whole", "completed", 5, 5))
                raise RuntimeError("boom")
        # The upsert was rolled back.
        assert store.get("SRC.EMP", "whole") is None
        # Autocommit is restored for subsequent calls.
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 7, 7))
        assert store.get("SRC.EMP", "whole") is not None


def test_deferred_commit_batches_sidecar_writes(tmp_path: Path) -> None:
    """Completed sidecars are written only when the deferred block commits."""
    db_path = tmp_path / "state.sqlite"
    sidecar_dir = db_path.parent / "sidecars"
    with StateStore(db_path, sidecar=True) as store:
        with store.deferred_commit():
            store.upsert(ChunkState("SRC.EMP", "whole", "completed", 5, 5))
            # No sidecar yet — write is deferred to block exit.
            assert not (sidecar_dir.exists() and list(sidecar_dir.glob("*.rowcount")))
        # After exit the sidecar exists.
        assert list(sidecar_dir.glob("*.rowcount"))
