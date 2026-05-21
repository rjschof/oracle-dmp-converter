import sqlite3
from pathlib import Path

from oracle_dmp_converter.io.state import ChunkState, StateStore


def test_state_store_upserts_and_reads_chunk_state(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite") as store:
        store.upsert(ChunkState("SRC.EMP", "whole", "running"))
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 10, 10))
        state = store.get("SRC.EMP", "whole")
    assert state == ChunkState("SRC.EMP", "whole", "completed", 10, 10)


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
