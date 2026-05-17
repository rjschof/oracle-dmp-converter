from oracle_dmp_converter.io.state import ChunkState, StateStore


def test_state_store_upserts_and_reads_chunk_state(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "state.sqlite")
    try:
        store.upsert(ChunkState("SRC.EMP", "whole", "running"))
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 10, 10))
        state = store.get("SRC.EMP", "whole")
    finally:
        store.close()
    assert state == ChunkState("SRC.EMP", "whole", "completed", 10, 10)


def test_state_store_migrates_legacy_parquet_rows_column(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A legacy state.sqlite with parquet_rows should be transparently migrated."""
    import sqlite3

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
    store = StateStore(db_path)
    try:
        state = store.get("SRC.EMP", "whole")
    finally:
        store.close()
    assert state is not None
    assert state.output_rows == 5

