from dmp_to_parquet.state import ChunkState, StateStore


def test_state_store_upserts_and_reads_chunk_state(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "state.sqlite")
    try:
        store.upsert(ChunkState("SRC.EMP", "whole", "running"))
        store.upsert(ChunkState("SRC.EMP", "whole", "completed", 10, 10))
        state = store.get("SRC.EMP", "whole")
    finally:
        store.close()
    assert state == ChunkState("SRC.EMP", "whole", "completed", 10, 10)
