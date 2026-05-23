"""Unit tests for the sidecar-reconciliation behavior of StateStore.

Covers:
- A completed upsert writes a matching .rowcount JSON sidecar.
- A failed upsert does *not* write a sidecar (only completed transitions do).
- ``reconcile_from_sidecars`` re-populates an empty state.sqlite from
  the sidecar files left behind by a previous run.
- A malformed sidecar is skipped with a warning, not a hard failure.
"""

from __future__ import annotations

from pathlib import Path

from oracle_dmp_converter.persistence.state import ChunkState, StateStore


def test_completed_upsert_writes_sidecar(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite") as store:
        store.upsert(
            ChunkState(
                table_name="HR.EMPLOYEES",
                chunk_name="whole",
                status="completed",
                imported_rows=30,
                output_rows=30,
            )
        )
    sidecars = list((tmp_path / "sidecars").glob("*.rowcount"))
    assert len(sidecars) == 1
    assert sidecars[0].name == "HR.EMPLOYEES__whole.rowcount"


def test_failed_upsert_does_not_write_sidecar(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite") as store:
        store.upsert(
            ChunkState(
                table_name="HR.EMPLOYEES",
                chunk_name="whole",
                status="failed",
                error="ORA-12345",
            )
        )
    assert not (tmp_path / "sidecars").exists() or not list(
        (tmp_path / "sidecars").glob("*.rowcount")
    )


def test_reconcile_after_corrupted_database(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    with StateStore(db_path) as store:
        store.upsert(
            ChunkState(
                table_name="HR.EMPLOYEES",
                chunk_name="whole",
                status="completed",
                imported_rows=30,
                output_rows=30,
            )
        )
        store.upsert(
            ChunkState(
                table_name="FIN.ACCOUNTS",
                chunk_name="whole",
                status="completed",
                imported_rows=20,
                output_rows=20,
            )
        )
    # Simulate corruption by deleting the SQLite file (sidecars remain).
    db_path.unlink()
    with StateStore(db_path) as store2:
        recovered = store2.reconcile_from_sidecars()
        assert recovered == 2
        recovered_emp = store2.get("HR.EMPLOYEES", "whole")
        assert recovered_emp is not None
        assert recovered_emp.status == "completed"
        assert recovered_emp.output_rows == 30


def test_reconcile_skips_records_already_in_db(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    with StateStore(db_path) as store:
        store.upsert(
            ChunkState(
                table_name="HR.EMPLOYEES",
                chunk_name="whole",
                status="completed",
                imported_rows=30,
                output_rows=30,
            )
        )
        # Sidecar was written; reconciling now should be a no-op.
        recovered = store.reconcile_from_sidecars()
        assert recovered == 0


def test_reconcile_skips_malformed_sidecar(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    sidecar_dir = tmp_path / "sidecars"
    sidecar_dir.mkdir()
    (sidecar_dir / "garbage.rowcount").write_text("not json")
    with StateStore(db_path) as store:
        recovered = store.reconcile_from_sidecars()
        assert recovered == 0  # malformed sidecar skipped, no exception


def test_transaction_rolls_back_on_exception(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    with StateStore(db_path) as store:
        try:
            with store.transaction() as conn:
                conn.execute(
                    "INSERT INTO chunks(table_name, chunk_name, status) "
                    "VALUES ('A', 'B', 'completed')"
                )
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert store.get("A", "B") is None  # rollback happened


def test_sidecar_disabled_when_flag_false(tmp_path: Path) -> None:
    with StateStore(tmp_path / "state.sqlite", sidecar=False) as store:
        store.upsert(
            ChunkState(
                table_name="HR.EMPLOYEES",
                chunk_name="whole",
                status="completed",
                imported_rows=30,
                output_rows=30,
            )
        )
    assert not (tmp_path / "sidecars").exists() or not list(
        (tmp_path / "sidecars").glob("*.rowcount")
    )
