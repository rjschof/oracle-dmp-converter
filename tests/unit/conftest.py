"""Shared pytest fixtures for unit tests."""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest


@pytest.fixture
def simple_arrow_schema() -> pa.Schema:
    """Minimal three-column Arrow schema used across format-writer tests."""
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("amount", pa.float64()),
        ]
    )


@pytest.fixture
def make_arrow_table() -> Callable[[pa.Schema, list[tuple]], pa.Table]:
    """Factory: build a ``pa.Table`` from a schema and a list of row tuples."""

    def _make(schema: pa.Schema, rows: list[tuple]) -> pa.Table:
        arrays = [
            pa.array([r[i] for r in rows], type=schema.field(i).type) for i in range(len(schema))
        ]
        return pa.Table.from_arrays(arrays, schema=schema)

    return _make
