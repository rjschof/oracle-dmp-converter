"""Validation helpers for Parquet output."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pyarrow.parquet as pq


def count_parquet_rows(paths: Iterable[Path]) -> int:
    total = 0
    for path in paths:
        total += pq.ParquetFile(path).metadata.num_rows  # type: ignore[no-untyped-call]
    return total
