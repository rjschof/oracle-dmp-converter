"""Validation helpers for output files produced by oracle-dmp-converter."""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path

import fastavro
import pyarrow.parquet as pq

from oracle_dmp_converter.models import OutputFormat

LOGGER = logging.getLogger(__name__)


def count_output_rows(paths: Iterable[Path], output_format: OutputFormat) -> int:
    """Return the total row count across *paths* for the given *output_format*.

    * **Parquet** — reads row-group metadata; no data decoding.
    * **Avro** — iterates all records via :mod:`fastavro`.
    * **CSV** — parses with :mod:`csv` and counts data rows.  Naively
      counting newlines is wrong for cells that contain embedded
      newlines (e.g. multi-line XML / CLOB values), which would inflate
      the apparent row count.
    """
    total = 0
    if output_format == OutputFormat.PARQUET:
        for path in paths:
            total += pq.ParquetFile(path).metadata.num_rows  # type: ignore[no-untyped-call]
    elif output_format == OutputFormat.AVRO:
        for path in paths:
            with open(path, "rb") as fh:
                reader = fastavro.reader(fh)
                total += sum(1 for _ in reader)
    elif output_format == OutputFormat.CSV:
        for path in paths:
            with open(path, encoding="utf-8", errors="replace", newline="") as fh:
                reader = csv.reader(fh)
                try:
                    next(reader)  # discard header
                except StopIteration:
                    continue
                total += sum(1 for _ in reader)
    else:
        raise ValueError(f"Unsupported output format for row counting: {output_format!r}")
    return total


# ---------------------------------------------------------------------------
# Backwards-compatibility alias
# ---------------------------------------------------------------------------


def count_parquet_rows(paths: Iterable[Path]) -> int:
    """Count rows in Parquet files.  Prefer :func:`count_output_rows`."""
    return count_output_rows(paths, OutputFormat.PARQUET)
