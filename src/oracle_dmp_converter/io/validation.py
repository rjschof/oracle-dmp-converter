"""Validation helpers for output files produced by oracle-dmp-converter."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

import pyarrow.parquet as pq

from oracle_dmp_converter.models import OutputFormat

LOGGER = logging.getLogger(__name__)


def count_output_rows(paths: Iterable[Path], output_format: OutputFormat) -> int:
    """Return the total row count across *paths* for the given *output_format*.

    * **Parquet** — reads row-group metadata; no data decoding.
    * **Avro** — iterates all records via :mod:`fastavro`.
    * **CSV** — counts newlines minus one header line per file.
    """
    total = 0
    if output_format == OutputFormat.PARQUET:
        for path in paths:
            total += pq.ParquetFile(path).metadata.num_rows  # type: ignore[no-untyped-call]
    elif output_format == OutputFormat.AVRO:
        import fastavro

        for path in paths:
            with open(path, "rb") as fh:
                reader = fastavro.reader(fh)
                total += sum(1 for _ in reader)
    elif output_format == OutputFormat.CSV:
        for path in paths:
            text = path.read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            # Subtract 1 for the header row; guard against completely empty files.
            total += max(0, len(lines) - 1)
    else:
        raise ValueError(f"Unsupported output format for row counting: {output_format!r}")
    return total


# ---------------------------------------------------------------------------
# Backwards-compatibility alias
# ---------------------------------------------------------------------------


def count_parquet_rows(paths: Iterable[Path]) -> int:
    """Count rows in Parquet files.  Prefer :func:`count_output_rows`."""
    return count_output_rows(paths, OutputFormat.PARQUET)
