"""Conversion report builder and serializer."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from oracle_dmp_converter.core.results import PlanConversionResult
from oracle_dmp_converter.models import (
    ChunkReport,
    ConversionPlan,
    ConversionReport,
    ConversionStatistics,
    SkippedTableReport,
    TableStrategy,
)


def build_conversion_report(
    plan: ConversionPlan,
    result: PlanConversionResult,
    output_format: str,
) -> ConversionReport:
    """Build a :class:`~oracle_dmp_converter.models.ConversionReport` from a completed run.

    Args:
        plan: The conversion plan that was executed.
        result: The aggregated conversion result.
        output_format: Output file format value, e.g. ``"parquet"``.

    Returns:
        A fully populated :class:`~oracle_dmp_converter.models.ConversionReport`.
    """
    result_by_name = {f"{r.source_schema}.{r.table}": r for r in result.tables}

    successful: list[ChunkReport] = []
    skipped: list[SkippedTableReport] = []

    for tp in plan.tables:
        if tp.strategy == TableStrategy.UNSUPPORTED:
            skipped.append(
                SkippedTableReport(
                    schema=tp.schema,
                    table=tp.table,
                    strategy=tp.strategy.value,
                    reason=tp.reason,
                )
            )
        else:
            tcr = result_by_name[tp.qualified_name]
            for chunk_result in tcr.chunks:
                successful.append(
                    ChunkReport(
                        schema=tp.schema,
                        table=tp.table,
                        chunk=chunk_result.name,
                        strategy=tp.strategy.value,
                        output_rows=chunk_result.output_rows,
                        output_path=str(chunk_result.output_path),
                    )
                )

    tables_converted = len(plan.tables) - len(skipped)
    statistics = ConversionStatistics(
        total_output_rows=sum(c.output_rows for c in successful),
        tables_total=len(plan.tables),
        tables_converted=tables_converted,
        tables_skipped=len(skipped),
    )

    return ConversionReport(
        version=1,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        started_at=result.started_at.isoformat(timespec="seconds"),
        completed_at=result.completed_at.isoformat(timespec="seconds"),
        dump_format=plan.dump_format.value,
        output_format=output_format,
        dump_paths=plan.dump_paths,
        statistics=statistics,
        successful=tuple(successful),
        skipped=tuple(skipped),
    )


def save_conversion_report(work_dir: Path, report: ConversionReport) -> None:
    """Write *report* to ``conversion_report.yaml`` and ``conversion_report.json``.

    Both files are written to *work_dir*, which is created if it does not
    already exist.

    Args:
        work_dir: Directory to write the report files into.
        report: The :class:`~oracle_dmp_converter.models.ConversionReport` to serialise.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    payload = dataclasses.asdict(report)
    (work_dir / "conversion_report.yaml").write_text(yaml.safe_dump(payload, sort_keys=False))
    (work_dir / "conversion_report.json").write_text(json.dumps(payload, indent=2) + "\n")
