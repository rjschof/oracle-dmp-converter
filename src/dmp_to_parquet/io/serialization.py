"""Manifest and plan file serialization."""

from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import Any

import yaml

from dmp_to_parquet.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpFormat,
    DumpManifest,
    PartitionMetadata,
    TableMetadata,
    TablePlan,
    TableStrategy,
)

LOGGER = logging.getLogger(__name__)


def column_to_dict(column: ColumnMetadata) -> dict[str, Any]:
    return {
        "name": column.name,
        "data_type": column.data_type,
        "ordinal": column.ordinal,
        "nullable": column.nullable,
        "data_precision": column.data_precision,
        "data_scale": column.data_scale,
        "char_length": column.char_length,
    }


def column_from_dict(data: dict[str, Any]) -> ColumnMetadata:
    return ColumnMetadata(
        name=str(data["name"]),
        data_type=str(data["data_type"]),
        ordinal=int(data["ordinal"]),
        nullable=bool(data.get("nullable", True)),
        data_precision=data.get("data_precision"),
        data_scale=data.get("data_scale"),
        char_length=data.get("char_length"),
    )


def partition_to_dict(partition: PartitionMetadata) -> dict[str, Any]:
    return {"name": partition.name, "position": partition.position}


def partition_from_dict(data: dict[str, Any]) -> PartitionMetadata:
    return PartitionMetadata(name=str(data["name"]), position=int(data["position"]))


def table_metadata_to_dict(table: TableMetadata) -> dict[str, Any]:
    return {
        "schema": table.schema,
        "name": table.name,
        "columns": [column_to_dict(column) for column in table.columns],
        "estimated_bytes": table.estimated_bytes,
        "row_count": table.row_count,
        "partitions": [partition_to_dict(partition) for partition in table.partitions],
        "primary_key": list(table.primary_key),
        "unique_keys": [list(key) for key in table.unique_keys],
    }


def table_metadata_from_dict(data: dict[str, Any]) -> TableMetadata:
    return TableMetadata(
        schema=str(data["schema"]),
        name=str(data["name"]),
        columns=tuple(column_from_dict(column) for column in data.get("columns", [])),
        estimated_bytes=data.get("estimated_bytes"),
        row_count=data.get("row_count"),
        partitions=tuple(
            partition_from_dict(partition) for partition in data.get("partitions", [])
        ),
        primary_key=tuple(data.get("primary_key", [])),
        unique_keys=tuple(tuple(key) for key in data.get("unique_keys", [])),
    )


def chunk_plan_to_dict(chunk: ChunkPlan) -> dict[str, Any]:
    return {
        "name": chunk.name,
        "strategy": chunk.strategy.value,
        "query": chunk.query,
        "partition_name": chunk.partition_name,
        "bucket_index": chunk.bucket_index,
        "bucket_count": chunk.bucket_count,
    }


def chunk_plan_from_dict(data: dict[str, Any]) -> ChunkPlan:
    return ChunkPlan(
        name=str(data["name"]),
        strategy=TableStrategy(str(data["strategy"])),
        query=data.get("query"),
        partition_name=data.get("partition_name"),
        bucket_index=data.get("bucket_index"),
        bucket_count=data.get("bucket_count"),
    )


def table_plan_to_dict(plan: TablePlan) -> dict[str, Any]:
    return {
        "schema": plan.schema,
        "table": plan.table,
        "strategy": plan.strategy.value,
        "chunks": [chunk_plan_to_dict(chunk) for chunk in plan.chunks],
        "split_column": plan.split_column,
        "reason": plan.reason,
        "warnings": list(plan.warnings),
        "extra": plan.extra,
    }


def table_plan_from_dict(data: dict[str, Any]) -> TablePlan:
    return TablePlan(
        schema=str(data["schema"]),
        table=str(data["table"]),
        strategy=TableStrategy(str(data["strategy"])),
        chunks=tuple(chunk_plan_from_dict(chunk) for chunk in data.get("chunks", [])),
        split_column=data.get("split_column"),
        reason=data.get("reason"),
        warnings=tuple(data.get("warnings", [])),
        extra=dict(data.get("extra", {})),
    )


def save_manifest(path: Path, manifest: DumpManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": manifest.version,
        "dump_format": manifest.dump_format.value,
        "dump_paths": list(manifest.dump_paths),
        "tables": [table_metadata_to_dict(table) for table in manifest.tables],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_manifest(path: Path) -> DumpManifest:
    data = json.loads(path.read_text())
    raw_format = data.get("dump_format", DumpFormat.DATAPUMP.value)
    return DumpManifest(
        version=int(data.get("version", 1)),
        dump_format=DumpFormat(raw_format),
        dump_paths=tuple(data.get("dump_paths", [])),
        tables=tuple(table_metadata_from_dict(table) for table in data.get("tables", [])),
    )


def save_plan(path: Path, plan: ConversionPlan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": plan.version,
        "dump_format": plan.dump_format.value,
        "dump_paths": list(plan.dump_paths),
        "oracle_image": plan.oracle_image,
        "max_stage_gb": plan.max_stage_gb,
        "tables": [table_plan_to_dict(table_plan) for table_plan in plan.tables],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def load_plan(path: Path) -> ConversionPlan:
    data = yaml.safe_load(path.read_text()) or {}
    raw_format = data.get("dump_format", DumpFormat.DATAPUMP.value)
    return ConversionPlan(
        version=int(data.get("version", 1)),
        dump_format=DumpFormat(raw_format),
        dump_paths=tuple(data.get("dump_paths", [])),
        oracle_image=str(data["oracle_image"]),
        max_stage_gb=int(data["max_stage_gb"]),
        tables=tuple(table_plan_from_dict(plan) for plan in data.get("tables", [])),
    )
