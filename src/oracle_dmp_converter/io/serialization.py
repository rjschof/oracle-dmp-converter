"""Manifest and plan file serialization."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml

from oracle_dmp_converter.models import (
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
    """Serialise a :class:`~oracle_dmp_converter.models.ColumnMetadata` to a plain dict.

    Args:
        column: Column metadata to serialise.

    Returns:
        Dictionary suitable for JSON serialisation.
    """
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
    """Deserialise a :class:`~oracle_dmp_converter.models.ColumnMetadata` from a plain dict.

    Args:
        data: Dictionary as produced by :func:`column_to_dict`.

    Returns:
        Reconstructed :class:`~oracle_dmp_converter.models.ColumnMetadata`.
    """
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
    """Serialise a :class:`~oracle_dmp_converter.models.PartitionMetadata` to a plain dict.

    Args:
        partition: Partition metadata to serialise.

    Returns:
        Dictionary with ``"name"`` and ``"position"`` keys.
    """
    return {"name": partition.name, "position": partition.position}


def partition_from_dict(data: dict[str, Any]) -> PartitionMetadata:
    """Deserialise a :class:`~oracle_dmp_converter.models.PartitionMetadata` from a plain dict.

    Args:
        data: Dictionary as produced by :func:`partition_to_dict`.

    Returns:
        Reconstructed :class:`~oracle_dmp_converter.models.PartitionMetadata`.
    """
    return PartitionMetadata(name=str(data["name"]), position=int(data["position"]))


def table_metadata_to_dict(table: TableMetadata) -> dict[str, Any]:
    """Serialise a :class:`~oracle_dmp_converter.models.TableMetadata` to a plain dict.

    Args:
        table: Table metadata to serialise.

    Returns:
        Dictionary containing all fields, with nested column and partition
        dicts.
    """
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
    """Deserialise a :class:`~oracle_dmp_converter.models.TableMetadata` from a plain dict.

    Args:
        data: Dictionary as produced by :func:`table_metadata_to_dict`.

    Returns:
        Reconstructed :class:`~oracle_dmp_converter.models.TableMetadata`.
    """
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
    """Serialise a :class:`~oracle_dmp_converter.models.ChunkPlan` to a plain dict.

    Args:
        chunk: Chunk plan to serialise.

    Returns:
        Dictionary with ``"name"``, ``"strategy"``, and ``"partition_name"``
        keys.
    """
    return {
        "name": chunk.name,
        "strategy": chunk.strategy.value,
        "partition_name": chunk.partition_name,
    }


def chunk_plan_from_dict(data: dict[str, Any]) -> ChunkPlan:
    """Deserialise a :class:`~oracle_dmp_converter.models.ChunkPlan` from a plain dict.

    Args:
        data: Dictionary as produced by :func:`chunk_plan_to_dict`.

    Returns:
        Reconstructed :class:`~oracle_dmp_converter.models.ChunkPlan`.
    """
    return ChunkPlan(
        name=str(data["name"]),
        strategy=TableStrategy(str(data["strategy"])),
        partition_name=data.get("partition_name"),
    )


def table_plan_to_dict(plan: TablePlan) -> dict[str, Any]:
    """Serialise a :class:`~oracle_dmp_converter.models.TablePlan` to a plain dict.

    Args:
        plan: Table plan to serialise.

    Returns:
        Dictionary with strategy, chunk list, optional reason, warnings, and
        extra fields.
    """
    return {
        "schema": plan.schema,
        "table": plan.table,
        "strategy": plan.strategy.value,
        "chunks": [chunk_plan_to_dict(chunk) for chunk in plan.chunks],
        "reason": plan.reason,
        "warnings": list(plan.warnings),
        "extra": plan.extra,
    }


def table_plan_from_dict(data: dict[str, Any]) -> TablePlan:
    """Deserialise a :class:`~oracle_dmp_converter.models.TablePlan` from a plain dict.

    Args:
        data: Dictionary as produced by :func:`table_plan_to_dict`.

    Returns:
        Reconstructed :class:`~oracle_dmp_converter.models.TablePlan`.
    """
    return TablePlan(
        schema=str(data["schema"]),
        table=str(data["table"]),
        strategy=TableStrategy(str(data["strategy"])),
        chunks=tuple(chunk_plan_from_dict(chunk) for chunk in data.get("chunks", [])),
        reason=data.get("reason"),
        warnings=tuple(data.get("warnings", [])),
        extra=dict(data.get("extra", {})),
    )


def save_manifest(path: Path, manifest: DumpManifest) -> None:
    """Write a :class:`~oracle_dmp_converter.models.DumpManifest` to a JSON file.

    Creates parent directories as needed.

    Args:
        path: Destination file path.
        manifest: Manifest to serialise.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": manifest.version,
        "dump_format": manifest.dump_format.value,
        "dump_paths": list(manifest.dump_paths),
        "tables": [table_metadata_to_dict(table) for table in manifest.tables],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_manifest(path: Path) -> DumpManifest:
    """Load a :class:`~oracle_dmp_converter.models.DumpManifest` from a JSON file.

    Args:
        path: Path to the ``manifest.json`` file.

    Returns:
        Deserialised :class:`~oracle_dmp_converter.models.DumpManifest`.
    """
    data = json.loads(path.read_text())
    raw_format = data.get("dump_format", DumpFormat.DATAPUMP.value)
    return DumpManifest(
        version=int(data.get("version", 1)),
        dump_format=DumpFormat(raw_format),
        dump_paths=tuple(data.get("dump_paths", [])),
        tables=tuple(table_metadata_from_dict(table) for table in data.get("tables", [])),
    )


def save_plan(path: Path, plan: ConversionPlan) -> None:
    """Write a :class:`~oracle_dmp_converter.models.ConversionPlan` to a YAML file.

    Creates parent directories as needed.

    Args:
        path: Destination file path (conventionally ``plan.yaml``).
        plan: Conversion plan to serialise.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": plan.version,
        "dump_format": plan.dump_format.value,
        "dump_paths": list(plan.dump_paths),
        "oracle_image": plan.oracle_image,
        "tables": [table_plan_to_dict(table_plan) for table_plan in plan.tables],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def load_plan(path: Path) -> ConversionPlan:
    """Load a :class:`~oracle_dmp_converter.models.ConversionPlan` from a YAML file.

    Args:
        path: Path to the ``plan.yaml`` file.

    Returns:
        Deserialised :class:`~oracle_dmp_converter.models.ConversionPlan`.
    """
    data = yaml.safe_load(path.read_text()) or {}
    raw_format = data.get("dump_format", DumpFormat.DATAPUMP.value)
    return ConversionPlan(
        version=int(data.get("version", 1)),
        dump_format=DumpFormat(raw_format),
        dump_paths=tuple(data.get("dump_paths", [])),
        oracle_image=str(data["oracle_image"]),
        tables=tuple(table_plan_from_dict(plan) for plan in data.get("tables", [])),
    )
