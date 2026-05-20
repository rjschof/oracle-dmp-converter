"""Configuration loading for conversion planning."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_ORACLE_IMAGE = "gvenzl/oracle-free:23-faststart"

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableOverride:
    strategy: str | None = None


@dataclass(frozen=True)
class ColumnOverride:
    expression: str | None = None
    parquet_type: str | None = None


@dataclass(frozen=True)
class ConverterConfig:
    oracle_image: str = DEFAULT_ORACLE_IMAGE
    tables: dict[str, TableOverride] = field(default_factory=dict)
    columns: dict[str, ColumnOverride] = field(default_factory=dict)


def load_config(path: Path | None) -> ConverterConfig:
    if path is None:
        return ConverterConfig()
    data = yaml.safe_load(path.read_text()) or {}
    oracle = data.get("oracle", {})
    tables = {
        name: TableOverride(**(value or {})) for name, value in (data.get("tables") or {}).items()
    }
    columns = {
        name: ColumnOverride(**(value or {})) for name, value in (data.get("columns") or {}).items()
    }
    return ConverterConfig(
        oracle_image=oracle.get("image", DEFAULT_ORACLE_IMAGE),
        tables=tables,
        columns=columns,
    )


def table_override(config: ConverterConfig, schema: str, table: str) -> TableOverride | None:
    key = f"{schema}.{table}"
    return config.tables.get(key) or config.tables.get(key.upper())


def column_override(
    config: ConverterConfig,
    schema: str,
    table: str,
    column: str,
) -> ColumnOverride | None:
    key = f"{schema}.{table}.{column}"
    return config.columns.get(key) or config.columns.get(key.upper())


def dump_config(config: ConverterConfig) -> dict[str, Any]:
    return {
        "oracle": {
            "image": config.oracle_image,
        },
        "tables": {name: vars(value) for name, value in config.tables.items()},
        "columns": {name: vars(value) for name, value in config.columns.items()},
    }
