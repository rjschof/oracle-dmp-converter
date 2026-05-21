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
    """Per-table conversion strategy override from the config file.

    Attributes:
        strategy: Desired conversion strategy name.  Only ``"whole"`` is
            currently supported; any other value causes the table to be marked
            ``UNSUPPORTED`` with a descriptive reason.
    """

    strategy: str | None = None


@dataclass(frozen=True)
class ColumnOverride:
    """Per-column export expression and type override from the config file.

    Attributes:
        expression: SQL expression template used in place of the bare column
            reference when importing from the staging schema.  Use
            ``{column}`` as a placeholder for the quoted column name, e.g.
            ``"SDO_UTIL.TO_WKTGEOMETRY({column})"``.
        parquet_type: Arrow/Parquet type name to use for this column,
            overriding the default Oracle-to-Arrow type mapping (e.g.
            ``"string"`` for a geometry column serialised as WKT).
    """

    expression: str | None = None
    parquet_type: str | None = None


@dataclass(frozen=True)
class ConverterConfig:
    """Runtime configuration for the oracle-dmp-converter.

    Populated by :func:`load_config` from a YAML config file; defaults are
    used when no config file is supplied.

    Attributes:
        oracle_image: Docker image tag for the Oracle Free staging container.
            ``None`` when not explicitly set in the config file; the caller
            should fall back to :data:`DEFAULT_ORACLE_IMAGE` or a value from
            the inspection manifest.
        tables: Mapping from ``"SCHEMA.TABLE"`` keys to
            :class:`TableOverride` instances.  Lookups are tried first
            case-exact, then upper-cased.
        columns: Mapping from ``"SCHEMA.TABLE.COLUMN"`` keys to
            :class:`ColumnOverride` instances.  Same case-insensitive lookup
            semantics as ``tables``.
    """

    oracle_image: str | None = None
    tables: dict[str, TableOverride] = field(default_factory=dict)
    columns: dict[str, ColumnOverride] = field(default_factory=dict)


def load_config(path: Path | None) -> ConverterConfig:
    """Load a :class:`ConverterConfig` from a YAML file, or return defaults.

    The YAML schema mirrors the :class:`ConverterConfig` dataclass.  Unknown
    keys are silently ignored.  If *path* is ``None`` a default
    :class:`ConverterConfig` is returned without reading any file.

    Args:
        path: Path to the YAML config file, or ``None`` to use defaults.

    Returns:
        A fully populated :class:`ConverterConfig` instance.
    """
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
        oracle_image=oracle.get("image"),
        tables=tables,
        columns=columns,
    )


def table_override(config: ConverterConfig, schema: str, table: str) -> TableOverride | None:
    """Return the :class:`TableOverride` for a given table, if any.

    The lookup is performed first with the exact ``SCHEMA.TABLE`` key, then
    with the fully upper-cased key.

    Args:
        config: The active :class:`ConverterConfig`.
        schema: Oracle schema name.
        table: Oracle table name.

    Returns:
        The matching :class:`TableOverride`, or ``None`` if the table has no
        override configured.
    """
    key = f"{schema}.{table}"
    return config.tables.get(key) or config.tables.get(key.upper())


def column_override(
    config: ConverterConfig,
    schema: str,
    table: str,
    column: str,
) -> ColumnOverride | None:
    """Return the :class:`ColumnOverride` for a given column, if any.

    The lookup is performed first with the exact ``SCHEMA.TABLE.COLUMN`` key,
    then with the fully upper-cased key.

    Args:
        config: The active :class:`ConverterConfig`.
        schema: Oracle schema name.
        table: Oracle table name.
        column: Oracle column name.

    Returns:
        The matching :class:`ColumnOverride`, or ``None`` if the column has
        no override configured.
    """
    key = f"{schema}.{table}.{column}"
    return config.columns.get(key) or config.columns.get(key.upper())


def dump_config(config: ConverterConfig) -> dict[str, Any]:
    """Serialise a :class:`ConverterConfig` to a plain dictionary.

    The returned structure mirrors the YAML schema accepted by
    :func:`load_config` and can be written directly to a config file.

    Args:
        config: The :class:`ConverterConfig` instance to serialise.

    Returns:
        A dictionary with ``"oracle"``, ``"tables"``, and ``"columns"`` keys.
    """
    return {
        "oracle": {
            "image": config.oracle_image or DEFAULT_ORACLE_IMAGE,
        },
        "tables": {name: vars(value) for name, value in config.tables.items()},
        "columns": {name: vars(value) for name, value in config.columns.items()},
    }
