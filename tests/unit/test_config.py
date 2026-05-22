"""Unit tests for config.py."""

from __future__ import annotations

from pathlib import Path

from oracle_dmp_converter.config import (
    ColumnOverride,
    ConverterConfig,
    TableOverride,
    column_override,
    dump_config,
    load_config,
    table_override,
)


class TestLoadConfig:
    def test_returns_default_when_path_is_none(self) -> None:
        cfg = load_config(None)
        assert cfg.oracle_image is None
        assert not cfg.tables
        assert not cfg.columns

    def test_loads_oracle_image(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("oracle:\n  image: my-custom-image:latest\n")
        cfg = load_config(config_file)
        assert cfg.oracle_image == "my-custom-image:latest"

    def test_loads_table_overrides(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("tables:\n  MYSCHEMA.MYTABLE:\n    strategy: whole\n")
        cfg = load_config(config_file)
        assert "MYSCHEMA.MYTABLE" in cfg.tables
        assert cfg.tables["MYSCHEMA.MYTABLE"].strategy == "whole"

    def test_loads_column_overrides(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "columns:\n"
            "  MYSCHEMA.MYTABLE.GEOM:\n"
            "    expression: SDO_UTIL.TO_WKT({column})\n"
            "    parquet_type: string\n"
        )
        cfg = load_config(config_file)
        assert "MYSCHEMA.MYTABLE.GEOM" in cfg.columns
        col = cfg.columns["MYSCHEMA.MYTABLE.GEOM"]
        assert col.expression == "SDO_UTIL.TO_WKT({column})"
        assert col.parquet_type == "string"

    def test_handles_empty_yaml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        cfg = load_config(config_file)
        assert cfg.oracle_image is None


class TestTableOverride:
    def _cfg(self) -> ConverterConfig:
        return ConverterConfig(
            tables={
                "MYSCHEMA.ORDERS": TableOverride(strategy="whole"),
            }
        )

    def test_exact_case_hit(self) -> None:
        cfg = self._cfg()
        result = table_override(cfg, "MYSCHEMA", "ORDERS")
        assert result is not None
        assert result.strategy == "whole"

    def test_upper_case_fallback(self) -> None:
        cfg = ConverterConfig(tables={"myschema.orders": TableOverride(strategy="whole")})
        result = table_override(cfg, "myschema", "orders")
        assert result is not None

    def test_returns_none_when_not_found(self) -> None:
        cfg = self._cfg()
        assert table_override(cfg, "OTHER", "TABLE") is None


class TestColumnOverride:
    def _cfg(self) -> ConverterConfig:
        return ConverterConfig(
            columns={
                "MYSCHEMA.ORDERS.GEOM": ColumnOverride(
                    expression="TO_WKT({column})", parquet_type="string"
                ),
            }
        )

    def test_exact_case_hit(self) -> None:
        cfg = self._cfg()
        result = column_override(cfg, "MYSCHEMA", "ORDERS", "GEOM")
        assert result is not None
        assert result.parquet_type == "string"

    def test_upper_case_fallback(self) -> None:
        cfg = ConverterConfig(columns={"myschema.orders.geom": ColumnOverride(expression="X")})
        result = column_override(cfg, "myschema", "orders", "geom")
        assert result is not None

    def test_returns_none_when_not_found(self) -> None:
        cfg = self._cfg()
        assert column_override(cfg, "S", "T", "C") is None


class TestDumpConfig:
    def test_round_trips_oracle_image(self) -> None:
        cfg = ConverterConfig(oracle_image="gvenzl/oracle-free:23-faststart")
        dumped = dump_config(cfg)
        assert dumped["oracle"]["image"] == "gvenzl/oracle-free:23-faststart"

    def test_uses_default_image_when_none(self) -> None:
        cfg = ConverterConfig()
        dumped = dump_config(cfg)
        assert dumped["oracle"]["image"] is not None

    def test_round_trips_table_overrides(self) -> None:
        cfg = ConverterConfig(tables={"S.T": TableOverride(strategy="whole")})
        dumped = dump_config(cfg)
        assert "S.T" in dumped["tables"]

    def test_round_trips_column_overrides(self) -> None:
        cfg = ConverterConfig(
            columns={"S.T.C": ColumnOverride(expression="X({column})", parquet_type="string")}
        )
        dumped = dump_config(cfg)
        assert "S.T.C" in dumped["columns"]
