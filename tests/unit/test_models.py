"""Unit tests for models.py — uncovered methods."""

from __future__ import annotations

from oracle_dmp_converter.models import ColumnMetadata, TableMetadata


def _col(name: str) -> ColumnMetadata:
    return ColumnMetadata(name=name, data_type="VARCHAR2", ordinal=1)


def _table(*col_names: str) -> TableMetadata:
    return TableMetadata(
        schema="S",
        name="T",
        columns=tuple(_col(n) for n in col_names),
    )


class TestTableMetadataQualifiedName:
    def test_returns_schema_dot_table(self) -> None:
        meta = _table()
        assert meta.qualified_name == "S.T"


class TestTableMetadataColumnLookup:
    def test_finds_exact_case(self) -> None:
        meta = _table("ID", "NAME")
        col = meta.column("ID")
        assert col is not None
        assert col.name == "ID"

    def test_finds_case_insensitive(self) -> None:
        meta = _table("id", "name")
        col = meta.column("ID")
        assert col is not None
        assert col.name == "id"

    def test_returns_none_when_not_found(self) -> None:
        meta = _table("ID")
        assert meta.column("MISSING") is None
