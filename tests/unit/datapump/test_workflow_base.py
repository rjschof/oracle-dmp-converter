"""Unit tests for datapump/_workflow_base.py default import_chunks_batch."""
# pylint: disable=protected-access

from __future__ import annotations

from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow
from oracle_dmp_converter.models import DumpFormat


class _ConcreteWorkflow(DumpWorkflow):
    """Minimal concrete subclass to test the base class default."""

    @property
    def dump_format(self) -> DumpFormat:
        return DumpFormat.DATAPUMP

    def discover_tables(self):
        return ()

    def required_tablespaces(self):
        return frozenset()

    def import_all_metadata(self, source_schema, stage_schema):
        pass

    def import_metadata(self, source_schema, stage_schema, table):
        pass

    def import_chunk(self, source_schema, stage_schema, table, chunk_name, partition_name):
        self._recorded.append((source_schema, stage_schema, table, chunk_name, partition_name))

    def __init__(self):
        self._recorded = []


class TestDefaultImportChunksBatch:
    def test_falls_back_to_individual_import_chunks(self) -> None:
        wf = _ConcreteWorkflow()
        chunks = [
            ("S1", "STAGE1", "T1", "whole", None),
            ("S2", "STAGE2", "T2", "whole", "P1"),
        ]
        wf.import_chunks_batch(chunks)
        assert wf._recorded == chunks
