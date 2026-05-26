"""Unit tests for legacy exp/imp parameter file rendering."""

from __future__ import annotations

from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyExportJob,
    LegacyImportJob,
    LegacyIndexFileJob,
    render_legacy_export_parfile,
    render_legacy_import_parfile,
    render_legacy_indexfile_parfile,
)
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _conn() -> OracleCredentials:
    return OracleCredentials(user="system", password="OraclePwd_123", service="FREEPDB1")


class TestLegacyConnection:
    def test_userid_format(self) -> None:
        conn = _conn()
        assert conn.userid == "system/OraclePwd_123@FREEPDB1"


class TestRenderLegacyExportParfile:
    def test_basic_owner_export(self) -> None:
        job = LegacyExportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="export.log",
            owner=("SRC",),
        )
        output = render_legacy_export_parfile(job)
        assert "USERID=system/OraclePwd_123@FREEPDB1" in output
        assert "FILE=/dumps/export.dmp" in output
        assert "LOG=export.log" in output
        assert "OWNER=(SRC)" in output
        assert "ROWS=Y" in output
        assert "INDEXES=N" in output
        assert "GRANTS=N" in output
        assert "COMPRESS=N" in output
        # FULL= should not appear when owner is specified
        assert "FULL=" not in output

    def test_full_export(self) -> None:
        job = LegacyExportJob(
            connection=_conn(),
            files=("/dumps/full.dmp",),
            logfile="full.log",
            full=True,
        )
        output = render_legacy_export_parfile(job)
        assert "FULL=Y" in output
        assert "OWNER=" not in output

    def test_multiple_files(self) -> None:
        job = LegacyExportJob(
            connection=_conn(),
            files=("/dumps/part1.dmp", "/dumps/part2.dmp"),
            logfile="export.log",
            owner=("SRC",),
        )
        output = render_legacy_export_parfile(job)
        assert "FILE=/dumps/part1.dmp,/dumps/part2.dmp" in output

    def test_rows_false(self) -> None:
        job = LegacyExportJob(
            connection=_conn(),
            files=("/dumps/schema.dmp",),
            logfile="schema.log",
            owner=("SRC",),
            rows=False,
        )
        output = render_legacy_export_parfile(job)
        assert "ROWS=N" in output

    def test_compress_enabled(self) -> None:
        job = LegacyExportJob(
            connection=_conn(),
            files=("/dumps/compressed.dmp",),
            logfile="compressed.log",
            owner=("SRC",),
            compress=True,
        )
        output = render_legacy_export_parfile(job)
        assert "COMPRESS=Y" in output


class TestRenderLegacyImportParfile:
    def test_metadata_only(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp-meta.log",
            fromuser="SRC",
            touser="DMP_STAGE",
            tables=("EMPLOYEES",),
            rows=False,
            indexes=False,
            grants=False,
            constraints=False,
        )
        output = render_legacy_import_parfile(job)
        assert "USERID=system/OraclePwd_123@FREEPDB1" in output
        assert "FILE=/dumps/export.dmp" in output
        assert "LOG=imp-meta.log" in output
        assert "FROMUSER=SRC" in output
        assert "TOUSER=DMP_STAGE" in output
        assert "TABLES=(EMPLOYEES)" in output
        assert "ROWS=N" in output
        assert "INDEXES=N" in output
        assert "GRANTS=N" in output
        assert "CONSTRAINTS=N" in output
        assert "TRIGGERS" not in output

    def test_full_data_import(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp-data.log",
            fromuser="SRC",
            touser="DMP_STAGE",
            tables=("ORDERS",),
            rows=True,
        )
        output = render_legacy_import_parfile(job)
        assert "ROWS=Y" in output
        assert "TABLES=(ORDERS)" in output

    def test_no_tables_filter(self) -> None:
        """No TABLES= line when tables is empty (full schema import)."""
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
        )
        output = render_legacy_import_parfile(job)
        assert "TABLES=" not in output

    def test_ignore_flag(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
            ignore=False,
        )
        output = render_legacy_import_parfile(job)
        assert "IGNORE=N" in output

    def test_data_only_default_false_emits_no_data_only_line(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
        )
        output = render_legacy_import_parfile(job)
        assert "DATA_ONLY" not in output
        assert "IGNORE=Y" in output

    def test_data_only_true_emits_data_only_y_and_suppresses_ignore(self) -> None:
        """DATA_ONLY=Y is incompatible with IGNORE=Y (IMP-00402)."""
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
            data_only=True,
        )
        output = render_legacy_import_parfile(job)
        assert "DATA_ONLY=Y" in output
        # IGNORE must be omitted entirely (even IGNORE=N would trigger IMP-00402).
        assert "IGNORE" not in output

    def test_data_only_true_with_ignore_false_still_omits_ignore(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
            ignore=False,
            data_only=True,
        )
        output = render_legacy_import_parfile(job)
        assert "DATA_ONLY=Y" in output
        assert "IGNORE" not in output

    def test_multiple_dump_files(self) -> None:
        job = LegacyImportJob(
            connection=_conn(),
            files=("/dumps/part1.dmp", "/dumps/part2.dmp"),
            logfile="imp.log",
            fromuser="SRC",
            touser="DMP_STAGE",
        )
        output = render_legacy_import_parfile(job)
        assert "FILE=/dumps/part1.dmp,/dumps/part2.dmp" in output


class TestRenderLegacyIndexfileParfile:
    def test_full_discovery(self) -> None:
        job = LegacyIndexFileJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp-indexfile.log",
            indexfile="/tmp/dmpconverter-discovery.sql",
            full=True,
        )
        output = render_legacy_indexfile_parfile(job)
        assert "USERID=system/OraclePwd_123@FREEPDB1" in output
        assert "FILE=/dumps/export.dmp" in output
        assert "LOG=imp-indexfile.log" in output
        assert "INDEXFILE=/tmp/dmpconverter-discovery.sql" in output
        assert "FULL=Y" in output
        assert "OWNER=" not in output

    def test_owner_filter(self) -> None:
        job = LegacyIndexFileJob(
            connection=_conn(),
            files=("/dumps/export.dmp",),
            logfile="imp-indexfile.log",
            indexfile="/tmp/discovery.sql",
            full=False,
            owner=("SRC", "SRC2"),
        )
        output = render_legacy_indexfile_parfile(job)
        assert "FULL=N" in output
        assert "OWNER=(SRC, SRC2)" in output
