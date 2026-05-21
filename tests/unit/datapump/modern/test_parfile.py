from oracle_dmp_converter.datapump.modern.parfile import (
    BatchImportJob,
    BulkMetadataImportJob,
    ExportJob,
    ImportJob,
    render_batch_import_parfile,
    render_bulk_metadata_import_parfile,
    render_export_parfile,
    render_import_parfile,
)
from oracle_dmp_converter.oracle.conn import OracleCredentials


def test_export_parfile_full_dump_with_schema_include() -> None:
    text = render_export_parfile(
        ExportJob(
            connection=OracleCredentials("system", "pw"),
            directory="DATA_PUMP_DIR",
            dumpfile="full.dmp",
            logfile="full.log",
            include_schemas=("SRC",),
        )
    )
    assert "FULL=Y" in text
    assert "INCLUDE=SCHEMA:\"IN ('SRC')\"" in text


def test_import_parfile_partition_import() -> None:
    text = render_import_parfile(
        ImportJob(
            connection=OracleCredentials("system", "pw"),
            directory="DATA_PUMP_DIR",
            dumpfiles=("full.dmp",),
            logfile="imp.log",
            source_schema="SRC",
            table="PART_TABLE",
            remap_schema=("SRC", "DMP_STAGE"),
            partition_name="P_HIGH",
        )
    )
    assert "TABLES=SRC.PART_TABLE:P_HIGH" in text
    assert "REMAP_SCHEMA=SRC:DMP_STAGE" in text
    assert "TRANSFORM=SEGMENT_ATTRIBUTES:N" in text
    assert "EXCLUDE=INDEX" in text
    assert "QUERY" not in text


def test_batch_import_parfile_multiple_tables() -> None:
    """render_batch_import_parfile combines all specs onto one TABLES= line."""
    text = render_batch_import_parfile(
        BatchImportJob(
            connection=OracleCredentials("system", "pw"),
            directory="DATA_PUMP_DIR",
            dumpfiles=("full.dmp",),
            logfile="batch.log",
            table_specs=(
                ("SRC", "ORDERS", None),
                ("SRC", "PRODUCTS", None),
                ("SRC", "EVENTS", "P_2024"),
            ),
            remap_schemas=(("SRC", "DMP_SRC"),),
            content="DATA_ONLY",
        )
    )
    assert "TABLES=SRC.ORDERS, SRC.PRODUCTS, SRC.EVENTS:P_2024" in text
    assert "REMAP_SCHEMA=SRC:DMP_SRC" in text
    assert "CONTENT=DATA_ONLY" in text
    assert "TRANSFORM=SEGMENT_ATTRIBUTES:N" in text
    assert "EXCLUDE=INDEX" in text


def test_batch_import_parfile_multiple_remap_schemas() -> None:
    """One REMAP_SCHEMA line is emitted per distinct schema pair."""
    text = render_batch_import_parfile(
        BatchImportJob(
            connection=OracleCredentials("system", "pw"),
            directory="DATA_PUMP_DIR",
            dumpfiles=("full.dmp",),
            logfile="batch.log",
            table_specs=(
                ("HR", "EMPLOYEES", None),
                ("FIN", "INVOICES", None),
            ),
            remap_schemas=(("HR", "DMP_HR"), ("FIN", "DMP_FIN")),
            content="DATA_ONLY",
        )
    )
    assert "REMAP_SCHEMA=HR:DMP_HR" in text
    assert "REMAP_SCHEMA=FIN:DMP_FIN" in text
    # Both tables on the single TABLES= line.
    assert "HR.EMPLOYEES" in text
    assert "FIN.INVOICES" in text


def test_bulk_metadata_import_parfile_no_tables_line() -> None:
    """render_bulk_metadata_import_parfile must not contain a TABLES= line."""
    text = render_bulk_metadata_import_parfile(
        BulkMetadataImportJob(
            connection=OracleCredentials("system", "pw"),
            directory="DATA_PUMP_DIR",
            dumpfiles=("full.dmp",),
            logfile="bulk-meta.log",
            remap_schema=("SRC", "DMP_SRC"),
        )
    )
    assert "TABLES=" not in text
    assert "REMAP_SCHEMA=SRC:DMP_SRC" in text
    assert "CONTENT=METADATA_ONLY" in text
    assert "TABLE_EXISTS_ACTION=REPLACE" in text
    assert "TRANSFORM=SEGMENT_ATTRIBUTES:N" in text
    assert "EXCLUDE=INDEX" in text
