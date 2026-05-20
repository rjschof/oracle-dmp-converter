from oracle_dmp_converter.datapump.parfile import (
    DataPumpConnection,
    ExportJob,
    ImportJob,
    render_export_parfile,
    render_import_parfile,
)


def test_export_parfile_full_dump_with_schema_include() -> None:
    text = render_export_parfile(
        ExportJob(
            connection=DataPumpConnection("system", "pw"),
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
            connection=DataPumpConnection("system", "pw"),
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
