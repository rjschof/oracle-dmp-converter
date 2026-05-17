"""Command-line interface."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import click
from rich.console import Console

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig, load_config
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.docker_oracle import DockerOracle, docker_available
from oracle_dmp_converter.errors import LegacyDumpError
from oracle_dmp_converter.io.serialization import load_manifest, load_plan, save_manifest, save_plan
from oracle_dmp_converter.io.state import StateStore
from oracle_dmp_converter.models import ConversionPlan, OutputFormat
from oracle_dmp_converter.oracle.conn import create_directory, oracle_connection
from oracle_dmp_converter.planner import plan_tables

LOGGER = logging.getLogger(__name__)

console = Console()
DEFAULT_DUMP_DIRECTORY = "ORACLE_DMC_DUMP"
DEFAULT_CONTAINER_DUMP_PATH = "/dumps"


@click.group()
def main() -> None:
    """Convert Oracle Data Pump dumps to Parquet, Avro, or CSV."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


@main.command()
def doctor() -> None:
    """Check local runtime prerequisites."""

    if docker_available():
        console.print("[green]Docker is available[/green]")
    else:
        raise click.ClickException("Docker is not available")
    console.print("[green]Python dependencies are importable[/green]")


def _dump_paths_or_plan_dump_paths(
    dump_paths: tuple[Path, ...],
    plan_dump_paths: tuple[str, ...] = (),
) -> tuple[Path, ...]:
    if dump_paths:
        return tuple(path.resolve() for path in dump_paths)
    return tuple(Path(path).resolve() for path in plan_dump_paths)


def _validate_dump_paths(dump_paths: tuple[Path, ...]) -> tuple[Path, tuple[str, ...]]:
    if not dump_paths:
        raise click.ClickException("At least one --dump is required")
    dump_dirs = {path.parent.resolve() for path in dump_paths}
    if len(dump_dirs) != 1:
        raise click.ClickException("All dump files must be in the same directory")
    dump_dir = next(iter(dump_dirs))
    return dump_dir, tuple(path.name for path in dump_paths)


def _admin_for_container(container: DockerOracle, password: str) -> OracleAdminConnection:
    return OracleAdminConnection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=password,
    )


def _create_dump_directory(admin: OracleAdminConnection) -> None:
    with oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    ) as conn:
        create_directory(conn, DEFAULT_DUMP_DIRECTORY, DEFAULT_CONTAINER_DUMP_PATH)


def _build_converter(
    *,
    container: DockerOracle,
    admin: OracleAdminConnection,
    work_dir: Path,
    dumpfiles: tuple[str, ...],
    output_format: OutputFormat = OutputFormat.PARQUET,
    config: ConverterConfig = ConverterConfig(),
) -> OracleDumpConverter:
    return OracleDumpConverter(
        container=container,
        admin=admin,
        work_dir=work_dir,
        dumpfiles=dumpfiles,
        directory=DEFAULT_DUMP_DIRECTORY,
        directory_path=DEFAULT_CONTAINER_DUMP_PATH,
        output_format=output_format,
        config=config,
    )


@main.command()
@click.option(
    "--dump",
    "dump_paths",
    multiple=True,
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--work-dir",
    type=click.Path(path_type=Path),
    default=Path("work"),
    show_default=True,
)
@click.option(
    "--output",
    "manifest_path",
    type=click.Path(path_type=Path),
    default=Path("work/manifest.json"),
    show_default=True,
)
@click.option(
    "--oracle-image",
    default=lambda: os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE),
    show_default="gvenzl/oracle-free:23-faststart",
)
@click.option("--oracle-password", default="OraclePwd_123", show_default=True)
def inspect(
    dump_paths: tuple[Path, ...],
    work_dir: Path,
    manifest_path: Path,
    oracle_image: str,
    oracle_password: str,
) -> None:
    """Inspect a full Data Pump dump and write a manifest."""

    dump_dir, dumpfiles = _validate_dump_paths(dump_paths)
    with DockerOracle.start(
        image=oracle_image,
        password=oracle_password,
        mounts=((dump_dir, DEFAULT_CONTAINER_DUMP_PATH, "rw"),),
    ) as container:
        console.print(f"Started Oracle container [bold]{container.name}[/bold]")
        container.wait_ready()
        admin = _admin_for_container(container, oracle_password)
        _create_dump_directory(admin)
        converter = _build_converter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=dumpfiles,
        )
        manifest = converter.inspect_dump()
        manifest = type(manifest)(
            dump_paths=tuple(str(path.resolve()) for path in dump_paths),
            tables=manifest.tables,
            version=manifest.version,
            dump_format=manifest.dump_format,
        )
        save_manifest(manifest_path, manifest)
        console.print(f"[green]Wrote manifest with {len(manifest.tables)} tables[/green]")


@main.command("plan")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output",
    "plan_path",
    type=click.Path(path_type=Path),
    default=Path("work/plan.yaml"),
    show_default=True,
)
def plan_command(
    manifest_path: Path,
    config_path: Path | None,
    plan_path: Path,
) -> None:
    """Build a conversion plan from an inspection manifest."""

    manifest = load_manifest(manifest_path)
    config = load_config(config_path)
    table_plans = plan_tables(manifest.tables, config)
    plan = ConversionPlan(
        dump_paths=manifest.dump_paths,
        tables=table_plans,
        oracle_image=config.oracle_image,
        max_stage_gb=config.max_stage_gb,
        dump_format=manifest.dump_format,
    )
    save_plan(plan_path, plan)
    unsupported = [table for table in table_plans if table.reason]
    console.print(
        f"[green]Wrote plan for {len(table_plans)} tables ({len(unsupported)} unsupported)[/green]"
    )


@main.command("convert")
@click.option("--plan", "plan_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--dump",
    "dump_paths",
    multiple=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "output_dir", type=click.Path(path_type=Path), required=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["parquet", "avro", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Output file format.",
)
@click.option(
    "--work-dir",
    type=click.Path(path_type=Path),
    default=Path("work"),
    show_default=True,
)
@click.option(
    "--oracle-image",
    default=lambda: os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE),
    show_default="gvenzl/oracle-free:23-faststart",
)
@click.option("--oracle-password", default="OraclePwd_123", show_default=True)
def convert(
    plan_path: Path | None,
    dump_paths: tuple[Path, ...],
    config_path: Path | None,
    output_dir: Path,
    output_format: str,
    work_dir: Path,
    oracle_image: str,
    oracle_password: str,
) -> None:
    """Convert all tables in a plan, or inspect/plan/convert in one command."""

    fmt = OutputFormat(output_format.lower())
    config = load_config(config_path)

    if plan_path:
        plan = load_plan(plan_path)
        resolved_dump_paths = _dump_paths_or_plan_dump_paths(dump_paths, plan.dump_paths)
    else:
        resolved_dump_paths = _dump_paths_or_plan_dump_paths(dump_paths)
        plan = None
        oracle_image = config.oracle_image if oracle_image == DEFAULT_ORACLE_IMAGE else oracle_image

    dump_dir, dumpfiles = _validate_dump_paths(resolved_dump_paths)
    image = plan.oracle_image if plan else oracle_image

    with DockerOracle.start(
        image=image,
        password=oracle_password,
        mounts=((dump_dir, DEFAULT_CONTAINER_DUMP_PATH, "rw"),),
    ) as container:
        console.print(f"Started Oracle container [bold]{container.name}[/bold]")
        container.wait_ready()
        admin = _admin_for_container(container, oracle_password)
        _create_dump_directory(admin)
        converter = _build_converter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=dumpfiles,
            output_format=fmt,
            config=config,
        )
        if plan is None:
            manifest = converter.inspect_dump()
            plan = ConversionPlan(
                dump_paths=tuple(str(path) for path in resolved_dump_paths),
                tables=plan_tables(manifest.tables, config),
                oracle_image=image,
                max_stage_gb=config.max_stage_gb,
                dump_format=manifest.dump_format,
            )
            save_manifest(work_dir / "manifest.json", manifest)
            save_plan(work_dir / "plan.yaml", plan)
        converter.dump_format = plan.dump_format
        state_store = StateStore(work_dir / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, output_dir, state_store)
        finally:
            state_store.close()
        console.print(
            f"[green]Converted {result.rows} rows across {len(result.tables)} tables[/green]"
        )


@main.command("convert-hash-table")
@click.option(
    "--dump",
    "dump_paths",
    multiple=True,
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--source-schema", required=True)
@click.option("--table", "table_name", required=True)
@click.option("--split-column", required=True)
@click.option("--buckets", default=64, show_default=True, type=int)
@click.option("--output", "output_dir", type=click.Path(path_type=Path), required=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["parquet", "avro", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Output file format.",
)
@click.option(
    "--work-dir",
    type=click.Path(path_type=Path),
    default=Path("work"),
    show_default=True,
)
@click.option(
    "--oracle-image",
    default=lambda: os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE),
    show_default="gvenzl/oracle-free:23-faststart",
)
@click.option("--oracle-password", default="OraclePwd_123", show_default=True)
@click.option(
    "--null-bucket/--no-null-bucket",
    "include_null_bucket",
    default=True,
    show_default=True,
    help=(
        "Include an extra import pass for NULL values in the split column. "
        "Safe to disable with --no-null-bucket when the split column is NOT NULL."
    ),
)
def convert_hash_table(
    dump_paths: tuple[Path, ...],
    source_schema: str,
    table_name: str,
    split_column: str,
    buckets: int,
    output_dir: Path,
    output_format: str,
    work_dir: Path,
    oracle_image: str,
    oracle_password: str,
    include_null_bucket: bool,
) -> None:
    """Convert one table from a dump using Data Pump QUERY hash buckets."""

    if buckets < 1:
        raise click.ClickException("--buckets must be at least 1")
    fmt = OutputFormat(output_format.lower())
    dump_dir, dumpfiles = _validate_dump_paths(dump_paths)

    with DockerOracle.start(
        image=oracle_image,
        password=oracle_password,
        mounts=((dump_dir, "/dumps", "rw"),),
    ) as container:
        console.print(f"Started Oracle container [bold]{container.name}[/bold]")
        container.wait_ready()
        port = container.mapped_port()
        admin = OracleAdminConnection(
            host="localhost",
            port=port,
            service=container.service,
            user="system",
            password=oracle_password,
        )
        with oracle_connection(
            host=admin.host,
            port=admin.port,
            service=admin.service,
            user=admin.user,
            password=admin.password,
        ) as conn:
            create_directory(conn, DEFAULT_DUMP_DIRECTORY, DEFAULT_CONTAINER_DUMP_PATH)
        converter = OracleDumpConverter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=dumpfiles,
            directory=DEFAULT_DUMP_DIRECTORY,
            directory_path=DEFAULT_CONTAINER_DUMP_PATH,
            output_format=fmt,
        )
        try:
            result = converter.convert_hash_table(
                source_schema=source_schema,
                table=table_name,
                split_column=split_column,
                buckets=buckets,
                output_dir=output_dir,
                include_null_bucket=include_null_bucket,
            )
        except LegacyDumpError as exc:
            raise click.ClickException(str(exc)) from exc
        console.print(
            f"[green]Converted {result.rows} rows from {source_schema}.{table_name}[/green]"
        )
