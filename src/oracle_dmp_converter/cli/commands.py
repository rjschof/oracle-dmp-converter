"""Click commands: doctor / inspect / plan / convert."""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from pathlib import Path

import click

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, load_config
from oracle_dmp_converter.converter import OracleDMPConverter
from oracle_dmp_converter.models import ConversionPlan, OutputFormat
from oracle_dmp_converter.persistence.serialization import load_manifest, save_plan
from oracle_dmp_converter.planner import plan_tables
from oracle_dmp_converter.runtime.container_oracle import (
    DEFAULT_CONTAINER_RUNTIME,
    _podman_socket_url,
    docker_available,
)
from oracle_dmp_converter.runtime.session import cleanup_stale_session, session_path_for
from oracle_dmp_converter.settings import ConverterSettings

LOGGER = logging.getLogger(__name__)

_DEFAULT_PASSWORD = "OraclePwd_123"


def _default_runtime() -> str:
    return os.environ.get("DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME)


def _default_image() -> str:
    return os.environ.get("DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE)


def _container_options(func: Callable) -> Callable:
    func = click.option(
        "--container-runtime",
        default=None,
        show_default="docker",
        help="Container runtime to use (docker or podman).",
    )(func)
    func = click.option("--oracle-password", default=_DEFAULT_PASSWORD, show_default=True)(func)
    func = click.option(
        "--oracle-image",
        default=_default_image,
        show_default="gvenzl/oracle-free:23-faststart",
    )(func)
    func = click.option(
        "--userns-mode",
        default=None,
        show_default=False,
        help=(
            "User-namespace mode passed to the container runtime "
            "(e.g. 'keep-id' for rootless Podman). "
            "Most rootless Podman setups do not need this."
        ),
    )(func)
    return func


def _dump_option(func: Callable) -> Callable:
    return click.option(
        "--dump",
        "dump_paths",
        multiple=True,
        type=click.Path(exists=True, path_type=Path),
        required=True,
    )(func)


def _work_dir_option(func: Callable) -> Callable:
    return click.option(
        "--work-dir",
        type=click.Path(path_type=Path),
        default=Path("work"),
        show_default=True,
    )(func)


def _build_settings(
    *,
    dump_paths: tuple[Path, ...],
    work_dir: Path,
    oracle_image: str,
    oracle_password: str,
    container_runtime: str,
    userns_mode: str | None = None,
    output_dir: Path | None = None,
    output_format: OutputFormat = OutputFormat.PARQUET,
    config=None,
    keep_alive: bool = False,
) -> ConverterSettings:
    try:
        return ConverterSettings(
            dump_paths=tuple(p.resolve() for p in dump_paths),
            oracle_password=oracle_password,
            work_dir=work_dir,
            output_dir=output_dir,
            output_format=output_format,
            oracle_image=oracle_image,
            container_runtime=container_runtime,
            userns_mode=userns_mode,
            config=config,
            keep_alive=keep_alive,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _selinux_enforcing() -> bool:
    """Return ``True`` if SELinux is currently in Enforcing mode."""
    try:
        result = subprocess.run(["getenforce"], capture_output=True, text=True, check=False)
        return result.returncode == 0 and result.stdout.strip().lower() == "enforcing"
    except OSError:
        return False


@click.command()
@click.option(
    "--container-runtime",
    default=_default_runtime,
    show_default="docker",
    help="Container runtime to use (docker or podman).",
)
def doctor(container_runtime: str) -> None:
    """Check local runtime prerequisites."""
    if docker_available(container_runtime):
        LOGGER.info("%s is available", container_runtime)
    else:
        raise click.ClickException(f"{container_runtime} is not available")

    if container_runtime == "podman":
        socket_url = _podman_socket_url()
        if socket_url is None:
            click.echo(
                "WARNING: Podman socket not found. "
                "Start it with: systemctl --user enable --now podman.socket",
                err=True,
            )
        else:
            LOGGER.info("Podman socket found at %s", socket_url)

    if _selinux_enforcing():
        click.echo(
            "INFO: SELinux is Enforcing. "
            "Bind mounts will be relabelled with :z (shared) when using Podman."
        )

    LOGGER.info("Python dependencies are importable")


@click.command()
@_dump_option
@_work_dir_option
@_container_options
def inspect(
    dump_paths: tuple[Path, ...],
    work_dir: Path,
    oracle_image: str,
    oracle_password: str,
    container_runtime: str | None,
    userns_mode: str | None,
) -> None:
    """Inspect a Data Pump dump and write ``<work-dir>/manifest.json``."""
    runtime = container_runtime or _default_runtime()
    session_path = session_path_for(work_dir)
    if session_path.exists():
        LOGGER.warning(
            "Existing session found at %s — stopping stale container and overwriting",
            session_path,
        )
        cleanup_stale_session(session_path)

    settings = _build_settings(
        dump_paths=dump_paths,
        work_dir=work_dir,
        oracle_image=oracle_image,
        oracle_password=oracle_password,
        container_runtime=runtime,
        userns_mode=userns_mode,
        keep_alive=True,
    )
    try:
        with OracleDMPConverter(settings) as converter:
            converter.inspect()
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str((work_dir / "manifest.json").resolve()))


@click.command("plan")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--oracle-image",
    default=None,
    help=(
        "Override the oracle_image for this plan. Required when the manifest and "
        "config file disagree on oracle_image."
    ),
)
def plan_command(
    manifest_path: Path,
    config_path: Path | None,
    oracle_image: str | None,
) -> None:
    """Build a conversion plan from an inspection manifest."""
    work_dir = manifest_path.parent
    plan_path = work_dir / "plan.yaml"
    LOGGER.info("Loading manifest from %s", manifest_path)
    manifest = load_manifest(manifest_path)
    config = load_config(config_path)
    LOGGER.info("Planning %d tables (format=%s)", len(manifest.tables), manifest.dump_format.value)

    if oracle_image is None:
        if (
            manifest.oracle_image
            and config.oracle_image is not None
            and manifest.oracle_image != config.oracle_image
        ):
            raise click.ClickException(
                f"oracle_image mismatch: manifest has {manifest.oracle_image!r} but "
                f"config has {config.oracle_image!r}. "
                "Pass --oracle-image to select one, update config.yaml to match, "
                "or remove oracle_image from manifest.json."
            )
        effective_image = config.oracle_image or manifest.oracle_image or DEFAULT_ORACLE_IMAGE
    else:
        effective_image = oracle_image

    table_plans = plan_tables(manifest.tables, config, dump_format=manifest.dump_format)
    plan = ConversionPlan(
        dump_paths=manifest.dump_paths,
        tables=table_plans,
        oracle_image=effective_image,
        container_runtime=manifest.container_runtime or DEFAULT_CONTAINER_RUNTIME,
        dump_format=manifest.dump_format,
    )
    save_plan(plan_path, plan)
    unsupported = [t for t in table_plans if t.reason]
    LOGGER.info(
        "Wrote plan for %d tables (%d unsupported): %s",
        len(table_plans),
        len(unsupported),
        plan_path,
    )
    click.echo(str(plan_path.resolve()))


@click.command("convert")
@click.option("--plan", "plan_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--dump",
    "dump_paths",
    multiple=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output",
    "output_dir",
    type=click.Path(path_type=Path),
    default=None,
    show_default="<work-dir>/output",
)
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
    default=None,
    show_default="<plan-dir> if --plan given, else work/",
)
@_container_options
@click.option(
    "--keep-alive",
    is_flag=True,
    default=False,
    help=(
        "Keep the Oracle container running after conversion completes and "
        "preserve session.json. By default the container is stopped and "
        "session.json is deleted."
    ),
)
def convert(
    plan_path: Path | None,
    dump_paths: tuple[Path, ...],
    config_path: Path | None,
    output_dir: Path | None,
    output_format: str,
    work_dir: Path | None,
    oracle_image: str,
    oracle_password: str,
    container_runtime: str | None,
    userns_mode: str | None,
    keep_alive: bool,
) -> None:
    """Convert all tables in a plan, or inspect/plan/convert in one go."""
    fmt = OutputFormat(output_format.lower())
    config = load_config(config_path)

    if work_dir is None:
        work_dir = plan_path.parent if plan_path else Path("work")
    if output_dir is None:
        output_dir = work_dir / "output"

    if plan_path:
        existing_plan = OracleDMPConverter.load_plan(plan_path)
        effective_dump_paths = _resolve_dump_paths(dump_paths, existing_plan.dump_paths)
        effective_image = existing_plan.oracle_image
        effective_runtime = (
            container_runtime or existing_plan.container_runtime or _default_runtime()
        )
    else:
        existing_plan = None
        effective_dump_paths = _resolve_dump_paths(dump_paths)
        effective_image = oracle_image
        if effective_image == DEFAULT_ORACLE_IMAGE and config.oracle_image is not None:
            effective_image = config.oracle_image
        effective_runtime = container_runtime or _default_runtime()

    session_path = session_path_for(work_dir)
    session_existed = session_path.exists()
    if existing_plan is not None and not session_existed:
        raise click.UsageError(
            "A plan was provided but no active inspect session was found "
            f"({session_path} does not exist).\n"
            "The staging schema must be prepared by 'inspect' before 'convert --plan' "
            "can run data-only imports.\n"
            "Run 'inspect' first, then re-run 'convert --plan'."
        )

    settings = _build_settings(
        dump_paths=effective_dump_paths,
        work_dir=work_dir,
        oracle_image=effective_image,
        oracle_password=oracle_password,
        container_runtime=effective_runtime,
        userns_mode=userns_mode,
        output_dir=output_dir,
        output_format=fmt,
        config=config,
        keep_alive=keep_alive,
    )
    try:
        with OracleDMPConverter(settings) as converter:
            if existing_plan is None:
                converter.run()
            else:
                converter.convert(existing_plan)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _resolve_dump_paths(
    cli_paths: tuple[Path, ...],
    plan_paths: tuple[str, ...] = (),
) -> tuple[Path, ...]:
    if cli_paths:
        return tuple(p.resolve() for p in cli_paths)
    return tuple(Path(p).resolve() for p in plan_paths)
