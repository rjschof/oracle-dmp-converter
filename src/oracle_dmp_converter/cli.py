"""Command-line interface."""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import click

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig, load_config
from oracle_dmp_converter.converter import OracleAdminConnection, OracleDumpConverter
from oracle_dmp_converter.docker_oracle import (
    DEFAULT_CONTAINER_RUNTIME,
    DockerOracle,
    docker_available,
)
from oracle_dmp_converter.io.serialization import (
    load_manifest,
    load_plan,
    load_session,
    save_manifest,
    save_plan,
    save_session,
)
from oracle_dmp_converter.io.state import StateStore
from oracle_dmp_converter.models import ContainerSession, ConversionPlan, OutputFormat
from oracle_dmp_converter.oracle.conn import create_directory, oracle_connection
from oracle_dmp_converter.planner import plan_tables

LOGGER = logging.getLogger(__name__)

DEFAULT_DUMP_DIRECTORY = "ORACLE_DMC_DUMP"
DEFAULT_CONTAINER_DUMP_PATH = "/dumps"
SESSION_FILENAME = "session.json"


def _default_runtime() -> str:
    """Return the container runtime to use, respecting the environment override.

    Returns:
        The value of ``ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME`` if set,
        otherwise the compiled-in :data:`DEFAULT_CONTAINER_RUNTIME`.
    """
    return os.environ.get("ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME)


@click.group()
def main() -> None:
    """Convert Oracle Data Pump dumps to Parquet, Avro, or CSV."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


@main.command()
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
    LOGGER.info("Python dependencies are importable")


def _dump_paths_or_plan_dump_paths(
    dump_paths: tuple[Path, ...],
    plan_dump_paths: tuple[str, ...] = (),
) -> tuple[Path, ...]:
    """Resolve the effective set of dump file paths.

    If the caller supplied explicit ``--dump`` paths, those are returned
    (resolved to absolute form).  Otherwise the paths embedded in a previously
    loaded plan are converted from strings and resolved.

    Args:
        dump_paths: CLI-supplied dump paths, which take priority.
        plan_dump_paths: Fallback dump paths read from a serialised
            :class:`~oracle_dmp_converter.models.ConversionPlan`.

    Returns:
        Tuple of resolved absolute :class:`~pathlib.Path` instances.
    """
    if dump_paths:
        return tuple(path.resolve() for path in dump_paths)
    return tuple(Path(path).resolve() for path in plan_dump_paths)


def _validate_dump_paths(dump_paths: tuple[Path, ...]) -> tuple[Path, tuple[str, ...]]:
    """Validate that dump paths are non-empty and all reside in one directory.

    Oracle Data Pump mounts a single host directory into the container, so all
    ``.dmp`` files must share the same parent directory.

    Args:
        dump_paths: Resolved absolute paths to the ``.dmp`` files.

    Returns:
        A two-tuple of ``(dump_dir, filenames)`` where *dump_dir* is the
        common parent directory and *filenames* is a tuple of bare file names.

    Raises:
        click.ClickException: If *dump_paths* is empty or the files span more
            than one directory.
    """
    if not dump_paths:
        raise click.ClickException("At least one --dump is required")
    dump_dirs = {path.parent.resolve() for path in dump_paths}
    if len(dump_dirs) != 1:
        raise click.ClickException("All dump files must be in the same directory")
    dump_dir = next(iter(dump_dirs))
    return dump_dir, tuple(path.name for path in dump_paths)


def _admin_for_container(container: DockerOracle, password: str) -> OracleAdminConnection:
    """Build an :class:`~oracle_dmp_converter.converter.OracleAdminConnection` for a container.

    Uses the container's mapped host port and the ``system`` user to create
    the administrative connection descriptor used by the converter.

    Args:
        container: Running :class:`~oracle_dmp_converter.docker_oracle.DockerOracle` instance.
        password: Oracle ``system`` user password.

    Returns:
        An :class:`~oracle_dmp_converter.converter.OracleAdminConnection` for
        the running container.
    """
    return OracleAdminConnection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=password,
    )


def _create_dump_directory(admin: OracleAdminConnection) -> None:
    """Create the Oracle DIRECTORY object that maps to the container dump path.

    Connects as the admin user and issues a ``CREATE OR REPLACE DIRECTORY``
    statement pointing at :data:`DEFAULT_CONTAINER_DUMP_PATH`.

    Args:
        admin: Administrative connection descriptor for the running container.
    """
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
    config: ConverterConfig | None = None,
) -> OracleDumpConverter:
    """Construct an :class:`~oracle_dmp_converter.converter.OracleDumpConverter`.

    Wires together the container, admin credentials, working directory, and
    dump filenames into a converter instance ready for inspection or conversion.

    Args:
        container: Running Oracle Free Docker container.
        admin: Administrative connection descriptor.
        work_dir: Host-side directory for intermediate artefacts.
        dumpfiles: Bare filenames of the ``.dmp`` files inside the container
            dump directory.
        output_format: Target output format for converted data.
        config: Optional :class:`~oracle_dmp_converter.config.ConverterConfig`
            with per-table and per-column overrides.

    Returns:
        A configured :class:`~oracle_dmp_converter.converter.OracleDumpConverter`
        instance.
    """
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


def _cleanup_stale_session(session_path: Path) -> None:
    """Stop the container recorded in *session_path* and delete the file.

    Silently ignores errors so that callers can always proceed even when the
    container has already exited or the session file is malformed.

    Args:
        session_path: Path to an existing ``session.json`` file.
    """
    try:
        session = load_session(session_path)
        stale = DockerOracle.reconnect(
            name=session.container_name,
            image=session.oracle_image,
            service=session.oracle_service,
            runtime=session.container_runtime,
        )
        stale.stop()
        LOGGER.info("Stopped stale session container %s", session.container_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Could not stop stale session container: %s", exc)
    try:
        session_path.unlink()
    except OSError:
        pass


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
    default=None,
    show_default="<work-dir>/manifest.json",
)
@click.option(
    "--oracle-image",
    default=lambda: os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE),
    show_default="gvenzl/oracle-free:23-faststart",
)
@click.option("--oracle-password", default="OraclePwd_123", show_default=True)
@click.option(
    "--container-runtime",
    default=_default_runtime,
    show_default="docker",
    help="Container runtime to use (docker or podman).",
)
def inspect(
    dump_paths: tuple[Path, ...],
    work_dir: Path,
    manifest_path: Path,
    oracle_image: str,
    oracle_password: str,
    container_runtime: str,
) -> None:
    """Inspect a full Data Pump dump and write a manifest."""

    dump_dir, dumpfiles = _validate_dump_paths(dump_paths)

    session_path = work_dir / SESSION_FILENAME
    if session_path.exists():
        LOGGER.warning(
            "Existing session found at %s — stopping stale container and overwriting",
            session_path,
        )
        _cleanup_stale_session(session_path)

    LOGGER.info("Starting Oracle container (image=%s, dump_dir=%s)", oracle_image, dump_dir)
    container = DockerOracle.start(
        image=oracle_image,
        password=oracle_password,
        mounts=((dump_dir, DEFAULT_CONTAINER_DUMP_PATH, "rw"),),
        runtime=container_runtime,
    )
    try:
        LOGGER.info("Oracle container %s started; waiting for readiness", container.name)
        container.wait_ready()
        LOGGER.info("Oracle container %s is ready", container.name)
        admin = _admin_for_container(container, oracle_password)
        LOGGER.info(
            "Creating dump directory object %s -> %s",
            DEFAULT_DUMP_DIRECTORY,
            DEFAULT_CONTAINER_DUMP_PATH,
        )
        _create_dump_directory(admin)
        converter = _build_converter(
            container=container,
            admin=admin,
            work_dir=work_dir,
            dumpfiles=dumpfiles,
        )
        LOGGER.info("Inspecting dump: %s", ", ".join(dumpfiles))
        manifest = converter.inspect_dump()
        manifest = replace(
            manifest,
            dump_paths=tuple(str(path.resolve()) for path in dump_paths),
            oracle_image=oracle_image,
            container_runtime=container_runtime,
        )
        if manifest_path is None:
            manifest_path = work_dir / "manifest.json"
        save_manifest(manifest_path, manifest)
        LOGGER.info("Wrote manifest with %d tables: %s", len(manifest.tables), manifest_path)

        session = ContainerSession(
            container_name=container.name,
            container_runtime=container_runtime,
            oracle_image=oracle_image,
            oracle_service=container.service,
            work_dir=str(work_dir.resolve()),
            dump_dir=str(dump_dir),
            created_at=datetime.now(UTC).isoformat(timespec="seconds"),
        )
        save_session(session_path, session)
        LOGGER.info(
            "Session saved to %s — container %s is still running for reuse by 'convert'",
            session_path,
            container.name,
        )
    except Exception:
        container.stop()
        raise


@main.command("plan")
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
        "Override the oracle_image for this plan.  Required when the manifest and "
        "config file disagree on oracle_image."
    ),
)
@click.option(
    "--output",
    "plan_path",
    type=click.Path(path_type=Path),
    default=None,
    show_default="<manifest-dir>/plan.yaml",
)
def plan_command(
    manifest_path: Path,
    config_path: Path | None,
    oracle_image: str | None,
    plan_path: Path | None,
) -> None:
    """Build a conversion plan from an inspection manifest."""

    if plan_path is None:
        plan_path = manifest_path.parent / "plan.yaml"

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
    unsupported = [table for table in table_plans if table.reason]
    LOGGER.info(
        "Wrote plan for %d tables (%d unsupported): %s",
        len(table_plans),
        len(unsupported),
        plan_path,
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
    default=None,
    show_default="<plan-dir> if --plan given, else work/",
)
@click.option(
    "--oracle-image",
    default=lambda: os.environ.get("ORACLE_DMP_CONVERTER_IMAGE", DEFAULT_ORACLE_IMAGE),
    show_default="gvenzl/oracle-free:23-faststart",
)
@click.option("--oracle-password", default="OraclePwd_123", show_default=True)
@click.option(
    "--container-runtime",
    default=None,
    show_default="docker",
    help=(
        "Container runtime to use (docker or podman). "
        "Defaults to the value in the plan, the "
        "ORACLE_DMP_CONVERTER_CONTAINER_RUNTIME env var, or 'docker'."
    ),
)
@click.option(
    "--keep-alive",
    is_flag=True,
    default=False,
    help=(
        "Keep the Oracle container running after conversion completes and "
        "preserve session.json for diagnostic purposes or re-use. "
        "By default the container is stopped and session.json is deleted."
    ),
)
def convert(
    plan_path: Path | None,
    dump_paths: tuple[Path, ...],
    config_path: Path | None,
    output_dir: Path,
    output_format: str,
    work_dir: Path | None,
    oracle_image: str,
    oracle_password: str,
    container_runtime: str | None,
    keep_alive: bool,
) -> None:
    """Convert all tables in a plan, or inspect/plan/convert in one command."""

    fmt = OutputFormat(output_format.lower())
    config = load_config(config_path)

    if work_dir is None:
        work_dir = plan_path.parent if plan_path else Path("work")

    if plan_path:
        plan = load_plan(plan_path)
        resolved_dump_paths = _dump_paths_or_plan_dump_paths(dump_paths, plan.dump_paths)
    else:
        resolved_dump_paths = _dump_paths_or_plan_dump_paths(dump_paths)
        plan = None
        if oracle_image == DEFAULT_ORACLE_IMAGE and config.oracle_image is not None:
            oracle_image = config.oracle_image

    # Resolve the effective container runtime: explicit CLI > plan value > env/default
    effective_runtime = (
        container_runtime or (plan.container_runtime if plan else None) or _default_runtime()
    )

    dump_dir, dumpfiles = _validate_dump_paths(resolved_dump_paths)
    image = plan.oracle_image if plan else oracle_image

    # Auto-detect a session written by a prior 'inspect' run.
    session_path = work_dir / SESSION_FILENAME
    session = load_session(session_path) if session_path.exists() else None

    if session is not None:
        LOGGER.info(
            "Found session at %s — reconnecting to container %s",
            session_path,
            session.container_name,
        )
        container = DockerOracle.reconnect(
            name=session.container_name,
            image=session.oracle_image or image,
            service=session.oracle_service,
            runtime=effective_runtime,
        )
    else:
        LOGGER.info("Starting Oracle container (image=%s, dump_dir=%s)", image, dump_dir)
        container = DockerOracle.start(
            image=image,
            password=oracle_password,
            mounts=((dump_dir, DEFAULT_CONTAINER_DUMP_PATH, "rw"),),
            runtime=effective_runtime,
        )

    try:
        LOGGER.info("Oracle container %s started; waiting for readiness", container.name)
        container.wait_ready()
        LOGGER.info("Oracle container %s is ready", container.name)
        admin = _admin_for_container(container, container.password)
        LOGGER.info(
            "Creating dump directory object %s -> %s",
            DEFAULT_DUMP_DIRECTORY,
            DEFAULT_CONTAINER_DUMP_PATH,
        )
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
            LOGGER.info("No plan provided; running inspect + plan inline")
            manifest = converter.inspect_dump()
            manifest = replace(
                manifest,
                dump_paths=tuple(str(path) for path in resolved_dump_paths),
                oracle_image=image,
                container_runtime=effective_runtime,
            )
            plan = ConversionPlan(
                dump_paths=manifest.dump_paths,
                tables=plan_tables(manifest.tables, config, dump_format=manifest.dump_format),
                oracle_image=image,
                container_runtime=effective_runtime,
                dump_format=manifest.dump_format,
            )
            save_manifest(work_dir / "manifest.json", manifest)
            save_plan(work_dir / "plan.yaml", plan)
            LOGGER.info(
                "Inline inspect+plan complete: %d tables, manifest and plan written to %s",
                len(manifest.tables),
                work_dir,
            )
        else:
            if session is None:
                raise click.UsageError(
                    "A plan was provided but no active inspect session was found "
                    f"({session_path} does not exist).\n"
                    "The staging schema must be prepared by 'inspect' before 'convert --plan' "
                    "can run data-only imports.\n"
                    "Run 'inspect' first, then re-run 'convert --plan'."
                )
            converter.use_format(plan.dump_format)
        state_store = StateStore(work_dir / "convert" / "state.sqlite")
        try:
            result = converter.convert_plan(plan, output_dir, state_store)
        finally:
            state_store.close()
        LOGGER.info(
            "Conversion complete: %d rows across %d tables -> %s",
            result.rows,
            len(result.tables),
            output_dir,
        )
    finally:
        if keep_alive:
            if session is None:
                # Fresh container started by this convert run — write a session
                # so it can be reused or inspected later.
                new_session = ContainerSession(
                    container_name=container.name,
                    container_runtime=effective_runtime,
                    oracle_image=image,
                    oracle_service=container.service,
                    work_dir=str(work_dir.resolve()),
                    dump_dir=str(dump_dir),
                    created_at=datetime.now(UTC).isoformat(timespec="seconds"),
                )
                save_session(session_path, new_session)
            LOGGER.info(
                "Container %s kept alive (--keep-alive); session at %s",
                container.name,
                session_path,
            )
        else:
            container.stop()
            if session_path.exists():
                session_path.unlink()
                LOGGER.debug("Deleted session file %s", session_path)
