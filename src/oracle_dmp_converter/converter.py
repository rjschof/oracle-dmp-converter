"""High-level facade for converting Oracle Data Pump dumps.

:class:`OracleDMPConverter` owns container lifecycle plus the three phases
(:meth:`inspect`, :meth:`plan`, :meth:`convert`) and exposes a single
context-manager friendly entry point that any caller — CLI, FastAPI, Celery,
notebook, test — can drive directly.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

from oracle_dmp_converter.config import DEFAULT_ORACLE_IMAGE, ConverterConfig
from oracle_dmp_converter.core.executor import StagingExecutor
from oracle_dmp_converter.core.results import PlanConversionResult
from oracle_dmp_converter.models import ConversionPlan, DumpManifest
from oracle_dmp_converter.persistence.report import (
    build_conversion_report,
    save_conversion_report,
)
from oracle_dmp_converter.persistence.serialization import (
    load_manifest as _load_manifest,
)
from oracle_dmp_converter.persistence.serialization import (
    load_plan as _load_plan,
)
from oracle_dmp_converter.persistence.serialization import (
    save_manifest as _save_manifest,
)
from oracle_dmp_converter.persistence.serialization import (
    save_plan as _save_plan,
)
from oracle_dmp_converter.persistence.state import StateStore
from oracle_dmp_converter.planner import plan_tables
from oracle_dmp_converter.runtime import container_manager
from oracle_dmp_converter.runtime.admin import (
    DEFAULT_CONTAINER_DUMP_PATH,
    DEFAULT_DUMP_DIRECTORY,
    admin_for_container,
    configure_omf,
    create_dump_directory,
    create_work_dir_directories,
)
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle
from oracle_dmp_converter.runtime.session import (
    cleanup_stale_session,
    load_session_if_exists,
    session_path_for,
    verify_session_fingerprint,
    write_session,
)
from oracle_dmp_converter.settings import ConverterSettings

LOGGER = logging.getLogger(__name__)


def _safe_stop_container(
    container: ContainerOracle,
    *,
    reason: str,
    reraise: bool = False,
) -> None:
    """Stop *container* with narrow error handling and structured logging.

    Replaces a previous bare ``except Exception: pass`` that could silently
    leak Docker containers on failure paths.  Errors are logged at WARNING
    with the container id + name so they're visible in CI logs.

    Args:
        container: Running :class:`ContainerOracle` to stop.
        reason: Short phrase describing why ``stop()`` is being called
            (``"normal shutdown"``, ``"start failure"``, ...).  Included
            in the log line for traceability.
        reraise: When ``True``, any underlying error from
            :meth:`ContainerOracle.stop` is re-raised after logging.  Used
            on the normal shutdown path so an unexpected Docker error is
            surfaced to the caller; left ``False`` on best-effort cleanup
            paths (e.g. unwinding from a failed start) where the original
            exception is more important than this secondary failure.
    """
    try:
        container.stop()
    except Exception as exc:  # noqa: BLE001 — docker client wraps errors
        LOGGER.warning(
            "Container stop failed (reason=%s, container=%s): %s",
            reason,
            container.name,
            exc,
        )
        if reraise:
            raise


class OracleDMPConverter:
    """Inspect, plan, and convert Oracle Data Pump dumps.

    Each method writes its canonical artifact to ``settings.work_dir``:
    :meth:`inspect` always writes ``manifest.json``, :meth:`plan` always
    writes ``plan.yaml``, and :meth:`convert` (or :meth:`run`) always writes
    ``conversion_report.yaml``.
    """

    def __init__(self, settings: ConverterSettings) -> None:
        self.settings = settings
        self._container: ContainerOracle | None = None
        self._executor: StagingExecutor | None = None
        self._session_existed_at_start = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start (or reconnect to) the Oracle container and prepare DIRECTORYs."""
        if self._container is not None:
            return

        session_path = session_path_for(self.settings.work_dir)
        self._session_existed_at_start = session_path.exists()

        container = container_manager.start_or_reconnect(self.settings)
        try:
            LOGGER.info("Oracle container %s started; waiting for readiness", container.name)
            container.wait_ready()
            LOGGER.info("Oracle container %s is ready", container.name)
            admin = admin_for_container(container, self.settings.oracle_password)
            create_dump_directory(admin)
            create_work_dir_directories(admin)
            configure_omf(admin)
            self._container = container
            self._executor = StagingExecutor(
                container=container,
                admin=admin,
                work_dir=self.settings.work_dir,
                dumpfiles=self.settings.dump_filenames,
                directory=DEFAULT_DUMP_DIRECTORY,
                directory_path=DEFAULT_CONTAINER_DUMP_PATH,
                output_format=self.settings.output_format,
                config=self.settings.config,
            )
            # If a prior phase left a session.json behind, seed the executor
            # with the recorded ``metadata_imported`` flag and previously
            # prepared staging schemas so that ``validate_metadata_state``
            # and ``get_prepared_schemas`` reflect cross-invocation state.
            if self._session_existed_at_start:
                session = load_session_if_exists(session_path)
                if session is not None:
                    ok, reason = verify_session_fingerprint(
                        session,
                        container_name=container.name,
                        container_runtime=self.settings.container_runtime,
                        oracle_image=self.settings.oracle_image,
                        prepared_schemas=session.prepared_schemas,
                    )
                    if not ok:
                        LOGGER.warning(
                            "Stale session detected for %s: %s. "
                            "Treating staging state as empty and re-importing.",
                            session_path,
                            reason,
                        )
                        self._executor.metadata_imported = False
                    else:
                        if reason.startswith("unverified"):
                            LOGGER.info(
                                "Session %s %s; reusing recorded state.",
                                session_path,
                                reason,
                            )
                        self._executor.metadata_imported = session.metadata_imported
                        # pylint: disable-next=protected-access
                        self._executor._prepared_schemas = set(session.prepared_schemas)  # noqa: SLF001
        except Exception:
            _safe_stop_container(container, reason="start failure")
            self._container = None
            self._executor = None
            raise

    def stop(self) -> None:
        """Stop the container, or keep it alive when :attr:`settings.keep_alive`."""
        if self._container is None:
            return
        container = self._container
        session_path = session_path_for(self.settings.work_dir)
        try:
            if self.settings.keep_alive:
                executor = self._executor
                write_session(
                    session_path,
                    container=container,
                    container_runtime=self.settings.container_runtime,
                    oracle_image=self.settings.oracle_image,
                    work_dir=self.settings.work_dir,
                    dump_dir=self.settings.dump_dir,
                    metadata_imported=(
                        executor.metadata_imported if executor is not None else False
                    ),
                    prepared_schemas=(
                        executor.get_prepared_schemas() if executor is not None else None
                    ),
                )
                LOGGER.info(
                    "Container %s kept alive (keep_alive=True); session at %s",
                    container.name,
                    session_path,
                )
            else:
                _safe_stop_container(container, reason="normal shutdown", reraise=True)
                if session_path.exists():
                    session_path.unlink()
                    LOGGER.debug("Deleted session file %s", session_path)
        finally:
            self._container = None
            self._executor = None

    def __enter__(self) -> OracleDMPConverter:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Phases
    # ------------------------------------------------------------------

    @property
    def container(self) -> ContainerOracle:
        if self._container is None:
            raise RuntimeError("Converter is not started; call start() or use as a context manager")
        return self._container

    @property
    def executor(self) -> StagingExecutor:
        if self._executor is None:
            raise RuntimeError("Converter is not started; call start() or use as a context manager")
        return self._executor

    @property
    def session_existed_at_start(self) -> bool:
        """Whether a ``session.json`` existed when :meth:`start` was last called."""
        return self._session_existed_at_start

    def inspect(self) -> DumpManifest:
        """Run inspect, write ``<work_dir>/manifest.json``, return the manifest."""
        manifest = self.executor.inspect_dump()
        manifest = replace(
            manifest,
            dump_paths=tuple(str(p.resolve()) for p in self.settings.dump_paths),
            oracle_image=self.settings.oracle_image,
            container_runtime=self.settings.container_runtime,
        )
        manifest_path = self.settings.work_dir / "manifest.json"
        _save_manifest(manifest_path, manifest)
        LOGGER.info("Wrote manifest with %d tables: %s", len(manifest.tables), manifest_path)
        return manifest

    def plan(
        self,
        manifest: DumpManifest,
        *,
        config: ConverterConfig | None = None,
        oracle_image: str | None = None,
    ) -> ConversionPlan:
        """Build a conversion plan and write ``<work_dir>/plan.yaml``."""
        cfg = config if config is not None else (self.settings.config or ConverterConfig())

        if oracle_image is None:
            if (
                manifest.oracle_image
                and cfg.oracle_image is not None
                and manifest.oracle_image != cfg.oracle_image
            ):
                raise ValueError(
                    f"oracle_image mismatch: manifest has {manifest.oracle_image!r} but "
                    f"config has {cfg.oracle_image!r}. "
                    "Pass oracle_image to select one, update config to match, "
                    "or remove oracle_image from the manifest."
                )
            effective_image = cfg.oracle_image or manifest.oracle_image or DEFAULT_ORACLE_IMAGE
        else:
            effective_image = oracle_image

        table_plans = plan_tables(manifest.tables, cfg, dump_format=manifest.dump_format)
        plan = ConversionPlan(
            dump_paths=manifest.dump_paths,
            tables=table_plans,
            oracle_image=effective_image,
            container_runtime=manifest.container_runtime or self.settings.container_runtime,
            dump_format=manifest.dump_format,
        )
        plan_path = self.settings.work_dir / "plan.yaml"
        _save_plan(plan_path, plan)
        unsupported = [t for t in table_plans if t.reason]
        LOGGER.info(
            "Wrote plan for %d tables (%d unsupported): %s",
            len(table_plans),
            len(unsupported),
            plan_path,
        )
        return plan

    def convert(self, plan: ConversionPlan) -> PlanConversionResult:
        """Convert all tables in *plan* and write ``conversion_report.yaml``."""
        if self.settings.output_dir is None:
            raise ValueError("output_dir is required for convert()")

        executor = self.executor
        # pylint: disable-next=protected-access
        if executor._workflow is None:  # noqa: SLF001
            executor.use_format(plan.dump_format)

        # Fail fast if the running container's staging state does not match
        # what the inspect phase recorded (e.g. container restarted between
        # phases).  Method is a no-op when metadata_imported is False.
        executor.validate_metadata_state(plan)

        state_store = StateStore(self.settings.work_dir / "convert" / "state.sqlite")
        try:
            result = executor.convert_plan(plan, self.settings.output_dir, state_store)
        finally:
            state_store.close()
        LOGGER.info(
            "Conversion complete: %d rows across %d tables -> %s",
            result.rows,
            len(result.tables),
            self.settings.output_dir,
        )
        report = build_conversion_report(plan, result, executor.output_format.value)
        save_conversion_report(self.settings.work_dir, report)
        LOGGER.info(
            "Wrote conversion report: %s",
            self.settings.work_dir / "conversion_report.yaml",
        )
        return result

    def run(self) -> PlanConversionResult:
        """Run inspect → plan → convert; return the final conversion result."""
        manifest = self.inspect()
        plan = self.plan(manifest)
        return self.convert(plan)

    # ------------------------------------------------------------------
    # Persistence wrappers
    # ------------------------------------------------------------------

    def save_manifest(self, manifest: DumpManifest, path: Path | None = None) -> Path:
        target = path if path is not None else self.settings.work_dir / "manifest.json"
        _save_manifest(target, manifest)
        return target

    def save_plan(self, plan: ConversionPlan, path: Path | None = None) -> Path:
        target = path if path is not None else self.settings.work_dir / "plan.yaml"
        _save_plan(target, plan)
        return target

    @staticmethod
    def load_manifest(path: Path) -> DumpManifest:
        return _load_manifest(path)

    @staticmethod
    def load_plan(path: Path) -> ConversionPlan:
        return _load_plan(path)


__all__ = ["OracleDMPConverter", "cleanup_stale_session"]
