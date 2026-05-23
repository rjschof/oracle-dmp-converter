"""Shared base class for Data Pump and legacy imp runner implementations.

Both :class:`~oracle_dmp_converter.datapump.modern.runner.DataPumpRunner`
and :class:`~oracle_dmp_converter.datapump.legacy.runner.LegacyRunner`
need to write a parameter file locally, copy it into the container, and
execute an Oracle utility binary - capturing combined stdout+stderr and
deciding whether a non-zero exit is fatal or merely a warning.  This
module provides that shared plumbing so neither subclass duplicates it.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from oracle_dmp_converter.datapump._exit_policy import (
    STRICT_POLICY,
    ExitClassification,
    ToolExitPolicy,
)
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)


class _BaseRunner:
    """Internal base: parfile write/copy and Oracle tool execution."""

    def __init__(
        self,
        container: ContainerOracle,
        work_dir: Path,
        *,
        keep_parfiles: bool = False,
    ) -> None:
        """Store the target container and ensure the local work directory exists.

        Args:
            container: Running :class:`ContainerOracle`
                instance that will execute ``expdp``/``impdp``/``imp`` commands.
            work_dir: Local directory where generated parfiles are written
                before being copied into the container.  Created automatically
                if it does not already exist.
            keep_parfiles: When True, the local + remote parfiles are not
                deleted after the tool finishes.  Useful for debugging
                tool invocations.  Defaults to False so transient parfiles
                do not accumulate in long-lived work directories.
        """
        self.container = container
        self.work_dir = work_dir
        self.keep_parfiles = keep_parfiles
        work_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _parfile(self, content: str, prefix: str) -> Iterator[str]:
        """Yield the in-container path of a parfile holding *content*.

        On exit, both the local copy and the remote copy inside the container
        are removed unless ``self.keep_parfiles`` is True.  Cleanup runs even
        when the body of the ``with`` block raises.
        """
        local_path = self.work_dir / f"{prefix}-{uuid.uuid4().hex}.par"
        local_path.write_text(content)
        remote_path = f"/tmp/{local_path.name}"
        LOGGER.debug("Copying parfile %s -> container:%s", local_path.name, remote_path)
        self.container.copy_to(local_path, remote_path)
        try:
            yield remote_path
        finally:
            if not self.keep_parfiles:
                self._cleanup_parfile(local_path, remote_path)

    def _cleanup_parfile(self, local_path: Path, remote_path: str) -> None:
        """Remove the local and remote parfile copies, logging failures."""
        try:
            local_path.unlink(missing_ok=True)
        except OSError as exc:
            LOGGER.warning("Failed to remove local parfile %s: %s", local_path, exc)
        try:
            self.container.exec(["rm", "-f", remote_path], check=False)
        except Exception as exc:  # noqa: BLE001 — container exec wraps errors
            LOGGER.warning("Failed to remove remote parfile %s: %s", remote_path, exc)

    def _run_tool(
        self,
        cmd: list[str],
        parfile_content: str,
        prefix: str,
        *,
        policy: ToolExitPolicy = STRICT_POLICY,
    ) -> str:
        """Write a parfile, copy it into the container, run *cmd*, return output.

        The tool's exit is classified via *policy*:

        - SUCCESS  -> returns the combined stdout/stderr.
        - WARNING  -> logs at WARNING level with tool name + returncode and
                      still returns the combined output (caller can ignore
                      or inspect).  This is the legacy-``imp`` exit-2 case.
        - FATAL    -> raises :class:`~oracle_dmp_converter.errors.DataPumpError`
                      with the full output as the exception message.
        """
        with self._parfile(parfile_content, prefix) as remote_path:
            LOGGER.info("Running %s (parfile=%s)", cmd[0], remote_path)
            result = self.container.exec([*cmd, f"parfile={remote_path}"], check=False)
            output = result.stdout + result.stderr
            classification = policy.classify(result.returncode, output)
            if classification is ExitClassification.SUCCESS:
                LOGGER.info("%s completed successfully", cmd[0])
                return output
            if classification is ExitClassification.WARNING:
                LOGGER.warning(
                    "%s completed with warnings (returncode=%d); continuing.\n%s",
                    cmd[0],
                    result.returncode,
                    output,
                )
                return output
            LOGGER.error("%s failed (returncode=%d):\n%s", cmd[0], result.returncode, output)
            raise DataPumpError(output)
