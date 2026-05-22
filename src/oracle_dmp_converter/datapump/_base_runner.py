"""Shared base class for Data Pump and legacy imp runner implementations.

Both :class:`~oracle_dmp_converter.datapump.modern.runner.DataPumpRunner`
and :class:`~oracle_dmp_converter.datapump.legacy.runner.LegacyRunner`
need to write a parameter file locally, copy it into the container, and
execute an Oracle utility binary - capturing combined stdout+stderr and
raising on non-zero exit.  This module provides that shared plumbing so
neither subclass duplicates it.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)


class _BaseRunner:
    """Internal base: parfile write/copy and Oracle tool execution."""

    def __init__(self, container: ContainerOracle, work_dir: Path) -> None:
        """Store the target container and ensure the local work directory exists.

        Args:
            container: Running :class:`ContainerOracle`
                instance that will execute ``expdp``/``impdp``/``imp`` commands.
            work_dir: Local directory where generated parfiles are written
                before being copied into the container.  Created automatically
                if it does not already exist.
        """
        self.container = container
        self.work_dir = work_dir
        work_dir.mkdir(parents=True, exist_ok=True)

    def _write_and_copy(self, content: str, prefix: str) -> str:
        """Write *content* to a local parfile and copy it into the container.

        Returns the absolute path of the remote copy (under ``/tmp``).
        """
        local_path = self.work_dir / f"{prefix}-{uuid.uuid4().hex}.par"
        local_path.write_text(content)
        remote_path = f"/tmp/{local_path.name}"
        LOGGER.debug("Copying parfile %s -> container:%s", local_path.name, remote_path)
        self.container.copy_to(local_path, remote_path)
        return remote_path

    def _run_tool(self, cmd: list[str], parfile_content: str, prefix: str) -> str:
        """Write a parfile, copy it into the container, run *cmd*, return output.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit, with the full combined stdout+stderr as the exception message.
        """
        remote_path = self._write_and_copy(parfile_content, prefix)
        LOGGER.info("Running %s (parfile=%s)", cmd[0], remote_path)
        result = self.container.exec([*cmd, f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            LOGGER.error("%s failed (returncode=%d):\n%s", cmd[0], result.returncode, output)
            raise DataPumpError(output)
        LOGGER.info("%s completed successfully", cmd[0])
        return output
