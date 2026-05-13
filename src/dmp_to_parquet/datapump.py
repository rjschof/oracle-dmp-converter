"""Data Pump command execution through Docker."""

from __future__ import annotations

import uuid
from pathlib import Path

from dmp_to_parquet.docker_oracle import DockerOracle
from dmp_to_parquet.errors import DataPumpError
from dmp_to_parquet.parfile import (
    ExportJob,
    ImportJob,
    SqlFileJob,
    render_export_parfile,
    render_import_parfile,
    render_sqlfile_parfile,
)


class DataPumpRunner:
    def __init__(self, container: DockerOracle, work_dir: Path) -> None:
        self.container = container
        self.work_dir = work_dir
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def _write_and_copy(self, content: str, prefix: str) -> str:
        local_path = self.work_dir / f"{prefix}-{uuid.uuid4().hex}.par"
        local_path.write_text(content)
        remote_path = f"/tmp/{local_path.name}"
        self.container.copy_to(local_path, remote_path)
        return remote_path

    def run_expdp(self, job: ExportJob) -> str:
        remote_path = self._write_and_copy(render_export_parfile(job), "expdp")
        result = self.container.exec(["expdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    def run_impdp(self, job: ImportJob) -> str:
        remote_path = self._write_and_copy(render_import_parfile(job), "impdp")
        result = self.container.exec(["impdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output

    def run_sqlfile(self, job: SqlFileJob) -> str:
        remote_path = self._write_and_copy(render_sqlfile_parfile(job), "sqlfile")
        result = self.container.exec(["impdp", f"parfile={remote_path}"], check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            raise DataPumpError(output)
        return output
