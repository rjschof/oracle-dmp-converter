# AGENTS.md

## Project Basics
- Python 3.12 package managed by `uv`; use `uv add` / `uv add --dev`, not direct `pip` edits.
- CLI entrypoint is `dmp-to-parquet = dmp_to_parquet.cli:main` in `pyproject.toml`.
- The tool supports Oracle Data Pump `expdp` dumps only; it deliberately does not parse proprietary `.dmp` files directly.
- Default Oracle runtime image is `gvenzl/oracle-free:23-slim`; override with `DMP_TO_PARQUET_ORACLE_IMAGE` or CLI `--oracle-image`.

## High-Value Files
- `src/dmp_to_parquet/cli.py`: user-facing commands and Docker mount conventions.
- `src/dmp_to_parquet/converter.py`: inspect/convert orchestration and staged imports.
- `src/dmp_to_parquet/parfile.py` and `datapump.py`: exact `expdp` / `impdp` parfile behavior.
- `src/dmp_to_parquet/planner.py`: whole-table vs partition vs hash-bucket strategy selection.
- `src/dmp_to_parquet/exporter.py`: Oracle row streaming and Arrow/Parquet coercion.
- `scripts/create_complex_sample_dump.py`: standalone sample-data generator; keep this out of the product CLI.

## Commands
- Unit tests: `uv run pytest tests/unit`
- Integration tests: `uv run pytest tests/integration`
- Full tests: `uv run pytest`
- Format/fix: `make format`
- Ruff check: `make format-check`
- Pylint: `make lint`
- Local prerequisite check: `uv run dmp-to-parquet doctor`

## Docker / Oracle Testing Gotchas
- Integration tests start real Oracle Free containers and run real `expdp` / `impdp`; expect minutes, not seconds.
- Docker must be running; tests skip if unavailable, but CLI commands fail fast via `doctor`.
- If an integration run is interrupted, check `docker ps --format '{{.Names}}'` for `dmp2parquet-*` containers and stop leftovers.
- Tests and CLI mount dump directories into containers; all `--dump` files must be in the same host directory.

## Conversion Workflow
- Normal flow is `inspect -> plan -> convert`, producing `manifest.json`, `plan.yaml`, Parquet output, and `state.sqlite`.
- Full-dump discovery uses Data Pump `SQLFILE`; table metadata comes from `CONTENT=METADATA_ONLY` imports into a staging schema.
- Large unpartitioned tables need a usable scalar split column for hash chunking; `ROWID` is not a pre-import split strategy.
- Hash chunking uses Data Pump `QUERY` plus `ORA_HASH`; nullable split columns get an extra null bucket.

## Sample Dump
- Generate local complex sample data with `uv run python scripts/create_complex_sample_dump.py --force`.
- Generated files live under `sample-data/complex/` and are intentionally ignored by git.
- The generated sample validates with the README `inspect`, `plan`, and `convert` commands.

## Generated / Ignored Artifacts
- Do not commit `.dmp`, `.log`, `sample-data/`, `work/`, `parquet/`, `.venv/`, or cache directories.
- Data Pump logs and generated parfiles may appear beside mounted dump directories during manual runs.
