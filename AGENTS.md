# AGENTS.md

## Project Basics
- Python 3.12 package managed by `uv`; use `uv add` / `uv add --dev`, not direct `pip` edits.
- CLI entrypoint is `dmp-to-parquet = dmp_to_parquet.cli:main` in `pyproject.toml`.
- The tool supports both Oracle Data Pump (`expdp`) and legacy (`exp`) dumps; it deliberately does not parse proprietary `.dmp` files directly.
- Default Oracle runtime image is `gvenzl/oracle-free:23-faststart`; override with `DMP_TO_PARQUET_ORACLE_IMAGE` or CLI `--oracle-image`.

## Package Structure
The package uses three subpackages; `__init__.py` files are empty — always import via the full submodule path, never from the subpackage root.
- `oracle/` — `conn.py` (DB helpers), `exporter.py` (row streaming / Arrow coercion), `identifiers.py`, `metadata.py`, `types.py`
- `datapump/` — `runner.py` (Docker exec of expdp/impdp/exp/imp), `parfile.py` (Data Pump parfile rendering), `legacy_parfile.py` (legacy exp/imp parfile rendering), `sqlfile.py` (SQLFILE DDL parser), `imp_show.py` (INDEXFILE / SHOW=Y parser)
- `io/` — `serialization.py` (manifest + plan JSON/YAML), `state.py` (SQLite resumability), `validation.py` (Parquet row-count check)
- Top-level: `cli.py` (Click commands + Docker mount conventions), `converter.py` (inspect/plan/convert orchestration), `planner.py` (strategy selection), `docker_oracle.py`, `models.py`, `errors.py`, `config.py`
- `scripts/create_complex_sample_dump.py` and `scripts/create_legacy_exp_sample.py`: standalone sample-data generators; keep out of the product CLI.

## Commands
- Unit tests: `uv run python -m pytest tests/unit` (`uv run pytest` fails — pytest is not on PATH)
- Integration tests: `uv run python -m pytest tests/integration`
- Full tests: `uv run python -m pytest`
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
- Legacy `exp` format is auto-detected: `impdp SQLFILE=` is tried first; if `ORA-39142` or `ORA-39143` appears in the output, the converter falls back to `imp INDEXFILE=`. Oracle 23ai Free emits `ORA-39143`; older versions emit `ORA-39142`.
- Legacy dumps use whole-table strategy only — `imp` has no `QUERY=` support, so hash chunking is unavailable.
- Large unpartitioned tables need a usable scalar split column for hash chunking; `ROWID` is not a pre-import split strategy.
- Hash chunking uses Data Pump `QUERY` plus `ORA_HASH`; nullable split columns get an extra null bucket.

## Sample Dumps
- Generate complex (Data Pump) sample data: `uv run python scripts/create_complex_sample_dump.py --force` → `sample-data/complex/`
- Generate legacy exp sample: `uv run python scripts/create_legacy_exp_sample.py` → `sample-data/legacy/`
- Both directories are git-ignored.

## Generated / Ignored Artifacts
- Do not commit `.dmp`, `.log`, `sample-data/`, `work/`, `parquet/`, `.venv/`, or cache directories.
- Data Pump logs and generated parfiles may appear beside mounted dump directories during manual runs.
