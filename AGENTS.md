# AGENTS.md

## What this repo is

Python 3.12 CLI tool that converts Oracle Data Pump (`expdp`) and legacy (`exp`) dump files to Parquet, Avro, or CSV. It does **not** parse `.dmp` files directly — it spins up Oracle Database Free in Docker/Podman as a temporary staging reader, then exports via PyArrow/fastavro.

Library entry point: `src/oracle_dmp_converter/converter.py` — `OracleDMPConverter` facade with `start`/`stop`/`inspect`/`plan`/`convert`/`run`. CLI lives in `src/oracle_dmp_converter/cli/` and is a thin Click adapter — Click group with four subcommands: `doctor`, `inspect`, `plan`, `convert`.

---

## Setup

```bash
make setup          # uv sync --all-groups; creates .venv
```

Run commands via `uv run ...` or activate the venv first. Do not use `pip`.

---

## Developer commands

```bash
make format         # ruff format + ruff check --fix (modifies files)
make format-check   # check only, no changes; equivalent: uv run ruff check .
make lint           # uv run pylint src scripts tests
make clean          # removes .venv dist build work parquet sample-data tests/runs caches
```

---

## Testing

```bash
# Unit tests — no Docker required
uv run python -m pytest tests/unit

# Single test file
uv run python -m pytest tests/unit/test_planner.py

# Single test by name
uv run python -m pytest tests/unit/test_planner.py::test_partitioned_table_plans_partition_chunks

# Keyword filter
uv run python -m pytest -k "test_small_table"

# With coverage
uv run python -m pytest tests/unit --cov=oracle_dmp_converter

# Integration tests — require Docker; pull ~1-2 GB Oracle image on first run; several minutes
uv run python -m pytest tests/integration
```

Integration tests auto-skip when Docker is unavailable (autouse `skip_if_no_docker` fixture in `tests/integration/conftest.py`). No marker needed on individual tests.

`tests/data/` holds real `impdp` log and dump fixtures captured from actual Oracle runs — used by unit tests for DDL parser coverage without a live container.

---

## Architecture notes

### Dump format auto-detection
`create_workflow()` (`src/oracle_dmp_converter/datapump/workflow.py`) first tries modern `impdp SQLFILE=`. On `ORA-39142` or `ORA-39143` it automatically falls back to legacy `imp INDEXFILE=`. A `_ProbedModernWorkflow` wrapper caches the discovered table list so `discover_tables()` is not called twice.

### Staging schema pattern
Source schema `SCHEMA` is imported into staging schema `DMP_SCHEMA`. Each chunk import drops the staging table, imports, exports, and drops again. Resumability is tracked in a SQLite `StateStore` at `<work_dir>/convert/state.sqlite`.

### Docker/Podman
The tool uses the Docker SDK Python library for container management but runs `docker exec` / `docker cp` via `subprocess`, not the SDK exec API (SDK's chunked HTTP stream never closes EOF). Works with both Docker and Podman.

**Apple Silicon:** set `DMP_CONVERTER_DOCKER_PLATFORM=linux/amd64` — Oracle Free only has an amd64 image.

### Oracle-to-Arrow type mapping (`src/oracle_dmp_converter/oracle/types.py`)
- `NUMBER(p,0)` with `p <= 18` → `int64`; larger or no-scale → `decimal128` or `double`
- `DATE` / `TIMESTAMP` → `timestamp[us]`
- `XMLTYPE`, `SDO_GEOMETRY`, `INTERVAL *`, `ANYDATA`, `TIMESTAMP WITH TIME ZONE` → `string` via `TO_CHAR()`
- Per-column overrides in `config.yaml` can override both the SQL expression and Arrow type

### Output path convention
`<output_dir>/<schema_lower>/<table_lower>/<chunk>.<ext>` where non-alphanumeric chars in names are replaced with `_`. Chunk name patterns: `whole`, `partition-00001-P_NORTH`, `hash-00000-of-00064`, `hash-null`.

---

## Key env vars

| Var | Effect |
|---|---|
| `DMP_CONVERTER_CONTAINER_RUNTIME` | `docker` (default) or `podman` |
| `DMP_CONVERTER_IMAGE` | Override Oracle image tag |
| `DMP_CONVERTER_DOCKER_PLATFORM` | Override `--platform` (set `linux/amd64` on Apple Silicon) |

---

## Style conventions

- Line length: **100** characters (ruff + pylint)
- All models are **frozen dataclasses**; use `dataclasses.replace()` to derive new instances
- `from __future__ import annotations` in all source files
- Pylint disabled: `broad-exception-caught`, `duplicate-code`, `missing-*-docstring`, `too-many-*` — docstrings present but not enforced; "too-many" checks suppressed
- Circular import avoidance: `datapump/workflow.py` uses deferred imports inside function bodies with `# noqa: PLC0415`
- pytest runs with `--import-mode=importlib` (set in `pyproject.toml`)
- Ruff rules: `E`, `F`, `I` (isort), `UP` (pyupgrade), `B` (bugbear)

---

## No CI or pre-commit configured

No `.github/workflows/`, no `.pre-commit-config.yaml`. The `Makefile` is the only task runner.
