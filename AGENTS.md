# AGENTS.md

## Project Basics
- Python 3.12 package managed by `uv`; use `uv add` / `uv add --dev`, not direct `pip` edits.
- **Src-layout**: package lives at `src/oracle_dmp_converter/`, not at the repo root.
- CLI entrypoint is `oracle-dmp-converter = oracle_dmp_converter.cli:main` in `pyproject.toml`.
- The tool supports both Oracle Data Pump (`expdp`) and legacy (`exp`) dumps; it deliberately does not parse proprietary `.dmp` files directly.
- Default Oracle runtime image is `gvenzl/oracle-free:23-faststart`; override with `ORACLE_DMP_CONVERTER_IMAGE` (env var) or CLI `--oracle-image`.
- Override Docker `--platform` with `ORACLE_DMP_CONVERTER_DOCKER_PLATFORM` (e.g. `linux/amd64` on Apple Silicon).

## Package Structure
The package uses three subpackages; `__init__.py` files are empty — always import via the full submodule path, never from the subpackage root.
- `oracle/` — `conn.py` (DB helpers), `exporter.py` (row streaming / Arrow coercion), `format_writer.py` (pluggable Parquet/Avro/CSV writers), `identifiers.py`, `metadata.py`, `types.py`
- `datapump/` — `runner.py` (Docker exec of expdp/impdp/exp/imp), `parfile.py` (Data Pump parfile rendering), `legacy_parfile.py` (legacy exp/imp parfile rendering), `sqlfile.py` (SQLFILE DDL parser), `imp_show.py` (INDEXFILE / SHOW=Y parser)
- `io/` — `serialization.py` (manifest + plan JSON/YAML), `state.py` (SQLite resumability), `validation.py` (row-count check)
- Top-level: `cli.py` (Click commands + Docker mount conventions), `converter.py` (inspect/plan/convert orchestration), `planner.py` (strategy selection), `docker_oracle.py`, `models.py`, `errors.py`, `config.py`
- `scripts/create_complex_sample_dump.py` and `scripts/create_legacy_exp_sample.py`: standalone sample-data generators; keep out of the product CLI.

## Commands
- Initial setup: `make setup` (runs `uv sync --all-groups`)
- Unit tests: `uv run python -m pytest tests/unit` (`uv run pytest` fails — pytest is not on PATH)
- Integration tests: `uv run python -m pytest tests/integration`
- Full tests: `uv run python -m pytest`
- Format/fix: `make format`
- Ruff check: `make format-check`
- Pylint: `make lint` (lints `src scripts tests`, not just `src`)
- Local prerequisite check: `oracle-dmp-converter doctor`
- No typecheck target — no mypy/pyright configured.

## Docker / Oracle Testing Gotchas
- Integration tests start real Oracle Free containers and run real `expdp` / `impdp`; expect minutes, not seconds.
- Docker must be running; tests skip if unavailable, but CLI commands fail fast via `doctor`.
- If an integration run is interrupted, check `docker ps --format '{{.Names}}'` for `oracle-dmp-converter-*` containers and stop leftovers.
- Tests and CLI mount dump directories into containers; all `--dump` files must be in the same host directory.
- `DockerOracle.exec()` uses `subprocess.run(["docker", "exec", ...])`, not the Docker SDK's `exec_run()` — SDK's chunked HTTP stream never sends EOF when the container stops mid-exec.

## Conversion Workflow
- Normal flow is `inspect -> plan -> convert`, producing `manifest.json`, `plan.yaml`, output files, and `state.sqlite`.
- Full-dump discovery uses Data Pump `SQLFILE`; table metadata comes from `CONTENT=METADATA_ONLY` imports into a staging schema.
- Staging schema naming: source schema `APP` is imported as `DMP_APP` (prefix is `DMP_`).
- Legacy `exp` format is auto-detected: `impdp SQLFILE=` is tried first; if `ORA-39142` or `ORA-39143` appears in the output, the converter falls back to `imp INDEXFILE=`. Oracle 23ai Free emits `ORA-39143`; older versions emit `ORA-39142`.
- Legacy dumps use whole-table strategy only — `imp` has no `QUERY=` support, so hash chunking is unavailable.
- Large unpartitioned tables need a usable scalar split column for hash chunking; `ROWID` is not a pre-import split strategy.
- Hash chunking uses Data Pump `QUERY` plus `ORA_HASH`; nullable split columns get an extra null bucket.
- `range` strategy is **not implemented** — `plan_table()` returns `UNSUPPORTED` for it immediately.
- `convert-hash-table` is a standalone CLI command that converts a single known table via hash buckets without a prior inspect/plan step.

## Output Formats
The `convert` and `convert-hash-table` commands accept `--format parquet` (default), `--format avro`, or `--format csv`.
- Parquet: written via `pyarrow.parquet.ParquetWriter` (streaming, columnar).
- Avro: written via `fastavro`; Arrow schema is mapped to an Avro record schema; all fields are nullable unions.
- CSV: written via `pyarrow.csv.write_csv`; header is written once on the first batch.
- Writer implementations live in `oracle/format_writer.py`; the factory is `make_writer(output_format, path, schema)`.

## Planner: Hash Column Eligibility
Types that are **never** hash candidates: `BFILE`, `BLOB`, `CLOB`, `LONG`, `LONG RAW`, `NCLOB`, `RAW`, `ROWID`, `UROWID`, `XMLTYPE`.
Preference order: (1) single-col primary key, (2) single-col unique key, (3) non-nullable scalar, (4) nullable scalar.

## Output Structure
Output files land at `<output_dir>/<schema>/<table>/<chunk>.<ext>` where names are lowercased and non-alphanumeric chars replaced with `_`.
- Whole-table chunk: `whole.parquet` / `whole.avro` / `whole.csv`
- Hash chunks: `hash-00000-of-00064.parquet` (zero-padded 5 digits) + `hash-null.parquet` for null bucket
- Partition chunks: `partition-00001-<PARTITION_NAME>.parquet`

## Config File (YAML, `--config`)
See `docs/config.yaml.example` for a fully annotated reference. Core structure:
```yaml
oracle:
  image: gvenzl/oracle-free:23-faststart
  max_stage_gb: 8          # default staging size limit

default_hash_buckets: 64   # global default; also accepted under oracle: key

tables:
  SCHEMA.TABLE:
    strategy: hash | whole  # range is unsupported
    split_column: COLUMN_NAME
    buckets: 256
    force_large: true       # bypass staging size check

columns:
  SCHEMA.TABLE.COLUMN:
    expression: "SDO_UTIL.TO_WKTGEOMETRY({column})"
    parquet_type: string
```
Keys are looked up case-insensitively (exact match, then `.upper()`).

## Sample Dumps
- Generate complex (Data Pump) sample data: `uv run python scripts/create_complex_sample_dump.py --force` → `sample-data/complex/`
- Generate legacy exp sample: `uv run python scripts/create_legacy_exp_sample.py` → `sample-data/legacy/`
- Both directories are git-ignored.

## Generated / Ignored Artifacts
- Do not commit `.dmp`, `.log`, `sample-data/`, `work/`, `parquet/`, `.venv/`, or cache directories.
- Data Pump logs and generated parfiles may appear beside mounted dump directories during manual runs.
