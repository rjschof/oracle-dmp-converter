# oracle-dmp-converter

Convert Oracle Data Pump (`expdp`) and legacy (`exp`) dump files to Parquet, Avro, or CSV — without a paid Oracle license — by using Oracle Database Free in Docker as a temporary reader.

The converter does not parse `.dmp` files directly. Oracle Data Pump is proprietary, so the reliable path is:

```text
expdp / exp dump -> Oracle Free staging database -> Parquet / Avro / CSV
```

The implementation is intentionally chunk-oriented so large dumps can be processed table-by-table, partition-by-partition, or hash-bucket-by-hash-bucket without requiring the whole dump to fit in Oracle Free at once.

## Implemented workflow

This build includes:

- Python 3.12 project managed by `uv`.
- Docker-managed Oracle Free container support.
- Data Pump parfile generation for `expdp` and `impdp`.
- Legacy `exp`/`imp` format auto-detection and fallback.
- Full-dump table discovery using Data Pump `SQLFILE` (or `imp INDEXFILE=` for legacy dumps).
- Per-table metadata inspection using `CONTENT=METADATA_ONLY` imports.
- Plan generation for whole-table, partition, and hash-bucket conversion.
- Real Data Pump import using `QUERY` hash buckets.
- Real partition-level Data Pump imports.
- Streaming Oracle table export to Parquet, Avro, or CSV with PyArrow / fastavro.
- Row-count validation helpers.
- SQLite state tracking for resumable chunk conversion.
- Unit tests for deterministic planning/parfile/type behavior.
- Docker integration tests that create Oracle schemas, export full Data Pump dumps, inspect/plan/convert them, and verify output.

## Default Oracle image

```text
gvenzl/oracle-free:23-faststart
```

Override with `ORACLE_DMP_CONVERTER_IMAGE` (env var) or `--oracle-image` (CLI flag).

Override Docker `--platform` with `ORACLE_DMP_CONVERTER_DOCKER_PLATFORM` (e.g. `linux/amd64` on Apple Silicon).

## Commands

Check runtime prerequisites:

```bash
oracle-dmp-converter doctor
```

Inspect a full Data Pump dump:

```bash
oracle-dmp-converter inspect \
  --dump ./input/full.dmp \
  --work-dir ./work \
  --output ./work/manifest.json
```

Create a conversion plan:

```bash
oracle-dmp-converter plan \
  --manifest ./work/manifest.json \
  --config ./config.yaml \
  --output ./work/plan.yaml
```

Convert all planned tables:

```bash
oracle-dmp-converter convert \
  --plan ./work/plan.yaml \
  --output ./parquet
```

One-shot inspect, plan, and convert (Parquet output is the default):

```bash
oracle-dmp-converter convert \
  --dump ./input/full.dmp \
  --config ./config.yaml \
  --work-dir ./work \
  --output ./parquet
```

Output as Avro or CSV instead:

```bash
oracle-dmp-converter convert \
  --dump ./input/full.dmp \
  --config ./config.yaml \
  --work-dir ./work \
  --output ./avro \
  --format avro

oracle-dmp-converter convert \
  --dump ./input/full.dmp \
  --config ./config.yaml \
  --work-dir ./work \
  --output ./csv \
  --format csv
```

Convert one known table from a Data Pump dump using hash buckets:

```bash
oracle-dmp-converter convert-hash-table \
  --dump ./input/full.dmp \
  --source-schema SALES \
  --table BIG_FACT \
  --split-column CUSTOMER_ID \
  --buckets 64 \
  --output ./parquet
```

This command starts Oracle Free, mounts the dump directory, imports one bucket at a time into a staging schema, exports each bucket to the chosen output format, then drops the staged table before continuing. Use `--format avro` or `--format csv` to change the output format.

## Config

See [`docs/config.yaml.example`](docs/config.yaml.example) for a fully annotated example. The minimal structure is:

```yaml
oracle:
  image: gvenzl/oracle-free:23-faststart
  max_stage_gb: 8

default_hash_buckets: 64

tables:
  SALES.BIG_FACT:
    strategy: hash
    split_column: CUSTOMER_ID
    buckets: 256
    force_large: true

columns:
  GIS.ROADS.GEOM:
    expression: "SDO_UTIL.TO_WKTGEOMETRY({column})"
    parquet_type: string
```

Config keys (`tables`, `columns`) are looked up case-insensitively — exact match first, then `.upper()`.

## Legacy `exp` dumps

Legacy (`exp`) dumps are auto-detected at inspect time. `impdp SQLFILE=` is attempted first; if Oracle returns `ORA-39142` or `ORA-39143` the converter falls back to `imp INDEXFILE=` for table discovery.

Constraints for legacy dumps:
- Only the **whole-table** strategy is available — `imp` has no `QUERY=` support, so hash chunking is unavailable.
- Partitioned tables are imported as a single unit.

## Output structure

Files are written to `<output_dir>/<schema>/<table>/<chunk>.<ext>` where schema and table names are lowercased and non-alphanumeric characters are replaced with `_`.

| Chunk type | Filename pattern |
|---|---|
| Whole table | `whole.parquet` / `whole.avro` / `whole.csv` |
| Hash bucket | `hash-00000-of-00064.parquet` + `hash-null.parquet` for the null bucket |
| Partition | `partition-00001-<PARTITION_NAME>.parquet` |

## Sample dumps

Create a richer local Data Pump file for manual testing:

```bash
uv run python scripts/create_complex_sample_dump.py --force
```

This writes generated files under `sample-data/complex/`:

- `complex_full.dmp`: full Data Pump dump.
- `config.yaml`: matching converter config with forced hash examples.
- `README.md`: generated row counts and commands.

The sample includes three schemas:

- `D2P_APP`: customers and account settings.
- `D2P_SALES`: partitioned orders, hash-chunk order items, and nullable hash-chunk event facts.
- `D2P_DOCS`: document rows with CLOB and BLOB columns.

Generate a legacy `exp` sample:

```bash
uv run python scripts/create_legacy_exp_sample.py
```

Validate the generated Data Pump sample:

```bash
oracle-dmp-converter inspect \
  --dump sample-data/complex/complex_full.dmp \
  --work-dir sample-data/complex/work \
  --output sample-data/complex/work/manifest.json

oracle-dmp-converter plan \
  --manifest sample-data/complex/work/manifest.json \
  --config sample-data/complex/config.yaml \
  --output sample-data/complex/work/plan.yaml

oracle-dmp-converter convert \
  --plan sample-data/complex/work/plan.yaml \
  --output sample-data/complex/parquet
```

## Constraints

The converter still relies on Oracle Data Pump as the dump reader. It does not directly parse proprietary `.dmp` files.

A single table larger than Oracle Free's staging capacity must be partitioned or split with a usable scalar column. If a table cannot be partitioned and has no usable split column, the tool reports it as `UNSUPPORTED` under the no-paid-license constraint.

The `range` chunking strategy is not implemented — the planner immediately marks such tables as `UNSUPPORTED`.

## Tests

Unit tests:

```bash
uv run python -m pytest tests/unit
```

Integration tests with Docker and Oracle Free:

```bash
uv run python -m pytest tests/integration
```

The integration tests may take several minutes on the first run because the Oracle image must be pulled and the database must initialize.
