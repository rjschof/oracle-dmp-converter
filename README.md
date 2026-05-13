# dmp-to-parquet

Convert Oracle Data Pump (`expdp`) dump files to Parquet without a paid Oracle license by using Oracle Database Free in Docker as a temporary reader.

The converter does not parse `.dmp` files directly. Oracle Data Pump is proprietary, so the reliable path is:

```text
expdp dump -> Oracle Free staging database -> streamed Parquet files
```

The implementation is intentionally chunk-oriented so large dumps can be processed table-by-table, partition-by-partition, or hash-bucket-by-hash-bucket without requiring the whole dump to fit in Oracle Free at once.

## Implemented workflow

This build includes:

- Python 3.12 project managed by `uv`.
- Docker-managed Oracle Free container support.
- Data Pump parfile generation for `expdp` and `impdp`.
- Full-dump table discovery using Data Pump `SQLFILE`.
- Per-table metadata inspection using `CONTENT=METADATA_ONLY` imports.
- Plan generation for whole-table, partition, and hash-bucket conversion.
- Real Data Pump import using `QUERY` hash buckets.
- Real partition-level Data Pump imports.
- Streaming Oracle table export to Parquet with PyArrow.
- Row-count validation helpers.
- SQLite state tracking for resumable chunk conversion.
- Unit tests for deterministic planning/parfile/type behavior.
- Docker integration tests that create Oracle schemas, export full Data Pump dumps, inspect/plan/convert them, and verify Parquet output.

## Default Oracle image

```text
gvenzl/oracle-free:23-slim
```

Override with `DMP_TO_PARQUET_ORACLE_IMAGE` in tests or `--oracle-image` in CLI commands.

## Commands

```bash
uv run dmp-to-parquet doctor
```

Inspect a full Data Pump dump:

```bash
uv run dmp-to-parquet inspect \
  --dump ./input/full.dmp \
  --work-dir ./work \
  --output ./work/manifest.json
```

Create a conversion plan:

```bash
uv run dmp-to-parquet plan \
  --manifest ./work/manifest.json \
  --config ./config.yaml \
  --output ./work/plan.yaml
```

Convert all planned tables:

```bash
uv run dmp-to-parquet convert \
  --plan ./work/plan.yaml \
  --output ./parquet
```

One-shot inspect, plan, and convert:

```bash
uv run dmp-to-parquet convert \
  --dump ./input/full.dmp \
  --config ./config.yaml \
  --work-dir ./work \
  --output ./parquet
```

Convert one known table from a Data Pump dump using hash buckets:

```bash
uv run dmp-to-parquet convert-hash-table \
  --dump ./input/full.dmp \
  --source-schema SALES \
  --table BIG_FACT \
  --split-column CUSTOMER_ID \
  --buckets 64 \
  --output ./parquet
```

This command starts Oracle Free, mounts the dump directory, imports one bucket at a time into a staging schema, exports each bucket to Parquet, then drops the staged table before continuing.

## Config

```yaml
oracle:
  image: gvenzl/oracle-free:23-slim
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

## Sample Dump

Create a richer local Data Pump file for manual testing with the standalone script:

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

Validate the generated sample:

```bash
uv run dmp-to-parquet inspect \
  --dump sample-data/complex/complex_full.dmp \
  --work-dir sample-data/complex/work \
  --output sample-data/complex/work/manifest.json

uv run dmp-to-parquet plan \
  --manifest sample-data/complex/work/manifest.json \
  --config sample-data/complex/config.yaml \
  --output sample-data/complex/work/plan.yaml

uv run dmp-to-parquet convert \
  --plan sample-data/complex/work/plan.yaml \
  --output sample-data/complex/parquet
```

## Constraints

The converter still relies on Oracle Data Pump as the dump reader. It does not directly parse proprietary `.dmp` files.

A single table larger than Oracle Free's staging capacity must be partitioned or split with a usable scalar column. If a giant table cannot be partitioned and has no usable split column, the tool reports it as unsupported under the no-paid-license constraint.

## Tests

Unit tests:

```bash
uv run pytest tests/unit
```

Integration tests with Docker and Oracle Free:

```bash
uv run pytest tests/integration
```

The integration test may take several minutes on the first run because the Oracle image must be pulled and the database must initialize.
