# oracle-dmp-converter

Convert Oracle Data Pump (`expdp`) and legacy (`exp`) dump files to Parquet, Avro, or CSV — without a paid Oracle license — by using Oracle Database Free in Docker as a temporary reader.

The converter does not parse `.dmp` files directly. Oracle Data Pump is proprietary, so the reliable path is:

```text
expdp / exp dump -> Oracle Free staging database -> Parquet / Avro / CSV
```

The implementation is chunk-oriented so large dumps can be processed table-by-table or partition-by-partition without requiring the whole dump to fit in Oracle Free at once.

## Implemented workflow

- Python 3.12 project managed by `uv`.
- Docker/Podman-managed Oracle Free container support.
- Data Pump parfile generation for `expdp` and `impdp`.
- Legacy `exp`/`imp` format auto-detection and fallback.
- Full-dump table discovery using Data Pump `SQLFILE` (or `imp INDEXFILE=` for legacy dumps).
- Per-table metadata inspection using `CONTENT=METADATA_ONLY` imports.
- Plan generation for whole-table and partition conversion strategies.
- Real partition-level Data Pump imports.
- Streaming Oracle table export to Parquet, Avro, or CSV with PyArrow / fastavro.
- Row-count validation for each converted chunk.
- SQLite state tracking for resumable chunk conversion.
- Unit tests for deterministic planning/parfile/type behaviour.
- Docker integration tests that create Oracle schemas, export full Data Pump dumps, inspect/plan/convert them, and verify output.

## Default Oracle image

```text
gvenzl/oracle-free:23-faststart
```

Override with `DMP_CONVERTER_IMAGE` (env var) or `--oracle-image` (CLI flag).

Override Docker `--platform` with `DMP_CONVERTER_DOCKER_PLATFORM` (e.g. `linux/amd64` on Apple Silicon).

## Commands

Check runtime prerequisites:

```bash
oracle-dmp-converter doctor
```

Inspect a Data Pump dump (writes `<work-dir>/manifest.json`):

```bash
oracle-dmp-converter inspect \
  --dump ./input/full.dmp \
  --work-dir ./work
```

`--dump` can be repeated for multi-file dump sets; all files must share the same directory.

Create a conversion plan (writes `<manifest-dir>/plan.yaml`):

```bash
oracle-dmp-converter plan \
  --manifest ./work/manifest.json \
  --config ./config.yaml
```

Convert all planned tables (`--output` defaults to `<work-dir>/output`):

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

### Common options

All commands that start an Oracle container accept these options:

| Option | Default | Description |
|---|---|---|
| `--oracle-image IMAGE` | `gvenzl/oracle-free:23-faststart` | Docker image for the staging container |
| `--oracle-password PW` | `OraclePwd_123` | Oracle `system` user password |
| `--container-runtime RT` | `docker` | Container runtime (`docker` or `podman`) |

## Using as a library

The CLI is a thin wrapper around `OracleDMPConverter`. Drive it directly from any Python process:

```python
from pathlib import Path
from oracle_dmp_converter import OracleDMPConverter, ConverterSettings

settings = ConverterSettings(
    dump_paths=(Path("dumps/export01.dmp"),),
    oracle_password="OraclePwd_123",
    work_dir=Path("work"),
    output_dir=Path("work/output"),
)
with OracleDMPConverter(settings) as converter:
    result = converter.run()   # inspect → plan → convert
```

Each phase is also available individually: `converter.inspect()`, `converter.plan(manifest)`, `converter.convert(plan)`. Every phase writes its canonical artifact under `settings.work_dir` (`manifest.json`, `plan.yaml`, `conversion_report.yaml`).

## Config

See [`docs/config.yaml.example`](docs/config.yaml.example) for a fully annotated example. The minimal structure is:

```yaml
oracle:
  image: gvenzl/oracle-free:23-faststart

tables:
  SALES.ORDERS:
    strategy: whole   # force whole-table import for a partitioned table

columns:
  GIS.ROADS.GEOM:
    expression: "SDO_UTIL.TO_WKTGEOMETRY({column})"
    parquet_type: string
```

Config keys (`tables`, `columns`) are looked up case-insensitively — exact match first, then `.upper()`.

### Table strategy overrides

The only supported `strategy` value is `"whole"`, which forces a partitioned table to be imported as a single unit rather than partition-by-partition. Any other value marks the table `UNSUPPORTED` and it is skipped with a warning.

## Oracle-to-Arrow type mapping

| Oracle type | Arrow / Parquet type |
|---|---|
| `NUMBER(p,0)` where `p ≤ 18` | `int64` |
| `NUMBER(p,s)` where `p ≤ 38` | `decimal128(p,s)` |
| `NUMBER` (no bounded precision/scale) | `double` |
| `FLOAT`, `BINARY_FLOAT`, `BINARY_DOUBLE` | `double` |
| `CHAR`, `VARCHAR2`, `NCHAR`, `NVARCHAR2`, `CLOB`, `NCLOB`, `LONG` | `string` |
| `XMLTYPE`, `SDO_GEOMETRY`, `ANYDATA`, `UROWID` | `string` (via `TO_CHAR()`) |
| `INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH` | `string` (via `TO_CHAR()`) |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE` | `string` (via `TO_CHAR()`) |
| `DATE`, `TIMESTAMP` | `timestamp[us]` |
| `RAW`, `LONG RAW`, `BLOB` | `binary` |

Per-column `parquet_type` and `expression` overrides in `config.yaml` take unconditional priority over the defaults above.

## Legacy `exp` dumps

Legacy (`exp`) dumps are auto-detected at inspect time. `impdp SQLFILE=` is attempted first; if Oracle returns `ORA-39142` or `ORA-39143` the converter falls back to `imp INDEXFILE=` for table discovery.

Constraints for legacy dumps:
- Only the **whole-table** strategy is available — `imp` has no `QUERY=` support.
- Partitioned tables are imported as a single unit.

## Output structure

Files are written to `<output_dir>/<schema>/<table>/<chunk>.<ext>` where schema and table names are lowercased and non-alphanumeric characters are replaced with `_`.

| Chunk type | Filename pattern |
|---|---|
| Whole table | `whole.parquet` / `whole.avro` / `whole.csv` |
| Partition | `partition-00001-<PARTITION_NAME>.parquet` |

## Sample dumps

Create a sample dump with multiple schemas and partition types for manual testing:

```bash
uv run python scripts/create_full_combined_dump.py --force
```

This writes generated files under `sample-data/full-combined/`:

- `full_combined_modern.dmp`: Oracle Data Pump (`expdp`) format dump.
- `full_combined_legacy.dmp`: Legacy `exp` format dump.
- `config.yaml`: matching converter config.
- `README.md`: generated schema summary and commands.

The sample includes four schemas:

- `HRDATA`: employees, departments, and jobs (includes `INTERVAL`, `NCLOB`, and `TIMESTAMP WITH TIME ZONE` columns).
- `INVENTORY`: warehouses, products (list-partitioned by region), and stock levels.
- `FINANCE`: accounts and transactions (range-partitioned), materialized view.
- `AUDITLOG`: change log (hash-partitioned).

Validate the generated Data Pump sample:

```bash
oracle-dmp-converter inspect \
  --dump sample-data/full-combined/full_combined_modern.dmp \
  --work-dir sample-data/full-combined/work \
  --output sample-data/full-combined/work/manifest.json

oracle-dmp-converter plan \
  --manifest sample-data/full-combined/work/manifest.json \
  --config sample-data/full-combined/config.yaml \
  --output sample-data/full-combined/work/plan.yaml

oracle-dmp-converter convert \
  --plan sample-data/full-combined/work/plan.yaml \
  --output sample-data/full-combined/parquet
```

## Constraints

The converter relies on Oracle Data Pump as the dump reader. It does not directly parse proprietary `.dmp` files.

A non-partitioned table that is too large for Oracle Free's staging capacity cannot be split — whole-table import is the only available strategy. If the table cannot fit in one import, consider applying an export `QUERY=` filter when creating the original dump to reduce its size.

Tables assigned an unrecognised strategy in the config are marked `UNSUPPORTED` and skipped during conversion.

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
