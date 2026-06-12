---
title: Export to Parquet
description: Write CQLite query results to a Parquet file using the --out parquet flag.
sidebar:
  label: Export to Parquet
  order: 4
---

# Export to Parquet

**Task**: Write query results to a Parquet file for downstream analytics.

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out parquet \
  --output /tmp/simple_table.parquet \
  --overwrite
```
<!-- /SMOKE:CLI -->

**Exit code**: `0` on success. File is created at the path given by `--output`.

**Expected**: `simple_table.parquet` created; file size varies by row count (around 1.3 KB for 3 rows).

## Required flags

| Flag | Purpose |
|------|---------|
| `--out parquet` | Select Parquet output format |
| `--output <path>` | Destination file path (required for Parquet) |
| `--overwrite` | Replace existing file; omit to get exit code `6` on collision |

## Export all rows

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table" \
  --out parquet \
  --output /tmp/all_rows.parquet \
  --overwrite
```

## Export time-series data

```bash
cqlite \
  --schema test-data/schemas/time-series.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT sensor_id, timestamp, temperature, humidity FROM test_timeseries.sensor_data LIMIT 1000" \
  --out parquet \
  --output /tmp/sensor_data.parquet \
  --overwrite
```

## Type fidelity

| CQL type | Parquet logical type |
|----------|---------------------|
| `text`, `varchar`, `ascii` | `STRING` |
| `int`, `smallint`, `tinyint` | `INT32` / `INT16` / `INT8` |
| `bigint`, `counter` | `INT64` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `uuid`, `timeuuid` | `STRING` (UUID format) |
| `timestamp` | `INT64` (microseconds since epoch) |
| `date` | `INT32` (days since epoch) |
| `blob` | `BYTE_ARRAY` |
| `list<T>`, `set<T>` | `LIST` group |
| `map<K,V>` | `MAP` group |

See [Output Formats](/cqlite/user-docs/output-formats/) for the full type map and precision notes.

## Read the Parquet file (Python)

```python
import pyarrow.parquet as pq

table = pq.read_table('/tmp/simple_table.parquet')
print(table.to_pandas())
```

## Failure modes

| Symptom | Error | Fix |
|---------|-------|-----|
| `--output` not provided with `--out parquet` | `Error: --output is required for Parquet format` | Add `--output /path/to/file.parquet` |
| File exists | exit code `6` | Add `--overwrite` |
| No rows matched | Empty Parquet file (0 row groups) | Check WHERE clause and schema |
