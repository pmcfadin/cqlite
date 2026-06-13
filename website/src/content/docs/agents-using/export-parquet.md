---
title: Export to Parquet
description: Write CQLite query results to a Parquet file from the CLI, Python, Node.js, or embedded Rust.
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

When the query runs against a schema (the normal case), columns export with high-fidelity Arrow/Parquet types, recursively for nested types (epic #673):

| CQL type | Arrow/Parquet type |
|----------|--------------------|
| `text`, `varchar`, `ascii` | `Utf8` (`STRING`) |
| `int`, `smallint`, `tinyint` | `Int32` / `Int16` / `Int8` |
| `bigint`, `counter` | `Int64` |
| `float` / `double` | `Float32` / `Float64` |
| `boolean` | `Boolean` |
| `uuid`, `timeuuid` | `FixedSizeBinary(16)` with the `arrow.uuid` extension annotation |
| `timestamp` | `Timestamp(Millisecond, UTC)` |
| `date` | `Date32` |
| `time` | `Time64(Nanosecond)` |
| `decimal` | `Decimal128(38, 9)` (checked rescale; overflow is an error) |
| `varint` | `Decimal128(38, 0)` |
| `duration` | `Utf8` (CQL text form; Parquet cannot encode `Interval(MonthDayNano)`) |
| `inet` | `Utf8` (canonical text) |
| `blob` | `Binary` (`BYTE_ARRAY`) |
| `list<T>`, `set<T>` | `List<T>` with typed elements |
| `map<K,V>` | `Map<K, V>` with typed keys and values |
| `tuple<...>` | `Struct` with positional `field_N` children |
| UDT | `Struct` with the UDT's field names |
| `frozen<T>` | Same as `T` (transparent) |

See [Output Formats](/cqlite/user-docs/output-formats/) for the full type map and precision notes.

## Export from Python (no CLI)

The Python bindings expose the same writer directly (Epic #682) — no
subprocess needed:

```python
import cqlite

with cqlite.open('test-data/datasets/sstables',
                 schema='test-data/schemas/basic-types.cql') as db:
    rows = db.export_parquet(
        'SELECT * FROM test_basic.simple_table',
        '/tmp/simple_table.parquet',
        row_group_size=10000,      # rows per Parquet row group
        compression='snappy',      # 'snappy' (default), 'zstd', or 'none'
    )
    print(f'Exported {rows} rows')
```

The query streams, so large tables export within bounded memory, and the
GIL is released for the duration of the export.

## Export from Node.js (no CLI)

```javascript
const { Database } = require('@cqlite/node');

const db = await Database.open('test-data/datasets/sstables', {
  schema: 'test-data/schemas/basic-types.cql',
});
const rows = await db.exportParquet(
  'SELECT * FROM test_basic.simple_table',
  '/tmp/simple_table.parquet',
  { rowGroupSize: 10000, compression: 'snappy' }
);
console.log(`Exported ${rows} rows`);
await db.close();
```

The export runs as an async task off the JavaScript main thread.

## Export from Rust (library embedders)

The writer lives in `cqlite-core` behind the off-by-default `parquet`
cargo feature:

```toml
[dependencies]
cqlite-core = { version = "*", features = ["parquet"] }
```

```rust
use cqlite_core::export::parquet::{ParquetExportOptions, StreamingParquetWriter};

let mut iter = db.execute_streaming(query, Default::default()).await?;
let file = std::fs::File::create("/tmp/out.parquet")?;
let mut writer = StreamingParquetWriter::new(file, &iter.metadata, &ParquetExportOptions::default())?;
while let Some(row) = iter.next_async().await {
    writer.write_chunk(&[row?])?;
}
writer.finalize()?;
```

CQLite produces Parquet **files** only; committing them to Iceberg/Delta
table formats is an external committer's job.

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
