---
title: Export to CSV
description: Write CQLite query results to CSV for spreadsheet import or downstream processing.
sidebar:
  label: Export to CSV
  order: 5
---

# Export to CSV

**Task**: Write query results to CSV format.

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out csv
```
<!-- /SMOKE:CLI -->

**Expected output** (real output, written to stdout):

```
id,name,age
0023ece7-7c4e-4705-9068-d1a59ec5fe19,Debbie Soto,79
009fb913-7173-40df-b4ea-67ed6834cfe5,Richard Parker,58
00a74226-9bde-4259-9ba0-d74359e8013e,Andrew Meyers,47
```

**Exit code**: `0` on success.

**Format**: First row is a header with column names. Values use standard CSV escaping (double-quote wrapping for values containing commas or newlines).

## Write to a file

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table" \
  --out csv \
  --output /tmp/simple_table.csv \
  --overwrite
```

## Export time-series data

```bash
cqlite \
  --schema test-data/schemas/time-series.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT sensor_id, timestamp, temperature, humidity FROM test_timeseries.sensor_data LIMIT 5" \
  --out csv
```

**Expected output**:

```
sensor_id,timestamp,temperature,humidity
0284a718-be7b-49e6-b6b9-8e82f5ff1660,2025-10-06 01:00:30.616+0000,-16.17206573486328,92.88220977783203
0284a718-be7b-49e6-b6b9-8e82f5ff1660,2025-10-05 13:48:21.616+0000,16.26676368713379,1.5052613019943237
0284a718-be7b-49e6-b6b9-8e82f5ff1660,2025-10-05 13:34:21.616+0000,1.2208592891693115,41.793243408203125
0284a718-be7b-49e6-b6b9-8e82f5ff1660,2025-10-05 13:08:14.616+0000,19.574493408203125,59.132102966308594
0284a718-be7b-49e6-b6b9-8e82f5ff1660,2025-10-05 05:02:36.616+0000,8.865622520446777,25.052871704101562
```

## Type rendering in CSV

| CQL type | CSV rendering |
|----------|--------------|
| `text`, `varchar`, `ascii` | literal string |
| Numbers (`int`, `bigint`, `float`, `double`) | decimal literal |
| `boolean` | `true` / `false` |
| `uuid`, `timeuuid` | `xxxxxxxx-xxxx-...` |
| `timestamp` | `YYYY-MM-DD HH:MM:SS.mmm+0000` |
| `date` | `YYYY-MM-DD` |
| `blob` | hex-encoded (`0x...`) |
| `list<T>`, `set<T>` | bracket-enclosed, e.g. `[a, b, c]` |
| `map<K,V>` | `{key: value, ...}` |
| `null` | empty field |

## Failure modes

| Symptom | Error | Fix |
|---------|-------|-----|
| No `--schema` flag | `Error: Schema not found for table '...'` | Add `--schema path/to/schema.cql` |
| Output file exists | exit code `6` | Add `--overwrite` |
