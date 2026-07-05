---
title: Write a mutation and flush
description: Insert a row into a CQLite write-enabled database and flush the memtable to an SSTable file.
sidebar:
  label: Write mutation + flush
  order: 10
---

# Write a mutation and flush

**Task**: Insert a row via the JSON mutation format and flush the in-memory write buffer to disk.

**Prerequisite**: The CLI must be built with write support:

```bash
cargo build --package cqlite-cli --features write-support
```

The write-support flag adds `--writable`, `--write-dir`, `--mutation`, `--flush`, and write subcommands.

## Write a single row

<!-- SMOKE:CLI:write -->
```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  --mutation '{
    "table": {"keyspace": "test_basic", "table": "simple_table"},
    "partition_key": {"columns": [["id", {"Uuid": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}]]},
    "clustering_key": null,
    "operations": [{"Write": {"column": "name", "value": {"Text": "Agent Test"}}}],
    "timestamp_micros": 1704067200000000,
    "ttl_seconds": null,
    "partition_tombstone": null,
    "range_tombstones": []
  }'
```
<!-- /SMOKE:CLI:write -->

**Expected output** (real output):

```
OK: 1 row(s) affected (4.5ms)
CQLite CLI v0.13.0
Use --help for available commands
```

**Exit code**: `0` on success.

The row is written to the WAL (`/tmp/cqlite-write/wal/commitlog.wal`) and held in the memtable. It is not yet queryable from the SSTable path until flushed.

## Flush the memtable to SSTable

```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  --flush
```

**Expected output**:

```
Flushed: 1 partitions, 50 bytes
  Output: /tmp/cqlite-write/data/test_basic/simple_table/nb-1-big-Data.db
CQLite CLI v0.13.0
Use --help for available commands
```

The flushed SSTable is written to `--write-dir/data/<keyspace>/<table>/`.

## Combine: write + flush in one command

```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  --mutation '{
    "table": {"keyspace": "test_basic", "table": "simple_table"},
    "partition_key": {"columns": [["id", {"Uuid": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}]]},
    "clustering_key": null,
    "operations": [{"Write": {"column": "name", "value": {"Text": "Agent Test"}}}],
    "timestamp_micros": 1704067200000000,
    "ttl_seconds": null,
    "partition_tombstone": null,
    "range_tombstones": []
  }' \
  --flush
```

**Expected output**:

```
OK: 1 row(s) affected (20.2ms)
Flushed: 1 partitions, 50 bytes
  Output: /tmp/cqlite-write/data/test_basic/simple_table/nb-1-big-Data.db
CQLite CLI v0.13.0
Use --help for available commands
```

## Mutation JSON format

```jsonc
{
  // Required: target table
  "table": {
    "keyspace": "test_basic",
    "table": "simple_table"
  },

  // Required: partition key — array of [column_name, value] pairs
  // value type tags: Uuid, Integer, Text, BigInt, Boolean, Float, Double, Blob, ...
  "partition_key": {
    "columns": [
      ["id", {"Uuid": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}]
    ]
  },

  // Required: clustering key or null for tables without clustering columns
  "clustering_key": null,

  // Required: list of cell operations
  "operations": [
    {"Write": {"column": "name", "value": {"Text": "some text"}}},
    {"Write": {"column": "age", "value": {"Integer": 42}}}
  ],

  // Required: microseconds since Unix epoch
  "timestamp_micros": 1704067200000000,

  // Optional: row TTL in seconds (null = no expiration)
  "ttl_seconds": null,

  // Optional: partition tombstone (deletes entire partition)
  "partition_tombstone": null,

  // Optional: range tombstones (delete clustering key ranges)
  "range_tombstones": []
}
```

## Value type tags

| CQL type | JSON tag | Example |
|----------|----------|---------|
| `UUID` | `"Uuid"` | `{"Uuid": [1,2,...,16]}` (16-byte array) |
| `INT` | `"Integer"` | `{"Integer": 42}` |
| `TEXT` | `"Text"` | `{"Text": "hello"}` |
| `BIGINT` | `"BigInt"` | `{"BigInt": 9223372036854775807}` |
| `BOOLEAN` | `"Boolean"` | `{"Boolean": true}` |
| `FLOAT` | `"Float"` | `{"Float": 3.14}` |
| `DOUBLE` | `"Double"` | `{"Double": 3.141592653589793}` |
| `BLOB` | `"Blob"` | `{"Blob": [72,101,108,108,111]}` (byte array) |

## Write stats

Check the write engine state:

```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  write-stats
```

**Expected output**:

```
Write Engine Statistics:
  Memtable size: 0 bytes
  Memtable rows: 0
  WAL size: 0 bytes
  Generation: 2
```

(After a flush, memtable size and rows reset to 0; generation increments.)

## Failure modes

| Symptom | Error | Fix |
|---------|-------|-----|
| `--writable` omitted | `Error: Export requires --writable mode` | Add `--writable --write-dir /path` |
| Wrong value type tag | `Error: Type mismatch: value Integer(42) does not match comparator Uuid` | Use correct value tag for column type |
| Bad JSON (wrong format) | `Error: Failed to parse mutation JSON: ...` | Ensure JSON matches the Mutation schema above |
| `Nothing to flush (memtable empty)` | Flush called before any mutations | Write at least one mutation first |
