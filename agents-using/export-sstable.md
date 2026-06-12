---
title: Export SSTable for Cassandra import
description: Re-export CQLite-written rows as Cassandra-compatible nb-format SSTables for sstableloader import.
sidebar:
  label: Export SSTable
  order: 11
---

# Export SSTable for Cassandra import

**Task**: Export rows written through CQLite's write engine as Cassandra-compatible SSTable files for use with `sstableloader` or direct Cassandra data directory placement.

**Prerequisite**: The CLI must be built with write support:

```bash
cargo build --package cqlite-cli --features write-support
```

## Full workflow: write, flush, then export

The `export-sstable` subcommand re-exports data from the write engine's SSTable output. You must write and flush mutations before exporting. The three steps below run in sequence.

<!-- SMOKE:CLI:write -->
```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":{"columns":[["id",{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}]]},"clustering_key":null,"operations":[{"Write":{"column":"name","value":{"Text":"Export Test"}}}],"timestamp_micros":1704067200000000,"ttl_seconds":null,"partition_tombstone":null,"range_tombstones":[]}' \
  --flush && \
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  --writable \
  --write-dir /tmp/cqlite-write \
  export-sstable /tmp/cqlite-export \
  --keyspace test_basic \
  --table simple_table
```
<!-- /SMOKE:CLI:write -->

**Expected output** (real output):

```
Export complete:
  Output: /tmp/cqlite-export/test_basic/simple_table
  Rows: 2
  Size: 50 bytes
  Time: 2.0ms
```

**Exit code**: `0` on success.

The exported SSTable files are placed at:

```
/tmp/cqlite-export/
└── test_basic/
    └── simple_table/
        └── nb-1-big-Data.db
        ...
```

## Export flags

| Flag | Required | Description |
|------|----------|-------------|
| `<output-dir>` | Yes | Base directory for exported SSTables |
| `--keyspace` | Yes | Keyspace to export |
| `--table` | Yes | Table to export |

## Use with sstableloader

To import exported SSTables into a running Cassandra cluster:

```bash
sstableloader -d <cassandra-host> /tmp/cqlite-export/test_basic/simple_table
```

The output directory structure (`<keyspace>/<table>/`) matches the path layout expected by `sstableloader`.

**Note**: CQLite writes `nb`-format (Cassandra 5.0 BigTable) SSTables. These are compatible with Cassandra 5.0+. Cassandra 4.x uses `mc`-format and cannot read `nb` files directly.

## Export requires --writable mode

If `--writable` is omitted, the export subcommand fails:

```bash
cqlite \
  --schema test-data/schemas/write-test.cql \
  --data-dir test-data/datasets/sstables \
  export-sstable /tmp/export --keyspace test_basic --table simple_table
```

**Error**:

```
Error: Export requires --writable mode
```

**Exit code**: `0` (error is printed but exits cleanly).

**Fix**: Add `--writable --write-dir /path/to/write-dir` before the subcommand.

## Failure modes

| Symptom | Error | Fix |
|---------|-------|-----|
| `--writable` omitted | `Error: Export requires --writable mode` | Add `--writable --write-dir <dir>` |
| Output dir already has files | Re-exports cleanly (does not fail) | Safe to re-run |
| Empty export (Rows: 0) | No mutations flushed to `--write-dir` | Write and flush mutations first |
| Wrong `--write-dir` | Empty export or IO error | Use the same `--write-dir` as your write operations |
