---
title: Inspect schema
description: Discover which tables, keyspaces, and columns are available in an SSTable directory.
sidebar:
  label: Inspect schema
  order: 6
---

# Inspect schema

**Task**: Discover the tables and columns available in a data directory before writing queries.

## Verify schema loads correctly

Run a deliberately limited query to confirm schema loading and data accessibility:

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name FROM test_basic.simple_table LIMIT 1" \
  --out json
```
<!-- /SMOKE:CLI -->

**Expected** (confirms schema loaded and data is accessible):

```json
[
  {
    "id": "0023ece7-7c4e-4705-9068-d1a59ec5fe19",
    "name": "Debbie Soto"
  }
]
```

If this returns an empty array or an error, see [Failure modes](#failure-modes) below.

## Discover available tables

List every `Data.db` file in the dataset root to enumerate keyspaces and tables:

```bash
find test-data/datasets/sstables -name "*-Data.db" \
  | sed 's|.*/\([^/]*\)/[^/]*-Data.db|\1|' \
  | sort -u
```

**Expected output** (test datasets):

```
collection_clustering_table-6bf78680a25111f0a3fef1a551383fb9
collection_table-6b8c8fb0a25111f0a3fef1a551383fb9
sensor_data-...
simple_table-...
...
```

Each SSTable directory is named `<table_name>-<uuid_hash>`. The prefix before the first `-` is the table name. Keyspace directories contain one or more such table directories.

## Discover available keyspaces

```bash
ls -1 test-data/datasets/sstables/
```

**Expected output**:

```
system
system_auth
system_distributed
system_schema
system_traces
test_basic
test_collections
test_timeseries
test_wide_rows
```

The `system_*` keyspaces contain Cassandra internal metadata. The `test_*` keyspaces contain application data.

## Read the schema file

```bash
grep -E "^(CREATE TABLE|USE )" test-data/schemas/basic-types.cql
```

**Expected output** (enumerates keyspace and table names):

```
USE test_basic;
CREATE TABLE IF NOT EXISTS simple_table (
CREATE TABLE IF NOT EXISTS multi_pk_table (
CREATE TABLE IF NOT EXISTS typed_table (
...
```

## Inspect column types for a specific table

```bash
awk '/CREATE TABLE IF NOT EXISTS simple_table/,/^\)/' \
  test-data/schemas/basic-types.cql
```

**Expected output** (column definitions for `simple_table`):

```
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    ...
)
```

## Available schema files

| Schema file | Keyspace | Tables |
|-------------|----------|--------|
| `basic-types.cql` | `test_basic` | `simple_table`, `multi_pk_table`, `typed_table`, and others |
| `collections.cql` | `test_collections` | `collection_table`, `nested_collections_table`, and others |
| `time-series.cql` | `test_timeseries` | `sensor_data`, `events_table`, and others |
| `wide-rows.cql` | `test_wide_rows` | `wide_partition_table`, `many_columns_table`, and others |

## Failure modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Zero rows returned | Data.db file missing (only JSONL goldens present) | Run `bash test-data/scripts/fetch-datasets.sh` |
| `Schema not found` error | Wrong table name in query, or wrong `--schema` file | Check schema file for exact table name |
| Empty `ls` output | Dataset not fetched | Run `bash test-data/scripts/fetch-datasets.sh` |
