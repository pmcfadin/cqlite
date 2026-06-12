---
title: Query from Python
description: Open a CQLite database and run SELECT queries from Python using the cqlite package.
sidebar:
  label: Query from Python
  order: 2
---

# Query from Python

**Task**: Open a Cassandra 5.0 SSTable directory and query it from Python.

## Install

```bash
pip install cqlite
```

Requires Python 3.9+. Pre-built wheels for Linux (x86_64, ARM64), macOS (Intel and Apple Silicon), and Windows (x64). See [Python Bindings](/cqlite/user-docs/python/) for full install details.

## Basic query

<!-- SMOKE:PYTHON -->
```python
import cqlite

with cqlite.open(
    "test-data/datasets/sstables",
    schema="test-data/schemas/basic-types.cql",
) as db:
    for row in db.execute(
        "SELECT id, name, age FROM test_basic.simple_table LIMIT 3"
    ):
        print(row.to_dict())
```
<!-- /SMOKE:PYTHON -->

**Expected output** (real output):

```
{'id': UUID('0023ece7-7c4e-4705-9068-d1a59ec5fe19'), 'name': 'Debbie Soto', 'age': 79}
{'id': UUID('009fb913-7173-40df-b4ea-67ed6834cfe5'), 'name': 'Richard Parker', 'age': 58}
{'id': UUID('00a74226-9bde-4259-9ba0-d74359e8013e'), 'age': 47, 'name': 'Andrew Meyers'}
```

Note: column order in `to_dict()` may vary; do not rely on insertion order.

## Type mapping

Python receives native types — no string serialization:

| CQL type | Python type |
|----------|------------|
| `text`, `varchar`, `ascii` | `str` |
| `int`, `smallint`, `tinyint` | `int` |
| `bigint`, `counter` | `int` |
| `float`, `double` | `float` |
| `boolean` | `bool` |
| `uuid`, `timeuuid` | `uuid.UUID` |
| `timestamp` | `datetime.datetime` (UTC) |
| `date` | `datetime.date` |
| `time` | `datetime.timedelta` |
| `blob` | `bytes` |
| `inet` | `str` (dotted decimal or IPv6) |
| `decimal` | `decimal.Decimal` |
| `varint` | `int` |
| `list<T>` | `list` |
| `set<T>` | `frozenset` |
| `map<K,V>` | `dict` |

## Row access

```python
with cqlite.open("test-data/datasets/sstables",
                 schema="test-data/schemas/basic-types.cql") as db:
    for row in db.execute(
        "SELECT id, name, age FROM test_basic.simple_table LIMIT 1"
    ):
        # Dict-style access
        d = row.to_dict()
        print(d["name"])        # "Debbie Soto"

        # Attribute access
        print(row["name"])      # "Debbie Soto"

        # Column names
        print(row.column_names) # ['id', 'name', 'age']
```

## Filtering with WHERE

```python
with cqlite.open("test-data/datasets/sstables",
                 schema="test-data/schemas/basic-types.cql") as db:
    for row in db.execute(
        "SELECT id, name, age FROM test_basic.simple_table"
        " WHERE age > 70 LIMIT 3"
    ):
        print(row.to_dict())
```

**Expected** (first 3 rows where age > 70):

```
{'id': UUID('0023ece7-...'), 'name': 'Debbie Soto', 'age': 79}
{'id': UUID('029fc1c4-...'), 'name': 'Angela Davis', 'age': 72}
{'id': UUID('05d341e9-...'), 'name': 'Sabrina Rodriguez', 'age': 78}
```

## Querying collections

```python
with cqlite.open("test-data/datasets/sstables",
                 schema="test-data/schemas/collections.cql") as db:
    for row in db.execute(
        "SELECT id, tags, scores FROM test_collections.collection_table LIMIT 2"
    ):
        d = row.to_dict()
        print("tags:", d["tags"])    # frozenset — SET<TEXT>
        print("scores:", d["scores"]) # list — LIST<INT>
```

**Expected**:

```
tags: frozenset({'smile', 'dinner', 'media'})
scores: [78, 12, 11, 31, 83, 30, 10, 38, 40]
```

## Failure modes

| Symptom | Exception | Fix |
|---------|-----------|-----|
| No `schema=` argument | `cqlite.CqliteError: Schema not found for table '...'` | Pass `schema=` path |
| Path does not exist | `cqlite.CqliteError: ...` (IO category) | Check path |
| Table not in schema | `cqlite.CqliteError: Schema not found for table '...'` | Verify table name |
| BTI-format SSTables | Query succeeds but 0 rows | BTI format not yet supported |
