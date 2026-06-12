---
title: Read collections
description: Query LIST, SET, and MAP columns from Cassandra 5.0 SSTables using CQLite.
sidebar:
  label: Read collections
  order: 8
---

# Read collections

**Task**: Query tables with `LIST`, `SET`, and `MAP` columns.

## CLI: LIST and SET columns

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/collections.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, tags, scores FROM test_collections.collection_table LIMIT 2" \
  --out json
```
<!-- /SMOKE:CLI -->

**Expected output** (real output):

```json
[
  {
    "id": "00a6220f-d42c-4f49-938e-08ada7ddc398",
    "tags": ["dinner", "media", "smile"],
    "scores": [78, 12, 11, 31, 83, 30, 10, 38, 40]
  },
  {
    "id": "013ba6da-bce5-4cd8-b3a0-a5b20c211e4c",
    "tags": ["base", "draw", "girl", "last", "parent", "perhaps", "safe", "single", "student"],
    "scores": [78, 58, 57, 54, 98, 2]
  }
]
```

In JSON output:
- `SET<TEXT>` columns render as a JSON array (order is not guaranteed — a set has no defined order).
- `LIST<INT>` columns render as a JSON array (preserving insertion order).

## CLI: MAP columns

```bash
cqlite \
  --schema test-data/schemas/collections.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, properties FROM test_collections.collection_table LIMIT 2" \
  --out json
```

**Expected**:

```json
[
  {
    "id": "00a6220f-d42c-4f49-938e-08ada7ddc398",
    "properties": {
      "key1": "value1",
      "key2": "value2"
    }
  }
]
```

`MAP<TEXT, TEXT>` renders as a JSON object with string keys.

## Python: collections as native types

```python
import cqlite

with cqlite.open(
    "test-data/datasets/sstables",
    schema="test-data/schemas/collections.cql",
) as db:
    for row in db.execute(
        "SELECT id, tags, scores FROM test_collections.collection_table LIMIT 2"
    ):
        d = row.to_dict()
        print("tags:", d["tags"])    # frozenset — SET<TEXT>
        print("scores:", d["scores"]) # list — LIST<INT>
```

**Expected output** (real output):

```
tags: frozenset({'smile', 'dinner', 'media'})
scores: [78, 12, 11, 31, 83, 30, 10, 38, 40]
```

Python type mapping for collections:
- `SET<T>` → `frozenset` (iteration order unspecified)
- `LIST<T>` → `list` (order preserved)
- `MAP<K,V>` → `dict`

## Schema for collections

The `test_collections.collection_table` schema:

```sql
CREATE TABLE test_collections.collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<TEXT, TEXT>,
    numbers_set SET<INT>,
    ordered_values LIST<TIMESTAMP>,
    metadata_map MAP<TEXT, BIGINT>
);
```

## Nested/frozen collections

Frozen collections (`FROZEN<LIST<INT>>`, `FROZEN<SET<TEXT>>`, etc.) serialize the same way as their non-frozen equivalents in query output.

## Known limitations

- `set` element tombstones (partial set deletions) are not fully supported; a set with partial tombstones may return extra elements. See issue #493.
- Collection ordering in `SET` output is not deterministic; do not assert a specific order in tests.

## Failure modes

| Symptom | Error | Fix |
|---------|-------|-----|
| Empty array for collection column | Null / empty collection in data | Normal; row may have no items |
| Wrong schema file | `Schema not found` | Use `collections.cql` for `test_collections.*` tables |
