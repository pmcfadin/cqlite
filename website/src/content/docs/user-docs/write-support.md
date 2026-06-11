---
title: Write Support
description: Write Cassandra 5.0 SSTables offline with CQLite's write engine (M5).
sidebar:
  label: Write Support
  order: 7
---

# Write Support

CQLite v0.9.0 (M5) ships offline SSTable write support across all interfaces:
Rust core, Python, Node.js, and CLI. Written data flushes to portable Cassandra 5.0
BIG-format SSTables that Cassandra can load with `nodetool refresh`.

For known limitations (promoted index, compaction deferral, etc.) see
[Limitations](/cqlite/user-docs/limitations/#write-support-limitations).

## CLI

Build with write support enabled:

```bash
cargo build --package cqlite-cli --features write-support --release
```

### Write a mutation

```bash
cqlite \
  --writable \
  --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{
    "table":{"keyspace":"test_basic","table":"simple_table"},
    "partition_key":[{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}],
    "clustering_key":[],
    "operations":[{"Write":{"column":"name","value":{"Text":"Alice"}}}],
    "timestamp_micros":1704067200000000
  }'
```

### Flush memtable to an SSTable file

```bash
cqlite \
  --writable \
  --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --flush
```

### Write subcommands

```bash
# Run incremental maintenance (flush + partial compaction)
cqlite maintenance --budget-ms 100 \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

# Show write-engine statistics
cqlite write-stats \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

# Export flushed SSTables to a distribution directory
cqlite export-sstable /tmp/export --keyspace my_ks --table my_tbl \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql
```

## Python

```python
import cqlite

# Open in writable mode — write_dir stores the WAL and flushed SSTables
with cqlite.open(
    'test-data/datasets/sstables',
    schema='test-data/schemas/write-test.cql',
    writable=True,
    write_dir='/tmp/my-writes',
) as db:
    db.execute(
        "INSERT INTO test_basic.simple_table (id, name, age) "
        "VALUES (11111111-1111-1111-1111-111111111111, 'Alice', 30)"
    )
    path = db.flush_run()
    print(f'Flushed SSTable: {path}')
```

## Node.js

```javascript
const { Database } = require('@cqlite/node');

const db = await Database.open('test-data/datasets/sstables', {
  schema: 'test-data/schemas/write-test.cql',
  writable: true,
  writeDir: '/tmp/my-writes',
});

await db.execute(
  "INSERT INTO test_basic.simple_table (id, name, age) " +
  "VALUES (11111111-1111-1111-1111-111111111111, 'Bob', 25)"
);
const path = await db.flushRun();
console.log('Flushed SSTable:', path);
await db.close();
```

## Load into Cassandra

Once you have flushed SSTable files, load them into a running Cassandra 5.0 cluster
by copying the files to the appropriate data directory and running:

```bash
# Copy SSTable files to Cassandra data directory
cp /tmp/my-writes/*.db \
   /var/lib/cassandra/data/test_basic/simple_table-<uuid>/

# Tell Cassandra to pick them up
nodetool refresh test_basic simple_table
```

## Schema file format

CQLite write operations require a `.cql` schema file that defines the keyspace and
table structure:

```cql
CREATE KEYSPACE test_basic
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_basic;

CREATE TABLE simple_table (
    id   UUID    PRIMARY KEY,
    name TEXT,
    age  INT
);
```

A sample schema is included in the repository at
`test-data/schemas/write-test.cql`.

## Supported write operations

| Operation | Status |
|-----------|--------|
| INSERT / upsert | Full |
| UPDATE (column write) | Full |
| DELETE (partition or row) | Full |
| DELETE (column) | Full |
| TTL (expiring cells) | Full |
| Frozen collections | Full |
| Non-frozen collections (list, set, map) | Full |
| Static columns | Full |
| Composite partition keys | Full |
| Counter columns | Not yet supported |
| Lightweight transactions (CAS) | Not applicable (offline) |

## Compaction status

The k-way merge compaction API (`STCSPolicy`, `maintenance_step()`) is defined and
tested, but execution requires M5.3 reader integration. `maintenance_step()`
currently performs flush operations only; `set_merge_policy()` returns an error.
Full compaction support is planned for M5.3.
