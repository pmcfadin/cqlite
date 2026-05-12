# Write Support (M5)

CQLite v0.9.0 ships write support across the Rust core, Python bindings, Node.js
bindings, and CLI. Written data lands in portable Cassandra 5.0 BIG-format SSTables
that Cassandra can read directly via `nodetool refresh`.

For a concise list of current limitations see
[docs/write-support-limitations.md](write-support-limitations.md).

---

## Architecture

```
User call (CQL text or Mutation struct)
        |
        v
   WriteEngine
  /     |      \
WAL  Memtable  (auto-flush trigger)
               |
               v
         SSTableWriter  ──>  nb-{gen}-big-*.{ext}  (BIG format)
               |
               v
       STCS Compaction (maintenance_step)
```

1. Every write is appended to the WAL for durability.
2. The WAL entry is inserted into the memtable.
3. When the memtable exceeds the flush threshold (default 64 MB) or when you call
   `flush()` / `flush_run()` / `--flush`, the memtable is serialised to a new
   SSTable generation.
4. `maintenance_step()` runs Size-Tiered Compaction (STCS) within a time budget,
   merging small SSTables into larger ones and dropping overwritten cells.

---

## Rust API

```rust
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

// 1. Build configuration
let config = WriteEngineConfig::new(
    PathBuf::from("data"),          // SSTable output directory
    PathBuf::from("data/wal"),      // WAL directory
    schema,                         // TableSchema loaded from .cql file
);

// 2. Open engine (replays WAL on startup)
let mut engine = WriteEngine::new(config)?;

// 3a. Write via the mutation API (no parser dependency)
let mutation = Mutation::new(
    TableId::new("my_keyspace", "my_table"),
    PartitionKey::single("id", Value::Integer(1)),
    None,   // clustering key
    vec![CellOperation::Write {
        column: "name".into(),
        value: Value::Text("Alice".into()),
    }],
    chrono::Utc::now().timestamp_micros(),
    None,   // TTL
);
engine.write(mutation)?;

// 3b. Write via CQL (convenience; requires CQL parser feature)
engine.execute("INSERT INTO my_keyspace.my_table (id, name) VALUES (2, 'Bob')")?;

// 4. Flush to SSTable
let info = engine.flush().await?;
println!("Flushed to {:?}", info.map(|i| i.data_path));

// 5. Background compaction
let budget = std::time::Duration::from_millis(100);
let report = engine.maintenance_step(budget)?;
if report.pending_compaction {
    println!("More compaction work available");
}
```

---

## Python Quickstart

```python
import cqlite

# Open in writable mode
with cqlite.open(
    'test-data/datasets/sstables',
    schema='test-data/schemas/write-test.cql',
    writable=True,
    write_dir='/tmp/my-writes',
) as db:
    # Write via CQL
    db.execute(
        "INSERT INTO test_basic.simple_table (id, name, age) "
        "VALUES (11111111-1111-1111-1111-111111111111, 'Alice', 30)"
    )

    # Flush memtable to SSTable
    path = db.flush_run()
    print(f'Flushed to: {path}')

    # Optional: run background compaction
    report = db.maintenance_step(budget_ms=100)
    print(f'Merged {report.rows_merged} rows in {report.time_spent_ms:.1f} ms')

    # Write statistics
    stats = db.write_stats
    print(f'Total flushed: {stats.total_written_bytes} bytes')
```

See the full Python API in [bindings/python/README.md](../bindings/python/README.md).

---

## Node.js Quickstart

```javascript
const { Database } = require('@cqlite/node');

const db = await Database.open('test-data/datasets/sstables', {
  schema: 'test-data/schemas/write-test.cql',
  writable: true,
  writeDir: '/tmp/my-writes',
});

// Write via CQL
await db.execute(
  "INSERT INTO test_basic.simple_table (id, name, age) " +
  "VALUES (22222222-2222-2222-2222-222222222222, 'Bob', 25)"
);

// Flush to SSTable
const path = await db.flushRun();
console.log('Flushed to:', path);

// Optional: background compaction
const report = await db.maintenanceStep({ budgetMs: 100 });
console.log(`Merged ${report.rowsMerged} rows in ${report.timeSpentMs}ms`);

// Write statistics
const stats = db.writeStats;
console.log('Total bytes flushed:', stats.totalWrittenBytes);

await db.close();
```

See the full Node.js API in [bindings/node/README.md](../bindings/node/README.md).

---

## CLI Quickstart

```bash
# Build with write support enabled
cargo build --package cqlite-cli --features write-support

# Write via CQL INSERT
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql \
  --execute "INSERT INTO test_basic.simple_table (id, name, age) \
             VALUES (33333333-3333-3333-3333-333333333333, 'Carol', 28)"

# Flush memtable to SSTable
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql \
  --flush

# Run compaction (100 ms budget)
cargo run --package cqlite-cli --features write-support -- \
  maintenance --budget-ms 100 \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql

# Print write statistics
cargo run --package cqlite-cli --features write-support -- \
  write-stats \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql
```

### JSON mutation format (CLI --mutation flag)

You can bypass the CQL parser and write mutations directly in JSON. This is the
lowest-overhead path and has no parser dependency:

```bash
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql \
  --mutation '{
    "table":{"keyspace":"test_basic","table":"simple_table"},
    "partition_key":{"columns":[["id",{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}]]},
    "clustering_key":null,
    "operations":[{"Write":{"column":"name","value":{"Text":"Alice"}}}],
    "timestamp_micros":1704067200000000,
    "ttl_seconds":null,
    "partition_tombstone":null,
    "range_tombstones":[]
  }'
```

For bulk imports, put one JSON object per line in a `.jsonl` file and pass it with
`--mutations-file path/to/mutations.jsonl`.

---

## Cassandra Export Workflow

After flush, CQLite produces files named `nb-{gen}-big-*.{ext}` directly inside
`write_dir/data/{keyspace}/{table}/`. These are standard Cassandra BIG-format
SSTable components. Copy them into the Cassandra data directory and refresh:

```bash
# 1. Flush to portable SSTables
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema schema.cql --flush

# 2. Copy into Cassandra table directory
# (Replace the UUID with the actual table directory name from Cassandra)
cp /tmp/my-writes/data/my_ks/my_tbl/nb-*-big-* \
   /var/lib/cassandra/data/my_ks/my_tbl-<uuid>/

# 3. Tell Cassandra to discover the new SSTables
nodetool refresh my_ks my_tbl

# 4. Verify
cqlsh -e "SELECT COUNT(*) FROM my_ks.my_tbl"
```

The `export-sstable` subcommand packages the current write directory SSTables into
a separate export path:

```bash
cargo run --package cqlite-cli --features write-support -- \
  export-sstable /tmp/export --keyspace my_ks --table my_tbl \
  --writable --write-dir /tmp/my-writes \
  --schema schema.cql
```

---

## Type Support

All CQL types roundtrip through write→flush→read:

| Category | Types |
|----------|-------|
| Primitive | `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double` |
| Text | `text`, `varchar`, `ascii` |
| Binary | `blob` |
| Temporal | `timestamp`, `date`, `time`, `duration` |
| Identity | `uuid`, `timeuuid` |
| Network | `inet` |
| Numeric | `varint`, `decimal` |
| Collections | `list<T>`, `set<T>`, `map<K,V>` |
| Structured | `tuple<...>`, `frozen<T>`, UDT |

Counter columns are **not writable** — the write engine returns
`Error::InvalidOperation` when a counter cell is submitted. See
[docs/write-support-limitations.md](write-support-limitations.md) for details.
