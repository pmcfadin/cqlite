# Using `cqlite-core` as a dependency

This page is for developers who want to embed `cqlite-core` (the Rust library) in
their own project — reading and/or writing Cassandra 5.0 SSTables without a
cluster. It covers the dependency line, the feature flags you need, and a
**compiling** end-to-end write example.

`cqlite-core` is not published on crates.io, so downstream projects pin it by git
tag.

## 1. Add the dependency

Pin a released tag (see the [CHANGELOG](../CHANGELOG.md) for what each tag contains):

```toml
# Cargo.toml — read + query path (default features)
[dependencies]
cqlite-core = { git = "https://github.com/pmcfadin/cqlite.git", tag = "v0.9.2" }
```

The write path (`WriteEngine`, `Mutation`) is gated behind the `write-support`
feature. On **v0.9.2** it is opt-in, so enable it explicitly:

```toml
# Cargo.toml — read + write path on v0.9.2
[dependencies]
cqlite-core = { git = "https://github.com/pmcfadin/cqlite.git", tag = "v0.9.2", features = ["write-support"] }
```

> As of [#558](https://github.com/pmcfadin/cqlite/issues/558) (next release),
> `write-support` is a **default** feature, so the explicit `features` line is no
> longer needed for the write path. It gates only first-party code and pulls in no
> extra dependencies, so keeping it on is free for read-only consumers.

## 2. Feature → API map

Which Cargo feature enables which public API:

| Want… | Enable feature | In defaults? |
|-------|----------------|--------------|
| Read / query path (`Database::open`, `execute`, `scan`, `get`) | `state_machine` | ✅ yes |
| Compression (LZ4 / Snappy / Deflate / Zstd) | `all-compression` | ✅ yes |
| Write path (`WriteEngine`, `Mutation`, `WriteEngine::write`/`flush`) | `write-support` | ✅ yes (next release; opt-in on v0.9.2) |
| `Database::flush` / `Database::compact` (high-level convenience) | `experimental` | ❌ opt-in |

This mirrors the table in the [README](../README.md#feature-flags); the README is
the canonical copy.

## 3. A minimal write example

The snippet below constructs a `Mutation` and persists it with
`WriteEngine::write`. It is maintained as a compiling example at
[`cqlite-core/examples/write_a_mutation.rs`](../cqlite-core/examples/write_a_mutation.rs)
so it cannot drift from the API. Run it with:

```bash
cargo run -p cqlite-core --example write_a_mutation --features write-support
```

```rust
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;

// Describe the target table. `is_static` is `#[serde(default)]` (false) when a
// schema is deserialized from JSON/CQL; in a Rust literal you set it explicitly.
let schema = TableSchema {
    keyspace: "demo".to_string(),
    table: "users".to_string(),
    partition_keys: vec![KeyColumn { name: "id".to_string(), data_type: "int".to_string(), position: 0 }],
    clustering_keys: vec![],
    columns: vec![
        Column { name: "id".to_string(),   data_type: "int".to_string(),  nullable: false, default: None, is_static: false },
        Column { name: "name".to_string(), data_type: "text".to_string(), nullable: true,  default: None, is_static: false },
    ],
    comments: HashMap::new(),
};

// `data_dir` holds flushed SSTables; `wal_dir` holds the write-ahead log.
let base = std::env::temp_dir().join("cqlite-write-a-mutation");
let config = WriteEngineConfig::new(base.join("data"), base.join("wal"), schema);
let mut engine = WriteEngine::new(config)?;

// INSERT INTO demo.users (id, name) VALUES (1, 'Alice')
// The cell op is `Write` — there is no `Insert` variant.
let mutation = Mutation::new(
    TableId::new("demo", "users"),
    PartitionKey::single("id", Value::Integer(1)),
    None,                                  // no clustering key on this table
    vec![CellOperation::Write { column: "name".to_string(), value: Value::Text("Alice".to_string()) }],
    1_704_067_200_000_000,                 // write timestamp, microseconds since Unix epoch
    None,                                  // no TTL
);

// `write` is SYNCHRONOUS, takes `&mut self`, and fsyncs the WAL on every call.
engine.write(mutation)?;
```

### API facts worth knowing up front

These trip up first-time consumers because the real API differs from what the
on-disk format spec implies:

- **`WriteEngine::write(&mut self, Mutation) -> Result<()>` is synchronous** and
  fsyncs the WAL on every call. (`write_async` exists for async call sites; it has
  the same `&mut self` / per-write durability semantics.)
- **`CellOperation` has no `Insert` variant.** The variants are `Write`,
  `WriteWithTtl`, `Delete`, and `DeleteRow`.
- **`Mutation` fields**: `table`, `partition_key`, `clustering_key` (`Option`),
  `operations`, `timestamp_micros`, `ttl_seconds`, `partition_tombstone`,
  `range_tombstones`. The `Mutation::new(...)` constructor takes the first six;
  the two tombstone fields default to empty.
- **`Column.is_static` is not required** when a schema is deserialized from
  JSON/CQL — it is `#[serde(default)]` and defaults to `false`. In a Rust struct
  literal (as above) all fields are set explicitly.
- **`flush` and `close` are async** (`flush(&mut self) -> Result<Option<SSTableInfo>>`,
  `close(&mut self) -> Result<()>`); the example wraps them in a Tokio runtime.

## 4. Reading data

For the read/query path, open a `Database` and run CQL — this needs only the
default features (`state_machine` + `all-compression`). See the [README](../README.md)
Quick Start and [`docs/write-support.md`](write-support.md) for the writable
`Database` wrapper that combines reads and writes behind one handle.

## 5. Write-path concurrency & durability model

`WriteEngine` is a **single-writer** component. Plan your write path around these
two properties, both verified in the source
(`cqlite-core/src/storage/write_engine/mod.rs`):

- **`&mut self` ⇒ one writer at a time.** `write`, `write_async`, `flush`,
  `maintenance_step`, and `close` all take `&mut self`, so the borrow checker
  already prevents concurrent calls on a single engine. If multiple threads or
  tasks produce mutations, funnel them to one owner — e.g. wrap the engine in a
  `Mutex` / `tokio::sync::Mutex`, or send mutations over a channel to a single
  writer task. The engine is intentionally **not** internally synchronized; there
  is no sharded or multi-writer mode.

- **The WAL is fsync'd on every `write` ⇒ durability-first, throughput-bounded.**
  Each `write` appends to the write-ahead log and fsyncs before returning, so a
  successful call means the mutation is durable on disk. The cost is that
  single-thread write throughput is bounded by fsync latency rather than CPU —
  expect low-hundreds of ops/sec on typical disks (a downstream benchmark harness
  measured ~282 ops/sec single-thread). Adding threads does **not** raise this:
  writers serialize through the single engine, and each still pays one fsync.

### Intended usage

- **Throughput comes from batching work *before* the fsync, not from concurrency.**
  Group many cells into a single `Mutation` where the data model allows, and let
  the memtable accumulate across many `write` calls before a `flush` — flushing is
  amortized, the per-write fsync is not.
- For bulk-load / benchmarking where you can trade durability for speed, an opt-in
  WAL durability toggle is tracked in
  [#547](https://github.com/pmcfadin/cqlite/issues/547). That is the planned lever
  for lifting the per-write fsync bound.

### Is a batched / async write path on the roadmap?

`write_async` already exists for async call sites, but it has the **same**
single-writer `&mut self` semantics and the same per-write WAL fsync — it is not a
throughput escape hatch. There is **no separate batched-mutation API currently
planned**; the single-writer + per-write-durability model above is the intended
design. The one roadmap item that changes the throughput envelope is the optional
WAL durability toggle ([#547](https://github.com/pmcfadin/cqlite/issues/547)).
