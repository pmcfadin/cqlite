# Position: Parquet Projections from Cassandra Memtable Flush Events

**Status:** Position document (not a committed roadmap)
**Audience:** CQLite maintainers, integrators evaluating CQLite for lakehouse pipelines
**Date:** 2026-06-10
**Related:** [Apache Cassandra Sidecar](https://github.com/apache/cassandra-sidecar), `cqlite-cli/src/output/parquet.rs`, `cqlite-core/src/lib.rs`

## Summary

This document evaluates using CQLite together with the Apache Cassandra Sidecar
to materialize **Parquet projections** of Cassandra tables, triggered by
**memtable flush events**. The goal is a vendor-neutral, lake-native columnar
copy of operational data without a cluster or JVM dependency in the read path.

Our position:

1. **The read → Parquet pipeline is largely built.** CQLite already reads
   externally-discovered SSTables and writes Parquet (batch and streaming).
2. **The hard problems are semantic, not mechanical.** A flushed SSTable is a
   *delta*, not a table snapshot. Correct projections require carrying cell
   write-timestamps and representing deletes/tombstones — Cassandra's storage
   model does not give these to us for free.
3. **The type mapping is now analytics-grade for schema-aware queries** (Issues
   #675–#678): `Date32`, `Time64`, `Decimal128`, UUID extension metadata,
   `List<T>`, `Map<K,V>`, `Struct` for UDTs/tuples, and `frozen` transparency
   are all implemented.  The only remaining deviation is `duration` → `Utf8`
   (blocked on `parquet` crate v53 lacking `IntervalMonthDayNano` write support).
4. **This is fundamentally a bolt-on CDC pattern.** Compared to TiDB's TiFlash
   (a native, Raft-replicated columnar replica), our approach trades
   consistency and freshness for openness and zero-cluster-dependency.

We recommend treating this as a **change-data-capture (CDC) / append-log**
projection landing in a table format (Iceberg/Delta), *not* as a
"current-snapshot" exporter, and we recommend **commitlog CDC** over raw flush
events as the trigger if correctness matters.

## Background: what CQLite provides today

All claims below are grounded in the current codebase.

### Reading externally-discovered SSTables

The integration hook for "the Sidecar told me where a new SSTable is — read it":

```rust
// cqlite-core/src/lib.rs
pub async fn open_with_discovered_sstables(
    storage_path: &Path,
    discovered_table_dirs: Vec<PathBuf>,
    config: Config,
) -> Result<Self>
```

Paired with memory-bounded streaming reads:

```rust
// cqlite-core/src/lib.rs
pub async fn execute_streaming(
    &self,
    sql: &str,
    chunk_size: usize,
) -> Result<impl Stream<Item = Result<QueryRow>>>
```

### Writing Parquet

`cqlite-cli/src/output/parquet.rs` implements both a batch `ParquetWriter` and a
`StreamingParquetWriter<W>` (default 10,000-row row groups, Snappy compression
to match Cassandra defaults), exposed via the CLI as `--out parquet`. A minimal
per-SSTable projection is therefore already expressible as a one-shot CLI call:

```bash
cqlite --schema users.cql \
       --data-dir /var/lib/cassandra/data/my_ks/users-<uuid>/ \
       --query "SELECT * FROM my_ks.users" \
       --out parquet -o /projections/users/nb-<gen>.parquet
```

> **Note:** The Parquet writer currently lives in `cqlite-cli`, not
> `cqlite-core`. Embedding projection in a long-running service means either
> shelling out to the CLI or lifting the writer into the core crate behind a
> `parquet` feature flag.

## Architecture

```
┌─────────────────┐   flush     ┌──────────────────────┐
│  Cassandra node │────────────▶│  data dir (SSTables) │
└─────────────────┘  new SSTable└──────────┬───────────┘
                                            │ TOC.txt appears (file complete)
                                ┌───────────▼───────────┐
                                │  Projection service   │  (co-located w/ Sidecar)
                                │  - detect new SSTable  │
                                │  - debounce + dedupe   │
                                └───────────┬───────────┘
                                            │ path + keyspace/table + schema
                                ┌───────────▼───────────┐
                                │  CQLite                │
                                │  open_with_discovered_ │
                                │    sstables()          │
                                │  execute_streaming()   │
                                │  StreamingParquetWriter│
                                └───────────┬───────────┘
                                            │
                                ┌───────────▼───────────┐
                                │  /projections/<ks>/    │
                                │    <table>/nb-<gen>.parquet
                                │  (ideally Iceberg/Delta)
                                └────────────────────────┘
```

### The trigger: where do "flush events" come from?

The Cassandra Sidecar does **not** expose a push stream of "memtable flushed"
events out of the box; its REST surface is oriented around SSTable component
listing, upload/import, health, and bulk streaming. Trigger options:

| Trigger source | Mechanism | Trade-off |
|---|---|---|
| **Filesystem watch** | `inotify` on the table data dir; key on `TOC.txt` (written last ⇒ SSTable complete) | Simplest, reliable; per-host |
| **Commitlog CDC** | Cassandra CDC on the commitlog | Ordered + carries timestamps; closest to correct CDC; more plumbing |
| **Sidecar API polling** | Diff SSTable component listings per table | Easy; latency = poll interval |
| **Diagnostic events / JMX** | SSTable lifecycle notifications | JVM-side; reintroduces a cluster dependency |

For a simple bulk projection, **filesystem watch keyed on `TOC.txt`** is the
pragmatic default. For correctness-sensitive pipelines, **commitlog CDC** is the
better source (see "Semantics" below).

> A `*-Data.db` file appearing is not always a memtable flush — it can be a
> **compaction** output. For a downstream lake this is usually fine (and often
> desirable), but if "flush only" semantics matter, the source must distinguish
> them (e.g. via diagnostic events).

## The data-model question: does Cassandra map onto Parquet?

Short answer: **scalars map cleanly; richer types are currently lossy; and the
real friction is semantic, not type-level.**

### Type mapping (as implemented in `parquet.rs`)

When `ColumnInfo.cql_type` is populated from the schema (the default for
schema-aware queries), the following high-fidelity mappings are applied.  When
`cql_type` is `None`, the legacy `data_type` path is used (last column).

| CQL type | Arrow/Parquet type | Notes |
|---|---|---|
| `boolean` | `Boolean` | Clean |
| `tinyint` / `smallint` / `int` / `bigint` | `Int8` / `Int16` / `Int32` / `Int64` | Clean |
| `float` / `double` | `Float32` / `Float64` | Clean |
| `text` / `varchar` / `ascii` | `Utf8` | Clean |
| `blob` | `Binary` | Clean |
| `timestamp` | `Timestamp(ms, UTC)` | Clean |
| `date` | `Date32` | Signed days since 1970-01-01 (Issue #675) |
| `time` | `Time64(Nanosecond)` | Nanos since midnight (Issue #675) |
| `decimal` | `Decimal128(38, 9)` | Fixed scale = 9; checked rescale; overflow → error (Issue #675) |
| `varint` | `Decimal128(38, 0)` | Integer domain; values > 38 digits → error (Issue #675) |
| `duration` | `Utf8` (CQL text form, e.g. `"1mo2d3ns"`) | `parquet` crate v53 cannot write `IntervalMonthDayNano`; Utf8 fallback until upstream support lands (Issue #675) |
| `uuid` / `timeuuid` | `FixedSizeBinary(16)` + `ARROW:extension:name=arrow.uuid` metadata | Arrow UUID logical type via extension metadata (Issue #675) |
| `inet` | `Utf8` (canonical text, e.g. `"192.168.1.1"`) | No standard Arrow InetAddress type; text is most portable for downstream tools (Issue #675) |
| `counter` | `Int64` | Label lost; value intact (Issue #675) |
| `list<X>` | `List<mapped(X)>` | Element type mapped recursively through this table (Issue #676) |
| `set<X>` | `List<mapped(X)>` | Arrow has no dedicated Set type; uses `List` (Issue #676) |
| `map<K,V>` | `Map<Struct(key:mapped(K) non-null, value:mapped(V) nullable)>` | Entries struct named `"entries"` per Arrow convention; keys/values typed recursively (Issue #677) |
| `tuple<A,B,…>` | `Struct(field_0:mapped(A), field_1:mapped(B), …)` | Positional field names; per-position types mapped recursively (Issue #678) |
| `udt<name>` | `Struct(f1:T1, f2:T2, …)` | Field names from schema; all fields nullable; zero-field UDT falls back to `Utf8` (Issue #678) |
| `frozen<T>` | Same as `T` | `Frozen` is transparent in both schema and value mapping |
| `custom` | `Utf8` | Serialized via `ValueFormatter` |

**Fallback path** (when `cql_type = None`): collections → `List<Utf8>` /
`Map<Utf8,Utf8>`, UDT/tuple → `Utf8`, date/time → plain `Int32`/`Int64`.

**Batch and streaming writer parity**: both `ParquetWriter` (batch) and
`StreamingParquetWriter` (streaming) share the same `build_schema` and
`convert_to_arrays` code paths, producing identical Arrow schemas and values
for all types above (verified by `test_streaming_batch_parity_*` in
`cqlite-cli/tests/parquet_writer_tests.rs`).

A table of scalars (IDs, metrics, timestamps, text — the common analytics case)
projects faithfully with full Arrow logical type annotations. Collections and
structured types (UDT, tuple, map) are now fully typed when schema information
is available.  The remaining non-ideal cases are:

- **`duration`** serializes as `Utf8` rather than `Interval(MonthDayNano)` because the `parquet` crate v53 does not support writing `IntervalMonthDayNano` (NYI upstream).
- **`varint`** larger than 38 decimal digits fails rather than silently truncating.
- **`inet`** uses canonical text rather than raw bytes (intentional — no Arrow InetAddress type).

### The deeper mismatch: a flushed SSTable is a *delta*, not a snapshot

This is the part that bites people, and it is inherent to Cassandra's storage
model — not a CQLite defect:

1. **Rows are partial / superseded.** The same primary key can appear across
   many SSTables; the live value is the per-cell last-write-wins (LWW) merge of
   all of them. One flush's Parquet reflects only that flush's version.
2. **Tombstones carry deletes.** A flush can contain row/range/cell tombstones
   and TTL expirations. The current projection has no faithful representation of
   these, so a naive union of insert rows would **resurrect deleted data**.
3. **Reconciliation needs write-timestamps.** LWW merge is driven by each cell's
   `writetime`. A plain `SELECT *` drops it, so two flushes cannot be correctly
   merged downstream without it.
4. **Sparse/wide schema.** CQLite's SELECT already flattens Cassandra's sparse
   cell-sets into a rectangular result, so this part is handled — but
   absent-vs-null is collapsed.

### Two coherent target semantics — pick deliberately

- **CDC / append-log projection (recommended).** Treat each flushed SSTable as
  an immutable event batch → one Parquet file per generation. Embrace that it is
  a delta. To be correct, **carry `writetime` and represent tombstones** (e.g.
  `__writetime` / `__deleted` columns, Debezium-style). Downstream
  (Spark/Trino/Iceberg) performs the merge. Idempotent; matches the
  immutable-SSTable grain.
- **Current-snapshot projection (heavier).** To reflect current table state, do
  not project a single SSTable; point `open_with_discovered_sstables()` at *all*
  live SSTables for the table so CQLite's read path performs the LWW merge, then
  export. Correct, but re-reads everything on each flush.

## Comparison: how TiDB approaches this

TiDB is instructive because it solves the same "analytics on operational data"
problem *natively*, and it does so with two components that map onto the two
halves of our design.

### TiFlash — in-database columnar replica

- **Replication:** TiFlash is a **Raft learner**. Every write committed to TiKV
  (the row store) replicates to TiFlash's columnar store via the same Raft log.
  No flush-watching, no out-of-band ETL.
- **Consistency & freshness:** Reads are **strongly consistent and real-time**
  (MVCC snapshot at the read timestamp).
- **Updates/deletes/MVCC:** Reconciled **in-engine** (DeltaTree: a delta layer
  plus a stable columnar layer — conceptually like memtable+SSTable+compaction,
  but columnar and MVCC-aware). This is exactly the hard part we would have to
  build by hand.
- **Query path:** TiDB's optimizer transparently routes between TiKV and
  TiFlash, with an MPP engine for distributed analytics.

TiFlash is what Cassandra+CQLite would be *if* Cassandra shipped a built-in,
consistently-replicated columnar twin.

### TiCDC — the actual structural analog to our pipeline

- Tails the ordered **row-change log** (not flushed files) and emits a
  deduplicated changefeed.
- Sinks: Kafka (canal-json, open-protocol, Debezium, Avro) and a cloud-storage
  sink (S3/GCS/Azure/NFS).
- Carries **commit timestamps and explicit insert/update/delete events** — i.e.
  it solves the writetime + tombstone problem *as part of the protocol*.
- One gap vs. our idea: TiCDC's storage sink emits **CSV/canal-json, not Parquet
  natively** — so our approach's strength is landing directly in an open
  columnar format.

### Why the architectures diverge — it is about the database

|  | TiDB | Cassandra + CQLite |
|---|---|---|
| Replication | **Raft consensus** (ordered, linearizable log) | Gossip + per-cell **last-write-wins**, eventually consistent |
| Natural columnar hook | Raft learner → real-time consistent replica | Immutable **SSTable / flush** (or commitlog CDC) → async delta |
| Projection consistency | Transaction-consistent snapshot | Per-SSTable delta, no cross-partition consistency |
| Update/delete/MVCC | In-engine (DeltaTree) | **Caller's responsibility** (tombstones, LWW, writetime) |
| Output format | Internal columnar / CSV-JSON | **Open Parquet** |
| Query engine | Built-in optimizer + MPP | Bring your own (Trino/Spark/DuckDB) |
| Coupling | Native, in-database | External, bolt-on |

TiDB gets a consistent columnar replica "for free" because it has a single
ordered log with commit timestamps. Cassandra has no such log — its source of
truth is a set of independently-flushed, LWW-merged SSTables — so **any**
columnar projection is inherently a delta-reconciliation problem. That is the
root reason this document keeps returning to "carry writetime and tombstones":
we are hand-rolling the consistency guarantees TiDB derives from Raft.

## Recommendations

1. **Model it as CDC/append-log, not snapshot.** It matches the
   immutable-SSTable grain and is idempotent per generation.
2. **Land in a table format (Iceberg or Delta), not bare Parquet files.** Those
   provide the snapshot isolation, compaction, and merge-on-read that TiFlash's
   DeltaTree provides internally — i.e. they give us the consistency story we
   otherwise lack.
3. **Preserve `writetime` and represent tombstones** (`__writetime` /
   `__deleted` columns) so projections are actually reconcilable. Without this,
   a union of per-flush Parquet is silently wrong (resurrected deletes, stale
   cells).
4. **Prefer commitlog CDC over raw flush events** when correctness matters;
   flush/SSTable watching is fine for cheap periodic bulk snapshots.
5. **Arrow type mapping is now analytics-grade** for schema-aware queries:
   `List<T>`, `Map<K,V>`, `Struct` for UDTs and tuples, `Decimal128`, `Date32`,
   `Time64`, and `FixedSizeBinary(16)+arrow.uuid` are all implemented (Issues
   #675–#678).  The remaining deviation (`duration` → `Utf8`) is blocked on the
   `parquet` crate v53 upstream; no action needed until that crate is upgraded.
6. **Lift the Parquet writer into `cqlite-core`** behind a `parquet` feature
   flag if we want embeddable (non-CLI) projection.
7. **Set expectations honestly.** This cannot match TiFlash on freshness or
   consistency — those come from being inside the transactional system. Its
   distinct value is **open, lake-native columnar output from Cassandra with no
   cluster dependency in the read path.**

## Risks and non-goals

- **Not real-time or transactionally consistent.** Async, per-flush latency;
  no cross-partition consistency.
- **Compaction vs. flush ambiguity** in the trigger if "flush only" is required.
- **Schema sourcing.** CQLite requires the CQL schema to decode (no-heuristics
  mandate); the projection service must source and cache it (e.g. from
  `DESCRIBE TABLE`) — flush events do not carry schema.
- **Not a query engine for the lake.** Downstream compute (Trino/Spark/DuckDB)
  is the consumer; CQLite produces files, it does not serve analytical queries.

## Open questions

- Should the writetime/tombstone-aware projection schema be standardized (a
  Debezium-compatible envelope vs. a CQLite-native one)?
- Is direct Iceberg/Delta writing in scope for CQLite, or is bare Parquet +
  an external committer the right boundary?
- Is there appetite for a first-party "projection service" in this repo, or
  should CQLite remain a library/CLI that such a service consumes?
