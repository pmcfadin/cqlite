# Design: DataFusion Table Provider for CQLite (#941)

**Status:** Draft for review
**Issue:** [#941 — Add data fusion table provider](https://github.com/pmcfadin/cqlite/issues/941)
**Depends on:** #942 (efficient bulk read), Epic D (#1516 streaming by default), Epic AB (#1467 streaming egress), Epic AC (#1468 fail-closed conversion)
**Related:** #874 (Flight/Trino pushdown), #906 (read-path I/O backend), #673/#682 (Arrow/Parquet type mapping), #663 (docker e2e), #696 (delta-scan envelope)

---

## 1. Summary

Add an Apache DataFusion `TableProvider` that exposes Cassandra tables as SQL-queryable
DataFusion tables. The provider operates in two modes:

- **Local mode** — point at a data directory (or explicit SSTable set) on disk; wraps the
  #942 bulk-read scan contract directly. No cluster interaction.
- **Cluster mode (Topology A)** — use the Apache Cassandra Sidecar to discover ring
  topology and schema, create snapshots on the replicas covering each token range, stream
  snapshot SSTable components to the reader, and execute the same local scan against the
  fetched files. All parsing, merging, and filtering happens **reader-side**; Cassandra
  nodes serve files only.

The design goal is to be **complementary to the existing ecosystem**: the provider consumes
exactly the sidecar surface that Apache Cassandra Analytics (the Spark bulk reader) already
consumes. If the Spark bulk reader works against a cluster, cqlite-datafusion works against
that cluster. We deploy nothing on Cassandra nodes and require no endpoints beyond the
Analytics minimum.

### 1.1 Goals

1. `SELECT ... FROM cassandra_table` from any DataFusion host: `datafusion-cli`, embedded
   Rust, DataFusion Python, Ballista.
2. Predicate, projection, and limit pushdown exploiting Cassandra's physical layout
   (token routing, bloom filters, partition/clustering indexes, Statistics.db pruning).
3. Parallel execution: token-range splits map 1:1 to DataFusion partitions.
4. Correct merged reads: multi-SSTable reconciliation with tombstone and TTL semantics
   identical to the parity-audited compaction merge, in "query view" mode.
5. Safe snapshot lifecycle: no leaked snapshots even on crash; bounded local disk use.
6. Zero footprint on Cassandra nodes beyond the stock sidecar.

### 1.2 Non-goals (v1)

- **Writes.** Read-only provider. (INSERT-via-SSTable-generation is a natural v2+, since
  M5 write support exists, but is out of scope here.)
- **Aggregation pushdown.** DataFusion's `TableProvider` has no aggregate hook; a custom
  optimizer rule could rewrite `Aggregate → scan`, but #918's follow-up list shows this is
  where subtle correctness bugs live. DataFusion's own vectorized aggregation over Arrow
  batches is fast; defer.
- **Cluster-consistent point-in-time reads.** Snapshots are taken per-replica and are not
  coordinated; results are per-replica views (see §8, Consistency).
- **Multi-replica reconciliation.** Designed-for but not shipped in v1 (see §8.2).
- **Secondary index / SAI usage.** Regular-column predicates are evaluated reader-side.
- **Topology B** (deploying cqlite-flight next to Cassandra nodes as a data plane).
  Rejected for v1 because it requires deployment on data nodes and burns their CPU —
  see §14, Alternatives.

---

## 2. Background and existing assets

The provider is deliberately a *thin integration layer*. Nearly every hard sub-problem is
already owned by an existing subsystem or in-flight epic:

| Concern | Owned by | State |
|---|---|---|
| SSTable parsing (all C5 types, compression, BTI) | cqlite-core, M1 + Epics H–M | Shipped, hardening in progress |
| Efficient bulk read scan contract | **#942** | In progress (owner) |
| Streaming, bounded-memory reads + pushdown | Epic D (#1516) | In progress |
| Multi-SSTable merge w/ tombstone + TTL semantics | compaction merge (`merge/mod.rs`), Epic #1378 remediation | In progress; parity-audited |
| CQL → Arrow type mapping | #673/#682 Parquet/Arrow work, Epics AC/AD | Shipped, fail-closed hardening in progress |
| Streaming Arrow egress (no materialize-then-emit) | Epic AB (#1467) | In progress |
| Internal predicate representation + token-range pruning | #874 (Flight/Trino pushdown) | Shipped |
| Pluggable I/O backend (seam for remote sources) | #906 | In progress |
| Statistics.db min/max timestamps, deletion times | #1728/#1729 + enhanced_statistics_parser | In progress |
| Docker e2e infrastructure w/ real Cassandra | #663 | Shipped |

What is genuinely new in this design:

1. A **sidecar client crate** (`cqlite-sidecar`): HTTP client for ring, schema, snapshot,
   and component-streaming endpoints, plus snapshot lifecycle management.
2. A **DataFusion adapter crate** (`cqlite-datafusion`): `TableProvider`,
   `ExecutionPlan`, expression lowering, split planning.
3. A **fetch/spool layer** mapping remote snapshot components to local scan inputs.

---

## 3. Architecture overview

```
┌────────────────────────────────────────────────────────────────────┐
│ DataFusion host (datafusion-cli / embedded / Python / Ballista)    │
│                                                                    │
│  SessionContext                                                    │
│    └─ CassandraCatalog / CassandraTableProvider   (cqlite-datafusion)
│         ├─ schema(): CQL schema → Arrow schema                     │
│         ├─ supports_filters_pushdown(): Expr → CqlPredicate        │
│         ├─ statistics(): from Statistics.db aggregates             │
│         └─ scan(projection, filters, limit)                        │
│              └─ CassandraScanExec (ExecutionPlan)                  │
│                   partitions[i] = one TokenSplit                   │
│                   execute(i) →                                     │
│                     1. FetchSession: acquire local SSTable set     │
│                     2. cqlite bulk-read scan (#942 contract)       │
│                     3. SendableRecordBatchStream out               │
└────────────────────────────────────────────────────────────────────┘
                     │  control plane                │ data plane
                     ▼                               ▼
        ┌───────────────────────┐        ┌───────────────────────┐
        │ Apache Cassandra       │        │ Apache Cassandra      │
        │ Sidecar (per node)     │        │ Sidecar (per node)    │
        │  - token-range replicas│        │  - snapshot component │
        │  - schema              │        │    streaming (HTTP,   │
        │  - snapshot create/del │        │    Range requests)    │
        └───────────────────────┘        └───────────────────────┘
                     │                               │
              Cassandra node                  Cassandra node
              (untouched)                     (untouched)
```

Crate layout and dependency arrows (all new crates workspace members; none are
dependencies of `cqlite-core`, protecting the M6 WASM < 2 MB target):

```
cqlite-core  ◄──  cqlite-datafusion  ──►  datafusion (pinned major)
    ▲                    │
    │                    ▼
    └────────────  cqlite-sidecar  ──►  reqwest/hyper, tokio
                   (no DataFusion dep; usable by CLI, Flight, future consumers)
```

- **`cqlite-sidecar`** knows nothing about DataFusion. It exposes: topology, schema,
  `SnapshotSession`, and component streams. The CLI grows a `cqlite fetch` verb on top of
  it (§11 Phasing) so the crate is exercised by a trivial consumer before DataFusion wiring.
- **`cqlite-datafusion`** depends on `cqlite-core` (scan contract) and `cqlite-sidecar`
  (cluster mode). Local mode compiles without the sidecar crate via a `cluster` feature
  flag, keeping the minimal dependency footprint for embedded/local users.
- **Split planning lives in `cqlite-datafusion`**, not the sidecar crate: mapping token
  ranges to DataFusion partitions is query-engine policy, not cluster-client mechanics.

---

## 4. The scan contract (interface to #942)

The provider is the second consumer of the bulk-read contract (Flight is the first). This
section is the requirements list the provider imposes on #942; it should be reviewed as
part of #942's API design rather than retrofitted.

```rust
/// Owned by cqlite-core (#942). Sketch — names illustrative.
pub struct ScanRequest {
    /// Explicit SSTable set. The provider always resolves the set itself
    /// (local dir listing or fetched snapshot manifest); the scan layer
    /// never lists directories in cluster mode.
    pub sstables: Vec<SstableHandle>,
    pub schema: TableSchema,
    /// Half-open token range filter; None = full range. Required for
    /// distribution: ring splits become DataFusion partitions.
    pub token_range: Option<TokenRange>,
    /// Column projection by ID. Unprojected cells are skipped at decode
    /// time (cheap for collections/UDTs even though rows are still walked).
    pub projection: Option<Vec<ColumnId>>,
    /// Predicates in the #874 internal representation. The scan layer
    /// applies what it can exploit physically (partition-key routing,
    /// clustering ranges, stats pruning) and evaluates the rest per-row
    /// where cheap; residual handling is reported per-predicate.
    pub predicates: Vec<CqlPredicate>,
    /// Stop after N rows *post-merge* (LIMIT pushdown).
    pub limit: Option<usize>,
    /// Target rows per emitted batch (DataFusion batch_size, default 8192).
    pub batch_hint: usize,
    /// Merge output policy — see §7.
    pub view: MergeView, // QueryView | CompactionView{gc_grace}
}

pub trait BulkScan {
    /// Plan-time, no-I/O-beyond-metadata estimates for
    /// TableProvider::statistics() and pruning decisions.
    fn estimate(&self, req: &ScanRequest) -> ScanEstimate;

    /// Streaming, bounded-memory execution. The stream MUST:
    ///  - be Send (DataFusion executes partitions on a threadpool)
    ///  - release file handles/buffers promptly on drop (DataFusion drops
    ///    streams aggressively: LIMIT, early join termination) — ties into
    ///    Epic AA cancellation semantics
    ///  - support N concurrent independent scans of one table (no global
    ///    cursor/lock; shared caches fine)
    fn execute(&self, req: ScanRequest) -> Result<BoxStream<Result<RecordBatch>>>;
}
```

Hard requirements, restated:

1. **Token range is a first-class parameter.** Local/Flight usage doesn't force it;
   distribution cannot exist without it.
2. **Explicit SSTable sets**, not directory paths, as the canonical input. Cluster mode
   feeds fetched files that live in a spool directory with generated names.
3. **Per-predicate disposition** in the response (`Exact` / `Inexact` per predicate) so
   the provider can answer `supports_filters_pushdown` truthfully (§6).
4. **Batch-size hint honored**, not a compile-time constant.
5. **Drop = cancel**, promptly.
6. **`estimate()` cheap enough to call at plan time** — Statistics.db + Summary/BTI
   metadata only, never Data.db.

---

## 5. Schema and type mapping (CQL → Arrow)

`TableProvider::schema()` derives the Arrow schema from the CQL table schema — from the
sidecar schema endpoint in cluster mode, from SSTable serialization headers +
`system_schema` snapshot components in local mode. The mapping **must be the single shared
mapping** used by the Parquet exporter and Flight (#673/#682, Epics AC/AD); this design
lifts it into a `cqlite-arrow` module in core (or re-exports the existing one) rather than
defining a third.

Proposed canonical mapping (aligned with the existing Parquet/Arrow decisions; flagged
rows are the ones needing confirmation against the current exporter):

| CQL type | Arrow type | Notes |
|---|---|---|
| ascii, text, varchar | Utf8 | |
| blob | Binary | |
| boolean | Boolean | |
| tinyint / smallint / int / bigint | Int8 / Int16 / Int32 / Int64 | |
| float / double | Float32 / Float64 | |
| decimal | Decimal128(38, s) when it fits; else Utf8 fallback **fail-closed per Epic AC** ⚠ | CQL decimal is arbitrary precision |
| varint | Decimal128 when it fits; else Binary (two's-complement big-endian) ⚠ | unbounded; must not silently truncate |
| timestamp | Timestamp(Millisecond, "UTC") | C* stores millis |
| date | Date32 | C* epoch-centered u32 → rebased |
| time | Time64(Nanosecond) | |
| uuid / timeuuid | FixedSizeBinary(16) | Utf8 render option at the edge only |
| inet | Utf8 (canonical form) ⚠ | alt: FixedSizeBinary(4/16) union — rejected, unions are poorly supported |
| duration | Struct{months: Int32, days: Int32, nanos: Int64} ⚠ | Arrow MonthDayNano interval is the alternative; struct is what Parquet round-trips today |
| counter | Int64 | read-only anyway |
| list\<T\> | List\<T\> | |
| set\<T\> | List\<T\> | ordered per comparator |
| map\<K,V\> | Map\<K,V\> (or List\<Struct\{key,value\}\>) | match Parquet exporter |
| tuple\<...\> | Struct with positional field names "0","1",… | |
| UDT | Struct with field names | nested UDTs recurse |
| frozen\<X\> | same as X | frozenness is a write-path concern |
| vector\<float, n\> | FixedSizeList\<Float32, n\> | C5 |

Additional schema rules:

- **Field nullability:** primary-key columns non-nullable; all regular columns nullable.
- **Metadata columns (optional, feature-gated):** `_writetime_<col>` (Timestamp µs) and
  `_ttl_<col>` (Int32) virtual columns, reusing the #689 WRITETIME()/TTL() machinery.
  Exposed only when requested via provider config; absent by default to keep `SELECT *`
  clean.
- **Fail-closed:** any type the mapping cannot represent faithfully is an error at
  `schema()` time, not a silent lossy conversion (Epic AC policy).

---

## 6. Predicate pushdown

### 6.1 Expression lowering

`supports_filters_pushdown(&[&Expr])` lowers each DataFusion `Expr` into the internal
`CqlPredicate` representation from #874. Lowering is conservative: anything that doesn't
map cleanly is `Unsupported` and stays in DataFusion's `FilterExec` above the scan.

### 6.2 Pushdown matrix

| Predicate shape | Mechanism | Effect | Reported as |
|---|---|---|---|
| full partition key `=` | murmur3 → token → route to owning split + replica; bloom filter; partition index/BTI point seek | scan → point lookup; unneeded splits return empty streams without fetching | **Exact** |
| partition key `IN (…)` | per-key token routing, fan-out | prunes splits + SSTables | **Exact** |
| clustering prefix `=` / range (with full PK) | clustering index within partition | seeks within partition | **Exact** |
| token(pk) range (`WHERE token(pk) > …`) | intersect with split ranges | split pruning | **Exact** |
| regular column vs Statistics.db min/max (timestamps; clustering min/max) | whole-SSTable pruning | skips SSTables | **Inexact** (prunes files, doesn't filter rows) |
| regular column comparisons | evaluated per-row post-merge in scan layer where cheap | reduces egress | **Inexact** |
| anything else (functions, OR across keys, subqueries) | not lowered | — | **Unsupported** |

Two correctness rules:

1. **Never report Exact for anything evaluated pre-merge.** A predicate checked against a
   cell in one SSTable can be invalidated by a newer cell or tombstone in another.
   Partition/clustering-key predicates are safe (keys are immutable identity); regular-
   column predicates are only safe **post-merge**, and even then reporting Inexact and
   letting DataFusion re-filter costs little and buys safety while #1378-class merge bugs
   are still being remediated. **v1 policy: keys Exact, everything else Inexact.**
2. **Pruning consumes #1728/#1729.** Timestamp-based SSTable pruning must use the
   authoritative maxTimestamp (fail-closed when absent), exactly as #1388's overlap gate
   does. Same code path, same fixtures.

### 6.3 Limit pushdown

`scan(..., limit)` forwards to `ScanRequest::limit`, applied post-merge per partition.
Combined with drop-on-cancel this makes `SELECT * LIMIT 10` cheap even in cluster mode:
the fetch layer (§9) fetches lazily enough that unstarted splits never fetch at all.

---

## 7. Merge semantics: one engine, two views

Correct reads over multiple SSTables require k-way streaming reconciliation by
(partition key, clustering key): last-write-wins per cell by timestamp, with cell, row,
range, and partition tombstones suppressing older data, and expired TTL cells treated as
tombstones. This machinery exists in the compaction merge and is under active parity
remediation (Epic #1378: TTL application #1382, range-tombstone boundary synthesis #1383,
zombie prevention #1384, gc_grace boundary #1385).

**Design decision: the provider does not get its own merge.** The compaction merge is
refactored (or wrapped) to expose two output policies:

- **CompactionView { gc_grace }** — current behavior: purge markers older than gc_grace,
  emit everything else including live tombstones, write SSTables.
- **QueryView** — the provider's mode: gc_grace is effectively infinite (markers are never
  "purged", they simply suppress), expired-TTL cells suppress as tombstones at read
  time (`now` fixed per-query for stability across batches), and **only live rows/cells
  are emitted**. No markers appear in output.

Consequences:

- Every byte-parity fixture and every #1378 remediation automatically covers the
  DataFusion read path. No second parity audit.
- The #1378-audited bug classes (TTL re-emitted live, RT boundary loss, zombies) would
  otherwise surface as *silently wrong SQL results* — the worst failure mode a query
  engine has. **#941 therefore sequences after the #1378 P0/P1 children land**, and the
  provider's test plan includes the same fixtures run through QueryView (§12).
- Static rows, partition-deletion + re-insert, and multi-generation shadowing follow the
  same rules with no provider-specific logic.

Wide-partition safety: the merge must remain streaming (bounded memory per partition
regardless of partition width), which is exactly Epic D/#751's constraint — the provider
adds no new requirement, it just cannot tolerate regression.

---

## 8. Consistency model

### 8.1 v1: single replica per range (CL.ONE-analog)

For each token split the planner picks **one** replica and reads only its snapshot.
Semantics are equivalent to Analytics at CL=ONE: results reflect that replica's view at
snapshot time; un-repaired writes on other replicas are missed; tombstoned-elsewhere data
may appear. This is inherited from the Analytics model and must be **stated loudly in
docs** — including the fact that snapshots across nodes are not a coordinated point in
time (they're created seconds apart; each range is internally consistent to its replica,
ranges are mutually skewed by snapshot-creation spread).

Replica selection policy (planner, pluggable):

1. Filter to replicas the sidecar reports as available.
2. Prefer same-DC as configured `local_dc` (mandatory config in cluster mode; analytics
   traffic must not silently cross DCs).
3. Balance: spread splits across replicas to even fetch load (rendezvous hash of
   (range, replica) as default; round-robin acceptable).
4. On fetch failure: fail over to next replica for that split, re-snapshotting if needed;
   configurable retry budget.

### 8.2 v2 design headroom: multi-replica reconciliation

The merge does not distinguish "SSTables from one node" from "SSTables from N nodes" —
more replicas is just more inputs to the same k-way merge, and LWW-by-timestamp *is*
Cassandra's read-repair reconciliation rule. Therefore CL=LOCAL_QUORUM-analog reads are:
fetch snapshot components from ⌈RF/2⌉+1 replicas per range, feed the union to QueryView.
Cost: RF/2+1× fetch volume. The only v1 obligations this imposes: `ScanRequest.sstables`
is already a flat set (done), and the planner's split type carries replica *lists* not a
single replica (cheap to do now). Everything else is a config knob later.

---

## 9. Cluster mode: sidecar client and snapshot lifecycle

### 9.1 Compatibility contract

`cqlite-sidecar` consumes **only** endpoints the Cassandra Analytics bulk reader requires
from the stock Apache Cassandra Sidecar:

| Function | Sidecar surface (indicative paths — pin exact routes + minimum sidecar version during implementation, matching Analytics' declared minimum) |
|---|---|
| Ring / ownership | token-range replicas endpoint (`GET /api/v1/keyspaces/{ks}/token-range-replicas`) |
| Schema | keyspace/table schema endpoint (`GET /api/v1/keyspaces/{ks}/schema`) |
| Snapshot create | `PUT /api/v1/keyspaces/{ks}/tables/{tbl}/snapshots/{name}` (with TTL param where supported) |
| Snapshot list components | `GET .../snapshots/{name}` (component manifest) |
| Component stream | `GET .../snapshots/{name}/components/{component}` with HTTP `Range` support |
| Snapshot delete | `DELETE .../snapshots/{name}` |
| Health/liveness | sidecar health endpoint |

Explicit constraint recorded in the epic: **no endpoint Analytics doesn't already
require.** "Works with the Spark bulk reader" ⇒ "works with cqlite-datafusion" is both the
test matrix and the pitch. Client details: TLS + mTLS support, sidecar auth passthrough
(bearer/keystore as the sidecar supports), bounded retries with jitter, per-host
connection pooling, checksum verification of fetched components against the sidecar-
provided digests where available.

### 9.2 SnapshotSession (RAII lifecycle)

```rust
/// cqlite-sidecar. One session per (query, replica set).
pub struct SnapshotSession { /* name, ttl, hosts, created state */ }

impl SidecarCluster {
    /// Creates snapshot `cqlite-df-{query_uuid}` with TTL on every host in
    /// `replicas`. Returns only after all creations succeed (or cleans up
    /// partial creations and errors).
    pub async fn snapshot(&self, ks: &str, table: &str,
                          replicas: &[HostId], ttl: Duration)
        -> Result<SnapshotSession>;
}

impl SnapshotSession {
    pub async fn manifest(&self, host: HostId) -> Result<Vec<Component>>;
    pub fn stream(&self, host: HostId, c: &Component, range: Option<ByteRange>)
        -> impl Stream<Item = Result<Bytes>>;
    /// Explicit cleanup; also spawned best-effort from Drop.
    pub async fn close(self) -> Result<()>;
}
```

Lifecycle rules:

1. **Names are unique per query** (`cqlite-df-{uuid}`), never reused, greppable by
   operators.
2. **TTL is mandatory** (default 6h, configurable). Cassandra 4.1+ snapshot TTL is the
   backstop: a SIGKILLed DataFusion process must not leak hardlinked disk on a production
   cluster. If the sidecar/Cassandra version doesn't support TTL at creation, the client
   refuses cluster mode unless the operator sets
   `allow_snapshots_without_ttl = true` (documented as an ops hazard).
3. **Cleanup runs on success, error, and Drop** (best-effort async in Drop; deterministic
   in `close()`). Failed deletes are logged with snapshot name + hosts so an operator (or
   the TTL) can finish the job.
4. One snapshot session per query, shared across all splits touching the same replica —
   not one per split.

### 9.3 Fetch strategies

Behind the #906 I/O seam as `SstableSource` implementations:

- **v1 — spool-whole-components.** For each split, download the replica's snapshot
  components for the table into a local spool directory
  (`{spool_root}/{query}/{host}/{table}-{generation}/...`), verify sizes/digests, then
  hand the file set to the local scan. Simple, uses the local read path unchanged,
  identical to the Analytics approach. Spool eviction: per-query directory removed when
  the query's last stream drops; global disk budget with LRU eviction across queries and
  hard-fail when a single query exceeds `max_spool_bytes`.
  Optimization within v1: fetch small components first (Statistics, Filter, Summary/BTI
  Partitions/Rows, CompressionInfo, TOC), run pruning (§6.2), and **skip downloading
  Data.db for pruned-out SSTables entirely.**
- **v2 — chunk-ranged Data.db.** Exploit format knowledge Analytics doesn't: with
  CompressionInfo.db in hand, compressed-chunk offsets are exact, so partition-index hits
  translate to precise HTTP Range requests for only the needed Data.db chunks. Point
  lookups over huge tables become a few MB of transfer. Requires the scan layer to read
  Data.db through a chunk-addressable `SstableSource` rather than a contiguous file —
  this is the concrete requirement to keep in view while #906 lands.

### 9.4 Split planning

```
splits = for each token_range in sidecar ring info:
           subdivide so that estimated_bytes(range, replica) ≈ target_split_bytes
           (default 512 MB pre-filter, config; estimate from sidecar component
            sizes prorated by range fraction, refined later by Statistics.db)
         assign replica per §8.1 policy
         intersect with token predicates (§6.2) — drop empty splits
partitions announced to DataFusion = splits (capped at max_partitions, default 128;
excess splits are chained sequentially within a partition's stream)
```

Ordering: no output ordering is declared (`ExecutionPlan::output_ordering = None`) in v1.
Within a split, rows are (token, clustering)-ordered by construction; declaring
per-partition sort keys to enable DataFusion sort elision is a cheap v1.x follow-up once
the ordering guarantee is tested.

---

## 10. DataFusion integration details

- **Versioning:** `cqlite-datafusion` pins a DataFusion major version per release and
  tracks upstream on its own cadence; core is insulated. DataFusion's API churn is the
  main maintenance cost of this crate — isolate all `datafusion::` imports here.
- **Registration UX:**
  ```rust
  // local
  ctx.register_table("ks.t", CassandraTable::local(dir, options)?)?;
  // cluster
  let cluster = SidecarCluster::connect(hosts, tls, auth).await?;
  ctx.register_catalog("cassandra", CassandraCatalog::new(cluster, catalog_opts));
  // → SELECT * FROM cassandra.ks.t
  ```
  The catalog implementation lists keyspaces/tables from the sidecar schema endpoint,
  enabling `SHOW TABLES` and lazy provider construction.
- **`statistics()`** returns row-count and byte-size estimates from `estimate()`
  aggregates (Statistics.db partition/row counts summed over the SSTable set, prorated by
  token fraction), marked `Precision::Inexact`. This feeds join-order planning — cheap
  and high-value.
- **EXPLAIN transparency:** `CassandraScanExec::fmt_as` displays: mode, splits, replicas
  chosen, pushed predicates (exact/inexact), pruned SSTable counts, and fetch strategy —
  ops teams will ask "what did this query do to my cluster" and EXPLAIN should answer it.
- **Metrics:** standard DataFusion `MetricsSet` per partition (rows, batches, bytes
  fetched, SSTables pruned/scanned, snapshot latency, fetch latency) — wired to the Epic
  AI observability story rather than a new one.

---

## 11. Phasing

**Phase 0 — contract review (now, zero code).** Review §4 against #942's in-progress API;
adjust either side while both are wet. Confirm §5 mapping against the current
Parquet/Arrow module. Exit: #942 API carries token_range, batch_hint, limit,
per-predicate disposition, drop-cancel.

**Phase 1 — local TableProvider.** `cqlite-datafusion` crate, local mode only:
schema mapping, scan over #942, pushdown lowering, LIMIT, statistics, EXPLAIN, metrics.
No sidecar crate. Deliverable: `datafusion-cli` querying an SSTable directory; joins
against Parquet in the same query. Exit criteria: parity fixtures (§12) green through
QueryView; second-consumer feedback filed against #942.
*Sequencing gate: after #1378 P0/P1 children (#1382, #1383, #1384) land.*

**Phase 2 — `cqlite-sidecar` + CLI fetch verb.** Sidecar client per §9.1–9.2, exercised
by `cqlite fetch --keyspace ks --table t [--token-range a,b] [--replica h] DIR`:
snapshot → download → verify → clean up. Independently useful (debugging, "give me a
local copy"), tested against docker e2e (#663) with a real sidecar container, zero
DataFusion in the loop. Exit: fetch verb green in CI against the pinned minimum sidecar
version; snapshot-leak chaos test (kill -9 mid-fetch → TTL cleans up) documented.

**Phase 3 — cluster mode.** Split planner + FetchSession wiring + spool management +
replica failover. Exit: distributed `SELECT` with partition-key pushdown demonstrating
split pruning against a 3-node docker cluster; Analytics-compatibility statement
validated (same cluster runs the Spark bulk reader).

**Phase 4+ (separate issues):** chunk-ranged Data.db fetch; multi-replica CL knob;
per-partition ordering declaration; WRITETIME/TTL virtual columns; delta-scan/CDC table
function over #696; aggregation-pushdown optimizer rule (revisit after #918 conclusions).

---

## 12. Testing strategy

1. **Correctness via existing parity fixtures.** Every Cassandra-compacted byte-parity
   fixture family (issue_1017/1019/1020/1240 + the #1387 TTL/tombstone/RT additions) runs
   through QueryView with results asserted against the Cassandra-computed expected live
   view. This is the primary defense against silent-wrong-results.
2. **Differential vs CQL.** Docker e2e (#663): load dataset, run a query corpus through
   (a) cqlite-datafusion and (b) CQL at CL=ONE against the snapshot-source replica; diff.
   Corpus includes: point lookups, IN fan-out, clustering ranges, token ranges, regular-
   column filters, LIMIT, projections of collections/UDTs, WRITETIME columns (when
   enabled), and tombstone-heavy tables.
3. **DataFusion SQL-level tests** using `sqllogictest` (DataFusion's own harness) against
   fixture directories — cheap, huge coverage of expression edge cases interacting with
   the type mapping.
4. **Pushdown assertions.** EXPLAIN-based tests asserting split pruning and SSTable
   pruning counts for each row of the §6.2 matrix; regression-gate the point-lookup
   fast path (Epic C alignment).
5. **Lifecycle/chaos.** kill -9 mid-query → no snapshot older than TTL remains; sidecar
   returns 500 on delete → logged, TTL backstop verified; spool disk budget exceeded →
   query fails cleanly, other queries unaffected; replica down mid-fetch → failover.
6. **Cancellation.** LIMIT 1 over a huge table: assert file handles/spool released and
   fetches aborted within a bound after the stream drops.
7. **Compatibility matrix in CI**: pinned minimum sidecar version + latest; Cassandra
   4.1 and 5.x snapshot-TTL behaviors.

---

## 13. Open questions (decisions wanted at review)

1. **Sidecar version floor.** Match Analytics' current declared minimum, or the oldest
   sidecar with snapshot TTL? Proposal: match Analytics; gate TTL-less operation behind
   the explicit config flag (§9.2).
2. **`inet` and `duration` Arrow mappings** (§5 ⚠ rows): confirm against what the Parquet
   exporter ships today; changing later is a breaking schema change for consumers.
3. **Schema authority in cluster mode:** sidecar schema endpoint vs system_schema
   components in the snapshot. Proposal: sidecar endpoint is authoritative; error on
   mismatch with SSTable serialization headers (fail-closed, Epic AC style) rather than
   silently trusting either.
4. **Multi-table queries, one snapshot or many?** A join of two Cassandra tables
   currently implies two snapshot sessions. Acceptable for v1? (Proposal: yes; a
   query-scoped session manager that batches per-host snapshot creation is a v2 nicety.)
5. **Spool encryption at rest.** Snapshot data lands on the DataFusion host's disk;
   environments with encrypted Cassandra disks may require spool encryption or
   tmpfs-only operation. Config surface or documented deployment guidance?
6. **Where does QueryView live** — refactor of `merge/mod.rs` into a shared engine with
   policies (preferred; keeps one parity surface), or a wrapper filtering CompactionView
   output (faster to ship, risks divergence)? This interacts with Epic T's 12k-line
   merge split — ideally the split produces the seam QueryView needs.
7. **Naming**: `cqlite-datafusion` + `cqlite-sidecar`, or fold the sidecar client into a
   broader `cqlite-cluster`? (Ring + schema come from the sidecar today but could come
   from CQL system tables later; `cqlite-cluster` leaves room.)

---

## 14. Alternatives considered

- **Topology B — cqlite-flight deployed per-node as the data plane.** Predicates ship to
  the node; only post-filter Arrow crosses the wire. Rejected for v1: requires deploying
  and operating cqlite on every Cassandra node and burns data-node CPU — the opposite of
  complementary-to-the-ecosystem, and the opposite of why bulk readers exist. The
  control-plane/data-plane split in this design leaves the door open: a Flight-backed
  `SstableSource` could be added later without touching the provider.
- **Direct CQL range scans instead of SSTable reads.** Simple, consistent, no snapshot
  machinery — and hammers the cluster's read path, which is precisely what bulk readers
  avoid. Rejected; CQL remains only a possible control-plane fallback for ring/schema.
- **A new read-path merge for the provider.** Rejected (§7): duplicates the hardest,
  most parity-sensitive logic in the project and would need its own audit.
- **Contribute a Cassandra connector to DataFusion upstream instead.** Upstream `datafusion-contrib`
  visibility is appealing, but the connector's value is cqlite's parser and merge; it
  belongs in this repo with these tests. Publishing the crate to crates.io and listing it
  in DataFusion's known-providers docs achieves the visibility without splitting the code.

---

## Appendix A — end-to-end flow (cluster mode)

```
SELECT name, ts FROM cassandra.ks.events
WHERE device_id = 'abc'            -- partition key
  AND ts > '2026-06-01'            -- clustering key

1. Plan     : provider lowers both predicates (Exact); statistics() consulted.
2. Route    : token('abc') → split S17 → replicas {n2, n5, n9} → pick n5 (local DC,
              rendezvous). All other splits pruned; ExecutionPlan has 1 partition.
3. Snapshot : PUT snapshot cqlite-df-3f9c… (ttl=6h) on n5 (session shared if reused).
4. Manifest : list components for ks.events on n5.
5. Prune    : fetch Statistics/Filter/Index/CompressionInfo for each SSTable;
              bloom-check 'abc'; ts-range check vs min/max clustering + timestamps;
              3 of 11 SSTables survive.
6. Fetch    : spool the 3 surviving SSTables' components (v2: only the Data.db chunks
              the partition index points at).
7. Scan     : #942 execute(ScanRequest{ sstables: 3, token_range: S17,
              projection: [name, ts], predicates: [pk=, ts>], view: QueryView,
              batch_hint: 8192 }) → merged, tombstone-correct live rows.
8. Stream   : RecordBatches to DataFusion; FilterExec re-checks ts> (Inexact-safe).
9. Cleanup  : streams complete → session.close() → DELETE snapshot; spool dir removed.
```
