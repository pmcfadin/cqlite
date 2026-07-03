# CQLite Arrow Flight Server + Trino Connector Index

**Subsystem**: `cqlite-flight-trino`  
**Status**: Phases 1–6 complete (E2E functional, live Cassandra + Trino integration verified)  
**Owner**: Jon Haddad  
**Key docs**: `docs/flight-trino/PLAN.md` (design locked), `JOURNAL.md` (append-only changes)

---

## Summary

CQLite's Arrow Flight server runs co-located on every Cassandra node and exposes SSTables to analytical queries (via Trino) through a **compact-on-read** model. The Rust server enumerates node-local SSTables, performs a k-way compaction merge (originals untouched), applies server-side token-range and predicate filters, and streams results as Arrow record batches. The Java Trino connector discovers nodes/tokens via Cassandra Sidecar, builds splits (one token range → one replica), and pulls Arrow Flight streams. **Freshness guarantee**: reads are pinned to Sidecar-created snapshot hardlink sets (not live files), avoiding file-shifting SIGBUS and in-flight inconsistency; explicit refresh contract landed in issue #1749 (`Database::refresh()` on the connector/client side, stateless on the server).

---

## Key Classes (File:Line)

### Rust Server (`cqlite-flight/src/`)

| Class | Responsibility | File:Line |
|-------|---|---|
| `CqliteFlightService` | gRPC Flight service impl; parses tickets, builds producers, streams Arrow | `service.rs:107` |
| `MergeProducer` | Drives k-way merge over SSTable paths; applies token/predicate/projection filters; emits RecordBatch | `producer.rs:262` |
| `DirSource` | Enumerates `*-Data.db` files under a table directory (live or snapshot); resolves both Cassandra UUID-layout and write-engine layout | `producer.rs:86` |
| `SstableSource` trait | Abstraction for enumerating SSTable paths (live dir vs snapshot — enables swapping without touching merge logic) | `producer.rs:80` |
| `FlightTicket` | Request payload (JSON): keyspace, table, DDL, snapshot name, token range, predicates, columns, aggregation | `ticket.rs:225` |
| `PredicateExpr` | Recursive boolean tree (AND/OR/NOT + leaves) for server-side predicate pushdown | `ticket.rs:110` |
| `Aggregation` | Server-side PARTIAL aggregate spec (group-by + function pushdown, issue #841) | `ticket.rs:211` |

### Java Connector (`trino-connector/src/main/java/`)

| Class | Responsibility | File:Line |
|-------|---|---|
| `CqliteFlightConnector` | Plugin entry point; composes Metadata, SplitManager, PageSource | `CqliteFlightConnector.java:15` |
| `CqliteFlightMetadata` | Sidecar-backed metadata: keyspace existence, table DDL extraction, constraint translation | `CqliteFlightMetadata.java:48` |
| `CqliteFlightSplitManager` | Token-range-replicas → splits (1 range → 1 replica); emits Flight endpoints | `CqliteFlightSplitManager.java` |
| `CqliteFlightPageSourceProvider` | Flight `DoGet` (Java arrow-flight) → `VectorSchemaRoot` → Trino `Page` | `CqliteFlightPageSourceProvider.java` |
| `SidecarClient` | HTTP client for Cassandra Sidecar (ring, token-range-replicas, schema) | `sidecar/SidecarClient.java:23` |
| `FlightTicketJson` | JSON serialization for tickets sent to server | `FlightTicketJson.java` |

---

## Extension Points / Pluggability Seams

1. **`SstableSource` trait** (`producer.rs:80`)  
   **Seam**: Abstraction for SSTable enumeration. Currently `DirSource` (filesystem walk); Phase 3 swaps snapshot-dir reading without touching merge logic.  
   **Extenders**: A memtable source could implement this trait, feeding merged rows to the same producer→Arrow pipeline.

2. **`MergeProducer::with_spec` / `with_aggregation`** (`producer.rs:282–310`)  
   **Seam**: Configurable scan spec (token range, predicates, projection) and aggregation plan. Filters are applied post-merge, making them reusable for any row source.  
   **Extenders**: A hybrid live+memtable source could feed identical rows through the same filter chain.

3. **`FlightTicket` JSON contract** (`ticket.rs`, v2 format)  
   **Seam**: Cross-language wire format (Rust server ↔ Java connector). Extensible via `#[non_exhaustive]` enums (`PredicateOp`, `PredicateExpr` node types, `Aggregation`).  
   **Extenders**: Clients (Spark, DuckDB, etc.) can produce tickets as plain JSON without Rust coupling; server vets all input.

4. **`SidecarClient` HTTP endpoints** (`sidecar/SidecarClient.java:39–52`)  
   **Seam**: Three GET endpoints (`/ring`, `/token-range-replicas`, `/schema`). Snapshot PUT/DELETE/GET not yet wired in Java client.  
   **Extenders**: Cassandra can add memtable-flushing or read-freshness endpoints; connector calls them before Flight tickets.

5. **`CqliteFlightService` batch size** (`service.rs:116–120`)  
   **Seam**: Configurable `batch_size` (default 8192 rows/Arrow batch). Tuned per workload (TUI scan → small batches; Trino full scan → large).  
   **Extenders**: Could feed from config or per-query hints in the ticket.

---

## Hard Couplings (Assumptions that Would Break)

1. **Live data dir layout: `<data_dir>/<keyspace>/<table>[-<uuid>]/`**  
   **Coupling**: `DirSource::table_base_dir` (producer.rs:141) hardcodes Cassandra's UUID-suffixed layout and the write engine's bare-name layout. Snapshot paths assume hardlinks under `snapshots/<name>/`.  
   **Break**: Alternative engine with a different directory structure (e.g., per-partition files) requires a new `SstableSource` impl.

2. **SSTable file-naming convention: `<gen>-<size>-<type>-Data.db`, `*-Summary.db`**  
   **Coupling**: `generation_of` (producer.rs:210) parses generation from the filename; `sstable_token_span` (producer.rs:223) assumes a sibling `Summary.db`.  
   **Break**: Non-Cassandra storage formats (e.g., Parquet-based SSTable) need custom parsers for generation and token spans.

3. **Compaction-merge API: `KWayMerger::new(paths, schema) → step() → MergeStep`**  
   **Coupling**: `producer.rs:430` assumes the merge engine accepts file paths and drives sync iteration. If Cassandra's merge API changes to async-only or requires handles instead of paths, producer logic rewrites.  
   **Break**: Embedded storage engines (in-JVM plugins) must expose a compatible merge interface.

4. **Sidecar snapshot model**  
   **Coupling**: `DirSource::resolve` (producer.rs:119) assumes snapshots are hardlink sets under `<table>/snapshots/<name>/`. Cassandra Sidecar creates/deletes them; Flight reads frozen copies.  
   **Break**: If Cassandra moves snapshots to remote storage (S3, etc.), Flight needs a different snapshot source (cloud-aware `SstableSource`).

5. **Separate memtable and SSTable domains**  
   **Coupling**: `DirSource` only discovers SSTables; memtable is completely invisible. Flush/compaction is external (Cassandra's job). Connector gets a point-in-time snapshot BEFORE queries run.  
   **Break**: For real-time analytics (Q1), a memtable-aware source must merge memtable iterator(s) with SSTables; requires Cassandra to export memtable-flushing or iterator hooks (CEP-11 or sidecar extension).

6. **Row reconstruction and tombstone suppression**  
   **Coupling**: `build_row_from_scan` (query/select_executor.rs, reused by Flight) assumes `LWW` timestamp reconciliation and Cassandra-style tombstone markers. Non-LWW or MVCC engines need different row builders.  
   **Break**: Alternative consensus models (vector clocks, CRDT) require new reconciliation logic.

7. **Token filtering via `DecoratedKey.token` (i64 Murmur3)**  
   **Coupling**: `token_in_half_open_range` (ticket.rs:396) assumes tokens are signed 64-bit Murmur3 hashes in half-open `(start, end]` ranges.  
   **Break**: Ring topologies with different token spaces (consistent hashing, etc.) need custom range predicates.

---

## Q1 Relevance: Freshness for Analytical Reads

**Question**: When DataFusion or Trino reads a node through CQLite's Arrow Flight connector, the read sees only flushed SSTables. What must change so an analytical read reflects ALL node-local state — memtable + SSTable?

### Current State (Stateless Server, Snapshot-Based Freshness)

- **Server side**: `DirSource::data_paths` enumerates SSTables from a **Sidecar snapshot** (`ticket.snapshot`). No refresh call; read is atomic at snapshot creation time.
- **Client side**: Trino connector (Phase 5, JOURNAL) triggers `PUT .../snapshots/<name>` before splits, passes snapshot name in tickets, deletes it after scans complete.
- **Staleness window**: Connector's split → snapshot lifetime (typically seconds). Memtable data **completely invisible** (not in snapshot).

### What Would Need to Change (Two Paths)

**Path A: Sidecar Extension (Recommended for Cassandra 5.0+)**
- Sidecar adds `/api/v1/keyspaces/:ks/flush/:table` endpoint: flushes a table's memtable to SSTable **synchronously** (or returns when flush completes).
- Connector calls `flush` for each table BEFORE creating snapshot; snapshot then includes the newly-flushed generation.
- **Pros**: Cassandra retains write control; CQLite is pure reader; no CEP-11 dependency.
- **Cons**: Latency spike; blocks memtable writes.

**Path B: CEP-11 Pluggable Memtable API (Cassandra Trunk/7.0+)**
- Cassandra trunk has `org/apache/cassandra/db/memtable/Memtable_API.md` (pluggable API).
- CQLite could run **in-JVM** (WASM or native plugin) with direct access to Cassandra's memtable iterator.
- Flight server merges `memtable.iterator() + sstables` in-process, no snapshot needed.
- **Pros**: Zero-latency; realtime analytics.
- **Cons**: Requires JVM plugin; tight coupling to Cassandra internals; CEP-11 still evolving.

**Path C: Separate Memtable Tail Source (CQLite Standalone)**
- Cassandra CDC pipes memtable commits (mutations) to a side channel (e.g., Sidecar-hosted WAL log).
- CQLite Flight reads SSTable snapshot + CDC tail from Sidecar in parallel; merges in the `MergeProducer`.
- **Pros**: Decoupled; works with Cassandra as-is (CDC built-in).
- **Cons**: Eventual-consistency lag (CDC tail behind live writes); network overhead.

### Code Anchor for Issue #1749

- **Database::refresh()** (`cqlite-core/src/lib.rs:321`): Async method on the client-side `Database` object; calls `self.storage.refresh().await`.
- **RefreshReport** (`cqlite-core/src/storage/sstable/refresh.rs:55`): Reports tables scanned, readers added/removed.
- **Refresh contract** (`lib.rs:303–323`): Explicit only (no watching); in-flight queries unaffected (snapshot isolation); atomic fail-closed; no index rebuild.
- **Flight server** does NOT call refresh; it's a stateless handler. Refresh is a **client concern** — connector could call it between query batches if Cassandra exports a real-time API.

---

## Q2 Relevance: Feasibility as Storage Engine

**Question**: How feasible is CQLite as (a) an alternative/replacement storage engine inside Cassandra, and (b) an adjacent OLAP storage engine?

### (A) CQLite as Replacement Engine (In-JVM Plugin)

**Feasibility: Medium-High (Trunk only, CEP-11 required)**

- **Hard requirement**: Cassandra Trunk (7.0+) must expose:
  - `MemtableFactory` (CEP-11, exists in Trunk)
  - `SSTableFormat` pluggable API (exists; Cassandra uses it for BIG vs BTI)
  - Query engine integration (would need new SeekIterator impl for CQLite rows)
- **Effort**:
  - Wrap CQLite's `KWayMerger` as a Cassandra `SeekIterator<CellName, ByteBuffer>`.
  - Implement `MemtableFactory` creating a Rust memtable (via napi-rs or in-JVM FFI).
  - Route compaction to CQLite's write-engine (already Cassandra-compatible).
- **Seams that exist**:
  - `SSTableFormat` interface (src/java/.../io/sstable/format/SSTableFormat.java) — CQLite writes/reads exist.
  - Pluggable Memtable API (CEP-11, pluggable factory).
  - Compaction framework (delegates to `SSTableFormat.compactWithoutSnapshots`).
- **Seams that DON'T exist**:
  - No pluggable **read path**; query engine hardcodes Cassandra's row builders.
  - No FFI layer for calling Rust merge from JVM (would need JNI or sidecar).
  - Storage-agnostic `Platform` (async I/O layer) — Cassandra's I/O is hardwired.
- **Verdict**: Feasible but requires CEP-11 completion + significant JVM-side glue.

### (B) CQLite as Adjacent OLAP Engine (Sidecar)

**Feasibility: High (Current architecture proves it)**

- **Status**: Already live. Flight server is a sidecar; Trino queries via Arrow without touching Cassandra's write path.
- **Deployment**: One `cqlite-flight` process per node; Trino connects via HTTP/gRPC.
- **Isolation**: Reads are snapshots; compaction is CQLite-only; no contention with OLTP writes.
- **Extensibility**:
  - Add Sidecar `/flush` endpoint → zero memtable staleness at cost of flush latency.
  - Wire CDC tail (WAL log from Sidecar) → tail SSTables → hybrid freshness-cost tradeoff.
  - Cache Arrow schemas in Trino connector → avoid ticket round-trip (optimization).
- **Existing seams**:
  - `SstableSource` trait (producer.rs:80) — swap snapshot dir for memtable iterator + snapshot.
  - `FlightTicket` JSON (ticket.rs:225) — add freshness hints (e.g., `max_staleness_ms`).
  - Sidecar client (sidecar/SidecarClient.java:23) — add flush, tail-read, memtable-stat endpoints.
- **Verdict**: Proven. Incremental extensions (flush + CDC tail) unlock real-time analytics without restructuring.

### Comparison: Storage Engine vs Sidecar

| Aspect | In-JVM (Replacement) | Sidecar (Adjacent) |
|--------|---|---|
| **Cassandra changes** | CEP-11, FFI, query engine rewiring | Sidecar extensions only (optional) |
| **Latency** | Zero (in-process merge) | Network + snapshot creation overhead |
| **Risk** | High (JVM crash → C* data loss) | Low (sidecar failure = read-only downtime) |
| **Freshness** | Realtime (memtable visible immediately) | Snapshot lag (seconds) or flush lag (latency spike) |
| **Deployment** | JVM plugin, requires C* restart | Docker container, zero C* changes |
| **Analyst workflow** | SELECT queries go through C* | SELECT queries go through Trino (different skills) |

---

## Trunk vs Cassandra 5.0 Notes

### Cassandra 5.0.x Gaps (Missing Seams)

1. **No CEP-11 pluggable Memtable API** → Can't wire in-JVM CQLite engine; sidecar-only.
2. **No Sidecar flush API** → Analyst reads lag behind real-time writes (snapshot staleness).
3. **No schema registry hooks** → CQLite DDL must be passed in tickets; schema changes require client refresh.

### Cassandra Trunk (7.0) New Capabilities

1. **CEP-11 Memtable_API.md** (merged 2026-07-03) → Pluggable memtable factories; enables in-JVM CQLite.
2. **`SSTableFormat` API matured** → Format plugins stable (BIG na/nb + BTI da all tested).
3. **Sidecar `/schema` endpoint** → Already stable; Trino connector uses it.

### Code Refs (Trunk)

- Memtable API: `src/java/org/apache/cassandra/db/memtable/Memtable_API.md`
- SSTableFormat: `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java`

---

## Current Limitations & Future Hooks

| Limitation | Workaround | Future Hook |
|---|---|---|
| Memtable invisible | Flush before scan (latency) | CEP-11 / sidecar memtable iterator |
| Compaction-during-scan | Snapshot read (staleness) | Streaming snapshot merge (hard) |
| Index rebuild on refresh | Manual call to `db.refresh()` | Lazy index revalidation on access |
| Counters unsupported | Document in schema | In-flight counter merge (Cassandra design gap) |
| Wide rows (>1GB) | Batched export (memory) | Streaming merge (issue #591 tracked) |

---

## References

- **PLAN.md** — Locked design (compact-on-read, snapshot-based freshness, token-range filtering, predicate pushdown).
- **JOURNAL.md** — Phase-by-phase implementation log (6 phases, all shipped as of 2026-06-27).
- **cqlite-core** — `storage/write_engine/merge.rs:645`, `storage/sstable/refresh.rs:216`, `query/select_executor.rs:156`.
- **Cassandra Trunk** — CEP-11 (pluggable memtable), `SSTableFormat` API, Sidecar `/schema`.
- **Issue #1749** — Explicit `Database::refresh()` contract (landed; no server changes needed).
- **Issue #591** — Wide-row streaming (compaction-merge in-progress, status: deferred to v0.13).

---

**Last updated**: 2026-07-03 (post-Phase 6, pre-freshness audit)
