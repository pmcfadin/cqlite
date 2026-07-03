# Cassandra Memtable API — CEP-11 Pluggable Memtable Index

## Summary

The CEP-11 Pluggable Memtable API (CASSANDRA-17034, Cassandra 4.1+) abstracts write-in-memory storage behind a pluggable `Memtable` interface. Implementations (SkipListMemtable, TrieMemtable, ShardedSkipListMemtable) define in-process writes, partition iteration (for reads and flushes), durability signaling (commitlog coordination), and lifecycle management. Configuration via YAML and `CREATE TABLE ... WITH memtable = '<name>'` is per-table, cluster-wide (with fallback). The API governs when writes are durable (via `Factory.writesAreDurable()`, disabling commitlog replay), when they bypass the commitlog entirely (`writesShouldSkipCommitLog()`), and whether the memtable is the primary data store (`streamToMemtable()`, `streamFromMemtable()` — bypassing zero-copy streaming). **Hard truth**: A memtable is a write-only buffer; its contents are **not directly queryable as SSTables** until flushed. External readers (like CQLite's Arrow Flight server) see only flushed SSTables, not live memtable data.

---

## Key Classes & Interfaces

| Class | File | Responsibility |
|-------|------|-----------------|
| **Memtable** (interface) | `src/java/org/apache/cassandra/db/memtable/Memtable.java:60` | Core contract: `put()`, `partitionIterator()`, flush/lifecycle hooks, statistics, memory tracking |
| **Memtable.Factory** | `src/java/org/apache/cassandra/db/memtable/Memtable.java:77` | Instantiation via reflection; durability flags (`writesAreDurable`, `writesShouldSkipCommitLog`, `streamToMemtable`, `streamFromMemtable`) |
| **Memtable.Owner** | `src/java/org/apache/cassandra/db/memtable/Memtable.java:160` | Backward signals from memtable (flush requests, index collection); usually `ColumnFamilyStore` |
| **Memtable.FlushablePartitionSet** | `src/java/org/apache/cassandra/db/memtable/Memtable.java:320` | Read-only view for flushing (partition iterator + encoding stats + commit log bounds) |
| **AbstractMemtable** | `src/java/org/apache/cassandra/db/memtable/AbstractMemtable.java:46` | Statistics tracking (min timestamp, min local deletion time, columns, encoding stats); flush listener registration |
| **AbstractMemtableWithCommitlog** | `src/java/org/apache/cassandra/db/memtable/AbstractMemtableWithCommitlog.java:32` | Commit log position tracking (lower/upper bounds, write barrier); controls write acceptance during switchout |
| **AbstractAllocatorMemtable** | `src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:57` | Memory allocation integration via `MemtableAllocator` from shared pool; flush scheduling on TTL/size thresholds |
| **SkipListMemtable** | `src/java/org/apache/cassandra/db/memtable/SkipListMemtable.java:68` | Canonical impl: single `ConcurrentSkipListMap<PartitionPosition, AtomicBTreePartition>`; Cassandra ≤4.1 default |
| **TrieMemtable** | `src/java/org/apache/cassandra/db/memtable/TrieMemtable.java:91` | GC-efficient sharded trie-based partitions (in-memory trie + off-heap buffers); trunk default; VLDB'24 paper design |
| **ShardedSkipListMemtable** | `src/java/org/apache/cassandra/db/memtable/ShardedSkipListMemtable.java` | Token-space sharded variant (N skip lists); reduces write contention; hashing partitioners only |
| **MemtableParams** | `src/java/org/apache/cassandra/schema/MemtableParams.java:51` | Configuration resolver: YAML → factory factory (via reflection, caching by name); schema binding |

---

## Extension Points (Pluggability Seams)

1. **Factory Registration (Reflection-based)**
   - **Seam**: `MemtableParams.getMemtableFactory()` at `src/java/org/apache/cassandra/schema/MemtableParams.java:217`
   - **Mechanism**: Class name in `cassandra.yaml` `memtable.configurations.<name>.class_name` (prefixed with `org.apache.cassandra.db.memtable.` if short name); invokes static `factory(Map<String, String>)` method or reads static `FACTORY` field.
   - **Config Inheritance**: YAML `inherits:` propagates class and parameter overrides; default configuration must exist.
   - **Per-Table Binding**: `CREATE TABLE ... WITH memtable = '<config_name>'` stores in `TableParams.memtable`; fetched by `ColumnFamilyStore.init()` at line 523.
   - **Fallback**: `getWithFallback()` silently reverts to default on instantiation errors (schema mismatch avoidance).

2. **Write Durability Signaling**
   - **Seam**: `Factory.writesAreDurable()`, `Factory.writesShouldSkipCommitlog()` at `src/java/org/apache/cassandra/db/memtable/Memtable.java:107–152`
   - **Effect**: Disables commitlog replay on crash recovery / disables commitlog writes entirely (persistent memtables only; incompatible with CDC/PITR).
   - **Usage**: Queried at table init; affects `CommitLogReplayer` and commitlog writing paths (not exposed here but implicit).

3. **Streaming / Long-Lived Memtables**
   - **Seam**: `Factory.streamToMemtable()`, `Factory.streamFromMemtable()` at `src/java/org/apache/cassandra/db/memtable/Memtable.java:133–152`
   - **Effect**: Disables zero-copy streaming; incoming SSTables replayed as mutations into memtable; outgoing flush creates temporary SSTables for streaming.
   - **Usage**: Queried by streaming/repair logic (Gossiper, StreamCoordinator—not exposed here).

4. **Flush Decision Hook**
   - **Seam**: `Memtable.shouldSwitch(FlushReason, TableMetadata)` at `src/java/org/apache/cassandra/db/memtable/Memtable.java:420`
   - **Callback Pattern**: Reason enum includes `SCHEMA_CHANGE`, `OWNED_RANGES_CHANGE`, `SNAPSHOT`, `STREAMING`, `REPAIR`, `SIZE_EXCEEDED`, etc.
   - **Post-Rejection Signals**: If `shouldSwitch()` returns false, follow-up calls: `metadataUpdated()`, `localRangesUpdated()`, `performSnapshot()`.
   - **Default Behavior**: Unconditionally flush on `SIZE_EXCEEDED`; persistent memtables return false.

5. **Partition Sharding (Token Space Awareness)**
   - **Seam**: `Owner.localRangeSplits(int shardCount)` at `src/java/org/apache/cassandra/db/memtable/Memtable.java:181`
   - **Returns**: `ShardBoundaries` for splitting locally-owned token ranges evenly; used by sharded impls to assign partitions to shards.
   - **Invalidation Signal**: `shouldSwitch(OWNED_RANGES_CHANGE)` → `localRangesUpdated()` on ring changes.

6. **Metadata Evolution**
   - **Seam**: `Memtable.metadataUpdated()` hook and `shouldSwitch(SCHEMA_CHANGE, latest)` at `src/java/org/apache/cassandra/db/memtable/Memtable.java:436, 420`
   - **Checks**: comparator, memtable factory; triggers flush on mismatch (via `initialComparator`, `initialFactory` in `AbstractAllocatorMemtable` line 122).

---

## Hard Couplings

1. **CommitLog Coupling**
   - `AbstractMemtableWithCommitlog.switchOut()` at line 55 receives `OpOrder.Barrier` and `AtomicReference<CommitLogPosition>` (upper bound).
   - Write acceptance gated by barrier + commit log position comparison (`accepts()` at line 71); memtable must reconcile operator-order constraints with log position atomicity.
   - **Implication for Q1**: A memtable can do internal durability, but the commitlog position tracking is **mandatory** for cluster consistency (replication, repair, streaming require position markers).

2. **MemtableAllocator Integration (Memory Pool)**
   - `AbstractAllocatorMemtable.MEMORY_POOL` (static singleton) at line 61 shared across all memtables on a node.
   - `put()` blocks if pool exhausted; `allocate()` coordinates with global cleaner (`flushLargestMemtable` callback).
   - **Implication for Q2**: Custom memtables must either (a) use the allocator pool (coupling to heap/slab logic), (b) override `addMemoryUsageTo()` for tracking, or (c) bypass entirely (rare, high-risk; can cause OOM if pool is unaware).

3. **ColumnFamilyStore as Owner**
   - Memtable holds reference to `Owner` (usually CFS); signals flush via `signalFlushRequired()`, queries current memtable, collects index memtables.
   - CFS owns the `memtableFactory` (line 286 of ColumnFamilyStore.java) and creates memtables via `Factory.create()`.
   - **Implication**: External readers (Q1) must hook at CFS level, not memtable level, to access live writes.

4. **Partition Representation (AtomicBTreePartition / ImmutableBTreePartition)**
   - Concrete impls return `BTreePartitionData`-based partitions in flush sets.
   - Schema-aware clustering/value encoding baked into partition representation; no decoupling layer.
   - **Implication for Q2**: A custom memtable must understand Cassandra's partition encoding (clustering, cell order) to serve reads; this is not abstracted by the interface.

5. **OpOrder Write Ordering**
   - `put()` requires `OpOrder.Group` (at line 186); must respect globally-ordered barriers (`switchOut()` barrier).
   - Barrier is issued before writes are re-routed to the next memtable; late arrivals update commit log upper bound atomically.
   - **Implication**: Memtable switchout is not merely switching a reference; it's a multi-phase coordinated halt that requires compliance with the barrier protocol.

6. **Encoding Stats Snapshot (Non-Incremental)**
   - `FlushablePartitionSet.encodingStats()` must return a frozen stats object at the moment of flush.
   - If memtable is still being written to during flush (some impls allow), stats may be inconsistent with partition contents.
   - **Implication**: Flushing a live memtable is dangerous; Cassandra currently flushes after `switchOut()` (barrier stops writes), but the interface allows for long-lived memtables that may not stop.

---

## Q1 Relevance: Freshness (Memtable Visibility in OLAP Readers)

**The Core Blocker**: Memtable contents are **not exposed to external readers** by the current API. CQLite's Arrow Flight server or Trino would see only flushed SSTables.

**Current Options to See Memtable Data**:

1. **Option A: Memtable Iterator + Handoff**
   - `Memtable.FlushablePartitionSet` is returned by `getFlushSet(from, to)` (line 311) as read-only.
   - In theory, a reader could call `getFlushSet()` on the live memtable, bypassing flush. **But**: This is not a public API; no per-table access to memtable references; ColumnFamilyStore keeps memtables private.
   - **Risk**: Iterating a live memtable during concurrent writes = stale/torn reads; no contract for isolation.

2. **Option B: `streamFromMemtable()` Workaround**
   - If `Factory.streamFromMemtable()` returns true, Cassandra's streaming creates temporary SSTables from memtable for outbound transfer.
   - A sidecar could hook into the streaming layer to intercept these temporary SSTables. **But**: Not designed for OLAP; no control over flushing triggers.

3. **Option C: Persistent Memtable (Long-Term Store)**
   - `Factory.writesAreDurable() == true` + `shouldSwitch(FLUSH) == false` → memtable is **never flushed**.
   - Memtable becomes the primary data store (e.g., persistent memory, DiskLSMTree). External reader could iterate live memtable if it's queryable.
   - **Requirement**: Memtable must remain open for reads; interface has no "read from memtable" seam, but iterator is exposed via `UnfilteredSource` (line 60).

**Missing Seam for Q1**: A public, versioned `Memtable.getSnapshotIterator(from, to)` with isolation guarantees (MVCC snapshot, timestamp cutoff) would enable OLAP readers to include memtable data without flushing.

---

## Q2 Relevance: Feasibility as Alternative Storage Engine

### (a) **CQLite-as-Memtable (Embedded)**
Can CQLite replace SkipListMemtable?

- **Interface Compliance**: CQLite would need to implement `Memtable`, `Factory`, and provide `put()`, `partitionIterator()`, `getFlushSet()`, lifecycle hooks.
- **Blocking Issue 1 — Write Ordering**: CQLite is currently read-only; implementing durable ordered writes + commit log coordination is 6–9 month effort (memtable API doesn't simplify this).
- **Blocking Issue 2 — Memory Pooling**: CQLite uses Rust memory; `MemtableAllocator` is Java heap/off-heap. A wrapper around CQLite as memtable would need to report memory to the pool or risk OOM (coupling at line 190–193 of AbstractAllocatorMemtable).
- **Blocking Issue 3 — Clustering/Partition Encoding**: CQLite uses Arrow + Parquet; Cassandra uses BTreePartition + clustering comparators. Transcoding at flush boundary is expensive.
- **Verdict**: **Not feasible** without CQLite becoming a full Java library (or a JNI bridge with gc/ordering guarantees).

### (b) **CQLite as Adjacent OLAP Engine (Sidecar)**
Can CQLite read Cassandra's flushed SSTables and serve analytical queries alongside the write path?

- **Seam Exists**: `Memtable.Owner.getIndexMemtables()` (line 172) and flush signals allow a sidecar to listen for completed flushes and import SSTables.
- **Current Roadblock**: Memtable flush decisions are internal to CFS; no public hook for "flush completed" events. Must poll SSTables directory or integrate via replication/CDC.
- **Data Freshness Trade-off**: An OLAP reader sees only flushed SSTables + (optionally) live memtable via snapshot iterator. **Gap**: For analytical recency (minutes-old data), must either (1) trigger periodic flushes (expensive), (2) stream memtable snapshot (requires custom hook), or (3) accept eventual consistency (streamed SSTables lag by flush interval).
- **Why This Works**: Cassandra's SSTable format is immutable and durable post-flush; no ordering constraint needed. CQLite can read any completed SSTable without coordination.
- **Verdict**: **Feasible, with modest integration work**. Requires: (1) memtable snapshot iterator hook, (2) CDC-like "flush completion" event feed, or (3) polling flushed SSTables directory. CQLite does not need to be embedded.

---

## Seams for OLAP Integration (Proposals)

1. **Memtable Snapshot Iterator (For Freshness)**
   ```java
   interface Memtable {
       /**
        * Return a read-only, point-in-time snapshot of the memtable for OLAP queries.
        * Must not expose uncommitted writes (respect isolation level).
        */
       UnfilteredPartitionIterator snapshotIterator(long maxTimestamp, ColumnFilter columns);
   }
   ```
   - Would allow Arrow Flight to include memtable data in `SELECT` results without flushing.
   - Needs MVCC timestamp coordination (not currently in Memtable interface).

2. **Flush Completion Event**
   ```java
   interface Memtable.Owner {
       void onFlushCompleted(List<SSTableReader> readers, ColumnFamilyStore table);
   }
   ```
   - Would allow a sidecar listener to ingest flushed SSTables into a separate analytical store (CQLite Arrow Flight) with low latency.
   - Currently, flushing is async; `LifecycleTransaction` tracks it, but no public callback.

3. **Sharded Flush Cursor**
   ```java
   interface FlushablePartitionSet {
       long getFlushSequenceNumber(); // Monotonic, used to detect replay
   }
   ```
   - Would allow an external reader to checkpoint progress and resume from last flushed position (like Kafka offset).

---

## Trunk vs. 5.0 Deltas

| Feature | Cassandra 5.0 | Trunk (7.0) | Notes |
|---------|---------------|------------|-------|
| **CEP-11 Pluggable API** | ✓ (added 4.1) | ✓ | Identical core interface; no breaking changes observed. |
| **TrieMemtable** | ✓ (default since 4.1) | ✓ | Off-heap trie; minor perf tuning in trunk. |
| **Factory.createMemtableMetricsReleaser()** | ✗ | ✓ (line 95) | Trunk: metrics cleanup hook for custom impls. |
| **Memtable.lastToken()** | ✗ | ✓ (line 463) | Trunk: shard-aware optimization (default throws `UnsupportedOperationException`). |
| **`streamFromMemtable()` Semantics** | ✓ | ✓ | Identical; streaming creates temp SSTables if true. |
| **ShardBoundaries API** | ✓ (line 181) | ✓ | No changes; used by TrieMemtable for shard splits. |
| **Allocator Integration** | ✓ | ✓ | Static pool, heap/off-heap tracking unchanged. |
| **Config Inheritance (YAML `inherits:`)** | ✓ (4.1+) | ✓ | No changes; config expansion logic stable. |

**Recommendation**: Code targeting both 5.0 and trunk can assume CEP-11 stable; use feature-detection for `createMemtableMetricsReleaser()` and `lastToken()` if needed.

---

## Hard Coupling Inventory (For Alternative Engine Integration)

| Coupling | Severity | Mitigation |
|----------|----------|-----------|
| CommitLog position tracking | **CRITICAL** | Memtable must track and report lower/upper bounds for replay/streaming. No way to opt out. |
| MemtableAllocator pool | **HIGH** | Custom memtable must report memory usage or risk silent OOM. Can override `addMemoryUsageTo()`. |
| BTreePartition encoding | **HIGH** | `getFlushSet()` returns BTree-based partitions; custom structures need conversion layer. |
| OpOrder.Barrier synchronization | **HIGH** | Write acceptance must respect barriers during switchout; tight coupling to global order. |
| ColumnFamilyStore.Owner dependency | **MEDIUM** | Memtable must signal flushes; owner unavoidable, but interface is minimal. |
| Schema-aware comparators | **MEDIUM** | Partitions must use CFS's ClusteringComparator; immutable post-construction. |

---

## Hypotheses & Open Questions

1. **Persistent Memtables + OLAP**: If a memtable is never flushed (persistent memory backend), can it serve both transactional reads (via CFS) and analytical reads (via external Arrow Flight) concurrently? Likely yes, but requires custom iterators for both.

2. **Memtable Replication**: The API has no multi-node coordination. If CQLite were a memtable, write ordering would need to be externalized (consensus, distributed journal). Cassandra's current model assumes single-node memtable ownership.

3. **Dual-Write Feasibility**: `streamFromMemtable()` suggests memtable contents can be extracted as SSTables. Could a memtable simultaneously (a) receive writes and (b) export a read-only snapshot for OLAP? Yes, in principle; TrieMemtable could support this with proper locking.

---

## Files Referenced

- Core interface: `/src/java/org/apache/cassandra/db/memtable/Memtable.java`
- API documentation: `/src/java/org/apache/cassandra/db/memtable/Memtable_API.md`
- Configuration resolver: `/src/java/org/apache/cassandra/schema/MemtableParams.java`
- Base classes: `/src/java/org/apache/cassandra/db/memtable/{AbstractMemtable,AbstractMemtableWithCommitlog,AbstractAllocatorMemtable}.java`
- Implementations: `/src/java/org/apache/cassandra/db/memtable/{SkipListMemtable,TrieMemtable,ShardedSkipListMemtable}.java`
- Flush orchestration: `/src/java/org/apache/cassandra/db/memtable/Flushing.java`
- CFS binding: `/src/java/org/apache/cassandra/db/ColumnFamilyStore.java:286, 403, 523`
