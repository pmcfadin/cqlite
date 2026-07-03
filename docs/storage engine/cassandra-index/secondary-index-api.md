# Apache Cassandra Secondary Index API — Extension Points & Coupling Analysis

## Summary

Cassandra's secondary index API is a pluggable write/flush/read hook system centered on the `Index` interface. Third-party code can register custom indexes via reflection-based instantiation (`CREATE CUSTOM INDEX ... USING 'classname'`). Per-mutation indexer callbacks (`insertRow`, `updateRow`, `removeRow`) fire during memtable writes; SSTable flush observers (`SSTableFlushObserver`) incrementally build index structures alongside data writes. The `Searcher` interface handles query planning and results. **Critical for Q1 (freshness):** Indexers receive memtable mutations in real-time, but only flushed SSTables are visible to reads—no automatic cross-tier visibility. **For Q2 (feasibility):** The index hook is viable for an adjacent engine ingesting writes but couples tightly to write path specifics; no seam yet exists for plugging storage engines.

---

## Key Classes & Interfaces

| Class/Interface | Location | Responsibility |
|---|---|---|
| `Index` | `src/java/org/apache/cassandra/index/Index.java:160` | Plugin interface; abstracts index lifecycle (init, flush, invalidate), write selection (indexerFor), read (searcherFor). Instantiated via reflection: `new MyIndex(ColumnFamilyStore, IndexMetadata)`. |
| `Index.Indexer` | `src/java/org/apache/cassandra/index/Index.java:614` | Per-partition mutation listener. Methods: `begin()`, `partitionDelete(DeletionTime)`, `rangeTombstone(RangeTombstone)`, `insertRow(Row)`, `updateRow(Row, Row)`, `removeRow(Row)`, `finish()`. Single use; scoped to one PartitionUpdate. |
| `Index.Searcher` | `src/java/org/apache/cassandra/index/Index.java:725` | Per-ReadCommand search executor. Single method: `search(ReadExecutionController)` → `UnfilteredPartitionIterator`. |
| `Index.Group` | `src/java/org/apache/cassandra/index/Index.java:762` | Multi-index cohort abstraction; batches indexes that share group logic (e.g., SAI compound indexes). Factory for group-scoped indexers + query plans. |
| `Index.QueryPlan` | `src/java/org/apache/cassandra/index/Index.java:940` | Query plan for a set of compatible indexes. Selectable by cost estimate (`getEstimatedResultRows()`). |
| `SecondaryIndexManager` | `src/java/org/apache/cassandra/index/SecondaryIndexManager.java:1` | Registry & lifecycle controller. Instantiates custom indexes via reflection (line 937-938); orchestrates indexer creation on writes (line 1513-1519, Type.UPDATE), compaction (Type.COMPACTION), cleanup (Type.CLEANUP). |
| `UpdateTransaction` | `src/java/org/apache/cassandra/index/transactions/UpdateTransaction.java:61` | Write-path transaction scoped to one PartitionUpdate. Sequence: `start()` → `onPartitionDeletion` / `onRangeTombstone` / `onInserted` / `onUpdated` → `commit()`. |
| `IndexTransaction.Type` | `src/java/org/apache/cassandra/index/transactions/IndexTransaction.java:50` | Enum: `UPDATE`, `COMPACTION`, `CLEANUP`—signals intent to indexer. |
| `SSTableFlushObserver` | `src/java/org/apache/cassandra/io/sstable/SSTableFlushObserver.java:28` | Memtable→SSTable write observer. Lifecycle: `begin()` → `startPartition(key, pos)` → `staticRow(row)` → `nextUnfilteredCluster(unfiltered)*` → `complete()`. Async index building during flush. |
| `StorageAttachedIndex` | `src/java/org/apache/cassandra/index/sai/StorageAttachedIndex.java:122` | Trunk's flagship index impl. `isSSTableAttached()=true` (line 433); uses `getFlushObserver()` (line 570) for incremental builds. Parallel per-SSTable on-disk structures. |
| `IndexRegistry` | `src/java/org/apache/cassandra/index/IndexRegistry.java:69` | Index collection interface. `registerIndex(index, groupKey, groupSupplier)` called by Index during `register()` callback (double dispatch). |

---

## Extension Points / Pluggability Seams

### 1. Custom Index Registration (via Reflection)
- **Mechanism:** `CREATE CUSTOM INDEX idx ON table(col) USING 'my.pkg.MyCustomIndex'`
- **Discovery:** `SecondaryIndexManager:937-938` instantiates via reflection:
  ```java
  Constructor<? extends Index> ctor = indexClass.getConstructor(ColumnFamilyStore.class, IndexMetadata.class);
  newIndex = ctor.newInstance(baseCfs, indexDef);
  ```
- **Requirement:** Constructor must accept `(ColumnFamilyStore baseCfs, IndexMetadata indexDef)`.
- **Validation Hook:** Optional static methods `validateOptions(Map<String,String>)` or `validateOptions(Map, TableMetadata)` called at `CREATE INDEX` time.
- **Test example:** `test/unit/org/apache/cassandra/index/CustomIndexTest.java:126` uses `org.apache.cassandra.index.internal.CustomCassandraIndex`.

### 2. Memtable Mutation Callbacks (Write-Time Indexing)
- **Trigger:** `SecondaryIndexManager.newUpdateTransaction(PartitionUpdate, WriteContext, nowInSec, Memtable)` (line 1504)
- **Flow:**
  1. For each `Index.Group`, call `group.indexerFor(predicate, key, columns, nowInSec, ctx, Type.UPDATE, memtable)`.
  2. Returns per-partition `Index.Indexer` or null (if index uninterested).
  3. Indexer receives events: `begin()` → `insertRow(row)` / `updateRow(old, new)` / `partitionDelete(dt)` / `rangeTombstone(rt)` → `finish()`.
  4. Transaction wraps indexers, calling each method on each indexer in sequence.
- **Scope:** Single PartitionUpdate (one partition, one write operation).
- **Key insight (Q1):** Indexer sees **live memtable contents** but only at write time; read queries see **only flushed SSTables** unless index maintains separate read cache.

### 3. SSTable Flush Observation (Incremental Index Building)
- **Method:** `Index.getFlushObserver(Descriptor, ILifecycleTransaction)` (line 392).
- **Requirement:** Return non-null `SSTableFlushObserver` to participate; `isSSTableAttached()` must return true for flush-time building.
- **Lifecycle:** Attached to memtable flush or compaction; observes every partition/cell written to SSTable:
  - `begin()` (once per SSTable)
  - `startPartition(key, keyPosition, keyPositionForSASI)` (per partition)
  - `staticRow(row)` (if static columns present)
  - `nextUnfilteredCluster(unfiltered)` (per row/range-tombstone)
  - `complete()` (end of SSTable)
  - `onSSTableWriterSwitched()` / `abort(Throwable)` for error/sharded-flush cleanup.
- **Positioning:** `keyPositionForSASI` is SSTable-format-specific (data or index file position); enables SAI to build parallel on-disk metadata (line 571).
- **Example:** SAI (`StorageAttachedIndex.getFlushObserver()` line 570) builds inverted indexes incrementally during flush.

### 4. Transaction Type Discrimination
- **Type enum:** `IndexTransaction.Type` = {`UPDATE`, `COMPACTION`, `CLEANUP`} (line 50 in IndexTransaction.java).
- **Passed to:** `Index.indexerFor(…, transactionType, …)` (line 592 in Index.java).
- **Use:** Index decides whether to track this event (e.g., skip compaction cleanup if not needed).
- **Query hook:** `Index.Group.handles(IndexTransaction.Type type)` (line 885 in Index.java) — index can veto transaction types.

### 5. Index Building Support (Pluggable Bulk Indexing)
- **Interface:** `Index.IndexBuildingSupport` (line 188).
- **Default impl:** `CollatedViewIndexBuildingSupport` (line 197) uses `ReducingKeyIterator` (collated view of SSTables).
- **Hook:** `Index.getBuildTaskSupport()` returns supplier of builder strategy; `SecondaryIndexManager.buildIndexesBlocking()` groups indexes by supplier and runs one pass per group.
- **Seam:** Override `getBuildTaskSupport()` to provide custom bulk-index logic (e.g., parallelized, distributed, or external indexers).

### 6. Query Planning & Searcher Selection
- **Query time:** RowFilter expressions matched against `Index.supportsExpression(column, operator)` (line 422).
- **Selection:** `SecondaryIndexManager.getBestIndexQueryPlanFor(RowFilter)` ranks by `getEstimatedResultRows()`.
- **Factory:** `Index.searcherFor(ReadCommand)` → `Searcher` instance (per-command, single use).
- **Execution:** `Searcher.search(ReadExecutionController)` → `UnfilteredPartitionIterator`.
- **Post-processing:** Optional `Index.postProcessorFor(RowFilter)` for coordinator-side result filtering (default no-op).

### 7. Lifecycle Callbacks
- **Initialization:** `Index.getInitializationTask()` called at index creation (can build index from existing SSTables).
- **Metadata reload:** `Index.getMetadataReloadTask(IndexMetadata)` on schema changes.
- **Flush:** `Index.getBlockingFlushTask(Memtable)` (line 313) or `getBlockingFlushTask()` (line 328).
- **Invalidation:** `Index.getInvalidateTask()` on index drop (cleanup resources).
- **Truncation:** `Index.getTruncateTask(long truncatedAt)` on table truncate.
- **Pre-join:** `Index.getPreJoinTask(boolean hadBootstrap)` before node joins ring.

---

## Hard Couplings

### 1. **Memtable-Specific Row Event Model**
- Indexer receives `insertRow(row)` and `updateRow(old, new)` only for in-memtable mutations.
- **Coupling:** If row already in SSTable, compaction treats it as `insertRow` (not `updateRow`), so cleanup logic differs.
- **Implication (Q1):** No unified view of delta—separate event streams for memtable vs. on-disk.
- **Location:** `Index.java:635-663` (updateRow/insertRow javadoc), `SecondaryIndexManager.java:1513-1520` (write path dispatches Type.UPDATE).

### 2. **Write Path Integration (No Read-Time Indexing)**
- Indexer bound to `PartitionUpdate` ↔ SSTable lifecycle; no indexer involvement during read path data assembly.
- **Coupling:** Read queries bypass indexer event stream; index must pre-compute or cache results.
- **Implication (Q1):** Adjacent OLAP engine needs separate change-capture stream (e.g., CDC, custom UpdateTransaction listener) to see mutations; index API alone insufficient for real-time query freshness.
- **Location:** `SecondaryIndexManager.java:1504-1526` (write-side only), no read-side indexer callback.

### 3. **Memtable Object Passed to Indexer**
- `Index.indexerFor(…, Memtable memtable)` parameter (line 588-593 in Index.java).
- **Coupling:** Indexer can inspect/react to memtable state (e.g., size, full status) at index creation.
- **Implication:** Index tightly coupled to memtable identity; if memtable implementation changes (CEP-11 pluggable Memtable), indexer logic must adapt.
- **Trunk-only (CEP-11):** Memtable became pluggable API in trunk; Cassandra 5.0.x has fixed memtable impls.

### 4. **SSTable Format Specificity**
- `SSTableFlushObserver.startPartition(…, keyPositionForSASI)` is SSTable-format-aware position (line 44-47).
- **Coupling:** Index must understand Cassandra's SSTable layout (BIG format: Data.db offsets; BTI format: index file offsets).
- **Implication:** Hard to decouple index from SSTable internals; adjacent engine must replicate position logic.
- **Variant:** Descriptor.version signals format (BIG na/nb vs BTI da in Cassandra 5.0; trunk may have more).

### 5. **Descriptor & ILifecycleTransaction in Flush Observer**
- `getFlushObserver(Descriptor, ILifecycleTransaction txn)` (line 392).
- **Coupling:** Observer tied to Cassandra's SSTable lifecycle (component tracking, deletion, ref counting).
- **Implication (Q2):** External engine reading/indexing SSTables must replicate or hook into lifecycle transaction semantics to avoid orphaned components.

### 6. **Write Context & OpOrder.Group**
- `WriteContext ctx` passed to indexer (line 579 in Index.java); wraps `OpOrder.Group` for mutual exclusion.
- **Coupling:** Indexer inherits memtable write ordering guarantees; external indexer can deadlock if bypassing context.
- **Implication (Q2):** Adjacent storage engine cannot easily index in parallel; must respect write-order constraints.
- **Location:** `UpdateTransaction.java:34-39`, `SecondaryIndexManager.java:1118-1119` (WriteContext.createContextForIndexing()).

### 7. **No Seam for Pluggable Storage Formats**
- **Coupling:** Index interface assumes Cassandra's PartitionKey/Row/Cell/Unfiltered model; no abstraction over storage encoding.
- **Implication (Q2):** Cannot swap storage engine to alternative format (e.g., Parquet, Iceberg) without rewriting Index implementations; no SeekableIndexFormat or EngineAdapter abstraction.
- **Comparison:** SSTable format recently became pluggable (`SSTableFormat.java`); no parallel seam for index + storage plugins.

### 8. **Schema Metadata Dependency**
- `Index.validate(PartitionUpdate, ClientState)` receives full update (line 533).
- `Index.dependsOn(ColumnMetadata)` checked on column operations (line 412).
- **Coupling:** Index must validate against Cassandra's CQL type system; no foreign format support.
- **Implication (Q2):** Can't index alternative schemas (SQL, Avro, Protobuf) without mapping layer.

---

## Q1 Relevance: Freshness (Memtable + SSTable Visibility)

### Q1 Statement
> When DataFusion or Trino reads a node through CQLite's Arrow Flight connector, the read sees only flushed SSTables. What must change so a read reflects ALL node-local state—memtable contents PLUS every SSTable?

### Findings

1. **Indexer Sees Memtable, Searcher Doesn't**
   - Indexer receives mutations in-memtable via `insertRow`, `updateRow` (line 642-663 in Index.java).
   - Searcher operates only on flushed data: `Searcher.search()` scans `SSTableReader`s, not memtable (line 738).
   - **Gap:** No automatic bridge from memtable events to search results.

2. **No Built-In Memtable Read Hook**
   - `Searcher.search(ReadExecutionController)` receives OpOrder.Group but no memtable handle.
   - To see memtable, searcher must be passed memtable reference explicitly or maintain side index.
   - **No seam in Index API** for searcher to optionally scan memtable alongside SSTable results.

3. **CDC/Change Capture as Workaround**
   - Cassandra's CDC module captures mutations to log; external readers can replay.
   - Index API alone doesn't provide CDC; would need separate CDC listener + external aggregation.
   - **Not a core index feature.**

4. **Third-Party Index Building Approach (Viable for CQLite)**
   - Implement custom `Index` subclass with:
     - `indexerFor()` creates indexer that caches memtable writes to in-memory structure or external DB (e.g., RocksDB, SQLite).
     - `searcherFor()` creates searcher that merges in-memory cache + SSTable scan results.
     - `getFlushObserver()` removes cache entries post-flush (dedup).
   - **Pros:** Uses existing plugin seam; no core changes needed.
   - **Cons:** Indexer must maintain dual-write invariant (memtable + index cache in sync); flush race windows exist.

5. **Architecture Implication**
   - Current design assumes index is **lossy optimization** (approximate or filtered results).
   - For "all state" visibility, need **comprehensive change stream** (memtable + SSTable) + **index materialization** outside Cassandra.
   - **Trunk seam (CEP-11):** Pluggable memtable API (line 53 in Index.java, `db.memtable.Memtable`) may allow custom memtable with integrated external flush observer, but no standard yet.

---

## Q2 Relevance: Feasibility (Alternative/Adjacent Engine)

### Q2 Statements
> (a) How feasible is CQLite as an alternative/replacement storage engine inside Cassandra?
> (b) How feasible as an adjacent OLAP storage engine running alongside the normal engine?

### Findings

#### (a) **Alternative Storage Engine: VERY DIFFICULT**

**Current Seams (Negative):**
- **No storage engine abstraction.** `Index` interface couples to Row/Cell/Unfiltered model; no generic KeyValue abstraction.
- **Write path tightly coupled.** `UpdateTransaction` directly manipulates partition updates with CQL-specific Row encoding. Would need parallel `WriteTransaction` interface.
- **No ColumnFamilyStore seam.** Memtable/SSTable lifecycle hardcoded in `ColumnFamilyStore`; index only observes, doesn't control.
- **Query engine assumption.** `Searcher` returns `UnfilteredPartitionIterator`; expects Cassandra's clustering model (no range keys, compound keys only).
- **Replication/repair tied to SSTable.** Anti-entropy, read repair, streaming all assume SSTable format; no abstraction for alternative storage.

**Effort:** ~6-12 months to extract storage abstraction (analogous to Kubernetes storage plugins or HDFS pluggable formats). Would require:
- Generic Write/Read context objects (decouple from CQL Row model).
- ColumnFamilyStore → TableEngine interface refactor.
- Replication/repair abstraction.

**Viability:** 2/10 (very hard; CQLite as alternative is exploratory).

---

#### (b) **Adjacent OLAP Engine Alongside Cassandra: FEASIBLE**

**Best-Fit Seams (Positive):**
1. **Custom Index Ingestion**
   - Implement `Index` subclass that fires on writes (via indexerFor + Indexer callbacks).
   - Route mutations to external OLAP engine (CQLite) in real-time or async.
   - **Pros:** Zero core changes; exists today.
   - **Cons:** Indexer tied to write ordering; must buffer/deduplicate; memtable mutations not visible to reads (CQLite server can only read SSTables).
   - **Latency:** Write → Cassandra memtable → Indexer callback → CQLite engine = fast-path, ~ms overhead per mutation.

2. **SSTableFlushObserver for Bulk Sync**
   - `getFlushObserver()` receives every SSTable being written.
   - CQLite can observe flush events, build/import SSTable metadata in parallel.
   - **Pros:** Incremental, avoids full rebuild.
   - **Cons:** Only sees flushed data; memtable invisible (unless custom index also maintains read cache).
   - **Design:** Create custom index that returns both Indexer (memtable cache) + SSTableFlushObserver (persistent sync).

3. **CDC + External Streaming**
   - Use Cassandra's CDC module (separate logging) to stream mutations to external system.
   - CQLite ingests CDC log independently.
   - **Pros:** Decoupled from index interface; works with built-in indexes.
   - **Cons:** CDC adds per-write overhead; requires separate infrastructure; not real-time (log batching).

4. **Direct SSTables Reading**
   - CQLite can directly read Cassandra's SSTables on disk (already implemented).
   - Custom index not required for reads; just needs file-system access.
   - **Pros:** Simple; no integration layer needed.
   - **Cons:** Only sees flushed state; no memtable visibility.

**Best Practice (Q2b):**
- **Real-time freshness:** Custom Index (seam 1) + Index Caching for memtable mutations.
- **Eventual consistency:** SSTable flush observer (seam 2) only.
- **External control:** CDC (seam 3) if decoupling is priority.
- **Read-only:** Direct SSTable access (seam 4) if acceptable.

**Effort Estimate:** 2-4 weeks for Index → CQLite bridge (basic); 4-8 weeks for full dual-write consistency + cache coherency.

**Viability:** 8/10 (well-supported by existing seams; mostly engineering + integration testing).

---

## Trunk vs. Cassandra 5.0 Deltas

### Changes Relevant to Storage/Index Seams

| Feature | Location/Commit | Trunk Only? | Impact |
|---|---|---|---|
| **Pluggable Memtable (CEP-11)** | `db/memtable/Memtable_API.md`, commit `e4e19e33fa` | YES | Index.indexerFor() receives `Memtable` interface (not concrete impl). Enables custom memtable (e.g., RocksDB-backed) to integrate indexing differently. Cassandra 5.0 has fixed HeapMemtable only. |
| **Pluggable SSTable Format (SSTableFormat API)** | `io/sstable/format/SSTableFormat.java`, commit `b7e1e44a90` | YES (partial) | 5.0 has BIG (na/nb); trunk adds pluggable format. Affects `SSTableFlushObserver.keyPositionForSASI` (position meaning changes per format). |
| **Abstract Write Path** | commit `d31ed0f51b` | YES | "Abstract write path for pluggable storage"—suggests trunk explores write-path abstraction. Not evident in current Index.java. Likely future CQL/storage separation. |
| **Pluggable Index Building** | commit `440366edd0` | Partial | `Index.IndexBuildingSupport` (line 188) exists in 5.0; trunk may refine. Allows custom bulk-indexing strategy. |
| **Accord Transactions** | CEP-20 (not indexed in Index.java) | YES | Trunk integrates distributed transactions (Accord consensus). May affect write ordering guarantees passed to indexers (OpOrder.Group semantics could change). Custom indexes must adapt to new consistency model. |

### 5.0-Compatible Index Development
- Cassandra 5.0 indexers should avoid Memtable API assumptions (treat as opaque); use reflection or interface-based detection.
- SSTable position handling: assume BIG format in 5.0 (no pluggable SSTableFormat yet).
- Write ordering: rely on OpOrder.Group invariants (likely stable trunk→5.0).

---

## Dead Code / Non-Seams Observed

- `Index.internal` package (legacy internal indexes) — superseded by SAI; not a modern seam.
- Virtual indexes (`index/virtual/`) — restricted to system tables; not relevant for OLAP.
- Accord-specific index hooks (consensus-aware indexing) — new in trunk; experimental.

---

## Summary: Is the Index API a Viable Hook for CQLite?

| Question | Answer | Confidence |
|---|---|---|
| Can CQLite observe Cassandra mutations in real-time? | **Yes** (custom Index indexer). | HIGH |
| Can CQLite see memtable + SSTable state in single query? | **No (not without dual caching)**. Index sees memtable; searches see only SSTable. Requires external index to cache memtable writes. | HIGH |
| Can CQLite replace Cassandra's storage engine? | **No (not feasible)**. Would require 6-12 months of storage abstraction work. | HIGH |
| Can CQLite run as adjacent OLAP engine? | **Yes** (4-8 weeks of engineering). Use custom Index for write ingestion + SSTableFlushObserver for batch sync. | HIGH |
| Are there new seams in trunk? | **Yes.** CEP-11 (Memtable API), pluggable SSTableFormat, abstract write path exploratory. 5.0 has fewer hooks. | MEDIUM |

**Recommendation:** Pursue CQLite as **adjacent OLAP engine** via custom Index + flush observer. For full memtable visibility, implement index-side cache (essentially a secondary memtable). Monitor trunk for write-path abstraction (may yield tighter coupling opportunities).

