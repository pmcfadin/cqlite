# Apache Cassandra Read Path Index

## Summary

Cassandra's read path merges memtable(s) and SSTables in a single iterator flow starting from `ReadCommand.executeLocally()`. The merge happens at the partition level (single-partition reads via `SinglePartitionReadCommand`) and the partition-iterator level (range reads via `PartitionRangeReadCommand`). Key: all memtable data and qualifying SSTables are collected into iterators, then merged via `UnfilteredRowIterators.merge()` / `UnfilteredPartitionIterators.mergeLazily()`. Row-level filters, limits, repaired-data tracking, and index usage are applied *after* the merge. Memtable access is mediated by the pluggable CEP-11 Memtable API on trunk; SSTable access routes through `StorageHook` (pluggability seam). The boundary between coordinator (StorageProxy) and local storage (ReadCommand) is clean: StorageProxy calls `command.executeLocally(controller)`, which returns an iterator ready for serialization.

## Key Classes / Interfaces

| Class/Interface | File:Line | Responsibility |
|---|---|---|
| `ReadCommand` | `db/ReadCommand.java:127` | Abstract base for all read commands; orchestrates execution flow, applies filters/limits/repaired-data logic post-merge |
| `SinglePartitionReadCommand` | `db/SinglePartitionReadCommand.java:98` | Reads a single partition; merges memtable+SSTable iterators at row level via `UnfilteredRowIterators.merge()` |
| `PartitionRangeReadCommand` | `db/PartitionRangeReadCommand.java:66` | Reads a partition range; merges memtable+SSTable iterators at partition level via `UnfilteredPartitionIterators.mergeLazily()` |
| `ReadExecutionController` | `db/ReadExecutionController.java:33` | Resource holder for op-order, write-context, index-read controller; tracks repaired-data info and oldest-unrepaired-tombstone min |
| `InputCollector<T>` | `db/ReadCommand.java:1174` | Collects and separates memtable+SSTable iterators by repaired status; orchestrates merge and repaired-data wrapping |
| `UnfilteredRowIterators` | `db/rows/UnfilteredRowIterators.java:46` | Static factory; core merge via `UnfilteredRowMergeIterator.create()` at row level |
| `UnfilteredPartitionIterators` | `db/partitions/UnfilteredPartitionIterators.java` | Static factory; lazy merge via `mergeLazily()` at partition level (range reads) |
| `StorageHook` | `db/StorageHook.java:33` | Pluggability interface for row-iterator creation; configurable via `STORAGE_HOOK` property |
| `StorageProxy` | `service/StorageProxy.java:244` | Coordinator; invokes `command.executeLocally(controller)` → processes response at line 2753 |
| `Memtable` (CEP-11) | `db/memtable/Memtable.java` | Abstract pluggable memtable; provides `rowIterator()` / `partitionIterator()` methods (trunk only) |

## Extension Points / Pluggability Seams

1. **StorageHook** (db/StorageHook.java:33–91)
   - Pluggable interface configured via `cassandra.yml` property `storage_hook` or `STORAGE_HOOK` env var
   - Methods: `makeRowIteratorWithLowerBound()`, `makeRowIterator()`, `reportRead()`, `reportWrite()`
   - Default: direct SSTable access via `SSTableReader.rowIterator()`, `UnfilteredRowIteratorWithLowerBound` construction
   - **Seam use-case**: Alternative storage engines (e.g., CQLite via Arrow Flight) can intercept row-iterator creation

2. **Index.QueryPlan / Index.Searcher** (index/Index.java:160+)
   - Query-plan interface allows index-based read optimization; `searcher.search(executionController)` replaces `queryStorage()` call (ReadCommand.java:534)
   - Extensions: custom indexes implement `Index.Searcher` to return filtered iterator
   - **Q1 relevance**: Index filtering happens post-merge; cannot help with memtable-freshness visibility

3. **Memtable pluggability** (CEP-11, trunk only)
   - `db/memtable/Memtable.java` (abstract) — `rowIterator()`, `partitionIterator()` are called per-memtable (lines 789, 413)
   - Cassandra 5.0: single in-heap memtable; trunk: pluggable (e.g., offheap, custom structures)
   - **Q2 relevance**: Alternative engines can provide custom memtable impls, but must satisfy `Memtable` interface contract

4. **RowFilter application** (ReadCommand.java:556)
   - Applied *after* merge on full iterator; custom row-filter transformations via `RowFilter.filter()`
   - **Q1/Q2 coupling**: Filters run post-merge, cannot push down to memtable level in current design

5. **RepairedDataInfo wrapping** (ReadCommand.java:1240, 1136–1140)
   - Merged iterators wrapped in `RepairedDataInfo.withRepairedDataInfo()` to produce digests
   - Repaired/unrepaired iterators separately merged then concatenated (InputCollector lines 1235–1243)
   - **Q1 relevance**: Repaired digest generation happens on merged result; does not reflect memtable state

## Hard Couplings

1. **Memtable must implement Memtable interface (trunk) / single in-heap impl (5.0)**
   - Lines 787–805 (SinglePartitionReadCommand), 411–416 (PartitionRangeReadCommand): hard-coded loop over `view.memtables`
   - Returns `UnfilteredRowIterator` / `UnfilteredPartitionIterator` only; no pluggable memtable-source interface
   - **Impact**: Alternative engines must either:
     - Implement `Memtable` interface (trunk), or
     - Provide fake memtables that wrap external data (5.0)

2. **SSTable access hard-wired to SSTableReader**
   - Lines 826–896, 419–434: loop over `view.sstables` (SSTableReader collection)
   - Calls `sstable.rowIterator()`, `sstable.partitionIterator()` directly; no abstraction layer
   - StorageHook routes only row-iterator *creation*, not SSTable selection/filtering
   - **Impact**: Alternative engines cannot replace SSTable-set discovery; must provide mock SSTableReader objects or extend SSTableReader

3. **Iterator merge operators are final**
   - `UnfilteredRowIterators.merge()` → `UnfilteredRowMergeIterator.create()` (rows), `UnfilteredPartitionIterators.mergeLazily()` (partitions)
   - No callback/listener during merge (except `MergeListener.NOOP`)
   - Merge order: memtable iterators (newest first, per-memtable order), then SSTable iterators (by maxTimestamp descending, line 778)
   - **Impact**: Cannot intercept or modify merge algorithm; timestamp-based ordering is baked in

4. **View snapshot taken at query start**
   - Line 513 (SinglePartitionReadCommand), line 403 (PartitionRangeReadCommand): `cfs.select(View.selectLive())` captures memtables + SSTables at *query start*
   - Mutations / flushes after view capture are not visible to this query
   - **Q1 critical**: Read sees only state at snapshot time; concurrent writes (in memtable) are invisible
   - **Impact**: Alternative engine must shadow memtable-write stream in parallel to see new writes mid-query

5. **ColumnFamilyStore.ViewFragment structure**
   - Type: holds `List<Memtable> memtables` and `List<SSTableReader> sstables`
   - Provides `select(keyRange)` filtering by partition range (PartitionRangeReadCommand line 403)
   - No extension point; all iteration happens at ReadCommand level
   - **Impact**: Cannot customize which memtables/SSTables are included without replacing entire CFS.select() method

6. **Timestamp is sole ordering signal for repaired data**
   - Line 836–837 (partition tombstone elimination), line 833–864 (SSTable visitation): maxTimestamp comparison gates SSTable inclusion
   - Repaired vs unrepaired distinction is binary; no gradations
   - **Impact**: Cannot implement hybrid freshness (e.g., "include recent unrepaired + older repaired"); all-or-nothing per timestamp

7. **Filter / Limit application order is fixed**
   - Line 548 (RowFilter), lines 562–575 (DataLimits): applied *always after merge* in ReadCommand.executeLocally()
   - Cannot be pushed earlier (e.g., at memtable read time)
   - **Q1 consequence**: Cannot limit query early to only memtable or recent SSTables; full merge always happens first

## Q1 Relevance: Memtable Freshness in Arrow Flight / Trino Analytics Reads

**Problem**: When DataFusion / Trino reads a node via CQLite's Arrow Flight connector, the read sees only flushed SSTables. Memtable contents are invisible.

**Cassandra Root Cause**:
- Read path takes a `View` snapshot at query start (line 513, 403) that includes only memtables + SSTables visible at that moment
- Memtable is designed for durability (WAL + flush), not for external read access; no async/streaming API
- StorageProxy and ReadCommand assume single-threaded, immediate-response query model (no streaming analytics workload)
- No hook to stream memtable writes to external readers; index listener (Indexer interface) only fires on flush/compaction, not on write

**Cassandra Lacks** (Trunk gaps):
1. **Async memtable-read API**: `rowIterator()` is blocking; no cursor to fetch rows on-demand for external readers
2. **Write-time listener**: Write events (cell, row, partition) not exposed to external readers until flush
3. **Pluggable memtable storage**: CEP-11 allows custom Memtable impls, but all derive from abstract `Memtable` (must fit in-process memory)
4. **Coordinator-aware streaming**: StorageProxy does not coordinate memtable visibility with remote readers; flush is the only seam

**Needed for Freshness**:
- Add a pluggable **WriteListener** (or extend Indexer) that fires on every write, allowing external readers to subscribe to fresh data
- Or: make Memtable support external **cursor APIs** (e.g., arrow's Flight do_get with server-side offset tracking)
- Or: add a **flush-on-demand** API for analytics queries (e.g., "flush memtable X to SSTable before query starts")

## Q2 Relevance: CQLite as Alternative Engine or OLAP Sidecar

### (a) CQLite as In-JVM Storage Engine

**Feasibility: Low** (5.0) / **Medium** (trunk)

**Where "Storage Engine" Lives**:
- Memtable abstraction (CEP-11 on trunk): `db/memtable/Memtable.java` (pluggable)
- SSTable abstraction (all versions): `io/sstable/format/SSTableReader`, `SSTableWriter` (via SSTableFormat plugin)
- Merge algorithm: `db/rows/UnfilteredRowMergeIterator` (hardcoded)
- Lifecycle: `ColumnFamilyStore` (table-level resource holder; not pluggable)

**Seams Present**:
- Memtable interface (trunk only): `rowIterator()`, `partitionIterator()`
- SSTableFormat plugin system: allows custom SSTable codecs (already used for BIG vs BTI)
- StorageHook: can intercept row-iterator creation, but cannot avoid SSTableReader loop

**Seams Missing**:
- No ColumnFamilyStore abstraction; hardcoded to assume single Keyspace → ColumnFamilyStore mapping
- No merge-algorithm plugin; merge order and strategy fixed
- No lifecycle hook between ColumnFamilyStore and Memtable; cannot customize flush → SSTable pathway
- ReadCommand.queryStorage() is abstract but subclasses (SinglePartition, PartitionRange) are concrete; cannot extend without forking

**Verdict**: Would require:
- Implement `Memtable` interface (trunk) — feasible
- Extend `SSTableReader` / `SSTableFormat` — feasible but invasive
- Fork or wrap `ColumnFamilyStore` — major surgery (state, lifecycle, compaction, repair all coupled)
- Replace merge algorithm — no seam; would need Cassandra patch

### (b) CQLite as OLAP Sidecar (via Arrow Flight + StorageHook)

**Feasibility: High**

**Design Pattern** (already sketched in CLAUDE.md):
1. Implement StorageHook subclass that:
   - Routes `makeRowIterator()` / `makeRowIteratorWithLowerBound()` calls to CQLite Arrow Flight via network RPC
   - Keeps calls to `reportRead()` local (for metrics)
   - Returns CQLite-sourced row iterators in Cassandra's `UnfilteredRowIterator` format
2. Enable via `storage_hook=org.example.CqliteStorageHook` in cassandra.yaml
3. StorageProxy calls `ReadCommand.executeLocally()` → queries CQLite's Flight connector instead of local SSTable

**Gaps to Bridge**:
- CQLite Iterator must conform to `UnfilteredRowIterator` contract (ordering, tombstones, range-markers)
- Memtable shadowing: still see only flushed SSTables unless CQLite's on-disk datasets are kept in sync with Cassandra via compaction export (issue #1406-adjacent)
- Repaired data: CQLite has no repaired/unrepaired distinction; may need dummy wrapper

**Verdict**: Viable, non-forking path; CQLite becomes a smart cache / alternative read backend for analytics workloads.

## Trunk vs 5.0 Deltas

| Aspect | 5.0 | Trunk | Impact to Q1/Q2 |
|---|---|---|---|
| **Memtable pluggability** | Single hardcoded in-heap memtable | CEP-11 pluggable abstract Memtable | Trunk can swap memtable impl; 5.0 requires wrapping |
| **SSTableFormat plugin** | BIG (na/nb) only | BIG + BTI (da) + plugin API | Both allow custom codec; Trunk has more implementations |
| **ReadCommand.queryStorage()** | Abstract, 2 impls | Abstract, 2 impls | No change; both support override |
| **StorageHook** | Not documented; likely present | Documented, CEP-11-aware | Both have pluggable row-iterator creation |
| **WriteListener / Indexer** | On-flush only | On-flush + write (context-dependent) | No memtable-write visibility in either; Trunk slightly closer |
| **Coordinator routing** | Hard-wired to StorageProxy methods | Accord consensus + new read coordinator | Different coordinator, same local-read seams |
| **Compaction API** | STCS/LCS/TWCS | + pluggable CompactionStrategy | Trunk allows custom compaction; impacts WAL + flush pathway |
| **FlushWriter** | Single stream (SSTableWriter) | pluggable per-format | Trunk allows alternative flush targets |

**Biggest Trunk Advantage**: CEP-11 (pluggable Memtable, compaction strategies, flush writers) makes it possible to route writes to alternative backend (e.g., object store, tiered storage). 5.0 requires all mutable data to flow through in-heap memtable.

## Hypotheses (Speculative, Marked)

1. **Q1: Memtable freshness will not be solved by read-path-only changes** — write-time listener or flush-on-demand API is required in Cassandra itself; read-path architecture is sound, but missing upstream visibility hook.

2. **Q2: CQLite-as-in-JVM-engine is lower-ROI than CQLite-as-sidecar** — sidecar (StorageHook + Flight) requires no Cassandra patches and solves OLAP freshness separately from Cassandra's consistency model; in-engine integration would inherit all of Cassandra's architectural constraints (memtable, LSM, compaction) with minimal benefit.

3. **Trunk CEP-11 opens door for tiered storage + analytics fusion** — if CQLite implements pluggable Memtable + custom FlushWriter, Cassandra writes could dual-write to heap (for transactional reads) and CQLite (for analytics), avoiding snapshot staleness entirely. 5.0 cannot easily do this.
