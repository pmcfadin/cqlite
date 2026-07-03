# Materialized Views Subsystem Index

## Summary
Materialized Views (MVs) are Cassandra's primary in-tree precedent for maintaining a node-local derived dataset synchronized with a base table via the write path. MVs demonstrate read-before-write materialization (reading memtable+SSTables during writes to compute deltas) and per-partition serialization via heavyweight locks. The MV subsystem is tightly coupled to Cassandra's storage engine: it assumes a standard memtable, depends on SSTable format knowledge for backfill, and hardcodes its synchronization protocol in `Keyspace.applyInternal`. Any alternative storage engine or OLAP materializer must replicate this locking model and read-before-write pattern or deadlock/diverge.

## Key Classes & Responsibilities

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| **ViewManager** | src/java/org/apache/cassandra/db/view/ViewManager.java:65 | Single coordinator per Keyspace; manages all views for a table; owns global partition-lock pool via `Striped<Lock>` (line 69, 1024 * concurrent_view_writers slots) |
| **View** | src/java/org/apache/cassandra/db/view/View.java:56 | Individual MV definition; owns ViewBuilder for backfill; wraps schema metadata + SelectStatement |
| **ViewUpdateGenerator** | src/java/org/apache/cassandra/db/view/ViewUpdateGenerator.java:55 | Stateful delta-computer; given incoming update + existing row state, computes view mutations per partition (one generator per view per partition) |
| **TableViews** | src/java/org/apache/cassandra/db/view/TableViews.java:82 | View collection per base table; **core read-before-write orchestrator** (line 173 `pushViewReplicaUpdates`) — reads existing rows via `SinglePartitionReadCommand.executeLocally()` (line 192), passes to `generateViewUpdates()` (line 195) |
| **ViewBuilder** | src/java/org/apache/cassandra/db/view/ViewBuilder.java:62 | Initial backfill engine; parallelizes SSTable scans into `NUM_TASKS` (line 66 = 4*cores) `ViewBuilderTask`s executed by CompactionManager |
| **ViewBuilderTask** | src/java/org/apache/cassandra/db/view/ViewBuilderTask.java | Parallel worker scanning a token-range slice of the base table's SSTables |

## Integration Points (Write Path Coupling)

### Lock Acquisition (Keyspace.java:479)
```java
lock = ViewManager.acquireLockFor(lockKey);  // lockKey = hash(partition_key, table_id)
```
- **Striped<Lock>** pool: 1024 concurrent locks per keyspace (line 69)
- **Non-blocking tryLock** (line 231-236): returns null on contention → triggers deferred/dropped/retry logic
- **Scope**: Per-partition-per-table; acquired BEFORE any write handler invocation
- **Deadlock hazard**: If lock held during compute-intensive work (e.g., a complex MV filter), writer threads starve

### Read-Before-Write Invocation (Keyspace.java:568)
```java
viewManager.forTable(upd.metadata()).pushViewReplicaUpdates(upd, makeDurable, baseComplete);
```
- **When**: After lock acquisition (line 479), before CFS write handler (line 579)
- **What it does** (TableViews.java:173-201):
  - Line 184: `readExistingRowsCommand()` builds a `SinglePartitionReadCommand` for affected rows
  - Lines 191-192: Executes read locally via `executeLocally()` → reads memtable + SSTables in age order
  - Line 195: `generateViewUpdates()` merges existing + incoming row state, calls ViewUpdateGenerator per view
  - Line 200: `StorageProxy.mutateMV()` sends view mutations to replicas (async, batchlogged)
- **Error handling** (lines 570-576): Propagates MV exceptions → aborts entire mutation (atomicity)

### Lock Release (Keyspace.java:489, 505)
- Locks released in reverse acquisition order if lock fails to acquire or mutation deferred
- Retry-on-same-thread if non-deferrable (line 516-529)

## Hard Couplings (Assumptions That Break If Engine Changed)

| Coupling | Rationale | Impact on Alt Engine |
|----------|-----------|---------------------|
| **Hardcoded lock in Keyspace.applyInternal** | Synchronizes MV updates with base writes; prevents concurrent conflicting updates | Alt engine must replicate same per-partition serialization or deadlock (MV reader races with incompatible writer state) |
| **Memtable assumption in read-before-write** | `command.executeLocally()` (TableViews.java:192) reads memtable + SSTables; assumes single coherent memtable | If alt engine has different memtable structure (e.g., pluggable CEP-11 implementation), MV read must adapt query logic; if no memtable (e.g., pure log-based), read-before-write fails |
| **SSTable format knowledge in backfill** | ViewBuilder scans base SSTables directly (line 80+), assumes BigFormat/BtiFormat readers available | Backfill cannot initialize MV from custom storage formats without custom scanner |
| **Replication via StorageProxy.mutateMV** | MV updates sent as separate mutations to view replicas; assumes CassandraKeyspaceWriteHandler path | Custom write handler must invoke MV updates at same point in pipeline or MVs miss writes |
| **Single-partition-at-a-time semantics** | Each lock protects one partition of one base table; no cross-partition atomicity | Bulk/range-scoped writes may serialize poorly; sharded engines may deadlock if shard-lock policy differs |
| **Explicit error propagation** | MV mutation failures abort base write (line 574 `throw t`) | Custom write handler that silently fails on MV update diverges diverges from base silently |

## Extension Points & Seams

| Seam | Location | Pluggability |
|------|----------|--------------|
| **KeyspaceWriteHandler** | src/java/org/apache/cassandra/db/KeyspaceWriteHandler.java:23 (interface) | Interface defined; only one impl (CassandraKeyspaceWriteHandler) in codebase. MV locking+updates hardcoded in Keyspace.applyInternal, not delegated to handler. **No seam for swapping write path.** |
| **ViewManager.updatesAffectView** | ViewManager.java:82 | Filters which mutations trigger MV processing; uses `SelectStatement.selectsKey()`. Could theoretically override, but hardcoded in Keyspace. |
| **CEP-11 Memtable API** | src/java/org/apache/cassandra/db/memtable/Memtable_API.md (TRUNK ONLY) | Pluggable memtable implementations (SkipList, Trie) discoverable at runtime. MVs do NOT use pluggable API; assume vanilla memtable. **Missed seam**: MV read-before-write could be adapted to pluggable memtable if Memtable interface exposed read-existing-for-key, but it doesn't. |
| **SSTableFormat API** | src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java (TRUNK & 5.0) | BigFormat, BtiFormat discoverable at schema level. ViewBuilder.java doesn't parameterize format; assumes standard scan path. **Weak seam**: format chosen at table create time; no runtime override for alt formats. |

## Q1 & Q2 Relevance

### Q1: Analytical Read Freshness
**Problem**: DataFusion/Trino over CQLite Arrow Flight connector sees only flushed SSTables, not memtable.

**MV Lesson**: MVs solve this for their own case by reading-before-write (TableViews.java:192 `executeLocally()` → includes memtable). An OLAP read connector could emulate this:
- Add a pre-read step before returning query results: fetch existing memtable+SSTable state for the queried partition key
- Merge memtable + SSTable state into result set (similar to ViewUpdateGenerator logic)
- Caveat: memtable lacks indexing; pre-read is O(memtable size) per query partition

**Trunk Delta**: CEP-11 pluggable memtable could, in principle, expose read_existing_rows(key, columns) interface, making pre-read faster. Currently unavailable.

### Q2: CQLite as Alt Engine (Feasibility)

#### In-JVM Alternative (Option A: Replace Storage Engine)
**Blocker**: The MV lock/update protocol is hardcoded in Keyspace.applyInternal (line 479 `ViewManager.acquireLockFor`). Replacing the storage engine requires:
1. Retaining `ViewManager` and per-partition lock discipline
2. Implementing a compatible memtable (or wrapping CEP-11 Memtable impl) to feed read-before-write
3. Implementing SSTable backfill via ViewBuilder (format adapter required)
4. Replicate error atomicity: if MV update fails, base write fails

**Feasibility**: Moderate-hard. CQLite would need Java bindings + embedded in-JVM operation, plus a synthetic memtable wrapper.

#### Adjacent OLAP Engine (Option B: Parallel Materializer)
**MV as Template**: MVs show the pattern: hook at write-time via hardcoded integration point (line 568), read-before-write (line 192), compute deltas (ViewUpdateGenerator), replicate mutations (line 200). 

**For CQLite Materializer**:
- Cannot hook into `Keyspace.applyInternal` without forking Cassandra
- Could tail CDC (separate channel) if available in version; MVs don't use CDC
- Could poll memtable+SSTable via remote query (push against performance)
- Cannot guarantee per-partition atomicity without custom locking

**Feasibility**: Hard. Requires CDC or polling; loses the atomic, single-read-per-write guarantee that MVs have.

**Trunk vs 5.0**:
- CEP-11 pluggable Memtable (TRUNK ONLY): Could expose memtable read hook; not available in 5.0
- Both versions: ViewManager, ViewBuilder stable; no version guards observed
- 5.0: Less flexible memtable API; adjacent materializer must poll/CDC

## Trunk vs 5.0 Deltas

| Item | 5.0 | Trunk | Impact |
|------|-----|-------|--------|
| **CEP-11 Pluggable Memtable** | ✗ | ✓ | MVs assume vanilla memtable on both; alt engine has more hooks on trunk but MVs don't use them |
| **SSTableFormat API** | ✓ (BigFormat, BtiFormat) | ✓ (+ Hybrid) | Backfill format-agnostic in both; hybrid format on trunk only |
| **KeyspaceWriteHandler** | ✓ | ✓ | Identical; no new seams trunk-side |
| **ViewManager.LOCKS** | ✓ | ✓ | Identical striped-lock pool |
| **read-before-write (executeLocally)** | ✓ | ✓ | No deltas; both read memtable + SSTables |

**Conclusion**: Trunk offers no new MV-aware hooks. CEP-11 memtable API on trunk could theoretically help an adjacent OLAP engine, but would require custom Memtable impl; MVs don't leverage it. Both versions equally coupled.

## Hypotheses

1. **MV locking is the "worst case" for alt engines**: Per-partition serialization is necessary for correctness but degrades to single-threaded under high write concurrency on overlapping partitions. An alt engine with finer-grained (per-cell) or coarser-grained (per-table) locks would deadlock.

2. **Read-before-write is invisible to CDC**: CDC changes (in separate tables) are generated post-mutation; MV generation is pre-write. If an alt engine tails CDC instead of hooking write path, it loses the existing-state read needed to compute deltas (viewpoint divergence).

3. **ViewBuilder backfill is indexing-dependent**: Backfill speed is O(SSTable count * scan cost); BTree range-seeks in Index.db + bloom filters make it tractable. A pure log-based alt engine would need re-indexing for fast backfill.

