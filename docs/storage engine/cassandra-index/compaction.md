# Cassandra Compaction Subsystem Index

## Summary

Cassandra's compaction subsystem (`src/java/org/apache/cassandra/db/compaction/`) is **fully pluggable at the strategy level** via per-table configuration. Strategies are instantiated via reflection (ColumnFamilyStore.createCompactionStrategyInstance, line 72 of CompactionStrategyHolder.java) from CompactionParams, which parses `CREATE TABLE ... WITH compaction = {'class': '...'}`. SSTables flow through LifecycleTransaction (mutation + commit transactions atomically). **Trunk adds CEP-11 pluggable Memtable API** (parallel to strategies; per-table, configurations in cassandra.yaml). For Q1 (freshness): analytical readers see only flushed SSTables; memtable contents (unflushed writes) are opaque to compaction-aware querying. For Q2 (alternative engine): custom strategies can delegate work, but compaction **deeply couples** to ColumnFamilyStore lifecycle, SSTable I/O, and single-node scheduling.

---

## Key Classes & Interfaces

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| **AbstractCompactionStrategy** | CompactionStrategy.java:67 | Pluggable strategy base; defines task generation (getNextBackgroundTasks, getMaximalTasks, getUserDefinedTask) and SSTable management (addSSTable, removeSSTable, replaceSSTables). **Subclass contract**: implement task methods + SSTable tracking. |
| **CompactionStrategyManager** | CompactionStrategyManager.java:111 | Multiplexer for 4 strategy-holder instances (transientRepairs, pendingRepairs, repaired, unrepaired); routes SSTables by repair status + repair ID. Manages CompactionParams and per-table disk boundaries (DiskBoundaries). |
| **CompactionStrategyHolder** | CompactionStrategyHolder.java:44 | Container for N strategies (one per token partition); instantiates via **ColumnFamilyStore.createCompactionStrategyInstance()** (line 72). Routes SSTables to correct strategy by token range (DestinationRouter). |
| **CompactionManager** | CompactionManager.java:154 | Singleton orchestrator; manages thread pools (executor, validationExecutor, viewBuildExecutor, cacheCleanupExecutor, secondaryIndexExecutor) and pending task queues. Calls CompactionStrategyManager.getNextBackgroundTasks() in a loop. |
| **ColumnFamilyStore** | db/ColumnFamilyStore.java | Per-table data holder; **createCompactionStrategyInstance()** calls reflection to instantiate strategy from CompactionParams.klass() with constructor(ColumnFamilyStore, Map<String,String>). |
| **CompactionParams** | schema/CompactionParams.java | Schema-persisted config; **classFromName()** (line ~200) does FBUtilities.classForName("org.apache.cassandra.db.compaction." + shortName) + subclass check. Validates strategy class and options. |
| **ILifecycleTransaction** | db/lifecycle/ILifecycleTransaction.java:21 | Transactional interface: trackNew(SSTable), obsolete(SSTableReader), update(Collection, boolean original), checkpoint(), commit(). Orchestrates atomic SSTable replacement. |
| **LifecycleTransaction** | db/lifecycle/LifecycleTransaction.java | Implements ILifecycleTransaction; stages mutations invisibly, logs them on checkpoint(). Ensures failed compactions abort without data loss. |
| **AbstractStrategyHolder** | compaction/AbstractStrategyHolder.java:51 | Parent of CompactionStrategyHolder; defines interface for strategy routing (setStrategy(), getBackgroundTaskSuppliers(), replaceSSTables()). **DestinationRouter** (line 75) routes by token range. |
| **UnifiedCompactionStrategy** | UnifiedCompactionStrategy.java:70 | Latest trunk strategy (CEP-26); extends AbstractCompactionStrategy. Organizes SSTables into level hierarchy. Delegates to Controller + ShardManager. |

---

## Extension Points / Pluggability Seams

### 1. **Strategy Class Registration (Q2a)**
- **Entry**: CREATE/ALTER TABLE `WITH compaction = {'class': 'FqnOrShortName', ...}` → TableMetadata → CompactionParams
- **Instantiation**: CompactionParams.classFromName() (line ~200 of schema/CompactionParams.java)
  - Short names get "org.apache.cassandra.db.compaction." prefix auto-added
  - Full FQN allowed (e.g., 'com.example.MyStrategy')
  - Validation: must be subclass of AbstractCompactionStrategy
- **Construction**: ColumnFamilyStore.createCompactionStrategyInstance() (line ~3800+ in ColumnFamilyStore.java)
  - Reflection: `constructor.newInstance(this, options_map)`
  - **Seam**: Strategy subclass must have `public Strategy(ColumnFamilyStore cfs, Map<String,String> options)` constructor
- **Lifetime**: One instance per token partition per repair-status holder (repaired/unrepaired/pendingRepair); reloaded on schema ALTER or disk boundary change

### 2. **Task Generation Contract**
- `AbstractCompactionStrategy.getNextBackgroundTasks(long gcBefore)` — called in loop by CompactionManager
  - **Must** mark selected SSTables as compacting in-place
  - Returns Collection<AbstractCompactionTask>
- `getMaximalTasks(long gcBefore, boolean splitOutput)` — user-requested full compaction
- `getUserDefinedTask(Collection<SSTableReader> sstables, long gcBefore)` — user selects specific SSTables
- **Seam**: Custom strategy defines compaction eligibility, order, concurrency

### 3. **SSTable Lifecycle (Q2b)**
- **Flow**: CompactionTask → CompactionIterator (reads input SSTables) → SSTableMultiWriter (writes output) → LifecycleTransaction.update() → LifecycleTransaction.obsolete()
- **LifecycleTransaction methods**:
  - `trackNew(SSTable)` — new file created, not yet live
  - `obsolete(SSTableReader)` — old file marked for deletion
  - `checkpoint()` — make changes visible to reads
  - `commit()` — finalize, clear transaction
  - `abort()` — rollback (delete new files, unmark old files)
- **Seam**: Custom strategies can wrap LifecycleTransaction to intercept SSTable mutations (e.g., for external coordination)

### 4. **Memtable API (Trunk, CEP-11, Q1 Partial Answer)**
- **Config**: cassandra.yaml `memtable.configurations.<name>: {class_name: ..., parameters: {...}}`
- **Selection**: CREATE/ALTER TABLE `WITH memtable = 'config_name'` → TableMetadata.params.memtable
- **Factory instantiation** (MemtableParams.java): Reflection on `Memtable.Factory factory(Map<String,String>)` method or static `FACTORY` field
- **Pluggability**: Custom Memtable implementations control flush behavior (Memtable.getFlushSet()), durability (writesAreDurable(), writesShouldSkipCommitLog()), and streaming (streamToMemtable(), streamFromMemtable())
- **Limitation for Q1**: Memtable contents **not exposed to compaction strategy** — compaction only sees flushed SSTables. No hook to "include unflushed memtable in read view" from the compaction side.

### 5. **Repair Status Routing**
- **SSTables partitioned by**: repaired status + pending repair ID + directory
- CompactionStrategyManager holds 4 holders: transientRepairs (PendingRepairHolder), pendingRepairs (PendingRepairHolder), repaired (CompactionStrategyHolder), unrepaired (CompactionStrategyHolder)
- **Seam**: Custom strategy sees only SSTables of its repair cohort; cross-repair compaction requires overriding CompactionStrategyManager (invasive)

---

## Hard Couplings (Q2c: Friction for Alternative Engine)

### 1. **ColumnFamilyStore Dependency**
- Every AbstractCompactionStrategy holds reference to ColumnFamilyStore (line 87, AbstractCompactionStrategy.java)
- Strategy constructor signature **must** be `(ColumnFamilyStore, Map<String,String>)`
- CFS provides: Directories, metadata(), getPartitioner(), indexManager, tracker (SSTable visibility)
- **Friction**: CQLite must provide a "fake" CFS or adapt strategy interface to decouple

### 2. **SSTableReader/Writer Formats**
- Compaction reads via SSTableReader (io/sstable/format/SSTableReader.java) — coupled to Cassandra's binary format
- Writes via SSTableMultiWriter + MetadataCollector — Cassandra-specific metadata (repairedAt, pendingRepair, replicatedAt, statsMetadata with minTimestamp, etc.)
- Strategy calls AbstractCompactionStrategy.getCompactionTask(LifecycleTransaction, long gcBefore, long maxSSTableBytes) → CompactionTask
- **Friction**: External compaction (CQLite) must re-read Cassandra's SSTable format or invoke Cassandra's SSTableWriter; no "import pre-compacted SSTable" hook

### 3. **LifecycleTransaction Atomicity**
- Compaction **must** use LifecycleTransaction to mark old SSTables obsolete + new ones live atomically
- Transaction creation: new LifecycleTransaction(cfs.getTracker(), COMPACTION, oldSSTables) — **tied to tracker**
- Tracker notifies listeners (IndexManager, ViewBuilder, etc.) on SSTable changes
- **Friction**: External compaction delegated to CQLite cannot use this transaction system; must call back to Cassandra to mutate SSTable set (latency, complexity)

### 4. **Tracker + Notification System**
- ColumnFamilyStore.getTracker() → ITracker (ViewerAdditionListener, SSTableAddedNotification, SSTableDeletingNotification, SSTableListChangedNotification, etc.)
- Compaction inserts new SSTables into tracker → fires SSTableAddedNotification → triggers secondary index builds, view materialization, cache updates
- Strategy is not notified; all routing is tracker → listener
- **Friction**: CQLite generates SSTables; Cassandra must fetch them and re-integrate via tracker notifications

### 5. **No Compaction Strategy Replacement Hook**
- CompactionStrategyManager.setStrategy() reloads all strategies on schema ALTER
- No hook to **export compaction work** or **import pre-compacted SSTables**
- Compaction tasks are always generated and executed in-process (CompactionExecutor thread pool)
- **Friction**: No native "offload compaction to external engine, poll for results" interface

### 6. **Repair Status Baking into Strategy**
- RepairStatusChanged notification → CompactionStrategyManager routes to different holder
- Pending repair IDs create per-repair strategies (PendingRepairManager.getOrCreate())
- Custom strategy must handle: unrepaired + repaired + transient + pending cohorts separately
- **Friction**: External engine must track repair status, apply cohort isolation, callback to Cassandra to commit transitions

### 7. **CompactionManager Executor Dependency**
- CompactionManager.submitBackground() enqueues tasks to CompactionExecutor thread pool
- No "custom executor" hook; all tasks must be AbstractCompactionTask subclasses runnable in default executor
- **Friction**: CQLite cannot plug a sidecar to run compactions; must wrap tasks or fork CompactionManager

---

## Q1 Relevance: Freshness (Analytical Read Visibility)

**Problem**: DataFusion/Trino read a node via CQLite Arrow Flight connector; sees only flushed SSTables, not memtable writes.

**Current Architecture**:
- Memtable (in-memory, unflushed) — opaque to query engine; only visible through normal Cassandra read path (read repair, digest)
- CompactionStrategyManager operates on SSTableSet (from Tracker); **does not include memtable**
- Arrow Flight reader (CQLite) scans SSTableSet; memtable excluded by design

**Options for Memtable Visibility**:
1. **Force flush before analytical read** — CompactionManager has no "await-all-flushes" hook; would require ColumnFamilyStore.forceBlockingFlush() (complex, disruptive)
2. **Expose Memtable.partitionIterator() to compaction** — CEP-11 Memtable API provides rowIterator(key), partitionIterator(), but compaction strategy has no method to query current memtables (Q1 answer: **gap exists**)
3. **Truck-native memtable compaction** — Add Memtable as a "flush-less" compaction source; would require AbstractCompactionStrategy.getMemtableSnapshot(ColumnFamilyStore) + union with SSTable iterators in CompactionIterator
   - Not implemented in trunk; no CEP filed

**Trunk-vs-5.0**:
- **5.0**: Single memtable type (SkipListMemtable); no pluggability
- **Trunk**: CEP-11 pluggable memtables; still no strategy-facing memtable exposure hook

---

## Q2 Relevance: Feasibility as Alternative/Adjacent Engine

### Q2a: CQLite Inside Cassandra (Custom CompactionStrategy)

**Possible**:
- Write CQLite as AbstractCompactionStrategy subclass with `(ColumnFamilyStore, Map<String,String>)` constructor
- Receive getNextBackgroundTasks() calls; return CompactionTasks that invoke CQLite byte-parity STCS
- Read input SSTables via Cassandra's SSTableReader; write via Cassandra's SSTableMultiWriter (to stay in-tree)
- Commit via LifecycleTransaction (atomic replacement)

**Friction**:
- Must link/embed CQLite Rust bindings (FFI overhead)
- No "skip writing in Cassandra's format" — CQLite byte-parity must output Cassandra SSTable format (defeat some CQLite advantages)
- Performance: Cassandra's Java→Rust→Java transition (JNI latency, GC stalls)
- **Verdict**: Viable for testing/validation (byte-parity proof); not practical for production

### Q2b: CQLite Alongside (Sidecar Compaction Delegator)

**Possible**:
- Custom strategy that accepts task requests, sends to CQLite sidecar, polls for completion
- Sidecar reads SSTables from disk (direct file I/O, no Cassandra libraries); compacts with CQLite; outputs SSTables to staging dir
- Strategy uses LifecycleTransaction to import staged SSTables (call SSTableReader.open(descriptor); tracker.addInitialSSTables())

**Friction**:
- No native hook for "import pre-compacted SSTables"; must use low-level Tracker/View mutations (fragile)
- Repair status, pending repair IDs, and replicatedAt timestamps must be re-applied to output SSTables (CQLite has no notion of these)
- Failure handling: if sidecar crashes mid-compaction, Cassandra's LifecycleTransaction cannot rollback (staged files stranded in disk)
- **Verdict**: Possible but invasive; requires Cassandra patch for first-class sidecar support

### Q2c: Storage Engine Seam (In-JVM Alternative to LeveledCompactionStrategy)

**Possible Swap Points**:
1. **Replace CompactionStrategy only**: CQLite strategy controls compaction logic; still read/write Cassandra SSTable files (binary parity enforced)
   - **Seam**: CompactionParams.classFromName() + strategy plugin
   - **Friction**: No Cassandra 5.0 SSTable format alternative; CQLite must read/write exact Cassandra binary
   
2. **Replace SSTableReader/Writer**: Plug custom Storage (CQLite's internal index structures)
   - **Seam**: io/sstable/format/SSTableFormat.java (defines Components, Reader, Writer factories)
   - **In 5.0**: No SSTableFormat SPI; SSTableFormat.BIG + SSTableFormat.BTI are hard-coded switches
   - **In Trunk**: SSTableFormat may have become a seam (CEP status unknown; assume not fully pluggable)
   - **Friction**: Extreme; breaks read/write paths, metadata, indices, streaming
   
3. **Replace ColumnFamilyStore**: Alternative DataStore class
   - **No seam**: ColumnFamilyStore is hard-coded throughout (recovery, bootstrapping, anti-compaction, streaming, reads, writes)
   - **Friction**: Infeasible without forking entire codebase

**Verdict**: CQLite as CompactionStrategy alone (Q2b, sidecar) is the **realistic path**; in-JVM replacement (Q2c) would require CEP-level work to expose SSTableFormat SPI + storage engine selection hooks (not present in Trunk as of 7.0).

---

## Trunk-vs-5.0 Deltas

| Feature | 5.0 | Trunk | Impact |
|---------|-----|-------|--------|
| **Pluggable Compaction Strategy** | Yes (since 2.x) | Yes, enhanced | Q2a/2b viable in both; no delta |
| **CEP-11 Memtable API** | No (SkipListMemtable only) | Yes (4.1+) | Q1: Trunk allows custom memtables; still no strategy-facing hook to read unflushed data |
| **CEP-26 Unified Strategy** | LeveledCompactionStrategy, SizeTieredCompactionStrategy, TimeWindowCompactionStrategy | UnifiedCompactionStrategy (default) | Compaction logic evolved; strategy interface unchanged; custom strategies still work |
| **SSTableFormat SPI** | BIG/BTI hard-coded in format/ | BIG/BTI hard-coded (assume no SPI) | Q2c: No alternative storage engine seam detected |
| **LifecycleTransaction** | Present, same contract | Same | SSTables replace via transaction; no delta |
| **Repair Status Cohorts** | PendingRepairHolder, transientRepairs | Same | Repair-aware isolation unchanged |

---

## File Anchors & Seams (Quick Ref)

- **Strategy instantiation**: ColumnFamilyStore.java, line ~3800+, method `createCompactionStrategyInstance()`
- **Strategy interface**: AbstractCompactionStrategy.java:67, methods getNextBackgroundTasks(), getMaximalTasks(), getUserDefinedTask(), addSSTable(), removeSSTable(), replaceSSTables()
- **Strategy routing**: CompactionStrategyManager.java:111, fields repaired/unrepaired/pendingRepairs/transientRepairs
- **Task execution**: CompactionManager.java:154, method submitBackground(), executor.submit(task)
- **SSTable replacement**: LifecycleTransaction.java, methods trackNew(), obsolete(), checkpoint(), commit()
- **Memtable config** (Trunk): cassandra.yaml memtable.configurations.*, schema/MemtableParams.java
- **Repair cohort routing**: CompactionStrategyManager.java, method getHolder(repairedAt, pendingRepair, isTransient)

---

## Hypotheses / Open Questions

1. **Memtable exposure to strategies** — CEP-11 does not document a hook for strategies to include unflushed memtable in compaction reads. Could AbstractCompactionStrategy gain a `getMemtableSnapshot(ColumnFamilyStore)` method? (Requires Cassandra change)

2. **Sidecar compaction callback** — No first-class hook for "import pre-compacted SSTables"; would require ColumnFamilyStore.importSSTable(File, metadata) method to wrap tracker mutations (CEP candidate)

3. **Cursor-based compaction** (Trunk feature) — CursorCompactor.java may enable streaming compaction; unclear if it's an extension point or internal-only

