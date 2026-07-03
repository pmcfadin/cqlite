# CFS Flush Lifecycle Index

## Summary

The flush lifecycle in Cassandra orchestrates transitions from in-memory memtables to durable SSTables. **The critical freshness blocker (Q1)**: a node's analytical read via Arrow Flight sees only flushed SSTables, not the live memtable or memtables pending flush. The View abstraction atomically tracks current + live memtables + flushing memtables + live SSTables + compacting SSTables; **reads iterate ALL memtables before SSTables** (via `getAllMemtables()`), but external connectors (Arrow Flight) have **no access to memtables**—they see only the SSTable snapshot. Trunk (post-CEP-11) has pluggable memtable factories but no seam for exposing memtable contents to external readers. forceFlush blocks until all writes finish and SSTables are live; `switchMemtable` is lock-free (Tracker.apply) + barrier-coordinated. The write-barrier (OpOrder) permits writes started before the barrier to finish in the old memtable; those after go to the new memtable.

## Key Classes / Interfaces

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| **ColumnFamilyStore.Flush** | ColumnFamilyStore.java:1222 | Orchestrates full flush: switches memtable, waits write barrier, marks as flushing, flushes to SSTables, runs post-flush cleanup |
| **ColumnFamilyStore.switchMemtable()** | ColumnFamilyStore.java:1075 | Entry point for memtable switch; enqueues Flush task; returns Future that completes on CL clean |
| **ColumnFamilyStore.forceFlush()** | ColumnFamilyStore.java:1107 | Blocks until unflushed data is flushed; if dirty, calls switchMemtable; else returns completion of in-flight flushes |
| **ColumnFamilyStore.switchMemtableIfCurrent()** | ColumnFamilyStore.java:1056 | Atomically switches only if the memtable is still current (blocks re-flush after discard) |
| **View** | lifecycle/View.java:66 | Immutable snapshot of live memtables, flushing memtables, live SSTables, compacting SSTables, + interval tree for range lookups |
| **View.switchMemtable()** | lifecycle/View.java:340 | Functional transformation: appends newMemtable to liveMemtables (now has 2 live for write coordination) |
| **View.markFlushing()** | lifecycle/View.java:354 | Moves a memtable from liveMemtables → flushingMemtables (atomic, preserves ordering) |
| **View.replaceFlushed()** | lifecycle/View.java:373 | Replaces flushed memtable in flushingMemtables; adds new SSTables to live set; rebuilds interval tree |
| **Tracker** | lifecycle/Tracker.java:91 | Holds the current View (volatile); gate for all View transitions via `apply(Predicate, Function)` + ReentrantLock |
| **Tracker.switchMemtable()** | lifecycle/Tracker.java:416 | Calls View.switchMemtable(), notifies observers of switch, returns old memtable |
| **Tracker.markFlushing()** | lifecycle/Tracker.java:427 | Calls View.markFlushing() atomically |
| **Tracker.replaceFlushed()** | lifecycle/Tracker.java:432 | Calls View.replaceFlushed(), updates size metrics, notifies Added + Discarded observers |
| **Memtable (interface)** | memtable/Memtable.java:60 | Write + read operations (put, rowIterator); lifecycle signals (switchOut, setFlushTransaction) |
| **Memtable.Factory** | memtable/Memtable.java:77 | Pluggable factory (CEP-11): creates memtable instances; declares write-durability features, streaming preferences |
| **Memtable.Owner** | memtable/Memtable.java:160 | Callback interface: memtable → CFS for flush signals, current memtable queries, index coordination |
| **LifecycleTransaction** | lifecycle/LifecycleTransaction.java:86 | ACID guard for SSTable lifecycle (new, update, obsolete); transactional checkpoints on Tracker |
| **Flushing.flushRunnables()** | memtable/Flushing.java:61 | Factory for FlushRunnable(s) per disk boundary; wraps SSTableMultiWriter(s) |
| **Flushing.FlushRunnable** | memtable/Flushing.java:134 | Callable: serializes memtable partitions → SSTable via writer; measures throughput + updates metrics |

## Extension Points / Pluggability Seams

1. **Memtable.Factory (CEP-11)** (memtable/Memtable.java:77)  
   - Pluggable memtable implementations via `MemtableParams` + static `FACTORY` or `factory(Map)` method  
   - Methods: `writesShouldSkipCommitLog()`, `writesAreDurable()`, `streamToMemtable()`, `streamFromMemtable()`  
   - **Seam**: Factory can declare alternate write-durability (e.g., persistent memory). Custom impl owns `put()`, `rowIterator()`, `getPartitions()`  
   - **Q2 implication**: Alternative storage engines could implement Memtable to intercept writes before CL/Disk

2. **Memtable.Owner (memtable/Memtable.java:160)**  
   - Memtable → CFS callback for flush signals: `signalFlushRequired(memtable, reason)`  
   - Allows memtable to trigger flushes based on internal thresholds (size, expiry, etc.)  
   - **Seam**: Custom memtables can emit custom FlushReason(s)

3. **Tracker.apply(Predicate, Function)** (lifecycle/Tracker.java:171)  
   - Gate for all View mutations via lock-free atomic check-and-set  
   - Predicate permits or rejects the function based on current view state (e.g., "is this SSTable live?")  
   - **Seam**: External readers could register **read-only** predicates (e.g., "notify if memtables change")—**NOT wired**

4. **SSTableFormat pluggable registry**  
   - DatabaseDescriptor.getSelectedSSTableFormat() (referenced Flushing.java:110)  
   - Allows format plugins to customize writer factories, descriptors, storage layout  
   - **Seam**: Alternative formats could plug into flush pipeline via writerFactory.estimateSize() + createSSTableMultiWriter()

5. **Secondary Index Flush Coordination** (ColumnFamilyStore.java:1361)  
   - `indexManager.flushAllNonCFSBackedIndexesBlocking(memtable)`  
   - CFS-backed indexes flush via the same CFS path (no special seam); custom indexes flush here  
   - **TODO** (ColumnFamilyStore.java:1357): SecondaryIndex lacks `setBarrier()` for exact CL coordination

6. **DiskBoundaries + Multi-disk Flush Sharding** (Flushing.java:71–92)  
   - ColumnFamilyStore.getDiskBoundaries() → splits memtable flush across disks/ranges  
   - Each shard gets its own FlushRunnable + SSTableMultiWriter  
   - **Seam**: Directories.DataDirectory allows storage policy plugins

## Hard Couplings (Assumptions Blocking Alternative Engines / External Readers)

1. **Memtable List is CFS-Private (No External Visibility)**  
   - View.liveMemtables + flushingMemtables are accessed only via Tracker (ColumnFamilyStore.data.getView())  
   - **Arrow Flight connector has zero access to View or memtables**—only SSTable snapshots via `data.liveSSTables()`  
   - **Q1 blocker**: No read-safe memtable export API; adding one requires new seam or Tracker changes

2. **Write Barrier Couples ColumnFamilyStore ↔ Keyspace.writeOrder**  
   - Flush creates `Keyspace.writeOrder.newBarrier()` (ColumnFamilyStore.java:1247)  
   - All writes (CFS + Index + Secondary) must register with global writeOrder  
   - **Q2 implication**: Alternative storage engines must integrate with Keyspace.writeOrder or fork barrier logic

3. **View Transitions Are Atomic + Synchronous**  
   - Tracker.apply() holds ReentrantLock during View mutation (lifecycle/Tracker.java:175–186)  
   - No async or delayed observer notifications—Tracker is the bottleneck  
   - **Impact**: Custom memtable signaling (Owner.signalFlushRequired) blocks until Tracker.apply finishes

4. **CommitLog Position Coupling**  
   - Memtable owns commitLogLowerBound + commitLogUpperBound (references span for recovery)  
   - ColumnFamilyStore.setCommitLogUpperBound() atomically sets the boundary (ColumnFamilyStore.java:1266)  
   - Tracker.replaceFlushed() → CL marked clean only after SSTable → LifecycleTransaction commit  
   - **Q2**: Alternative storage without CL (e.g., persistent memtable) must still participate in this handshake or disable CL replay

5. **Memtable is Immutable After switchOut()**  
   - No concurrent reads + writes after switchOut() is called (ColumnFamilyStore.java:1260)  
   - The old memtable becomes read-only; new memtable is wired for all future writes  
   - **Q2 implication**: Secondary storage engines must enforce the same barrier; no lazy migration

6. **SSTableIntervalTree is Mandatory for Range Queries**  
   - View.replaceFlushed() rebuilds SSTableIntervalTree on every flush (lifecycle/View.java:388)  
   - Tracked via LatencyMetrics; no opt-out for alternative data structures  
   - **Q2**: Replacement engines must also build/maintain an interval tree or fork View entirely

7. **Observers (INotificationConsumer) Are CopyOnWriteArrayList**  
   - Tracker subscribers notified synchronously after View.apply() (lifecycle/Tracker.java:95)  
   - Secondary indexes, caches, custom listeners all block the flush thread  
   - **Impact**: Custom observers (e.g., external analytics system) would block flushes if not async

## Q1 Relevance: Freshness / Analytical Read Visibility

**The Problem**: When DataFusion queries a node via CQLite's Arrow Flight connector, it sees only live SSTables (Tracker.getView().liveSSTables()). Uncommitted memtables + pending-flush memtables are invisible.

**Evidence**:
- View.liveSSTables() returns only sstables set (lifecycle/View.java:122)
- Read path calls `Tracker.getView()` → iterates SSTables + memtables
  - CFS reads: `ColumnFamilyStore.getViewLock()` + `data.getView()` (implicit from ColumnFamilyStore.java:2105)
  - Arrow Flight: No seam to access Tracker.getView().getAllMemtables()
- switchMemtable puts FlushRunnable on executor; Future doesn't complete until SSTable is live + CL marked clean

**Gaps**:
1. No "read snapshot" API exposing flushing memtables + live SSTables atomically
2. No memtable export/serialization seam (Memtable interface has no toSSTable() or toArrow())
3. No barrier coordination for external readers (Tracker.apply only allows CFS → CFS mutations)

**To Fix (Hypothetical)**:
- Add Tracker.getMemtables() + getReadSnapshot() returning (liveMemtables + flushingMemtables + liveSSTables)
- Add Memtable.export(Consumer<UnfilteredRowIterator>) for Arrow Flight to iterate without materializing
- Re-coordinate Tracker.apply to notify external consumers of fresh snapshots (new seam at Tracker.java:189)

## Q2 Relevance: Storage Engine Feasibility

### (a) Alternative Engine Inside Cassandra

**Hard Barriers**:
1. **Memtable.Factory must implement put(PartitionUpdate) + rowIterator(PartitionKey)** → couples to CFS write path  
2. **CommitLog handshake is non-optional**: setFlushTransaction() → commitLogLowerBound/Upper → CL.markClean()  
   - If alt storage claims `writesShouldSkipCommitLog()=true`, CL still replays on restart (unless writesAreDurable=true too)
3. **Write barrier (OpOrder) is global**: Keyspace.writeOrder.newBarrier() must work; no per-table barriers
4. **Flush produces SSTables**: ColumnFamilyStore.forceFlush() → Flushing.flushRunnables() → SSTableMultiWriter  
   - Alt storage engine would have to override flushMemtable() (ColumnFamilyStore.java:1323) to emit custom format
5. **Compaction is SSTable-only**: CompactionManager works on sstables; no memtable-compaction seam

**Pluggable Seams**:
- ✓ Memtable.Factory fully pluggable (CEP-11)  
- ✓ SSTableFormat pluggable for write/read format  
- ✓ DiskBoundaries allows placement policy  
- ? Flush coordinator (ColumnFamilyStore.Flush) is final; would need subclass or fork  
- ✗ OpOrder/Barrier is global; no seam to use alternate coordination

**Verdict**: **Possible but not clean**. An alternative engine could implement Memtable.Factory + intercept put()/rowIterator(), then in forceFlush() override or inject custom writer. But CL handshake, barrier coupling, and SSTable-only compaction mean the engine is still tethered to Cassandra's full lifecycle. Feasibility: **Medium** (3–6 months to prototype).

### (b) External OLAP Engine Alongside Cassandra

**What Exists Now** (CQLite Arrow Flight):
- Reads live SSTables directly from disk (no CFS coupling)
- **Problem**: Misses uncommitted memtables + pending-flush data

**Seams Available**:
1. ✓ INotificationConsumer (Tracker subscribers): Could listen to MemtableSwitchedNotification + SSTableAddedNotification
   - But notifications only fire AFTER View.apply() (post-hoc), and no memtable data is sent
2. ✓ SSTableIntervalTree: Can query live set by range  
3. ✗ No memtable export API: Would need to read MemorySegment directly (unsafe) or wait for SSTable flush

**Gaps**:
- No way to subscribe to "memtable contents changed" with data  
- No backward replay from CL for uncommitted data  
- No point-in-time snapshot API (View is always live)

**Mitigations** (without Cassandra changes):
- Poll Tracker.getView().getAllMemtables() + serialize via rowIterator(PartitionKey) per external request (expensive)
- Replay CL segments for the table (complex, unsafe without CL format guarantee)
- Accept "stale snapshot" for reads until next flush (viable for BI, not streaming)

**Verdict**: **Viable as external read-only replica**. CQLite could listen to MemtableSwitch + SSTableAdded notifications, then periodically poll + export memtables for recent data. Feasibility: **High** (1–2 months, if Tracker.getView().getAllMemtables() is exported; otherwise requires internal CFS reading).

## Trunk vs. 5.0 Notes

| Feature | 5.0 | Trunk | Impact |
|---------|-----|-------|--------|
| **Memtable.Factory (CEP-11)** | Monolithic SkipListMemtable | Pluggable via MemtableParams + reflection | Q2: Alt storage now possible (was hard-coded) |
| **SSTableFormat pluggable** | BIG (na/nb) only | Pluggable (BIG + BTI + custom) | Q2: Alternate formats can be storage engines |
| **Memtable.Owner callback** | Implicit (signals via exceptions) | Explicit interface (signalFlushRequired) | Q1: Could notify external readers (not wired) |
| **OpOrder.Barrier** | Keyspace-scoped | Still Keyspace-scoped (no change) | Q2: Still global; no per-table alternative |
| **View / Tracker lock** | ReentrantLock (Trunk behavior same) | ReentrantLock (unchanged) | Q2: Bottleneck still present |
| **Memtable_API.md docs** | N/A | Present (src/java/org/apache/cassandra/db/memtable/Memtable_API.md) | **Source of truth for CEP-11 extensibility** |

**Trunk Advantage for Q2**: CEP-11 + pluggable memtable factories opened the door for alternative storage inside Cassandra. Cassandra 5.0 had monolithic in-memory memtables; alternative engines required forking the entire CFS class.

## Hypothetical: External Arrow Flight + Memtable Visibility

To enable "all node-local state" (Q1 freshness), Cassandra would need one or more of:
1. **Export seam on Memtable** (minimal): `memtable.exportRows(PartitionKey, Consumer<UnfilteredRowIterator>)` → Arrow Flight reads it directly  
2. **Tracker.getReadSnapshot()** (better): returns frozen (liveMemtables + flushingMemtables + liveSSTables) with consistent timestamps  
3. **INotificationConsumer for memtable writes** (advanced): Fire UpdateNotification during put() → external indexers get real-time updates  
4. **Point-in-time read API** (hard): "Read the state as of commit position X" → replay CL or hold memtable versions

**Rank by Feasibility**: #1 < #2 < #4 < #3 (intrusive).

---

**Investigation Date**: 2026-07-03 | **Cassandra Version**: Trunk (7.0-dev, post-CEP-11) | **Indexer**: cfs-flush-lifecycle
