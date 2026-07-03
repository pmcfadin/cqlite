# CEP-11 Pluggable Memtable Mechanics for a "cqlite" Wrapper — Findings

*Research pass for the memtable-plugin design doc (spike #1807). Verified on `origin/cassandra-5.0` @ `464b2e54`, trunk deltas flagged. Produced 2026-07-03.*

All `file:line` anchors are on the **cassandra-5.0 branch** of the local Cassandra clone unless marked **[trunk]**. The canonical upstream plugin doc is `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` (exists on 5.0).

---

## 1. Factory contract

**Interface** — `src/java/org/apache/cassandra/db/memtable/Memtable.java:75-152` (`Memtable.Factory`). The single required method (line 86):

```java
Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Owner owner);
```

Optional defaults on the factory (all default `false`/`null`): `writesShouldSkipCommitLog()` (:95), `writesAreDurable()` (:108), `streamToMemtable()` (:121), `streamFromMemtable()` (:137), `createMemtableMetrics(TableMetadataRef)` (:148, returns `TableMetrics.ReleasableMetric`). CFS caches these at init: `ColumnFamilyStore.java:500` (`memtableFactory = metadata.get().params.memtable.factory()`), `:510` (`memtableMetrics = memtableFactory.createMemtableMetrics(metadata)`), and delegates `writesShouldSkipCommitLog/writesAreDurable/streamTo/FromMemtable` at `:642-657`.

**What our class must expose** (per `Memtable.java:64-74` javadoc and `MemtableParams.getMemtableFactory`, `schema/MemtableParams.java:217-257`): EITHER a static field `public static final Memtable.Factory FACTORY` (no-parameter case) OR a static method:

```java
public static Memtable.Factory factory(Map<String, String> options)
```

Reflection path (`MemtableParams.java:230-249`): `Class.forName(className)` → `clazz.getDeclaredMethod("factory", Map.class)` invoked with a **mutable copy** of `parameters`; on `NoSuchMethodException` falls back to `clazz.getDeclaredField("FACTORY")`. **Parameter validation contract**: the `factory(Map)` method must `map.remove(...)` every option it consumes; if the map is non-empty afterwards → `ConfigurationException("Memtable class ... does not accept any futher parameters, but {…} were given.")` (:246-248). Class-name resolution: names without a `.` are prefixed `org.apache.cassandra.db.memtable.` (:227). Factories should implement `equals`/`hashCode` (used by `AbstractAllocatorMemtable.shouldSwitch(SCHEMA_CHANGE)` to detect memtable-type changes, `AbstractAllocatorMemtable.java:137`; `TrieMemtable.Factory` implements both, `TrieMemtable.java:681-698`).

**Configuration plumbing**: `cassandra.yaml` `memtable.configurations` deserializes to `Config.MemtableOptions` = `LinkedHashMap<String, InheritingClass>` ("order must be preserved", `config/Config.java:192-201`). `InheritingClass` (`config/InheritingClass.java`) = `ParameterizedClass` (`class_name`, `parameters`) + `inherits`; `resolve()` merges parent parameters with child overrides (child wins). `MemtableParams.expandDefinitions` (`MemtableParams.java:139-205`) resolves the inheritance graph (implicit `default` = SkipListMemtable, `:98-100`; self-inheritance and loops → `ConfigurationException`, `:158,182`). `MemtableParams.get(key)` (`:111-120`) lazily instantiates and caches per configuration key (`CONFIGURATIONS.computeIfAbsent` → `parseConfiguration` `:207-214` → `getMemtableFactory`). Instantiation is **lazy**: a bogus yaml configuration does NOT fail node startup; it fails only when first referenced by a table (Memtable_API.md: "the database will only validate the memtable class and its parameters when a configuration needs to be instantiated for a table").

**Missing jar / unknown configuration behavior** — two distinct paths:
- **DDL on the coordinator**: `cql3/statements/schema/TableAttributes.java:125` calls `MemtableParams.get(getString(MEMTABLE))` → throws `ConfigurationException` → the `CREATE/ALTER TABLE` **fails** on that node.
- **Schema arriving at a node that can't instantiate** (jar missing, or config absent from its yaml): `schema/SchemaKeyspace.java:1070` uses `MemtableParams.getWithFallback(...)` (`MemtableParams.java:122-136`) — logs `"Invalid memtable configuration … Falling back to default to avoid schema mismatch"` and **silently uses the default (SkipList) factory** while keeping the configured key in schema. Node starts and serves; it just doesn't run your memtable. Same applies at restart. Memtable_API.md documents this and recommends the two-step "remapped default" rollout.

---

## 2. Wrapping TrieMemtable — wrap vs extend

**TrieMemtable is NOT final**: `public class TrieMemtable extends AbstractShardedMemtable` (`TrieMemtable.java:89`). No `FACTORY` field exists (grep confirms). Its factory access:
- `public static Factory factory(Map<String, String> optionsCopy)` — `TrieMemtable.java:658-663` (consumes `shards`).
- `static class Factory implements Memtable.Factory` — `TrieMemtable.java:665` — **package-private class**, but `create(...)` is public and the instance is usable as `Memtable.Factory` from any package (call `TrieMemtable.factory(map)` and assign to `Memtable.Factory`). So composition CAN create an inner TrieMemtable: `TrieMemtable.factory(params).create(clLowerBound, metadataRef, owner)`.
- The **constructor is package-private**: `TrieMemtable(AtomicReference<CommitLogPosition>, TableMetadataRef, Owner, Integer shardCountOption)` (`TrieMemtable.java:123`), as is `AbstractShardedMemtable`'s (`AbstractShardedMemtable.java:56`). **Extending TrieMemtable therefore requires our subclass to live in package `org.apache.cassandra.db.memtable`** (legal for a classpath jar — no JPMS sealing — but it's a split package; see risks).

**Overridability for the extend path**: the load-bearing methods are all public, non-final, and defined in the hierarchy: `shouldSwitch` (`AbstractAllocatorMemtable.java:131`), `performSnapshot` (`:157` — throws `AssertionError` by default), `switchOut` (`:162`), `discard` (`TrieMemtable.java:161` / `AbstractAllocatorMemtable.java:168`), `getFlushSet` (`TrieMemtable.java:350`), `put` (`TrieMemtable.java:~184`), `partitionIterator`/`rowIterator` (`:281,319`), `metadataUpdated` (`AbstractAllocatorMemtable.java:145`). Package-private `columns()`/`encodingStats()` (`AbstractMemtable.java:119-126`, overridden `TrieMemtable.java:~230-246`) are also visible to a same-package subclass.

**Full forwarding surface if composing** (implement `Memtable` directly; everything below must be forwarded to the inner TrieMemtable — enumerated from `Memtable.java` + `UnfilteredSource.java` + `CellSourceIdentifier.java`):
- Writes/reads: `put(PartitionUpdate, UpdateTransaction, OpOrder.Group)` (:197); `rowIterator(DecoratedKey, Slices, ColumnFilter, boolean, SSTableReadsListener)` + default `rowIterator(DecoratedKey)`; `partitionIterator(ColumnFilter, DataRange, SSTableReadsListener)`; `getMinTimestamp()`; `getMinLocalDeletionTime()` (`rows/UnfilteredSource.java:42-68`).
- Stats: `partitionCount()` (:204), `getLiveDataSize()` (:207), `operationCount()` (:213), `metadata()` (:221).
- Memory: `addMemoryUsageTo(MemoryUsage)` (:230), `markExtraOnHeapUsed` (:285), `markExtraOffHeapUsed` (:295).
- Flush: `getFlushSet(PartitionPosition, PartitionPosition)` (:303).
- Lifecycle: `switchOut(OpOrder.Barrier, AtomicReference<CommitLogPosition>)` (:354), `discard()` (:360), `accepts(OpOrder.Group, CommitLogPosition)` (:367), `getApproximateCommitLogLowerBound()` (:370), `getCommitLogLowerBound()` (:373), `getFinalCommitLogUpperBound()` (:376), `mayContainDataBefore(CommitLogPosition)` (:379), `isClean()` (:382), `setFlushTransaction`/`getFlushTransaction` (:385-386), `shouldSwitch(FlushReason)` (:406), `metadataUpdated()` (:413), `localRangesUpdated()` (:420), `performSnapshot(String)` (:426), default `compareTo` (:389, delegates to `getApproximateCommitLogLowerBound` — safe to keep default if that forwards), and `CellSourceIdentifier.isEqualSource` (default `equals`; `db/CellSourceIdentifier.java` — **exists on 5.0**; forward or ensure wrapper/inner identity consistency).

**What composition breaks (verified — decisive evidence):**
1. **Memory-pressure flush selection skips wrappers.** `AbstractAllocatorMemtable.flushLargestMemtable()` (`AbstractAllocatorMemtable.java:249-318`) — the `MemtableCleaner` bound to the global pool (`:84`) — iterates `ColumnFamilyStore.activeMemtables()` (`ColumnFamilyStore.java:1447-1452`, returns each CFS's **current memtable = the wrapper**) and at `:260-261` does `if (!(currentMemtable instanceof AbstractAllocatorMemtable)) continue;`. A non-`AbstractAllocatorMemtable` wrapper is invisible to the cleaner even though its inner allocator's memory counts against the pool → if the cqlite table is the largest consumer, the cleaner flushes the wrong tables or nothing, and writes stall on pool backpressure with no relief.
2. **Flush-signal identity mismatch.** The inner TrieMemtable calls `owner.signalFlushRequired(this /* = inner */, MEMTABLE_LIMIT)` on trie-size threshold (`TrieMemtable.java:~193`); `CFS.signalFlushRequired` → `switchMemtableIfCurrent(memtable, …)` compares by **reference identity**: `if (data.getView().getCurrentMemtable() == memtable)` (`ColumnFamilyStore.java:1014-1022`). The current memtable is the wrapper, not the inner → the switch silently never happens. Composition would need a delegating `Owner` that rewrites `signalFlushRequired(inner, r)` → `realOwner.signalFlushRequired(wrapper, r)` (and forwards `getCurrentMemtable`, `getIndexMemtables`, `localRangeSplits`).
3. **Periodic flush (`memtable_flush_period_in_ms`) is also instanceof-gated**: the scheduled task only acts `if (current instanceof AbstractAllocatorMemtable)` (`AbstractAllocatorMemtable.java:218-220`).
4. `Flushing.FlushRunnable` calls `toFlush.memtable().getFinalCommitLogUpperBound()` (`Flushing.java`, `writeSortedContents`) — TrieMemtable's `FlushablePartitionSet.memtable()` returns the **inner** (`TrieMemtable.this`, `:373-376`), so `switchOut` must have been forwarded to the inner or this asserts (`AbstractMemtableWithCommitlog.java:116-121`).

**What extension gets for free**: `AbstractMemtable` (stats/columns collectors, flush-transaction holder, `AbstractFlushablePartitionSet` — `AbstractMemtable.java:41-255`); `AbstractMemtableWithCommitlog` (commit-log bounds, write barrier, the subtle `accepts()` CAS loop — `AbstractMemtableWithCommitlog.java:32-127`); `AbstractAllocatorMemtable` (`MEMORY_POOL` registration via `MEMORY_POOL.newAllocator(...)` in ctor `:115-123`, `addMemoryUsageTo` from allocator ownership `:186-192`, `switchOut→allocator.setDiscarding()` `:162-166`, `discard→setDiscarded()` `:168-172`, periodic-flush scheduling `:204-243`, cleaner eligibility); `AbstractShardedMemtable` (shard boundaries from `owner.localRangeSplits` — `AbstractShardedMemtable.java:56-63`); plus all of TrieMemtable's read/write/flush machinery.

### Recommendation: **EXTEND TrieMemtable** (subclass in `org.apache.cassandra.db.memtable`, shipped in the plugin jar)

Composition is *technically* possible but requires forwarding ~30 methods, a delegating `Owner`, and — fatally — still loses memory-pressure flush selection and periodic flush because of the two `instanceof AbstractAllocatorMemtable` gates (`AbstractAllocatorMemtable.java:260`, `:219`), which cannot be fixed from outside. Extending gives correct pool accounting, cleaner eligibility, flush signaling, and commit-log bookkeeping for free; the subclass only overrides `shouldSwitch(SNAPSHOT)` → `false` (+ delegate to `super` otherwise), `performSnapshot(name)` → export, optionally hooks `switchOut`/`discard` for export finalization, and adds a factory:

```java
public class CqliteMemtable extends TrieMemtable {   // same package, plugin jar
    CqliteMemtable(AtomicReference<CommitLogPosition> clb, TableMetadataRef ref, Owner owner, Integer shards, ...) {
        super(clb, ref, owner, shards);
    }
    public static Memtable.Factory factory(Map<String, String> options) { /* consume options via remove() */ }
}
```

Note `AbstractAllocatorMemtable.initialFactory` is read from `metadata().params.memtable.factory()` at construction (`:120`) — i.e., OUR factory — so `shouldSwitch(SCHEMA_CHANGE)` comparisons remain correct without overriding.

---

## 3. Read/iteration surface

**`UnfilteredSource`** (`rows/UnfilteredSource.java:31-69`). TrieMemtable implementations:
- `partitionIterator(ColumnFilter, DataRange, SSTableReadsListener)` — `TrieMemtable.java:281-306`: takes a `subtrie` view of the live `mergedTrie` (merged view over per-shard `InMemoryTrie`s, `:118,151-158`) and returns `MemtableUnfilteredPartitionIterator` (`:536`) that materializes partitions lazily via `getPartitionFromTrieEntry`, copying off-heap data on-heap per-read via `allocator.ensureOnHeap()`. The `readsListener` is ignored ("only accepts sstable signals").
- `rowIterator(DecoratedKey, Slices, ColumnFilter, boolean, SSTableReadsListener)` — `:319-326`; returns `null` when the partition is absent.

**Safety protocol for iterating a LIVE memtable** — the reader must hold an `OpOrder.Group` on the CFS's **`readOrdering`** (`ColumnFamilyStore.java:305`): normal reads do this via `ReadExecutionController.forCommand` → `baseCfs.readOrdering.start()` (`db/ReadExecutionController.java:129-153`); ad-hoc internal scans do `try (OpOrder.Group op = readOrdering.start())` (`ColumnFamilyStore.java:2042`). The reason: after flush, `Flush.reclaim(memtable)` (`ColumnFamilyStore.java:1391-1405`) issues a `readOrdering.newBarrier()`, waits for it (`readBarrier.await()`), and only then calls `memtable.discard()` → `allocator.setDiscarded()` (frees the trie's off-heap buffers, `TrieMemtable.discard():161-181`). Iterating without a read-ordering group risks reading freed buffers. **An exporter thread inside the memtable must therefore either (a) hold CFS `readOrdering.start()` for the duration of iteration, or (b) only iterate at points where discard is provably impossible.**

**Flush path mechanism** — `getFlushSet(PartitionPosition from, to)` (`TrieMemtable.java:350-403`) builds a subtrie plus key-count/size stats and returns an `AbstractFlushablePartitionSet` whose `iterator()` transforms trie entries into `MemtablePartition`s with **`EnsureOnHeap.NOOP`** ("During flushing we are certain the memtable will remain at least until the flush completes. No copying to heap is necessary" — `:389-392`). `FlushablePartitionSet` also exposes `commitLogLowerBound()/commitLogUpperBound()/columns()/encodingStats()` (`Memtable.java:312-338`).

**Can an exporter reuse `getFlushSet` off the hot path? Yes — with a lifetime caveat.** Precedent in-tree: `ColumnFamilyStore.writeMemtableRanges` (`:2654-2695`, the `streamFromMemtable()` path) calls `current.getFlushSet(range.left, range.right)` on the **live current** memtable and writes the result to temporary sstables via `Flushing.FlushRunnable` on the calling thread. Two caveats: (1) `getFlushSet` on TrieMemtable does a full key-iteration pass up front to count/size keys — O(partitions) even before export; (2) the `EnsureOnHeap.NOOP` assumption holds only while the memtable cannot be discarded — for a live-memtable export, hold a `readOrdering` group (or use `partitionIterator`, which does the on-heap copy and is the safe-by-construction choice).

**Consistency semantics while writes continue (TrieMemtable specifics)**: shards are single-writer-locked (`ReentrantLock writeLock`, `MemtableShard.put`, `TrieMemtable.java:459-503`) while "reads are carried out concurrently (including with any write)" (`:108-112` shard comment). Snapshot granularity is **per partition**: each partition's `BTreePartitionData` is swapped atomically by `BTreePartitionUpdater.mergePartitions` under the shard lock, so an iterator sees each partition either before or after any single update — but there is **no cross-partition point-in-time snapshot**: a scan concurrent with writes may see partition A pre-update and partition B post-update, and may or may not see partitions inserted after the subtrie cursor passed their position. Commit-log-position cutoff is only well-defined at switch time (`switchOut` barrier + `LastCommitLogPosition`, `AbstractMemtableWithCommitlog.accepts`, `:69-109`) — a timer-driven live export has *fuzzy* upper watermark by construction (approximate upper bound = `CommitLog.instance.getCurrentPosition()` sampled before starting iteration is a safe *inclusive-may-contain* bound, not an exact one).

---

## 4. `performSnapshot` + timer hooks

**Caller — exactly one on 5.0**: `ColumnFamilyStore.snapshot(...)` (`ColumnFamilyStore.java:2375-2390`):

```java
if (!skipMemtable) {
    Memtable current = getTracker().getView().getCurrentMemtable();
    if (!current.isClean()) {
        if (current.shouldSwitch(FlushReason.SNAPSHOT))
            FBUtilities.waitOnFuture(switchMemtableIfCurrent(current, FlushReason.SNAPSHOT));
        else
            current.performSnapshot(snapshotName);
    }
}
return snapshotWithoutMemtable(snapshotName, predicate, ephemeral, ttl, rateLimiter, creationTime);
```

Reached from `nodetool snapshot` → `StorageService.takeSnapshot` (`StorageService.java:4334`, `skipFlush` option honored at `:4488,4555`) → `Keyspace.snapshot` (`Keyspace.java:251`) → `cfs.snapshot`. There is **no `TakeSnapshotTask` on 5.0** (that class exists only on trunk: `service/snapshot/TakeSnapshotTask.java`, which calls `current.performSnapshot(snapshotName)` at **[trunk]** `:136` — same semantics, relocated into `SnapshotManager`).

**Key semantics**: a plain snapshot **flushes first** because `AbstractAllocatorMemtable.shouldSwitch` returns `true` for every reason except SCHEMA_CHANGE-without-change and OWNED_RANGES_CHANGE (`AbstractAllocatorMemtable.java:131-143` — `default: return true`). So for stock memtables `performSnapshot` is **never called** (the default impl even throws `AssertionError`, `:157-160`). `performSnapshot(name)` is invoked **only if the memtable itself returns `false` from `shouldSwitch(SNAPSHOT)`** — with the memtable **live and current, pre-flush, writes still flowing** (the snapshot then hardlinks only existing sstables via `snapshotWithoutMemtable`; the memtable's data is otherwise *absent from the snapshot*). With `nodetool snapshot --skip-flush`, the memtable is not consulted at all.

**Verdict for tail export**: `performSnapshot` is a usable, upstream-sanctioned hook — override `shouldSwitch(SNAPSHOT) → false` and write the export file in `performSnapshot(name)`. But it only fires on explicit snapshots. For continuous freshness, **a self-managed timer is the real mechanism** — precedent: `AbstractAllocatorMemtable.scheduleFlush` uses `ScheduledExecutors.scheduledTasks.scheduleSelfRecurring(...)` (`AbstractAllocatorMemtable.java:204-224`), deliberately capturing the `Owner` rather than the memtable and re-resolving `owner.getCurrentMemtable()` to avoid pinning a dead memtable. A cqlite exporter timer should copy this pattern exactly.

**Lifecycle callbacks on flush switch** (order, from `ColumnFamilyStore.Flush` ctor `:1175-1218` and `Flush.flushMemtable` `:1290-1390`):
1. `cfs.createMemtable(commitLogUpperBound)` (`:1413-1416`) — our factory creates the replacement;
2. `Tracker.switchMemtable` swaps the view and fires `MemtableSwitchedNotification` (`lifecycle/Tracker.java:559`);
3. `oldMemtable.switchOut(writeBarrier, commitLogUpperBound)` (`:1213`) — write barrier assigned, allocator `setDiscarding`;
4. `setCommitLogUpperBound` seals the bound with a `LastCommitLogPosition` CAS (`:1418-1432`), then `writeBarrier.issue()`;
5. `Flushing.flushRunnables(cfs, memtable, txn)` → `getFlushSet` per disk region → sstables written/committed;
6. `cfs.replaceFlushed(memtable, sstables)` then `reclaim(memtable)`: readOrdering barrier → await → `memtable.discard()` (`:1391-1405`), and Tracker fires `MemtableDiscardedNotification` (`Tracker.java:564`). `getFinalCommitLogUpperBound()` is valid from step 4 onward.

The best "flush-event hook" for finalizing/rotating an export segment is overriding `switchOut` (export cut point: nothing new will arrive after the barrier issues) and `discard` (sstable now live on disk → export segment for this memtable can be retired), plus optionally `INotificationConsumer` on the Tracker for `MemtableDiscarded/SSTableAdded` from a helper component.

---

## 5. Watermark primitives

**On the memtable** (`AbstractMemtableWithCommitlog.java:36-126`): `getApproximateCommitLogLowerBound()` (sampled at construction, `:36,50`), `getCommitLogLowerBound()` (precise, = predecessor's upper bound, `:111`), `getFinalCommitLogUpperBound()` (sealed `LastCommitLogPosition`, valid post-switch, `:116-121`), `mayContainDataBefore(CommitLogPosition)` (`:123`). `CommitLogPosition` = `(segmentId, position)` pair.

**Flushed-sstable correlation — verified**: `Flushing.createFlushWriter` (`db/memtable/Flushing.java:203-221`) passes `new IntervalSet<>(flushSet.commitLogLowerBound(), flushSet.commitLogUpperBound())` into the sstable writer, and **5.0 `StatsMetadata` stores it**: `public final IntervalSet<CommitLogPosition> commitLogIntervals` (`io/sstable/metadata/StatsMetadata.java:64`, serializer `:60`, serialized into `Statistics.db` `:319+`). So every flushed sstable's Statistics.db carries the exact commit-log interval `[memtable clLowerBound, clUpperBound]` — CQLite already parses Statistics.db, so **the dedup protocol is: stamp each export segment with the source memtable's `(commitLogLowerBound, sealed-or-approx upper bound)`; a flushed sstable whose `commitLogIntervals` covers the export segment's interval supersedes it.**

**SSTable identity**: descriptor generation comes from `cfs.newSSTableDescriptor` (`Flushing.java:110-111`); the id is sequence-based (`nb-<N>-...`) by default, UUID-based when `uuid_sstable_identifiers_enabled: true` (`conf/cassandra.yaml:1267`, `config/Config.java:841`). The sstable filename id is *not* directly correlated with the memtable — commitLogIntervals in Statistics.db is the authoritative join key. (Streaming-from-memtable sstables also get correct `commitLogIntervals`, `ColumnFamilyStore.java:2666-2686`.)

---

## 6. Deployment

**cassandra.yaml stanza** (5.0 ships this at `conf/cassandra.yaml:783-790`):

```yaml
memtable:
  configurations:
    skiplist:
      class_name: SkipListMemtable
    trie:
      class_name: TrieMemtable
    default:
      inherits: skiplist
    cqlite:
      class_name: org.apache.cassandra.db.memtable.CqliteMemtable
      parameters:
        export_dir: /var/lib/cassandra/cqlite
        export_interval_ms: "1000"
        shards: "..."          # forwarded to TrieMemtable.factory
```

(Keep the stock `skiplist`/`trie`/`default` entries — overriding the yaml replaces the whole map, and `default` must remain resolvable; `expandDefinitions` injects `default`→SkipList only when `memtable.configurations` is entirely absent, `MemtableParams.java:141-149`.)

**Jar-on-classpath**: `bin/cassandra.in.sh:53-55` adds every `$CASSANDRA_HOME/lib/*.jar` to the classpath — drop the plugin jar in `lib/`. No plugin registry, no ServiceLoader; pure reflection by class name.

**Per-table DDL**: `CREATE TABLE ... WITH memtable = 'cqlite';` / `ALTER TABLE t WITH memtable = 'cqlite';` / reset via `WITH memtable = 'default'` (Memtable_API.md; `TableParams.java:315` serializes `AND memtable = '<key>'` in schema CQL). Schema stores only the configuration **key**; the class/parameters are node-local yaml — heterogeneous rollout is supported by design (fallback-to-default on nodes lacking the config, §1).

**Guardrails**: **none.** No guardrail gates custom memtable classes, table `memtable` property, or `memtable_configurations`. The only related validation is `TableParams.validate`: `cdc=true` is rejected iff the factory's `writesShouldSkipCommitLog()` (`TableParams.java:200-201`) — irrelevant for us (we keep the default `false`; NO CDC interaction).

---

## 7. MemtablePool safety (composition-mode accounting)

Mechanics: the singleton pool `AbstractAllocatorMemtable.MEMORY_POOL` (`AbstractAllocatorMemtable.java:59`, built from `memtable_allocation_type`/`memtable_heap_space` etc., `:78-112`) hands each memtable a `MemtableAllocator` in the `AbstractAllocatorMemtable` constructor (`MEMORY_POOL.newAllocator(...)`, `:118`; `MemtablePool.newAllocator` abstract at `utils/memory/MemtablePool.java:84`). The pool's `MemtableCleanerThread` (`MemtableCleanerThread.java:71-97`) fires `cleaner.clean()` = `AbstractAllocatorMemtable::flushLargestMemtable` when `needsCleaning()`.

If we **wrap by composition**: the inner TrieMemtable DOES register with `MEMORY_POOL` correctly, **but** `flushLargestMemtable` iterates `ColumnFamilyStore.activeMemtables()` (`ColumnFamilyStore.java:1447` — returns the **wrapper**, the inner never appears there) and skips anything not `instanceof AbstractAllocatorMemtable` (`AbstractAllocatorMemtable.java:260-261`) **before** ever calling `addMemoryUsageTo` — so it is the *wrapper's type*, not its forwarded accounting, that decides eligibility. A plain-`implements Memtable` wrapper makes the table permanently ineligible for pressure-triggered flush (and for periodic flush, `:219`). The only composition-shaped fix is to make the wrapper itself `extends AbstractAllocatorMemtable` — at which point it constructs a *second* allocator and duplicates lifecycle, i.e., worse than just extending TrieMemtable. This is the clinching argument for **extend** (§2). Under extension, everything is trivially correct: one allocator, wrapper == the registered AbstractAllocatorMemtable, `addMemoryUsageTo` is the inherited implementation (`:186-192`), and the exporter is pure additional behavior (any exporter-side heap should be reported via `markExtraOnHeapUsed` if significant).

---

## Trunk deltas (deploy target is 5.0)

`git diff origin/cassandra-5.0..origin/trunk` on the memtable package (~340 insertions):
- **`Memtable.Factory.createMemtableMetrics(TableMetadataRef)` → renamed `createMemtableMetricsReleaser(TableMetadataRef)` returning `Runnable`** — source-breaking for a factory that overrides it.
- **`put` gains a 4th arg**: `long put(update, indexer, opGroup, boolean assumeMissing)` is the abstract method ([trunk] `Memtable.java:205`); the 3-arg form becomes a default.
- **New abstract members**: `long getMemtableId()` ([trunk] `:377`), `void notifyFlushed()` ([trunk] `:429`), `<T extends BiConsumer<Long, TableMetadata>> T ensureFlushListener(Object key, Supplier<T> factory)` ([trunk] `:428` — a built-in flush-listener registry, potentially *useful* to the exporter design later), and `shouldSwitch(FlushReason, TableMetadata latest)` (1-arg form becomes default).
- **Snapshot path moved**: `CFS.snapshot`'s memtable branch relocated into `service/snapshot/TakeSnapshotTask.java` (calls `performSnapshot` at [trunk] `:136`); semantics unchanged.
- `TrieMemtable` on trunk: still `public class ... extends AbstractShardedMemtable` ([trunk] `:91`), still `factory(Map)` with only `shards` ([trunk] `:756`), +130 lines of internal changes. `MemtableParams.java`: **zero diff** — the reflection contract is stable.
- `CellSourceIdentifier` exists on both 5.0 and trunk.

## Risks / unknowns

1. **Split package**: extending requires the plugin class in `org.apache.cassandra.db.memtable` inside a foreign jar. Works on the 5.0 flat classpath (no sealing), but fragile against future JPMS modularization. Mitigation: keep only the thin subclass + factory in that package; all export logic in a `cqlite.*` package.
2. **Live-iteration watermark fuzziness** (§3): timer-driven exports have no exact commit-log cutoff; only switch-time bounds are exact. Treat timer segments as "may contain up to ~X"; rely on sstable `commitLogIntervals` for authoritative supersession.
3. **Export-thread lifetime safety**: a timer export must hold `readOrdering.start()` (needs the CFS — `Owner` *is* the CFS on 5.0, but `Owner` doesn't expose `readOrdering`; casting `owner` to `ColumnFamilyStore` is an implementation-coupling decision) or restrict iteration to windows where discard is impossible.
4. **`getFlushSet` pre-pass cost**: O(partitions) key-size counting pass per call (`TrieMemtable.java:353-366`) — fine at flush, possibly wasteful for frequent tail exports; `partitionIterator` (with on-heap copying) may be the better exporter surface. Needs a benchmark decision.
5. **`performSnapshot` opt-out side effect**: returning `false` from `shouldSwitch(SNAPSHOT)` means **snapshots no longer contain memtable data as sstables** — restore tooling sees less data unless it also consumes the CQLite export file. Product decision required (alternative: keep stock flush-on-snapshot behavior, rely solely on timer + flush hooks).
6. **Exporter memory/IO is unaccounted**: buffering during export should use `markExtraOnHeapUsed` or stay bounded; never block inside `switchOut` (called under the flush path) — hand export finalization off to an executor.
7. **Trunk churn**: per-major-version plugin builds required; 5.0→trunk is not binary compatible.
8. **`getWithFallback` silence**: a node missing the jar silently runs SkipList for the table (error log only) — CQLite must detect "no export file appearing" as a deployment fault rather than assume an empty memtable.
9. **Unverified small item**: whether any 5.0 code path calls `Memtable.isEqualSource` in a way that distinguishes wrapper vs inner (moot under the extend recommendation).
