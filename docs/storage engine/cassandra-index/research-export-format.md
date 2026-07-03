# CEP-11 Memtable Tail-Export: Format + Writer Mechanism — Findings

*Research pass for the memtable-plugin design doc (spike #1807). Verified against `origin/cassandra-5.0` @ `464b2e54` unless marked TRUNK-DELTA. Produced 2026-07-03.*

---

## 1. Option 1 — Write a real SSTable from inside the JVM (RECOMMENDED)

### 1.1 The flush path is directly reusable, and it is NOT Tracker-coupled

The flush pipeline is `Flushing.flushRunnable()` → `memtable.getFlushSet(from, to)` → `cfs.createSSTableMultiWriter(...)` → `FlushRunnable.writeSortedContents()` which just iterates partitions and calls `writer.append(partition.unfilteredIterator())`:

- `src/java/org/apache/cassandra/db/memtable/Flushing.java` (5.0): `flushRunnable` :102–120 (`getFlushSet` :105, `cfs.newSSTableDescriptor(dir, format)` :110–111); `createFlushWriter` :203–218 builds the writer with `IntervalSet<>(flushSet.commitLogLowerBound(), flushSet.commitLogUpperBound())` (:214) and `new SerializationHeader(true, metadata, flushSet.columns(), flushSet.encodingStats(), false)` (:216). The write loop (trunk `Flushing.java:164–181`, same shape on 5.0) is a plain for-loop over `Iterable<Partition>` calling `writer.append(iter)`.
- **Tracker registration is a separate, skippable step.** The Tracker/live-set registration happens in `ColumnFamilyStore.Flush`/`Tracker.replaceFlushed`, *not* inside the writer. `SSTableTxnWriter.create(cfs, descriptor, …)` (`src/java/org/apache/cassandra/io/sstable/SSTableTxnWriter.java:111–116`, byte-identical on 5.0 and trunk) uses `LifecycleTransaction.offline(OperationType.WRITE)`, which builds a **dummy tracker**: `src/java/org/apache/cassandra/db/lifecycle/LifecycleTransaction.java:176–180` (5.0) — `Tracker dummy = Tracker.newDummyTracker(); new LifecycleTransaction(dummy, new LogTransaction(operationType, dummy), emptyList())`. This is the exact mechanism `CQLSSTableWriter`, streaming, and scrub tools use to write real SSTables with zero live-set impact.
- **Directory is caller-chosen.** `Descriptor` carries an arbitrary target directory; `cfs.newSSTableDescriptor(File directory, ...)` (`ColumnFamilyStore.java:975–995`, 5.0) accepts any dir and asserts only that `Data.db` doesn't already exist there.

**Recipe (all public/package APIs on 5.0):**
```java
FlushablePartitionSet fs = memtable.getFlushSet(minBound, maxBound);      // Memtable.java:303
Descriptor desc = cfs.newSSTableDescriptor(exportStagingDir, BigFormat);  // CFS.java:980
SSTableTxnWriter w = SSTableTxnWriter.create(cfs, desc, fs.partitionCount(), UNREPAIRED, null, false, header);
for (Partition p : fs) try (UnfilteredRowIterator it = p.unfilteredIterator()) { w.append(it); }
w.finish(false);   // openResult=false — never open a reader, never touch live set
```
`finish(false)` → `SSTableWriter.TransactionalProxy.doPrepare` (5.0 `format/SSTableWriter.java:386–393`): prepares data/index/filter writers, writes **TOC at prepare** (`TOCComponent.updateTOC(descriptor, components)` :390), and skips `openFinal` when `openResult==false`.

### 1.2 Live (unflushed) memtable iteration is supported — with three documented cares

- `Memtable.getFlushSet(from, to)` is on the `Memtable` interface (5.0 `db/memtable/Memtable.java:303`) and `TrieMemtable`'s implementation (5.0 `TrieMemtable.java:350–404`) returns a **live view** (`mergedTrie.subtrie(...)`) — nothing in it requires `switchOut` to have happened. It does one O(n) key-count pre-pass (:356–366) then a transforming iterator. It does **not** call `setFlushTransaction`, so a concurrent *real* flush is not blocked and won't trip the double-flush precondition (`Flushing.java:61–63` — `memtable.setFlushTransaction(txn)` is only set by `flushRunnables`, present on both 5.0 and trunk).
- **Care 1 — stats/columns race (documented in-tree):** `FlushablePartitionSet` javadoc (5.0 `Memtable.java:308–311`): *"if the memtable is still being written to, care must be taken to not list newer items as they may violate the bounds collected by the encoding stats or refer to columns that don't exist in the collected columns set."* Mitigation for export: build the `SerializationHeader` with `EncodingStats.NO_STATS` (epoch-based bases, always safe — `db/rows/EncodingStats.java:69`) and the table's **full** `regularAndStaticColumns()` instead of `flushSet.columns()`/`flushSet.encodingStats()`. Cost: slightly larger vint deltas in Data.db. The *authoritative* Statistics.db min/max timestamps are unaffected (see 1.4).
- **Care 2 — memtable discard / off-heap use-after-free:** `TrieMemtable.getFlushSet`'s iterator uses `EnsureOnHeap.NOOP` with the comment *"During flushing we are certain the memtable will remain at least until the flush completes"* (5.0 `TrieMemtable.java:393–395`). For a live export that guarantee doesn't hold: after a real flush, `reclaim()` issues a `readOrdering` barrier and then `memtable.discard()` → `NativeAllocator.setDiscarded()` → `MemoryUtil.free` (5.0 `ColumnFamilyStore.java:1391–1404`; `utils/memory/NativeAllocator.java:200–205`). Mitigation: run the export inside `try (OpOrder.Group op = cfs.readOrdering.start())` — exactly how reads are protected (`ColumnFamilyStore.java:305, 2042`); discard awaits the barrier, so the memtable memory is pinned for the export's duration. (Consequence: a long export delays memory reclaim of a concurrently-flushed memtable — bound export time.)
- **Care 3 — iteration does not block writes:** TrieMemtable shards take write locks only on the write path; the trie supports lock-free concurrent reads. Export is plain background iteration + serialization on any executor — off the write hot path.

### 1.3 Components written, and what a reader needs

Default component set from the writer builders (5.0):
- `format/SortedTableWriter.java` builder → `DATA, STATS, DIGEST, TOC` (5.0 :487) + `FILTER` if bf enabled;
- BIG adds `PRIMARY_INDEX, SUMMARY` (`format/big/BigTableWriter.java:368–372`) + `COMPRESSION_INFO`/`CRC` per table params. `FILTER` and `SUMMARY` are `GENERATED_ON_LOAD_COMPONENTS` (`format/big/BigFormat.java:103`) — optional for readers.

You cannot trivially suppress components below the defaults without a custom builder, and you shouldn't: CQLite already consumes the full nb set (and needs Summary.db for token pruning, Statistics.db for open-time checks + watermark). Component set follows the table's compression params automatically.

### 1.4 Statistics.db is authoritative for watermarking

`StatsMetadata` (5.0 `io/sstable/metadata/StatsMetadata.java:62–101`) carries `commitLogIntervals` (:64), `minTimestamp`/`maxTimestamp` (:65–66), min/max localDeletionTime, TTLs, `totalRows`, `firstKey`/`lastKey` (:100–101). **min/max timestamps are collected from actual cells during `append`**, not copied from the memtable's claimed stats: `SortedTableWriter.java` calls `Rows.collectStats(row, metadataCollector)` per row (:195, :214) and `metadataCollector.update(deletionTime)` for RT markers (:227–232); `MetadataCollector.updateTimestamp` (5.0 `metadata/MetadataCollector.java:222–262`). `commitLogIntervals` is whatever the writer-creator passes (`MetadataCollector.commitLogIntervals(...)` :284–286; flush passes `IntervalSet(lower, upper)` via `Flushing.createFlushWriter` :214). So an export gets **free, authoritative** min/max timestamps, and the exporter controls the commit-log interval stamped into Statistics.db.

**Watermark semantics for a live export**: capture `P_start = CommitLog.instance.getCurrentPosition()` *before* `getFlushSet`. All writes with position < `P_start` destined to this memtable are already in the trie, so the export contains **all** of them (iteration may additionally include newer items — harmless under CQLite's LWW k-way merge). Stamp `IntervalSet(memtable.getCommitLogLowerBound(), P_start)` (lower bound getter: 5.0 `Memtable.java:373–375`). CQLite reads this from Statistics.db with zero new code.

### 1.5 Hazards (option 1)

| Hazard | Evidence | Disposition |
|---|---|---|
| Txn log file `<ver>_txn_write_<uuid>.log` appears in export dir during write | `LogTransaction` created even by `offline()` (`LifecycleTransaction.java:179`); log filename format `db/lifecycle/LogFile.java:545–548`; commit = append COMMIT record then delete (`LogFile.java:196, 323, 343–345`) | Benign; external reader must ignore dirs containing a `*_txn_*.log` (or use staging-dir rename, §4) |
| Files are written at **final names** (no tmp-rename for Data.db) | `BigTableWriter.java:253` opens `descriptor.fileFor(Components.PRIMARY_INDEX)` directly; `Descriptor.TMP_EXT` only used for streaming/legacy | Atomic publication must be layered on top (§4) |
| TOC written in-place with `CREATE\|TRUNCATE_EXISTING\|SYNC`, no rename | 5.0 `format/TOCComponent.java:92–113` | TOC-exists is still a good "writer finished prepare" signal since it's written at `prepareToCommit` (`SSTableWriter.java:390` 5.0) |
| SSTable id collision with real flushes | None — `cfs.newSSTableDescriptor` draws from the **same** per-CFS `sstableIdGenerator` (`ColumnFamilyStore.java:308, 993`), so export ids and flush ids share one sequence; `assert !Data.db exists` (:994) | Non-issue if descriptor comes from the CFS; do NOT mint ids independently |
| Double-flush precondition | `Flushing.java:61–63` only triggered via `flushRunnables` | Non-issue: call `getFlushSet` directly, never `flushRunnables`, never `setFlushTransaction` |
| Stats/columns race + UAF on live iteration | §1.2 Cares 1–2 | `EncodingStats.NO_STATS` + full column set; hold `cfs.readOrdering` group |
| `SSTableTxnWriter.finish(true)` would open a reader | `SSTableTxnWriter.java:103–108` | Always `finish(false)` |

### 1.6 CQLSSTableWriter — rejected for in-server use

`io/sstable/CQLSSTableWriter.java` (5.0): static initializer calls `DatabaseDescriptor.clientInitialization(false)` (:134). With `failIfDaemonOrTool=false` and the daemon already initialized this returns early without crashing (`config/DatabaseDescriptor.java:342–345` 5.0), **but** the builder then mutates global schema — `Schema.instance.transform(SchemaTransformations.addKeyspace/addTable(...))` under `synchronized (CQLSSTableWriter.class)` (:682–732) — i.e., it would attempt real schema changes on a live node, expects CQL-statement input (not `UnfilteredRowIterator`s), and buffers/re-sorts in heap. It's a bulk-load *tool* API. TRUNK-DELTA: trunk's version is heavily TCM-ified (233-line diff vs 5.0). `SSTableTxnWriter` + flush-writer path avoids all of this.

`SimpleSSTableMultiWriter` / `RangeAwareSSTableWriter`: both are reached via `cfs.createSSTableMultiWriter` (5.0 `ColumnFamilyStore.java:660–672`) and call `lifecycleNewTracker.trackNew(writer)` (`SimpleSSTableMultiWriter.java:41–43,136`) — with the offline txn that just records into the LogFile, which is fine. RangeAware only matters for multi-disk boundary splitting; unnecessary for export (single side dir).

---

## 2. Option 2 — Arrow IPC: no dependency exists; bundling is heavy

- **Zero Arrow anywhere**: no Arrow in `build.xml` (5.0 or trunk), no arrow/parquet jars in `lib/`.
- Bundling arrow-java in a plugin jar entails `arrow-vector`, `arrow-memory-core` + a memory impl (`arrow-memory-netty`), `flatbuffers-java`, and netty-buffer version reconciliation with Cassandra's own netty; shaded footprint ~10–15 MB, plus Arrow's memory module conventionally requires `--add-opens java.base/java.nio=ALL-UNNAMED` (a server JVM-flag change) [external knowledge, not from repo]. CQLite would also need a new Arrow-IPC-tail reader AND would *lose* Statistics.db-style authoritative min/max timestamps + commitLogIntervals — they'd have to be reinvented as sidecar metadata. Strictly dominated by option 1.

## 3. Option 3 — Custom format via Cassandra's row serializers: unstable, strictly worse

`db/rows/UnfilteredRowIteratorSerializer.java` (5.0, header comment :15–42) serializes whole unfiltered partitions, **but** the comment is explicit: *"the format described above is the on-wire format"* — versioned by `MessagingService` version, its `SerializationHeader.Serializer` encodes a column *subset* against schema known out-of-band, no cross-release stability guarantee. CQLite would need a brand-new parser for a format less stable than the SSTable format it already parses, missing partition index/stats. Reject.

## 4. Atomicity + naming

**How Cassandra itself does it**: modern Cassandra does **not** tmp+rename data files. Files are written at final names; crash-atomicity comes from the LogFile transaction-log protocol: a `<version>_txn_<optype>_<TimeUUID>.log` in the sstable's directory lists ADD records; commit appends a COMMIT record then the tidier deletes the log; startup treats ADD-listed files without COMMIT as leftovers to delete (`LogFile.java:65–67, 144, 196, 343–345`; name format :545–548). TOC is written (synced, in-place) at `prepareToCommit`.

**Proposed export protocol** (external Rust reader must never see a torn export — don't make CQLite parse txn logs):
1. Write the sstable into a **staging subdir**: `<export_root>/<tableId>/.staging-<seq>/` via the §1.1 recipe; `finish(false)` (txn log lives and dies inside staging).
2. fsync staging dir; then `Files.move(staging, final, ATOMIC_MOVE)` → `<export_root>/<tableId>/gen-<seq>/`; fsync parent. Directory rename on one filesystem is atomic — reader either sees a complete generation dir or nothing.
3. Reader contract: only descend into `gen-*` dirs; optionally verify Digest.crc32.

**Naming scheme** (metadata that can't ride in the sstable filename goes in the dir name + a tiny manifest; the sstable inside keeps its native Cassandra name `nb-<sstableId>-big-*` — `Descriptor.appendFileName`, 5.0 `Descriptor.java:185–200`):
```
<export_root>/<keyspace>/<tableId>/            # TableMetadata.id (UUID) — survives DROP/recreate
  gen-<seq10>-clb-<segId>,<pos>-wm-<P_start_segId>,<pos>-epoch-<memtableLowerBoundMicros>/
    nb-<id>-big-Data.db … TOC.txt
    export-manifest.json   # {tableId, schemaVersion, seq, commitLogLowerBound, watermark=P_start, wallClock}
```
`seq` = monotonically increasing export sequence per table (exporter-owned). `watermark` = `P_start` from §1.4 ("export contains everything < P_start that this memtable owns"). `commitLogIntervals` inside Statistics.db carries the same watermark authoritatively — the dir/manifest copies are for cheap discovery without parsing Statistics.db. Superseded-export GC: a new `gen-N` with same memtable epoch supersedes `gen-(N-1)`; after real flush of that memtable (observable by the flushed sstable's own `commitLogIntervals` covering the export's interval), all its exports are garbage.

## 5. Snapshot precedent — confirmed: snapshots do NOT capture the memtable tail

5.0 `ColumnFamilyStore.snapshot(...)` (`ColumnFamilyStore.java:2375–2389`): if `!skipMemtable` it flushes (`switchMemtableIfCurrent`) or calls `current.performSnapshot`; **with `skipMemtable=true` it goes straight to `snapshotWithoutMemtable`** (:2124–2160), which only hardlinks already-flushed sstables (`ssTable.createLinks(snapshotDirectory.path(), rateLimiter)` :2157). Live memtable content is simply absent. So `nodetool snapshot --skip-flush` cannot serve as tail export, and snapshot-with-flush is exactly the flush cost we're trying to schedule independently. TRUNK-DELTA: `service/snapshot/TakeSnapshotTask.java` **does not exist on 5.0** — cite the CFS methods, not TakeSnapshotTask, in the design doc.

## 6. SAI precedent (brief)

SAI maintains a parallel live in-memory index per memtable: `index/sai/memory/MemtableIndexManager.index(key, row, memtable)` on the write path (:69–94), backed by sharded `TrieMemoryIndex`; queries hit it live via `MemtableIndex.search` (:106–108); raw `iterator()` used at flush time to write index files. Pattern relevance: precedent for safely reading live memtable-adjacent state concurrently with writes — but SAI serializes only at flush, not periodically; existence proof, not a mechanism to copy.

## 7. Overhead framing (option 1)

- **Same serialization work as a flush**: identical code path — `writer.append(UnfilteredRowIterator)` → `SortedTableWriter.append` (5.0 `SortedTableWriter.java:184–248`). Plus one extra O(n) key-walk in `TrieMemtable.getFlushSet` (:356–366).
- **Non-incremental**: each export rewrites the *entire* live memtable contents to date. Cost per export ≈ cost of flushing a memtable of that size; exporting every T seconds while the memtable fills linearly writes ~½ × (lifetime/T) × final-size extra bytes.
- **Off the write hot path**: export runs on a background thread; TrieMemtable reads are concurrent/lock-free vs writes. The only coupling is (a) CPU/IO contention and (b) the `readOrdering` pin delaying memory reclaim if a real flush overlaps the export (§1.2 Care 2).
- The flush path self-reports cost (`FlushRunnable` logs duration, CPU time, heap allocated) — free instrumentation for cadence tuning.

---

## Comparison table

| Criterion | 1. Real SSTable (flush-writer reuse) | 2. Arrow IPC | 3. Custom (UnfilteredRowIteratorSerializer) |
|---|---|---|---|
| CQLite reader work | **Zero** (reads nb natively) | New Arrow-tail reader | New parser for unstable format |
| JVM-side dep/footprint | Zero (all in-tree APIs) | ~10–15 MB shaded + `--add-opens` | Zero |
| Tracker/live-set isolation | Proven (`LifecycleTransaction.offline` dummy tracker, used by tools/streaming) | n/a | n/a |
| Watermark metadata | **Free + authoritative** (StatsMetadata min/max ts from actual cells; caller-stamped commitLogIntervals) | Must reinvent | Must reinvent |
| Format stability | SSTable `nb` — the most stable surface Cassandra has | Arrow IPC stable, sidecar metadata ad hoc | On-wire, MessagingService-versioned, no guarantee |
| Atomic publication | Staging dir + atomic rename (§4) | Same effort | Same effort |
| Cost per export | = flush-equivalent serialization (non-incremental) | Similar | Slightly cheaper but not materially |
| Hazards | 3 known, all mitigable with in-tree mechanisms | Netty/memory-module conflicts in-server | Version skew corrupts silently |

## Recommendation

**Option 1 — real-SSTable export via the flush machinery with an offline LifecycleTransaction.** `memtable.getFlushSet(...)` + `cfs.newSSTableDescriptor(sideDir)` + `SSTableTxnWriter.create(cfs, …)`/`finish(false)` reuses the flush serialization path with **zero** Tracker/TOC/live-set registration (dummy tracker is a first-class, tool-proven mechanism), yields authoritative Statistics.db min/max timestamps for free, and lets the exporter stamp `commitLogIntervals` as the watermark. The two genuine subtleties of live-memtable iteration — encoding-stats/column races and memtable-discard UAF — are both solvable with in-tree primitives (`EncodingStats.NO_STATS` + full column set; `cfs.readOrdering.start()` pin), and both are *documented* in 5.0's own javadoc. Do NOT use `CQLSSTableWriter`.

## Risks / unknowns

1. **`getFlushSet` on a never-switched-out memtable is untested upstream** — flush always calls it post-`switchOut`. The live-view mechanics hold by construction (subtrie over live trie), but no upstream test exercises concurrent-write iteration; the spike needs its own stress test (trie iterator vs concurrent shard mutation).
2. **`flushSet.commitLogUpperBound()` is null/undefined pre-switchOut** (`getFinalCommitLogUpperBound` only set at switch-out) — exporter MUST supply its own `IntervalSet(lower, P_start)` via direct `cfs.createSSTableMultiWriter(...)` (the 8-arg overload with commitLogPositions, `ColumnFamilyStore.java:665–672`) rather than `Flushing.createFlushWriter`.
3. **readOrdering pin duration**: a slow export delays off-heap reclaim of a concurrently flushed memtable — bound export size/time, or chunk by token range (`getFlushSet(from,to)` supports ranges natively).
4. **Trunk deltas to track for forward-port**: `TrieMemtable` +130 lines on trunk; `CQLSSTableWriter` TCM-ified; `TakeSnapshotTask.java` trunk-only; `Flushing.java` +37. Core seams (`SSTableTxnWriter`, `LifecycleTransaction.offline`, `TOCComponent`, `getFlushSet` signature) are stable across both.
5. **SSTable-id sequence sharing**: drawing export ids from the CFS generator is collision-safe but export generations consume ids visible in future flush filenames (cosmetic). Alternative: enable UUID sstable identifiers.
6. **Wrapper-memtable interaction**: if delegating `getFlushSet` to an inner memtable, `EnsureOnHeap.NOOP` assumptions and shard locking are the inner implementation's (SkipListMemtable's flush set differs from TrieMemtable's). Moot under the extend recommendation.
7. **Crashed-exporter staging leftovers**: the atomic-dir-rename protocol makes torn state invisible externally, but a crashed exporter leaves `.staging-*` dirs — exporter must sweep them on startup (Cassandra's own `removeUnfinishedLeftovers` only runs for real data dirs).
