> DROPPED (2026-06-11): Jon is not concerned about this constant; removed from the branch concern list. Kept only as reference.

# Proposal: lazily allocate the StreamingTombstoneHistogramBuilder spool (3 MiB per sstable writer, allocated even for tombstone-free tables)

> Investigation + solution proposal. NOT committed code; nothing in src/ has been changed.
> This is SHARED infrastructure (memtable flush, both compaction paths, streaming, scrub,
> anticompaction all go through MetadataCollector), so it is OUT OF SCOPE for the
> cursor-compaction JIRA and should be filed as its own upstream ticket.

## Summary

Every `MetadataCollector` constructed in Cassandra eagerly allocates a
`StreamingTombstoneHistogramBuilder` whose `Spool` buffer is a `long[262144]` (2 MiB) plus an
`int[262144]` (1 MiB) — 3 MiB per sstable writer, unconditionally, before a single
row is written. One `MetadataCollector` is built per sstable writer: every memtable flush,
every compaction output (multi-output compactions build several), every streamed sstable,
scrub, upgrade, anticompaction. For a table with zero tombstones and no TTLs the spool is
never written to after construction; the 3 MiB is pure allocation churn. JFR allocation
profiling of cursor compaction showed it as the single largest allocation source inside
`CompactionTask.execute` (62-69% of within-compaction sampled allocation in one profile,
~90 MiB over 30 small compactions). The fix proposed here — allocate the spool lazily on the
first histogram update — is ~20 lines in one file, byte-identical in output (Statistics.db
unchanged for any input), and eliminates the cost entirely for tombstone/TTL-free writers
while merely deferring it (one-time, first-tombstone) for everyone else.

## Background: what the histogram and spool are for

`StatsMetadata.estimatedTombstoneDropTime`
(`src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java:71`) is a ~100-bucket
histogram of local deletion times, serialized into each sstable's Statistics.db. Its consumer
is `SSTableReader.getDroppableTombstonesBefore(gcBefore)`
(`src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java:1303` →
`StatsMetadata.java:180-183`, `estimatedTombstoneDropTime.sum(gcBefore)`), which drives the
droppable-tombstone ratio used by `AbstractCompactionStrategy.worthDroppingTombstones`
(`AbstractCompactionStrategy.java:400`) and the single-sstable tombstone-compaction
heuristics in STCS/LCS/TWCS, plus the `EstimatedDroppableTombstoneRatio` metric and
`sstablemetadata` tooling.

The histogram is built during write by `StreamingTombstoneHistogramBuilder`
(`src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java`),
an implementation of the Ben-Haim/Tom-Tov streaming histogram with two layers:

- **`DataHolder bin`** (line 72, class at 170): the real histogram — `maxBinSize + 1 = 101`
  sorted `(point, value)` pairs (`long[101]` + `int[101]`, ~1.2 KiB, negligible). When full,
  the two nearest points are merged (`mergeNearestPoints`, line 248). Merging on every
  insert is O(bin) per point, which was slow for TTL-heavy tables.
- **`Spool spool`** (line 75, class at 417): a fixed-size open-addressing primitive hash map
  that batches and pre-aggregates points before they reach the bin, so identical (rounded)
  deletion times accumulate without touching the bin at all. When the spool fills, it is
  drained into the bin (`flushHistogram`, line 125). The spool exists purely as a
  **performance buffer** — it changes when merges happen, not what data is recorded.

History (via `git log --follow` on the builder):

- **CASSANDRA-13038** (`a5ce963117`, 2017-02, "Faster streaming histograms"): introduced the
  100,000-entry spool concept and `-Dcassandra.streaminghistogram.roundseconds=60`
  (`CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS`,
  `CassandraRelevantProperties.java:587`), explicitly to fix TTL-heavy compaction CPU cost.
- **CASSANDRA-13444** (`06da35fdda`, 2017-04, "Fast and garbage-free Streaming Histogram"):
  rewrote spool + bin as primitive arrays (the current `Spool` class) to eliminate per-point
  boxing garbage. "Garbage-free" here meant per-point garbage; the fixed buffers themselves
  got big.
- **CASSANDRA-14773** (`00fb6d76d0`, 2020-03): widened points to `long` for large deletion
  times and split the spool into separate `long[] points` + `int[] values` (partly to avoid
  humongous G1 allocations of a single combined array). This is where the current
  2 MiB + 1 MiB shape comes from.
- **CASSANDRA-14834** (`5e8f7f591d`, 2020-12, "Release StreamingTombstoneHistogramBuilder
  spool when switching writers"): added `releaseBuffers()` (builder line 139) /
  `MetadataCollector.release()` (`MetadataCollector.java:494-497`) /
  `SSTableWriter.releaseMetadataOverhead()` (`SSTableWriter.java:361-364`), called from
  `SSTableRewriter.switchWriter` (`SSTableRewriter.java:255`). **Prior art**: upstream already
  recognized the spool as a memory problem for many-writer compactions — but 14834 only fixes
  *retained* heap (the spool of a finished writer is dropped early); every writer still
  *allocates* the full 3 MiB up front. The allocation-churn half was never addressed.

### Size math (verified against the code)

Constructor chain: `MetadataCollector` field init (`MetadataCollector.java:122`)

```java
protected StreamingTombstoneHistogramBuilder estimatedTombstoneDropTime =
    new StreamingTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE,      // 100
                                           SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE,    // 100000
                                           SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS); // 60 (property)
```

Constants at `src/java/org/apache/cassandra/io/sstable/SSTable.java:71-73`. There is no yaml
knob; only the round-seconds value is a system property. In `Spool.<init>`
(`StreamingTombstoneHistogramBuilder.java:425-436`):

- `capacity = getPowerOfTwoCapacity(100000)` = 2^ceil(log2(100000)) = **131,072**
- `points = new long[capacity * 2]` → `long[262144]` = **2,097,152 B (2 MiB)** (line 433)
- `values = new int[capacity * 2]` → `int[262144]` = **1,048,576 B (1 MiB)** (line 434)
- plus `clear()` immediately writes all 2 MiB of `points` with -1 (line 446) — the arrays are
  touched at construction, so this is not even lazily-committed zero pages.

Total: **3 MiB allocated and dirtied per MetadataCollector**, i.e. per sstable writer. The
`DataHolder` bin (~1.2 KiB) and the rest of MetadataCollector (~few KiB incl. HyperLogLog++)
are noise by comparison.

## Evidence (JFR, this branch)

From JFR allocation profiling of cursor compaction on branch `cursor-compaction-completion`
(recordings in `garbage-free-compaction-improvements/jfr-reports/`):

- `StreamingTombstoneHistogramBuilder$Spool.<init>` allocating the 2 MiB `long[]` (line ~433)
  and 1 MiB `int[]` (line ~434), via `StreamingTombstoneHistogramBuilder.<init>` via
  `MetadataCollector.<init>(ClusteringComparator, UUID)` (`MetadataCollector.java:122` field
  initializer), was the **single largest allocation source inside `CompactionTask.execute`**.
- ~**90 MiB over 30 small compactions** (3 MiB × outputs per compaction).
- **62-69% of within-compaction sampled allocation** in one profile.
- It passed the allocation-scaling gate
  (`test/unit/org/apache/cassandra/db/compaction/differential/CursorCompactionAllocationGateTest.java`)
  because it does NOT scale per-row — it is a per-writer constant. The gate currently has no
  assertion on per-writer constants, so this cannot regress-trip today.

## When it matters

The spool is only ever *used* when a deletion time is recorded.
`MetadataCollector.updateLocalDeletionTime` (`MetadataCollector.java:326-331`) calls
`estimatedTombstoneDropTime.update(ldt)` only when `ldt != Cell.NO_DELETION_TIME`; callers
are `update(Cell)` (only non-live cells / expiring cells have a deletion time),
`update(LivenessInfo)` (only when `localExpirationTime()` is set, i.e. TTL'd rows), and
`update(DeletionTime)` (only when `!dt.isLive()`). **A workload with no tombstones, no TTLs,
and no deletions never touches the spool after construction** — the 3 MiB (plus the 2 MiB
`Arrays.fill` in `clear()`) is 100% waste for such tables. Confirmed by reading every update
path; there is no unconditional `update()` call.

Where the 3 MiB is paid (every site below funnels into a fresh `MetadataCollector`):

- **Memtable flush**: `Flushing.createFlushWriter` (`db/memtable/Flushing.java:242`) →
  `cfs.createSSTableMultiWriter` → strategy →
  `SimpleSSTableMultiWriter.create` (`io/sstable/SimpleSSTableMultiWriter.java:128`) or, for
  UCS, `ShardedMultiWriter.createWriter` (`db/compaction/unified/ShardedMultiWriter.java:109`)
  — one collector per flush writer per data directory/shard. A node flushing once a minute
  allocates+dirties **~4.2 GiB/day** of spool from flushes alone (3 MiB/min); with UCS a
  sharded write allocates one per shard it crosses.
- **Compaction (both iterator and cursor paths)**: `CompactionAwareWriter.sstableWriter`
  (`db/compaction/writers/CompactionAwareWriter.java:239`) — one per output sstable; the
  cursor pipeline (`CursorCompactionPipeline.java:48`) uses the same `CompactionAwareWriter`,
  so cursor compaction inherits the cost. Multi-output writers (`MaxSSTableSizeWriter`,
  `MajorLeveledCompactionWriter`, `SplittingSizeTieredCompactionWriter`, UCS sharding) pay it
  per output: an LCS L0→L1 compaction emitting 10 sstables allocates 30 MiB of spool.
- **Streaming**: `RangeAwareSSTableWriter` (`io/sstable/RangeAwareSSTableWriter.java:77,99`)
  → `cfs.createSSTableMultiWriter` per disk-range switch; bulk load / bootstrap / repair
  streams pay 3 MiB per received sstable writer.
- **Scrub / upgrade / anticompaction / relocation**: `CompactionManager.java:1835,1876`,
  `Upgrader.java:74`.

Lifecycle: the collector lives exactly as long as its writer. `release()`
(CASSANDRA-14834) frees the spool early when `SSTableRewriter.switchWriter` moves to the next
output (`SSTableRewriter.java:255`), so *peak retained* heap is bounded — but every new writer
allocates a fresh spool, so *allocation rate* is unchanged. The collector is confined to the
writing thread (flush writer thread, compaction thread, streaming deserializer thread);
`update()`/`build()`/`release()` are all invoked from that thread (early-open `build()`
included — it runs in the append path). There is no synchronization in the class and none is
needed today.

## Solution options

### Option A (recommended): lazy allocation on first update

Keep `maxSpoolSize` as a field; leave `spool == null` until the first
`update(point, value)` call, then allocate. Tombstone-free writers never allocate;
tombstone-bearing writers allocate once, at first tombstone, on the writing thread.

- **Correctness**: identical histogram. The spool's content and capacity are exactly the same
  from the first update onward, so flush timing, bin insertion order, and merge sequence are
  bit-for-bit what they are today. `flushHistogram()` (line 125-133) already null-checks the
  spool; `build()` and `releaseBuffers()` work unchanged.
- **Determinism / format**: **Statistics.db is byte-identical for any input** — for the same
  code version AND versus current code. This is the only option with that property. The
  differential byte-comparison harness
  (`test/unit/org/apache/cassandra/db/compaction/differential/DifferentialCompactionTester.java`,
  `assertEquivalentOutputs`) passes unchanged.
- **Concurrency**: allocation happens on the same single writer thread that calls `update()`;
  no new sharing. One subtlety: `update()` currently asserts `spool != null` as a
  use-after-`releaseBuffers` canary (line 104); lazy allocation needs a separate `released`
  boolean so the canary survives (null now also means "not yet needed").
- **Cost not addressed**: tombstone/TTL-heavy workloads still allocate 3 MiB per writer
  (deferred, not removed). Acceptable: for those workloads the spool is doing its job.
- **Patch size**: ~20 lines in `StreamingTombstoneHistogramBuilder.java`, plus tests.

### Option B: pool/reuse spools across writers

A small global pool (e.g. `MpmcArrayQueue` or per-thread via `FastThreadLocal`) of Spool
instances; `Spool.clear()` (line 444) already fully resets state (`points` filled with -1,
`size = 0`), so a recycled spool is behaviorally identical to a fresh one → deterministic.

- **Correctness/determinism**: fine *if* clear() is always run and ownership is strict.
- **Concurrency**: this is the risk. Writers live on flush, compaction, and streaming threads;
  `releaseBuffers()` returns the spool to the pool, and a use-after-release bug becomes
  cross-writer histogram corruption (silently wrong tombstone estimates) instead of an NPE.
  The existing assert at line 104 shows use-after-release has been worried about before.
  FastThreadLocal reuse is safer but pins 3 MiB per flush/compaction/streaming thread forever
  (compaction executors can be sizable) and still needs the lazy trick to avoid populating
  threads that never see tombstones.
- **Benefit over A**: also removes the churn for tombstone-heavy workloads.
- **Patch size**: ~100-150 lines + lifecycle audit of every writer abort/finish path. Not
  justified until A lands and a tombstone-heavy profile shows the deferred allocation still
  matters.

### Option C: shrink the default spool (e.g. 100,000 → 8,192)

With 60-second rounding (CASSANDRA-13038), 131,072 distinct spool slots cover ~91 days of
*distinct per-minute deletion times in a single sstable* — wildly oversized for typical
sstables. 8,192 slots (≈ 5.7 days of distinct minutes, 192 KiB) would very rarely flush
mid-write.

- **Determinism problem**: a different capacity changes the flush threshold and the hash mask,
  so for sstables with > `maxBinSize`+1 = 101 distinct rounded points the bin overflows at
  different moments with different contents → `mergeNearestPoints` merges different pairs →
  **Statistics.db bytes differ from current code for the same input**. Deterministic for the
  same code version (the algorithm has no randomness), but it breaks old-vs-new byte
  comparison — the differential harness would need "Statistics.db" in its `byteDiffAllowlist`
  with logical-equivalence checking, and the estimate quality for tombstone-heavy sstables
  changes (more merges = coarser histogram, the exact regression CASSANDRA-13038 fixed).
- **Patch size**: 1 line — but it needs a perf bake-off on TTL-heavy workloads and an upstream
  conversation about acceptable estimate drift. Could optionally come with a yaml/property
  knob (there is none today).

### Option D: start small and grow (e.g. 1,024 → ×4 → 131,072)

Rehash into a larger table on overflow instead of flushing to bin; only flush at max size.
Bounds waste at 12 KiB for low-tombstone writers AND keeps full batching for heavy ones.

- **Determinism**: final `flushHistogram()` iterates the spool in array (hash) order
  (`Spool.forEach`, line 474), and array order depends on table size. If the bin never
  overflows the result is order-independent (bin is sorted, values accumulate), but for > 101
  distinct points the merge sequence changes → same byte-diff problem as C versus current
  code, while being deterministic within a code version.
- **Patch size**: ~80 lines (rehash loop, growth policy, tests). More moving parts than A for
  marginally more benefit (A already gets tombstone-free writers to zero; D only improves the
  light-tombstone middle ground).

## Recommended option: A (lazy allocation), with C as an optional follow-up discussion

A is the only option that is simultaneously: byte-identical output (no differential-harness
or upstream-compatibility caveats), trivially safe (no new sharing, no lifecycle changes,
single file), and a complete fix for the pure-waste case that motivated this investigation
(tombstone-free tables; also INSERT-only and most read-mostly workloads). It converts the
per-writer constant from "always 3 MiB" to "3 MiB iff the sstable actually contains
deletions/TTLs", which also makes the allocation-gate assertion meaningful. C and B remain
available as independent follow-ups if a tombstone-heavy profile later shows the deferred
allocation still dominates; they should be separate tickets because they change
output bytes (C) or object lifecycle (B).

## Implementation sketch

All production changes in
`src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java`:

1. Add fields `private final int maxSpoolSize;` and `private boolean buffersReleased;`.
   Constructor (line 80-87): store `maxSpoolSize`, do NOT construct the Spool
   (`this.spool = null`).
2. `update(long point, int value)` (line 102-120): replace the `assert spool != null` canary
   with `assert !buffersReleased`; then
   `if (spool == null && maxSpoolSize > 0) spool = new Spool(maxSpoolSize);`
   The existing `spool.capacity > 0` branch becomes `spool != null` (a `maxSpoolSize == 0`
   builder — used by some tests — never allocates and falls through to `flushValue`, same as
   today's zero-capacity Spool path, which exists per the `maxSpoolSize >= 0` assert).
3. `releaseBuffers()` (line 139-143): set `buffersReleased = true` in addition to
   `spool = null`. `flushHistogram()` (line 125) already handles `spool == null`.
4. No changes to `MetadataCollector`, `SSTableWriter`, or any writer — the laziness is fully
   encapsulated.

Tests:

- `test/unit/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilderTest.java`
  already exists — add: (a) histogram equality between an eagerly-driven builder and the lazy
  one for a randomized point stream (should be trivially true since it's the same class, but
  guards the refactor); (b) zero-update builder `build()` still yields the empty histogram;
  (c) the use-after-release canary still fires.
- `test/unit/org/apache/cassandra/db/compaction/differential/CursorCompactionAllocationGateTest.java`:
  add a **per-writer-constant assertion** — measure thread-allocated bytes across a
  single-output compaction of a tombstone-free table and assert the constant (non-row-scaling)
  component stays under a ceiling well below 3 MiB (e.g. 1 MiB). Today that measurement is
  ≥ 3 MiB purely from the spool; after the patch it drops out. This converts the finding into
  a regression tripwire. (The existing gate only asserts the *delta* between small and big
  tables, which is blind to per-writer constants by design — see the class javadoc.)
- The differential harness (`DifferentialCompactionTester.assertEquivalentOutputs`) must keep
  passing with an EMPTY `byteDiffAllowlist` for Statistics.db — this is the determinism proof.
  Run the full `differential` package plus `StreamingTombstoneHistogramBuilderTest` and
  `MetadataCollectorTest`/sstable metadata tests.
- Perf sanity: `test/microbench/org/apache/cassandra/test/microbench/StreamingTombstoneHistogramBuilderBench.java`
  exists; confirm the added null-check in `update()` is invisible (it's one branch on a path
  that already does a hash probe).

## Pickup notes for a fresh session

- **Branch/context**: branch `cursor-compaction-completion` in this repo; the JFR evidence is
  in `garbage-free-compaction-improvements/jfr-reports/` and the broader plan in
  `garbage-free-compaction-improvements/cursor-compaction-plan.md`. This proposal's sibling
  `keeporiginals-jira-draft.md` shows the intended JIRA style.
- **Entry points**: `MetadataCollector.java:122` (the eager field init);
  `StreamingTombstoneHistogramBuilder.java:80` (ctor), `:102` (update), `:125`
  (flushHistogram), `:139` (releaseBuffers), `:425-436` (Spool ctor — the 2 MiB/1 MiB arrays
  at 433/434); `SSTable.java:71-73` (constants 100 / 100,000 / 60s property
  `cassandra.streaminghistogram.roundseconds`).
- **Size math**: pow2(100,000) = 131,072 capacity; arrays are `capacity * 2` = 262,144
  elements → `long[]` 2 MiB + `int[]` 1 MiB; `clear()` dirties the 2 MiB immediately.
- **Git landmarks**: CASSANDRA-13038 `a5ce963117` (spool concept), CASSANDRA-13444
  `06da35fdda` (current primitive Spool), CASSANDRA-14773 `00fb6d76d0` (long points, split
  arrays), CASSANDRA-14834 `5e8f7f591d` (releaseBuffers — retained-heap half of this problem;
  cite it in the new ticket as precedent that the spool's footprint is a known issue).
  No upstream commit addresses the *allocation* (churn) half; no JIRA reference to it found
  in git history (`git log -i --grep=spool` returns only the above).
- **Determinism constraints**: the differential harness compares compaction outputs
  byte-for-byte including Statistics.db; lazy allocation (A) is byte-identical, capacity
  changes (C/D) are not (merge order in `DataHolder.mergeNearestPoints` depends on spool
  flush timing once > 101 distinct minute-rounded deletion times exist). Any accepted
  solution must stay deterministic within a code version; A and B also preserve cross-version
  bytes.
- **Open questions**: (1) confirm on a TTL-heavy profile whether the deferred 3 MiB still
  shows up enough to justify Option B/C follow-ups; (2) early-open `build()` thread
  confinement is assumed (it runs on the writing thread via the append/rewriter path) —
  re-verify if anything moves `openEarly` off-thread; (3) whether upstream wants a config
  knob for spool size while in there (none exists today).
- **Scoping**: file as a standalone upstream JIRA against trunk (constants and code identical
  in recent branches; verify how far back to offer the patch — the lazy change applies
  cleanly anywhere post-14834). Do NOT fold into the cursor-compaction ticket: the allocation
  happens in shared writer infrastructure (flush + iterator compaction + cursor compaction +
  streaming) and benefits all of them equally.
