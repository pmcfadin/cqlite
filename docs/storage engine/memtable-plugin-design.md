# CEP-11 `cqlite` Memtable Plugin — Design (Spike #1807)

**Date:** 2026-07-03
**Status:** Draft — feeds spike #1807; becomes the OpenSpec design at `flow-activate`.
**Deploy target:** Apache Cassandra **5.0** (CEP-11 shipped in 4.1 — verified on `origin/cassandra-5.0` @ `464b2e54`). Trunk deltas flagged where they matter.
**Supersedes:** the a1 sketch in `report-1-memtable-freshness.md` §2a (this doc carries the researched detail; where the research corrected earlier assumptions, the correction is stated inline).
**Changelog:** 2026-07-03: export trigger revised timer → on-demand + dirty-check (owner decision); a2 live-stream evaluated and deferred.

## Design summary

Analytical reads through Arrow Flight / Trino / (future) DataFusion currently see only flushed SSTables — the node's unflushed memtable tail is invisible. The owner has decided (2026-07-03): **no CDC**; the path is a custom CEP-11 Memtable plugin. The full data path: a Cassandra write lands in **`CqliteMemtable extends TrieMemtable`** (a thin subclass in the plugin jar — inheritance, not wrapping, because two `instanceof AbstractAllocatorMemtable` gates make a composition wrapper invisible to memory-pressure and periodic flush); exports are **on-demand + dirty-checked** — a Flight query for a tail-enabled table that finds the tail stale drops a request marker; the plugin exports only if the memtable is dirty (`operationCount()` advanced since the last export) and a min-interval cap has elapsed (plus `switchOut`/`discard` flush hooks; an optional interval mode remains as fallback) — the exporter iterates the live memtable under a `readOrdering` pin and writes a **real `nb` SSTable** via `getFlushSet` + `SSTableTxnWriter` with an offline dummy-tracker `LifecycleTransaction` (`finish(false)` — never registered with the live set); the export is built in a **staging dir and published by atomic directory rename** into a well-known tail dir, stamped with a commit-log-interval watermark in Statistics.db; on the CQLite side a ~50-LOC **`CompositeSource`** (plus `--tail-dir` on the Flight server) concatenates tail paths with the main data-dir paths and feeds the **existing k-way LWW merge unchanged** — reconcile, tombstones, TTL, token pruning, and aggregation pushdown all work as-is because the tail is just one more `nb` generation. On-demand kills the write amplification of blind cadence (idle tables cost zero; hot tables pay one export per query burst, amortized across queries) at the price of export latency on the first stale read. LWW makes tail/flush overlap harmless; dedup is an efficiency optimization plus one narrow correctness backstop (orphaned exports older than gc_grace).

---

## 1. Goals

- **G1 — Freshness:** a Flight/Trino read merges the unflushed memtable tail of opted-in tables (`WITH memtable='cqlite'`) with flushed SSTables, exported on demand at query time (dirty-checked, min-interval-capped) — idle tables cost zero; the first stale read pays one export latency, subsequent reads in the burst reuse it.
- **G2 — Correctness:** the merged pre-flush result equals the post-flush result for the same data (upserts, row/range tombstones, TTL) under CQLite's existing byte-parity LWW reconcile. This is the spike's primary acceptance criterion.
- **G3 — OLTP safety:** delegation-by-inheritance preserves TrieMemtable's write path, `MemtablePool` accounting, cleaner eligibility, and flush signaling exactly; the exporter runs off the write hot path with characterized (not necessarily minimized) overhead.
- **G4 — Zero merge-engine change:** the tail is a real `nb` SSTable; CQLite's merge/reconcile/pruning code is untouched (~255 LOC total, all in source-enumeration, handshake, and config).
- **G5 — Operational honesty:** torn or missing exports fail closed (request error or stale-but-correct), never wrong data.

## 2. Non-goals

- **CDC** in any form (owner decision 2026-07-03 — rejected for complexity).
- **a2 live FFI surface** (in-JVM Flight / shared memory / per-query row streaming over a socket, no files) — the owner explicitly considered and rejected it **for the spike**; it stays documented as the measured follow-on if handshake+export latency provably cannot meet the SLA. Honest comparison: a2 re-streams the memtable per query with no disk; a1+on-demand serializes once per query burst to disk and serves many queries from that file.
- **Counter tables** — counter cells don't reconcile by pure LWW (Cassandra context-merges shards); duplicated counter shards across tail+flush could misresolve. Counters are excluded from tail merging (`research-cqlite-tail-seam.md` §3).
- **Cluster/RF dedup, repaired-status gating** — the tail dir is per-node; Trino's split-per-replica routing already pins each token range to one node.
- **Upstreaming to ASF** — separate decision.
- **Trunk build** — 5.0 only for the spike; trunk deltas are catalogued (`research-plugin-mechanics.md` "Trunk deltas") but not built against (`createMemtableMetrics` rename, 4-arg `put`, new abstract members are source-breaking).
- **Watermark-dedup productization** — the spike ships the cheap max-age backstop; the precise `commitLogIntervals` ⊇-rule is specced (§6) but deferred.
- Tail tombstone-fidelity ceiling: inherits #844 cell-path/complex-deletion partial shadowing — same ceiling as the SSTable path, no worse; flagged in the spike report.

---

## 3. Architecture

Two processes, one filesystem contract between them: the **tail dir**.

```
┌────────────────────────── Cassandra 5.0 JVM ──────────────────────────┐
│                                                                       │
│  write ──► CommitLog ──► CqliteMemtable (extends TrieMemtable)        │
│                              │        │                               │
│                              │        ├── stock TrieMemtable behavior │
│                              │        │   (put/reads/pool/flush) —    │
│                              │        │   ALL inherited, untouched    │
│                              │        │                               │
│           on-demand trigger: request-<seq> marker (WatchService)      │
│           → dirty-check (operationCount) + min-interval cap           │
│           + switchOut/discard hooks (+ optional interval mode)        │
│                              │                                        │
│                              ▼                                        │
│                    CqliteTailExporter                                 │
│                    readOrdering pin → getFlushSet →                   │
│                    SSTableTxnWriter (offline txn, finish(false))      │
│                              │                                        │
│              <tail_root>/<ks>/<tableId>/.staging-<seq>/               │
│                              │  fsync + ATOMIC_MOVE                   │
│                              ▼                                        │
│              <tail_root>/<ks>/<tableId>/gen-<seq>-.../                │
│                nb-<id>-big-Data.db … TOC.txt, export-manifest.json    │
└──────────────▲────────────────┬───────────────────────────────────────┘
               │ request-<seq>  │  tail dir contract:
               │ marker file    │  • only complete gen-* dirs are visible
               │ (handshake §4.4)  • full nb component set inside
               │                │  • watermark in Statistics.db + manifest
               │                ▼
┌──────────────┴─────────── CQLite process ─────────────────────────────┐
│                                                                       │
│  Flight do_get ──► tail stale? ──► write request marker,              │
│                      bounded wait for new gen-* (else serve stale)    │
│                              │                                        │
│                              ▼                                        │
│                    CompositeSource(SstableSource)                     │
│                      = tail gen-* Data.db paths (first)               │
│                      + DirSource(data_dir) paths                      │
│                              │                                        │
│                              ▼                                        │
│              MergeProducer → KWayMerger (UNCHANGED)                   │
│              7-step LWW/tombstone reconcile (UNCHANGED)               │
│                              │                                        │
│                              ▼                                        │
│              Arrow batches ──► Flight ──► Trino / DataFusion          │
└───────────────────────────────────────────────────────────────────────┘
```

The tail dir is the entire integration surface — in both directions: exports flow Cassandra→CQLite as `gen-*` dirs, export requests flow CQLite→Cassandra as `request-<seq>` marker files (§4.4). The Cassandra side promises: atomically-published, complete `nb` SSTables with authoritative Statistics.db timestamps and a watermark; superseded/dead exports garbage-collected; a bounded-effort response to request markers (export only if dirty). The CQLite side promises: treat tail files as ordinary generations; never parse partial state (`.staging-*` and txn-log-bearing dirs are invisible by protocol); fail closed on races; serve stale-but-correct on handshake timeout.

---

## 4. Cassandra-side design

### 4.1 `CqliteMemtable` — extend, don't wrap (correction to #1807's wording)

Issue #1807 says "wrap-and-delegate." The researched answer is **extend `TrieMemtable`** — same intent (never reimplement write/read/flush/pool behavior; delegate everything), but delegation happens via inheritance, because composition is fatally broken by two `instanceof` gates that cannot be fixed from outside (`research-plugin-mechanics.md` §2, §7):

1. **Memory-pressure flush selection skips wrappers.** The pool's cleaner, `AbstractAllocatorMemtable.flushLargestMemtable()` (`AbstractAllocatorMemtable.java:249-318`), iterates each CFS's *current* memtable (the wrapper) and at `:260-261` does:
   ```java
   if (!(currentMemtable instanceof AbstractAllocatorMemtable)) continue;
   ```
   A composed wrapper is invisible to the cleaner even though its inner allocator's memory counts against the global pool → if the cqlite table is the largest consumer, the cleaner flushes the wrong tables or nothing, and writes stall on pool backpressure with no relief.
2. **Periodic flush (`memtable_flush_period_in_ms`) is also instanceof-gated**: the scheduled task only acts `if (current instanceof AbstractAllocatorMemtable)` (`AbstractAllocatorMemtable.java:218-220`).

Composition additionally requires forwarding ~30 methods, a delegating `Owner` (to fix the flush-signal reference-identity mismatch at `ColumnFamilyStore.java:1014-1022`), and still loses both gates. Extension gets pool registration, cleaner eligibility, flush signaling, commit-log bookkeeping, and all TrieMemtable machinery for free (`research-plugin-mechanics.md` §2 "What extension gets for free").

**Consequence:** TrieMemtable's constructor is package-private (`TrieMemtable.java:123`), so the subclass must live in **package `org.apache.cassandra.db.memtable` inside the plugin jar** (legal split package on 5.0's flat classpath; keep only the thin subclass + factory there, all export logic in a `cqlite.*` package — risk #1, `research-plugin-mechanics.md`).

```java
package org.apache.cassandra.db.memtable;              // required: pkg-private super ctor

public class CqliteMemtable extends TrieMemtable {
    private final CqliteTailExporter exporter;          // lives in cqlite.* package

    CqliteMemtable(AtomicReference<CommitLogPosition> clb, TableMetadataRef ref,
                   Owner owner, Integer shards, CqliteExportConfig cfg) {
        super(clb, ref, owner, shards);
        this.exporter = CqliteTailExporter.register(this, owner, cfg);
    }

    @Override
    public boolean shouldSwitch(FlushReason reason) {
        if (reason == FlushReason.SNAPSHOT)
            return false;                               // NEEDS-DECISION (a) — see §10
        return super.shouldSwitch(reason);
    }

    @Override
    public void performSnapshot(String name) { exporter.exportNow(name); }

    @Override
    public void switchOut(OpOrder.Barrier barrier, AtomicReference<CommitLogPosition> upper) {
        super.switchOut(barrier, upper);
        exporter.onSwitchOut();                          // schedule final export cut; NEVER block here
    }

    @Override
    public void discard() {
        super.discard();
        exporter.onDiscard();                            // memtable flushed & live → retire its exports
    }

    public static Memtable.Factory factory(Map<String, String> options) {
        CqliteExportConfig cfg = CqliteExportConfig.consume(options); // map.remove() each key we own
        // remaining keys (e.g. "shards") forwarded to TrieMemtable's option handling;
        // MemtableParams throws ConfigurationException if anything is left unconsumed
        // (MemtableParams.java:246-248)
        return new CqliteFactory(cfg, options);          // implements equals/hashCode (schema-change compare)
    }
}
```

Points held by the research, not by hope:

- All overridden methods are public, non-final, and defined in the hierarchy: `shouldSwitch` (`AbstractAllocatorMemtable.java:131`), `performSnapshot` (`:157` — default throws `AssertionError`), `switchOut` (`:162`), `discard` (`TrieMemtable.java:161`), `getFlushSet` (`TrieMemtable.java:350`) (`research-plugin-mechanics.md` §2).
- `AbstractAllocatorMemtable.initialFactory` is read from `metadata().params.memtable.factory()` at construction (`:120`) — i.e. **our** factory — so `shouldSwitch(SCHEMA_CHANGE)` comparisons remain correct without overriding; the factory must implement `equals`/`hashCode` (as `TrieMemtable.Factory` does, `TrieMemtable.java:681-698`).
- `performSnapshot(name)` is invoked only when the memtable returns `false` from `shouldSwitch(SNAPSHOT)` — live, current, pre-flush, writes still flowing (`ColumnFamilyStore.java:2375-2390`); for stock memtables it is never called. There is no `TakeSnapshotTask` on 5.0 (trunk-only) — cite the CFS path.

### 4.2 Factory, yaml, DDL

Reflection contract (`MemtableParams.getMemtableFactory`, `schema/MemtableParams.java:217-257`): `Class.forName` → `factory(Map)` invoked with a mutable copy of parameters; the method must `map.remove(...)` every option it consumes; leftovers → `ConfigurationException`. Instantiation is lazy — a bogus yaml entry fails at first table reference, not node startup (`MemtableParams.java:111-120`).

```yaml
# cassandra.yaml — keep the stock entries; 'default' must remain resolvable
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
        export_dir: /var/lib/cassandra/cqlite-tail
        export_mode: on_demand              # on_demand (default) | interval
        min_export_interval_ms: "250"       # dirty-check rate cap — NEEDS-DECISION (b)
        export_interval_ms: "1000"          # interval mode only (fallback)
        shards: "..."                       # forwarded to TrieMemtable
```

Deployment: drop the jar in `$CASSANDRA_HOME/lib/` (`bin/cassandra.in.sh:53-55` classpath-globs it; no ServiceLoader, pure reflection). Opt-in per table: `ALTER TABLE t WITH memtable = 'cqlite';` reset with `'default'`. Schema stores only the configuration **key**; class/parameters are node-local yaml — heterogeneous rollout supported by design. **No guardrail gates any of this** (`research-plugin-mechanics.md` §6).

**Silent-fallback hazard:** a node whose yaml lacks the config or whose lib lacks the jar logs an error and **silently runs SkipList** for the table (`MemtableParams.getWithFallback`, `SchemaKeyspace.java:1070`) — the node serves normally, just without exports. CQLite must treat "no exports appearing for an opted-in table" as a deployment fault, not an empty memtable (§7).

### 4.3 Exporter

**Trigger — on-demand + dirty-check (owner decision 2026-07-03; supersedes the earlier timer-primary design).** Blind periodic export has real write amplification: each export rewrites the entire live memtable to date (flush-equivalent serialization, `research-export-format.md` §7), so at interval T over memtable lifetime L the extra bytes are ~½ × (L/T) × final-size — **~300× at 1s cadence over a 10-minute memtable lifetime**. On-demand kills it: idle tables cost zero; hot tables pay one export per query burst, amortized across the queries in the burst. The trigger chain:

1. **Query arrival (CQLite side):** on a Flight query for a tail-enabled table, CQLite checks tail staleness (§4.4 staleness rule) and, if stale, requests an export via the handshake (§4.4), then waits — bounded — for a new `gen-*` dir.
2. **Dirty-check (plugin side):** on an export request the plugin exports **only if** the memtable is dirty — `operationCount()` (`Memtable.java:213`) has advanced since the last export — **and** the min-interval cap (`min_export_interval_ms`) has elapsed. A clean memtable or a too-recent export answers the request by (re)publishing nothing; the existing newest `gen-*` already satisfies it (the plugin touches its manifest/ack so the requester's wait can complete).
3. **Interval fallback mode:** `export_mode: interval` retains the original timer design for deployments that prefer push cadence — the timer copies the upstream pattern exactly: `ScheduledExecutors.scheduledTasks.scheduleSelfRecurring(...)`, deliberately capturing the **`Owner`** (the CFS) rather than the memtable and re-resolving `owner.getCurrentMemtable()` each tick to avoid pinning a dead memtable (`AbstractAllocatorMemtable.scheduleFlush`, `AbstractAllocatorMemtable.java:204-224`). Default is `on_demand`. The dirty-check + min-interval cap applies in both modes.

`performSnapshot` fires only on explicit snapshots (§4.1, NEEDS-DECISION (a)). Flush hooks are **unchanged** by the trigger revision: `switchOut` marks the export cut point (nothing new arrives after the write barrier issues) and hands finalization to the exporter executor — **never block inside `switchOut`**, it runs under the flush path; `discard` means the flushed sstable is live on disk → this memtable's exports are retirable (§4.5 GC).

**Query-latency implication — stated plainly:** the first stale read pays the export latency, which is bounded by memtable size (flush-equivalent serialization plus `getFlushSet`'s O(partitions) pre-pass, `TrieMemtable.java:353-366`) plus handshake latency. Subsequent reads in the burst (within the staleness window / min-interval cap) serve the already-published export at zero marginal cost. This trade is measured explicitly in the spike (§8 export-latency + amortization bench).

**Iteration safety — the readOrdering pin.** After a real flush, `Flush.reclaim` issues a `readOrdering.newBarrier()`, awaits it, then `memtable.discard()` frees the trie's off-heap buffers (`ColumnFamilyStore.java:1391-1405`; `TrieMemtable.discard():161-181`). Iterating without a read-ordering group risks reading freed memory. The exporter therefore runs its whole iteration inside:

```java
try (OpOrder.Group op = cfs.readOrdering.start()) {   // ColumnFamilyStore.java:305, pattern at :2042
    ... getFlushSet + write ...
}
```

`Owner` *is* the CFS on 5.0 but doesn't expose `readOrdering`; casting `owner` to `ColumnFamilyStore` is an accepted implementation coupling (risk #3, `research-plugin-mechanics.md`). Consequence: a slow export delays off-heap reclaim of a concurrently-flushed memtable — bound export time, or chunk by token range (`getFlushSet(from, to)` supports ranges natively) (§7).

**Header safety — the documented live-iteration race.** `FlushablePartitionSet`'s own javadoc warns that a still-written memtable may violate collected encoding stats / column sets (`Memtable.java:308-311`). Mitigation: build the `SerializationHeader` with **`EncodingStats.NO_STATS`** (epoch-based, always safe — `EncodingStats.java:69`) and the table's **full `regularAndStaticColumns()`** instead of `flushSet.columns()`/`flushSet.encodingStats()`. Cost: slightly larger vint deltas. Statistics.db min/max timestamps are unaffected — they're collected from actual cells during `append` (`SortedTableWriter.java:195,214,227-232`; `MetadataCollector.java:222-262`), so the export gets authoritative timestamps for free (`research-export-format.md` §1.2, §1.4).

**The writer recipe** (all in-tree 5.0 APIs; this is the flush serialization path with a tool-proven offline transaction — the same dummy-tracker mechanism `CQLSSTableWriter`, streaming, and scrub use):

```java
// Inside readOrdering pin. All anchors: cassandra-5.0 @ 464b2e54.
CommitLogPosition pStart = CommitLog.instance.getCurrentPosition();      // watermark, BEFORE getFlushSet

FlushablePartitionSet fs = memtable.getFlushSet(minBound, maxBound);     // Memtable.java:303; live view,
                                                                          // does NOT require switchOut
                                                                          // (TrieMemtable.java:350-404)
SerializationHeader header = new SerializationHeader(
    true, metadata,
    metadata.regularAndStaticColumns(),                                   // FULL columns, not fs.columns()
    EncodingStats.NO_STATS, false);                                       // not fs.encodingStats()

Descriptor desc = cfs.newSSTableDescriptor(stagingDir, BigFormat);        // CFS.java:975-995; any dir accepted;
                                                                          // id from the CFS's own generator
                                                                          // — do NOT mint ids independently

// commitLogUpperBound is null/undefined pre-switchOut, so the exporter stamps its own interval
// via the 8-arg createSSTableMultiWriter overload (CFS.java:665-672) rather than
// Flushing.createFlushWriter:
//   IntervalSet<>(memtable.getCommitLogLowerBound(), pStart)             // Memtable.java:373-375

SSTableTxnWriter w = SSTableTxnWriter.create(cfs, desc, fs.partitionCount(),
                                             UNREPAIRED, null, false, header);
// SSTableTxnWriter.create → LifecycleTransaction.offline(OperationType.WRITE)
//   → Tracker.newDummyTracker() — zero live-set impact (SSTableTxnWriter.java:111-116;
//     LifecycleTransaction.java:176-180)

for (Partition p : fs)
    try (UnfilteredRowIterator it = p.unfilteredIterator()) { w.append(it); }

w.finish(false);   // openResult=false — never open a reader, never touch the live set
                   // (finish(true) would open a reader: SSTableTxnWriter.java:103-108)
```

Notes held by the research: `getFlushSet` on the live memtable never calls `setFlushTransaction`, so a concurrent real flush is neither blocked nor double-flush-tripped (`Flushing.java:61-63`); iteration doesn't block writes (trie reads are lock-free vs single-writer-locked shards); the component set follows the writer builders — `DATA, STATS, DIGEST, TOC, FILTER` + BIG's `PRIMARY_INDEX, SUMMARY` (`SortedTableWriter.java:487`; `BigTableWriter.java:368-372`) — and CQLite needs the **full set** (Summary.db for token pruning, Statistics.db for open-time checks + watermark); don't suppress components (`research-export-format.md` §1.3).

**Rejected alternatives** (`research-export-format.md` §§1.6, 2, 3): `CQLSSTableWriter` mutates global schema on a live node and expects CQL-statement input — it's a bulk-load tool; Arrow IPC means a ~10–15 MB shaded dependency stack, `--add-opens` JVM flags, a brand-new CQLite tail reader, and reinventing the watermark metadata Statistics.db gives us free; the on-wire `UnfilteredRowIteratorSerializer` format is MessagingService-versioned with no cross-release stability. Real-SSTable export strictly dominates.

### 4.4 The export-request handshake (CQLite → plugin "export now")

New design surface introduced by the on-demand decision (not in the research docs — design addition, to be validated by the spike). Two candidate mechanisms:

- **(a) Filesystem handshake (RECOMMENDED for the spike).** CQLite writes an empty marker file `request-<seq>` (seq = requester-monotonic; content optional) into `<tail_root>/<ks>/<tableId>/`. The plugin watches its per-table export dirs with `java.nio.file.WatchService` (falling back to a coarse poll where native watching is unreliable) and treats any new `request-*` as an export request, subject to the §4.3 dirty-check + min-interval cap; it deletes consumed markers (and sweeps stale ones at startup). CQLite then waits for a `gen-*` dir newer than its staleness observation, or a bounded timeout. Properties: **no ports, no auth surface, no new network exposure** — the same fail-closed filesystem contract as the rest of the tail protocol; latency is WatchService delivery (~10s of ms) + export time. The marker path goes through the same `pathsafe` containment as everything else in the tail root.
- **(b) Local listener in the plugin (upgrade path).** A localhost-only socket (or Unix domain socket) in the plugin accepting "export <tableId>" and answering when the `gen-*` is published. Lower and more predictable latency (no watch delivery jitter), request/ack semantics for free — but more moving parts: a listening endpoint inside the Cassandra JVM, port/permission management, lifecycle on restart. Adopt only if the spike measures the filesystem handshake's latency as a real problem.

**Staleness rule (CQLite side):** the tail for a table is *fresh* iff the newest `gen-*` dir's watermark (manifest `watermark`/`wallClock`; mtime as fallback) is younger than the freshness window (default: the min-interval cap — there is no point requesting more often than the plugin will export). Fresh tail → serve immediately, no handshake. Stale or absent tail → write the request marker and wait.

**Serve-stale-on-timeout posture:** the wait is bounded (`tail_wait_timeout`, strawman 2s — NEEDS-DECISION (b)). On timeout, CQLite **proceeds with the existing files + stale tail** rather than failing the query — freshness degrades, correctness never does (a stale tail is exactly the LWW-harmless overlap case of §6; an absent tail is today's flushed-only behavior). Timeouts are counted and alerted on (a persistently unanswered marker is the jar-missing/plugin-dead signal, §7).

### 4.5 Export protocol — atomic publication, naming, GC

Cassandra writes data files at final names (no tmp+rename) and gets crash-atomicity from its txn-log protocol (`LogFile.java:65-67,144,196,343-345`) — CQLite must not parse txn logs, so atomicity is layered on top (`research-export-format.md` §4):

1. Write the SSTable into a **staging subdir** `<export_root>/<ks>/<tableId>/.staging-<seq>/` via the §4.3 recipe; the `<ver>_txn_write_<uuid>.log` lives and dies inside staging.
2. fsync the staging dir; `Files.move(staging, final, ATOMIC_MOVE)` → `gen-<seq>-.../`; fsync the parent. Same-filesystem directory rename is atomic — a reader sees a complete generation dir or nothing.
3. Reader contract: descend only into `gen-*` dirs; optionally verify `Digest.crc32`.

**Naming + manifest** (dir name carries cheap-discovery metadata; the sstable inside keeps its native `nb-<id>-big-*` name):

```
<export_root>/<keyspace>/<tableId>/                 # TableMetadata.id (UUID) — survives DROP/recreate
  gen-<seq10>-clb-<segId>,<pos>-wm-<segId>,<pos>-epoch-<memtableLowerBoundMicros>/
    nb-<id>-big-Data.db … TOC.txt
    export-manifest.json   # {tableId, schemaVersion, seq, commitLogLowerBound, watermark=P_start, wallClock}
```

`seq` = exporter-owned monotonic sequence per table. The manifest/dir-name watermark copies are for discovery without parsing Statistics.db; **Statistics.db `commitLogIntervals` is the authoritative copy**.

**GC rules:**
- *Superseded:* a new `gen-N` from the same memtable epoch supersedes `gen-(N-1)` — delete the older one after publishing the newer (each export is a full rewrite of the memtable to date, so the newest per epoch is sufficient).
- *Flushed:* on `discard()` (flushed sstable is live), all of that memtable's exports are garbage — delete them. Independently observable: a flushed sstable whose Statistics.db `commitLogIntervals` covers the export's interval supersedes it (`Flushing.createFlushWriter` stamps `IntervalSet(lower, upper)`, `Flushing.java:203-221`; stored at `StatsMetadata.java:64`).
- *Crash-sweep:* on plugin startup, delete all `.staging-*` dirs (Cassandra's own `removeUnfinishedLeftovers` only runs for real data dirs — risk #7, `research-export-format.md`).

### 4.6 Watermark stamping

Capture `P_start = CommitLog.instance.getCurrentPosition()` **before** `getFlushSet`. Every write with position < `P_start` destined to this memtable is already in the trie, so the export contains all of them; iteration may additionally include newer items — harmless under LWW (§6). Stamp **`IntervalSet(memtable.getCommitLogLowerBound(), P_start)`** into Statistics.db via the writer (§4.3). Semantics are deliberately **fuzzy-upper**: any live export (on-demand or interval) has no exact commit-log cutoff (only switch-time bounds are exact — `AbstractMemtableWithCommitlog.accepts`, `:69-109`); treat the interval as "contains at least everything < P_start, possibly more" (`research-plugin-mechanics.md` §3, §5). Exact-cutoff semantics exist only for the final `switchOut`-hook export.

---

## 5. CQLite-side design

The tail needs **zero merge changes** — `KWayMerger::new(paths, schema)` opens one iterator per path with no shared-directory assumption (`merge/mod.rs:1933`); generations from N directories are indistinguishable from N generations of one directory. LWW/tombstone reconciliation is the existing 7-step pipeline (`merge/reconcile.rs`, tie-break `reconcile_rules::cell_wins`). The full inventory (`research-cqlite-tail-seam.md` §2, "Minimal-change inventory"):

| # | Change | File(s) | Size |
|---|--------|---------|------|
| 1 | `CompositeSource` impl of `SstableSource` (tail `gen-*` paths first, then `DirSource` paths) | new small file in `cqlite-flight/src/` (trait at `producer.rs:80-83`) | ~50 LOC |
| 2 | `--tail-dir` CLI arg + service field + wire into `do_get_inner`/`table_stats` | `main.rs:19-31`, `service.rs:109,116,375,443` | ~30 LOC |
| 3 | Pathsafe containment for the tail root (own containment base) | reuse `pathsafe.rs:90` | ~5 LOC |
| 4 | (Optional) `include_tail: bool` ticket opt-in | `ticket.rs:225` | ~10 LOC |
| 5 | (Backstop, later) decode + expose `commitLogIntervals` (currently skip-parsed) | `parser/repair_metadata.rs:621-633,740-747` → `statistics_reader.rs` | ~80 LOC |
| 6 | (Backstop, now) max-age tail prune in CompositeSource | item 1's file | ~40 LOC |
| 7 | Parity tests | new `cqlite-flight/src/tail.rs` test module reusing `testutil.rs` | ~150 LOC |
| 8 | Export-request handshake: staleness check + `request-<seq>` marker write + bounded wait in `do_get_inner` (new design surface per the on-demand decision — not in the research inventory) | item 1's file / `service.rs` | ~40 LOC |
| — | **Merge/reconcile/LWW/tombstone/TTL/aggregation code** | `merge/mod.rs`, `merge/reconcile.rs`, `reconcile_rules.rs` | **ZERO changes** |

Total: **~255 LOC** of new CQLite code for the spike — the research inventory's ~215 (items 1–4, 6, 7; item 5 deferred per NEEDS-DECISION (c)) plus ~40 for the on-demand handshake (item 8).

**Handshake behavior in `do_get_inner`:** before building the composite source, apply the §4.4 staleness rule (newest `gen-*` watermark/mtime vs the freshness window); if stale, write the request marker and wait up to `tail_wait_timeout` for a newer `gen-*`; on timeout, proceed with what exists (serve-stale, never fail the query on freshness). The wait runs inside the existing `spawn_blocking` request context (`service.rs:386`).

**Ordering:** the composite lists tail paths first (= newest) for deterministic equal-timestamp+equal-liveness tie-break (first-seen wins in run order). A tail export named `nb-1-…` would otherwise sort below main generations in `data_paths`' filename-generation sort (`producer.rs:203,210-215`) — identical bytes in the duplicate case, so no wrong answers either way, but determinism is free.

**Pathsafe:** the tail root goes through `pathsafe::assert_within` with itself as containment base, matching the existing symlink-escape posture on `DirSource` (`producer.rs:135,186-198`, issue #1430).

**Freshness model:** the Flight surface is a fresh directory listing per request (`service.rs:375` — `DirSource::resolve` per `do_get`); tail churn between requests is invisible, and a within-request race fails closed (§7).

**Library surface / future DataFusion provider:** `SSTableManager::new_from_discovered_paths` already accepts multiple table dirs, and `DiscoverySource::TableDirs(Vec<PathBuf>)` records them for `refresh()` (`sstable/mod.rs:344`, `refresh.rs:67-74`) — the tail dir becomes one more discovery root and `refresh()` handles export churn natively (removed exports drop, new exports add, atomically, per the #1749 contract). No DataFusion code exists in the repo today; this seam is where a `Database`-backed provider would pick up the tail with zero additional design.

---

## 6. Correctness analysis

**Why tail/flush overlap is harmless (the design-simplifying result, `research-cqlite-tail-seam.md` §3):** the flushed generation contains a superset of the tail export's cells at ≥ timestamps — flush persists the same memtable the export snapshotted, plus later writes, **with identical timestamps** (memtable snapshot; nothing rewrites timestamps).

- **Upserts:** LWW (`cell_wins`) picks the identical-or-newer cell; equal-ts + equal-liveness keeps first-seen with identical bytes. Result ≡ flushed-only. Double-counting is impossible: dedup is per `(partition, clustering, column, cell_path)` in reconcile, and aggregation pushdown consumes post-reconcile rows (`producer.rs:478-517`).
- **Tombstones:** a tombstone in the tail is also in the flush; equal-ts tombstone-beats-live protects the delete; a newer flushed tombstone shadows stale tail live cells (reconcile steps 3/4).
- **TTL:** expiry is evaluated per-cell from `localDeletionTime` — identical cells expire identically. (#1382's expire-then-purge concern is compaction-path only.)
- **No purge in Flight reads:** `KWayMerger::new` passes `gc_before_secs = None`, `purge_safe = false` (`merge/mod.rs:1053-1063` region) — the read merge can never itself resurrect.

**The ONE exception — orphaned export older than gc_grace.** Cassandra's compaction purge (`maxPurgeableTimestamp`) considers only SSTables *it* knows about; the tail dir is invisible to it. If an export containing live row X (ts=100) is orphaned (plugin crash) and, ≥ gc_grace (default ~10 days) later, Cassandra compacts away both a covering tombstone (ts=200) and X, a merge of {compacted gens + orphaned tail} **resurrects X**. For a properly-churned tail (retired at each flush, lifetime ≪ gc_grace), watermark dedup is purely an efficiency optimization; the backstop exists specifically for the orphan case.

**Chosen backstop (spike): max-age prune.** The `CompositeSource` ignores tail exports whose manifest wallClock/mtime is older than a small TTL (minutes) — ~40 LOC, no new parsing. **Precise later option:** promote the `commitLogIntervals` skip-parse to decode+expose (`repair_metadata.rs:621-633,740-747`; format known, version-gated at `version_gate/big.rs:22-26` / `bti.rs:18-19,64-65`; ~80 LOC) and drop any tail path whose interval is ⊇-covered by a flushed generation's interval — a pre-merge file-level prune exactly analogous to the existing token prune (`producer.rs:389-412`). NEEDS-DECISION (c).

**Counters:** excluded (§2). **Watermark fuzziness:** live exports carry a may-contain-more upper bound (§4.6); this is fine because extra newer cells are exactly the overlap case LWW already handles — the watermark's only correctness job is the orphan backstop, where ⊇-coverage of the *lower* portion is what matters.

**Tombstone-fidelity ceiling:** #844 (cell-path/complex-deletion partial shadowing) applies to the tail exactly as it applies to flushed SSTables — no new gap, flagged in the spike report.

---

## 7. Failure modes

| Failure | Behavior | Detection / recovery |
|---|---|---|
| Plugin crashes mid-export | Torn state confined to `.staging-<seq>/` — never visible to CQLite (ATOMIC_MOVE publication) | Exporter startup sweep deletes all `.staging-*` dirs (§4.5) |
| Jar missing / yaml config absent on a node | Cassandra logs an error and **silently runs SkipList** for the table (`MemtableParams.getWithFallback`, `SchemaKeyspace.java:1070`); node serves; request markers go unanswered, no exports ever appear | CQLite side: alert on persistently-unanswered request markers / repeated handshake timeouts for an opted-in table — deployment fault, not empty memtable |
| Slow export | `readOrdering` pin delays off-heap reclaim of a concurrently-flushed memtable (`ColumnFamilyStore.java:1391-1405`) — memory held, not corrupted | Bound export time; chunk by token range (`getFlushSet(from,to)`); export duration metric + alert |
| Torn read on Flight (tail file deleted between list and open) | `KWayMerger::new` open error → `ProducerError::Merge` → gRPC error — **fail-closed, no partial results**; file unlinked *after* open is safe (fd held, POSIX) | Trino retries the split or the query fails cleanly; if error rate matters, adopt snapshot-style hardlink sets for the tail dir |
| Node restart | Memtable rebuilt from commit log; stale exports on disk describe pre-restart state — same data the replay reproduces, so LWW-harmless; superseded on the first new export | Max-age prune caps the stale window; startup crash-sweep clears staging |
| Export contains rows newer than watermark | By design (fuzzy upper, §4.6) | Harmless — LWW overlap case |
| Handshake timeout (plugin slow, export bigger than the wait) | CQLite proceeds with existing files + stale tail (§4.4) — **freshness degrades, correctness never does** | Timeout counter + alert; tune `tail_wait_timeout` / min-interval; escalate to the §4.4(b) local listener if latency is structural |
| Stale/leftover `request-*` markers (CQLite crash mid-handshake) | At worst one spurious dirty-checked export | Plugin deletes consumed markers and sweeps stale ones at startup |
| Orphaned export survives past gc_grace | Potential resurrection (the ONE correctness exception, §6) | Max-age prune (spike); `commitLogIntervals` ⊇-rule (later) |
| Exporter memory/IO pressure | Unaccounted exporter heap distorts pool math | Keep buffers bounded; report significant exporter heap via `markExtraOnHeapUsed`; never block in `switchOut` |

---

## 8. Spike plan — mapping #1807 acceptance criteria to evidence

| #1807 criterion | Concrete check |
|---|---|
| **Parity** — pre-flush merged read ≡ post-flush read on mixed upsert + row/range-tombstone workload, byte-equal under the parity comparator | **In-crate:** new `cqlite-flight/src/tail.rs` test module (NOT appended to `producer.rs` — 2,080 lines, over the campsite ratchet) reusing `testutil.rs::build_sstables`: batch A into `main_dir` (the "flushed" state), batch A′ = subset/equal rows with same timestamps into `tail_dir` (simulated export); assert `produce(composite{main+tail})` ≡ `produce(main only)`, including a tombstone case and a TTL case; plus a churn case (delete tail file mid-sequence, re-request, clean result). Run via `cargo test --package cqlite-flight --lib` (CI lane `flight-ci.yml:70`). **E2E:** extend `trino-connector/docker/e2e-test.sh` (compose: Cassandra + Sidecar + flight server + Trino) with a stage that creates a table `WITH memtable='cqlite'`, writes the mixed workload, queries via Trino pre-flush with `--tail-dir` — the query itself drives the on-demand handshake (marker → export → serve), so this stage is also the handshake's wiring evidence — then `nodetool flush` and re-queries — assert equal result sets. In-crate additions for the handshake: a bounded-wait test (marker written, no responder → serve-stale within timeout) alongside the parity cases. Helper-only unit tests do NOT satisfy this criterion (issue wiring-evidence note). |
| **Write-path overhead** — wrapped vs stock TrieMemtable under identical load | JMH-or-equivalent bench in the plugin repo: identical insert workload against `memtable='trie'` vs `memtable='cqlite'` (exporter idle — on-demand with no requests — and under a query-driven request load), report throughput + p99 deltas. Expected shape: near-zero on the write path itself (the subclass adds no per-write code; on-demand means an idle table's exporter does nothing); the per-export cost is background CPU/IO ≈ flush-equivalent serialization (`research-export-format.md` §7), now paid once per query burst instead of per timer tick. Report both numbers. |
| **(Revised) export-latency + amortization bench** — the cadence-curve bench is replaced per the on-demand decision | Measure (1) end-to-end first-stale-read latency (request marker → new `gen-*` visible → query served) vs memtable size — this is the number the on-demand trade hinges on; (2) amortization: N queries in a burst against one export, confirming marginal cost ≈ zero within the freshness window; (3) handshake-only latency (WatchService delivery + dirty-check short-circuit on a clean memtable) to decide whether §4.4(b)'s local listener is ever needed. |
| **Pool-accounting safety** — normal flush cadence, no starvation/OOM under sustained load | Soak: sustained write load on a cqlite-memtable table sized to trigger pool-pressure flushes; assert flushes fire at normal cadence (the extend design makes the memtable a first-class `AbstractAllocatorMemtable` — this test proves the inheritance claim end-to-end), no `MemtableCleaner` stalls, no OOM; monitor pinned-memory during overlapping export+flush. |
| **Spike report** — go/no-go, export cadence achieved, watermark shape | Committed under `docs/storage engine/` after the above; includes the export-latency-vs-memtable-size curve, the achieved on-demand freshness (request → visible), and the stamped `IntervalSet(lower, P_start)` watermark description. |
| **(Added) upstream-untested risk** — `getFlushSet` on a never-switched memtable under concurrent writes | No upstream test exercises this (flush always calls it post-`switchOut`; the live-view mechanics hold by construction — subtrie over live trie — but unverified under mutation). Dedicated stress test in the plugin: continuous writes + concurrent repeated `getFlushSet`-iterate-serialize loops; assert no exception, no torn partition, exported content ⊇ everything written before each `P_start`. This is the spike's highest-value de-risking test (`research-export-format.md` risk #1). |

**Repo placement (proposed, NEEDS-DECISION (e)):** `integrations/cassandra-memtable-plugin/` — Java, own Gradle/Maven build, excluded from the cargo workspace and from `agent-gate.sh` Rust components; keeps 1:1:1:1 and the evidence harness next to CQLite test data. Alternative: separate scratch repo.

---

## 9. Effort and phasing

**Spike (effort M, matching report-1's a1 estimate — now de-risked by research):**
- Java: `CqliteMemtable` + factory (~150 LOC), exporter (request-marker watcher + dirty-check + flush hooks + optional interval mode + writer recipe + staging protocol + GC, ~450–650 LOC), config plumbing, benches + stress test.
- Rust: ~255 LOC (§5 items 1–4, 6–8).
- Evidence: the §8 table.

**Productization deltas (post-spike, only on go):**
1. `commitLogIntervals` decode+expose + ⊇-prune (~80+40 LOC) if the max-age backstop proves insufficient (NEEDS-DECISION (c) revisit with spike data).
2. Chunked/token-range exports if the readOrdering-pin bound bites at production memtable sizes.
3. Snapshot-integration posture per NEEDS-DECISION (a).
4. Library-surface wiring (`DiscoverySource::TableDirs` tail root) for the DataFusion provider when that lands.
5. Trunk forward-port (source-breaking deltas catalogued: `createMemtableMetricsReleaser`, 4-arg `put`, `getMemtableId`/`notifyFlushed`/`ensureFlushListener` — the last is potentially *useful* to the exporter); per-major-version plugin builds.
6. Operational surface: export-lag/export-failure metrics, the no-exports-appearing alert, docs.

---

## 10. NEEDS-DECISION (owner calls)

- **(a) Snapshot semantics.** Overriding `shouldSwitch(SNAPSHOT) → false` (so `performSnapshot` fires and the export path handles snapshots) means **snapshots no longer contain memtable data as SSTables** — restore tooling sees less data unless it also consumes the tail export. Alternative: keep stock flush-on-snapshot behavior (don't override; rely solely on on-demand + flush hooks) — snapshots stay complete, we lose the sanctioned snapshot hook. Recommendation: keep stock behavior for the spike (on-demand-only); revisit if a snapshot-driven export is ever wanted.
- **(b) On-demand tuning defaults.** The cadence question is **resolved** by the on-demand + dirty-check decision (blind periodic export and its ~½ × L/T × memtable-size amplification are gone; each export still rewrites the whole live memtable, but only when a query finds the tail stale AND the memtable is dirty). What remains: the **min-interval cap** default (`min_export_interval_ms`, strawman **250ms** — the floor between exports under a query storm) and the **bounded-wait timeout** default (`tail_wait_timeout`, strawman **2s** — how long a stale read waits before serving stale). Confirm the strawmen or direct the spike to sweep them in the export-latency bench.
- **(c) Backstop now vs precise.** Ship the spike with the cheap max-age tail prune (minutes TTL, ~40 LOC, no new parsing) and defer the precise `commitLogIntervals` ⊇-rule (~80 LOC decode in `repair_metadata.rs` + ~40 LOC prune) — or pull the precise rule into the spike. Recommendation: max-age now; the orphan window it leaves (an export that is both orphaned AND younger than the TTL) is closed by GC-on-discard in all non-crash paths.
- **(d) SSTable identity.** Export descriptors drawn from the CFS's own `sstableIdGenerator` (collision-safe, but export generations consume ids visible in future flush filenames — cosmetic) vs enabling `uuid_sstable_identifiers_enabled: true` on the node (clean separation, but a node-wide setting change). Recommendation: CFS sequence for the spike; revisit only if the id interleaving confuses operators.
- **(e) Plugin repo placement.** Confirm `integrations/cassandra-memtable-plugin/` in this repo (Java, own build, outside cargo workspace / agent-gate Rust components) vs a separate scratch repo. Recommendation: in-repo, per the issue's 1:1:1:1 argument.

---

## 11. References

- `docs/storage engine/cassandra-index/research-plugin-mechanics.md` — factory contract, extend-vs-wrap evidence (the two instanceof gates), live-iteration safety, `performSnapshot`/timer semantics, watermark primitives, deployment, trunk deltas.
- `docs/storage engine/cassandra-index/research-export-format.md` — export mechanism decision (real nb SSTable via `getFlushSet` + `SSTableTxnWriter`/offline txn), hazards table, atomic-publication protocol, rejected alternatives.
- `docs/storage engine/cassandra-index/research-cqlite-tail-seam.md` — CQLite seam map, change inventory, LWW overlap analysis, test-harness plan.
- `docs/storage engine/report-1-memtable-freshness.md` — parent research report (§1 correctness framework, §2a option analysis); this design supersedes its a1 sketch.
- `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` (cassandra-5.0) — the canonical upstream CEP-11 plugin doc.
- GitHub issue #1807 — the spike (scope, acceptance criteria, non-goals).
