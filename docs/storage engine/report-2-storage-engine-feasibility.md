# Report 2 — CQLite as an Alternative or Adjacent Cassandra Storage Engine

**Date:** 2026-07-03
**Audience:** internal engineering scoping — frank, decision-oriented, code-anchored.
**Targets:** Apache Cassandra **trunk** (`~/local_projects/cassandra`, base.version 7.0,
cassandra-6.0 merged 2026-07-03); Cassandra 5.0.x deltas flagged inline. CQLite v0.12.0.
**Companion:** `synthesis-q1-freshness.md` (the freshness question);
`synthesis-q2-storage-engine.md` (the full working notes behind this report).

---

## Verdict

**Adjacent: yes. Replacement: no.** Cassandra has no `StorageEngine` interface; the
engine's responsibilities are scattered, and they split cleanly into a *write-and-store*
half that is genuinely pluggable on trunk (memtable via CEP-11, on-disk format via
`SSTableFormat.Factory`, compaction strategy via classloader) and a *read-and-move* half
that is hardwired with no seam (read-path merge, `Tracker`/`View` lifecycle, streaming,
repair). Slotting CQLite behind the seams that exist would replace exactly the parts
CQLite already replicates byte-for-byte, while inheriting all of Cassandra's LSM
architecture *plus* a JNI crash domain — high risk, marginal benefit, and fork-only the
moment it touches merge or lifecycle. The adjacent posture inverts the economics: CQLite
as a per-node OLAP replica exploits only the seams that exist (read flushed SSTables,
tail CDC, Sidecar discovery), is mostly shipped already (Flight/Trino phases 1–6,
`Database::refresh()`), and closes the Q1 freshness gap with a CDC-tail adapter that
requires **no in-JVM Cassandra change at all**. Recommendation: invest in the adjacent
path (CDC tail → Iceberg materializer), keep in-JVM work to at most a bounded
`SSTableFormat` interop spike, and never claim the replacement story.

---

## 1. How Cassandra actually works here (the mechanism map)

### 1.1 There is no engine object — CFS is the closest thing

`ColumnFamilyStore` (CFS) `implements ColumnFamilyStoreMBean, Memtable.Owner,
SSTable.Owner` (`db/ColumnFamilyStore.java:187`). It owns:

- the memtable factory (`memtableFactory = tableMetadata.params.memtable.factory()`,
  CFS ~403/523),
- flush machinery (`flushMemtable` ~1136, `signalFlushRequired` ~1484),
- the lifecycle `Tracker` + `View` (~296, 305) — the authoritative "which memtables and
  SSTables exist right now" set,
- compaction scheduling.

Every read snapshots that set at query start via `cfs.select(View.selectLive())`
(`SinglePartitionReadCommand.java:513`, `PartitionRangeReadCommand.java:403`) and merges
it internally. Nothing outside CFS gets handed that union.

### 1.2 Seam inventory (verified in source)

| Responsibility | Lives in | Seam on trunk? | Mechanism / anchor |
|---|---|---|---|
| In-memory write buffer | `db/memtable/Memtable` | **YES — CEP-11** | `Memtable` + `Memtable.Factory` (`Memtable.java:60,77`); chosen per table via `CREATE TABLE … WITH memtable='<name>'` → `memtable_configurations` yaml (`schema/MemtableParams.java:51,111`). **5.0: no seam** (fixed SkipList). |
| On-disk table format | `io/sstable/format/SSTableFormat<R,W>` | **YES** | `SSTableFormat.Factory` registered + validated in `DatabaseDescriptor.java:291–292,1914`; writer format picked by `Config.selected_format` (default `BigFormat.NAME`, `Config.java:457`). Two impls ship (`big/BigFormat.java`, `bti/BtiFormat.java`); a third can register. Present in 5.0 too. |
| Compaction strategy | `db/compaction/*Strategy` | **YES** (long-standing) | `WITH compaction={'class':…}` classloader plugin. |
| Durability / recovery | `db/commitlog/CommitLog` | **Partial** | A memtable may opt out via `writesShouldSkipCommitLog()` / `writesAreDurable()` (`Memtable.java:108–121`) — a durable memtable can own its own log. The commit-log *implementation* is not pluggable, and recovery replays commit log + SSTables. |
| Read-path merge (memtable ∪ sstables) | `SinglePartitionReadCommand` / `PartitionRangeReadCommand` → `UnfilteredRowIterators.merge()` | **NO** | Hardcoded loops over `view.memtables` + `view.sstables` sorted `maxTimestampDescending` (`SinglePartitionReadCommand.java:778`); final merge at `:986`. No listener, no substitution. LWW + timestamp ordering baked in. |
| Lifecycle / SSTable set | `db/lifecycle/Tracker` + `View` | **NO** | CFS-internal; snapshot at query start; no plugin. |
| Streaming (bootstrap, repair movement) | `streaming/*`, `StreamManager` | **NO** | Format-aware but engine-hardwired; assumes SSTable components on disk and `SSTableReader` objects. |
| Repair / repaired-status | `service/reads/repair/*`, `RepairedDataInfo` | **NO** | Binary repaired/unrepaired split, wired through `InputCollector` (`ReadCommand.java:1174`); timestamp is the only ordering signal. |
| Secondary index | `index/Index` (`Searcher`, `Indexer`) | **YES (partial)** | Custom-index SPI; fires on write for index maintenance but is oriented to flush/compaction, not a general write tap. |
| Read-iterator instrumentation | `db/StorageHook` | YES, but **not an engine seam** | See correction below. |

### 1.3 Three corrections to the raw indexes (each changes the verdict)

1. **`StorageHook` is not a storage seam.** `read-path.md` and `cqlite-flight-trino.md`
   present it as the place "alternative storage engines can intercept row-iterator
   creation." Reading the source (`db/StorageHook.java:33–91`): it is handed an
   **already-open `SSTableReader`** and returns an iterator over *that one sstable*
   (`makeRowIterator(cfs, sstable, key, slices, …)`), plus `reportRead`/`reportWrite`
   telemetry, inside the per-sstable loop (`SinglePartitionReadCommand.java:941,955,968`).
   It cannot change SSTable selection, add memtable data, alter the merge, or route a
   query to CQLite. Discard it from every posture.
2. **`SSTableFormat` is a real, config-selected plugin** — stronger than some indexes
   implied. A `CqliteFormat` could register and be selected via yaml. The catch is the
   size of the object contract, not the seam's existence (§2.1).
3. **Accord/TCM prove Cassandra hosts second stores — but not via plugins, and not as
   SSTables.** `tcm-journal-accord.md` claims Accord persists segments in "Cassandra's
   standard SSTable format." Wrong: `AccordJournal`
   (`service/accord/journal/AccordJournal.java:91`) wraps a bespoke append-only
   `Journal<K,V>` of `Segment`s (`journal/Segments.java:36`); it borrows a system-keyspace
   CFS only for SAI range indexing. TCM likewise stores its consensus log in a system
   table (`tcm/log/LogStorage.java:25` → `SystemKeyspaceStorage`). Both were added as
   **first-class integration code**, not through a seam. The precedent supports *a second
   purpose-built store beside the engine* (posture b), not *swapping the engine's guts*
   (posture a).

### 1.4 Trunk vs 5.0 in one line each

- **CEP-11 memtable pluggability: 5.0 AND trunk.** CEP-11 (CASSANDRA-17034) shipped in 4.1; verified present on `origin/cassandra-5.0` (`Memtable.java:58`, `MemtableParams.java:111`, `performSnapshot` at `Memtable.java:426`). [Corrected 2026-07-03: an earlier draft wrongly called this trunk-only.]
- **SSTableFormat SPI: both**; trunk adds BTI `da` as a second in-tree proof.
- **Read merge, Tracker, streaming, repair: closed on both.** No trunk relief.
- **CDC: both** (predates CEP-11) — this is why the adjacent posture is 5.0-compatible.

### 1.5 What we would be slotting in (CQLite today)

Verified against source (`cqlite-write-engine.md`): token-sorted BTreeMap memtable
(`storage/write_engine/memtable.rs:18`), CRC-framed WAL (`wal.rs`), full BIG+BTI
`SSTableWriter` with atomic tmp-dir rename (`writer/mod.rs:200+`), k-way merge + STCS
with byte-for-byte Cassandra 5.0 compaction parity (`merge/mod.rs`,
`merge_policy.rs:80`); uncompressed-write-only boundary enforced fail-closed
(`compression_info_writer.rs:195`, #1406). OLAP surface: per-node Arrow Flight server
(`cqlite-flight/src/service.rs:107`), Trino connector, Sidecar discovery (three read-only
GET endpoints), explicit `Database::refresh()` (`cqlite-core/src/lib.rs:321`),
snapshot-pinned reads. Missing for OLTP: counters, MV write-through, repair/streaming,
replica coordination — and no JVM presence. CQLite is a near-complete single-node Rust
LSM engine with an analytical read plane, not a cluster citizen.

---

## 2. Part (a) — replacement engine inside Cassandra

### 2.1 The seams, one by one

**Memtable (CEP-11) — real seam, in reach, low ROI.** Implement `Memtable` +
`Memtable.Factory` (`Memtable.java:60,77`). Data model matches (both token-sorted). Two
hard sub-problems: (i) the interface extends `UnfilteredSource` — the impl must produce
Cassandra `UnfilteredRowIterator`s with the full `Row`/`Cell`/range-tombstone object
graph, so CQLite would materialize Java objects across FFI on every read, not just move
bytes; (ii) flush is driven by the `Owner` (CFS) through `Flushing`/`SSTableWriter` — a
custom memtable does not choose its flush target unless it also supplies the format.
Durability: `writesShouldSkipCommitLog()` lets a WAL-backed memtable avoid double-logging,
at the cost of CDC/PITR unless the WAL also honors that contract.

**SSTableFormat — real seam, invasive.** Register `CqliteFormat` and set
`selected_format`. CQLite already speaks the byte formats; the cost is the
**object-shape impedance**: `SSTableReaderFactory`/`SSTableWriterFactory`
(`SSTableFormat.java:107–135`) demand a real `SSTableReader` subclass that participates
in key cache, scrub, verify, compaction inputs, streaming, and the `Component` model —
roughly a dozen surfaces (`IScrubber`, `MetricsProviders`, …). CQLite's value (fast Rust
scan) sits *below* that Java surface, so the win evaporates into glue unless hot loops
delegate to Rust over JNI — which imports the crash domain: a Rust panic through FFI
aborts the JVM (the same panic=abort landmine the bindings audit flagged; cf. the
`release-unwind` firewall, #1440, which only softens it for CQLite's own bindings).

**Read-path merge — no seam.** `UnfilteredRowIterators.merge()` /
`UnfilteredPartitionIterators.mergeLazily()` are final; ordering
(`maxTimestampDescending` + LWW) is baked in (`SinglePartitionReadCommand.java:778,986`).
CQLite's reconcile is semantically equivalent, but there is no way to substitute it:
in-JVM, you feed Cassandra iterators into Cassandra's merge. CQLite can never own the
read hot path inside the JVM; it can only supply per-source iterators.

**Lifecycle / Tracker / streaming / repair — no seam, fork territory.** `Tracker`+`View`
are CFS-internal; streaming and repair assume on-disk SSTable components and
`SSTableReader` objects. Replacing any of it means subclassing/patching CFS,
`StreamManager`, and the repair path — upgrade-hostile surgery ASF has deliberately not
opened.

### 2.2 Concrete change inventory for a serious attempt

| Change | New/patch | Upstreamable? |
|---|---|---|
| `CqliteMemtable implements Memtable` + factory, yaml-wired, `writesShouldSkipCommitLog()` | new plugin | in principle — exactly what CEP-11 invites |
| `CqliteFormat implements SSTableFormat` + reader/writer/scrubber/verifier factories, `Component` set, metrics | new plugin, large | in principle, **if pure Java** |
| JNI/FFI bridge for iterator production + a crash-domain firewall (out-of-process or panic=unwind + catch) | new, no precedent | **no** — ASF will not take a native hot-path dependency in the storage core; fork/vendor-only |
| Merge substitution, `Tracker`/`View`, streaming, repair, hinted handoff | patch/fork CFS + StreamManager + repair | **no seam exists; fork-only** |

### 2.3 Verdict (a)

**Not recommended.** Feasible only as "pluggable memtable + pluggable SSTableFormat,"
and even that is low-ROI: the seams that exist cover exactly what CQLite already does
(memtable, format, compaction — where it has byte parity), and the parts where an engine
earns its keep (merge, lifecycle, streaming, repair) are closed. Best case, an in-JVM
CQLite satisfies the `Memtable`/`SSTableReader` object contracts, inherits all of
Cassandra's LSM architecture anyway, and adds a JNI crash domain. The Accord/TCM
precedent (§1.3) argues for a second store *beside* the engine, not a replacement of it.
At most: a bounded interop spike (§4, Tier 3).

---

## 3. Part (b) — adjacent OLAP engine beside the normal engine

The normal engine keeps OLTP; CQLite maintains a per-node analytical replica served by
Flight/Trino/DataFusion and materialized to Iceberg. Three feed postures, ascending in
freshness and cost.

### Posture (i) — flush-and-read (shipped today)

CQLite reads flushed SSTables (`DirSource`, `producer.rs:86`) pinned to a snapshot
hardlink set, with `Database::refresh()` (`lib.rs:321`, #1749) as the explicit freshness
contract. Honest claim: **"complete as of last flush/snapshot"** — staleness bounded by
memtable residency (seconds–minutes). No Cassandra change. Live since Flight phases 1–6.

Upgrade path: flush-before-snapshot gives request-time freshness and is correct by
construction (a plain user snapshot already flushes first —
`service/snapshot/TakeSnapshotTask.java:128`). But the HTTP lever is missing: Sidecar
exposes only `GET /ring`, `/token-range-replicas`, `/schema`
(`SidecarClient.java:39–52`; `cassandra-sidecar-server-surface.md`). Flush/snapshot
endpoints must be verified against, and likely contributed to, apache/cassandra-sidecar.
`nodetool`/JMX flush is the only confirmed lever today.

### Posture (ii) — CDC tail into CQLite's own WAL/memtable (the Q1 unlock)

Cassandra CDC (5.0-native, config-only) pipes mutations to a CQLite adapter that replays
them into CQLite's *own* memtable; the Flight query merges memtable ∪ snapshot SSTables.
CQLite already owns the hold-and-reconcile half (WAL, memtable with scan surface
`memtable.rs:118`, byte-parity LWW rules). Missing pieces: a CDC→CQLite-mutation decoder,
a flush-boundary watermark (commit-log position ↔ flushed generation, riding on the
authoritative Statistics.db work #1728/#1729), and the tail's route into the merge — the
`SstableSource` trait returns **paths**, not iterators (`producer.rs:80`), so the cheap
implementation materializes the tail to a temp SSTable the existing path-based merge
consumes (Q1 synthesis, Correction 2), rather than adding a second producer seam.

Honest claim: **"eventually complete, bounded by CDC lag"** (commit-log sync cadence,
typically ≤ seconds). LWW/tombstone-correct because CDC mutations carry authoritative
timestamps and deletes, replayed through the same reconcile that has byte parity.
**This is the Q1 answer that needs zero in-JVM Cassandra change**, and it works on 5.0
and trunk. Cost: the node's write volume flows through CQLite a second time.

### Posture (iii) — dual-write CEP-11 memtable (only if forced)

A `Memtable.Factory` that dual-writes into CQLite at write time. Honest claim:
**read-your-writes on that node**. This is posture (a)'s memtable seam wearing an OLAP
hat, with the same FFI crash-domain risk — now a bug in the analytics path can stall or
abort OLTP writes. Trunk-only. Justified only by a hard sub-CDC-lag freshness SLA.

### 3.1 Where Flight/Trino and the Iceberg epic slot in

- **Flight/Trino is the hot per-node read plane** for (i) and (ii): same
  producer/filter/aggregation pipeline (`MergeProducer::with_spec/with_aggregation`,
  `producer.rs:282` — filters apply post-merge, so any added row source inherits pushdown
  for free). The `FlightTicket` contract (`ticket.rs:225`, `#[non_exhaustive]`) has room
  for a `max_staleness_ms` hint so clients choose snapshot vs flush-first vs tail-merged.
- **The Iceberg materializer** (`epic-draft.md` + `proposal.md`/`design.md`/`spec.md`) is
  the cold complete-history plane: folds delta envelopes into Iceberg v2 with
  exactly-once generation consumption and the `cqlite.delta-horizon-micros` watermark.
  It consumes posture (i)/(ii) output; its children map 1:1 onto the cluster-consistency
  questions below (`add-materializer-primary-range-dedup`,
  `add-materializer-repaired-gating`).

### 3.2 Cluster-level dedup (RF) — per-node replicas overlap

Each node stores RF copies' worth of data; naively unioning nodes double-counts by RF.
Two complementary strategies, both already sketched:

- **Primary-token-range pruning** — each node serves/materializes only its primary
  ranges, reusing the Flight connector's `token_in_half_open_range` (`ticket.rs:396`) and
  Sidecar `token-range-replicas`. (Epic child 3.)
- **Repaired-status gating** — only repaired SSTables feed the lakehouse; the repair
  horizon is the consistency watermark. (Epic child 4.) **Prerequisite:** CQLite
  currently has no repaired/unrepaired notion; it must first read `repairedAt` from
  Statistics.db.

### 3.3 Posture comparison table

| Posture | Freshness / honest claim | Cassandra change class | Effort | Risk / blast radius |
|---|---|---|---|---|
| (a) memtable+format replacement | n/a (inherits Cassandra semantics) | **plugin (4.1+/5.0) + FFI; fork for merge/lifecycle/streaming/repair** | **XL** | JVM crash domain on the OLTP hot path; upgrade-hostile; fork-only past the seams |
| (b-i) snapshot read (today) | complete as of last flush/snapshot | **none** | shipped | lowest — read-only sidecar; staleness = memtable residency |
| (b-i)+flush | complete as of query start | **config/tooling** (nodetool/JMX now; Sidecar endpoints later) | **S** (+ **M** upstream Sidecar) | flush latency spike + small-SSTable/compaction pressure if abused |
| (b-ii) CDC tail | eventually complete, CDC-lag-bounded | **config only** (enable CDC) + CQLite adapter | **M** | lowest of the real-freshness options: zero write-hot-path code; sidecar failure = stale reads, never OLTP loss; double write volume |
| (b-iii) dual-write memtable | read-your-writes per node | **plugin (CEP-11, 4.1+/5.0)** + FFI firewall | **L** | analytics bug can stall/abort OLTP writes; version churn against CEP-11 |
| Iceberg materializer (on i/ii) | complete history ≤ delta/repair horizon | **none** | **M–L** (epic, drafted) | depends on #1728/#1729 watermark; RF dedup decisions |

### 3.4 Verdict (b)

**Recommended, and largely in flight.** The adjacency model uses exactly the seams that
exist (SSTable files, CDC, Sidecar discovery, snapshots) and avoids the ones that don't
(merge, Tracker, streaming, repair internals). This is where CQLite's differentiators —
Rust scan speed, byte-parity reconcile, zero-ETL lakehouse — convert to value at low
blast radius: a CQLite crash is read-only downtime, never OLTP data loss.

---

## 4. Recommendation (staged) and what to prototype first

**Tier 0 — shipped.** Posture (i): snapshot-pinned Flight/Trino reads + `refresh()`.

**Tier 1 (weeks, zero Cassandra core change) — build first:**

1. **CDC→CQLite adapter + tail merge (posture ii).** The single highest-value item; it
   is also the Q1 answer. Prototype = one node, CDC enabled on one table, adapter tails
   CDC segments into CQLite's WAL/memtable, tail materialized to a temp SSTable consumed
   by the existing `DirSource` merge, deduped at the commit-log/generation watermark.
   **Evidence the prototype must produce:** (a) a write acked by Cassandra is visible in
   a Flight read within a measured CDC-lag bound; (b) LWW + tombstone parity vs a
   Cassandra quorum read on a mixed workload including deletes and a mid-run flush —
   i.e., no duplicate and no resurrected row across the flush boundary; (c) measured
   adapter overhead per write.
2. **`max_staleness_ms` FlightTicket hint** + connector policy (snapshot vs flush-first
   vs tail-merged) so consistency is a client contract, not folklore.
3. **Iceberg materializer child 1** (`add-iceberg-materializer`) on posture-(i) input —
   OpenSpec already drafted; gated on #1728/#1729.

**Tier 2 (a quarter, upstream Sidecar, still no engine change):**

4. Verify then contribute **Sidecar flush/snapshot/components endpoints**
   (apache/cassandra-sidecar) → request-time freshness for posture (i)+flush and remote
   component range reads (Design B).
5. **Repaired-status ingestion in CQLite** (read `repairedAt` from Statistics.db) →
   repaired gating + primary-range dedup = an RF-safe cluster materialization story.

**Tier 3 (research spikes only, in-JVM; do not productize without a forcing
requirement):**

6. **Pure-Java (or thin-FFI) `CqliteFormat implements SSTableFormat`** registered via
   `selected_format`, on a dev cluster. Evidence: Cassandra flushes/compacts/streams a
   table in the format and CQLite reads it back with parity. Purpose: prove interop and
   keep the upstream option alive — not to ship.
7. **CEP-11 `CqliteMemtable` dual-write spike (posture iii)** behind an out-of-process or
   panic=unwind FFI firewall. Evidence: write-path latency delta and crash-containment
   under injected Rust panics. Go/no-go on the freshness-SLA decision below.

**Never:** substituting the read-path merge, `Tracker`/`View`, streaming, or repair.
No seam exists; that is a permanent fork and out of scope.

**Frank feasibility verdict.** (a) Replacement: **not feasible as a product** — plugin
seams cover only what CQLite already has, everything else is fork-only, and the FFI
crash domain lands on the OLTP hot path. (b) Adjacent: **feasible, partially shipped,
and the correct strategic frame** — "an adjacent per-node OLAP replica: snapshot reads
today, CDC-tailed freshness next, Iceberg-materialized history — plus, optionally, an
upstreamable pure-Java `SSTableFormat` for interop." Not "a Cassandra storage-engine
replacement."

---

## 5. NEEDS-DECISION (owner calls)

1. **Freshness posture commitment.** Ship posture (ii) CDC-tail (eventual,
   CDC-lag-bounded, no core change, 5.0-compatible) as the Q1 answer, or hold for
   posture (i)+flush (request-time, needs Sidecar endpoints, flush/stall cost), or both
   behind the `max_staleness_ms` hint? This decides the consistency claim we publish and
   whether Tier 3.7 ever runs.
2. **Upstream vs vendor for any in-JVM work.** A pure-Java `SSTableFormat`/`Memtable` is
   contributable to ASF; anything calling Rust in the hot path is fork/vendor-only.
   Decide before Tier 3.6 — it changes the entire engineering approach.
3. **Cluster dedup contract.** Primary-token-range pruning, repaired-status gating, or
   both, as the RF-dedup story for cluster-level materialization — accepting the
   prerequisite that CQLite must first read `repairedAt` from Statistics.db.
4. **Sidecar dependency ownership.** Are we willing to verify and then drive `/flush`,
   `/snapshots`, and Range-honoring `/components` endpoints into
   apache/cassandra-sidecar? Tier 2 blocks on this; the surface index is still
   unconfirmed against the real repo.
5. **CDC double-write cost.** Posture (ii) reprocesses full write volume through CQLite
   per node and requires CDC enabled (disk overhead, segment retention). Acceptable
   fleet-wide, or gated to selected keyspaces/tables?
6. **Iceberg epic open questions** (already flagged in `design.md`): OQ1 iceberg-rust v2
   equality-delete maturity (build vs adopt); OQ2 clustering types ineligible as Iceberg
   identifier fields (degrade to position deletes vs fail closed); OQ3 static-column
   materialization shape.

---

## 6. Appendix — drill-down pointers

All under `docs/storage engine/` unless noted. **Note:** three index files live in a
*separate, literally-backslash-named* directory `docs/storage\ engine/` (quote both paths
in shells).

| File | What it holds | Trust notes |
|---|---|---|
| `synthesis-q2-storage-engine.md` | Full working notes behind this report: seam table with anchors, change inventory, Accord/TCM analysis, roadmap | Source-verified; supersedes the raw indexes where they conflict |
| `synthesis-q1-freshness.md` | The freshness companion: correctness framework (LWW/tombstone/watermark), option analysis (a–e), staged Q1 recommendation | Source-verified; Corrections 1–3 there apply here too |
| `docs/storage\ engine/read-path.md` | Read-path class map (`ReadCommand`, `InputCollector`, merge, `View` snapshot), hard couplings 1–7 | **Caveat:** overstates `StorageHook` as an engine seam (§1.3.1); its posture-(b)-via-StorageHook sketch is invalid |
| `docs/storage\ engine/tcm-journal-accord.md` | TCM/`LogStorage`, generic `Journal<K,V>`, Accord journal anchors; CEP-11 + SSTableFormat pointers | **Caveat:** wrongly says Accord segments use SSTable format (§1.3.3) |
| `docs/storage\ engine/cqlite-flight-trino.md` | Flight server + Trino connector class map, `SstableSource`/`MergeProducer`/`FlightTicket` seams, hard couplings | **Caveat:** "memtable could implement `SstableSource`" is wrong — the trait returns paths (`producer.rs:80`); materialize the tail instead |
| `cassandra-sidecar-server-surface.md` | Sidecar HTTP surface as used by CQLite (3 GET endpoints) + Design-B proposal | INDEX ONLY — endpoints beyond the three GETs are unverified against apache/cassandra-sidecar |
| `cqlite-write-engine.md` | CQLite write-engine inventory (memtable, WAL, writer, merge/STCS, parity status) | Verified against source paths |
| `epic-draft.md`, `proposal.md`, `design.md`, `spec.md`, `tasks.md` | Iceberg-materializer epic + OpenSpec change for child 1 | Owner decisions OQ1–OQ3 pending (§5.6) |

Key source anchors for re-verification: `ColumnFamilyStore.java:187`;
`Memtable.java:60,77,108–121,449`; `MemtableParams.java:111`;
`DatabaseDescriptor.java:291–292,1914`; `Config.java:457`;
`SSTableFormat.java:107–135`; `SinglePartitionReadCommand.java:778,986`;
`ReadCommand.java:1174`; `StorageHook.java:33–91`; `AccordJournal.java:91`;
`journal/Segments.java:36`; `tcm/log/LogStorage.java:25`;
`TakeSnapshotTask.java:128`; CQLite: `producer.rs:80,86,262,282`; `ticket.rs:225,396`;
`lib.rs:321`; `memtable.rs:18,118`; `compression_info_writer.rs:195`.
