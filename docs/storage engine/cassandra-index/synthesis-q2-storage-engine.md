# Synthesis — Q2: CQLite as Alternative or Adjacent Storage Engine

**Audience:** internal engineering scoping (frank, code-anchored, effort-tiered).
**Targets:** Apache Cassandra **trunk** (base.version 7.0, cassandra-6.0 merged 2026-07-03,
HEAD `50ddce8455`), with 5.0.x deltas flagged. CQLite at v0.12.0.
**Source of truth:** spot-checked against `~/local_projects/cassandra` (Java) and
`cqlite-core` (Rust); Haiku index files in `docs/storage engine/` +
`docs/storage\ engine/`. Load-bearing claims re-verified in source — corrections below.

---

## 0. Corrections to the input indexes (verified in source)

These change the verdict, so they lead:

1. **`StorageHook` is NOT a viable engine-replacement seam.** `read-path.md` and
   `cqlite-flight-trino.md` both lean on `StorageHook` as the hook where "alternative
   storage engines (CQLite via Arrow Flight) can intercept row-iterator creation." Wrong.
   `StorageHook` (`src/java/org/apache/cassandra/db/StorageHook.java:33–91`) is handed an
   **already-opened `SSTableReader`** and returns an iterator over *that one sstable*
   (`makeRowIterator(cfs, sstable, key, slices, …)`), plus `reportRead`/`reportWrite`
   telemetry. It runs *inside* the per-sstable loop (`SinglePartitionReadCommand.java:941,
   955, 968`). It cannot: replace SSTable-set selection, add memtable data, change the
   merge, or route a query wholesale to CQLite. It is an instrumentation / per-sstable
   wrapper seam (full-query-logging, dtest), not a storage seam. **Do not build posture (a)
   or (b) on StorageHook.**

2. **`SSTableFormat` is a genuine, config-selected plugin — stronger than some indexes imply.**
   Formats register as `SSTableFormat.Factory` and are validated + indexed by name in
   `DatabaseDescriptor` (`sstableFormats` ImmutableMap + `selectedSSTableFormat`,
   `DatabaseDescriptor.java:291–292, 1914`); the active writer format is chosen by
   `Config.selected_format` (default `BigFormat.NAME`, `Config.java:457`). Two impls ship
   (`big/BigFormat.java`, `bti/BtiFormat.java`). A third (CQLite) *can* register and be
   selected via yaml — this is a real, upstreamable seam.

3. **The "commit log is mandatory" coupling has an escape hatch.** `Memtable.Factory`
   exposes `writesShouldSkipCommitLog()` and `writesAreDurable()`
   (`Memtable.java:108–121`): a memtable that provides its own durability can suppress the
   commit log (must still allow turning it on for CDC/PITR). So a CQLite-backed durable
   memtable need not double-write the commit log. `tcm-journal-accord.md`'s "commit log
   assumed" hard-coupling is softer than stated.

4. **Accord is a real "second store," but it does NOT reuse the SSTable format.**
   `tcm-journal-accord.md` says Accord "uses Cassandra's standard SSTable format for
   segment persistence." Wrong. `AccordJournal` (`service/accord/journal/AccordJournal.java:91`)
   wraps a bespoke generic `Journal<K,V>` whose on-disk unit is a `Segment`
   (`journal/Segments.java:36`, keyed by `descriptor.timestamp`) — an append-only journal
   format, *not* SSTables. It *also* holds a backing `ColumnFamilyStore table`
   (`AccordKeyspace.JOURNAL`) purely for SAI range-search indexing. The precedent this sets
   for Q2(a) is important but different from the index's framing (see §3.4).

---

## 1. Where "the storage engine" actually lives

Cassandra has **no single `StorageEngine` interface**. The responsibilities are spread
across classes, each with its own (or no) seam. `ColumnFamilyStore` (CFS) is the closest
thing to an engine object: it `implements ColumnFamilyStoreMBean, Memtable.Owner,
SSTable.Owner` (`ColumnFamilyStore.java:187`) and owns the memtable factory
(`memtableFactory = tableMetadata.params.memtable.factory()`, line 403/523), the flush
machinery (`flushMemtable`, line 1136; `signalFlushRequired`, line 1484), the lifecycle
`Tracker`/`View` (lines 296, 305), and compaction scheduling.

| Responsibility | Lives in | Seam on trunk? | Mechanism |
|---|---|---|---|
| In-memory write buffer | `db/memtable/Memtable` + `Memtable.Factory` | **YES (CEP-11)** | `CREATE TABLE … WITH memtable='<cfg>'` → `memtable_configurations` in cassandra.yaml; static `FACTORY`/`factory(Map)` |
| On-disk table format | `io/sstable/format/SSTableFormat<R,W>` | **YES** | `SSTableFormat.Factory` registered in `DatabaseDescriptor`; `Config.selected_format` |
| Compaction strategy | `db/compaction/*Strategy` | **YES (long-standing)** | `CREATE TABLE … WITH compaction={'class':…}` classloader plugin |
| Durability / recovery | `db/commitlog/CommitLog` | **Partial** | memtable may opt out via `writesShouldSkipCommitLog()`; the log impl itself is not pluggable |
| Read-path merge (memtable ∪ sstables) | `SinglePartitionReadCommand` / `PartitionRangeReadCommand` → `UnfilteredRowIterators.merge` | **NO** | hardcoded loops over `view.memtables` + `view.sstables` (sorted `maxTimestampDescending`), final merge at `SinglePartitionReadCommand.java:986` |
| Lifecycle / SSTable set | `db/lifecycle/Tracker` + `View` (owned by CFS) | **NO** | `cfs.select(View.selectLive())` snapshot at query start; no plugin |
| Streaming (bootstrap/repair movement) | `streaming/*`, `StreamManager` | **NO** (format-aware, not engine-pluggable) | assumes SSTable components on disk |
| Repair / digest / repaired-status | `service/reads/repair/*`, `RepairedDataInfo` | **NO** | binary repaired/unrepaired, timestamp-only ordering |
| Secondary index integration | `index/Index` (`Index.Searcher`, `Indexer`) | **YES (partial)** | custom index SPI; but fires post-merge / on-flush, not on write |
| Read-iterator instrumentation | `db/StorageHook` | YES but **irrelevant to engine replacement** (see §0.1) | `-Dcassandra.storage_hook=` |

**Takeaway:** the *write-and-store* half (memtable, format, compaction, durability opt-out)
is pluggable on trunk. The *read-and-move* half (merge, lifecycle/Tracker, streaming,
repair) is hardwired and assumes `Memtable` + `SSTableReader` object shapes. That split is
the whole story for Q2.

---

## 2. CQLite today (what we would be slotting in)

From `cqlite-write-engine.md`, verified against paths:
- Complete: `Memtable` (BTreeMap, token-sorted, `memtable.rs:18`), CRC-framed `WAL`
  (`wal.rs`), `SSTableWriter` (all BIG+BTI components, atomic tmp-dir rename,
  `writer/mod.rs:200+`), K-way merge + STCS (`merge/mod.rs`, `merge_policy.rs:80`),
  byte-for-byte Cassandra 5.0 compaction parity (~14/28 rules, tombstone/cell-path gaps).
- Enforced boundary: **uncompressed BIG (`nb`) writes only**; compressed writes fail closed
  (`compression_info_writer.rs:195`, issue #1406).
- Missing for OLTP: counters, MV write-through, local indexes, repair/streaming, replica
  coordination.
- OLAP surface: Arrow Flight server per node (`cqlite-flight/src/`), Trino connector
  (`trino-connector/`), Sidecar discovery (3 read-only endpoints), explicit
  `Database::refresh()` (`cqlite-core/src/lib.rs:321`), snapshot-pinned reads. **Sees only
  flushed SSTables** — the Q1 gap.

CQLite is a near-complete *single-node LSM engine* in Rust with a Cassandra-parity write
path and an analytical read surface. It has no cluster plane and no JVM presence.

---

## 3. Part (a) — CQLite as a replacement/alternative engine inside Cassandra

### 3.1 The four seams, one by one

**Memtable (CEP-11) — REAL SEAM, in reach.** Implement `Memtable` + `Memtable.Factory`
(`Memtable.java:60,77`). CQLite's memtable is already a token-sorted BTreeMap, so the data
model matches. Two hard sub-problems: (a) the interface demands `UnfilteredSource`
(`rowIterator`/`partitionIterator` producing Cassandra `UnfilteredRowIterator`s with
range-tombstone markers, deletion info, `Row`/`Cell` object graph) — CQLite would have to
materialize Cassandra's row objects across the FFI boundary, not just bytes; (b) flush is
driven by the `Owner` (CFS) and lands through `Flushing`/`SSTableWriter` — a CQLite memtable
does **not** get to choose the flush target unless it *also* supplies the format (below).
Durability: opt out of commit log via `writesShouldSkipCommitLog()` and back writes with
CQLite's WAL — feasible, but forfeits CDC/PITR unless the WAL also feeds the commit-log
contract.

**SSTableFormat — REAL SEAM, invasive.** Register a `CqliteFormat` `SSTableFormat.Factory`
and set `selected_format`. CQLite already reads/writes the byte formats. The cost is the
**object-shape impedance**: `SSTableReaderFactory` must produce a real `SSTableReader`
subclass that participates in key cache, scrub, verify, compaction inputs, streaming, and
the `Component` model (`SSTableFormat.java:107–135`). You are not shipping "a reader" — you
are implementing ~a dozen `SSTableReader`/`SSTableWriter`/`IScrubber`/`MetricsProviders`
surfaces the interface demands. CQLite's value (fast Rust scan) lives *below* this Java
surface, so most of the win is lost to glue unless the format delegates hot loops to Rust
via JNI/FFI (crash-domain risk: a Rust panic through JNI aborts the JVM — cf. the
panic=abort FFI landmine noted in the bindings audit).

**Read-path merge — NO SEAM.** `UnfilteredRowIterators.merge()` /
`UnfilteredPartitionIterators.mergeLazily()` are final; ordering is `maxTimestampDescending`
+ LWW, baked in (`SinglePartitionReadCommand.java:778, 986`). CQLite's reconcile is
equivalent in *semantics* but there is no way to substitute CQLite's merge — you feed
Cassandra iterators into Cassandra's merge. Fine for correctness, but it means CQLite cannot
own the read hot path in-JVM; it can only supply per-source iterators.

**Lifecycle / Tracker / streaming / repair — NO SEAM.** `Tracker`+`View` are CFS-internal;
`cfs.select(View.selectLive())` snapshots memtables+sstables at query start. Streaming and
repair assume on-disk SSTable components and the `SSTableReader` object. Replacing these is
**fork territory** — subclassing/patching CFS, StreamManager, and the repair path, all of
which are upgrade-hostile.

### 3.2 Change inventory for a serious in-JVM attempt

- `CqliteMemtable implements Memtable` + `CqliteMemtableFactory implements Memtable.Factory`
  (+ `writesShouldSkipCommitLog`), yaml-wired. — *new, upstreamable in principle.*
- `CqliteFormat implements SSTableFormat` + reader/writer/scrubber/verifier factories,
  `Component` set, metrics providers. — *new, large, upstreamable in principle.*
- JNI/FFI bridge Java↔Rust for iterator production and merge input; a crash-domain firewall
  (out-of-process or `panic=unwind` + catch, cf. #1440) so Rust panics can't abort the JVM.
  — *new, no upstream precedent, highest risk.*
- **No change possible without a fork** for: merge substitution, `Tracker`/`View`
  lifecycle, streaming, repair, hinted handoff.

### 3.3 Upstreamable vs fork-only

- **Upstreamable:** a new `Memtable.Factory` and a new `SSTableFormat.Factory` are exactly
  what CEP-11 and the format SPI invite. A *pure-Java* CQLite-format (or a memtable) could
  be contributed as an alternative impl. The moment it calls Rust over JNI in the hot path,
  it stops being upstreamable (ASF will not take a native-lib hot dependency in the storage
  core) and becomes a fork/vendor build.
- **Fork-only:** anything touching merge, Tracker, streaming, repair — i.e. any claim of
  "CQLite is *the* engine." There is no seam and the ASF surface is deliberately closed.

### 3.4 The TCM / journal / Accord precedent — and its limits

Trunk *does* host second, non-CFS stores: **TCM** (cluster metadata as a consensus log,
`tcm/log/LogStorage.java:25` → `SystemKeyspaceStorage`) and **Accord** (a bespoke
append-only `Journal<K,V>` of `Segment`s for transaction state,
`AccordJournal.java:91`, `journal/Segments.java:36`). This proves Cassandra will host a
second storage subsystem *in-tree*. But note what it actually shows:
- Each was added as **first-class integration code**, not through a plugin seam. Accord's
  journal is its own format; its indexing borrows a system-keyspace CFS + SAI, not the
  reverse.
- The precedent supports **posture (b)** ("a second, purpose-built store beside the OLTP
  engine, integrated where it needs to be") far more than **posture (a)** ("swap the OLTP
  engine's guts"). CQLite-as-Accord-shaped-sidestore is the plausible reading; CQLite-as-CFS
  is not.

### 3.5 Verdict (a)

**Feasible only as "pluggable memtable + pluggable SSTableFormat," and even that is
low-ROI.** The parts with seams (memtable, format, compaction) are the parts CQLite already
does; the parts with no seam (merge, lifecycle, streaming, repair) are where an engine
actually earns its keep, and they are fork-only. An in-JVM CQLite that satisfies the
`Memtable`/`SSTableReader` object contracts inherits all of Cassandra's LSM/merge/compaction
architecture while adding a JNI crash domain — you pay native-integration risk for
marginal benefit. **Recommendation: do not pursue (a) as "replacement." At most, prototype a
pure-Java or thin-FFI `SSTableFormat` to prove interop, and treat a CQLite `Memtable.Factory`
as a research spike, not a product.**

---

## 4. Part (b) — CQLite as an adjacent OLAP engine alongside the normal engine

Normal engine keeps OLTP; CQLite maintains a per-node analytical replica read by
Flight/Trino/DataFusion and materialized to Iceberg. Three feed postures, in ascending
freshness and cost:

### Posture (i) — Flush-and-read (what exists today)

CQLite reads flushed SSTables directly (`DirSource`, `producer.rs:86`), pinned to a
Sidecar snapshot hardlink set (`Database::refresh()`, `lib.rs:321`). **Consistency it can
honestly claim:** "all data flushed as of snapshot creation," i.e. *bounded staleness =
memtable-residency time* (seconds to minutes). Memtable data is invisible (Q1 gap). No
Cassandra change required. This is **proven and live** (Flight phases 1–6 complete).

- Freshness upgrade path A (5.0-compatible): a **Sidecar `/flush` endpoint** — connector
  flushes the table, then snapshots, then reads. Cost: latency spike + memtable write
  stall. **Blocked today: Sidecar exposes no flush/snapshot endpoints**
  (`cassandra-sidecar-server-surface.md` §Q1 — only `/ring`, `/token-range-replicas`,
  `/schema` exist). Needs upstream Sidecar work (`POST …/memtables/flush`,
  `POST …/snapshots`, `GET …/snapshots/:s/components`).

### Posture (ii) — CDC tail into CQLite's own WAL/memtable (fills the Q1 freshness gap)

Cassandra CDC (commit-log segment tailing, 5.0-native) pipes mutations to CQLite, which
replays them into its *own* memtable and merges memtable ∪ snapshot SSTables in the
`MergeProducer`. CQLite already has the WAL + memtable + reconcile to do this; the missing
piece is a CDC→CQLite-mutation adapter and a `SstableSource` impl that unions the live
memtable (the `SstableSource` trait at `producer.rs:80` is the intended seam — a
memtable source "could implement this trait, feeding merged rows to the same
producer→Arrow pipeline"). **Consistency it can honestly claim:** *eventual, CDC-lag-bounded*
— reads reflect all writes up to the CDC tail position, which trails live writes by the
commit-log flush/segment cadence. Cross-reference Q1: **this is the freshness answer that
needs no new in-JVM Cassandra hook** — only CDC (exists) + CQLite adapter. Trade-off:
duplicate the write volume through CQLite; CDC ordering/dedup vs the flushed SSTables must be
reconciled by mutation timestamp (CQLite's LWW already does this).

### Posture (iii) — Custom dual-write Memtable/Index (tightest, in-JVM)

A CEP-11 `Memtable.Factory` (or custom `Index`) that dual-writes into a CQLite structure at
write time, so the analytical replica is synchronous with OLTP. **Consistency:**
*read-your-writes on that node.* Cost: this is really posture (a)'s memtable seam wearing an
OLAP hat, with the same JNI crash-domain risk (a bug in the analytics path can now stall or
abort OLTP writes). **Only justified if sub-CDC-lag freshness is a hard requirement.**

### 4.1 How Flight/Trino and the Iceberg epic slot in

- **Flight/Trino** is the *hot per-node read plane* for postures (i) and (ii): same
  producer/filter/aggregation pipeline (`MergeProducer::with_spec/with_aggregation`,
  `producer.rs:282`), just a different `SstableSource` (snapshot-only vs snapshot∪memtable).
  The `FlightTicket` JSON contract (`ticket.rs:225`, `#[non_exhaustive]`) already has room
  for a `max_staleness_ms` freshness hint.
- **Iceberg-materializer epic** (`epic-draft.md`, `proposal.md`, `design.md`, `spec.md`) is
  the *cold complete-history plane*: folds delta envelopes into Iceberg v2 tables with
  exactly-once generation consumption and a `cqlite.delta-horizon-micros` watermark. It
  slots on top of posture (i)/(ii) as the durable OLAP sink. Its child issues map 1:1 to the
  consistency questions below: `add-materializer-primary-range-dedup` (RF dedup) and
  `add-materializer-repaired-gating` (repaired-status watermark).

### 4.2 Consistency contract per posture (the honest claims)

| Posture | Freshness | Cassandra change | Honest claim |
|---|---|---|---|
| (i) snapshot | memtable-residency stale | none (Sidecar flush endpoint = optional upgrade) | "complete as of last flush/snapshot" |
| (i)+flush | ~request-time | **Sidecar flush/snapshot endpoints (missing)** | "complete as of query start, at a latency/stall cost" |
| (ii) CDC tail | CDC-lag bounded | none (CDC exists) + CQLite adapter | "eventually complete, bounded by CDC lag" |
| (iii) dual-write | read-your-writes on node | CEP-11 memtable/index + JNI firewall | "synchronous per node" |

### 4.3 Per-node vs cluster-level dedup (RF)

Each node holds RF copies' worth of overlapping data. A per-node analytical replica that
naively unions all nodes double-counts by RF. Two dedup strategies, both already sketched:
- **Token-range pruning** — each node materializes/serves only its **primary** token ranges
  (reuse the Flight connector's `token_in_half_open_range`, `ticket.rs:396`, and
  Sidecar `token-range-replicas`). This is `add-materializer-primary-range-dedup`.
- **Repaired-status gating** — only *repaired* SSTables feed the lakehouse; the repair
  horizon is the consistency watermark. This is `add-materializer-repaired-gating`. Caveat:
  CQLite currently has **no repaired/unrepaired distinction** (`read-path.md` §b), so it must
  learn to read the repaired-at metadata from `Statistics.db` before it can gate on it.

### 4.4 Verdict (b)

**Feasible and largely built for posture (i); posture (ii) is the high-value next step and
needs no in-JVM Cassandra hook.** The adjacency model exploits exactly the seams that exist
(read SSTables, CDC tail, Sidecar discovery) and avoids the ones that don't (merge,
lifecycle, repair internals). This is where CQLite's Rust scan speed and Cassandra-parity
reconcile actually pay off, at low blast radius (a sidecar crash = read-only downtime, never
OLTP data loss).

---

## 5. Effort-tiered roadmap (what to prototype first)

**Tier 0 — already shipped:** posture (i) snapshot reads via Flight/Trino + `refresh()`.

**Tier 1 (weeks, no Cassandra core change) — close the freshness gap the cheap way:**
1. **CDC→CQLite adapter + memtable `SstableSource`** (posture ii). Highest freshness-per-effort;
   directly answers Q1 without touching the JVM engine. Reuses CQLite WAL/memtable/reconcile.
2. **`max_staleness_ms` FlightTicket hint** + connector policy (snapshot vs flush-first).
3. **Iceberg-materializer child 1** (`add-iceberg-materializer`) on posture-(i) input —
   already OpenSpec-drafted, gated on authoritative `Statistics.db` (#1729/#1728).

**Tier 2 (quarter, needs upstream Sidecar, still no engine change):**
4. **Sidecar flush/snapshot/components endpoints** (contribute to apache/cassandra-sidecar)
   → posture (i)+flush request-time freshness + Design-B component range reads.
5. **Repaired-status ingestion in CQLite** (read `repairedAt` from Statistics.db) →
   `add-materializer-repaired-gating` + primary-range dedup for RF-safe cluster materialization.

**Tier 3 (multi-quarter, research spike, in-JVM) — only if a hard requirement forces it:**
6. Pure-Java (or thin-FFI) **`CqliteFormat implements SSTableFormat`** to prove interop and
   register via `selected_format`. Bounded, upstreamable-in-principle experiment.
7. **`CqliteMemtable`/`Memtable.Factory`** spike (posture iii dual-write) with an
   out-of-process or `panic=unwind` FFI firewall. Treat as research; do not productize.

Never (fork-only, no seam, out of scope): substituting the read-path merge, `Tracker`/`View`
lifecycle, streaming, or repair.

---

## 6. Frank feasibility verdict

- **(a) Replacement engine inside Cassandra: NOT RECOMMENDED.** The seams that exist
  (memtable, format, compaction) are the parts CQLite already replicates; the parts that
  make an engine (merge, lifecycle, streaming, repair) are hardwired and fork-only. Best case
  is a pluggable memtable + format that inherits all of Cassandra's architecture and adds a
  JNI crash domain — high risk, marginal benefit. The Accord/TCM precedent argues for a
  *second store beside* the engine, not a *replacement of* it.
- **(b) Adjacent OLAP engine: RECOMMENDED, and mostly the path already in flight.** Posture
  (i) is live; posture (ii) CDC-tail is the freshness unlock and needs no core Cassandra
  change; the Iceberg epic is the durable OLAP sink. Low blast radius, exploits real seams,
  and this is where CQLite's differentiators (Rust scan, byte-parity reconcile, zero-ETL
  lakehouse) actually convert to value.

**One-line recommendation:** Position CQLite as *"an adjacent per-node OLAP replica — snapshot
reads today, CDC-tailed freshness next, Iceberg-materialized history — plus, optionally, an
upstreamable alternative `SSTableFormat`/`Memtable` for interop,"* and explicitly **not** as
a Cassandra storage-engine replacement.

---

## 7. NEEDS-DECISION list

1. **Freshness posture commitment.** Ship posture (ii) CDC-tail (eventual, CDC-lag-bounded,
   no core change) as the Q1 answer, or hold out for posture (i)+flush (request-time, needs
   Sidecar endpoints + write-stall cost)? These imply different consistency claims to users.
2. **Upstream vs vendor for any in-JVM work.** A pure-Java `SSTableFormat` is contributable;
   a Rust-FFI one is a fork/vendor build. Decide before investing in Tier 3.6, because it
   changes the entire engineering approach (and whether ASF would ever take it).
3. **Cluster dedup contract.** Primary-token-range pruning vs repaired-status gating (vs
   both) as the RF-dedup story for cluster-level materialization — and accept the prerequisite
   that CQLite must first learn to read `repairedAt` from Statistics.db.
4. **Sidecar dependency.** Are we willing to drive `/flush`, `/snapshot`, and Range-honoring
   `/components` endpoints into apache/cassandra-sidecar (verify they don't already exist —
   the surface index is unconfirmed against the real repo)? Tier 2 blocks on this.
5. **CDC double-write cost.** Posture (ii) reprocesses full write volume through CQLite per
   node. Acceptable operationally, or gate to selected keyspaces/tables?
6. **Iceberg epic open questions (already flagged in `design.md`):** OQ1 iceberg-rust v2
   equality-delete maturity (build vs adopt), OQ2 identifier-field-ineligible clustering
   types (position-delete degrade vs fail-closed), OQ3 static-column shape.
