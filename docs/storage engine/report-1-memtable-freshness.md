# Report 1 — Memtable Freshness for the Arrow Flight / Trino / DataFusion Path

**Date:** 2026-07-03
**Audience:** internal engineering scoping — frank, decision-oriented, code-anchored.
**Targets:** Apache Cassandra **trunk** (7.0-dev; cassandra-6.0 merged 2026-07-03), with
5.0.x flagged wherever a hook is missing. CQLite anchors are against the current `main`.

## Verdict

The memtable-freshness gap is a **source-of-truth gap, not a merge-engine gap**: CQLite's
Flight server already k-way-merges SSTables with byte-parity LWW reconcile
(`cqlite-flight/src/producer.rs:262`), and CQLite already owns a WAL + memtable that can
hold and reconcile an unflushed tail (`cqlite-core/src/storage/write_engine/memtable.rs:118`)
— what is missing is any way to get Cassandra's unflushed tail out of the JVM, because the
`DirSource` only sees `*-Data.db` files and the memtable is never a file. Correct freshness
(not merely fresh) requires three properties from any tail source — authoritative per-cell
timestamps, tombstone/range-tombstone fidelity, and a flush-boundary watermark for dedup —
and that framework collapses the option field to a three-stage answer: **Stage 0 (ship now,
5.0+trunk)** flush→snapshot→read, which is correct-by-construction and near-zero new code
because a plain user snapshot already flushes first (`TakeSnapshotTask.java:128`); **Stage 1
(the primary investment, 5.0+trunk)** a CDC/commitlog-tailing sidecar feeding CQLite's
existing WAL/memtable, config-only on Cassandra, LWW-correct, with **eventual (bounded-lag)**
freshness at effort M; and **Stage 2 (only if a strict read-your-writes SLA emerges,
**available on 5.0**: CEP-11 shipped in 4.1)** a custom CEP-11 Memtable that exports its contents via
`performSnapshot`/Arrow-IPC to a file CQLite merges. Rejected: StorageHook (verified: wraps
only per-SSTable iterator creation, cannot see the memtable), the in-JVM Flight/agent
family (highest coupling to private internals; the clean write-listener it needs does not
exist), and tiny-memtable/blind-flush postures (write amplification; stopgap only).
**Nothing must change in Cassandra to get correct-but-flush-latency freshness today; enabling
CDC is the only Cassandra-side change needed for steady-state freshness; only a tight-t+ε
SLA would ever justify in-JVM work.**

---

## 1. How Cassandra (and CQLite) actually work here — the mechanism map

Every claim in this section was spot-checked against source during synthesis; anchors are
`file:line` in `~/local_projects/cassandra` (trunk) or the CQLite repo.

### 1.1 The CQLite read path is stateless, path-driven, and snapshot-pinned

- `CqliteFlightService` (`cqlite-flight/src/service.rs:107`) parses a `FlightTicket`
  (`cqlite-flight/src/ticket.rs:225`) carrying a **snapshot name**; the read is atomic as of
  snapshot creation and never refreshes mid-stream.
- `MergeProducer` (`cqlite-flight/src/producer.rs:262`) drives a k-way compaction-merge over
  paths enumerated by an `SstableSource`. The trait is **path-based**:
  `fn data_paths(&self) -> Result<Vec<PathBuf>>` (`producer.rs:80`); the shipped `DirSource`
  (`producer.rs:86`) lists `*-Data.db` under a table dir. **Consequence: an in-memory tail
  cannot be plugged in as an `SstableSource` — it must be materialized to a file, or the
  producer needs a new second (in-memory row) seam.** This is a correction to the
  flight/trino index, and it *simplifies* integration: materialize-to-file reuses the whole
  existing merge/filter/Arrow pipeline unchanged.
- Filters and aggregation pushdown apply **post-merge** (`producer.rs:282`
  `with_spec`/aggregation plan), so any added row source inherits pushdown for free.
- `Database::refresh()` (`cqlite-core/src/lib.rs:321` → `RefreshReport`,
  `storage/sstable/refresh.rs:55`, landed in #1749) re-scans the SSTable directory only. It
  can never surface memtable state — there is nothing on disk to scan.

### 1.2 CQLite already has the "hold and reconcile a tail" half

- `Memtable::iter()` (`cqlite-core/src/storage/write_engine/memtable.rs:118`) is a scan
  surface over `(DecoratedKey, &[Mutation])`; the WAL frames whole mutations with CRC.
- Reconcile semantics are the byte-parity LWW rules used for compaction parity vs Cassandra
  (`docs/compaction/byte-parity-rules.md`): per `(column, cell_path)` on write-timestamp,
  tombstone-wins-on-tie. Cassandra memtable/CDC mutations carry the **same** authoritative
  timestamps, so a tail replayed through this machinery merges correctly with SSTable reads.
- Known ceiling: tombstone shadowing is row/partition-complete but cell-path /
  complex-deletion **partial** (#844) — the tail inherits exactly the same caveat as the
  SSTable path, no worse.

### 1.3 Cassandra's seams, trunk vs 5.0

| Seam | Anchor | What it gives you | Trunk/5.0 |
|---|---|---|---|
| **CEP-11 pluggable Memtable** | `db/memtable/Memtable.java:60` (`interface Memtable extends UnfilteredSource…`); scan via `UnfilteredSource` (`db/rows/UnfilteredSource.java:42,60`); `performSnapshot(String)` (`Memtable.java:449`) | A custom impl is a first-class per-table plugin with its own partition/row iterators and a snapshot hook | **5.0 and trunk.** CEP-11 (CASSANDRA-17034) shipped in 4.1; verified present on `origin/cassandra-5.0` (`Memtable.java:58` interface, `:426` `performSnapshot`) |
| **Memtable.Factory + config** | `Memtable.java:77`; `schema/MemtableParams.java:111` (`MemtableParams.get(key)`, keys from `memtable_configurations` in `cassandra.yaml`) | `CREATE TABLE … WITH memtable='<name>'` — per-table opt-in, no fork | 5.0 and trunk (verified on `origin/cassandra-5.0` `MemtableParams.java:111`) |
| **Durability flags** | `Memtable.java` `writesAreDurable()` / `writesShouldSkipCommitLog()` (defaults `false`, ~`:105–:125`) | A memtable that is its own durability store can skip commit-log replay without losing CDC/PITR (docs explicitly note writes are still logged for CDC unless both flags set) | 5.0 and trunk |
| **Snapshot infra** | `service/snapshot/TakeSnapshotTask.java:128` (flushes the current memtable unless `options.skipFlush`); `SSTableReader.createLinks` (`:1183`); `SnapshotManager.java:61` | Hardlink set + manifest, **flush-before-snapshot is the default** — a plain user snapshot is already "flush then snapshot" | **5.0 and trunk** |
| **CDC / commit log** | `cdc_enabled` + per-table CDC; commit-log segments become CDC-visible on sync | An authoritative, whole-mutation, timestamped stream of every write — no plugin, config only | **5.0 and trunk** |
| **StorageHook** | `db/StorageHook.java:33` — interface is exactly `reportWrite`, `reportRead`, `makeRowIterator[WithLowerBound]` per **single SSTable**; impl chosen by `-Dcassandra.storage_hook` | **Not a freshness seam.** It wraps creation of one SSTable's `UnfilteredRowIterator`; it does not see the memtable, does not choose the SSTable set, does not touch the merge | Both, but irrelevant |
| **Read-path merge** | `db/ReadCommand.java:1174` `InputCollector`; `UnfilteredRowIterators.merge()`; `View` snapshot at query start | The memtable+SSTable union happens *inside* `ReadCommand.executeLocally`; merge operators are `final`, no listener hook | **Not extensible without a fork**, both versions |
| **Sidecar HTTP** | CQLite's `SidecarClient.java:23` implements only three **GET**s (`/ring`, `/token-range-replicas`, `/schema`) | Flush/snapshot **POST** endpoints are **unverified** in apache/cassandra-sidecar and unwired in CQLite — treat "flush+snapshot over HTTP" as unbuilt on both sides; `nodetool`/JMX flush is the only confirmed lever | Unverified |

### 1.4 The correctness bar (this decides the ranking)

A "fresh" read is only useful if it is also **correct**. Five requirements:

1. **Read-your-writes at t+ε** — only sources that observe the write before/at flush (custom
   memtable, forced flush) meet a tight ε; CDC tailing has inherent bounded lag → eventual.
2. **LWW correctness across the boundary** — any tail source that preserves per-cell
   timestamps + tombstones merges correctly through CQLite's byte-parity reconcile. The
   failure mode is not LWW math; it is *losing* the timestamp/tombstone (a naive
   "current value" dump) or *double-counting* a mutation present in both tail and a
   just-flushed SSTable.
3. **Tombstone + range-tombstone fidelity** — a tail that drops deletes produces
   resurrection bugs. (Ceiling: #844, same as the SSTable path.)
4. **Atomicity / no torn reads** — memtable snapshot and flush are atomic by construction; a
   CDC tail must frame on mutation boundaries (CQLite's WAL already does, CRC-framed).
5. **Dedup at the flush boundary** — the instant a memtable flushes, its rows exist in a new
   SSTable *and* may still be in the tail. Every option needs a watermark
   (commit-log position ↔ flushed generation); CQLite's snapshot-generation identities +
   authoritative Statistics.db timestamps (#1728/#1729) are the natural carrier.

---

## 2. Option analysis

### (a) Custom CEP-11 Memtable exposing a snapshot/scan surface to CQLite

**Cassandra change: plugin-jar, no fork (Cassandra 4.1+ — works on 5.0).** Implement `Memtable` +
`Memtable.Factory` (wrap/delegate to `TrieMemtable`), register under
`memtable_configurations`, opt in per table with `WITH memtable='cqlite'`
(`MemtableParams.java:111`). Two sub-variants:

- **a1 — snapshot export (recommended shape).** On `performSnapshot(name)`
  (`Memtable.java:449`) and/or a timer, serialize live partitions via the memtable's own
  iterator to an **Arrow IPC / temp-SSTable file** in a well-known dir the existing
  `DirSource` scans. Zero new CQLite merge code.
- **a2 — live surface.** Expose the memtable's iterator over local socket / shared memory /
  in-JVM Flight; CQLite's `MergeProducer` gains a second, in-memory row seam.

**Freshness/consistency:** a1 = export cadence (sub-second feasible), atomic per export,
LWW/tombstone-correct (real memtable iterator preserves timestamps + deletes). a2 = true
t+ε. Both need the §1.4-5 watermark.
**Blast radius:** the memtable is the **write hot path** — double-serialization adds GC/CPU
to OLTP; a bug here can corrupt live data; strict memtable lifecycle/flush-switch contract.
Per-table opt-in contains it. `writesAreDurable()`/`writesShouldSkipCommitLog()` let you
avoid double durability while keeping the commit log for CDC/PITR.
**Effort:** a1 = **M**; a2 = **L** (JVM↔Rust bridge, new producer seam, op-order lifetimes).
**Risks:** write-path perf regression; tracking an evolving
CEP-11 API; FFI stability for a2.

### (b) In-JVM plugin/agent (Index, trigger, QueryHandler, virtual table, javaagent) exporting via Arrow Flight from inside Cassandra

**Cassandra change: plugin-jar or javaagent, no fork — but no clean hook exists** for what
you actually want (a write stream or a live whole-node union):

- **Custom `Index`/`Indexer`** — fires on write but is oriented to secondary-index
  maintenance and flush/compaction; awkward, per-partition contract, not a general tap.
- **Trigger** — fires on `PartitionUpdate` at write time, but adds latency on the write
  path and is a well-known foot-gun.
- **QueryHandler / virtual table** — can *serve* reads but still has to get the union from
  `ColumnFamilyStore.select(View.selectLive())` — exactly the internal, non-exported path.
- **javaagent** — can reflectively reach `ColumnFamilyStore` and call the same internal
  memtable+SSTable union the local read path uses, then serve it over an embedded Flight
  server. The only variant that yields a true fresh union — via reflection into private
  internals, brittle across versions, re-implementing read coordination.

**Freshness/consistency:** potentially real-time and correct (reuses Cassandra's own merge).
**Blast radius:** runs inside the Cassandra JVM — heap pressure and crash risk are OLTP
risk; reflection breaks on upgrade. **Effort: L–XL. Risks:** highest coupling-to-internals
of any option; the clean write listener this option wants **does not exist** — you would be
building it. Strictly worse than (a) for the same in-JVM risk. **De-prioritize.**

### (c) CDC / commitlog-tailing sidecar reconstructing the unflushed tail on the CQLite side

**Cassandra change: config only** (`cdc_enabled` + per-table CDC, or a commit-log reader).
A sidecar tails CDC segments, decodes mutations, and feeds **CQLite's own WAL/memtable** —
the hold-and-reconcile half already exists (`write_engine/memtable.rs`, CRC-framed WAL,
byte-parity LWW replay). At query time the Flight server merges SSTable snapshot + the
CQLite-held tail, deduped at the flush watermark. Cleanest wiring per §1.1: materialize the
tail to a temp SSTable/Arrow file the path-based `DirSource` picks up.

**Freshness/consistency: eventual** — bounded by CDC segment-visibility lag (visible on
commit-log sync; typically ≤ a few seconds, not t+ε). LWW/tombstone-**correct**: CDC
mutations carry authoritative timestamps and deletes and replay through the same reconcile
used for SSTables. Atomic (whole-mutation records both sides). Dedup: commit-log-position ↔
flushed-generation watermark; CDC segment offsets provide it.
**Blast radius: lowest of the real-freshness options.** Zero write-hot-path code; CDC is a
supported feature; sidecar failure degrades to *stale*, not *down*. **Works on 5.0 and
trunk.**
**Effort: M** (CDC segment reader/decoder → CQLite mutation → existing WAL/memtable +
watermark dedup + tail GC on flush).
**Risks:** CDC lag fails a strict read-your-writes SLA; CDC must be enabled (disk overhead,
retention); commit-log/CDC binary-format coupling (version-gate it like the SSTable reader
already is); back-pressure/space if the tailer falls behind.

### (d) Aggressive / forced flush postures

**Cassandra change: config only** (or `nodetool flush`/JMX; a Sidecar `POST …/flush`
endpoint **does not verifiably exist yet**, §1.3). Variants: (d1) tiny `memtable_*`
thresholds; (d2) periodic `nodetool flush`; (d3) **synchronous flush-before-snapshot in the
connector** — and note a plain user snapshot *already* flushes first
(`TakeSnapshotTask.java:128`, skipped only with `skipFlush`).

**Freshness/consistency:** freshness = flush/snapshot cadence; **fully correct by
construction** — after flush everything is an SSTable and the existing byte-parity merge is
authoritative; no boundary merge, no dedup, no new failure modes. d3 gives read-your-writes
as of snapshot time.
**Blast radius: write amplification.** Tiny memtables → many small SSTables → compaction
pressure and OLTP read-amp; cadence flushes are periodic IO/latency spikes; at high write
volume the flush cannot keep up and you are stale anyway.
**Effort: S.** Works on 5.0 and trunk.
**Risks:** trading OLTP health for OLAP freshness. d3 is a fine baseline; d1/d2 are
stopgaps for low-write tables only.

### (e) Hybrid: Flight fans out to a Cassandra tail source + CQLite for SSTables

Not a competing option — **the delivery vehicle** for (a) or (c). Keep the SSTable read
exactly as-is; add one tail source; merge with the reconcile engine we already trust.
Freshness = the tail source's guarantee (real-time for a2, eventual for c); correctness is
CQLite's LWW merge. The incremental cost over (a)/(c) is the seam choice (materialize-to-file
vs a new in-memory row source on `MergeProducer`) plus watermark dedup: **S–M**.

### Comparison table

| Option | Cassandra change class | Freshness guarantee | Correctness (LWW/tombstones/dedup) | 5.0? | Effort | Dominant risk |
|---|---|---|---|---|---|---|
| **d3** flush→snapshot→read | none/config (`nodetool`/JMX; snapshot already flushes) | Snapshot-time read-your-writes; stale between snapshots | Correct by construction (all state is SSTables) | **Yes** | **S** | Flush latency + IO on each refresh |
| **c** CDC-tail sidecar → CQLite WAL/memtable (via e) | **config only** (enable CDC) | **Eventual**, bounded lag (~commit-log sync, seconds) | Correct: authoritative ts + deletes, whole-mutation framing; needs watermark dedup | **Yes** | **M** | CDC lag; CDC enablement cost; format coupling |
| **a1** CEP-11 memtable, snapshot/Arrow-IPC export (via e) | plugin-jar (4.1+/5.0) | Export cadence (sub-second feasible) | Correct: real memtable iterator; needs watermark dedup | **No** | **M** | Write-hot-path perf; memtable lifecycle races |
| **a2** CEP-11 memtable, live surface | plugin-jar + new CQLite producer seam | Real-time t+ε | Correct if op-order held for the scan | **No** | **L** | JVM↔Rust FFI; lifetime management |
| **b** in-JVM agent/Flight | plugin/javaagent (reflection into internals) | Real-time possible | Correct only by re-using internal union | Partly | **L–XL** | Version-fragile; OLTP JVM risk; no clean hook exists |
| **d1/d2** tiny memtables / flush cadence | config | Cadence-bound | Correct | Yes | **S** | Write amplification, compaction backlog |
| StorageHook (any use) | — | — | — | — | — | **Rejected: per-SSTable iterator hook only; cannot see the memtable** (`StorageHook.java:33`) |

---

## 3. Recommendation (staged)

Rank by `(correctness × freshness) / (blast radius × effort)`, honoring 5.0 coverage:

**Stage 0 — ship now (5.0 + trunk): snapshot-flush posture (d3) + existing #1749 refresh.**
The connector already reads snapshot-pinned; a plain user snapshot flushes first
(`TakeSnapshotTask.java:128`). "Flush → snapshot → read" is achievable today with a
`nodetool`/JMX flush and zero new merge code. Accept flush latency as the baseline "fresh"
mode; document it as the honest current answer to Q1.
*Prototype evidence required:* measured end-to-end refresh latency (flush + snapshot +
first-batch) on a loaded node; measured flush-induced OLTP p99 write-latency delta at a
realistic cadence (e.g. every 30s/60s); confirmation the ticketed snapshot name pins the
read atomically under concurrent flushes.

**Stage 1 — primary investment (5.0 + trunk): CDC-tail sidecar (c) delivered via the hybrid
merge (e), path-i.** Tail CDC into CQLite's existing WAL/memtable; materialize the tail to
a temp file the path-based merge already consumes; dedup at the commit-log-position ↔
flushed-generation watermark. Eventual (bounded-lag) freshness, LWW-correct, lowest blast
radius, **no Cassandra code — only CDC enabled**. Effort M.
*Prototype evidence required:* (1) a value-parity harness proving CDC-replayed tail + SSTable
merge equals a real Cassandra `SELECT` (including deletes/range tombstones — the semantic
oracle, not a physical dump; cf. #1742); (2) measured tail lag distribution under write
load (acked-write → Flight-visible); (3) a flush-boundary torture test: flush mid-stream,
assert no duplicate and no lost row across the watermark; (4) tailer restart/backlog
recovery without resurrection.

**Stage 2 — only if a strict read-your-writes SLA is a real product requirement
(works on 5.0 — CEP-11 shipped in 4.1): custom CEP-11 Memtable (a1).** Per-table `WITH memtable='cqlite'` export via
`performSnapshot`/timer to Arrow-IPC files, merged via the same path-i seam. Start with a1;
escalate to a2's live surface only if export cadence provably cannot meet the SLA.
*Prototype evidence required:* OLTP write-path overhead (throughput + p99) of the wrapping
memtable vs `TrieMemtable` at target write rates; export-cadence-vs-ε curve; lifecycle
correctness under flush switch (no torn exports, no lost tail at generation handoff).

**Do not build:** option (b) in any variant (strictly worse than (a) at equal in-JVM risk);
anything on StorageHook; d1/d2 beyond a documented low-write-table stopgap.

---

## 4. NEEDS-DECISION (owner calls)

1. **Freshness SLA.** Is the product requirement *bounded-lag* (seconds — CDC is fine,
   Stage 1) or *read-your-writes t+ε* (Stage 2, 5.0-compatible, write-path risk)? This single
   call decides whether a custom Memtable is ever built.
2. **Sidecar flush/snapshot endpoints.** Pull `apache/cassandra-sidecar` and confirm whether
   `POST …/flush`, snapshot CRUD, and Range-honoring component reads exist. Stage 0
   ergonomics (and the remote-read Design B) depend on it; currently **unverified on both
   sides** — CQLite's `SidecarClient` has only 3 GET endpoints.
3. **CDC enablement cost.** Are operators willing to enable CDC (per-table disk overhead,
   segment retention) on analytical tables? If not, Stage 1 is blocked and Stage 0 (flush)
   is the only 5.0 answer.
4. **Watermark source.** Ratify commit-log-position ↔ flushed-generation as the dedup
   watermark, gated on #1728/#1729 (authoritative Statistics.db `maxTimestamp` /
   `maxLocalDeletionTime`) landing first — the tail/SSTable boundary dedup fails closed
   without it (same dependency the Iceberg materializer declares).
5. **Producer seam shape.** Materialize-tail-to-file (path-i: zero new merge code, one merge
   path) vs a new in-memory row source on `MergeProducer` (path-ii). Pick path-i unless
   Stage 2/a2 forces path-ii.
6. **Tombstone-fidelity ceiling.** The tail inherits CQLite's cell-path/complex-deletion
   partial shadowing (#844). Acceptable for the freshness claim, or must #844 close first?

---

## 5. Appendix — drill-down pointers

All paths relative to `docs/storage engine/` (note the space — quote in shells).

- **`synthesis-q1-freshness.md`** — the full intermediate synthesis this report distills:
  §1 verified seam map with the three index corrections (StorageHook, `SstableSource`
  path-based, unbuilt sidecar endpoints) and the CEP-11 upgrade; §2 the five-part
  correctness framework; §6 the correction log against the Haiku indexes.
- **`cqlite-write-engine.md`** — CQLite write-engine index: WriteEngine/Memtable/WAL
  class map ("Key Classes & Interfaces"), pluggability seams and hard couplings, the
  "Q1 Relevance: Analytical Freshness" section, and trunk-vs-5.0 deltas observed from the
  CQLite side.
- **`cassandra-sidecar-server-surface.md`** — Sidecar HTTP contract index: the three
  implemented GET endpoints, the *proposed* Design-B component range-read path, the
  "Sanctioned Q1 Answer (NOT YET IMPLEMENTED)" flush+snapshot flow, and "Outstanding
  Questions to Verify Against apache/cassandra-sidecar" (feeds NEEDS-DECISION #2).
- **Referenced-but-not-committed indexes** (`read-path.md`, `cqlite-flight-trino.md`, from
  the research scratchpads): their load-bearing claims are either corrected or absorbed in
  `synthesis-q1-freshness.md` §1/§6 — treat the synthesis as authoritative over them.
- **Source anchors for re-verification (trunk `~/local_projects/cassandra`):**
  `db/memtable/Memtable.java` (:60 interface, :449 `performSnapshot`, durability flags
  ~:105–:125), `schema/MemtableParams.java:111`, `db/StorageHook.java:33`,
  `service/snapshot/TakeSnapshotTask.java:128`, `db/ReadCommand.java:1174`,
  `db/memtable/Memtable_API.md` (CEP-11 doc).
- **CQLite anchors:** `cqlite-flight/src/producer.rs` (:80 `SstableSource`, :86 `DirSource`,
  :262 `MergeProducer`), `cqlite-flight/src/service.rs:107`, `cqlite-flight/src/ticket.rs:225`,
  `cqlite-core/src/lib.rs:321` (`Database::refresh`), `cqlite-core/src/storage/sstable/refresh.rs:55`,
  `cqlite-core/src/storage/write_engine/memtable.rs:118`, `docs/compaction/byte-parity-rules.md`.
- **Related issues:** #1749 (refresh contract), #1728/#1729 (authoritative Statistics.db
  timestamps — watermark dependency), #844 (tombstone-shadowing ceiling), #1742 (semantic
  oracle for value parity — reuse for the Stage-1 harness).
