# Synthesis Q1 — Memtable Freshness for the Arrow Flight / Trino / DataFusion Path

**Question.** When DataFusion or Trino reads a Cassandra node through CQLite's Arrow
Flight connector, the read sees only *flushed SSTables*. What must change or be added in
Apache Cassandra so an analytical read on one node reflects **all node-local state** —
memtable contents plus every SSTable — not just the latest flush?

**Audience.** Internal engineering scoping. Frank, code-anchored, effort-tiered.
**Targets** Cassandra **trunk** (7.0-dev, cassandra-6.0 merged 2026-07-03); flags where
Cassandra 5.0.x lacks the hook. Everything is on the table (in-JVM plugins, custom
Memtable impls, forks, sidecars, CDC tailing).

Status legend for effort: **S** ≤1wk, **M** ~1 month, **L** ~1 quarter, **XL** multi-quarter/ongoing.

---

## 0. What the code actually does today (verified)

- **Server is stateless and snapshot-pinned.** `CqliteFlightService` (`cqlite-flight/src/service.rs:107`)
  parses a `FlightTicket` (`ticket.rs:225`) that carries a **snapshot name**; `MergeProducer`
  (`producer.rs:262`) drives a k-way compaction-merge over paths enumerated by a
  `DirSource` (`producer.rs:86`). The read is atomic as of snapshot creation; the server
  never refreshes mid-stream.
- **Freshness is a client concern.** `Database::refresh()` exists
  (`cqlite-core/src/lib.rs:321` → `RefreshReport` in `storage/sstable/refresh.rs:55`),
  landed in #1749, but it only re-scans the SSTable directory. It cannot surface memtable
  state — there is nothing on disk to scan.
- **The memtable is invisible by construction.** Confirmed hard coupling #5 in the Flight
  index: `DirSource` discovers only `*-Data.db`; the Cassandra memtable is never a file.
- **CQLite already owns a memtable + WAL** (`storage/write_engine/memtable.rs:18`,
  `wal.rs`) with a scan surface (`memtable.rs:118 pub fn iter()`) and byte-parity LWW
  reconcile rules (`docs/compaction/byte-parity-rules.md`). This matters: CQLite can *hold
  and reconcile* an unflushed tail itself — the missing piece is a **source of that tail
  from Cassandra**, not the machinery to merge it.

**The gap is a source-of-truth gap, not a merge-engine gap.**

---

## 1. Verified seam map (spot-checked against source — with corrections)

### Cassandra trunk (`~/local_projects/cassandra`, branch `trunk`)

| Seam | Anchor | What it actually gives you | Verdict |
|---|---|---|---|
| **CEP-11 pluggable Memtable** | `db/memtable/Memtable.java:60` | `interface Memtable extends UnfilteredSource, ...`. A custom impl already exposes `rowIterator()` / `partitionIterator()` (from `UnfilteredSource`, `db/rows/UnfilteredSource.java:42,60`), plus `performSnapshot(String)` (`Memtable.java:449`). | **REAL, per-table, no fork** |
| **Memtable.Factory + config** | `Memtable.java:77`; `schema/MemtableParams.java:51,111` | Factory chosen via `CREATE TABLE ... WITH memtable='<name>'`, `<name>` defined under `memtable_configurations` in `cassandra.yaml`. Per-table opt-in. | **REAL config seam** |
| **Durable-memtable flags** | `Memtable.java:120 writesAreDurable()`, `:~110 writesShouldSkipCommitLog()` | A memtable that is its own durability store can tell Cassandra to skip/keep the commit log. Enables a dual-write memtable without double-logging. | **REAL** |
| **StorageHook** | `db/StorageHook.java:33` | Intercepts **only** `makeRowIterator[WithLowerBound]` — i.e. *per-SSTable* iterator creation. Configurable via `-Dcassandra.storage_hook`. | **CORRECTION (below)** |
| **Snapshot infra** | `service/snapshot/TakeSnapshotTask.java:56` (`:128` flushes memtable unless `skipFlush`), `SSTableReader.createLinks` (`:1183`), `SnapshotManager.java:61` | Hardlink set + manifest; flush-before-snapshot is the default. Format-agnostic. | **REAL, 5.0-compatible** |
| **Read-path merge** | `db/SinglePartitionReadCommand.java`, `db/ReadCommand.java:1174 InputCollector`, `UnfilteredRowIterators.merge()` | Memtable+SSTable union happens *inside* `ReadCommand.executeLocally`; `View` snapshot taken at query start. Merge operators are `final`; no listener hook. | **Not extensible without fork** |

**CORRECTION 1 (StorageHook).** The `read-path.md` and `cqlite-flight-trino.md` indexes
imply StorageHook could route analytical reads to CQLite / help freshness. Reading the
source: `StorageHook` only wraps the creation of a **single SSTable's** `UnfilteredRowIterator`
(`StorageHook.java:44,58`). It does **not** see the memtable, does **not** choose the
SSTable set, and does **not** touch `UnfilteredRowIterators.merge`. It is useless for Q1
freshness and near-useless for redirecting reads. Discard StorageHook from the option set.

**CORRECTION 2 (memtable ≠ SstableSource).** `cqlite-flight-trino.md` seam #1 says "a
memtable source could implement the `SstableSource` trait." The trait returns **file
paths** (`producer.rs:80 fn data_paths(&self) -> Result<Vec<PathBuf>>`), not row iterators.
So an in-memory tail cannot be plugged as an `SstableSource` directly — it must either
(i) be **materialized to a temp SSTable/Parquet file** that the existing path-based merge
consumes, or (ii) require a **new second seam** on `MergeProducer` that accepts an
in-memory `UnfilteredRowIterator`-equivalent. This shapes options (a), (c), (e) below:
the cheapest CQLite-side integration is *materialize-the-tail-to-a-file*, because the
merge/filter/Arrow pipeline is already path-driven and LWW-correct.

**CORRECTION 3 (5.0 sanctioned answer needs endpoints that don't exist).** The
`cassandra-sidecar-server-surface.md` index's "sanctioned answer" (flush → snapshot →
read) assumes Sidecar `POST .../flush` and `POST .../snapshots` endpoints. The CQLite Java
client (`SidecarClient.java:23`) implements **only** three GET endpoints (`/ring`,
`/token-range-replicas`, `/schema`). Flush/snapshot HTTP endpoints are **unverified in
apache/cassandra-sidecar** and not wired in CQLite. Treat "flush+snapshot over HTTP" as
*unbuilt on both sides* until the sidecar repo is pulled and confirmed.

### CQLite side (already present, reusable)

- `SstableSource` trait (`producer.rs:80`) + `DirSource` (`:86`) — path enumeration seam.
- `MergeProducer::with_spec/with_aggregation` (`producer.rs:282`) — filters/aggregation
  apply **post-merge**, so any added row source inherits pushdown for free.
- `WriteEngine` + `Memtable::iter()` + WAL replay — a ready place to *hold and reconcile*
  a Cassandra tail with LWW semantics.

---

## 2. Correctness framework (the part that actually decides the ranking)

A "fresh" read is only useful if it is also **correct**. Define the bar:

1. **Read-your-writes at t+ε.** A mutation acked by Cassandra at time *t* is visible to a
   Flight read that starts at *t+ε*. Only options that observe the write *before or at flush*
   (a, d) or observe it from an authoritative low-latency stream (b, c-with-sync) meet a
   tight ε. CDC/commitlog tailing has an inherent, bounded lag → **eventual** freshness.
2. **LWW correctness across the boundary.** CQLite's merge reconciles by `(column, cell_path)`
   on write-timestamp, tombstone-wins-on-tie (`byte-parity-rules.md`). Cassandra memtable
   cells carry the **same** authoritative timestamps. So *any* tail source that preserves
   per-cell timestamps + tombstones merges correctly with SSTable reads. The failure mode
   is not LWW math — it's **losing** the timestamp/tombstone (e.g. a naive "current value"
   memtable dump) or **double-counting** a mutation that is in both the tail and a
   just-flushed SSTable (generation/watermark dedup required).
3. **Tombstone + range-tombstone fidelity.** The tail must carry partition/row/range
   tombstones, not just live cells. A memtable snapshot or CDC record that drops deletes
   produces *resurrection* bugs. CQLite's tombstone shadowing is row/partition-complete but
   cell-path/complex-deletion partial (#844) — same caveat as its SSTable path, not worse.
4. **Atomicity / no torn reads.** A multi-cell mutation must not be half-visible. Options
   that snapshot a consistent memtable view (a via `performSnapshot`, d via flush) are
   atomic by construction. A live-iterator plug (b) must hold op-order/`View` stability for
   the scan; a CDC tail (c) must frame on mutation boundaries (CQLite's WAL already does
   CRC-framed whole-mutation entries — reuse it).
5. **Dedup at the flush boundary.** The instant the memtable flushes, its rows appear in a
   new SSTable *and* may still be in the tail. Every option needs a **watermark**
   (commit-log position / generation id) so the merge drops the tail below the flushed
   boundary. CQLite's snapshot-generation identities + Statistics.db `maxTimestamp`
   (#1728/#1729) are the natural watermark carrier.

---

## 3. Option analysis

### (a) Custom CEP-11 Memtable that exposes a snapshot/scan surface to CQLite

**Cassandra change:** *plugin-jar, no fork.* Implement `Memtable` + `Memtable.Factory`
(delegate storage to Cassandra's `TrieMemtable`/`SkipListMemtable` by subclass or wrap),
add a side export. Two sub-variants:
- **a1 — snapshot export.** On `performSnapshot(name)` (`Memtable.java:449`) *and/or* on a
  timer, serialize the live memtable partitions (via its own `partitionIterator`) to an
  **Arrow IPC / temp-SSTable file** in a well-known dir that CQLite's `DirSource` already
  scans. Zero new CQLite merge code (matches Correction 2 path-i).
- **a2 — live shared surface.** Expose the memtable's `UnfilteredPartitionIterator` over a
  **local socket / shared-memory / Arrow Flight** endpoint *inside the JVM*; CQLite's
  `MergeProducer` gains a second seam that consumes it (Correction 2 path-ii, more code).

Config: `CREATE TABLE ... WITH memtable='cqlite'` per analytical table, or a yaml default.
Trunk-only (`MemtableParams`, CEP-11). **5.0 has no such seam.**

- **Freshness/consistency:** a1 = periodic/opt-in snapshot → freshness = export cadence
  (sub-second feasible); atomic per export; LWW/tombstone-correct (real memtable iterator
  preserves timestamps + deletes). a2 = true real-time (t+ε ≈ iterator latency); atomic if
  it holds op-order for the scan. Both need the flush-boundary watermark (§2.5).
- **Blast radius:** the memtable is on the **write hot path**. A custom memtable that
  double-serializes on every flush/timer adds GC + CPU to OLTP. `writesAreDurable()` /
  `writesShouldSkipCommitLog()` let you avoid *double* durability but you generally want to
  keep the commit log (CDC/PITR). A bug here can corrupt live data. Per-table opt-in
  contains it to analytical tables.
- **Effort:** a1 = **M** (Java memtable-wrapper + serializer + CQLite reuses path merge).
  a2 = **L** (JVM↔Rust bridge, new producer seam, op-order lifetime management, JNI/socket).
- **Risks:** write-path perf regression; memtable lifecycle/flush-switch races
  (`Memtable.java` lifecycle contract is strict); JVM↔Rust FFI stability for a2; trunk-only
  (no 5.0 story); maintenance against an evolving CEP-11 API.

### (b) In-JVM plugin/agent exporting memtable via Arrow Flight from inside Cassandra

**Cassandra change:** *plugin-jar or javaagent, no fork* — but **no clean hook exists** for
the thing you actually want (a stream of writes or a whole-node live union). Candidate
seams and why each is weak:
- **Custom `Index` / `Indexer`:** fires on write, but the read-path index confirms the
  Indexer path is oriented to flush/compaction and secondary-index maintenance, not a
  general write-tap; contract is awkward and per-partition.
- **Trigger:** fires on `PartitionUpdate` at write time — usable as a write-tap, but
  triggers are coordinator-side, add latency to the write, and are widely considered a
  foot-gun.
- **QueryHandler / virtual table:** can serve reads but still has to *get* the data from
  somewhere — it does not itself see across memtable+SSTables without going through
  `ColumnFamilyStore`, and `CFS.select(View.selectLive())` union is exactly the internal,
  non-exported path (read-path hard coupling #4/#5).
- **javaagent:** can reach `ColumnFamilyStore` reflectively and call the *same* internal
  `getUnfilteredPartitionIterator`-style union the local read path uses, then serve it over
  an embedded Arrow Flight. This is the only (b) variant that yields a true fresh union —
  but it's **reflection into private internals**, brittle across versions, and re-implements
  Cassandra's read coordination.
- **Freshness/consistency:** potentially real-time and correct (it reuses Cassandra's own
  merge for the union). But it duplicates the read path and inherits all its couplings.
- **Blast radius:** runs inside the Cassandra JVM (heap pressure, crash risk = OLTP risk);
  reflection breaks on upgrade.
- **Effort:** **L–XL** (embedded Flight server in-JVM + reflection/internal-API glue +
  keeping it correct across trunk churn).
- **Risks:** highest coupling-to-internals of any option; version-fragile; the "clean write
  listener" the read-path index calls for **does not exist** — you are building it.
  *Recommendation: only the javaagent-reusing-internal-union variant is worth anything, and
  it is strictly worse than (a) for the same in-JVM risk.* De-prioritize.

### (c) CDC / commitlog-tailing sidecar that reconstructs the unflushed tail

**Cassandra change:** *config only* (enable `cdc_enabled` / CDC on the table, or point a
reader at the commit log). No plugin, no fork. A sidecar tails CDC segments (or the commit
log), decodes mutations, and feeds them to **CQLite's own WAL/memtable** (which already
holds + LWW-reconciles a tail, `write_engine/memtable.rs`). At query time the Flight server
merges: SSTable snapshot **+** CQLite-held tail, deduped at the flush watermark. Per
Correction 2, cleanest impl materializes the tail to a temp SSTable that `DirSource` picks
up, or adds the second producer seam.

- **Freshness/consistency:** **eventual** — bounded by CDC flush/segment-visibility lag
  (CDC becomes visible on commit-log sync, typically ≤ a few seconds; not t+ε). LWW/tombstone
  **correct** because CDC mutations carry authoritative timestamps + deletes and CQLite
  replays them through the same byte-parity reconcile it uses for SSTables. Atomicity: CDC
  records whole mutations; CQLite WAL framing is whole-mutation → no torn reads. Dedup:
  needs the commit-log-position ↔ flushed-generation watermark (§2.5); CDC segment offsets
  give it.
- **Blast radius:** **lowest of the real-freshness options.** Zero write-hot-path code;
  CDC is an existing, supported Cassandra feature; sidecar failure = stale (not down) reads.
  Works on **5.0 and trunk** (CDC predates CEP-11).
- **Effort:** **M** (CDC segment reader/decoder → CQLite mutation → existing WAL/memtable;
  watermark dedup; tail GC on flush). CQLite already has the hold-and-reconcile half.
- **Risks:** CDC lag = not real-time (fails a strict read-your-writes SLA); CDC must be
  enabled (disk overhead, per-table); commit-log/CDC binary format coupling (version-gated
  like the SSTable reader already is); back-pressure/space if the sidecar falls behind.

### (d) Aggressive / forced flush postures (flush-on-read, tiny memtables, nodetool cadence)

**Cassandra change:** *config only* (or `nodetool flush` / a `POST .../flush` sidecar
endpoint **that does not yet exist**, Correction 3). Variants: (d1) tiny `memtable_*` flush
thresholds so the memtable is nearly always small; (d2) periodic `nodetool flush` on a
cadence; (d3) synchronous flush-before-snapshot in the connector (extends today's snapshot
model — `TakeSnapshotTask` already flushes unless `skipFlush`, so a plain user snapshot is
*already* flush-then-snapshot).

- **Freshness/consistency:** freshness = flush cadence / snapshot latency. **Fully correct
  by construction** — after flush everything is an SSTable and the existing byte-parity
  merge is authoritative; no boundary-merge, no dedup, no new failure modes. d3 gives
  read-your-writes as of snapshot time (the write is flushed *before* the snapshot).
- **Blast radius:** **write amplification.** Tiny memtables → many small SSTables →
  compaction pressure, more files, worse OLTP read amplification, higher CPU/IO. Forced
  flush on cadence is a periodic latency/IO spike. This is trading OLTP health for OLAP
  freshness and does not scale to high write rates.
- **Effort:** **S** (config + connector already snapshots; a Sidecar `/flush` endpoint is a
  small addition *if/when* built). Works on **5.0 and trunk**.
- **Risks:** unbounded small-SSTable growth; compaction backlog; latency spikes; at high
  write volume the flush can't keep up and you're stale anyway. Good **stopgap / low-write
  tables**, bad steady state.

### (e) Hybrid: Flight fans out to a Cassandra-internal tail source + CQLite for SSTables

**Cassandra change:** depends on which tail source — this is (a) or (c) as the *tail
provider*, wired into a single Flight query that merges tail + SSTable snapshot with CQLite
reconcile. `MergeProducer` gets a second row source (Correction 2 path-ii) or the tail is
materialized to a file (path-i).

- **Freshness/consistency:** = the tail source's guarantee (real-time if (a2), eventual if
  (c)); correctness is CQLite's LWW merge, which is already the byte-parity oracle. Dedup at
  the flush watermark is the one shared hard part and it lives in the merge either way.
- **Blast radius / effort / risks:** = the chosen tail source + the merge seam. The
  *incremental* cost over (a)/(c) is the second producer seam + watermark dedup (**S–M**).
- This is not a competing option so much as **the delivery vehicle** for (a) or (c): keep
  the SSTable read exactly as it is, add one tail source, merge with the engine we already
  trust.

---

## 4. Ranked recommendation (staged)

The correctness framework (§2) collapses the field: freshness is easy, **correct** freshness
needs authoritative timestamps + tombstones + a flush-boundary watermark. Rank by
`(correctness × freshness) / (blast-radius × effort)`, and note 5.0-vs-trunk coverage.

1. **Stage 0 — ship now, both 5.0 & trunk: snapshot-flush hybrid (d3 + existing #1749
   refresh).** The connector already creates a snapshot; a plain *user* snapshot flushes
   first (`TakeSnapshotTask:128`), so "flush → snapshot → read" is achievable today with a
   `nodetool`/JMX flush (or the small Sidecar `/flush` endpoint once confirmed). Correct by
   construction, near-zero new code, works everywhere. **Accept the flush-latency cost as
   the baseline "fresh" mode.** Effort **S**. This is the honest current answer to Q1.

2. **Stage 1 — steady-state freshness without write-path risk: CDC-tail sidecar (c) via the
   hybrid merge (e).** Reuse CQLite's WAL/memtable to hold the tail; materialize it to a
   temp file the path-based merge already consumes (Correction 2 path-i); dedup at the
   commit-log/generation watermark. **Eventual (bounded-lag) freshness, LWW-correct, lowest
   blast radius, works on 5.0 and trunk.** Effort **M**. This is the recommended primary
   investment — it decouples freshness from flush cost and needs *no* Cassandra code, only
   CDC enabled.

3. **Stage 2 — real-time freshness on trunk: custom CEP-11 Memtable (a1) exporting via
   snapshot/Arrow-IPC, merged via (e).** Only pursue if a strict read-your-writes SLA
   (tight t+ε) is a real product requirement that CDC lag cannot meet. Per-table opt-in
   contains blast radius; a1 (periodic/`performSnapshot` export to a file) is far cheaper
   and safer than a2 (live in-JVM iterator) — start there. Effort **M** (a1) escalating to
   **L** (a2). **Trunk-only.**

**Rejected / de-prioritized:**
- **(b) in-JVM Flight/agent** — highest coupling to Cassandra internals, version-fragile,
  and the "clean write listener" it needs does not exist; the only viable variant
  (reflection reusing the internal union) is strictly worse than (a) for equal in-JVM risk.
- **StorageHook** — verified to be a per-SSTable-iterator hook only; irrelevant to Q1.
- **(d1/d2) tiny memtables / blind flush cadence** — write amplification makes them a
  stopgap for low-write tables, not a steady-state answer.

**One-line answer to Q1.** *Nothing must change in Cassandra to get correct-but-flush-latency
freshness today (Stage 0). For steady-state freshness with no write-path risk, enable CDC
and tail it into CQLite's existing WAL/memtable, merging at the flush watermark (Stage 1,
5.0+). For real-time freshness, a per-table CEP-11 custom Memtable that snapshots its
contents to CQLite is the only clean in-JVM path, and it is trunk-only (Stage 2).*

---

## 5. NEEDS-DECISION

1. **Freshness SLA.** Is the product requirement *bounded-lag* (seconds, CDC is fine →
   Stage 1) or *read-your-writes t+ε* (→ Stage 2, trunk-only, write-path risk)? This single
   call decides whether we ever build a custom Memtable.
2. **Sidecar flush/snapshot endpoints.** Pull `apache/cassandra-sidecar` and confirm whether
   `POST .../flush`, `POST/GET/DELETE .../snapshots`, and Range-honoring component reads
   exist. Stage 0's ergonomics and Design-B's remote reads both depend on this (currently
   *unverified on both sides*, Correction 3).
3. **CDC enablement cost.** Are operators willing to enable CDC (per-table disk overhead,
   segment retention) on analytical tables? If not, Stage 1 is blocked and Stage 0 (flush)
   becomes the only 5.0 answer.
4. **Watermark source.** Ratify commit-log-position ↔ flushed-generation as the dedup
   watermark, and confirm #1728/#1729 (authoritative Statistics.db `maxTimestamp` /
   `maxLocalDeletionTime`) land first — the tail/SSTable boundary dedup fails closed without
   it (same dependency the Iceberg materializer already declares).
5. **Producer seam shape.** Decide Correction-2 path-i (materialize tail to temp
   SSTable/Parquet, zero new merge code) vs path-ii (new in-memory row source on
   `MergeProducer`). Path-i is cheaper and keeps one merge path; pick it unless Stage 2/a2
   forces path-ii.
6. **Tombstone-fidelity ceiling.** The tail inherits CQLite's row/partition-complete but
   cell-path/complex-deletion-*partial* tombstone shadowing (#844). Is that acceptable for
   the freshness claim, or must #844 close first?

---

## 6. Corrections made to the Haiku indexes (for the final report)

- **StorageHook is not a freshness or read-redirect seam.** Source shows it only wraps
  per-SSTable `UnfilteredRowIterator` creation (`db/StorageHook.java:44,58`); it cannot see
  the memtable or the SSTable set. Remove it from Q1/Q2 option sets.
- **A memtable cannot implement `SstableSource`.** The trait returns `Vec<PathBuf>`
  (`producer.rs:80`), not iterators; an in-memory tail must be materialized to a file or
  given a new producer seam. Reframes options (a/c/e) around "materialize the tail."
- **The 5.0 "flush+snapshot over HTTP" answer is unbuilt on both sides.** CQLite's
  `SidecarClient` implements only 3 GET endpoints; flush/snapshot POST endpoints are
  unverified in apache/cassandra-sidecar. `nodetool`/JMX flush is the only confirmed lever.
- **CEP-11 is richer than indexed (upgrades option (a)'s feasibility).** Verified per-table
  config seam (`CREATE TABLE ... WITH memtable='<name>'` → `MemtableParams.java:111`), plus
  `writesAreDurable()`/`writesShouldSkipCommitLog()` (`Memtable.java:120`) and
  `performSnapshot(String)` (`:449`). A custom Memtable already exposes a scan surface via
  `UnfilteredSource` — no fork needed for option (a) on trunk.
</content>
</invoke>
