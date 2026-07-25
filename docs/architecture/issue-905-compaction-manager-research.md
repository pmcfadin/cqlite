# Issue #905 — Compaction Manager: Exploration Research (2026-07-04)

**Issue:** [#905](https://github.com/pmcfadin/cqlite/issues/905) — Support a compaction manager
**Companion:** `docs/compaction-manager-design.md` (draft design; predates M5 landing)
**Status:** Research synthesis — no epic/children filed yet (owner exploring)

Two research waves. Wave 1 (3 passes): UCS source read (trunk), CQLite reuse audit,
ecosystem prior art. Wave 2 (5 passes, deep dive): UCS verified against `cassandra-5.0`
(`origin/cassandra-5.0` @ `464b2e54f4`) + trunk diff, lifecycle txn-log protocol, purge/overlap
semantics, CompactionManager scheduling + standalone-tool template, and a CQLite
product-direction alignment analysis. This doc is the consolidated record; a future
refresh/grooming pass starts here.

---

## 1. Verdict on the design doc's premise

**Still valid, strengthened.** As of mid-2026 no tool in the Cassandra ecosystem performs
offline SSTable *merge* compaction:

- Cassandra's offline family (`sstablesplit`, `sstableofflinerelevel`, `sstablescrub`,
  `sstableupgrade`) rewrites/relevels/splits — never merges. `nodetool compact` is online-only.
- Instaclustr `ic-tools purge` runs *fake* compactions to estimate reclaimable bytes; no output.
- Medusa dedups backup uploads; never compacts. Its own docs recommend online major
  compaction to shrink snapshots — the exact workload an offline compactor relocates.
- **Strongest signal:** ScyllaDB has an OPEN, unshipped first-party feature request for
  exactly this — `scylla-sstable compact`, [scylladb/scylladb#20531](https://github.com/scylladb/scylladb/issues/20531)
  (Sept 2024, still open). Both engine teams see the gap; neither has filled it.
- **The simulator is the most defensibly novel piece.** Existing "simulators" are generic
  analytical models (VLDB'21 LSM design-space) or toy teaching tools (skyzh mini-lsm).
  Nothing replays a strategy against a real snapshot's on-disk metadata.
- **Offload:** proven pattern elsewhere (RocksDB `CompactionService`, Rockset, CaaS-LSM
  SIGMOD'24, D2Comp) but unclaimed for Cassandra — no CEP. Keep as forward-looking
  groundwork. Sidecar (CEP-40 era) is a plausible future host.
- **UCS adoption nuance:** UCS is NOT the OSS 5.0 default (STCS still is; CEP-26 plans the
  flip after a testing period; vendors like Instaclustr already default to UCS). Say
  "converging on," not "converged."
- No filed Cassandra JIRA asks for offline compaction — demand is real but expressed as
  snapshot/backup disk-blowup pain (widely documented), i.e. diffuse.

## 2. UCS ground truth — VERIFIED against cassandra-5.0

Wave-2 diffed every UCS file between `origin/cassandra-5.0` and trunk.
**The deterministic core math is byte-identical on both refs** — the earlier trunk-only
caveat is CLOSED. Path prefix: `src/java/org/apache/cassandra/db/compaction/`.

### Deterministic core (replicate this; identical 5.0 ↔ trunk)

- **Density** = `onDiskLength / rangeSpanned` (`ShardManager.density`, ShardManager.java:139).
  `rangeSpanned` prefers **`StatsMetadata.tokenSpaceCoverage` — a stored double in
  Statistics.db** (authoritative; no-heuristics-friendly), falling back to first/last-key
  token span. Floor guard `MINIMUM_TOKEN_COVERAGE = 2^-48`.
- **Levels** geometric in fanout F: level max density = prev max × F, from
  `baseSstableSize(F) = max(1MiB, flushSize) * (1 − 0.9/F)`. MAX_LEVELS=32.
  **UCS ignores the stored LCS `sstableLevel` int** — levels derive from density only.
- **Scaling parameter** `w`: F = `w<0 ? 2−w : 2+w`; threshold t = `w≤0 ? 2 : 2+w`;
  `Tn`→w=n−2, `Ln`→w=2−n, `N`→w=0. Per-level array, last value repeats.
- **Trigger:** a level compacts iff **maxOverlap ≥ t** (overlap count via sweep over
  first/last keys, `Overlaps.constructOverlapSets` — `utils/Overlaps.java` is byte-identical
  across refs). NOT raw sstable count (design doc Appendix A misstates this).
- **Level tie-break is deterministic** (strict `>`, lowest/first level wins);
  **bucket tie-break within a level is random** (reservoir sample).
- **Shard count** (`Controller.getNumShards`): power-of-two multiple of `base_shard_count`;
  `pow = log2(density/(target·base))·(1−growth) + 0.5`, growth default 0.333, min-size
  guard below base count.
- **Shard boundaries**: even token-fraction split of the node's weighted local ranges via
  `partitioner.split` — exact interpolation. **Deterministic given (partitioner, local
  ranges+weights, shardCount) → byte-identical boundaries achievable offline.**

### 5.0 → trunk deltas (track, don't implement for a 5.0 target)

| Delta | Class |
|---|---|
| Major compaction: 5.0 `getMaximalTask` splits by token-overlap only; trunk `getMaximalTasks` additionally density-shards via `splitSSTablesInShards` (new primitive, absent in 5.0) | MATH-CHANGE, major/manual path only; background pick unchanged |
| `parallelize_output_shards` (trunk, default on): one pick → N per-shard parallel tasks under a `CompositeLifecycleTransaction`. Same final file set; execution mechanism only. Caveat: inner tasks recompute *local* density — never call `getNumShards` twice at different scopes | RUNTIME-ONLY |
| `ShardManagerDiskAware.count()/shardIndex()` JBOD fix (CASSANDRA-18802): 5.0 under-counts shards across multiple data dirs | BUG-FIX; irrelevant for single-directory offline use |
| TCM epoch replaces ring-version staleness checks | RUNTIME-ONLY |

### More UCS facts (verified on 5.0)

- **No adaptive controller exists in mainline Cassandra** (exhaustive grep, both refs) —
  auto-tuning `w` is documentation aspiration only. A static-config offline planner is
  *faithful*, not approximate.
- **No legacy-STCS special case exists.** Arbitrary/legacy sstables flow through the same
  density → level assignment; a whole-ring STCS giant just lands in a high level,
  deterministically. **The design doc's "convergence as a first-class planner mode"
  dissolves** — convergence is just repeated planning, which the simulator can cost.
- **STCS-equivalent default = `T4` (w=2, F=t=4)**, matching STCS `min_threshold=4`
  (documented in `UnifiedCompactionStrategy.md`). Other defaults: base_shard_count 4,
  target 1GiB, min_sstable_size 100MiB, growth 0.333, expired-check 600s,
  overlap_inclusion TRANSITIVE.
- **Flush size:** `flush_size_override` config wins; else live metric; **fresh-node
  fallback = exact 1 MiB floor** in `getBaseSstableSize` (`max(1<<20, flushSize)`).
  Offline: take flush size as an explicit input; default to the documented 1 MiB floor;
  optionally allow "derive from smallest lowest-density sstables in the corpus" as an
  explicit opt-in (closer to warm-node behavior).
- **Expiration:** `unsafe_aggressive_sstable_expiration` is double-gated (table option AND
  JVM property; fails closed). Expired sstables are computed once per pick call (10-min
  wall-clock throttle), removed from the pool, then UNIONED into the chosen pick — or
  returned as a standalone drop-only pick (`level=-1,overlap=-1` sentinel) if nothing else
  triggers.
- **UCS does NO ratio-based single-sstable tombstone compaction** — inherited
  `tombstone_threshold` options are validated but inert. → design doc §15 Q4
  (`JobKind::TombstoneOnly`): **defer, confidently.**
- Minimal planner `SSTableMeta` = {size, firstToken, lastToken, tokenSpaceCoverage,
  maxTimestamp, maxLocalDeletionTime, repairedAt/pendingRepair}.
- Selection within a bucket: oldest-first by maxTimestamp; happy path (count ≤ F) takes
  the whole bucket.

### Fidelity posture (answers design doc §15 Q2)

**Shard-boundary parity = feasible + worthwhile** (deterministic; determines output key
ranges → byte layout), given topology/flush-size/config as **explicit inputs**.
**Pick-selection parity = ill-defined** (random bucket tie-break + live-set state). Frame
the planner as a **proposer/validator of candidate picks**. For `authoritative`/full-dataset
runs, topology degenerates to whole-ring coverage.

## 3. Purge / overlap semantics (verified on 5.0) — the precision ladder

From `CompactionController.getPurgeEvaluator` (CompactionController.java:244-284) and
collaborators:

| Rung | What it computes | Offline data needed | Verdict |
|---|---|---|---|
| **(a) CQLite today** | one global `min(minTimestamp)` over ALL outside sstables (all treated as overlapping) | Statistics.db (have) | Correct, maximally conservative |
| **(b) Interval overlap** | restrict (a) to sstables whose `[first,last]` key range intersects the compaction's merged range — port of `getOverlappingLiveSSTables` normalization (sort by first key, merge touching bounds) | first/last key per sstable (have) | **Recommended v1 target** — closes the "unrelated key range holds the bound hostage" gap with zero new I/O |
| **(c) Cassandra's per-partition evaluator** | per partition, lazily: interval test → bloom-filter probe (`mayContainAssumingKeyIsInRange`; index probe if filter uninformative) → fold only plausibly-containing sstables' minTimestamps | `Filter.db` reader (NEW read path) | v2, after (b)'s purge-yield is measured on real corpora |

Key findings:

1. **Range tombstones / partition deletions need nothing finer than partition-key
   granularity** — Cassandra applies the SAME per-partition bound to cell tombstones, range
   tombstones, and partition deletions; it never checks clustering-range overlap. Rung (c)
   is full Cassandra parity; anything finer exceeds the reference.
2. **CQLite's coarse global bound IS Cassandra's fully-expired-drop algorithm** —
   `getFullyExpiredSSTables` uses exactly that min-timestamp shape at file granularity (no
   bloom filters). Keep the coarse bound for whole-file drops (a separate, cheap, high-value
   tier Cassandra runs FIRST); add rung (b) for in-rewrite tombstone purging.
3. **TTL purge math (pin as a parity test; touches #1537/#1538):** an expired TTL cell
   converts to a synthetic tombstone with `localDeletionTime = original write time`
   (`ldt − ttl`), NOT expiration time (`AbstractCell.purge`, AbstractCell.java:76-99).
   Effective purge eligibility = `writeTime + max(ttl, gc_grace)` — **not**
   `writeTime + ttl + gc_grace`. Intentional repair-correctness choice (comment at :88-90).
   The cross-sstable shadowing check uses the original write *timestamp*, unchanged by the
   conversion.
4. `gcBefore = nowInSec − gc_grace`, wall clock of the compacting node, sampled ONCE per
   compaction run. 2i tables skip gc_grace entirely.
5. `onlyPurgeRepairedTombstones`: if on and inputs aren't all repaired → NO purging at all
   that run. `NEVER_PURGE_TOMBSTONES` → purge evaluator returns constant-false.
6. **`GarbageSkipper`/shadow-sources (TombstoneOption ROW/CELL) is optional** — proactive
   shadowed-data stripping that opens overlapping sstables at key positions. Purge
   *correctness* never requires reading overlapping data; bloom/interval checks suffice.

## 4. Crash safety — txn log verdict (verified on 5.0)

**The design doc's §7.3 commit-record txn log is NOT needed for CQLite's offline case.**

Why Cassandra needs it: component files are written directly under final filenames (no
tmp-then-rename for new sstables) — no filesystem rename barrier can make a multi-file
sstable appear atomically, so an out-of-band log (`<ver>_txn_<op>_<uuid>.log`, per-line
CRC32, ADD/REMOVE/COMMIT/ABORT records, replicated per data dir, tolerant of a torn last
line) is the only atomic publication mechanism. Recovery: last valid record COMMIT →
roll-forward (finish deleting REMOVEs); else roll-back (delete ADDs).

Why CQLite doesn't: **live/dead is a pure function of `TOC.txt` existence**, enforced
symmetrically — publish renames TOC last (compaction.rs:414-433); unpublish deletes TOC
first (maintenance.rs:489-538); the startup sweep reclaims any TOC-less Data.db
(sweep.rs:113-183). This already yields the functional equivalent of roll-forward for the
worst window (crash mid-input-deletion after publish): a not-yet-deleted input is a
harmless duplicate that LSM merge semantics reconcile; a half-deleted one is swept.
Re-running the whole job is always safe.

**The ONE real gap (fix regardless of #905): no directory fsyncs in the compaction
finalize path.** `durability.rs`'s ancestor-chain fsync barrier is wired into FLUSH only;
`compaction.rs`'s rename/delete sequence has none. Under power loss (not process kill) the
OS may reorder renames/unlinks so `TOC.txt` lands durably while a sibling component doesn't
— reopening exactly the ambiguity the TOC invariant closes. The existing crash-injection
test (`FAIL_COMPACTION_BEFORE_RENAME`) simulates process crash only (syscall order
preserved). Fix: reuse the `durability.rs` helper after the rename batch and around input
deletion. Small, oracle-driven, hardens the SHIPPED write engine.

Nice-to-haves (not correctness): a durable "output W superseded inputs X,Y,Z" audit record;
live-node detection should scan for Cassandra's `*_txn_*.log` filename pattern (regex
`^((?:[a-z]+-)?.{2}_)?txn_(.*)_(.*)\.log$`) in target dirs.

## 5. Manager/daemon + standalone-tool reference (verified on 5.0)

- **Cassandra's scheduler is event-driven, not polled**: state changes nudge
  `submitBackground(cfs)`; each nudge asks that table's strategy for ONE task; cross-table
  fairness is emergent from a flat shared pool (no priority queue, no manager-level
  ranking). Starvation guard: skip re-submit if the table already has a running task and
  the pool is saturated (CASSANDRA-4310). → The daemon needs a nudge queue + per-table
  polls, not a clever scheduler.
- **Throttle**: ONE global live-updatable RateLimiter shared by all tasks, acquiring
  **compression-ratio-adjusted bytes** (models physical disk I/O). Default 64MiB/s; 0 =
  unlimited. Copy exactly.
- **Disk-headroom**: per-task pre-check; worst-case estimate (output = Σ inputs) + two
  stacked margins (absolute floor per drive × fractional cap), accounts for other in-flight
  compactions' remaining writes; `reduceScopeForLimitedSpace` drops the largest input and
  retries; hard abort (+ `compactionsAborted` counter) when it can't shrink.
- **No write backpressure exists in Cassandra** — `estimatedRemainingTasks` is
  observational only. A CQLite daemon can consciously match (metrics-only) or exceed.
- **UCS major compaction always shards** (`--split-output` silently ignored by UCS):
  N non-overlapping token groups × M density shards. Single-monolithic-output major is an
  STCS-ism. Confirms design doc §6.3's sharded-major shape.
- **Standalone-tool template** (`StandaloneScrubber` et al.):
  `DatabaseDescriptor.toolInitialization()` (config subset + partitioner + formats; no
  gossip/sockets) → offline schema load → `openNoValidation` per sstable (one bad file
  doesn't abort) → work inside `LifecycleTransaction.offline()` (dummy tracker — proof the
  safety layer runs headless) → snapshot-by-hardlink before mutating.
- **Cassandra's standalone tools have NO live-node guard** — their docs say "the script
  does not verify that Cassandra is stopped." CQLite's refuse-if-live check (txn-log
  pattern scan + own flock) would EXCEED the reference. Differentiator.
- Status API reference (3 tiers): per-task `CompactionInfo` (id, table, completed/total/
  unit, inputs, target dir; pull-model snapshots); gauges (pendingTasks[-ByTable],
  active); counters/meters (completed rate, bytes logical+compressed,
  `compactionsAborted`, `compactionsReduced` — cheap high-value health signals).

## 6. CQLite reuse audit (what M5 already gives us)

| Design-doc layer | Status | Cost | Notes |
|---|---|---|---|
| Executor (k-way merge + reconciliation + write) | **EXISTS** | S–M | `compact_sstables_with_registry` (merge/mod.rs:1684) is a WriteEngine-free free function over an arbitrary input dir; CLI `cqlite compact` (#842) calls it. Missing: GC modes, throttle, dry-run, **sharded output** (single-output today). |
| Crash safety | **EXISTS — stronger than first assessed** | S | TOC-existence invariant ≈ functional roll-forward (see §4). Real gap = missing dir-fsyncs in finalize path. Commit-record log NOT needed. |
| Planner (trait + UCS) | **MISSING** | L | `STCSPolicy` bucketing pure (#1666) but behind an I/O-doing `&[PathBuf]→Vec<PathBuf>` trait, size-only metadata. UCS, density, TableState/CompactionJob, overlap sweep, explain/dry-run net-new. |
| Simulator inputs (Statistics.db) | **EXISTS-BUT-ENTANGLED** | M | size + min/max timestamp ready. `tokenSpaceCoverage` not yet exposed by our parser (wiring). Fallback: Summary.db first/last key (summary_reader.rs:150) + existing `cassandra_murmur3` → tokens (wiring). Droppable-tombstone scalar derivable from parsed `tombstone_drop_times` histogram (no consumer today). Per-sstable level absent — UCS doesn't need it. |

Additional: **no `CompactionManager` struct exists in code** (`docs/read-path/02-storage-engine.md`
shows an aspirational one — stale; #176 removed the old infra). Real driver =
`WriteEngine::maintenance_step(budget)`. One-shot `cqlite compact` purge policy is only the
boolean `--major`/`purge_safe`; the three-mode `off/grace/authoritative` policy layers over
existing plumbing (`compute_gc_before` already reads schema gc_grace, purge-disabled when
absent). Debt on this path: #1537/#1538 (TTL P0s in reconcile.rs — executor inherits),
#1849 (gates the read-equivalence property-test ORACLE, not the build), #1610 Epic Q
(merge perf; Q3 determinism), **#1633 T2 (12.3k-LOC merge/mod.rs — extraction rides that
split)**.

## 7. Product-direction alignment (#1934, #941, storage engine, milestones)

**Synergies**
- **#1934 engine split:** the pure `plan()` trait is a textbook exhibit for the Phase-1
  curated engine API (no entangled callers to migrate, unlike the query engine). The
  simulator is a plausible flagship demo for the new engine-tier product name (WS5).
- **Storage-engine/memtable path:** the tail dir `CqliteTailExporter` fills is exactly what
  #905's manager maintains — **same component, second data source**.
- **#941 Flight:** producer.rs re-drives the k-way merge per query (no pre-merge
  assumption) — hardening the shared merge core benefits both. **Design C
  (materialized-epoch provider, #1914) ≈ a #905 major-compaction snapshot — reconcile the
  two designs before #1914 activates** to avoid building "merge once, serve many" twice.
- **T2 #1633:** write planner/executor extraction as additional seams of that split.

**Conflicts / tensions**
- **#1406 uncompressed-only writes vs snapshot hygiene (NEEDS-OWNER):** real snapshots are
  LZ4-compressed; our compactor can only emit uncompressed output, so "compacted" can be
  LARGER than the compressed input. Either scope the claim ("removes shadowed
  data/tombstones; does not restore compression") or reopen compressed writes (#1406
  posture (a), currently an M7 candidate). Blocks Phase A′ *marketing*, not Phase B.
- **P0 gating, precise:** #1537 hard-gates Phase A′ correctness claims (small,
  oracle-driven — fix first); #1538 gates only byte-parity claims (trail as documented
  caveat); #1849 gates only the invariant-4 property-test oracle. **Phase B blocked by
  nothing.**
- **Resourcing:** #905 maps to neither M6 nor M7; 44 epics already in Backlog. Filing adds
  a 45th unless the owner explicitly reprioritizes a slice.

**Placement**
- Do NOT mint a published `cqlite-compaction` crates.io name now (the WS5 lesson).
  Planner/executor = in-repo module / unpublished workspace crate behind the Phase-1
  curated API. The daemon (`cqlite-compactd`) is the natural FIRST artifact published
  under the new engine name once WS5 lands one.

**Suggested sequencing (when the owner structures it)**
1. Owner decisions (below) + fast-track the dir-fsync gap (§4) as its own oracle-driven bug.
2. Ride T1 #1631 / T2 #1633; write new compaction code as new seams of the split tree.
3. **Phase B (planner + UCS + simulator) first and in parallel** — design-driven → OpenSpec
   (new public trait + CLI verbs), UCS math TDD'd oracle-style against 5.0 source as
   parity backstop. Unblocked by everything.
4. Phase A′ (GC modes, rung-(b) overlap, dir-fsyncs, throttle, dry-run) gated on #1537
   only; #1538/#1849 tracked as claim-boundary caveats (the #1406 documentation style).
5. Phase C daemon after WS5 names the engine product.
6. Phase D (schema pull, offload groundwork) exploratory, last.

## 8. Open decisions (owner)

1. **Product placement vs #1934 split** — recommendation in §7 (in-repo unpublished now;
   daemon waits for the engine name).
2. **v1 fidelity posture** — recommend: shard-boundary parity yes (topology + flush-size as
   explicit inputs; golden-tested), pick-selection parity out of scope.
3. **Priority** — #905 is P3; does the simulator's demo value justify promoting a Phase-B
   slice ahead of adjacent Backlog work?
4. **#1406 tension** — scope the snapshot-hygiene claim down, or reopen compressed writes.
5. **File the dir-fsync gap now?** — independent shipped-write-engine hardening (§4),
   oracle-driven, small.
6. Design doc §15 Q1 (schema pull seeding UCS config) — still open, Phase D product call.

## 9. Corrections to make when the design doc is refreshed

- §14 Phase A: executor largely shipped; re-scope to Phase A′ (§7 sequencing).
- §7.3: replace the commit-record txn log with the TOC-invariant + dir-fsync design (§4);
  keep `--repair-log` OUT (re-run is always safe); add the audit-record nice-to-have.
- §7.2/§15 Q5: adopt the precision ladder (§3) — coarse bound stays for whole-file drops,
  rung (b) for in-rewrite purge, rung (c) documented as v2 behind `Filter.db` reading.
- §6.2: DELETE the "legacy-layout convergence as first-class planner mode" — no such mode
  exists in Cassandra; convergence = repeated planning (simulator costs it).
- §6.1 `SSTableMeta`: add `tokenSpaceCoverage` (authoritative) as primary density input;
  add topology + flush-size as explicit planner inputs (flush-size default = the 1 MiB
  floor); drop `level_hint` (UCS ignores it); note droppable-tombstones derivable from the
  parsed histogram.
- §6.3: major compaction = sharded per UCS always (single-output major is an STCS-ism);
  on 5.0 maximal splits by token-overlap only (trunk adds density-sharding — track).
- §8: TTL purge math — synthetic tombstone LDT = original write time; eligibility =
  `writeTime + max(ttl, gc_grace)`. Pin as parity test.
- §11: daemon = nudge queue + per-table strategy polls (event-driven, not scanning);
  throttle = one global live RateLimiter on compression-adjusted bytes; add
  `compactionsAborted`/`compactionsReduced`-style counters; note Cassandra has no write
  backpressure (decide match-or-exceed explicitly).
- §15: record answers — Q2 split (boundaries yes / picks no), Q3 (~70% reuse; inherits
  #1537/#1538), Q4 (defer TombstoneOnly), Q5 (coarse shipped; rung (b) is the v1 upgrade).
- Appendix A: trigger is overlap-count ≥ threshold; UCS not yet OSS default; T4 is the
  STCS-equivalent default.
- `docs/read-path/02-storage-engine.md`: remove the fictional `CompactionManager` field.
