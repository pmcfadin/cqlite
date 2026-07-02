# Design: fully-expired-sstable-drop

## Context

The compaction pipeline has two entry surfaces, both of which already read per-input `Statistics.db`:

1. **CLI one-shot** — `compact_sstables_with_registry` (`cqlite-core/src/storage/write_engine/merge/mod.rs:1712`),
   behind `cqlite compact`. It receives an explicit `input_paths` list and a `purge_safe` flag mapped from
   `--major` (`cqlite-cli/src/commands/write.rs:371`). It has no knowledge of SSTables outside its input
   list, so today it passes `None` for the overlap bound.

2. **WriteEngine background** — `maintenance_step_inner`
   (`cqlite-core/src/storage/write_engine/maintenance.rs:204`). It scans this table's candidate set,
   asks the merge policy for a `selected` subset, computes `purge_safe = (selected == candidates)`, and —
   for a partial compaction — computes the outside/non-included overlapping set and calls
   `merge::compute_max_purgeable_timestamp(&non_included)` (`maintenance.rs:288`). It then calls
   `start_merge(selected, purge_safe, max_purgeable_timestamp)` (`compaction.rs:62`).

The authoritative metadata is already available and already parsed here:

- `compute_baseline_min` / `compute_max_purgeable_timestamp` read each SSTable's `Statistics.db` via
  `parse_statistics_with_fallback` into `TimestampStatistics` (`cqlite-core/src/parser/statistics.rs:98`),
  which exposes `min_timestamp`, `max_timestamp`, `min_deletion_time`, `max_deletion_time`, `min_ttl`,
  `max_ttl`.
- `gcBefore` is already computed as `compute_gc_before(schema, now_secs)` (`merge/mod.rs:1588`): a
  GC-clock second cutoff (`now - gc_grace_seconds`, Cassandra 10-day default on absence, `None` when the
  declared value is invalid — which disables purging and MUST also disable dropping).

So both the metadata inputs to the fully-expired decision and the overlap bound already exist in the two
call sites; this change composes them into a drop-set instead of adding a new I/O path.

## Authoritative fully-expired detection (no-heuristics)

A candidate SSTable is **fully expired** relative to a compaction iff **every** cell/tombstone in it has
`localDeletionTime < gcBefore`. The single authoritative metadata field that proves this is
`TimestampStatistics.max_deletion_time` (Cassandra `StatsMetadata.maxLocalDeletionTime`, tracked by
`EncodingStats.update_local_deletion_time` over every deletion/TTL localDeletionTime, including a TTL
cell's `createdAt + ttl`):

    max_deletion_time < gcBefore  ⇒  every cell's localDeletionTime < gcBefore  ⇒  SSTable fully expired

- `max_deletion_time` is the *maximum* localDeletionTime across the whole SSTable, so if the maximum is
  below `gcBefore`, all of them are. This is exactly Cassandra's `getFullyExpiredSSTables` predicate
  (`sstable.getSSTableMetadata().maxLocalDeletionTime < gcBefore`).
- The sentinel `NO_DELETION_TIME` / `Cell.NO_DELETION_TIME` (i32::MAX, "live, never expires") must be
  treated as NOT expired: any SSTable holding live non-TTL data has `max_deletion_time == i32::MAX`
  (LIVE sentinel), which is never `< gcBefore`, so it is correctly ineligible. The detection reads the
  raw metadata value and applies `value < gcBefore` with no other special-casing — the LIVE sentinel
  falls out of the comparison naturally.
- **No cell scan.** The decision is a single integer comparison against one metadata field. This is the
  no-heuristics-compliant path (issue #28) and is the whole point of the optimization: we must NOT read
  the SSTable's rows to decide to drop it (that would defeat the perf win and reintroduce a scan).
- `gcBefore == None` (invalid gc_grace) or an unreadable/absent `Statistics.db` ⇒ candidate is NOT
  droppable (conservative), matching `compute_gc_before` / `compute_max_purgeable_timestamp` degradation.

## Overlap-safety gate (mirroring Cassandra)

Even fully expired, an SSTable may hold a tombstone shadowing *older* data in an SSTable outside the
compaction set. Dropping it whole would resurrect that data. Cassandra guards this in
`getFullyExpiredSSTables` by removing from the drop candidate set any SSTable that overlaps (by key range)
a non-compacting SSTable holding data it could shadow, using a `maxTimestamp` comparison.

CQLite already has the exact bound: `compute_max_purgeable_timestamp(outside_paths)` returns the MINIMUM
write timestamp across the outside overlapping SSTables (`EncodingStats.minTimestamp`). A fully-expired
candidate is safe to drop iff its own **`max_timestamp` is strictly less than that bound**:

    candidate.max_timestamp < min(outside.min_timestamp for outside in overlapping)  ⇒  safe to drop

- If the candidate's newest write predates every outside SSTable's oldest write, nothing the candidate
  contains (tombstone or data) can shadow anything outside the set — dropping it can never resurrect data.
- A FULL/major compaction has an empty outside set (`purge_safe == true`) ⇒ bound is `+inf` ⇒ every
  fully-expired candidate is droppable. This matches Cassandra: a major compaction over all SSTables has
  nothing outside to shadow.
- If the outside bound is UNKNOWN (`compute_max_purgeable_timestamp` returned `None` because an outside
  `Statistics.db` was unreadable) in a PARTIAL compaction, the candidate is NOT dropped (conservative).
- This reuses the identical `min_timestamp` metadata and the identical conservatism as the existing #935
  tombstone-purge gate, so the two decisions stay consistent (a compaction that may not purge a tombstone
  also may not silently drop the SSTable that holds it).

## Chosen approach: metadata-only drop-set computed at the plan step

Add `fn fully_expired_sstables(input_paths, outside_paths, gc_before_secs) -> Vec<PathBuf>` to the merge
module. It reads each input's `Statistics.db` once (`stats_path_for` + `parse_statistics_with_fallback`,
the existing helpers), applies `max_deletion_time < gcBefore` AND the `max_timestamp < outside_min_ts`
overlap gate, and returns the droppable subset. The compaction surfaces then:

- Remove the dropped set from the merger's `input_paths` before building the `KWayMerger` (the merger
  never reads dropped SSTables → the perf win).
- Delete the dropped SSTables after the output publishes (same reclamation path as the merged inputs).
- Record the dropped set in the report/stats (`MergeStats` / `CompactReport` gain a `dropped_whole:
  Vec<PathBuf>`), so tests assert the plan decision, not just absence from output.

### Alternative considered (and beaten): cell-scan verification pass

*Rejected.* Compute the candidate drop-set from metadata, then run a verification pass that reads every
cell of each candidate to confirm all are expired before dropping. This would be belt-and-suspenders
against a mis-written `Statistics.db`, but it (a) reintroduces the full read cost the optimization exists
to eliminate — dropping becomes no cheaper than the current rewrite; (b) violates the no-heuristics
mandate's spirit by second-guessing authoritative metadata with a scan; and (c) diverges from Cassandra,
which trusts `maxLocalDeletionTime`. The metadata-only path is both faster and closer to Cassandra parity.
Trust in `Statistics.db` is already load-bearing across the write engine (`compute_baseline_min`,
`compute_max_purgeable_timestamp` both trust it), so adding a scan only here would be inconsistent.

### Alternative considered (and beaten): drop at the writer/finalize step

*Rejected.* Let the merger read every input (including expired ones) and drop the expired SSTables only at
finalize time by not deleting them / by post-filtering. This is simpler to wire but delivers **zero perf
win** — the whole point is to avoid reading the dead SSTable. Excluding it from the merger's input list at
the plan step is where the cost is actually saved.

## Where the exclusion hooks in

- **WriteEngine background** (`maintenance_step_inner`): the outside set is already computed at
  `maintenance.rs:283-288`. Compute the drop-set there from `selected` (inputs) + `non_included`
  (outside) + `gc_before_secs`, subtract it from `selected` before `start_merge`, and thread the dropped
  paths through `start_merge` so finalize deletes them and the report records them. This is the primary,
  fully-overlap-aware surface.
- **CLI one-shot** (`compact_sstables_with_registry`): the explicit input list has no outside set, so the
  drop is only overlap-safe when the operator asserts `--major` (`purge_safe == true` ⇒ empty outside set
  ⇒ `+inf` bound). Compute the drop-set with an empty outside set gated on `purge_safe`, subtract from
  `input_paths` before building the merger, delete dropped files after publish, and populate
  `CompactReport`. When `--major` is absent (conservative), no drop occurs (matches current
  purge conservatism). This is the surface acceptance-criterion 1 exercises.

## OPEN QUESTIONS for the owner (genuine design forks — do NOT decide unilaterally)

### OQ-1 — Should the CLI one-shot `compact` drop whole SSTables at all, given it has no outside set?

The CLI cannot see SSTables outside its `input_dir`, so it cannot compute a real overlap bound. Options:

- **(A) Recommended:** Allow the drop ONLY under `--major` (operator's existing assertion that
  `input_dir` contains ALL overlapping SSTables for the table). With `--major`, the outside set is
  empty by contract, the bound is `+inf`, and the drop is provably safe under the operator's assertion —
  identical to how `--major` already unlocks tombstone purging. Without `--major`, no drop. This keeps
  the CLI's existing safety contract and satisfies acceptance-criterion 1 (which specifies a major
  compaction).
- **(B)** Never drop in the CLI one-shot path; expose the drop only via the WriteEngine background path.
  Simpler/safer but leaves the CLI paying the rewrite cost for a major compaction and does not satisfy
  acceptance-criterion 1 as written (it asserts a major compaction drops whole).

Recommendation: **(A)** — it reuses the `--major`/`purge_safe` safety contract the operator already opts
into and directly satisfies acceptance-criterion 1. Flagged because it is the operator-facing safety
semantics of a data-destroying optimization, which is the owner's call.

### OQ-2 — Overlap bound precision: coarse table-wide `min_timestamp` vs key-range-aware overlap

Cassandra's real check is key-range-aware: only SSTables whose *token/key ranges overlap* the candidate
constrain the drop. CQLite's existing #935 gate is coarser — it treats *every* other SSTable for the
table as overlapping and uses the global `min_timestamp` bound. Options:

- **(A) Recommended:** Reuse the existing coarse "every other SSTable overlaps" + global-`min_timestamp`
  bound (identical to `compute_max_purgeable_timestamp`). Strictly conservative (never drops when
  Cassandra would keep), consistent with the tombstone-purge gate already shipped, and needs no new
  index/range plumbing.
- **(B)** Add key-range-aware overlap using Index.db/Summary min/max keys to match Cassandra's precision
  and drop in more cases. More parity-faithful but larger scope (new range-overlap computation) and a
  new source of divergence risk.

Recommendation: **(A)** for this change — conservative, consistent with #935, and the perf win is already
captured for the common major-compaction case. Flagged as a fork because it trades some drop-eligibility
(perf) for simplicity/safety, and the owner may want (B) for parity fidelity. **(A) can never resurrect
data** — it only declines to drop in cases Cassandra would drop, so it is safe to ship (A) and file (B)
as a follow-up if the owner wants tighter parity.

## Resolved decisions (owner, 2026-07-02)

- **OQ-1 → (A).** The CLI one-shot `compact` drops whole SSTables **ONLY under `--major`**. With `--major`
  the outside set is empty by the operator's contract ⇒ overlap bound is `+inf` ⇒ every fully-expired
  candidate is provably safe to drop (identical to how `--major` already unlocks tombstone purging).
  Non-major CLI compaction NEVER drops whole (conservative default, matching current purge conservatism).
- **OQ-2 → (A).** This change uses the **coarse #935 global-min-timestamp overlap gate** — the existing
  `compute_max_purgeable_timestamp` bound (every other SSTable for the table treated as overlapping, global
  `EncodingStats.minTimestamp`). It does NOT add key-range-aware overlap. Key-range precision (OQ-2 option
  B) is deferred to a follow-up issue if the owner later wants tighter parity; it is out of scope here.
  (A) is strictly conservative and can never resurrect data.
