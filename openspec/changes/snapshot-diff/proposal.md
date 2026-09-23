# snapshot-diff — issue #4201 (epic #4192)

**Milestone:** unmilestoned (owner convention; board `Backlog` pending #4193 — see Scheduling below).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — a new read-only verb whose input
shape, tombstone-identity matching and report granularity have real latitude, built on top of #4193's
reconciliation trail; the resurrection-risk classification itself is oracle-bound to Cassandra's own
purge/reconciliation rules (`cassandra-5.0.8` source), never invented.

## Scheduling note (read before approving) — do not call #4193 approved

Issue #4201 depends on #4193 (F0 decision trail + `cqlite explain`). At spec time #4193's OpenSpec
change is **drafted and `openspec validate --strict` clean on branch `issue-4193-forensics-explain`**
but the epic's own tracking line (#4192, 2026-09-23) reads *"parked in Backlog, re-activate by
adopting the branch"* — **#4193 is NOT confirmed Seam-1 approved as of this drafting.** This worktree
branches from `origin/main` and therefore contains none of #4193's `merge/trace.rs`
(`TraceSink`/`RecordingSink`/`CellDecision`/`TombstoneRecord`/`compute_gc_before`) or `explain`'s CLI
scaffolding. This spec is written against #4193's committed design
(`openspec/changes/forensics-explain/design.md` on that branch) so review can proceed in parallel,
but `tasks.md` Group 0 makes **both** re-confirming #4193's approval status **and** landing it on
`origin/main` explicit, re-checked premises — implementation cannot start until #4193 is actually
Seam-1 approved, merged, and this branch rebases onto it.

**#4200 (tombstone-surgery) is a sibling, not a foundation.** It depends on the same #4193 primitives
and this proposal reuses several of its *conventions* (per-side full-compaction scan, `--now`-first
output, fold-and-drop memory bound, affirmative-truncation pattern) — but #4201 has no code
dependency on #4200's `tombstone_density` module, since #4200 is itself unmerged and diffing two
independently-reconciled sides is a different mechanism than aggregating one side's density. Where
#4201 needs a capability #4200's design sketched (a range tombstone's clustering-span predicate), it
is implemented directly in this change rather than imported from an unmerged sibling.

## Why

Epic #4192, FIX slice: an operator comparing two replicas' on-disk state for the same table (after a
node was down, missed a repair window, or is suspected to be diverging) needs to know, per
partition, which cells and tombstones the two sides disagree about, and — critically — whether any
of that disagreement is a **resurrection hazard**: a tombstone one side already carries whose
gc_grace clock has run out, while the other side still holds the very data that tombstone would
shadow. If that side's tombstone is purged by compaction before the other side ever receives it (a
missed hint, a skipped repair, a down node past `gc_grace_seconds`), and the shadowed data later
finds its way back (repair, `sstableloader`, a restored snapshot), the deleted row reappears. This is
a real, named Cassandra operational hazard, not a hypothetical: Cassandra's own compactor refuses to
purge a tombstone unless every other source holding the key (including sources **outside** the
current compaction) has a timestamp *newer* than the tombstone
(`CompactionController#getPurgeEvaluator`, `cassandra-5.0.8`) — because purging past that boundary
with an older-timestamped source still outstanding is exactly what causes deleted data to reappear.
`cqlite diff` cannot see the *other node's* SSTables during that node's own compaction the way
`getPurgeEvaluator` can — it exists to let an operator see the same hazard **after the fact**, from
two snapshot directories pulled to one machine, before it silently resolves itself the wrong way.

CQLite already has every primitive this needs, all from #4193: the closed verdict vocabulary
(`winner` / `shadowed-by-timestamp` / `shadowed-by-tombstone{kind}` / `expired` / `purgeable` /
`dropped-column`), the `RecordingSink` that captures one `CellDecision`/`TombstoneRecord` per
decision at zero cost on the production path, `compute_gc_before(schema, now)`, and the same
per-partition merger `explain`/`tombstones` drive. `diff` adds nothing new to the reconciliation
engine — it drives that SAME merger once per side (each side's own generation set, independently),
then compares the two sides' winner sets and tombstone trails instead of rendering one side's trail
alone.

## What changes

**Library (`cqlite-core`).** A `snapshot_diff` module under `storage/write_engine/` exposing
`diff_table(dir_a, dir_b, table, schema, options) -> Result<DiffReport>` (full-table) and
`diff_partition(dir_a, dir_b, schema, partition_key, options) -> Result<PartitionDiff>`
(`--partition`-scoped). For each side independently: build the per-partition merger across THAT
side's own generations via `build_single_partition_merger`, run it in full-compaction configuration
at the pinned `now` (`purge_safe(true)`, `gc_before_secs = compute_gc_before(schema, now)`) with a
`RecordingSink` — the identical mechanism #4193's `explain` and #4200's `tombstones` use, applied to
two independent inputs rather than one. Compare the two sides' winner cells (`equal` /
`only-a`/`only-b` / `newer-in-a`/`newer-in-b`, by writetime) and the two sides' tombstone sets
(`only-a`/`only-b`; a tombstone present, with matching scope and deletion time, on both sides is
already-reconciled knowledge and is not reported as a divergence). For every `only-a`/`only-b`
tombstone, cross-check the OTHER side's winner set for live data inside that tombstone's scope
(partition / row / range / cell / collection) with writetime `<=` the tombstone's deletion time — the
literal Cassandra shadow rule (`DeletionPurger#shouldPurge`, `AbstractCell.purge`,
`cassandra-5.0.8`) — and if any exists, emit `resurrection_risk` classified `before-gc-grace` /
`past-gc-grace` from the SAME strict-`<` boundary #1385 already established for `purgeable`:
`local_deletion_time < now - gc_grace_seconds` (`ColumnFamilyStore#gcBefore`, `cassandra-5.0.8`).

**CLI (`cqlite-cli`).** `cqlite diff <snapshot-a-dir> <snapshot-b-dir> --table <ks.table> [--schema
<path>] [--partition <key>]... [--now <ts>] [--out text|json]`. Never opens either input for writing.

## What this change must establish

1. **Resurrection-risk classification is derived from Cassandra's own reconciliation + gc_grace
   rules, at the pinned `cassandra-5.0.8` tag** (`DeletionPurger#shouldPurge`,
   `ColumnFamilyStore#gcBefore(long)`, `CompactionController#getPurgeEvaluator`) — never invented,
   never from CQLite's own prior behavior (#3041/#3042). The `resurrection_gc_positive` fixture (2
   generations, `test_tomb`, `gc_grace_seconds=864000`) pins the boundary to the second, mirroring
   #1385's `gc_before_boundary` convention.
2. **The trail agrees with itself across sides**: each side's `winner`/`purgeable` classification is
   produced by the SAME unmodified #4193 mechanism `explain` uses; `diff` adds a comparison layer,
   never a second reconciliation implementation.
3. **Symmetry**: `diff A B` and `diff B A` are label-swapped mirrors of the same physical facts —
   asserted by a test, not just claimed.
4. **Fail closed**: an unreadable generation on either side is a named, non-zero-exit error, never a
   report computed from a shorter trail on one side (the #4159 class).
5. **Wiring evidence from the binary**, against the committed `resurrection_gc_positive` fixture
   staged as two directories (A = both generations, B = generation 1 only) per the issue's own AC 1.

## Non-goals

- **Repairing either side.** The report is input to `sstableloader`/repair, never a write path. No
  `--fix`/`--apply` flag exists on this verb, ever, by construction (issue's own Non-goals).
- **More than two snapshots.** N-way divergence is a distinct, larger problem; out of scope.
- **Cross-node fetch (S3, SSH, a running cluster).** Two local directories only — see the input-shape
  decision below.
- **A data-dir-wide sweep across tables** (#4194's territory); `diff` takes one table, named by
  `--table`, on each side.
- **Re-implementing #4200's tombstone density aggregation.** `diff` is a two-source comparison, not a
  one-side density report; it does not import #4200's (unmerged) module.

## Doctrine impact

- **No-heuristics (#28):** every classification comes from the reconciliation trail's own decisions,
  driven by the SAME merger `explain`/`tombstones`/`compact` use; no independent byte-pattern
  divergence heuristic.
- **Uncompressed-write claim boundary (#1406):** not engaged — this change writes nothing to either
  input or to a new output; it only reads.
- **Cassandra 5.0 only:** `na`/`nb` BIG, `da` BTI; partition enumeration for the un-scoped case reuses
  the same boundary-source primitive salvage/`tombstones` use, already format-gated.
- **Oracles:** the fixture's expected `only-a`/`only-b`/`resurrection_risk` values are derived from
  the Cassandra-written JSONL goldens plus `cassandra-5.0.8` purge-evaluator source, independently of
  `diff_table`'s own output (#3041/#3042).

## Open product decisions for the owner (do not decide here)

1. **Input shape (epic #4192 NEEDS-YOU #2).** Should `<snapshot-a-dir>`/`<snapshot-b-dir>` each be a
   *flat table directory* — the exact shape `--data-dir` already resolves for `query`/`explain`/
   `tombstones`, "two local dirs" being the epic's stated first cut, zero new resolution code — or a
   *real `nodetool snapshot`-shaped root* (`<dir>/<keyspace>/<table>-<uuid>/[snapshots/<tag>/]`) that
   `--table` walks to find the right subdirectory on each side, closer to what an operator actually
   has on disk but requiring new directory-walking code this change would otherwise not need.
   **Recommendation: flat table directory for this slice** (matches every sibling verb's resolution
   convention exactly); file the nodetool-layout resolver as a follow-up once this ships, since it is
   additive (a resolution-layer change, not a report-shape change).
2. **Are tombstones consistent across both sides rendered at all?** A tombstone present, with
   matching scope and deletion time, on both sides carries no divergence and no resurrection risk.
   **Recommendation: omit it from `tombstones[]` entirely** (matches "diff" semantics — the report
   shows disagreement, not a full inventory); the alternative (list every tombstone with a `status:
   both` entry) is easy to add later if an operator wants the full picture, but changes the JSON
   shape, so it is called out here rather than decided silently.
3. **Range-tombstone resurrection-risk scope.** The mandatory fixture (AC 1) exercises row, cell and
   partition tombstones only. Should V1 also cross-check RANGE tombstones for resurrection risk (the
   general case, reusing the same "is this cell inside the tombstone's clustering span with
   writetime `<=` deletion_time" predicate #4200's design sketched, implemented directly here since
   #4200 is unmerged) or defer range-tombstone resurrection-risk to a follow-up and report range
   tombstones as `only-a`/`only-b` without a risk classification in this slice?
   **Recommendation: include it** — the predicate is small (~30–50 lines) and its absence would leave
   the single tombstone kind most likely to shadow a lot of data unclassified in the tool built
   specifically to classify this hazard.
