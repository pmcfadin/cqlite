# snapshot-diff — issue #4201 (epic #4192)

**Milestone:** 0.19 (owner ruling 2026-11-21: `cqlite diff` joins 0.19, the SSTable utility release; the
tombstones report #4200 stays in 0.20 beside purge #4276).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — a new read-only verb whose input
shape, tombstone-identity matching and report granularity have real latitude, built on top of #4193's
reconciliation trail (merged, #4546); the resurrection-risk classification itself is oracle-bound to Cassandra's own
purge/reconciliation rules (`cassandra-5.0.8` source), never invented.

## Status refresh (2026-11-21, mechanical; Seam 1 approval of 2026-09-23 stands)

#4193 (`cqlite explain` and the F0 decision trail) is **merged to `origin/main`** (#4546, `7dd80495a`)
and its OpenSpec change is archived. The primitives this spec was drafted against exist under their
real names:

- `merge::trace::{TraceSink, RecordingSink, CellDecision, TombstoneRecord, TombstoneKind, DecidedBy,
  Verdict, ProbeOutcome}` (`cqlite-core/src/storage/write_engine/merge/trace.rs`);
- `merge::build_traced_merger_with_registry(paths, keys, schema, Some(&udt_registry), scan_cancel,
  sink)` (the traced point-read builder; it supersedes the draft's `build_single_partition_merger`,
  whose untraced form `explain` does not use), then `KWayMerger::with_now_secs` /
  `with_gc_before_secs` / `with_purge_safe(true)`; the sink rides the merger (`with_trace_sink` to
  attach one, `into_trace_sink` to take the recorded trail back);
- `merge::{compute_gc_before, effective_compaction_schema}`;
- `cqlite-cli/src/commands/explain_resolve.rs::{resolve_table_dir, discover_live_generations}` —
  keyspace-qualified table-directory resolution and live-generation discovery that follows
  Cassandra's `Directories.SSTableLister` (flat listing; `snapshots/`, `backups/` and `.<index>`
  subdirectories are never live). `diff` reuses both; it does NOT reintroduce recursive discovery.

The fixture and oracle version is **Cassandra 5.0.8** (`datasets-v4`); the reconciliation layout the
trail is wired into is the post-#4551/#4589 one (`merge/reconcile/build.rs`,
`merge/reconcile/marker.rs`, `merge/reconcile/steps.rs`, `merge/reconcile_cluster.rs`). None of this
changes a requirement, a scenario outcome or the CLI surface.

**#4200 (tombstone-surgery) is a sibling, not a foundation.** It reuses the same #4193 primitives;
#4201 has no code dependency on #4200's `tombstone_density` module and implements the one
capability it needs from #4200's sketch (a range tombstone's clustering-span predicate) directly.

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
per-partition merger `explain` drives. `diff` adds nothing new to the reconciliation
engine — it drives that SAME traced merger once per side (each side's own generation set, independently),
then compares the two sides' winner sets and tombstone trails instead of rendering one side's trail
alone.

## What changes

**Library (`cqlite-core`).** A `snapshot_diff` module under `storage/write_engine/` exposing
`diff_table(side_a, side_b, schema, udt_registry, options) -> Result<DiffReport>` (full-table) and
`diff_partition(side_a, side_b, schema, udt_registry, partition_key, options) -> Result<PartitionDiff>`
(`--partition`-scoped), where each side is its label plus the live generations the CLI resolved
(`explain_resolve`). For each side independently: build the per-partition merger across THAT
side's own generations via `build_traced_merger_with_registry`, run it in full-compaction
configuration at the pinned `now` (`purge_safe(true)`, `gc_before_secs = compute_gc_before(schema,
now)`) with a `RecordingSink` — the identical mechanism #4193's `explain` uses, applied to two
independent inputs rather than one. Compare the two sides' winner cells (`equal` /
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
   generations, `test_tomb`, `gc_grace_seconds=864000`, `datasets-v4` / Cassandra 5.0.8) pins the boundary to the second, mirroring
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
  driven by the SAME merger `explain`/`compact` use; no independent byte-pattern
  divergence heuristic.
- **Uncompressed-write claim boundary (#1406):** not engaged — this change writes nothing to either
  input or to a new output; it only reads.
- **Cassandra 5.0 only:** `na`/`nb` BIG, `da` BTI; partition enumeration for the un-scoped case reuses
  the structural Data.db partition walk `rebuild` uses
  (`SSTableReader::distinct_partition_keys_with_positions`), already format-gated by
  `SSTableReader::open`.
- **Oracles:** the fixture's expected `only-a`/`only-b`/`resurrection_risk` values are derived from
  the Cassandra-written JSONL goldens (`datasets-v4`, Cassandra 5.0.8) plus `cassandra-5.0.8` purge-evaluator source, independently of
  `diff_table`'s own output (#3041/#3042).

## Product decisions — RESOLVED by the owner at Seam 1, 2026-09-23 (all three adopted as recommended)

1. **Input shape (epic #4192 NEEDS-YOU #2).** Should `<snapshot-a-dir>`/`<snapshot-b-dir>` each be a
   *flat table directory* — the exact shape `explain_resolve::resolve_table_dir` /
   `discover_live_generations` already resolve, "two local dirs" being the epic's stated first cut,
   zero new resolution code — or a
   *real `nodetool snapshot`-shaped root* (`<dir>/<keyspace>/<table>-<uuid>/[snapshots/<tag>/]`) that
   `--table` walks to find the right subdirectory on each side, closer to what an operator actually
   has on disk but requiring new directory-walking code this change would otherwise not need.
   **Ruling: flat table directory for this slice** (matches every sibling verb's resolution
   convention exactly); file the nodetool-layout resolver as a follow-up once this ships, since it is
   additive (a resolution-layer change, not a report-shape change).
2. **Are tombstones consistent across both sides rendered at all?** A tombstone present, with
   matching scope and deletion time, on both sides carries no divergence and no resurrection risk.
   **Ruling: omit it from `tombstones[]` entirely** (matches "diff" semantics — the report
   shows disagreement, not a full inventory); the alternative (list every tombstone with a `status:
   both` entry) is easy to add later if an operator wants the full picture, but changes the JSON
   shape, so it is called out here rather than decided silently.
3. **Range-tombstone resurrection-risk scope.** The mandatory fixture (AC 1) exercises row, cell and
   partition tombstones only. Should V1 also cross-check RANGE tombstones for resurrection risk (the
   general case, reusing the same "is this cell inside the tombstone's clustering span with
   writetime `<=` deletion_time" predicate #4200's design sketched, implemented directly here since
   #4200 is unmerged) or defer range-tombstone resurrection-risk to a follow-up and report range
   tombstones as `only-a`/`only-b` without a risk classification in this slice?
   **Ruling: include it (R5 in scope)** — the predicate is small (~30–50 lines) and its absence would leave
   the single tombstone kind most likely to shadow a lot of data unclassified in the tool built
   specifically to classify this hazard.
