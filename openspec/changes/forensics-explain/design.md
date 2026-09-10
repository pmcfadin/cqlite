# Design — forensics-explain (issue #4193)

## D1. Where the decisions are made today (the trail hooks exactly these sites)

`merge/reconcile.rs::ReconcileState` runs the per-cluster pipeline in this order
(`merge/mod.rs::reconcile_cluster_with_overlap_counted` drives it):

| Step | Function | Decision made | Verdict emitted for the loser |
|---|---|---|---|
| fold | `fold_row_deletions` | partition / row deletion carried into the cluster | (tombstone recorded, kind `partition` / `row`) |
| 1 | `resolve_cell_winners` → `reconcile_rules::cell_wins` | per-column winner: strictly higher ts wins; equal ts, tombstone beats live/expiring; equal+equal keeps first seen | `shadowed-by-timestamp` (decided by the winner's generation + writetime) or, when the equal-ts winner is a cell tombstone, `shadowed-by-tombstone{cell}` |
| 2b | `apply_complex_deletions` → `complex_deletion_supersedes`, `element_survives_complex_deletion` | one active complex-deletion marker per column; elements with `ts <= mfda` are shadowed | `shadowed-by-tombstone{collection}` (decided by marker mfda + generation); superseded markers: `shadowed-by-timestamp` |
| range | `mod.rs::apply_range_shadowing` (before the cluster reconcile) | rows covered by a coalesced range tombstone | `shadowed-by-tombstone{range}` (decided by range bounds + deletion time) |
| 3 | `shadow_by_row_deletion` | cells with `ts <= row_del` (`<=`, #498) | `shadowed-by-tombstone{row}` or `{partition}` (whichever fold carried) |
| 3b | `filter_dropped_columns` | cells with `ts <= drop_time` | `dropped-column` (decided by drop time from the serialization header) |
| ttl | `expire_ttl_cells` | `localDeletionTime < now_secs` ⇒ converted to a cell tombstone | `expired` (decided by `expires_at` vs `now`) |
| 3c | `purge_gc_grace` | tombstones with `ldt < gcBefore` (strict `<`, #1385), overlap-safe | `purgeable` (decided by `ldt`, `gc_grace`, `now`) |
| build | `build` | survivors become the emitted row | `winner` |

These are the ONLY sites. The verdict vocabulary is therefore closed by construction: a decision
that reaches `build` without a recorded verdict is `winner`; every drop happens in a named step
that names its verdict. An `unknown` cannot arise, and the spec makes a sink that receives an
unrecognised decision a test failure.

## D2. Mechanism — static-dispatch trace sink (owner ruling)

New module `cqlite-core/src/storage/write_engine/merge/trace.rs`:

```rust
pub trait TraceSink {
    fn cell(&mut self, d: CellDecision);          // one per cell version, at decision time
    fn tombstone(&mut self, t: TombstoneRecord);  // one per tombstone seen (partition/range/row/cell/collection)
    fn generation_probe(&mut self, run_index: usize, outcome: ProbeOutcome); // hit | absent | scanned
}
#[derive(Default, Clone, Copy)] pub struct NoTrace;   // ZST; every method is an empty #[inline] body
pub struct RecordingSink { cells: Vec<CellDecision>, tombstones: Vec<TombstoneRecord>, probes: Vec<(usize, ProbeOutcome)> }
pub enum Verdict { Winner, ShadowedByTimestamp, ShadowedByTombstone(TombstoneKind), Expired, Purgeable, DroppedColumn }
pub enum TombstoneKind { Partition, Range, Row, Cell, Collection }
pub struct CellDecision { run_index, clustering: Option<ClusteringKey>, column: String, value: Option<Value>,
                          writetime: i64, ttl: Option<i32>, expires_at: Option<i64>, verdict: Verdict, decided_by: DecidedBy }
pub enum DecidedBy { Winner{run_index, writetime}, Tombstone{kind, run_index, deletion_time, local_deletion_time, droppable_at_now: bool},
                     DropTime(i64), Expiry{expires_at, now}, GcGrace{ldt, gc_before, now}, None }
```

- `KWayMerger<S: TraceSink = NoTrace>`; every existing constructor returns `KWayMerger<NoTrace>`
  (call sites keep compiling unchanged); `with_trace_sink<S2>(self, sink: S2) -> KWayMerger<S2>`
  moves the runs/heap/config into a traced merger. `ReconcileState` step methods take
  `&mut impl TraceSink` (or `&mut S` through a generic `reconcile_cluster_*`) and call the sink at
  the site in D1. `NoTrace` monomorphizes each call to nothing — the same construction the
  observability layer already uses (`benches/observability_overhead.rs`: "helpers are `#[inline]`
  and branch on a const predicate"), and here there is not even a branch.
- **Generation identity is `run_index`.** `KWayMerger` does not retain input paths (runs are boxed
  iterators). The point-read builder has the ordered candidate `paths` (run index = position), so
  the CLI keeps that list and renders `run_index` as the SSTable's basename (`nb-2-big`). The sink
  never sees a path.
- **What was beaten:** (a) a side channel on `MergeEntry` — every hot-path row pays a field and the
  egress types change; (b) a separate explain-mode reconciler — two copies of the rules to keep in
  sync, exactly the drift #947 just collapsed. Static dispatch is one mechanism at zero cost; the
  measured claim is R1 in the spec, not this paragraph.

## D3. `explain` = the full compaction of this table's generations at `--now`, with nothing written

This is the central claim and the spec's R6 guards it. `explain` builds the merger from ALL
generations of the table via `build_single_partition_merger` (so `sstables_pruned`, bloom/index
probes and the fail-safe scan all behave as a point read would), then configures it as a FULL
compaction: `with_now_secs(Some(now))`, `gc_before_secs = compute_gc_before(schema, now)`,
`with_purge_safe(true)` (the input set is the whole table, so the #935 overlap gate is `+inf` —
the same posture `issue_1385_gc_grace_boundary.rs` documents), `max_purgeable_timestamp = None`,
schema from `effective_compaction_schema(schema, paths)` so dropped columns are authoritative.

Why this equals what a read returns: shadowing (Steps 1/2b/range/3/3b) runs BEFORE purge (3c), so a
purgeable tombstone still hides its data in the trail; TTL expiry in-merge (`expire_ttl_cells`)
subsumes the read path's post-merge `PartitionShadow::cell_shadowed_or_expired`; and the point-read
merger "reconciles byte-identically to the scan" (#2207). So the `winner` set is the read result and
the trail additionally carries `purgeable` for tombstones past gc_grace — which is the annotation
operators asked for. R6 asserts the equality on every fixture under both read paths rather than
trusting this argument.

The single-generation case is forced through the same merger (no `#1741` single-gen shortcut) so
there is one mechanism; `explain` is not a hot path.

## D4. CLI shape

```
cqlite [--schema … --data-dir …|--dataset …] explain <keyspace.table> <partition-key> [--clustering <v>…] [--now <epoch-secs|RFC3339>] [--out table|json|csv]
```

- `<partition-key>`: CQL literal(s) in schema order, comma-separated for composite keys, parsed
  with the same literal parser `query` uses for `WHERE pk = …` — no new grammar.
- `--clustering`: render-side filter only. The whole partition is always reconciled (range
  tombstones and row deletions cannot be evaluated on a slice).
- `--now`: absent ⇒ wall clock, and the first line says so. Every test passes it.
- First line, all formats: `now=<epoch-secs> (<RFC3339>)  generations=<n> [<basename>, …]  gc_grace_seconds=<g>`.
- `json`: `{ "now": …, "generations": [{"run_index","sstable","probe"}], "cells": [CellDecision…],
  "tombstones": [TombstoneRecord…] }` with stable key names; `csv`: the `cells` rows flattened with
  the same keys (tombstone rows appended with `kind` prefixed `tombstone:`); `table`: the human
  rendering in the idea document §4.1.
- Exit codes: `0` rendered (including "key held by 0 generations"); `1` usage / table or schema not
  resolvable; `2` a generation could not be read (named). No shorter trace, ever (#4159 class).

## D5. Campsite (file-size ratchet)

`merge/mod.rs` (12,346 lines) and `merge/reconcile.rs` (803) are both over threshold and both are
hooked. Carried in this change: move `reconcile_cluster`, `reconcile_cluster_with_overlap*` and
`apply_range_shadowing` (+ their coalesce helpers) out of `mod.rs` into a new
`merge/reconcile_cluster.rs` — pure relocation, no behaviour change, byte-parity suites prove it.
`reconcile.rs`'s growth is the sink calls only (one line per step); if the ratchet still trips,
split its two `#[cfg(test)]` modules into `reconcile/` (a sibling dir that already exists) rather than
opting out. `CQLITE_ALLOW_FILE_GROWTH=1` is the last resort and shows as `OPT-OUT` in the SUMMARY.

## D6. Fixtures that pin each verdict (all Cassandra-written, committed JSONL references)

| Verdict | Fixture (table dir) | Gens | Pinned `now` |
|---|---|---|---|
| `shadowed-by-timestamp` | `test_tomb/resurrection_gc_positive` | 2 | any; ts order decides |
| `shadowed-by-tombstone{row}` | `test_compaction_tombstone_ttl/shadow_row_delete` | 1 | inside gc_grace |
| `shadowed-by-tombstone{range}` | `test_compaction_tombstone_ttl/rt_cross_gen`, `test_tomb/wide_range_tombstone` | 1 | inside gc_grace |
| `shadowed-by-tombstone{cell}` | `test_tomb/static_with_tombstones` | 1 | inside gc_grace |
| `shadowed-by-tombstone{partition}` | `test_tomb/skipped_partition_delete` | 2 | inside gc_grace |
| `shadowed-by-tombstone{collection}` | a `test_collections` / `test_compactionparity` fixture with a complex deletion (implementer names it from the parity manifest; if none exists, generate with `test-data/scripts/generate-tombstone-parity.sh` and commit the JSONL — never CQLite-written) | — | inside gc_grace |
| `expired` | `test_compaction_tombstone_ttl/ttl_expired_live` | 1 | after `expires_at` |
| `purgeable` | `test_tomb/gc_before_boundary` | 1 | `ldt + gc_grace` and `+1` (#1385 ±1 s) |
| `dropped-column` | `test_tomb/dropped_regular_col` | 2 | any |

Expected verdicts are derived from `cassandra-5.0.8` (`Cells#reconcile`, `Rows#merge`,
`DeletionPurger`, `AbstractCell.purge`) and written into the test as literals with the source cited;
never read back from CQLite. Roots resolve per TABLE (`sstables_root_for_table`, #3220), asserted
per case, fail-closed on a missing committed fixture.

## D7. Naming collision, recorded

`cqlite query --explain` (a query-plan flag) predates this verb. Owner ruling 2026-09-09: the verb is
`explain`; the flag keeps its meaning. The `explain` verb's help text says "reconciliation trace for
one partition; for a query plan use `query --explain`".
