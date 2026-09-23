# Design — tombstone-report (issue #4200, slice 1)

## D1. The plan: `explain`'s per-partition trail, driven over every partition, aggregated not rendered

```
partition enumeration (BIG Index.db entries | BTI Partitions.db trie leaves — read-only walk,
                        the SAME primitive salvage/boundaries.rs and explain use)
      │
      ▼
for each partition:
   build_single_partition_merger(all generations) ──► with_trace_sink(RecordingSink)
      ──► drive to completion in FULL-compaction config:
            purge_safe(true), gc_before_secs = compute_gc_before(schema, now), now_secs = Some(now)
      ──► RecordingSink { cells: Vec<CellDecision>, tombstones: Vec<TombstoneRecord>, .. }
      ▼
fold into DensityReport:
   - tombstone_count[kind][generation] += 1 per TombstoneRecord
   - droppable_at_now += 1 per TombstoneRecord where droppable_at_now (from DecidedBy::Tombstone)
   - for each Range TombstoneRecord: shadow_count = |{ CellDecision c : c.verdict ==
     ShadowedByTombstone(Range) && c.decided_by names THIS marker }|
   - per_generation attribution keyed by run_index -> sstable basename (explain's own convention)
```

This is deliberately NOT a new reconciliation primitive. It is #4193's `explain` mechanism
(`merge/trace.rs::TraceSink`/`RecordingSink`, `merge/point_read.rs::build_single_partition_merger`,
`merge/mod.rs::compute_gc_before`) driven once per partition instead of once for an operator-named
key, with the render step replaced by a fold. Any defect in the trail itself is #4193's to fix, not
re-litigated here; this change owns only the enumeration-and-fold layer and its own tests.

**Placement.** `cqlite-core/src/storage/write_engine/tombstone_density/` (new files; file-size
ratchet not engaged by construction, matching salvage's D7 precedent). Depends on `merge::trace`
being `pub(crate)`-visible from a sibling module in `write_engine/` — confirm in tasks.md Group 0
once #4193 lands; if `trace.rs`'s types are private to `merge`, this change's first task is a minimal
visibility change (`pub(super)` → `pub(crate)`), not a design change.

## D2. Why full-table enumeration is safe at the file-size / memory target

Unlike `salvage`, this verb never buffers a partition's write output — only its `RecordingSink`
Vecs, which are dropped after each partition is folded into the running `DensityReport` (bounded:
counts by kind/generation, plus a `--top N`-bounded set of the widest/most-shadowing range
tombstones — see D3). The <128 MB target is met by folding-and-dropping per partition, the same
"one partition resident at a time" posture salvage's D7 establishes, applied to the trail's `Vec`s
instead of a writer's row buffer.

## D3. Report shape (table default; `--top N` bounds the range-tombstone listing)

```json
{ "now": "<RFC3339>", "table": "<ks.tbl>", "generations": ["nb-1-big", "nb-2-big"],
  "gc_grace_seconds": 864000,
  "totals": { "partition": 2, "range": 5, "row": 11, "cell": 40, "collection": 3,
              "droppable_at_now": 38 },
  "by_generation": { "nb-1-big": { "partition": 1, "range": 3, ... }, "nb-2-big": { ... } },
  "range_tombstones": [
    { "partition_key": "<rendered>", "clustering_start": "<rendered>", "clustering_end": "<rendered>",
      "generation": "nb-1-big", "deletion_time": "<RFC3339>", "local_deletion_time": 1234567890,
      "droppable_at_now": true, "live_cells_shadowed": 12 }
  ],
  "range_tombstones_truncated": 0
}
```

`range_tombstones` is capped at `--top N` (default: unbounded up to a resident-entry ceiling
mirroring salvage's `losses_truncated` pattern — `range_tombstones_truncated` is the affirmative
overflow count, never a silent cap). Ordering: widest-shadow-first, so `--top N` on a large table
surfaces the tombstones worth acting on first, not file order.

## D4. Oracles (all Cassandra-written; never CQLite round-trip alone, #3042)

| Case | Fixture | Expected value derived by |
|---|---|---|
| tombstone counts by kind, per generation | `test_tomb/*`, `test_compaction_tombstone_ttl/*` (every subdirectory) | the test independently counts tombstone markers (partition/range/row/cell/collection deletions) directly from the Cassandra-written `*.jsonl` sstabledump goldens — never from `scan_tombstone_density`'s own output |
| range-tombstone live-cell shadow count | `test_tomb/wide_range_tombstone` | the test independently computes, from the golden JSONL's cell writetimes and the range tombstone's deletion time, which cells satisfy `writetime <= deletion_time` and fall inside the clustering span — the literal Cassandra range-tombstone shadowing rule, at the pinned `cassandra-5.0.8` `RangeTombstoneList`/`Slices` semantics, never from the trail's own count |
| cross-generation range shadowing | `test_compaction_tombstone_ttl/rt_cross_gen` | same rule, applied across BOTH generations' goldens — the range tombstone in one generation shadowing live cells written in the OTHER, proving the report attributes shadow counts across the whole table, not per-file |
| purgeable / droppable_at_now | every fixture, at a `now` derived per-case from `gc_before_boundary`'s documented ±1s boundary (#1385) | `compute_gc_before(schema, now)` applied by the TEST independently (same formula, re-derived, not called through the report) against each tombstone's `local_deletion_time` from the golden |

## D5. Slice 2 sketch — `cqlite purge` (design only; NOT specced in this change)

Recorded here so the owner can evaluate the split, not as a commitment to this shape.

`purge <table-dir> --out <dir> --now <ts>` is, mechanically, the SAME full-table-in configuration
D1 uses for the trail — `compact_sstables` over every generation under `<table-dir>`, with
`purge_safe(true)`, `gc_before_secs = compute_gc_before(schema, now)`, `now_secs = Some(now)` —
except it drives a real `SSTableWriter` instead of a `RecordingSink`, exactly like `cqlite compact
--major` does today (`cqlite-cli/src/cli_types.rs::CompactArgs`). The differences from `compact
--major` that justify a distinct verb rather than a `compact` flag:

- `--now` is **mandatory** (issue AC 4); `compact --major` accepts an implicit wall-clock `--now-sec`
  omission today, which is exactly the "no wall-clock purge" hazard #4200 exists to close.
- `gc_before` is **computed** from the schema's `gc_grace_seconds` via `compute_gc_before`, never
  operator-supplied — `compact --gc-before` today requires the operator to get the arithmetic right
  by hand, which is itself a resurrection hazard (an over-generous `gc_before` purges tombstones a
  real compaction would have kept).
- A **manifest** of what was dropped, by kind and partition (design goal: reuse the SAME
  `PurgeCounts`-shaped tally `compact_sstables` already accumulates for
  `cqlite.compaction.tombstones_suppressed`/`tombstones_emitted`/`tombstones_purged` (#2163), plus
  per-partition attribution from the trail if `purge` is driven through a `RecordingSink` in
  addition to the real writer — open design question, not resolved here).
- **Byte-for-byte oracle**: `purge` output MUST equal `compact_sstables`'s output at the same
  `--now`/computed `gc_before` (issue AC 3) — this is a parity assertion against CQLite's OWN
  existing, already byte-parity-proven compaction path, which is a legitimate oracle for
  "purge behaves like compaction" (an internal consistency claim) but NOT a substitute for a
  Cassandra-written oracle on "compaction purges correctly" (#3042) — that already-established
  oracle is `compaction-byte-parity`'s own suite, unchanged by this design, not re-verified here.

**Safety contract — the open product decisions in proposal.md apply here directly.** `compact
--major`'s existing posture is an unverified operator assertion that `<input-dir>` holds every
overlapping SSTable; `purge`'s framing in the epic ("purging data still shadowing live replicas is a
resurrection risk") suggests the bar should be higher, but CQLite cannot verify cluster-wide
completeness from local files alone — the honest limit of any local tool. The three product
decisions in proposal.md (dry-run posture, completeness assertion, required-flag bar) gate whether
slice 2's spec can even be written, since they determine `purge`'s R-series exit-code/refusal
contract the same way `salvage`'s D3 failure table anchors every one of its scenarios.

## D6. Sizing (why slice 1 alone, not both)

Slice 1: one new core module (enumeration + fold, ~250–400 lines), one CLI verb (~150–250 lines),
tests against 2 fixture directories with per-kind/per-generation/shadow-count assertions (~600–900
lines, matching salvage's per-scenario test density). Total estimate: **1.4–2k lines including
tests** — within the single-PR budget.

Slice 2 (`purge`), sketched above, is materially larger: a new writer-driving CLI verb with its own
D3-shaped failure/exit-code table, a manifest schema, a byte-parity oracle suite mirroring salvage's
own multi-round hardening history (salvage's PR took 9 roborev rounds to close write-path safety
gaps — `--out`/`--manifest` path-resolution guards, partial-write exit-code correctness, and so on),
plus the three unresolved safety-contract decisions above that must be answered BEFORE its spec can
be written, since they change its requirement scenarios' shape, not just its implementation. Bundling
both in one PR risks exactly the outcome salvage's own tasks.md documents repeatedly: reviewers
finding real safety gaps round after round in a large write-path diff. Splitting lets slice 1 (report,
no write path, no resurrection risk) ship and be reviewed on its own, independent timeline from
slice 2's higher-stakes write path.
