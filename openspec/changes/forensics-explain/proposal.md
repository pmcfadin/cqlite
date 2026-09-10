# forensics-explain — issue #4193 (epic #4192)

**Milestone:** unmilestoned (owner convention: unmilestoned = unscheduled program; board `Ready` so a
lane can take it without displacing the 0.18 headline). **Routing:** design-driven (OpenSpec + Seam 1)
— a new CLI verb and a new library trait surface, no external oracle for their *shape*; the verdicts
they emit ARE oracle-bound (Cassandra reconciliation source + Cassandra-written fixtures).

Source document: `docs/architecture/forensics-surface-2026-09.md` (PR #4191), slices **F0 + F1**.

## Why

CQLite's merge already decides, for every cell in every generation, whether it wins, is shadowed by
a newer write or a tombstone, has expired, is purgeable, or belongs to a dropped column — it has to,
to match Cassandra's compaction byte for byte. That decision is discarded at the egress: a `SELECT`
returns the reconciled answer and nothing else. Every operator question the idea document lists
("why does this row have this value", "why didn't my delete work", "is this tombstone droppable yet")
is answered by *keeping* that decision and rendering it. `sstabledump` cannot answer them: it shows
one SSTable at a time, raw, with the reader doing the reconciliation in their head.

This is a second egress from the engine we have, not a new engine. F0 is the only slice of the
program that touches the engine, and it is additive.

## What changes

**F0 — decision trail (library, `cqlite-core`).** A `TraceSink` trait in a new module
`storage/write_engine/merge/trace.rs`. The reconcile pipeline (`merge/reconcile.rs` Steps 1, 2b, 3,
3b, TTL expiry, 3c; `merge/mod.rs::apply_range_shadowing`; partition-deletion fold) reports each
per-cell and per-tombstone decision to the sink at the point it is made. `KWayMerger` gains a type
parameter `S: TraceSink = NoTrace`; every production constructor builds `KWayMerger<NoTrace>`, a
zero-sized sink whose methods are empty `#[inline]` bodies, so compaction and every read path
compile to today's code (owner ruling 2026-09-09: static dispatch). `RecordingSink` collects
`Vec<CellDecision>` per partition. `MergeEntry`, `MergeStep`, `CompactReport` gain no field.

**F1 — `cqlite explain <keyspace.table> <partition-key> [--clustering <ck>…] [--now <ts>]`
(CLI, `cqlite-cli`).** New top-level verb (owner ruling: `explain`; `cqlite query --explain` keeps
meaning "query plan"). Resolves the table directory and schema exactly as `query` does (`--schema`,
`--data-dir`/`--dataset`), seeks the partition across ALL generations through the existing
single-partition merger (`merge/point_read.rs::build_single_partition_merger`, forced even for a
single generation so there is ONE mechanism), runs that merger in **full-compaction configuration at
the pinned `now`** (`now_secs = now`, `gc_before = compute_gc_before(schema, now)`,
`purge_safe = true` because the input set is the table's whole generation set, dropped columns from
`effective_compaction_schema`) with a `RecordingSink`, writes nothing, and renders one line per cell
version per generation with a **verdict** and the **deciding fact**. Output `table` (default), `json`,
`csv`. The first line always states the `now` used.

**Closed verdict vocabulary**, 1:1 onto the reconcile rules: `winner`, `shadowed-by-timestamp`,
`shadowed-by-tombstone` (with tombstone kind `partition | range | row | cell | collection`, its
deletion time, and its gc_grace status at `now`), `expired`, `purgeable`, `dropped-column`. There is
no `unknown`.

## What this change must establish

1. **Zero cost untraced** — measured on the strict perf-regression benches, and structural
   (`NoTrace` is a ZST; production constructors are `KWayMerger<NoTrace>`; byte-parity suites unchanged).
2. **Every verdict has a Cassandra-written fixture in which it is the only correct answer**, at a
   pinned `now`, with the expectation derived from `cassandra-5.0.8` reconciliation source (#3041,
   #3042). A CQLite-written round trip cannot validate a verdict.
3. **The trail agrees with the answer** — the trail's `winner` set equals the existing read path's
   `SELECT *` row, under both `CQLITE_READ_PATH=point` and `=full`, columns compared in BOTH
   directions (#3890). This is the guard that "full-compaction configuration at `now`" and "what a
   read returns" coincide, which is the design's central claim.
4. **Wiring evidence from the binary** — `cqlite explain` driven end-to-end against committed fixtures.

## Non-goals

- `raw.cells` / `raw.tombstones` tables (F2), `cqlite tombstones` and `compact --dry-run --explain`
  (F3), `cqlite find` / CommitLog (F4), Trino `cqlite_raw` (F5), `--as-of` / `diff` (F6). Each is its
  own issue under #4192.
- Any write. `explain` never creates, modifies or compacts a file.
- Any change to what `SELECT` returns, on any surface. F0 is a side channel; the reconciled result
  is byte-identical (asserted by the unchanged parity suites).
- Python / Node / Flight exposure of the trail. Bindings are untouched in this slice.
- Distinguishing `bloom-negative` from `index-miss` for a generation that does not hold the key
  (F4 `find`); F1 reports `hit | absent | scanned` per generation.
- A `--as-of` writetime cutoff (F6). `--now` is the reconciliation clock (TTL + gc_grace), not time travel.

## Impact statements (openspec rules)

- **No-heuristics mandate (#28):** every verdict derives from the schema, `Statistics.db` (dropped
  columns, serialization header) and the cell bytes Cassandra wrote; no byte-pattern inference is
  introduced. The verdict is an *annotation* from the engine, never a filter.
- **Public binding surfaces:** Python/Node untouched. CLI gains one verb; `cqlite-core` gains one
  `pub` module (`merge::trace`) — `pub-surface` guard applies (unconditional, no inner `#![cfg]`).
- **<128 MB memory budget:** `explain` is one partition through the existing point-read seek; the
  recording sink is bounded by that partition's cell count across generations and the rendered
  output is governed by the existing `max_result_bytes` budget. A wide partition is large by
  construction and streams like any other query.
- **Gate:** file-size ratchet — `merge/mod.rs` (12,346 lines) and `merge/reconcile.rs` (803) are
  over threshold and both are touched; see design §Campsite for the split this change carries.
  New CLI test target `explain_cli_tests` must be named in `cli-tests`' target list (#3522) or it
  runs nowhere.
