# tombstone-density-scan — new capability (tombstone-report, issue #4200 slice 1)

`cqlite-core` SHALL expose a read-only table-wide tombstone density/coverage scan built entirely on
top of #4193's reconciliation trail (`merge::trace::{TraceSink, RecordingSink, CellDecision,
TombstoneRecord}`, `merge::compute_gc_before`). All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Counts by kind and generation match the Cassandra-written goldens

`scan_tombstone_density` SHALL drive the SAME per-partition merger `explain` drives, once per
partition across every generation of the table, and SHALL tally every `TombstoneRecord` the trail
emits by kind (`partition` / `range` / `row` / `cell` / `collection`) and by the generation
(`run_index` → sstable basename) that carried it.

#### Scenario: R1.1 per-kind, per-generation totals equal the golden-derived count
- **Given** `cqlite-core/tests/issue_4200_tombstone_report.rs`, every subdirectory of `test_tomb` and
  `test_compaction_tombstone_ttl`
- **When** `scan_tombstone_density` runs at a pinned `now` for each fixture
- **Then** for every kind and every generation, the reported count equals a count the test computes
  independently from that fixture's Cassandra-written `*.jsonl` sstabledump goldens (never from
  `scan_tombstone_density`'s own output, per #3042) — a mismatch names the kind, the generation and
  both numbers.

#### Scenario: R1.2 an empty table reports zero, affirmatively
- **Given** a table with no tombstones anywhere
- **When** the scan runs
- **Then** every count is `0`, and the report's own presence (not the absence of an error) is what a
  caller reads as "measured, found none" — never `unmeasured`.

### Requirement: R2 — Range-tombstone shadow counts equal the golden-derived live-cell count

For every RANGE tombstone, `scan_tombstone_density` SHALL report the clustering span it covers and
the count of live cells in that span whose writetime is `<=` the tombstone's deletion time — the
count of `CellDecision`s the trail attributes to that marker with verdict
`ShadowedByTombstone(Range)`.

#### Scenario: R2.1 single-generation range shadow count
- **Given** `test_tomb/wide_range_tombstone`
- **When** the scan runs
- **Then** the reported `live_cells_shadowed` for the table's range tombstone equals a count the test
  derives independently from the golden JSONL: cells inside the tombstone's clustering span with
  `writetime <= deletion_time` — the literal Cassandra range-tombstone shadowing rule at
  `cassandra-5.0.8`, never from the trail's own count.

#### Scenario: R2.2 cross-generation range shadow count
- **Given** `test_compaction_tombstone_ttl/rt_cross_gen` (a range tombstone in one generation
  shadowing live cells written in another)
- **When** the scan runs across BOTH generations
- **Then** `live_cells_shadowed` counts shadowed cells from EITHER generation, derived the same way
  as R2.1 but applied across both goldens — proving the count is table-wide, not per-file.

### Requirement: R3 — Droppable-at-`now` agrees with #4193's `purgeable` verdict, exactly

The scan SHALL compute `droppable_at_now` for every tombstone using the SAME
`compute_gc_before(schema, now)` formula #4193's `explain` uses for its `purgeable` annotation, so
the two never disagree for the same partition at the same `now`.

#### Scenario: R3.1 boundary agreement at `gc_before_boundary`
- **Given** `test_tomb/gc_before_boundary` at `now` one second either side of the documented boundary
  (#1385's `<` rule)
- **When** both `scan_tombstone_density` and `explain`'s per-partition trail run at the same `now`
- **Then** `droppable_at_now` for the report's tombstone and `purgeable` for `explain`'s equivalent
  `CellDecision`/`TombstoneRecord` agree on both sides of the boundary.

### Requirement: R4 — Memory bound: one partition's trail resident at a time

The scan SHALL fold each partition's `RecordingSink` output into the running aggregate and drop it
before moving to the next partition — no whole-table `Vec<CellDecision>` is ever materialized.

#### Scenario: R4.1 wide-row table stays within budget
- **Given** `test_wide_rows` (the existing memory-budget fixture)
- **When** the scan runs
- **Then** peak RSS stays within the crate's documented <128 MB target (same lane convention as
  salvage's R6/`memory-budget`, #4196 design D7).
