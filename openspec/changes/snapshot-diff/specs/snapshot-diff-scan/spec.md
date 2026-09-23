# snapshot-diff-scan — new capability (snapshot-diff, issue #4201)

`cqlite-core` SHALL expose a read-only, two-source diff of the same table's reconciled state, built
entirely on top of #4193's reconciliation trail (`merge::trace::{TraceSink, RecordingSink,
CellDecision, TombstoneRecord}`, `merge::compute_gc_before`), with resurrection-risk classification
derived from Cassandra's own purge/shadow rules at the pinned `cassandra-5.0.8` tag. All requirements
are ADDED.

## ADDED Requirements

### Requirement: R1 — Each side is reconciled independently by the unmodified #4193 mechanism

`diff_table`/`diff_partition` SHALL drive `build_single_partition_merger` once per side, over that
side's own generation set only, in full-compaction configuration (`purge_safe(true)`,
`gc_before_secs = compute_gc_before(schema, now)`, `now_secs = Some(now)`) with a `RecordingSink`,
at the same pinned `now` for both sides. No new reconciliation rule is introduced.

#### Scenario: R1.1 both sides use the same `now` and the same merger
- **Given** `cqlite-core/tests/issue_4201_diff_resurrection.rs` and `test_tomb/resurrection_gc_positive`
  staged as two directories (A = both generations, B = generation 1 only, design.md §D6)
- **When** `diff_table` runs at `--now 1783205955`
- **Then** side A's own winner/tombstone trail equals what `explain`'s library entry point would
  compute for A alone at the same `now`, and likewise for B (asserted by calling both entry points
  and comparing, not by re-implementing reconciliation).

### Requirement: R2 — Cell status is one of `equal`, `only-a`, `only-b`, `newer-in-a`, `newer-in-b`

For every `(clustering, column)` key present in either side's winner set, `diff_table` SHALL report
exactly one of the five statuses, per design.md §D1's fold.

#### Scenario: R2.1 identical snapshots — every cell equal, exit affirmatively
- **Given** the same table staged identically as both A and B (same generations, same bytes)
- **When** `diff_table` runs
- **Then** every reported cell has status `equal`, `summary.only_a == summary.only_b == 0`, and a
  table with zero partitions is reported as `partitions_compared: 0` explicitly — never a silently
  empty result indistinguishable from an error (issue AC 2).

#### Scenario: R2.2 cell-granularity divergence within one row
- **Given** the `resurrection_gc_positive` fixture staged per §D6
- **When** `diff_table` runs at `--now 1783205955`
- **Then** partition `1` clustering `3`: column `val` is `only-b`, column `extra` is `equal` — proving
  classification is per-cell, not per-row (design.md §D6).

### Requirement: R3 — Tombstone status is `only-a` or `only-b`; a tombstone consistent on both sides is omitted

`diff_table` SHALL classify each tombstone as `only-a` or `only-b`. A tombstone whose
`(kind, scope, deletion_time, local_deletion_time)` matches on both sides SHALL NOT appear in the
report (proposal.md open decision 2's adopted default); a tombstone present on exactly one side
SHALL be reported with that side and, per R4, a `resurrection_risk`.

#### Scenario: R3.1 divergent tombstones only
- **Given** the §D6 fixture
- **When** `diff_table` runs
- **Then** `tombstones[]` contains exactly the row tombstone (clustering `2`), the cell tombstone
  (clustering `3`, column `val`) and the partition tombstone (partition `2`), each `side: "a"` — no
  entry for a tombstone that does not exist on side B, since none does.

### Requirement: R4 — Resurrection-risk classification derives from Cassandra's purge/shadow rules

For every `only-a`/`only-b` tombstone `T`, `diff_table` SHALL check the OTHER side's winner set for
any cell within `T`'s scope with `writetime <= T.deletion_time` (the shadow rule,
`DeletionPurger#shouldPurge`, `cassandra-5.0.8`). If none exists, `resurrection_risk.status = "no"`.
If at least one exists, `resurrection_risk.status = "yes"` and `.phase` SHALL be `"past-gc-grace"`
when `T.local_deletion_time < now - gc_grace_seconds` (strict `<`, the SAME boundary #1385/#4193's
`purgeable` verdict uses, `ColumnFamilyStore#gcBefore(long)`, `cassandra-5.0.8`) and
`"before-gc-grace"` otherwise. Every shadowed cell SHALL be named in `shadowed_cells`, never just
counted.

#### Scenario: R4.1 past-gc-grace, named cells, all three tombstone kinds
- **Given** the §D6 fixture at `--now 1783205955` (one second past the boundary, design.md §D6)
- **When** `diff_table` runs
- **Then** all three tombstones (row, cell, partition) report `resurrection_risk: {status: "yes",
  phase: "past-gc-grace"}`, and the partition tombstone's `shadowed_cells` names all 6 cells
  (`extra`/`val` × clustering 1–3) it shadows on side B.

#### Scenario: R4.2 before-gc-grace at the exact boundary second
- **Given** the same fixture at `--now 1783205954` (exactly `local_deletion_time + gc_grace_seconds`)
- **When** `diff_table` runs
- **Then** all three tombstones report `resurrection_risk: {status: "yes", phase: "before-gc-grace"}`
  — the shadow relationship is unchanged; only the gc_grace phase flips, exact to the second
  (mirrors #4193's R2.3 boundary test).

#### Scenario: R4.3 no resurrection risk when nothing is shadowed
- **Given** a synthetic two-generation fixture where side A's tombstone covers a key side B never
  wrote (no live data at that coordinate on either side)
- **When** `diff_table` runs
- **Then** the tombstone is reported `only-a` with `resurrection_risk: {status: "no"}` — the field is
  always present, never omitted, so silence after a `grep` means "checked, none found" (#4159 class).

### Requirement: R5 — Range tombstones are cross-checked the same way

A RANGE tombstone `only-a`/`only-b` SHALL be checked against the other side's winner set for any
cell whose clustering falls within the tombstone's span AND whose writetime is `<=` the deletion
time — the same shadow rule as R4, applied over a span instead of one coordinate.

#### Scenario: R5.1 range tombstone shadows live cells on the other side
- **Given** `test_tomb/wide_range_tombstone` staged as side A (the generation carrying the range
  tombstone) and an independently-staged side B built from the SAME base generation with the range
  tombstone's generation withheld (so B still holds the cells the range would shadow)
- **When** `diff_table` runs at a `now` past the tombstone's gc_grace boundary
- **Then** the range tombstone reports `only-a`, `resurrection_risk: {status: "yes",
  phase: "past-gc-grace"}`, and `shadowed_cells` names every clustering row inside the span that B
  still holds live, derived independently from the golden JSONL's writetimes (never from
  `diff_table`'s own prior output, #3042).

### Requirement: R6 — Symmetry: `diff(A, B)` and `diff(B, A)` are label-swapped mirrors

Swapping the two input directories SHALL swap only the `a`/`b` labels throughout the report
(`only-a`↔`only-b`, `newer-in-a`↔`newer-in-b`, `side_a`↔`side_b`, tombstone `side` and
`shadowed_cells[].side`) — every other field, including `resurrection_risk.phase`, is unchanged.

#### Scenario: R6.1 mechanical mirror property, any fixture
- **Given** `cqlite-core/tests/issue_4201_diff_symmetry.rs`, run over every fixture R1–R5 use
- **When** `diff_table(A, B, ...)` and `diff_table(B, A, ...)` both run at the same `now`
- **Then** relabeling one report's `a`↔`b` tags (including `newer-in-a`↔`newer-in-b`) and comparing
  it to the other report yields structural equality, asserted generically (not a hand-written mirror
  fixture per case) so the property holds for any future fixture without re-derivation.

### Requirement: R7 — Fail closed on an unreadable generation, either side

Building either side's trail SHALL return `Err` naming the side (`a`/`b`) and the unreadable
generation's path when a component cannot be read, and MUST NOT emit a partial report (the #4159
class) — no diff is computed from a shorter trail on one side.

#### Scenario: R7.1 truncated Statistics.db on side B
- **Given** a temp copy of the §D6 fixture with side B's `nb-1-big-Statistics.db` truncated
- **When** `diff_table` runs
- **Then** it returns `Err` naming `side=b` and the truncated path, and no `DiffReport` is produced.

### Requirement: R8 — Memory bound: one partition's pair of trails resident at a time

Full-table `diff_table` SHALL fold each partition's pair of `RecordingSink` outputs into the running
`DiffReport` and drop them before the next partition — no whole-table `Vec` of either side's cells is
ever materialized.

#### Scenario: R8.1 wide-row table stays within budget
- **Given** `test_wide_rows` staged identically on both sides (a pure `equal` case, exercising the
  full enumeration-and-fold path without divergence noise)
- **When** `diff_table` runs
- **Then** peak RSS stays within the crate's documented <128 MB target (same lane convention as
  salvage's R6 / #4200's R4.1, `memory-budget`).
