# core-scrub — new capability (sstable-scrub, issue #4198)

`cqlite-core` SHALL provide a `scrub` module that reconciles every generation of one table through
the existing `compact_sstables`/`KWayMerger` path, corrects out-of-order partitions/rows into a
separate sorted output (Cassandra `Scrubber` behaviour), and — only under `--skip-corrupted` — drops
undecodable partitions using the SAME `LossClass`/decode-at-offset machinery issue #4196 (`salvage`)
already established. All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — A healthy table reconciles exactly like a no-purge compaction of it

Scrub SHALL produce output byte-identical to `compact_sstables` run over the same generations with
`purge_safe = false`, for a table whose generations are already fully in order and fully decodable
and with `--purge` absent — no new claim, the existing `compact --major`-adjacent oracle reused.

#### Scenario: R1.1 healthy multi-generation table, default (no `--purge`)
- **Given** a committed `test_basic`/`test_da` table with ≥2 generations, no corruption, no
  out-of-order material
- **When** `scrub_table` runs with `purge: false, skip_corrupted: false, reinsert_overflowed_ttl:
  false`
- **Then** the output is byte-identical to `compact_sstables(purge_safe=false)` over the same input
  generations, `out_of_order` is empty for every generation, and the report exits clean
  (`cqlite-core/tests/issue_4198_scrub_parity.rs`).

#### Scenario: R1.2 `--purge` asserts the same `purge_safe` contract as `compact --major`
- **Given** the same fixture, plus a gc-grace-expired tombstone
- **When** scrub runs with `purge: true`
- **Then** the output matches `compact_sstables(purge_safe=true)` for the same generation set —
  the expired tombstone is purged, identically to what `cqlite compact --major` would do over the
  same complete set (`cqlite-core/tests/issue_4198_scrub_parity.rs`).

### Requirement: R2 — Out-of-order partitions and rows are diverted, sorted, never silently reordered or dropped

Scrub SHALL write any out-of-order material — partitions or, within a partition, rows that are NOT
in the source's own physical key/clustering order — to a separate, sorted output
(`<out>/<generation>-outoforder`) rather than (a) silently re-sorting it into the primary output as
if it had always been in order, or (b) dropping it.

#### Scenario: R2.1 out-of-order partitions (BIG)
- **Given** a committed fixed-offset byte mutation of a Cassandra-written BIG generation's Index.db
  that swaps two adjacent partition-index entries (Data.db itself unmodified)
- **When** scrub runs
- **Then** the primary output is fully ordered and verify-clean, containing every partition except
  the swapped pair; `<out>/<generation>-outoforder` holds exactly those two partitions, correctly
  sorted; the report names both partition keys under `out_of_order`
  (`cqlite-core/tests/issue_4198_scrub_out_of_order.rs`).

#### Scenario: R2.2 intra-partition row order is a NAMED, distinct case
- **Given** the CASSANDRA-12127 shape — two rows within a single partition physically out of order —
  is a structurally different code path from R2.1 (row-level vs partition-level divergence, mirrored
  from `SortedTableScrubber`'s `OrderCheckerIterator` vs `saveOutOfOrderPartition`)
- **When** this slice ships without a dedicated fixture for it (a fixture-generation cost tradeoff
  recorded in `tasks.md`'s premise group)
- **Then** the gap is stated explicitly in this spec and in the report's own module doc — never
  implied covered by R2.1 — and tracked as a named follow-up rather than silently absent
  (mirrors the #4229 pattern #4196 used for its own deferred scenarios).

### Requirement: R3 — `--skip-corrupted` reuses salvage's loss machinery; the loss set is provably identical

`--skip-corrupted` SHALL classify and drop an undecodable partition through the SAME
`decode_partition_at_offset_for_salvage` function and `LossClass` enum issue #4196 established —
never a second, scrub-owned decode-and-classify path.

#### Scenario: R3.1 loss set equals salvage's loss set for the same input
- **Given** `test_comp_corrupt/data_db_bit_flip` (skip-clean if absent; required under
  `CQLITE_REQUIRE_FIXTURES=1`)
- **When** `scrub_table(..., skip_corrupted: true)` and `salvage_sstable` both run against the same
  generation
- **Then** scrub's `losses[]` for that generation equals salvage's `losses[]` field-for-field (key,
  offset, chunk indices, class, message) — asserted by direct structural comparison, not by two
  independently-derived expectations (`cqlite-core/tests/issue_4198_scrub_skip_corrupted_parity.rs`).

### Requirement: R4 — Without `--skip-corrupted`, a corrupted partition refuses the whole run

Scrub's default (flag absent) SHALL refuse the entire run — writing nothing under `--out` for that
generation — when any partition fails to decode. **This is a deliberate divergence from Cassandra's
own default for a regular (non-counter) table, which skips the corrupted partition and continues**;
see `design.md` D4 for the full parity table and the owner decision this requirement is pending on.

#### Scenario: R4.1 refusal without the flag
- **Given** `test_comp_corrupt/data_db_bit_flip`, `skip_corrupted: false`
- **When** scrub runs
- **Then** the run REFUSES for that generation: no output written under `--out` for it, the report
  names the corrupted partition, its offset, and that `--skip-corrupted` is the remedy
  (`cqlite-core/tests/issue_4198_scrub_skip_corrupted_parity.rs`).

### Requirement: R5 — `--reinsert-overflowed-ttl` is version-gated exactly as Cassandra's own `hasUIntDeletionTime()` gates it

`--reinsert-overflowed-ttl` SHALL rewrite a row whose local-deletion-time has overflowed
(CASSANDRA-14092: `localDeletionTime == INVALID_DELETION_TIME`) to the 2038 cap with its timestamp
incremented by one, and SHALL be a structural no-op for any source format where the overflow cannot
occur.

#### Scenario: R5.1 BIG `na`/`nb`: the overflow is representable, the rewrite is real
- **Given** a fixture from the `issue_1011_ttl_local_deletion_parity` family on a BIG `na` or `nb`
  generation with a row whose local-deletion-time has overflowed
- **When** scrub runs with `reinsert_overflowed_ttl: true`
- **Then** the row is rewritten with `MAX_DELETION_TIME_2038_LEGACY_CAP` and `timestamp + 1`,
  matching `cassandra-5.0.8`'s `FixNegativeLocalDeletionTimeIterator`; the report's `ttl_rewrites`
  count is exactly 1 (`cqlite-core/tests/issue_4198_scrub_reinsert_overflowed_ttl.rs`).

#### Scenario: R5.2 BTI `da`: asserted no-op, never silently skipped
- **Given** any committed `test_da` table, `reinsert_overflowed_ttl: true`
- **When** scrub runs
- **Then** `ttl_rewrites` is exactly `0` for every generation — an AFFIRMATIVE zero (the option was
  honored and found nothing to do, per `BtiFormat.hasUIntDeletionTime() == true` unconditionally),
  never an implicit "not applicable" the report is silent about
  (`cqlite-core/tests/issue_4198_scrub_reinsert_overflowed_ttl.rs`).

### Requirement: R6 — Counter tables refuse unconditionally; memory is bounded

A table whose schema declares a `counter` column SHALL be refused before any scrub work begins,
citing the write engine's existing restriction on writing counter mutations
(`storage/write_engine/mod.rs`). Scrub SHALL hold at most one partition resident at a time on both
the read and write side, matching `compact_sstables`'s existing streaming bound.

#### Scenario: R6.1 counter table refused
- **Given** a schema fixture declaring a `counter` column, and a matching (synthetic, since CQLite
  cannot write counters to begin with) table dir
- **When** scrub runs
- **Then** it refuses immediately, citing the counter restriction, before opening any generation for
  writing (`cqlite-core/tests/issue_4198_scrub_counter_refusal.rs`).

#### Scenario: R6.2 bounded memory
- **Given** `test_wide_rows`
- **When** scrub runs under the existing `memory-budget` dhat lane
- **Then** peak memory stays within the same bound `compact_sstables` already proves (issue #827) —
  scrub adds no new unbounded buffering; the out-of-order sidecar accumulator is bounded by the
  SAME cap salvage's `MAX_RESIDENT_LOSSES`-style ceiling uses for its own affirmatively-truncated
  list (named, not silently unbounded).
