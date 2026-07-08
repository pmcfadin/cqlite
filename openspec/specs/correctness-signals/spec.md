# correctness-signals Specification

## Purpose
TBD - created by archiving change correctness-signals. Update Purpose after archive.
## Requirements
### Requirement: Merge row-count reconciliation counters

The k-way merge reconcile boundary SHALL emit two monotonic counters,
`cqlite.merge.rows_in` (unit `{row}`) and `cqlite.merge.rows_out` (unit `{row}`), scoped to
the reconciliation step so that row-count changes caused by LWW collapse and tombstone
suppression are observable, and expected producer-level filtering (token-range prune,
predicate filter, `LIMIT`) is EXCLUDED. Both SHALL be emitted **once per merge** (not per
row or per cell), aggregated from stack-local counters, mirroring the existing single
per-merge `cqlite.compaction.tombstones_purged` emission. Both names SHALL be registered in
the metric catalog (`catalog.rs` + `ALL_METRICS`) and dispatched to pre-registered
instruments in `otel.rs`. When the `observability` feature is disabled the instrumentation
SHALL compile to a no-op with no OTel linkage.

#### Scenario: A multi-generation merge emits rows_in and rows_out
- **WHEN** a compaction (or a Flight `do_get` that drives `KWayMerger`) reconciles two
  overlapping SSTables whose combined input is N rows and whose reconciled output is M rows
  (M < N because duplicate/LWW-collapsed rows exist), with the `observability-testing`
  in-memory metric exporter installed
- **THEN** `cqlite.merge.rows_in` increments by exactly N and `cqlite.merge.rows_out`
  increments by exactly M
- **AND** the `rows_in - rows_out` delta equals the number of rows removed by reconciliation

#### Scenario: Reconciliation-only scope excludes producer filtering
- **WHEN** a Flight read applies a `WHERE`/token filter that drops rows AFTER the merge
  reconcile step emits its partition
- **THEN** those producer-level filtered rows are counted in `cqlite.merge.rows_in` and
  `cqlite.merge.rows_out` alike (they survived reconciliation), so the in/out delta reflects
  ONLY reconciliation drops, not query filtering

#### Scenario: Counters are inert when observability is disabled
- **WHEN** the crate is built without the `observability` feature
- **THEN** the merge path still compiles and runs, the emission calls are no-ops, and
  `cargo tree` links no OpenTelemetry crates on their account

### Requirement: Tombstone suppression-vs-emission counters

The merge reconcile path SHALL emit two monotonic counters distinct from
`cqlite.compaction.tombstones_purged`: `cqlite.compaction.tombstones_suppressed` (unit
`{tombstone}`) counting live cells/rows shadowed (suppressed) by a tombstone during
reconciliation, and `cqlite.compaction.tombstones_emitted` (unit `{tombstone}`) counting
tombstone markers RETAINED into the merge output. Both SHALL be emitted once per merge from
stack-local tallies. `cqlite.compaction.tombstones_purged` semantics SHALL be unchanged.
The three counters together SHALL let a dashboard distinguish suppression (a tombstone did
its job), emission (a tombstone was carried forward), and genuine purge, so a resurrection
smell (suppression without a safe purge or a retained marker) is visible.

#### Scenario: A tombstone that shadows live data is counted as suppressed
- **WHEN** a merge reconciles a newer row-tombstone over an older live cell in the same
  clustering slot, with the in-memory metric exporter installed
- **THEN** `cqlite.compaction.tombstones_suppressed` increments for the shadowed live
  cell(s)
- **AND** the counter is separate from `cqlite.compaction.tombstones_purged`, which does not
  increment unless the tombstone is also gc/overlap-safe to purge

#### Scenario: A retained tombstone marker is counted as emitted
- **WHEN** a merge writes a tombstone marker into its output because it is NOT purgeable
  (no gc cutoff / overlap-unsafe)
- **THEN** `cqlite.compaction.tombstones_emitted` increments by the number of retained
  markers and `cqlite.compaction.tombstones_purged` does not increment for them

### Requirement: SSTable-pruned-by-presence-oracle counter

The read path SHALL emit `cqlite.read.sstables_pruned` (unit `{sstable}`, bounded attribute
`cqlite.sstable.format`) incremented once for each SSTable excluded from a read because its
presence oracle returned a definitive negative — the bloom filter for BIG
(`might_contain_partition == false`) or the Partitions.db trie for BTI (a trie miss). The
counter SHALL be registered in the catalog and dispatched to a pre-registered instrument.
It SHALL be distinct from `cqlite.read.bloom.checks`, which counts checks (hit/miss); this
counter counts SSTables actually skipped, in `{sstable}` units.

#### Scenario: A point read over a multi-SSTable table skips absent SSTables
- **WHEN** a partition point lookup runs over a table with multiple SSTables where the key is
  present in one and absent from the others, through the public read surface, with the
  in-memory metric exporter installed
- **THEN** `cqlite.read.sstables_pruned` increments once per SSTable whose presence oracle
  reported the key definitely absent
- **AND** the increment carries `cqlite.sstable.format` = `"big"` or `"bti"` matching each
  skipped SSTable's format

### Requirement: Opt-in presence-oracle false-negative verification counter

The read path SHALL provide an OPT-IN, default-OFF verification that a presence-oracle
"definitely absent" verdict is truthful, emitting `cqlite.read.bloom.false_negatives` (unit
`1`, bounded attribute `cqlite.sstable.format`) ONLY when an authoritative scan of that
SSTable finds the key the oracle said was absent. Enabling it SHALL require an explicit
runtime switch (an `ObservabilityConfig` field / `CQLITE_VERIFY_PRESENCE_ORACLE` env, off by
default); when off the verification scan SHALL NOT run and the path SHALL cost nothing beyond
the existing check. The confirmation SHALL be an authoritative scan of the SSTable's own data
— never a heuristic inference from byte patterns (no-heuristics mandate). Under a correct
bloom/BTI-trie this counter SHALL remain 0; a non-zero value is a corruption/soundness alarm.

#### Scenario: Verification off by default costs nothing and never emits
- **WHEN** reads run with the verification switch unset (its default)
- **THEN** no confirmation scan is performed on a presence-oracle miss and
  `cqlite.read.bloom.false_negatives` is never emitted

#### Scenario: Verification on confirms a true negative without incrementing
- **WHEN** the verification switch is enabled and a point read hits a presence-oracle miss
  for a key that is genuinely absent from that SSTable, with the in-memory exporter installed
- **THEN** an authoritative confirmation scan runs, finds the key absent, and
  `cqlite.read.bloom.false_negatives` stays at 0

#### Scenario: Verification on surfaces a contradicted negative
- **WHEN** the verification switch is enabled and (via a fault-injected / synthetic oracle
  that reports a false negative for a key that IS present) a point read hits a miss the
  authoritative scan contradicts
- **THEN** `cqlite.read.bloom.false_negatives` increments by 1 with the offending SSTable's
  `cqlite.sstable.format`, proving the counter fires only on a real contradiction

### Requirement: Degraded read-path counter with bounded reason

The SELECT executor SHALL emit `cqlite.query.degraded_path.total` (unit `1`) with a single
bounded attribute `cqlite.query.fallback_reason` whose value comes from
`FallbackReason::label()` (the documented closed set: `no_schema`,
`partition_key_not_fully_constrained`, `partition_key_encoding_failed`,
`metadata_scan_path`, `legacy_executor_path`, `tombstones_build_no_prune`), incremented once
each time a query takes a soundness fallback recorded as `AccessPath::FallbackFullScan`. The
attribute key SHALL be registered in `catalog::attr`; the value space SHALL be bounded by the
enum and SHALL NEVER carry a partition key, predicate value, or query string. The counter
SHALL fire at the same decision sites that record the honest fallback today, so a green
targeted query does NOT increment it.

#### Scenario: A schema-less query increments the degraded counter with its reason
- **WHEN** a SELECT that cannot use a targeted path (e.g. no schema available) runs through
  the public query surface and records `AccessPath::FallbackFullScan { reason: NoSchema }`,
  with the in-memory metric exporter installed
- **THEN** `cqlite.query.degraded_path.total` increments by 1 with
  `cqlite.query.fallback_reason` = `"no_schema"`

#### Scenario: A targeted (non-degraded) query does not increment the counter
- **WHEN** a SELECT resolves to a real partition lookup (no fallback recorded)
- **THEN** `cqlite.query.degraded_path.total` does not increment for that query

#### Scenario: The fallback-reason attribute stays bounded
- **WHEN** any degraded-path increment is emitted
- **THEN** the `cqlite.query.fallback_reason` value is exactly one of `FallbackReason`'s
  `label()` strings and carries no key/predicate/query text

### Requirement: Catalog integrity and bounded cardinality for the new signals

All added metric names SHALL be defined as constants in
`cqlite-core/src/observability/catalog.rs`, listed in `ALL_METRICS`, rooted under `cqlite.`,
and unique; the added attribute key SHALL be under `catalog::attr` and namespaced. Each new
counter SHALL declare a pre-registered instrument with its documented unit in `otel.rs` and a
matching `add_counter` dispatch arm. No new metric SHALL carry an unbounded attribute value.

#### Scenario: New metric names pass the catalog invariants
- **WHEN** the catalog unit tests (`metric_names_are_namespaced_and_unique`,
  `attribute_keys_are_namespaced`) run over the extended `ALL_METRICS` and attribute set
- **THEN** every added name starts with `cqlite.`, is unique, appears in `ALL_METRICS`, and
  the added attribute key is namespaced

#### Scenario: Every added counter resolves to a registered instrument
- **WHEN** each added counter name is emitted through `observability::add_counter` in an
  `observability`-enabled build
- **THEN** it dispatches to its pre-registered `Counter<u64>` (its declared unit), not the
  ad-hoc `_ =>` fallback arm

