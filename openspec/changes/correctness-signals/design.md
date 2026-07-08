# Design — correctness / silent-miss signals

## Context

Five new counters (one opt-in) plus one attribute key, all routed through the existing
`cqlite_core::observability` foundation:

- Names live in `catalog.rs`, mirrored in `ALL_METRICS`.
- Emission goes through `observability::add_counter(name, value, &attrs)`, which is a
  **no-op with zero OTel linkage** when the `observability` feature is off (proven by the
  module's `cfg(not(feature = "observability"))` arm + `helpers_are_callable_in_any_build`
  test). Call sites are identical in every build.
- When the feature is on, `otel.rs` maps each name to a pre-registered `Counter<u64>` via a
  `match name` dispatch; an unregistered name still works via the `_ =>` ad-hoc-create arm,
  but convention (and this change) registers each new counter as a struct field so the
  instrument carries its unit.
- Tests assert against emitted metrics with the `observability-testing`
  `InMemoryMetricExporter` harness (`observability/testing.rs`), driven by a **real read /
  merge through a public surface** (Flight `do_get`, `Database` scan, or a compaction run).

Cited surfaces (read during design): `catalog.rs` (naming + bounded-attr doctrine),
`mod.rs` (feature gating), `otel.rs:520` (name→instrument dispatch), `producer.rs:628`
(`drive_merge`), `merge/mod.rs:2358/2600` (`PurgeCounts` tally + single per-merge
`tombstones_purged` emission), `merge/reconcile.rs:520` (per-cell purge decision),
`partition_lookup.rs:278` (single `READ_BLOOM_CHECKS` emission per BTI lookup),
`query/access_path.rs` (`FallbackReason` closed set + `record()`).

## Recommended design

### Metric catalog additions

| Const | Name | Unit | Bounded attrs | Fires |
|-------|------|------|---------------|-------|
| `MERGE_ROWS_IN` | `cqlite.merge.rows_in` | `{row}` | none | once per merge, sum of input rows consumed |
| `MERGE_ROWS_OUT` | `cqlite.merge.rows_out` | `{row}` | none | once per merge, sum of rows emitted post-reconcile |
| `COMPACTION_TOMBSTONES_SUPPRESSED` | `cqlite.compaction.tombstones_suppressed` | `{tombstone}` | none | once per merge, live cells/rows shadowed by a tombstone |
| `COMPACTION_TOMBSTONES_EMITTED` | `cqlite.compaction.tombstones_emitted` | `{tombstone}` | none | once per merge, tombstone markers retained into output |
| `READ_SSTABLES_PRUNED` | `cqlite.read.sstables_pruned` | `{sstable}` | `SSTABLE_FORMAT` | per SSTable excluded from a read by a presence-oracle negative |
| `READ_BLOOM_FALSE_NEGATIVES` | `cqlite.read.bloom.false_negatives` | `1` | `SSTABLE_FORMAT` | opt-in only: a "definitely absent" verdict contradicted by an authoritative scan |
| `QUERY_DEGRADED_PATH` | `cqlite.query.degraded_path.total` | `1` | `FALLBACK_REASON` | per SELECT that takes a soundness fallback |

New attribute key `FALLBACK_REASON = "cqlite.query.fallback_reason"`, value space =
`FallbackReason::label()` (a documented closed set: `no_schema`,
`partition_key_not_fully_constrained`, `partition_key_encoding_failed`,
`metadata_scan_path`, `legacy_executor_path`, `tombstones_build_no_prune`) — bounded by the
enum, never a key/query string.

### Where each counter is wired (single low-frequency emission)

- **rows_in / rows_out + suppressed / emitted** ride the merge's existing per-merge tally
  (the `PurgeCounts` pattern at `merge/mod.rs:2359`, emitted once at `:2600`). Per-partition
  reconcile increments *stack integers*; the four counters emit **once per merge**, exactly
  like `tombstones_purged` today. Because the Flight read path drives the SAME `KWayMerger`
  (`producer.rs`), a Flight `do_get` over multiple generations moves these counters too —
  one instrumentation site covers both the compaction-write and Flight-read paths. Scoped to
  the reconcile boundary so producer-level token-prune + predicate filtering (expected drops)
  are excluded — these count *reconciliation* drops only.
- **sstables_pruned** at the reader candidate-selection / presence-oracle-negative sites
  (the `miss` decision that skips an SSTable's data, e.g. `partition_lookup.rs:284`). One
  increment per SSTable not opened because its bloom (BIG) / trie (BTI) said absent.
- **degraded_path** folded into `access_path::record(FallbackFullScan { reason })` (or a
  sibling emit), so every honest fallback increments the counter with `reason.label()`.
  Fires once per fallback query (rare).
- **false_negatives** behind a runtime sampling switch (`ObservabilityConfig` /
  `CQLITE_VERIFY_PRESENCE_ORACLE`, default off). When enabled, a presence-oracle **negative**
  triggers an authoritative confirmation scan of that one SSTable; a contradiction increments
  the counter (and logs). Default-off = zero cost.

### Overhead posture (these are read-path counters — mandatory)

- **Off by default at the feature level.** `all` builds without `observability` link no OTel
  and every call site is a compiled-out no-op. This is the dominant production posture.
- **Feature on:** per-row work is a single `+= 1` on a **stack-local** integer inside a loop
  the merge already runs; the OTel `Counter::add` (a relaxed atomic) fires **once per merge**
  / once per query fallback / once per pruned SSTable — never per row or per cell. This
  matches the module doctrine ("aggregate with metrics; never per-cell spans on hot paths").
- **`sstables_pruned`** piggybacks the existing per-SSTable presence check — no new scan.
- **`false_negatives` is opt-in + sampled.** It is the only counter that costs real work
  (a confirmation scan), so it is default-off and gated by an explicit verify/sampling knob;
  operators turn it on transiently to *prove the presence-oracle soundness invariant*
  (expected value: 0), then turn it off.

## Alternatives considered

1. **Instrument rows-in/out in the Flight producer's `drive_merge` (`emitted` counter)
   instead of the core merge.** *Rejected.* `drive_merge`'s `emitted` is counted *after*
   token-prune + predicate filter + LIMIT, so its delta vs input conflates expected query
   filtering with reconciliation drops — it cannot isolate the silent-miss signal. It also
   would not observe the compaction-write path at all. Instrumenting the core reconcile
   boundary is the single site that means the same thing for both callers.
2. **A single `cqlite.merge.rows_dropped` delta counter** (issue's alt suggestion) instead of
   the in/out pair. *Rejected (kept the pair).* A lone delta hides the denominator: a dashboard
   cannot tell 5 dropped of 10 (alarming) from 5 of 5,000,000 (normal). The pair yields both
   ratio and volume and costs one extra `add` per merge.
3. **Reuse `cqlite.read.bloom.checks{result=miss}` as the skipped-SSTable signal** (no new
   metric). *Partially rejected.* The per-check miss counts *checks*, which for a batched
   candidate list need not equal *SSTables skipped*; a dedicated `{sstable}`-unit counter is
   unambiguous and dashboard-honest. (The existing checks metric stays as-is.) — This is the
   one borderline-redundancy call; see the open question below.
4. **Full re-merge verify mode** (drive the merge twice, diff row sets). *Rejected as a
   Non-goal* — doubles read cost; the narrow presence-oracle re-check gives the highest-value
   soundness proof (bloom/trie false negative = corruption) for O(1) per sampled miss.
5. **Per-reason separate metric names for degraded paths.** *Rejected.* A single metric with a
   bounded `fallback_reason` attribute (the established `access_path`/`result` pattern in this
   catalog) lets one series carry every arm for a stacked dashboard, and the reason enum is
   already closed.

## Open question for the owner (product-level) — RESOLVED

- **Requirement 3 granularity (fork):** ship a *new* `cqlite.read.sstables_pruned{format}`
  counter (recommended — `{sstable}`-unit, unambiguous), **or** declare the existing
  `cqlite.read.bloom.checks{result=miss}` arm the official skipped-SSTable signal and add
  *only* the opt-in false-negative counter (Requirement 4)?

  **OWNER DECISION (2026-07-08, #2163):** ship the NEW dedicated
  `cqlite.read.sstables_pruned` counter carrying the bounded `cqlite.sstable.format`
  attribute. Do NOT reuse the `cqlite.read.bloom.checks{result=miss}` arm as the
  skipped-SSTable signal — the per-check miss counts *checks*, not *SSTables skipped*, so a
  dedicated `{sstable}`-unit counter is unambiguous and dashboard-honest. The existing
  `cqlite.read.bloom.checks` metric stays as-is.
