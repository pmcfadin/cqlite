# Correctness / silent-miss observability signals

## Why

CQLite's error-side telemetry is mature: `observability/error_schema.rs` provides a
10-category bounded taxonomy with exhaustive `Error`-variant coverage, and
`cqlite.errors.total` is the canonical error-rate signal (issue #1038). #2193
additionally logs+counts Flight encoder-stage egress errors. But every one of those
signals fires only on an *explicit failure*. Nothing surfaces a **wrong-but-successful
read** — a query that returns `Ok` with the wrong rows because the read/merge path
silently dropped, resurrected, or skipped data.

Issue #2163 (epic #2103, `easy-db-lab` harness) names four concrete blind spots, each
over a real code surface:

1. **No row-count reconciliation.** The k-way merge (`KWayMerger` in
   `cqlite-core/src/storage/write_engine/merge/`, driven by the Flight producer's
   `drive_merge` in `cqlite-flight/src/producer.rs`) applies LWW + tombstone
   suppression and is unit-tested, but at runtime nothing emits how many rows entered
   the merge vs left it. A sudden change in the drop ratio (a schema/reconcile bug) is
   invisible.
2. **No tombstone suppression-vs-emission signal.** `cqlite.compaction.tombstones_purged`
   counts *genuine gc/overlap-safe purges* only. Nothing distinguishes a tombstone that
   **suppressed** older live data during reconcile from one **emitted** (retained) into
   the output — the two quantities whose divergence is the resurrection-risk smell.
3. **No presence-oracle miss / false-negative signal.** `cqlite.read.bloom.checks`
   counts checks (hit/miss), but there is no counter for *SSTables pruned* by a negative,
   and no way to catch a *false negative* (a "definitely absent" verdict that is wrong) —
   which, for a bloom/BTI-trie, must be impossible, so a non-zero count is a corruption
   alarm.
4. **No degraded-read-path signal.** The SELECT executor's honest soundness fallbacks
   (`AccessPath::FallbackFullScan { reason }` in `query/access_path.rs`, recorded at the
   decision sites in `query/select_executor/execute.rs` and `lookup.rs`) take
   `skip`/`unwrap_or_default` branches recorded only into a single-slot diagnostic
   `ArcSwap` — never a metric. An operator cannot alert on "queries are silently falling
   back to full scans."

All four are **additive, measurement-only** counters that reuse the existing
`observability` catalog + feature-gating machinery (`observability/catalog.rs`,
`observability/mod.rs`, `observability/otel.rs`). No production read/merge behavior
changes.

## Milestone & routing

- **Milestone**: maintenance / observability (epic #2103), targeting the v0.13 line.
- **Oracle-vs-design**: **design-driven.** There is no Cassandra byte-parity oracle for
  *what telemetry CQLite emits* — metric names, the bounded attribute space, the
  suppressed-vs-emitted decomposition, and the opt-in verification posture are all design
  latitude. (The underlying merge/purge *behavior* these counters observe is
  oracle-governed and unchanged here.) This is why #2163 is an OpenSpec change, not a
  bare issue + parity test.

## What changes (surfaces)

- **New catalog metric names** in `cqlite-core/src/observability/catalog.rs`
  (+ `ALL_METRICS`), one new bounded attribute key, and matching instrument registration
  + `add_counter` dispatch arms in `observability/otel.rs`.
- **Merge reconcile instrumentation** (`storage/write_engine/merge/`): aggregate
  rows-in / rows-out and tombstones-suppressed / tombstones-emitted into the existing
  per-merge tally struct, emit once per merge (never per row/cell).
- **Reader presence-oracle sites** (`storage/sstable/reader/partition_lookup.rs` +
  candidate-prune site): a pruned-SSTable counter, and an opt-in false-negative
  verification counter behind a runtime sampling config (default off).
- **SELECT executor** (`query/access_path.rs` + `select_executor/`): a degraded-path
  counter keyed by the existing bounded `FallbackReason`.

## Non-goals

- **A full re-read / re-merge disagreement ("verify") metric** that drives the whole
  merge a second time and diffs the two row sets. Deferred — it doubles read cost and is
  not "nearly free"; the four counters above surface silent-miss risk without a second
  pass. The one opt-in verification we *do* include (Requirement 4) is narrow and cheap:
  it re-checks only a presence-oracle **negative** against an authoritative scan, which
  is O(1) per sampled miss, not a whole-merge replay.
- **Changing any merge / purge / fallback behavior.** Every counter observes an existing
  decision; none moves one. `cqlite.compaction.tombstones_purged` semantics are untouched.
- **Re-speccing #2193** (Flight encoder-stage egress error logging/counting) — landing
  separately.
- **In-progress / latency metrics** — companion issue #2162, out of scope here.
- **New public CLI/Python/Node/Flight API surface.** These are internal telemetry
  counters observed through the existing OTLP exporter; no binding signature changes.

## Doctrine impact

- **No-heuristics mandate**: the false-negative verification (Requirement 4) contradicts a
  presence-oracle negative only against an **authoritative** scan of that SSTable — never
  by inferring from byte patterns. It is opt-in and default-off, so it never affects a
  production decode.
- **Memory budget (<128MB)**: counters are stack integers aggregated per-merge; zero heap.
- **Docs**: the `observability` catalog table on the website `agents-developing/` area and
  any metric reference in `docs/` gain the new names in the same change (user-facing
  telemetry surface).
