# In-progress read-path / query metrics (issue #2162)

## Milestone
Design-driven (epic #2103, easy-db-lab observability). Companion to #2163 (`correctness-signals`) —
that change owns silent-miss / correctness counters; **this change owns in-progress / incremental
visibility only.** The two scopes are disjoint (see Non-goals).

## Why (measured problem)
Every read-path / query-engine internal series is emitted **once, at query completion**:

- `cqlite.query.rows_scanned` — a single `add_counter` at
  `cqlite-core/src/query/select_executor/execute.rs:343`, after the whole scan finishes. The value
  accumulates in `context.scan_rows` (`select_executor/mod.rs:702`, `stream_agg.rs:170`) but is not
  emitted until the end.
- `cqlite.rpc.rows` / `cqlite.rpc.bytes` — accumulated per record batch in
  `MeteredDoGetStream::poll_next` (`cqlite-flight/src/streaming.rs:370`) but only attributed in
  `finalize`/`RpcMetrics::finish` (`cqlite-flight/src/obs.rs:145`), i.e. when the stream ends.
- `cqlite.read.{rows,partitions}`, `cqlite.read.bloom.checks`, `cqlite.read.partition_lookup`,
  per-query compaction merge counters — same emit-at-tail shape.

During the round-2 harness hang (#2157) a 24-minute `do_get` produced **zero** internal read-path
series — only the six rpc-level ones. From metrics alone an operator could see **that** a query was
stuck, never **where**. The #1476/#2176 streaming producer (now merged) addresses the *acute* hang
(rows stream before merge completion), but incremental emission is independently valuable and the
issue calls for it explicitly: a long-but-healthy scan and a truly-stalled one must be
distinguishable from telemetry, and a stall must localize to a phase.

## What changes
Three seams, all reusing the existing `cqlite_core::observability` chokepoint (no-op / zero-cost when
the `observability` feature is off), all emitting **per record batch or per bounded row threshold —
never per row**, all bounded-cardinality:

1. **Incremental streaming progress (Flight).** `MeteredDoGetStream` attributes `cqlite.rpc.rows` /
   `cqlite.rpc.bytes` as a counter delta **each batch** as it passes toward the client, instead of a
   single add at stream end. The per-batch accounting already exists; only the emission point moves.
   The monotonic-counter total is unchanged — a flat counter while `cqlite.rpc.in_flight > 0` now
   reads as "no forward progress"; a climbing one reads as "healthy long scan".
2. **Bounded per-`do_get` phase breakdown.** `do_get` records a phase-labeled duration
   (`cqlite.rpc.phase.duration`, new) and a span event at each transition across a **bounded** phase
   enum — `resolve` → `merge_setup` → `stream`. A `do_get` stuck opening SSTables shows its wall time
   accumulating in `merge_setup` **before the first batch**, turning "26-min do_get" into "25 min in
   merge_setup".
3. **Incremental core scan counters.** The core scan loop emits `cqlite.query.rows_scanned` (and
   `cqlite.read.rows` / `cqlite.read.partitions`) as deltas at a bounded row threshold during a long
   scan, so the counter climbs before the query returns. Exercised end-to-end through the Flight
   merge (public surface) and via a feature-independent progress-observation seam.

## Non-goals
- **No correctness signals.** Silent-miss / drop / no-heuristics-violation counters are #2163's
  scope (`correctness-signals`). This change never adds a correctness or data-quality metric.
- **No new public library / CLI / binding API and no new config knobs.** The only surface added is
  telemetry surface within the existing observability contract: new `catalog` metric-name constants
  and one new bounded attribute key. No user-facing method, flag, or tunable.
- **No per-row emission.** Emission is per record batch (Flight) or per bounded row threshold (core).
  A per-row `add_counter` on the hot path is explicitly rejected.
- **No tracing-backend / exporter changes.** No new exporter, protocol, sampler, or OTel-runtime
  wiring; the `observability` / `observability-testing` features and their gating are unchanged.
- **No new gauge is required.** The issue's optional "rows-examined-so-far gauge" is documented as a
  deferred alternative (a moving counter is the primary progress signal and composes correctly under
  concurrent `do_get`s, where a single last-value gauge does not).
