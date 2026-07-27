# Proposal: Split the Flight `stream` RPC phase into data-plane sub-phase timers (issue #2819 / M1)

**Milestone:** 0.17 · **Priority:** P2 · **Routing:** design-driven (new observability surface;
extends the existing per-RPC phase-timing instrument) · **Issue:** #2819 · **Epic:** #2817 (0.17
throughput program, manifest item M1) · **Extends:** observability epic AI #1686 (the #1701/#1705/#1707
why-slow phase timings).

## Why

The Flight `do_get` RPC is already broken into a closed, ordered five-phase timing set
(`cqlite-flight/src/obs.rs:215-232`, `RPC_PHASES`): `validate → admission → resolve → merge_setup →
stream`, each emitting a `cqlite.rpc.phase.duration` histogram sample tagged with the bounded
`cqlite.rpc.phase` attribute. This localizes latency to a phase without a profiler — the field-triage
tool used since #2398/#2399.

But the **entire data plane folds into the single `stream` phase**
(`docs/research/phase2-verify-field-gap.md` §2; `docs/architecture/throughput-program-2026-07.md` §5
item #2). The scan loop runs wholly inside `PHASE_STREAM` (`streaming.rs:345`, transition fired by the
`on_merger_built` hook), so cold body-chunk faults, LZ4 decompress, k-way merge + reconcile + row
materialize, Arrow encode, **and** gRPC channel write/park are all one histogram. The consequence
(field-gap §2): the field's standing instrumentation is **structurally blind** — cold-IO latency,
decompress-CPU, reconcile, and channel park/wake cannot be separated by any dashboard query. The only
cold signal the field can currently read is an elevated `resolve` (open/index faults), *not* the
in-`stream` body-scan faults where the disputed cost actually lives.

This blindness blocks the program's **#1 measurement** (throughput-program §5 #1, §7 M1): the i4i
cold-vs-warm server-direct profile whose cold−warm delta is meant to size the disputed "cold-IO
latency" bucket (P1.3 ≈ 1% CPU / non-binding bandwidth vs P1.5's 30-45% "cold-IO + LZ4" bucket).
Neither research doc could settle the contradiction from field data because *the field data cannot
express it*. Today that split needs a profiler attached on every run; the goal is to read it off the
standing dashboard.

## What Changes

1. **Decompose the in-`stream` data plane into sub-phase timers** — cold-fault / decompress / merge /
   encode / gRPC-write (≥4 sub-phases) — recorded off the **existing** `cqlite.rpc.phase.duration`
   histogram using **new bounded values** of the **existing** `cqlite.rpc.phase` attribute
   (`stream_cold_fault`, `stream_decompress`, `stream_merge`, `stream_encode`, `stream_grpc_write`).
   No new metric name, no new exporter, no new attribute key.
2. **Isolate the cold-fault sub-phase from send park/wake.** The cold-fault timer wraps ONLY the
   synchronous SSTable body page-in read; the channel send/backpressure park is captured in a
   disjoint `stream_grpc_write` sub-phase (never overlapping cold-fault), so cold-IO latency is
   readable off the dashboard instead of only under a profiler.
3. **A bounded per-request sub-phase accumulator** (not per-row/per-chunk `PhaseTimer` transitions):
   the sub-phases accrue on the CONCURRENT pipeline threads (feed thread for page-in + decompress;
   merge consumer thread for merge + encode; egress for gRPC-write) into a per-request
   `Arc<StreamSubPhaseTimings>` of five `AtomicU64` nanos counters, propagated to the scan-thread
   spawn sites, and emit exactly one histogram sample per sub-phase at stream teardown — bounded
   sample count (≤5/RPC) and bounded label cardinality. The sub-phases overlap in wall-clock and are
   NOT expected to sum to `stream`; the cold-IO signal is the cold−warm delta on `stream_cold_fault`.
4. **Doctrine + operator surface:** a `catalog` annotation update
   (`operator_docs_annotations.rs`) documenting the sub-phase values and their field interpretation
   (cold−warm delta = cold-IO latency bucket), so the dashboard panel is expressible.

## Non-goals

- **No new metrics stack.** Sub-phase timers hang off the existing observability surface (epic AI
  #1686): the same `cqlite.rpc.phase.duration` histogram and the same `cqlite.rpc.phase` bounded
  attribute. No new OTel meter, exporter, metric name, or attribute key is introduced.
- **Not changing the five top-level RPC phases' meaning.** `validate → admission → resolve →
  merge_setup → stream` keep their exact semantics; the sub-phases are a decomposition *within*
  `stream`. Because the data plane is a concurrent pipeline, the sub-phases overlap in wall-clock and
  do NOT sum to `stream`; `stream` retains its meaning as the whole data-plane wall-clock total.
- **Not a profiler.** This is standing, always-on, bounded-cardinality instrumentation — not a
  perf/flamegraph capture on every run. (The i4i profile of §5 #1 remains a separate one-time run;
  this issue makes the field dashboard able to attribute the same cost thereafter.)
- **Not a throughput optimization.** No lever from throughput-program §4 is implemented here; this is
  the *measurement instrument* that gates several of them.
- **Not changing admission, resolve, or merge_setup instrumentation.** Only the in-`stream` data
  plane is decomposed.

## Impact

- **Code:** `cqlite-flight/src/obs.rs` (sub-phase value table + Arc-atomic accumulator), `streaming.rs`
  / `producer.rs` / `producer_stream.rs` (scope wiring for merge/encode/gRPC-write + Arc propagation
  to the scan-thread spawns), and a thread-local Arc-atomic timing seam in the `cqlite-core` SSTable
  read/decompress path for cold-fault + decompress (see `design.md` for the concurrent-pipeline
  threading mechanism).
- **Docs/doctrine:** the observability metrics catalog annotation
  (`cqlite-core/src/observability/operator_docs_annotations.rs`) and the throughput-program §5 #1 /
  §7 M1 note (the instrument the cold-vs-warm profile reads). No CLAUDE.md rule change.
- **Cardinality:** the `cqlite.rpc.phase` bounded value set grows by the five sub-phase values, on the
  `cqlite.rpc.phase.duration` histogram and the `do_get` method ONLY (owner decision); the
  `cqlite.rpc.phase.active` gauge does NOT carry the sub-phase values (stays the 5-value set × methods).
  Both remain closed, static sets — never a ticket/key/query value.
