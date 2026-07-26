# Tasks: stream-subphase-timers (issue #2819 / M1)

> Blocked on the Seam-1 owner decisions in `design.md` (sub-phase count 4-vs-5, cardinality/scope,
> `stream_grpc_write` first-class-vs-residual, core-seam depth, dashboard scope). Do not start
> implementation until those are resolved.

## 1. Sub-phase value table + accumulator (surface: `cqlite-flight/src/obs.rs`)
- [ ] Add the bounded sub-phase values to the `cqlite.rpc.phase` closed set (`stream_cold_fault`,
      `stream_decompress`, `stream_merge`, `stream_encode`, `stream_grpc_write`), preserving the
      `phase_slot`/`phase_index` bounded-fallback invariant so an unknown value can never leak.
- [ ] Add `StreamSubPhaseTimings` (five `Duration` buckets) + a RAII `SubPhaseScope` guard that
      accumulates elapsed nanos into a bucket; emit one `cqlite.rpc.phase.duration` sample per bucket
      that recorded time at stream teardown (never a fabricated zero for an unentered sub-phase).
- [ ] Unit tests: value-set membership + bounded fallback; accumulator emits ≤5 samples; an
      unentered bucket emits nothing.

## 2. Cold-fault + decompress timing seam (surface: `cqlite-core` SSTable read/decompress path)
- [ ] Add the thread-local sub-phase accumulator seam (install/drain around the merge on the single
      `spawn_blocking` thread); the reader body-chunk fetch pushes `stream_cold_fault`, the
      chunk decompressor pushes `stream_decompress`. No-op when unset (non-flight callers pay nothing).
- [ ] Unit test: the seam is a no-op when unset; a wrapped read/decompress accumulates into the right
      bucket.

## 3. Stream-loop wiring (surface: `cqlite-flight/src/streaming.rs`, `producer.rs`, `producer_stream.rs`)
- [ ] Wrap the `stream_merge` scope around the reconcile/materialize step, `stream_encode` around
      `flush`/`arrow_convert`, and `stream_grpc_write` around `ChannelSink::emit`'s `reserve()`/send
      — the last provably disjoint from `stream_cold_fault`.
- [ ] Install/drain the thread-local accumulator inside the `spawn_blocking` merge closure alongside
      the existing `PhaseTimer` `stream` phase; emit sub-phase samples at the same teardown point.

## 4. Doctrine / operator surface (surface: `cqlite-core/src/observability/operator_docs_annotations.rs`)
- [ ] Extend the `cqlite.rpc.phase.duration` annotation to document the sub-phase values and the field
      interpretation (cold−warm delta on `stream_cold_fault` = cold-IO latency bucket;
      `stream_grpc_write` is client-paced).
- [ ] Note in `docs/architecture/throughput-program-2026-07.md` §5 #1 / §7 M1 that the instrument now
      exists (the cold-vs-warm profile reads it).

## 5. End-to-end wiring proof (surface: `cqlite-flight/tests/metrics_capture_test.rs`)
- [ ] Extend the existing drained-`do_get` metrics-capture test: assert ≥4 sub-phase samples over a
      real compressed fixture; assert every `cqlite.rpc.phase` value stays in the closed set; assert
      the sub-phase durations sum to within slack of the `stream` phase.
- [ ] Add the cold-fault-isolation assertion: a stalled-client run inflates `stream_grpc_write` but
      not `stream_cold_fault` (PINNED timing window; no wall-clock threshold in the correctness path
      — mark any deliberate perf assert `perf-gate-allow`).
- [ ] Add the bounded-sample-count assertion (sub-phase sample count independent of row/batch count).

## 6. Gate + review + sign-off
- [ ] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [ ] `rust-reviewer` + roborev on the lite-green diff (review-first).
- [ ] Full gate ONCE via `flow-closer`.
- [ ] **C (spec-auditor)** anchored to `openspec/changes/stream-subphase-timers/specs/**`: every
      requirement `satisfied` with a public-surface test as evidence.
- [ ] roborev clean (blockers fixed pre-merge; nits batched to a follow-up).
- [ ] `openspec validate stream-subphase-timers --strict` clean; `openspec archive` after merge.
