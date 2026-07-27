# Tasks: stream-subphase-timers (issue #2819 / M1)

> Seam-1 owner decisions RESOLVED in `design.md` (5 sub-phases; sub-phase values on `phase.duration`
> + `do_get` only, NOT `phase.active`; `stream_grpc_write` first-class + client-paced annotation;
> thread-local Arc-atomic core seam IN SCOPE; Grafana panel out of scope). Accounting-model amendment
> approved: sub-phases run on CONCURRENT pipeline threads, overlap in wall-clock, do NOT sum to
> `stream`; the cold-IO signal is the cold−warm delta on `stream_cold_fault`.

## 1. Sub-phase value table + accumulator (surface: `cqlite-flight/src/obs.rs`)
- [x] Add the bounded sub-phase values to the `cqlite.rpc.phase` closed set (`stream_cold_fault`,
      `stream_decompress`, `stream_merge`, `stream_encode`, `stream_grpc_write`) — used ONLY on
      `cqlite.rpc.phase.duration` for the `do_get` method (NOT `cqlite.rpc.phase.active`), preserving
      the `phase_slot`/`phase_index` bounded-fallback invariant so an unknown value can never leak.
- [x] Add `StreamSubPhaseTimings` (five `AtomicU64` nanos counters behind an `Arc`) + a RAII
      `SubPhaseScope` guard that `fetch_add`s elapsed nanos into a counter on whichever thread it runs;
      emit one `cqlite.rpc.phase.duration` sample per counter that recorded time at stream teardown
      (never a fabricated zero for an unentered sub-phase).
- [x] Unit tests: value-set membership + bounded fallback; accumulator emits ≤5 samples; an
      unentered counter emits nothing; concurrent adds from multiple threads accumulate correctly.

## 2. Cold-fault + decompress timing seam (surface: `cqlite-core` SSTable read/decompress path)
- [x] Add the thread-local `Option<Arc<StreamSubPhaseSink>>` seam in `cqlite-core::observability`
      (`install()`/`current()` RAII pair, no-op when unset); the reader body-chunk fetch
      (`read_compressed_chunk_sync`) pushes `stream_cold_fault`, the chunk decompressor
      (`decode_scan_chunk`) pushes `stream_decompress`.
- [x] Propagate the request's `Arc` sink to the three scan-thread spawn sites: capture `current()`
      before each spawn and re-`install()` inside the child — the per-SSTable producer thread
      (`merge/from_readers.rs` `open_from_reader`) and the windowed feed `spawn_blocking`
      (`scan_stream_windowed.rs`), so the feed thread's page-in/decompress reach the request's sink.
- [x] Unit test: the seam is a no-op when unset; a wrapped read/decompress accumulates into the right
      counter; the sink propagates across a spawned thread.

## 3. Stream-loop wiring (surface: `cqlite-flight/src/streaming.rs`, `producer.rs`, `producer_stream.rs`)
- [x] Wrap the `stream_merge` scope around the reconcile/materialize `step_row`, `stream_encode` around
      `flush`/`rows_to_record_batch`, and `stream_grpc_write` around `ChannelSink::emit`'s
      `reserve()`/send — the last measured on the egress thread, disjoint (distinct thread, no shared
      code interval) from `stream_cold_fault`.
- [x] Create the request's `Arc<StreamSubPhaseTimings>` in the `spawn_blocking` merge closure, install
      it on the merge thread, and propagate it into the merge (so the core seam's spawn sites receive
      it); read the atomics and emit the sub-phase samples at the same teardown point as the existing
      `PhaseTimer` `stream` close.

## 4. Doctrine / operator surface (surface: `cqlite-core/src/observability/operator_docs_annotations.rs`)
- [x] Extend the `cqlite.rpc.phase.duration` annotation to document the sub-phase values and the field
      interpretation (cold−warm delta on `stream_cold_fault` = cold-IO latency bucket; the sub-phases
      overlap and do NOT sum to `stream`; `stream_grpc_write` is client-paced).
- [x] Note in `docs/architecture/throughput-program-2026-07.md` §5 #1 / §7 M1 that the instrument now
      exists (the cold-vs-warm profile reads it).

## 5. End-to-end wiring proof (surface: `cqlite-flight/tests/metrics_capture_test.rs`)
- [x] Extend the existing drained-`do_get` metrics-capture test: assert ≥4 sub-phase samples over a
      real compressed fixture; assert every `cqlite.rpc.phase` value stays in the closed set; assert
      each recorded sub-phase duration is > 0 and ≤ the RPC wall time (NOT that they sum to `stream`).
- [x] Add the un-entered-sub-phase assertion: an uncompressed fixture records no `stream_decompress`
      sample while the other sub-phases still record theirs.
- [x] Add the cold-fault-isolation assertion: a stalled-client run inflates `stream_grpc_write` but
      not `stream_cold_fault` (PINNED timing window; no wall-clock threshold in the correctness path
      — mark any deliberate perf assert `perf-gate-allow`).
- [x] Add the bounded-sample-count assertion (sub-phase sample count independent of row/batch count).

## 6. Gate + review + sign-off
- [ ] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [ ] `rust-reviewer` + roborev on the lite-green diff (review-first).
- [ ] Full gate ONCE via `flow-closer`.
- [ ] **C (spec-auditor)** anchored to `openspec/changes/stream-subphase-timers/specs/**`: every
      requirement `satisfied` with a public-surface test as evidence.
- [ ] roborev clean (blockers fixed pre-merge; nits batched to a follow-up).
- [ ] `openspec validate stream-subphase-timers --strict` clean; `openspec archive` after merge.
