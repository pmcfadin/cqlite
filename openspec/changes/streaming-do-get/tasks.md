# Tasks — streaming-do-get (issue #1476)

## 1. TDD guards (fail on main first)

- [x] 1.1 Incremental-emission test: `streaming::tests::first_batch_available_before_merge_completes`
      — asserts the produced-batch counter is `< n` (and within the channel bound)
      once batch 1 is readable. FAILS on main (the streaming API is absent —
      captured compile failure: `no method resolve_paths/is_aggregating`,
      `unresolved import BatchSink`).
- [x] 1.2 Slow-consumer bound test:
      `streaming::tests::slow_consumer_bounds_produced_batches` — reads one batch,
      yields, asserts produced ≤ capacity + allowance. FAILS on main.
- [x] 1.3 Disconnect-cancels test: `streaming::tests::dropping_stream_cancels_merge`
      — drops the stream after batch 1, awaits the merge task, asserts produced
      `< n`. FAILS on main.

## 2. Producer refactor (`cqlite-flight/src/producer.rs`)

- [x] 2.1 Extracted batch emission in `drive_merge` behind `BatchSink`; the
      retained `produce`/`produce_cancellable` use `CollectSink` (byte-identical).
- [x] 2.2 Added `MergeProducer::produce_streaming` sending
      `Result<RecordBatch, ProducerError>` into a bounded channel via `ChannelSink`;
      send-failure ⇒ `ProducerError::Cancelled` (composes with `CancelFlag`).
- [x] 2.3 Stream/collect parity tests
      (`streaming::tests::stream_collect_parity_{no_constraints,limit_mid_batch,predicate,token_range}`)
      compare raw streamed batches to the collect path — byte-identical.

## 3. Service wiring (`cqlite-flight/src/service.rs`)

- [x] 3.1 `do_get_inner`: `spawn_streaming` builds a bounded
      `mpsc::channel(DO_GET_CHANNEL_CAPACITY)` (named const, documented bound),
      merge on `spawn_blocking`, response from the receiver stream through the
      existing encoder. `do_get_setup` resolves paths eagerly so a missing table
      stays `not_found` and empty results stay schema-only (existing tests green).
- [x] 3.2 `CancelFlag` guard moved to the stream lifetime (`MeteredDoGetStream`
      owns the `CancelGuard`: cancel on drop, disarm on normal end) + send-failure
      is the second stop signal; the obsolete single-await guard block is removed.
- [x] 3.3 rows/bytes accounting moved to per-batch accumulation in
      `MeteredDoGetStream`, recorded at stream end incl. cancelled-prefix
      (`streaming::tests::metrics_parity_on_full_consumption`,
      `metrics_attribute_emitted_prefix_on_cancel`).
- [x] 3.4 Aggregate path: `build_aggregate_response` wraps the materialized output
      in `stream::iter`; `streaming::tests::aggregate_path_matches_collect_content`.
- [x] 3.5 Review-first fix round (rust-reviewer APPROVE-with-Importants + roborev
      FAIL, 2 Medium): **B1** a panic in the blocking merge task dropped `tx`
      silently (clean EOF read as success) — fixed via `run_merge_catching_panics`
      (`catch_unwind` + new `ProducerError::Panicked`, mapped to
      `Status::internal`); tests `panicking_merge_forwards_a_terminal_error_not_silent_close`,
      `do_get_stream_surfaces_panic_as_internal_status_not_eof`. **B2** mid-stream
      errors skipped `crate::obs::record_status_error` — fixed in
      `MeteredDoGetStream`'s error arm; observed via `StreamProbe.errors_recorded`
      in the same panic test. **N1** in-flight allowance de-duplicated into
      `IN_FLIGHT_ALLOWANCE` (test-only const, doc + tests share one derivation).
- [x] 3.6 Review-first fix round 2 (roborev re-review, 2 new Mediums — both
      regressions vs pre-change behavior): **F1** `do_get_setup`'s eager
      path-resolution/prune `spawn_blocking` had no `CancelGuard` (pre-change the
      single merge `spawn_blocking` was covered end-to-end) — fixed by creating
      ONE `CancelFlag` in `do_get_inner` BEFORE setup, holding a `CancelGuard`
      across the setup `.await` (disarmed on success), and handing the SAME flag
      into `spawn_streaming`/`build_aggregate_response` for the merge stage; new
      `MergeProducer::resolve_paths_cancellable`/`prune_paths_cancellable` poll
      the flag before listing and once per SSTable during the token-span prune.
      Tests: `service::tests::do_get_setup_honors_cancellation_before_resolution`
      (+ `_resolves_normally_without_cancellation` baseline),
      `producer::tests::resolve_paths_cancellable_rejects_before_listing`,
      `producer::tests::prune_paths_cancellable_stops_before_any_summary_read_when_pre_cancelled`.
      **F2** mid-stream `record_status_error` ran outside the `flight.do_get`
      span once `do_get` itself had returned (later `poll_next`s happen outside
      the `.instrument(span)`-wrapped future) — fixed by capturing
      `Span::current()` into `MeteredDoGetStream` at construction (while still
      inside the instrumented future) and re-entering it around `poll_next` and
      `Drop`'s finalize; covered by the existing
      `metrics_attribute_emitted_prefix_on_cancel`/panic tests (same call sites,
      now span-wrapped) — `tracing::Span` has no cheap "is this span active"
      test hook, so this is exercised via the unchanged behavioral assertions
      rather than a new span-presence assertion.
- [x] 3.7 Review-first fix round 3 (roborev re-review, 1 new Medium): `do_get_setup`
      still ran `DirSource::resolve` (filesystem `is_dir`/`read_dir`, incl. the
      `<table>-<uuid>` layout scan) and producer/schema construction on the async
      request task BEFORE entering `spawn_blocking` — pre-change ALL filesystem
      access ran inside the blocking task; under slow/busy storage this stalls the
      gRPC reactor for unrelated RPCs. Fixed by moving the ENTIRE fallible setup
      sequence (producer/schema construction, `DirSource::resolve`,
      `resolve_paths_cancellable`) into ONE `spawn_blocking` closure returning
      `Result<DoGetSetup, Status>`; `do_get_inner`'s `CancelGuard` still covers the
      whole `.await` unchanged. Pure refactor — no behavior/signature change, so
      all existing tests (incl. the round-2 F1 tests) cover it unchanged;
      `cargo test -p cqlite-flight` stayed at 151 passed.

## 4. Verification & delivery

- [x] 4.1 `--lite` each fix round (summary-file redirect); serialized own runs.
- [x] 4.1a File-size ratchet: the new streaming machinery lives in the new
      `streaming.rs`. The irreducible seam additions grow `producer.rs`
      (2397→2497) and `service.rs` (857→896) — both already far over the
      800-line threshold BEFORE this change (needs the #1116/#1135 split, out of
      scope here). During the B1/B2/N1 fix round `streaming.rs` itself grew past
      800 (954 lines) with the new panic/error-observability tests; split its
      `#[cfg(test)] mod tests` out into a sibling `streaming_tests.rs` (via
      `#[path = "streaming_tests.rs"] mod tests;`) — `streaming.rs` is now 416
      lines (source), `streaming_tests.rs` 581 (test file, well under 1500),
      after fix round 2's F1/F2 additions. `producer.rs`/`service.rs` continue
      growing from their pre-existing over-threshold baseline (now 2607/1016) —
      out of scope (#1116/#1135). Lite re-run with `CQLITE_ALLOW_FILE_GROWTH=1`
      (for producer.rs/service.rs only) → PASS.
- [ ] 4.2 Review-first: `rust-reviewer` + roborev on the lite-green diff.
- [x] 4.3 Existing flight tests green (`do_get_streams_merged_rows`,
      `do_get_missing_table_is_not_found`, producer limit/token/predicate) — full
      `cargo test -p cqlite-flight` = 145 passed.
- [ ] 4.4 flow-closer endgame: ONE full `scripts/agent-gate.sh` (summary-file
      redirect) → C intent audit (`spec-auditor` vs this change's `specs/**`)
      → final roborev → merge-on-green → finalize (telemetry stamp, archive,
      board Done, worktree/branch cleanup).
- [ ] 4.5 Post-merge: rebuild `ghcr.io/pmcfadin/cqlite-flight:dev`; note on
      #2157 that the streaming fix is live for the next lab run (pods must
      restart to re-pull).
