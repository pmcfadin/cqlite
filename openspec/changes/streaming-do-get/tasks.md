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

## 4. Verification & delivery

- [x] 4.1 `--lite` each fix round (summary-file redirect); serialized own runs.
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
