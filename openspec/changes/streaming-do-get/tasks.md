# Tasks — streaming-do-get (issue #1476)

## 1. TDD guards (fail on main first)

- [ ] 1.1 Incremental-emission test: `do_get` over a ≥3-batch fixture; assert a
      merge-step/work counter shows remaining steps pending when batch 1 is
      readable. Surface: `FlightService::do_get` via `FlightRecordBatchStream`.
      MUST FAIL on pre-change main; record the failure output.
- [ ] 1.2 Slow-consumer bound test: read one batch, pause; assert producer
      blocked at ≤ capacity + allowance batches (batch-count budget). MUST FAIL
      on main. Surface: `do_get`.
- [ ] 1.3 Disconnect-cancels test: drop the stream after batch 1; assert merge
      work counter < full-scan steps and task exit (extends the #1473 test).
      Surface: `do_get`.

## 2. Producer refactor (`cqlite-flight/src/producer.rs`)

- [ ] 2.1 Extract batch emission in `drive_merge` behind a sink; retain
      `produce`/`produce_cancellable` as the collect sink (byte-identical).
- [ ] 2.2 Add the streaming produce entry point sending
      `Result<RecordBatch, ProducerError>` into a bounded channel;
      send-failure ⇒ stop merge (cancellation), compose with `CancelFlag`.
- [ ] 2.3 Stream/collect parity test across no-constraint / limit / predicate /
      token tickets (Requirement: byte-identical). Surface: both produce paths.

## 3. Service wiring (`cqlite-flight/src/service.rs`)

- [ ] 3.1 `do_get_inner`: bounded `mpsc::channel(K)` (named const, documented
      memory bound), merge on `spawn_blocking`, response from the receiver
      stream through the existing encoder; schema-first behavior preserved for
      empty results. Surface: `FlightService::do_get` (gRPC, end-to-end).
- [ ] 3.2 Move `CancelFlag` guard to stream lifetime (cancel on drop + on send
      failure); remove the now-obsolete single-await guard comment block.
- [ ] 3.3 Move rows/bytes accounting to per-batch accumulation recorded at
      stream end, incl. cancelled-prefix attribution (Requirement: metrics).
      Surface: RPC metrics observed in service tests.
- [ ] 3.4 Aggregate path: wrap materialized output in `stream::iter`; add the
      unchanged-content test (Requirement: aggregate).

## 4. Verification & delivery

- [ ] 4.1 `--lite` each fix round (summary-file redirect); serialize own runs.
- [ ] 4.2 Review-first: `rust-reviewer` + roborev on the lite-green diff.
- [ ] 4.3 Existing flight tests green (`do_get_streams_merged_rows`,
      `do_get_missing_table_is_not_found`, producer limit/token/predicate).
- [ ] 4.4 flow-closer endgame: ONE full `scripts/agent-gate.sh` (summary-file
      redirect) → C intent audit (`spec-auditor` vs this change's `specs/**`)
      → final roborev → merge-on-green → finalize (telemetry stamp, archive,
      board Done, worktree/branch cleanup).
- [ ] 4.5 Post-merge: rebuild `ghcr.io/pmcfadin/cqlite-flight:dev`; note on
      #2157 that the streaming fix is live for the next lab run (pods must
      restart to re-pull).
