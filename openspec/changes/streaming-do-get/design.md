# Design — streaming do_get (issue #1476)

## Chosen: bounded-channel producer→stream seam (Option A)

Convert the ONE eager seam — `Vec<RecordBatch>` collection between
`MergeProducer` and the gRPC response stream — into a bounded
`tokio::sync::mpsc` channel, leaving the (already lazy) merge machinery and the
Flight wire encoding untouched.

### Producer side (`cqlite-flight/src/producer.rs`)

- Add `MergeProducer::produce_streaming(&self, source, cancel: &CancelFlag,
  tx: mpsc::Sender<Result<RecordBatch, ProducerError>>)` (exact signature at
  implementer's discretion; the contract is the requirements, not the shape).
- Refactor `drive_merge` so batch emission is a sink call rather than
  `batches.push(...)` (producer.rs:538,550). The `Vec`-returning
  `produce`/`produce_cancellable` become the "collect sink" callers — they MUST
  keep byte-identical behavior (they are the parity oracle and serve the
  aggregate path and existing tests).
- `tx.blocking_send(...)` returning `Err` means the receiver (client) is gone:
  treat as cancellation — stop the merge, return. This composes with the #1473
  `CancelFlag` polled before each `merger.step()`; both paths stop the merge
  within a bounded number of steps.
- Errors mid-merge are sent INTO the channel (`Err(ProducerError)`) so the
  client sees a Status, matching `delta_scan/scan.rs:60-75`'s error forwarding.

### Service side (`cqlite-flight/src/service.rs` `do_get_inner`, ~371-431)

- Replace `spawn_blocking → Vec` (405-409) with:
  `let (tx, rx) = mpsc::channel(K)`; spawn the merge on `spawn_blocking`
  sending into `tx`; build the response from `ReceiverStream::new(rx)` through
  the existing `FlightDataEncoderBuilder.with_schema(...)` (424-428). The gRPC
  signature (`DoGetStream` = `BoxStream`) is unchanged.
- **K (channel capacity in batches):** a small named const (e.g. 4), documented
  as "peak resident payload ≈ (K + 2) · batch_size" (one batch being built, K
  queued, one in the encoder). Not a config knob in this change — the 2026-07
  platform audit's "~40/60 knobs decorative" lesson says don't add tunables
  without a consumer; #2162 can motivate one later.
- **Cancellation:** the existing `CancelGuard`-across-one-await (402-410) no
  longer covers the stream lifetime. The shared `CancelFlag` moves into a guard
  owned by the response stream (cancel on stream drop), and send-failure gives
  the second, independent stop signal. AA3 machinery (#1473) is reused, not
  replaced.
- **Metrics:** rows/bytes accumulate per batch as they pass to the encoder
  (e.g. an `inspect` layer or counter in the forwarding adapter) and
  `metrics.add_rows_bytes` records at stream end — cancelled streams attribute
  what was actually emitted. This is the one genuine restructure; it is also
  the seam #2162 (incremental observability) will build on.

### What stays

- `aggregate_paths` keeps materializing (small, one row per group) and is
  wrapped in `stream::iter` — explicit requirement, not an accident.
- Existing service tests (`do_get_streams_merged_rows`,
  `do_get_missing_table_is_not_found`) already consume a stream and keep
  passing; producer `Vec` tests (limit/token/predicate) stay valid against the
  retained collect path.

## Alternatives considered

- **B — async-ify the merge itself** (make `PartitionStepper` an async Stream
  end-to-end): touches the core write-engine merge used by compaction, far
  larger blast radius, and buys nothing — the merge is already lazy behind a
  bounded channel per input; the only eager seam is the collection Vec. Rejected.
- **C — unbounded channel + encoder backpressure**: loses the memory bound this
  change exists to establish (a slow consumer would buffer the whole table —
  the audit's exact complaint, relocated). Rejected.
- **D — chunked materialization (produce N batches, flush, repeat)**: still
  blocks time-to-first-row on chunk boundaries, adds a tuning knob, and is
  strictly dominated by A. Rejected.

## Risks / notes

- The blocking-pool thread is held for the stream's lifetime under a slow
  consumer (backpressure blocks in `blocking_send`). That is the intended
  behavior (bounded memory traded for a parked thread) and is bounded by the
  existing blocking-pool admission work (#2063 covers the eager path's pool
  admission; a parked-on-send thread is strictly cheaper than today's
  merge-to-completion thread).
- Byte-identity between streamed and collected output is the parity oracle and
  gets a dedicated test across limit/filter/token cases.
