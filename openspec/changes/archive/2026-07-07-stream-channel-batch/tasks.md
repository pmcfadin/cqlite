## 1. Windowed driver: batched forwarder arm

- [x] 1.1 Add `WindowedOut { PerRow, Batched }` and make `run_scan_stream_windowed`
      take `out: WindowedOut`; extract the forwarder into `spawn_windowed_forwarder`
      with a flatten arm (per-row) and a straight-through arm (batched). Reuse the
      existing internal `Vec`-batch channel unchanged. Surface = the windowed driver.
- [x] 1.2 Make `BATCH_EMIT_ROWS` `pub(crate)` so the batched channel sizing / re-chunk
      can reuse it (no new tunable).

## 2. Reader batched surface

- [x] 2.1 Add `SSTableReader::scan_stream_batched` (+ `_admitted`) and
      `run_scan_stream_batched`: stitching path drives the windowed driver with
      `WindowedOut::Batched`; non-stitching path emits one batch per parsed block.
      Batched channel bounded in batches (`ceil(buffer_size / BATCH_EMIT_ROWS)`).

## 3. Manager batched surface

- [x] 3.1 Add `SSTableManager::scan_stream_batched` (default build): single reader →
      forward the reader's batched receiver straight through; zero/multi reader → reuse
      the per-row `scan_stream` and re-chunk via `rechunk_into_batches`.
- [x] 3.2 Add the `tombstones`-build variant: re-chunk the (materializing) per-row
      `scan_stream` — no straight-through (would bypass reconciliation).
- [x] 3.3 `rechunk_into_batches` helper: FIFO chunk a per-row receiver into
      `BATCH_EMIT_ROWS` batches over a bounded channel; flush the trailing partial
      batch; forward a mid-stream error as a terminal item; preserve backpressure.

## 4. StorageEngine batched surface + wiring the real read path

- [x] 4.1 Add `StorageEngine::scan_stream_batched` delegating to the manager.
- [x] 4.2 Wire the query engine's three full-scan consumers (table scan, streaming
      SELECT producer, O(1) streaming aggregate fold) to `scan_stream_batched` and
      iterate each batch's rows.

## 5. Oracle tests

- [x] 5.1 Send-count oracle: over a real fixture, per-row `scan_stream` yields N items
      and `scan_stream_batched` yields strictly fewer (non-empty) items, none exceeding
      `BATCH_EMIT_ROWS` (deterministic item counts, no wall-clock).
- [x] 5.2 Content parity oracle: batched-then-flattened == per-row (keys + values,
      order), non-empty.
- [x] 5.3 Backpressure oracle: a bounded batched consumer that stalls after the first
      batch blocks the producer (bounded channel), and dropping the receiver terminates
      the scan.
- [x] 5.4 Existing per-row streaming parity (issue #790) still green — the query engine
      consuming the batched surface returns the materializing `execute` rows unchanged.

## 6. Validate

- [x] 6.1 `openspec validate stream-channel-batch --strict` clean.
- [x] 6.2 `cargo +1.88.0 fmt`; clippy `-D warnings` under default + `cli-helpers` +
      `tombstones`; `scripts/agent-gate.sh --lite` PASS.
