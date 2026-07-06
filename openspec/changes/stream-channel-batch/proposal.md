## Why

The internal windowed streaming scan (issue #1143) deliberately batches rows: its
blocking parse half accumulates up to `BATCH_EMIT_ROWS` surviving `(RowKey, ScanRow)`
entries and hands them across the blocking→async seam as ONE `Vec` item, because
samply attributed ~31.5% of read wall time under `mixed.read_while_write` conc=8 to
the one-`blocking_send`-per-row condvar wake.

But the PUBLIC streaming channel forwarder then **re-flattens that `Vec` back to one
row per channel send** (`spawn_windowed_forwarder`'s per-row arm; and the manager's
k-way merge / query-engine consumers all `recv()` one row at a time). So one async
wake per row survives the whole pipeline whose own docs measured that per-row seam at
31–42%. The 2026-07 read-path performance audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Epic F, item **F2**) flagged this and DECIDED the fix; this change encodes that
owner-decided posture faithfully.

- **Milestone:** M7 / read-path performance (Epic F, #1518). **Design-driven** perf
  lane under standing Seam-1 approval. No new Cassandra oracle — a stream-content
  parity oracle (batched-flattened == per-row) plus a deterministic send-count oracle.
- Extends the `scan-io-scheduling` capability (the windowed streaming scan surface).

## What Changes

- **Additive batched streaming surface.** Add `scan_stream_batched(...)` alongside the
  existing per-row `scan_stream(...)` at each level of the streaming stack
  (`SSTableReader`, `SSTableManager`, `StorageEngine`). Its channel item is a `Vec`
  BATCH of `(RowKey, ScanRow)` entries rather than a single entry, so the consumer is
  woken **once per batch**, not once per row. Existing per-row signatures are
  UNCHANGED (guardrail: API-additive only).
- **Forward internal batches straight through.** The windowed driver already produces
  one internal `Vec`-batched stream. A new `WindowedOut::Batched` arm forwards those
  batches straight to the batched channel — it does NOT flatten then re-batch. The
  historical per-row surface becomes the `WindowedOut::PerRow` flatten arm over that
  same internal stream, so per-row is a thin flattening adapter and the two surfaces
  are byte-identical when the batched output is flattened.
- **Reuse `BATCH_EMIT_ROWS`.** No new tunable is introduced (the audit specifies
  reusing the existing internal batch constant). The batched channel is bounded in
  BATCHES (`ceil(buffer_size / BATCH_EMIT_ROWS)`) so its resident-row budget stays
  comparable to the per-row surface's `buffer_size`.
- **Wire the real read path.** The query engine's three full-scan consumers (table
  scan, streaming SELECT producer, O(1) streaming aggregate fold) consume the batched
  surface and iterate each batch's rows — so the wake reduction is realized on the
  actual pipeline, not left built-but-unwired.
- **Backpressure preserved.** The batched channel is bounded and every send observes
  it (`blocking_send`/`send().await`), so a stalled consumer still stops the parse
  loop and ultimately disk reads — no unbounded buffering.

Out of scope (deferred, per audit §F): a generation-aware fully-batched cross-generation
merge (the multi-generation / `tombstones` cases reuse the correct per-row merge and
re-chunk); wiring the Python/Node streaming iterators to the batched surface (V/W epics).

## Impact

- Affected specs: `scan-io-scheduling` (new requirements for the batched surface).
- Affected code: `storage/sstable/reader/scan_stream_windowed.rs` (WindowedOut arm),
  `storage/sstable/reader/data_access/sequential.rs` (reader batched surface),
  `storage/sstable/mod.rs` (manager batched surface + rechunk), `storage/mod.rs`
  (StorageEngine batched surface), and the three `query/` full-scan consumers.
