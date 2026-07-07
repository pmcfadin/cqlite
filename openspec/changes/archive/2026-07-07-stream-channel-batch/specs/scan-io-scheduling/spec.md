## ADDED Requirements

### Requirement: The public streaming scan exposes a batched surface that forwards a batch of rows per async wake

The streaming scan SHALL expose an additive batched surface (`scan_stream_batched`)
whose channel item is a `Vec` batch of `(RowKey, ScanRow)` entries, alongside the
unchanged per-row `scan_stream`. For a single-generation table the batched surface
SHALL forward the internal windowed batches STRAIGHT THROUGH — one channel send per
batch, reusing the existing `BATCH_EMIT_ROWS` internal batch size — so that streaming a
result set of N rows performs O(N / BATCH_EMIT_ROWS) channel sends, not O(N). It SHALL
NOT flatten the internal batch to per-row items and re-batch it. No new configuration
knob SHALL be introduced for the batch size.

#### Scenario: batched surface sends far fewer items than the per-row surface for the same scan
- **WHEN** a full streaming scan runs over a real single-generation multi-row SSTable fixture (hundreds of rows) via both `scan_stream` (per-row) and `scan_stream_batched`
- **THEN** the per-row surface yields exactly one channel item per row (N items) AND the batched surface yields strictly fewer channel items than the per-row surface (each item a non-empty `Vec`), demonstrating O(rows/batch) sends rather than one send per row
- **AND** no batch exceeds `BATCH_EMIT_ROWS` entries

### Requirement: The per-row surface is a flattening adapter over the batched stream

The historical per-row `scan_stream` SHALL be delivered by a flattening arm over the
same internal batched stream the batched surface forwards, so existing per-row
consumers observe an unchanged contract (item type, order, backpressure). Adding the
batched surface SHALL NOT change any existing public signature.

#### Scenario: existing per-row streaming consumers are unchanged
- **WHEN** the query engine executes a streaming `SELECT *` full scan (which consumes the streaming surface) over a real fixture
- **THEN** it returns the same rows in the same order as the materializing `execute` path, across multiple `buffer_size` values including `buffer_size = 1` (the per-row backpressure path), with no change to the `scan_stream` signature

### Requirement: Batched streaming output is content- and order-identical to the per-row stream

Flattening the batched surface's output SHALL yield exactly the per-row surface's
output — the same `(RowKey, ScanRow)` entries in the same order — for the same scan
arguments.

#### Scenario: batched-then-flattened equals per-row over a real dataset table
- **WHEN** the same table is scanned via `scan_stream` (collected per-row) and via `scan_stream_batched` (collected, then each batch flattened in order)
- **THEN** the two row sequences are equal element-for-element (keys and values), and neither is empty (the fixture exercises real rows)

### Requirement: The batched streaming surface preserves bounded-channel backpressure

The batched surface SHALL use a bounded channel and every send SHALL observe
backpressure, so a stalled consumer stops the producer rather than buffering the whole
result. The batched channel SHALL be bounded in batches such that its resident-row
budget stays comparable to the per-row surface's `buffer_size`, not
`buffer_size × BATCH_EMIT_ROWS`.

#### Scenario: a stalled batched consumer blocks the producer instead of buffering the whole scan
- **WHEN** a batched streaming scan over a multi-batch fixture is opened and the consumer stops receiving after the first batch
- **THEN** the producer does not run to completion buffering all remaining batches — it blocks on the bounded channel — and resumes producing only as the consumer drains further batches, and dropping the receiver terminates the scan
