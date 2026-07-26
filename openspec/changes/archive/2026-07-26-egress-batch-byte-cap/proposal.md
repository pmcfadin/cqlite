# Byte-bounded Arrow egress batches — a dual row-cap / byte-cap batch boundary (issue #2825, T4/M11)

## Why

`cqlite-flight` finishes an Arrow record batch on **row count alone**. Verified on
`main`, both egress build sites are row-capped and nothing else:

- `cqlite-flight/src/producer.rs:951` — `if buffer.len() >= self.batch_size { sink.emit(self.flush_buffer(&mut buffer)?)?; }`,
  flushing `Vec<QueryRow>` (declared `producer.rs:888`) through
  `flush_buffer` (`producer.rs:1222`).
- `cqlite-flight/src/producer_stream.rs:206` — the identical trip, flushing through
  `flush` (`producer_stream.rs:87`).
- Both call `cqlite_core::export::rows_to_record_batch`
  (`cqlite-core/src/export/arrow_convert.rs:197`).
- `rg 'estimate|get_array_memory_size' cqlite-flight/src/producer*.rs` → **zero hits**.
  No byte accounting exists anywhere in the batch-accumulation path.

`batch_size` defaults to 8192 (`cqlite-flight/src/main.rs:35-36` →
`service.rs:327` → `producer.rs:422`). A batch's byte size is therefore
`8192 × row_width`, an **unbounded** function of schema shape: a table with a
64 KiB blob column produces a 512 MiB batch from the same code path that produces
a 192 KiB batch for `cassandra_easy_stress.keyvalue`. Peak resident egress payload
is `(DO_GET_CHANNEL_CAPACITY = 4 + ~2 in-flight) × batch_size` rows
(`cqlite-flight/src/streaming.rs:59-66`) — a bound stated in **rows**, which on
wide rows says nothing about memory.

Two concrete consequences already recorded in-tree:

- **B4 peak hazard.** The ratified B4 budget is **≤16Mi per-query working set at
  concurrency 1** (`docs/architecture/throughput-program-2026-07.md:41`). A
  row-only bound cannot hold it for any schema.
- **A fail-closed cliff, not graceful degradation.** `checked_value_bytes`
  (`arrow-convert.rs:128`) already rejects a batch whose cumulative Utf8/Binary
  bytes exceed `i32::MAX`, and its own error text prescribes the fix: *"reduce the
  batch row count (byte-bounded batching)"*. The remedy the code names does not exist.

Issue **#2821 (streaming `do_get` result-budget wiring gap)** is parked on this
change: its acceptance requires a per-stream egress ceiling whose guaranteed bound
is `ceiling + one maximum batch`, which is only expressible once "one maximum
batch" is a bounded quantity. This change is what makes #2821's B4 clause
satisfiable.

- **Milestone:** 0.17 scan-path throughput program, epic #2817; manifest item M11
  (`docs/architecture/throughput-program-2026-07.md:383-387`), lever T4.
- **Design-driven** — there is **no parity oracle** for a batch boundary. Where a
  batch ends is invisible to `sstabledump`, to the Cassandra format, and to query
  semantics. The cap's *currency* (payload bytes vs Arrow buffer capacity), its
  default, its estimator's conservatism contract, and the operator knob's surface
  are product/API decisions. Requires Seam-1 owner approval.
- **Creates capability** `flight-batch-byte-cap`.
- **Robustness/correctness lever, NOT a throughput lever.** The issue's own
  acceptance sets throughput impact at ~1.0–1.1×; the binding requirement is *no
  regression on narrow rows*.

## What Changes

- **A dual-trigger batch boundary.** A batch is finished on whichever of the
  row-cap (`batch_size`, default 8192) or a new **byte-cap** trips **first**,
  wired at **both** flush sites (`producer.rs` and `producer_stream.rs`). A cap
  present in only one path is a wiring-evidence failure, so both are specified and
  both are tested.
- **A conservative pre-materialization byte estimator.** The boundary decision is
  made **while rows accumulate**, before any `RecordBatch` exists — building a
  batch to discover it is too big defeats the purpose, so
  `RecordBatch::get_array_memory_size()` cannot be the trigger. A new
  `cqlite_core::export::estimate_arrow_row_bytes(&[ColumnInfo], &QueryRow)`
  returns a per-row width that **MUST NOT systematically under-estimate** the
  row's Arrow payload contribution; a running sum is compared against the cap at
  each `buffer.push`.
- **An honest, named estimate-vs-reality tolerance.** The cap's currency is
  **Arrow payload bytes** (the sum of buffer *lengths*). It is NOT
  `get_array_memory_size()`, which reports buffer **capacity**; the batch
  construction path (`StringArray::from` / `BinaryArray::from`,
  `arrow_convert.rs:666/686`) grows `MutableBuffer` by power-of-two doubling, so
  reported memory runs up to ~2× payload (measured in this change: 1.72–1.80× on
  realistic shapes). The spec names both quantities and the factor between them
  rather than asserting exactness.
- **`DEFAULT_MAX_BATCH_BYTES = 4 MiB`**, chosen so the row-cap still trips first on
  every narrow shape in the tree (design §b carries the arithmetic and the two
  contradictions it exposes).
- **A real operator knob**, mirroring the `--max-concurrent-scans` precedent
  exactly: `DEFAULT_MAX_BATCH_BYTES` const + `ENV_MAX_BATCH_BYTES`
  (`CQLITE_MAX_BATCH_BYTES`) + `--max-batch-bytes` clap arg
  (`cqlite-flight/src/main.rs`, the crate's only config surface) → service field →
  both producers, proven end-to-end through a real streamed `do_get`.
- **A liveness floor: always at least one row per batch.** A single row wider than
  the whole cap is still delivered as a one-row batch — never dropped, never an
  infinite loop.
- **A self-contained synthetic wide-row fixture** beside the existing
  `cqlite-flight/src/test_fixtures.rs` shapes. It must NOT depend on the fetched
  `test_wide_rows` dataset: a dataset-backed byte-cap test would pass vacuously on
  an empty dataset, which the testing doctrine forbids.
- **One documentation correction.** `docs/architecture/throughput-program-2026-07.md:385`
  (§7 manifest item M11) still cites the stale `57,344-row` egress figure. That
  number is a ~15% over-count — it multiplies in `IN_FLIGHT_ALLOWANCE = 3`, which
  is `#[cfg(test)]`-only (`cqlite-flight/src/streaming.rs:86-87`); real production
  residency is `(4 + ~2) × 8192 ≈ 49,152 rows`. The correction is already recorded
  at `docs/research/phase2-verify-parallelism.md:94-100`.

## Non-goals

- **No change to the read path or intra-partition materialization.** #1476/#2230/#2423
  bound what a merge materializes per partition; this bounds only the *egress batch
  boundary*. No overlap.
- **No streaming egress byte budget.** The per-stream in-flight ceiling is **#2821**,
  which consumes this change's published per-batch bound. This change bounds one
  batch, not the channel.
- **No change to `DO_GET_CHANNEL_CAPACITY`** (`streaming.rs:66`) and no new knob
  for it. That constant's doc comment records a deliberate prior decision against
  making it configurable; this change only makes its rows-denominated bound
  translatable into bytes.
- **No throughput optimisation.** Any measured speed-up is incidental. No
  correctness test may assert on wall-clock (#2642 / `roborev-lints`).
- **No change to `checked_value_bytes`'s `i32::MAX` fail-closed guard.** The
  byte-cap makes hitting it far less likely; it stays as the backstop.
- **No compression, no IPC-framing accounting, no client-side `maxInboundMessageSize`
  change.** Arrow-IPC wire size is a different (smaller) quantity than payload
  bytes; this change does not claim to bound it.
- **No edits to the dated research snapshots** (`phase1-6-parallelism.md`,
  `phase2-verify-row-engine.md`, `phase2-verify-transport.md`,
  `phase2-verify-parallelism.md`). They are analysis records; the doc footprint of
  this change is the single M11 line.

## Doctrine impact

- **No-heuristics (#28):** unaffected. The estimator derives width from the
  authoritative `ColumnInfo` CQL types plus the decoded `Value`s already in hand —
  it never infers a type from byte patterns and never changes a decode decision.
- **Memory budget:** strictly improves it. Turns an unbounded `8192 × row_width`
  egress batch into a configured ceiling, which is the whole point of the lever.
- **Public binding surfaces:** Python/Node/CLI unchanged. The new
  `cqlite_core::export::estimate_arrow_row_bytes` is additive and
  `#[cfg(feature = "arrow")]`-gated; the new `--max-batch-bytes` flag is additive
  with a default that preserves today's behaviour on every narrow shape.
- **Operator docs:** `--max-batch-bytes` joins the flight/ops knob documentation
  and the `main.rs` startup log line alongside `--max-concurrent-scans`.
