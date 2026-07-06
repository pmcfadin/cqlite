# Design — PartitionDriver: one partition/row emit loop (K1)

## Context

Five V5CompressedLegacy emit functions each re-implement the same partition/row
framing skeleton. Two of them — `parse_one_partition_with_timestamps` (streaming
scan / compaction-read `(TableId, RowKey, ScanRow, ts)` tuples) and
`parse_one_partition_for_compaction` (per-element `CompactionRow`) — share the
**identical** bounded sliding-window skeleton:

1. `partition_header_readiness` → `ParseStep::{Emitted(1) | Done | NeedMore}`.
2. `parse_partition_header_full` with the same defense-in-depth on `Err`.
3. A `pending: Vec<Row>` buffer flushed only on a CONFIRMED-complete partition
   (`flush_and_emitted!`), so a mid-partition `NeedMore` re-parse never
   double-emits (issue #827).
4. The per-row loop: `END_OF_PARTITION` → flush; buffer-exhausted → flush-or-
   `NeedMore`; range-tombstone marker → consumer-specific handling; row decode →
   consumer-specific handling; post-row boundary peek → flush.

The audit's K1 asks for ONE driver owning this skeleton with small policy hooks
for the per-consumer differences. This design consolidates exactly those two
sliding-window loops (the structurally-identical pair) and centralizes the
issue-#932 row-write-timestamp rule; the whole-block (`parse_block_emit_windowed`,
`parse_block_emit_with_metadata`) and `delta-scan` loops share the extracted
`row_write_timestamp` helper and are staged as the next incremental adapters
(issue #1640 recommends "driver + one adapter per commit, gate green each step").

## Goals

- One sliding-window skeleton (`drive_partition_sliding`); the two sliding emit
  functions become thin adapters.
- The issue-#932 row-write-timestamp coexistence rule lives in exactly ONE place
  (`row_write_timestamp`).
- Byte-identical observable output (parity is truth).

## Decisions

### D1 — `SlidingPartitionPolicy` trait with three hooks

The driver owns the skeleton; the policy owns per-consumer behavior:

- `on_partition_open(partition_key, partition_deletion, &mut pending)` — the
  timestamps policy opens a read-side `PartitionShadow` (when `read_shadowing`);
  the compaction policy pushes a synthetic `PartitionDelete` row for a
  partition-level tombstone (issue #1072) and opens no shadow.
- `on_range_marker(data, offset, schema, &mut pending) -> MarkerOutcome` — the
  timestamps policy feeds the range-tombstone FSM (or skips on a physical read);
  the compaction policy pairs bound markers into `RangeMarker` rows (issue #933).
  Both return `Advanced(next_offset)` or `Stop` (terminate the partition).
- `on_data_row(data, offset, schema, reader, resolution, &mut pending) ->
  Option<usize>` — the timestamps policy merges static cells, applies read-side
  shadowing, and builds the display `ScanRow`; the compaction policy captures
  per-element complex cells and builds a `CompactionRow`. `Some(next_offset)` on
  success, `None` when the row could not be parsed (driver treats as
  end-of-partition on the final chunk, else `NeedMore`).

Rationale: the range-marker and row handling are the two policies' genuinely
divergent parts (FSM-feed vs bound-pairing; `ScanRow` vs `CompactionRow`;
static-merge vs static-as-its-own-row), exactly the "policy hooks" the audit
prescribes. Everything else is identical skeleton owned by the driver.

### D2 — `row_write_timestamp` is the single #932 decision site

```
timestamp.or_else(|| pure-row-tombstone markedForDeleteAt).unwrap_or(0)
```

Formerly hand-copied into both sliding loops. Now one `pub(super)` helper in
`partition_driver.rs`. `build_compaction_row_data`'s separate row-*deletion*
capture (a distinct #932 facet — whether a deletion COEXISTS with surviving
cells) is unchanged and out of this seam.

### D3 — module placement respects Rust privacy

The trait + driver + helper live in `partition_driver.rs`. The two policy impls
live in their existing modules (`block_emit_windowed.rs`, `compaction.rs`) so
they retain access to module-private helpers (`build_compaction_row_data`,
`build_display_row`, `PartitionShadow`). Parent-module-private items are visible
to descendant modules, so `pub(super)` on the driver/helper suffices.

### D4 — file-size campsite

`block_emit_windowed.rs` (1173L) and `compaction.rs` (657L) are over/near the
source ratchet; moving the shared skeleton out reduces both. The new
`partition_driver.rs` stays well under the ~800L source target.

## Risks

- **Parity regression from a subtle skeleton slip.** Mitigation: the loop
  structure is transcribed line-for-line; the lockstep guard test pins the #932
  row-ts equivalence; the 33-table sstabledump harness and compaction byte-parity
  suite (full gate) are the invariant.
- **Borrow-checker friction** from threading `reader`/`resolution`/`shadow`
  through hooks. Mitigation: `resolution` is built once by the driver and passed
  by reference into `on_data_row`; shadow is owned per-partition by the policy.

## Migration

Pure internal refactor. No public API, on-disk format, or config change.
