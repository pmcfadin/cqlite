## Why

The July 2026 parser performance audit
(`docs/reports/parser-performance-audit-2026-07-01.md`, §Executive summary #3,
Epic K finding **K1**) found the V5CompressedLegacy partition/row emit skeleton
(partition-header parse → END_OF_PARTITION / range-tombstone-marker checks →
row-body decode → boundary peek → `ParseStep` advance / `flush_and_emitted!`)
duplicated across **five** emit functions:

- `block_emit_windowed.rs` — `parse_block_emit_windowed` (user-facing scan) and
  `parse_one_partition_with_timestamps` (streaming scan / compaction-read tuple).
- `block_emit.rs` — `parse_block_emit_with_metadata` (WRITETIME/TTL projection)
  and `parse_block_emit_delta` (delta-scan, `feature = "delta-scan"`).
- `compaction.rs` — `parse_one_partition_for_compaction` (per-element compaction
  rows), driven by `parse_block_for_compaction_emit(_with_offset)`.

The subtle **issue #932** row-write-timestamp coexistence decision — a
`HAS_DELETION` row may ALSO carry a liveness timestamp for surviving cells, so
the row timestamp prefers that liveness timestamp and only falls back to
`markedForDeleteAt` for a PURE row tombstone — is **hand-copied** into the two
sliding-window loops (`parse_one_partition_with_timestamps`,
`parse_one_partition_for_compaction`). As the audit puts it: "this is how parity
regressions are manufactured" — a future tombstone/type fix must land in N places
or the paths silently diverge.

**Routing: design-driven.** This is a structural consolidation of a hot-path
skeleton (an architecture change), not an oracle-driven parse-correctness bug, so
it is captured as an OpenSpec change per the spec-driven doctrine. The design and
priority are already owner-approved via the read-path performance audit and the
**#932 decision** (standing owner Seam-1 approval, 2026-07-06 drain directive);
this change encodes that decision rather than re-litigating it.

Milestone: **v0.14 performance wave** (Epic #1604, row/cell hot-loop mechanics).
This is a **pure factoring** — identical observable output. Parity is the proof:
the 33-table sstabledump JSONL harness and the compaction byte-parity suite must
stay green through the refactor.

## What Changes

- **Add** a single `PartitionDriver` seam in a new
  `v5_compressed_legacy/partition_driver.rs` that owns the shared sliding-window
  partition/row framing skeleton (header readiness → header parse → per-row loop
  with EOP / range-marker / row-decode / boundary-peek → `pending` buffering →
  `flush_and_emitted`) and drives it via a `SlidingPartitionPolicy` trait whose
  hooks capture the per-consumer differences (partition-open emit, range-marker
  handling, row handling).
- **Centralize** the issue-#932 row-write-timestamp coexistence rule into ONE
  `row_write_timestamp` helper, replacing the two hand-copied inline copies.
- **Convert** the two sliding-window emit functions
  (`parse_one_partition_with_timestamps`, `parse_one_partition_for_compaction`)
  into thin adapters over the driver — same public signatures, byte-identical
  observable output.
- **Add** a lockstep guard test proving the user-scan (timestamps) path and the
  compaction path compute identical row timestamps for an issue-#932 coexistence
  row, so a future divergence fails at the single decision site.

## Non-goals

- **K2 (#1641) non-allocating boundary peek**, **K3 (#1642) positional row
  emit**, **K4 (#1643) `Arc` key handles** — the rest of the K-emit chain. K1
  does NOT fold in that work; where the consolidation exposes a K2/K3/K4 seam it
  leaves a `TODO` referencing the owning issue.
- **No emit-semantics change of any kind** — no change to row ordering, tombstone
  coexistence display, static-row handling, TTL shadowing, or range-tombstone
  pairing. This is a factoring; the parity gates are the proof.
- **Not** unifying the windowed vs non-windowed *flush policies* — those differ
  by design (audit constraint). Only the skeleton unifies.
- **Not** re-litigating the pre-`na` version floor or the no-heuristics mandate.
