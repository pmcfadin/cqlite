# sstable-partition-emit Specification

## Purpose
TBD - created by archiving change partition-driver-emit-loop. Update Purpose after archive.
## Requirements
### Requirement: Single sliding-window partition/row emit skeleton

The V5CompressedLegacy sliding-window partition/row emit skeleton SHALL exist in exactly ONE driver, and the streaming-scan timestamps path and the per-element compaction path SHALL be thin adapters over that driver whose per-consumer behavior is expressed as policy hooks. The skeleton is: partition header readiness → header parse → per-row loop (END_OF_PARTITION / range-tombstone-marker / row-decode / boundary-peek) → `pending` buffering → `flush_and_emitted`.

#### Scenario: Two sliding emit functions share one driver

- **WHEN** `parse_one_partition_with_timestamps` and
  `parse_one_partition_for_compaction` are invoked
- **THEN** both drive the same `drive_partition_sliding` skeleton, differing only
  through their `SlidingPartitionPolicy` hooks (partition-open, range-marker, row
  handling)
- **AND** the public signatures of both functions are unchanged, so every
  existing caller compiles and behaves identically.

#### Scenario: Skeleton preserves ParseStep and buffering semantics

- **WHEN** the driver parses a partition that terminates with an
  `END_OF_PARTITION` marker, a confirmed next-partition header, a truncated tail
  on the final chunk, or a truncated tail on a non-final chunk
- **THEN** it returns `ParseStep::Emitted(consumed)` (flushing buffered rows) for
  the first three cases and `ParseStep::NeedMore` (flushing nothing) for the
  fourth, byte-identical to the pre-refactor behavior.

### Requirement: Single issue-932 row-write-timestamp decision site

The issue-932 row-write-timestamp coexistence rule SHALL be computed by exactly one shared helper, consumed by both sliding-window adapters. The rule prefers a `HAS_DELETION` row's liveness timestamp (surviving cells written after the deletion) and falls back to `markedForDeleteAt` only for a PURE row tombstone.

#### Scenario: Row-timestamp rule is not duplicated

- **WHEN** the source is searched for the row-write-timestamp coexistence
  computation
- **THEN** the `timestamp.or_else(row-tombstone deletion).unwrap_or(0)` decision
  appears in exactly one helper (`row_write_timestamp`), and both sliding
  adapters call it rather than re-implementing it inline.

#### Scenario: Scan and compaction agree on a #932 coexistence row's timestamp

- **GIVEN** a row header carrying BOTH `HAS_DELETION` and a liveness timestamp
  (surviving cells written strictly after the row deletion)
- **WHEN** the streaming-scan (timestamps) path and the compaction path each
  resolve that row's write timestamp
- **THEN** both yield the liveness timestamp (not `markedForDeleteAt`), and a
  lockstep guard test asserts this equality so a future divergence fails at the
  single decision site.

### Requirement: Emit output is byte-identical across the refactor

The consolidation SHALL be a pure factoring with no change to observable emit
output: row ordering, static-row merge/emit, read-side TTL/tombstone shadowing,
range-tombstone pairing, and tombstone-coexistence display SHALL be unchanged.

#### Scenario: Parity harnesses stay green

- **WHEN** the 33-table sstabledump JSONL parity harness and the compaction
  byte-parity suite run against the refactored driver
- **THEN** both pass with output identical to the pre-refactor baseline.

