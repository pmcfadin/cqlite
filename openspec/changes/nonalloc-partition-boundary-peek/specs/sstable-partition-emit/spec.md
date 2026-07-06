# sstable-partition-emit

## ADDED Requirements

### Requirement: Partition-boundary peek is non-allocating

The post-row partition-boundary detection SHALL be a non-allocating peek that reads bytes and returns a small `BoundaryPeek` enum (`Header`, `NotHeader`, `NeedMoreBytes`) to decide whether the next bytes begin a new partition header. It SHALL NOT allocate a
throwaway partition key, SHALL NOT construct error strings as a control-flow
sentinel, and SHALL NOT increment the `PARTITION_HEADER_TRY_PARSES` gauge. The
real allocating partition-header parse (`parse_partition_header_full`) SHALL run
only at a confirmed partition start, once per partition.

#### Scenario: A full scan try-parses per partition, not per row

- **GIVEN** a genuinely wide-partition fixture (many rows per partition)
- **WHEN** a full scan runs through the emit loop and the boundary peek fires
  after every row
- **THEN** `PARTITION_HEADER_TRY_PARSES` recorded by the scan is strictly less
  than the returned row count and at least the distinct-partition count (a
  per-partition bound), whereas on the pre-K2 behavior the per-row peek makes the
  count at least the row count.

### Requirement: Boundary peek does not full-parse the header to detect a boundary

The boundary peek SHALL derive its accept/reject decision from the SAME structural
walk the full partition-header parser uses (a shared non-allocating helper), so
the peek can never accept a header the full parser would reject and never rejects
one it would accept. The peek SHALL NOT weaken any validation the full parse
performs at a true boundary.

#### Scenario: Peek accepts exactly what the full parser accepts

- **GIVEN** an arbitrary byte prefix and a parser on either the oa/da or the nb
  DeletionTime form
- **WHEN** the boundary peek and the full partition-header parse both run at the
  same offset
- **THEN** the peek returns `Header` if and only if the leading byte is not an
  END_OF_PARTITION or range-tombstone marker AND the full parser returns `Ok`,
  and `peek_is_partition_header` returns that same boolean (a proptest asserts
  this equivalence, so the cheap peek and the real parse cannot drift).

#### Scenario: Truncated boundary reports NeedMoreBytes without a false parse

- **GIVEN** a partition-header prefix split across a chunk boundary (the header —
  or, for an oa/da deleted partition, its full DeletionTime — is not entirely
  present)
- **WHEN** the boundary peek runs at that offset
- **THEN** it returns `NeedMoreBytes` (never `Header`), allocating nothing and
  recording no `PARTITION_HEADER_TRY_PARSES`, so the caller can request more
  bytes rather than mis-detecting or mis-parsing the boundary.

### Requirement: Emit output is byte-identical across the peek refactor

Replacing the allocating peek with the non-allocating peek SHALL be a pure
factoring with no change to observable emit output: partition boundary detection
outcomes, row ordering, and every parsed value SHALL be unchanged, and
`parse_partition_header_full` SHALL still return identical values and identical
error messages.

#### Scenario: Parity harnesses stay green

- **WHEN** the multi-partition sstabledump JSONL parity harness and the compaction
  byte-parity suite run with the non-allocating peek
- **THEN** both pass with output identical to the pre-refactor baseline (row order
  and partition boundaries byte-identical).
