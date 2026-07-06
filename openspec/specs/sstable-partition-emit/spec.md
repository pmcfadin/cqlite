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

### Requirement: Row cells are emitted positionally with no per-row HashMap or sort

The V5CompressedLegacy row decoder SHALL assemble each decoded row's cells directly
into an ordered `RowCells` (`Vec<(Arc<str>, Value)>`) in serialization-header
(schema) column order — deterministic by CONSTRUCTION. It SHALL NOT allocate a
per-row `HashMap` for cell assembly, and the shared display-row builder SHALL NOT
perform a per-row `sort_by` (nor increment the `ROW_SORT_INVOCATIONS` gauge) to
order a row's cells. The `ROW_SORT_INVOCATIONS` counter is retained as a regression
tripwire; any reintroduced per-row cell sort SHALL record it.

#### Scenario: A full scan performs zero per-row cell sorts

- **GIVEN** a present fixture returning live rows (the simple, static-column,
  collection, and wide-partition shapes)
- **WHEN** a full scan decodes and emits every row through the shared display-row
  builder
- **THEN** `ROW_SORT_INVOCATIONS` recorded by the scan is exactly `0`, whereas on
  the pre-K3 behavior the per-row `sort_by` made the count at least the returned
  live-row count.

#### Scenario: Emit order is deterministic by construction

- **GIVEN** the same fixture scanned twice
- **WHEN** each scan assembles rows positionally in serialization-header column
  order
- **THEN** the two scans surface the identical per-row column set with
  `ROW_SORT_INVOCATIONS == 0` on both runs — determinism comes from the fixed
  serialization-header order, not from a per-row sort.

### Requirement: Positional emit preserves byte-identical observable output

Positional row emit SHALL be a pure factoring with no change to observable output.
Building rows positionally instead of via a per-row `HashMap` + alphabetical sort
does not change results: the public query result (`QueryRow.values`) is a
name-keyed map, so the INTERNAL `RowCells` order is not user-visible; the surfaced
column values and the ordering the public API presents SHALL be unchanged.
Column-name case semantics SHALL NOT change (no lowercasing or re-keying).

#### Scenario: Parity harnesses stay green

- **WHEN** the 33-table sstabledump JSONL parity harness and the compaction
  byte-parity suite run with positional row emit
- **THEN** both pass with output identical to the pre-refactor baseline (column
  values and result ordering unchanged, compaction bytes unchanged).

### Requirement: Static-column merge is positional and clustering-row-wins

Merging accumulated static-column cells into a clustering row SHALL preserve the
clustering-row-wins precedence: a static cell is included only when the clustering
row does not already carry that column. The merge SHALL operate on the positional
`RowCells` representation (the ordered analogue of the former
`HashMap::entry(..).or_insert_with(..)`), and the merged order SHALL NOT be relied
upon as user-visible.

#### Scenario: Static columns merge into a clustering row without duplication

- **GIVEN** a partition with a static row followed by clustering rows
- **WHEN** each clustering row is emitted with the static cells merged in
- **THEN** every static column appears exactly once per emitted clustering row, a
  column present on the clustering row is not overwritten by the static value, and
  the SELECT result for the static columns is identical to the pre-K3 output
  (validated by the static-column parity fixtures).

