# sstable-partition-emit

## ADDED Requirements

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
