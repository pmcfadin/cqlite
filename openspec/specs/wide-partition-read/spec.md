# wide-partition-read Specification

## Purpose
TBD - created by archiving change promoted-index-read-seek. Update Purpose after archive.
## Requirements
### Requirement: BIG promoted-index forward clustering-range seek

A ranged read (clustering predicate) on a BIG (`nb`) wide partition SHALL consume the decoded promoted
IndexInfo blocks to seek to and decode only the block range covering the predicate, rather than
full-scanning the partition. The seek SHALL be exercised by a production query call chain (wiring
evidence), and the executor SHALL record `AccessPath::ClusteringSlice` only when the seek truly engaged.

#### Scenario: Ranged read on a BIG wide partition seeks via the promoted index

- **GIVEN** the `test_big.wide_partition` fixture (BIG `nb` format, pk=1 has 290 live rows across ~10
  promoted-index blocks)
- **WHEN** a `SELECT ... WHERE pk = 1 AND ck > 100 AND ck < 140` is executed
- **THEN** the read consumes the decoded promoted IndexInfo blocks to bound the scan to the covering
  block range (not a full-partition decode)
- **AND** the recorded access path is `ClusteringSlice` (seek engaged), not a full-scan fallback
- **AND** the returned rows are exactly the live `ck` values in `(100, 140)` for pk=1.

#### Scenario: Promoted-index decode is consumed by a production call chain

- **GIVEN** the decoded promoted-index surface (`DecodedPromotedIndex` / `DecodedIndexInfo`)
- **WHEN** a clustering-range read on a BIG wide partition runs through the storage seam
  (`scan_partition_clustering`)
- **THEN** a production (non-test) code path calls the decode-and-select surface and uses the block
  `offset`/bounds to seek
- **AND** an end-to-end test asserts the seek path is taken (the decoded blocks are no longer consumed
  only by the `block_count()` stats counter or the #993 parity test).

#### Scenario: Block selection spans a clustering tombstone at a block boundary

- **GIVEN** pk=1 in the fixture has a range tombstone deleting `ck` 30..39 that straddles a
  promoted-index block boundary
- **WHEN** a ranged read covering `ck` 25..45 is executed
- **THEN** the selected block range includes both boundary blocks
- **AND** every live row adjacent to the deleted range (e.g. ck 29 and ck 40) is returned, with the
  deleted rows (30..39) absent.

### Requirement: BIG reverse partition iteration via promoted index

`ORDER BY <clustering> DESC` on a BIG (`nb`) wide partition SHALL be served by a reverse partition
iterator that walks the promoted IndexInfo blocks back-to-front and emits rows in descending clustering
order, rather than reading the whole partition forward and sorting the materialized result in memory.
Per-iteration memory SHALL remain bounded to a single block.

#### Scenario: Forward and reverse scans return the identical clustering set

- **GIVEN** pk=1 in `test_big.wide_partition` (290 live rows, deleted ck 30..39 straddling a block
  boundary)
- **WHEN** the partition is scanned forward (`ORDER BY ck ASC`) and reverse (`ORDER BY ck DESC`)
- **THEN** both return exactly the same 290-row clustering set
- **AND** no row is lost adjacent to the deleted block
- **AND** the reverse result is the exact reverse ordering of the forward result.

#### Scenario: Reverse iteration drives back-to-front block decoding

- **GIVEN** a BIG wide partition with multiple promoted-index blocks
- **WHEN** an `ORDER BY ck DESC` query is executed against it
- **THEN** the reverse partition iterator decodes the promoted-index blocks from last to first (not a
  post-fetch in-memory `sort_by` over a forward full-partition read)
- **AND** peak per-iteration memory is bounded to a single decoded block, not the whole partition.

#### Scenario: In-memory DESC sort remains the fallback for uncovered cases

- **GIVEN** a query with `ORDER BY ck DESC` against a non-BIG-wide partition (e.g. a small partition or
  a BTI table) or a multi-partition result
- **WHEN** the query is executed
- **THEN** the existing in-memory sort path serves the ordering unchanged (no regression).

### Requirement: Parity manifest reflects real reverse coverage

The `cass.sstable_scan.wide_partition.forward_reverse_bounds` parity scenario SHALL be promoted from
`partial` to mirrored once reverse iteration is real, and its assertion SHALL pin forward==reverse
equality.

#### Scenario: forward_reverse_bounds scenario is mirrored and pinned

- **GIVEN** the BIG reverse partition iterator is implemented and wired
- **WHEN** the parity suite runs the `forward_reverse_bounds` scenario
- **THEN** the manifest status for `cass.sstable_scan.wide_partition.forward_reverse_bounds` is no longer
  `partial`
- **AND** the scenario asserts that forward and reverse scans of pk=1 return the identical 290-row set
  with no rows lost adjacent to the deleted block.

