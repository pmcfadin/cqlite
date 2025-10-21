# Issue 165: Support Multi-Row Partitions in V5CompressedLegacy Reader

## TL;DR
- **Problem:** Our V5CompressedLegacy parser still assumes *one row per partition*. Real SSTables (e.g. `test_basic/simple_table`) store hundreds/thousands of rows under a single partition header.
- **Symptom:** After stitching the 41 decompressed chunks (~2.5 MB), we parse the first row correctly and then bail out when the next row appears because we mistake it for a new partition header (see `v5_compressed_legacy.rs:640-654`).
- **Goal:** Teach the parser to iterate all rows inside a partition before advancing the partition pointer, while keeping the new chunk-stitching logic.

---

## Current Behaviour
1. `ChunkedDataReader` now concatenates all NB-format chunks for the target range. The parser receives the full decompressed partition payload.
2. `parse_block`:
   - Reads a partition header (`flags`, `key_len`, `partition key`, `partition-level fields`).
   - Calls `parse_row_data_with_offset`.
3. `parse_row_data_with_offset`:
   - Parses a row header (`row_flags`, `row_size`, etc.).
   - Decodes cells using schema order (now filtered to regular columns).
   - Uses `row_size` to compute `next_partition_offset = row_start + row_size`.
4. The outer loop assumes the next byte belongs to the next partition. For `simple_table`, `row_size` ≈ 596 bytes; the next offset (row 2) is still within the same partition. Validation at line 648 trips (`row_start + row_size > data.len_of_block`), the parser returns `Err`, the outer loop breaks, and we only emit the first row.

### Repro Data
- Fixture: `cqlite-core/validation_artifacts/sstabledump/test_basic.simple_table`
- Test: `test_v5_compressed_legacy_extracts_cells` (currently only checking first row)
- Block layout: 1 partition, 1000 rows (row sizes vary)

---

## Root Cause
Cassandra’s legacy format groups all rows of a partition sequentially:

```
Partition Header
  Row Header #0 (row_size=…)
    Cell payload #0..N
  Row Header #1
    Cell payload #0..N
  …
Partition End Marker (empty row / deletion / etc.)
Next Partition Header
```

Our parser stops after Row #0 because we:
1. Treat `row_size` as “size of entire partition body”.
2. Never look for additional row headers once we enter `parse_row_data_with_offset`.
3. Bail on `row_size` > block length even though multi-chunk payloads are expected.

---

## Proposed Plan

### Phase 1 — Row Loop Refactor (P0)
1. **Partition Parsing**
   - Keep existing header parsing; record `partition_start` and `partition_remaining_bytes`.
2. **Row Iteration**
   - Replace the single call to `parse_row_data_with_offset` with a loop:
     ```rust
     let mut row_offset = offset_after_partition_header;
     while row_offset < partition_end {
         let (cells, row_header, next_offset) = self.parse_row(data, row_offset, schema, reader)?;
         results.push(Row { … });
         row_offset = next_offset;
         if row_offset == partition_end { break; }
     }
     ```
   - `parse_row` should return `(cells, header, row_end_offset)` and **no longer** compute partition offset.
3. **Partition Boundary Detection**
   - Define `partition_end` via:
     - Next partition header (lookahead), **or**
     - End-of-block (for last partition), **or**
     - Partition-level terminator (to be confirmed via cassandra source).
   - Practical approach: keep existing validation; if `row_offset >= data.len()`, stop. Otherwise, *peek* at the next byte:
     - If it looks like a row flag (bitset where upper bits < 0xE0), continue parsing row.
     - If it looks like a partition header (`flags <= 0x20` and `key_len` reasonable), break and return to outer loop.
4. **Error Handling**
   - Gracefully exit loop when row parsing fails due to EOF (treat as end-of-partition, log warning).
   - Reserve `Err` for genuine corruption.

### Phase 2 — Column Cache (P1)
1. Store filtered regular-column slice once per parser call:
   ```rust
   struct V5CompressedLegacyParser<'schema> {
       …,
       regular_columns: Cow<'schema, [Column]>
   }
   ```
2. Pass cached slice into `parse_row` to avoid HashSet creation each row.

### Optional Enhancements
- Detect partition-level tombstone rows (flags = deletion) and skip/emit accordingly.
- Surface per-row metadata (timestamp/ttl/deletion) in public API if needed.

---

## Acceptance Criteria
1. `test_v5_compressed_legacy_extracts_cells` (existing fixture) iterates **all 1000 rows** and asserts multiple sample rows for correctness.
2. Add synthetic test covering multi-row partition with varying row sizes (include row crossing chunk boundary).
3. No regressions in existing 759 tests; `cargo test` + `cargo clippy -- -D warnings` remain green.
4. Parser handles partitions where:
   - There is exactly one row.
   - There are multiple rows.
   - A row spans multiple decompressed chunks.
5. Performance: no per-row allocation of HashSet; baseline scan remains within acceptable time (document benchmarks if available).

---

## Open Questions / Tasks for Implementers
1. **Row Terminator Format:** Confirm via Cassandra Java source (`SSTableReader#getScanner`) how row boundaries are encoded in V5CompressedLegacy. Look at `LegacyLayout.decodeRowBody`.
2. **Partition Footer/Tombstone:** Identify if partition-level deletions insert special rows that should be interpreted differently.
3. **Chunk Stitching Ownership:** Current concatenation happens higher up. Decide whether to pass a slice per partition (preferred) or per block.
4. **Memory Footprint:** 2.5 MB per stitched partition is acceptable, but watch for super-wide partitions in production datasets.
5. **Instrumentation:** Add debug logging guarded by feature flag/env var to aid future debugging without spamming production logs.

---

## References
- Parser entry point: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
  - Partition loop: lines 210–320
  - Row parsing: lines 603–815
- Chunk reader: `cqlite-core/src/storage/sstable/reader/block_io.rs`
- Cassandra reference:
  - `org.apache.cassandra.io.sstable.format.big.BigRow` (legacy reader)
  - `org.apache.cassandra.io.sstable.metadata.SerializationHeader`
- Research: `CASSANDRA_50_ROW_FORMAT_RESEARCH.md`, `ISSUE_164_V5_COMPRESSED_LEGACY_CELL_FIX.md`

---

## Deliverables Checklist
- [ ] Implement multi-row partition iteration.
- [ ] Add regression tests (real + synthetic).
- [ ] Cache regular column list per schema.
- [ ] Document behaviour in `docs/research/issue_163_followup_items.md` (or new notes file).
- [ ] Update issue tracker/status dashboards once merged.

---

Please link this document in the new issue tracker entry (Issue #165) and attach any follow-up experiments. Once scoped, the build team estimates **~4–7 hours** to deliver the multi-row parsing support with tests. Adjust schedule accordingly.
