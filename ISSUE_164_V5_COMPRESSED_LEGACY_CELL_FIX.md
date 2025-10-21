# Issue 164: Fix `test_v5_compressed_legacy_extracts_cells` Null Cell Regression

## Context

During Issue #163 we validated schema extraction for V5CompressedLegacy SSTables and observed a pre-existing failure in `storage::sstable::reader::tests::tests::test_v5_compressed_legacy_extracts_cells`. The reader currently returns `Value::Null` instead of parsed rows for the fixture `test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db`.

This issue tracks the investigation and fix so the V5CompressedLegacy reader can hydrate cell payloads end-to-end.

## Goals

- Diagnose why the row reader yields `Value::Null` for non-empty payloads.
- Align the Rust implementation with Cassandra’s reference reader (see `tmp/SSTableReader.java`) for range selection and row decoding.
- Restore the failing unit/integration test and add regression coverage.

## Non-Goals

- Changes to schema extraction (handled in Issue #163).
- Clustering-key metadata extraction from `Statistics.db` (tracked separately).
- Alternative schema-name inference strategies.

## Proposed Approach

1. **Capture Repro Artifacts**
   - Save the failing `Data.db` segment and the corresponding `Statistics.db` slice as deterministic fixtures.
   - Add debug logging around `V5CompressedLegacyReader::read_rows` to confirm the data offsets, compression chunk boundaries, and decoded row counts.

2. **Trace Cursor Alignment**
   - Compare the on-disk sections requested by the Rust reader against `SSTableReader.getScanner()` in Cassandra.
   - Verify that the index entry offsets and row-level decompression headers are consumed correctly before attempting to deserialize cells.

3. **Fix Deserialization Path**
   - Correct any misaligned offset calculations or chunk decompression logic that result in `Value::Null`.
   - Ensure `Row::cells` are materialized for the simple table fixture.

4. **Regression Coverage**
   - Reactivate `test_v5_compressed_legacy_extracts_cells` with assertions on decoded cell values.
   - Add a targeted unit test that fails if `Value::Null` is returned for populated payloads.
   - (Optional) Introduce a golden-file check comparing decoded rows against Cassandra’s `sstabledump` output.

## Acceptance Criteria

- `test_v5_compressed_legacy_extracts_cells` passes without returning `Value::Null`.
- New regression test(s) fail if the reader stops decoding payload bytes.
- No regressions in existing schema-parsing or reader tests (`cargo test`, `cargo clippy -- -D warnings`).
- Follow-up documentation added to `docs/research/issue_163_followup_items.md` summarizing the fix.

## References

- `docs/research/issue_163_followup_items.md`
- `cqlite-core/src/parser/enhanced_statistics_parser.rs`
- `cqlite-core/src/storage/sstable/reader/v5_compressed_legacy.rs`
- Cassandra reference reader: `tmp/SSTableReader.java`
