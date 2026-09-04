---
name: Cassandra SSTable Format Parsing
description: Guide parsing of Cassandra 5.0+ SSTable components (Data.db, Index.db, Statistics.db, Summary.db, TOC) with compression support (LZ4, Snappy, Deflate, Zstd, plus Noop). Use when working with SSTable files, binary format parsing, hex dumps, compression issues, offset calculations, BTI index, partition layout, or debugging parsing errors.
allowed-tools: Read, Grep, Glob
---

# Cassandra SSTable Format Parsing

This skill helps with parsing and understanding Cassandra 5.0+ SSTable file formats.

## When to Use This Skill

- Parsing Data.db, Index.db, Statistics.db files
- Debugging binary format mismatches
- Analyzing hex dumps of SSTable data
- Working with compression (LZ4, Snappy, Deflate, Zstd; Noop = stored raw)
- Investigating offset calculation errors
- Understanding BTI (Big Table Index) format
- Validating partition boundaries

## Key SSTable Components

### Data.db
Contains the actual row data with:
- Partition headers
- Row data (clustering + cells)
- Compression blocks
- Checksums

### Index.db
Contains partition index entries with:
- BTI (Big Table Index) format in Cassandra 5.0+
- Partition key → file offset mapping
- Promoted index entries

### Statistics.db
Contains serialization metadata:
- Encoding stats (min/max timestamps, TTLs)
- Column definitions
- Schema information
- Compression parameters

### Summary.db
Contains sampling of index entries for faster lookups

## Format References

**Primary Source of Truth**: `docs/sstables-definitive-guide/`

Key chapters:
- **Ch.5**: Data.db Format - Row layout, flags, V5 row/partition encoding
- **Ch.6**: Index.db and Summary.db - Partition lookups
- **Ch.9**: CompressionInfo.db - Compression metadata, chunking
- **Ch.17**: BTI Formats - Trie-based indexes
- **Appendix B**: Encoding Cheat Sheet - VInt, cell flags
- **Appendix F**: Known Limitations - What doesn't work yet

## Common Debugging Techniques

### Hex Dump Analysis
When debugging parsing errors:

1. **Extract hex at specific offset**:
   ```bash
   hexdump -C Data.db -s <offset> -n 64
   ```

2. **Compare with expected format**:
   - Check magic bytes (if applicable)
   - Verify VInt encoding
   - Validate flag bytes

3. **Look for patterns**:
   - Repeated byte sequences may indicate arrays/collections
   - All zeros may indicate padding
   - Non-zero high bytes suggest multi-byte integers

### Offset Validation
Track byte consumption at each parsing stage:
- Clustering prefix (may be 0 bytes)
- Row sizes (2 VInts)
- Liveness info (conditional)
- Deletion info (conditional)
- Column bitmap (conditional)
- Cell data

### Zero-Copy Considerations
When implementing parsers:
- Use `Bytes` crate for buffer sharing
- Avoid copying large cell values
- Keep references to original buffer
- Use byte slices not owned Vecs

## Integration with Rust Code

Current implementation in `cqlite-core/src/storage/sstable/reader/parsing/`:
- **`row_decoder/`** — the main V5 row/partition decoder, a **directory of ~30 files**
  (split out of the former single-file parser by epic #1116). Start at
  `row_decoder/row_flags.rs` for the flag constants and `row_decoder/mod.rs` for the
  parser entry point; then
  `row_framing.rs` (row/partition framing), `row_data.rs`, `cell_value_scalar.rs` /
  `cell_value_complex.rs` (cell decode), `complex_column.rs` (non-frozen collections),
  `frozen.rs` (frozen collections), `udt.rs`, `partition_driver.rs`.
- Uses zero-copy patterns with `Bytes`
- Handles compression transparently

> The former single-file V5-compressed-legacy parser module **no longer exists** — it was
> deleted by epic #1116 (source splits), commit `cb049f7a8`, and replaced by the
> `row_decoder/` directory above. If an older doc or comment points you at a single
> `.rs` file for the V5 parser, that pointer is stale.

## PRD Alignment

**Supports Milestone M1** (Core Reading Library):
- 100% Cassandra 5 SSTable format support
- All compression formats (LZ4, Snappy, Deflate, Zstd) plus Noop
- Zero-copy deserialization
- Memory target: <128MB for large files

## Quick Reference

### Flag Bytes (Row) — main flag byte

| Value | Name | Meaning |
|-------|------|---------|
| `0x01` | `END_OF_PARTITION` | End-of-partition marker — **nothing follows this flag byte** |
| `0x02` | `IS_MARKER` | Unfiltered is a RangeTombstoneMarker, not a Row |
| `0x04` | `ROW_HAS_TIMESTAMP` | Row-level liveness timestamp present (delta-encoded) |
| `0x08` | `ROW_HAS_TTL` | Row-level TTL present (delta-encoded) |
| `0x10` | `ROW_HAS_DELETION` | Row deletion (tombstone) present |
| `0x20` | `ROW_HAS_ALL_COLUMNS` | All schema columns present — no column bitmap follows |
| `0x40` | `ROW_HAS_COMPLEX_DELETION` | Row carries a non-frozen collection column with deletion info |
| `0x80` | `ROW_HAS_EXTENDED_FLAGS` | A second (extended) flag byte follows |

### Flag Bytes (Row) — EXTENDED flag byte (only when `0x80` is set)

| Value | Name | Meaning |
|-------|------|---------|
| `0x01` | `EXTENDED_IS_STATIC` | Static row — has **NO** clustering prefix |

> **Citations**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/row_flags.rs:12-18`
> (`ROW_HAS_TIMESTAMP`/`TTL`/`DELETION`/`ALL_COLUMNS`/`COMPLEX_DELETION`/`EXTENDED_FLAGS`),
> `:24` (`END_OF_PARTITION = 0x01`), `:26` (`IS_MARKER = 0x02`), `:31`
> (`EXTENDED_IS_STATIC = 0x01`). Guide: `appendix-b-encodings-cheat-sheet.md:206-212`.
> Cassandra: `UnfilteredSerializer.java:102-109` (flags) and `:114-122` (extended flags).

**⚠️ The two highest-consequence bits.** `0x01` on the **main** byte is
`END_OF_PARTITION`, NOT `IS_STATIC` and NOT a marker flag — misreading it means
**mis-detecting partition boundaries**. `IS_STATIC` is `0x01` of the **EXTENDED** byte,
which only exists when `ROW_HAS_EXTENDED_FLAGS (0x80)` is set. There is exactly ONE
`HAS_ALL_COLUMNS` and its value is `0x20`.

### VInt Encoding
Variable-length integer encoding:
- First byte indicates length
- Subsequent bytes contain value
- Used for row sizes, timestamps, offsets

## Format authority (#3041)

**A CQLite `file:line` is NEVER format authority.** Citing CQLite's own code to justify CQLite's
behavior is circular reasoning. Authority is, in order:

1. The **pinned `cassandra-5.0.8` Cassandra source** — read it as a tag ref, never a working tree:
   ```bash
   git show cassandra-5.0.8:src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java
   ```
   (Browse: https://github.com/apache/cassandra/tree/cassandra-5.0.8.) A local clone is **optional and
   branch-sensitive** — none is guaranteed to exist, and one that does may sit on `trunk`/`6.0-alpha`,
   which is **not** the 5.0 on-disk format; read it via `git -C "$CQLITE_CASSANDRA_REPO" show
   cassandra-5.0.8:<path>`.
2. `sstabledump` output on a real SSTable.
3. `docs/sstables-definitive-guide/`.

A CQLite source line is evidence of *what CQLite does*, never of *what is correct*.

## Next Steps

When parser encounters issues:
1. Log byte offsets at each stage
2. Compare against the pinned Java source (`git show cassandra-5.0.8:…/UnfilteredSerializer.java`)
3. Validate against sstabledump output
4. Check compression block boundaries
5. Verify delta encoding calculations

