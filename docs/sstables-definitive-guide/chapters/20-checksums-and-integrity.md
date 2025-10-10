## Checksums and Integrity

SSTables carry integrity metadata at three levels: header CRC32 prefixes (Cassandra 5.0+), per-chunk checksums for compressed `Data.db` blocks, and the `Digest.crc32` file for component-level verification. Readers validate checksums at each level to ensure data integrity throughout the read path.

### In this chapter you will learn
- How header CRC32 prefixes protect SSTable metadata (Cassandra 5.0+)
- How per-chunk checksums are stored and validated
- What `Digest.crc32` covers and how it differs from other checksums
- How readers/writers interact with integrity metadata
- How to demonstrate a minimal verification example

## Header CRC32 Prefixes (Cassandra 5.0+)

Starting with Cassandra 5.0, some SSTable components (particularly `Data.db` files in certain formats) may include a **4-byte CRC32 checksum prefix** prepended to the file header. This provides early detection of header corruption before attempting to parse metadata.

### Format Structure

**Checksummed Header Format:**
```
[4 bytes: CRC32 checksum] [remaining bytes: actual header data]
│                          │
│                          └─ Starts with magic number (e.g., 0x00400000)
└─ CRC32 of all subsequent header bytes
```

**Example from real Cassandra 5.0 data:**
```
Offset  Bytes                 Interpretation
------  --------------------  ---------------------------
0x00    f1 18 5c 00          CRC32 = 0xf1185c00
0x04    00 40 00 00          Magic = 0x00400000 (V5_0NewBig)
0x08    f2 09                Version = 0xf209
0x0a    ...                  Remaining header data
```

### Detection Algorithm

Readers should detect checksummed headers using this two-step process:

1. **Read first 4 bytes** as potential magic number
2. **Check against known formats**: If the value doesn't match any known magic number, treat it as a CRC32 checksum

**Implementation pattern:**
```rust
let first_4_bytes = read_be_u32()?;

if CassandraVersion::from_magic_number(first_4_bytes).is_none() {
    // First 4 bytes are likely a CRC32 checksum
    let expected_checksum = first_4_bytes;
    let header_data = read_remaining_header()?;

    // Validate checksum
    let computed_checksum = crc32fast::hash(&header_data);
    if computed_checksum != expected_checksum {
        return Err(HeaderChecksumMismatch {
            expected: expected_checksum,
            computed: computed_checksum,
        });
    }

    // Parse actual header starting after checksum
    parse_header(&header_data)
} else {
    // First 4 bytes are the magic number (no checksum)
    parse_header_from_offset_0()
}
```

### When Headers Have Checksums

**Observed in:**
- Collection tables with complex types
- Tables with User-Defined Types (UDTs)
- Some V5_0NewBig format SSTables (format-specific, not universal)

**Not observed in:**
- Simple tables with basic types
- Many V5_0NewBig tables (checksums are optional)
- Legacy formats (pre-5.0)

This is an **optional integrity feature**, not a format requirement. Readers must handle both checksummed and non-checksummed headers.

### Validation Strategy

**Fail-fast approach (recommended):**
```
1. Detect checksum presence (first 4 bytes don't match magic numbers)
2. Compute CRC32 of header data
3. Compare with prefix value
4. On mismatch: reject file immediately (corruption detected)
5. On match: proceed with header parsing
```

**Why fail-fast?** Header corruption indicates severe problems:
- File system corruption
- Failed writes during SSTable flush/compaction
- Data transfer errors
- Storage media failures

Attempting to parse corrupt headers leads to undefined behavior, crashes, or silent data corruption. Modern formats (Cassandra 5.0+) mandate strict validation.

### Error Handling

**Checksum mismatch errors should:**
- Report both expected and computed checksums (hex format)
- Include file path for forensics
- Not attempt fallback parsing or recovery
- Trigger component quarantine in production systems

**Example error message:**
```
Header checksum mismatch for /var/lib/cassandra/data/ks/table-uuid/nb-1-big-Data.db
Expected: 0xf1185c00
Computed: 0xa3b4c5d6
Action:  Quarantine component and trigger repair
```

### Integration with Other Checksums

The three-level checksum hierarchy:

1. **Header CRC32** (this section) - Protects metadata before parsing
2. **Per-chunk CRCs** (next section) - Protects compressed data blocks
3. **Digest.crc32** (later section) - Validates whole components

Each level serves a distinct purpose:
- Header CRC32: Early validation, prevents parsing corrupt metadata
- Chunk CRCs: Runtime validation during reads, per-block granularity
- Digest.crc32: Offline verification, post-transfer validation

### Key Takeaways

- Header CRC32 prefixes are **optional** in Cassandra 5.0 (format-specific)
- Detect by checking if first 4 bytes match known magic numbers
- Validate using `crc32fast::hash()` over remaining header bytes
- Fail immediately on mismatch (never attempt recovery)
- Handle both checksummed and non-checksummed headers in readers

## Per-Chunk Checksums
When compression is enabled, `CompressionInfo.db` may include a CRC for each compressed chunk. Readers should compute CRC over the compressed bytes and compare with metadata prior to decompression. This catches corruption early and avoids propagating errors downstream.

Readers should validate chunk CRCs where present before decompression; modern formats expect strict CRC adherence. For validation walkthroughs, see Appendix C.

## Digest Files
`Digest.crc32` provides a fast verification for the main components of an SSTable generation. It is complementary to per-chunk CRCs: the digest validates whole-file contents, while per-chunk CRCs validate compressed block integrity during reads.

Minimal example (conceptual): During directory validation, ensure that for each generation listed in `TOC.txt`, all required components exist and optionally check `Digest.crc32` against recomputed CRCs when available.

## Recovery Strategies (Beyond Detection)

Scope note: focus on SSTable-level recovery patterns; node-level operations are out of scope.

- Isolate and quarantine:
  - Move suspected-corrupt components out of the live path; keep originals for forensics
  - Prevent partial reads by ensuring `TOC.txt` no longer references quarantined files

- Targeted file replacement:
  - Replace only failed components from known-good copies (snapshot/backup)
  - Validate digests and, if compressed, sample chunk CRCs before activation

- Range-based rehydration:
  - Trigger repair/streaming for affected token ranges to reconstruct data from replicas
  - Prefer re-streaming over attempting to salvage partially corrupt `Data.db`

- Post-recovery hygiene:
  - Run verification tools; schedule compaction to remove overlap and rebuild summaries if required
  - Monitor error counters; re-scan directories after compaction

### Key Takeaways
- **Header CRC32 prefixes** (Cassandra 5.0+) protect SSTable metadata; detected when first 4 bytes don't match known magic numbers.
- **Per-chunk CRCs** protect compressed `Data.db` blocks before decompression.
- **`Digest.crc32`** validates whole-file content at the component level.
- Readers should validate all checksums on-the-fly; tools may verify digests offline.
- Fail-fast on any CRC mismatch—corruption detected; do not attempt heuristic recovery in modern formats.
- The three-level hierarchy provides defense in depth: header validation → chunk validation → component validation.

### References
- Cassandra 5.0.0:
  - `DataIntegrityMetadata`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java`
  - `PureJavaCrc32`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/PureJavaCrc32.java`

- CQLite implementation:
  - Header CRC32 detection: `cqlite-core/src/storage/sstable/reader/header.rs` (see Issue #153)
  - CRC32 computation: `crc32fast` crate (Rust standard library compatible)

For implementation details and walkthroughs, see Appendix C.


