## Checksums and Integrity

SSTables carry integrity metadata at three levels: optional header CRC32 prefixes in legacy/BIG formats, per-chunk checksums for compressed `Data.db` blocks, and the `Digest.crc32` file for component-level verification. Readers validate checksums at each level to ensure data integrity throughout the read path.

### In this chapter you will learn
- How header CRC32 prefixes (Legacy/BIG only) protect SSTable metadata
- How per-chunk checksums are stored and validated
- What `Digest.crc32` covers and how it differs from other checksums
- How readers/writers interact with integrity metadata
- How to demonstrate a minimal verification example

### Checksum coverage at a glance (authoritative)

| Component / Format | Header CRC32 prefix | Trailing per-chunk CRCs | Byte order (stored) | CRC scope | Verified by |
|---|---|---|---|---|---|
| Data.db (BIG) | no | no | n/a | n/a | `Digest.crc32` |
| Data.db (NB) | no | yes | big-endian u32 | compressed chunk bytes only | reader per chunk + `Digest.crc32` |
| Index.db (BIG/NB) | no | n/a | n/a | n/a | `Digest.crc32` |
| Summary.db (BIG/NB) | no | n/a | n/a | n/a | `Digest.crc32` |
| Filter.db (BIG/NB) | no | n/a | n/a | n/a | `Digest.crc32` |
| Statistics.db (BIG/NB) | no | n/a | n/a | n/a | `Digest.crc32` |
| CompressionInfo.db (NB) | no | n/a | n/a | n/a | `Digest.crc32` |
| Legacy headers (select BIG sub-variants) | yes (where present) | n/a | big-endian u32 | header bytes only (after prefix) | reader on open |

Notes:
- “Legacy headers” are format-specific BIG-family artifacts, not used by NB `Data.db`.
- `Digest.crc32` validates whole components over full-file contents per `TOC.txt`.
- Full matrix with details appears later in this chapter.

## Header CRC32 Prefixes (Legacy Formats Only)

> **Note:** This section describes CRC32 prefixes in **legacy formats only**.
> NB format (Cassandra 4.x/5.x) does NOT use header CRC32 prefixes.
> See "NB Format: Trailing Chunk CRCs" section below for NB format CRC32 handling (trailing chunk CRCs).

Starting with certain Cassandra versions, some SSTable components in **legacy formats** (not NB) may include a **4-byte CRC32 checksum prefix** prepended to the file header. This provides early detection of header corruption before attempting to parse metadata.

### Format Structure

**Checksummed Header Format:**
```
[4 bytes: CRC32 checksum] [remaining bytes: actual header data]
│                          │
│                          └─ Starts with magic number (e.g., 0x6F610000 for 'oa' format)
└─ CRC32 of all subsequent header bytes
```

**Example from legacy Cassandra data (OA format):**
```
Offset  Bytes                 Interpretation
------  --------------------  ---------------------------
0x00    XX XX XX XX          CRC32 checksum
0x04    6F 61 00 00          Magic = 0x6F610000 ('oa' legacy format)
0x08    00 01                Version
0x0a    ...                  Remaining header data
```

> **IMPORTANT: NB Format is Headerless (Issue #211)**
>
> NB format Data.db files (`nb-*-big-Data.db`) have **NO magic number or header**.
> The file starts directly with compressed chunk data. The value `0x00400000` that
> sometimes appears at offset 0 is the **LZ4 chunk length prefix** (16384 in
> little-endian = `0x00004000`), NOT a magic number. NB format is identified solely
> by filename pattern, not by file content. See Chapter 9 for NB format details.

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

## NB Format: Trailing Chunk CRCs

NB format uses a different CRC strategy than legacy formats - CRCs are placed **after** (trailing) each chunk, not before.

### CRC Placement

```
[chunk_bytes: variable length] <- Compressed data
[crc32: 4 bytes, big-endian]   <- CRC32(chunk_bytes)
[next_chunk_bytes: variable]
[crc32: 4 bytes, big-endian]
...
```

### Validation Process

1. Read chunk bytes from Data.db (length from CompressionInfo.db)
2. Read next 4 bytes as big-endian u32 (expected CRC)
3. Compute CRC32 over chunk bytes using Java algorithm
4. Compare computed vs expected
5. On match: decompress and continue
6. On mismatch: corruption detected (fail or warn based on `crc_check_chance` config)

Explicit note: CRC32 is computed over the compressed chunk only and excludes the trailing 4-byte CRC itself.

Minimal illustration (excerpt from a real `Data.db`, first 32 bytes):
```
00000000: fe1e 0000 f209 0010 6b88 bf20 a251 11f0
00000010: a3fe f1a5 5138 3fb9 7fff ffff 8000 0100
```
When aligned to a chunk boundary, the 4 bytes immediately following the compressed chunk are the big-endian CRC32 for that chunk.

### CRC Algorithm Details

- **Standard:** Java `java.util.zip.CRC32`
- **Polynomial:** 0x04C11DB7 (IEEE standard)
- **Initial value:** 0
- **Reflected:** Yes (reversed polynomial: 0xEDB88320)
- **Output:** Big-endian u32

### Cassandra Configuration

- `crc_check_chance`: Probability of validating CRC (0.0 to 1.0)
- Default: 1.0 (always validate)
- Purpose: Trade integrity checking for performance

### Implementation Note

The `crc32fast` Rust crate implements the same algorithm. Ensure big-endian byte order when comparing.

## Per-Chunk Checksums (Legacy Formats)
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
- **Header CRC32 prefixes** (legacy formats only) protect SSTable metadata; **NB format does NOT use header CRC32 prefixes**.
- **NB format uses trailing chunk CRCs** - placed after each compressed chunk, not before.
- **Per-chunk CRCs** protect compressed `Data.db` blocks before decompression (legacy) or after reading (NB format).
- **`Digest.crc32`** validates whole-file content at the component level.
- Readers should validate all checksums on-the-fly; tools may verify digests offline.
- Fail-fast on any CRC mismatch—corruption detected; do not attempt heuristic recovery in modern formats.
- The three-level hierarchy provides defense in depth: header validation (legacy) or chunk validation (NB) → component validation.

### References
- Cassandra 5.0.0:
  - `DataIntegrityMetadata`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java`
  - `PureJavaCrc32`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/PureJavaCrc32.java`

- CQLite implementation:
  - Header CRC32 detection: `cqlite-core/src/storage/sstable/reader/header.rs` (see Issue #153)
  - CRC32 computation: `crc32fast` crate (Rust standard library compatible)

For implementation details and walkthroughs, see Appendix C.

## Format/Component Checksum Matrix (Cassandra 5.0)

| Component (format)     | Header CRC32 prefix | Trailing chunk CRCs | Byte order (stored) | CRC scope | `Digest.crc32` present |
|------------------------|---------------------|---------------------|---------------------|-----------|------------------------|
| Data.db (BIG)          | format-dependent    | no                  | n/a                 | n/a       | yes                    |
| Index.db (BIG)         | format-dependent    | n/a                 | n/a                 | n/a       | yes                    |
| Summary.db (BIG)       | format-dependent    | n/a                 | n/a                 | n/a       | yes                    |
| Filter.db (BIG)        | format-dependent    | n/a                 | n/a                 | n/a       | yes                    |
| Statistics.db (BIG)    | format-dependent    | n/a                 | n/a                 | n/a       | yes                    |
| CompressionInfo.db     | no                  | n/a                 | n/a                 | n/a       | yes                    |
| Data.db (NB)           | no                  | yes (per chunk)     | big-endian u32      | compressed chunk bytes only | yes |

Notes:
- “format-dependent” indicates presence varies by sub-version/feature flags in BIG/mc/mm families. NB does not use header CRCs.
- Trailing CRCs apply only to NB `Data.db` and are big-endian u32 immediately following each compressed chunk.
- `Digest.crc32` is emitted per generation and covers the component set listed in `TOC.txt` (see below).

## `Digest.crc32` Coverage

`Digest.crc32` is a per-generation file that stores CRC32 values for listed components. Coverage includes each component file named in `TOC.txt` for that generation (e.g., `Data.db`, `Index.db`, `Summary.db`, `Filter.db`, `Statistics.db`, `CompressionInfo.db` when present). Each entry records the CRC32 over the full file contents (entire byte range) of the corresponding component, computed independently per file.

Minimal verification example:
1. Read `TOC.txt` to enumerate components.
2. For each listed component, compute CRC32 over the entire file contents.
3. Compare against entries in `Digest.crc32`; on mismatch, quarantine and rehydrate via repair/streaming.


