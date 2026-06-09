## Checksums and Integrity

SSTables carry integrity metadata at two levels: per-chunk checksums for compressed `Data.db`
blocks, and the `Digest.crc32` file for component-level verification. Readers validate checksums
at each level to ensure data integrity throughout the read path.

> **Correction notice:** An earlier version of this chapter contained a section titled
> "Header CRC32 Prefixes" describing a CRC32 prefix that was believed to appear before certain
> NB-format SSTable headers. That section has been removed. Per
> `HEADER_CRC32_DOCUMENTATION.md` and verification against Cassandra 5.0.8 source, **NB format
> `Data.db` has NO magic number or global header** -- the file starts directly with compressed
> chunk data. The bytes previously mistaken for a header CRC32 (e.g., `0x71160000`,
> `0xf1185c00`) are the first bytes of compressed chunk data. NB format is identified by
> filename pattern, not file content. See `CompressedSequentialWriter` constructor and
> `flushData()` for confirmation.

### In this chapter you will learn
- How per-chunk checksums are stored and validated for NB compressed `Data.db`
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
Notes:
- NB format `Data.db` has no magic number or header; the file starts directly with compressed data.
- `Digest.crc32` is an independent per-component file; each component written by
  `ChecksummedSequentialWriter` or `CompressedSequentialWriter` generates its own
  `Digest.crc32`. There is no single digest enumerating all TOC components.
- Full matrix with details appears later in this chapter.

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

Source:
[`CompressedSequentialWriter.java:187-192`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java)
-- `channel.write(toWrite)` then `crcMetadata.appendDirect(toWrite, true)`.
Each chunk offset advances by `compressedLength + 4` (for the trailing CRC)
([`CompressedSequentialWriter.java:203`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java)).

### Validation Process

1. Read chunk bytes from Data.db (length from CompressionInfo.db)
2. Read next 4 bytes as big-endian u32 (expected CRC)
3. Compute CRC32 over chunk bytes using Java algorithm
4. Compare computed vs expected
5. On match: decompress and continue
6. On mismatch: corruption detected (fail or warn based on `crc_check_chance` config)

Explicit note: CRC32 is computed over the compressed chunk only and excludes the trailing
4-byte CRC itself
([`ChecksumWriter.java:68-69`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksumWriter.java)).

Minimal illustration (excerpt from a real `Data.db`, first 32 bytes):
```
00000000: fe1e 0000 f209 0010 6b88 bf20 a251 11f0
00000010: a3fe f1a5 5138 3fb9 7fff ffff 8000 0100
```
When aligned to a chunk boundary, the 4 bytes immediately following the compressed chunk are the big-endian CRC32 for that chunk.

### CRC Algorithm Details

- **Standard:** Java `java.util.zip.CRC32`
  ([`ChecksumType.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/ChecksumType.java))
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

## CRC.db Component (Separate Per-Chunk CRCs for Uncompressed Data)

For uncompressed (non-compressed) `Data.db`, a separate `CRC.db` file holds per-chunk CRC32
values.

Source:
[`ChecksummedSequentialWriter.java:33-40`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksummedSequentialWriter.java),
[`ChecksumWriter.java:43-53`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksumWriter.java).

**CRC.db layout:**
```
[Chunk Size: 4 bytes, signed int]   <- uncompressed buffer capacity (header)
[CRC32 chunk 0: 4 bytes]
[CRC32 chunk 1: 4 bytes]
...
```

**Seek formula** -- to locate the CRC for a given byte offset in `Data.db`:
```
chunk_index   = byte_offset / chunkSize
crc_file_pos  = (chunk_index * 4) + 4
```
where `chunkSize` is the 4-byte int at the start of `CRC.db`
([`DataIntegrityMetadata.java:52-53`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java):
`reader.seek(((start / chunkSize) * 4L) + 4)` where `start = chunkStart(offset)`).

Note: per-chunk CRC values written to `CRC.db` are **not** included in the full checksum
for `ChecksummedSequentialWriter` (`checksumIncrementalResult=false`,
[`ChecksummedSequentialWriter.java:49`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksummedSequentialWriter.java)).
For `CompressedSequentialWriter`, per-chunk CRC bytes **are** included in the full checksum
(`checksumIncrementalResult=true`,
[`CompressedSequentialWriter.java:192`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java)).

## Per-Chunk Checksums (Legacy Formats)
When compression is enabled, `CompressionInfo.db` may include a CRC for each compressed chunk. Readers should compute CRC over the compressed bytes and compare with metadata prior to decompression. This catches corruption early and avoids propagating errors downstream.

Readers should validate chunk CRCs where present before decompression; modern formats expect strict CRC adherence. For validation walkthroughs, see Appendix C.

## Digest Files
`Digest.crc32` is written by `ChecksumWriter.writeFullChecksum()` on SSTable finalization
([`ChecksumWriter.java:91-103`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksumWriter.java)).
It contains a **single line**: the CRC32 checksum value as a UTF-8 decimal string, flushed
and synced to disk.

Each component written by `ChecksummedSequentialWriter` or `CompressedSequentialWriter`
generates its own independent `Digest.crc32`. Each digest covers exactly one component file
([`DataIntegrityMetadata.FileDigestValidator`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java):
reads `Long.parseLong(digestReader.readLine())` -- decimal string -- and validates a single
`dataFile` against a single `digestFile`). There is no single `Digest.crc32` that enumerates
all components listed in `TOC.txt`.

`Digest.crc32` is complementary to per-chunk CRCs: the digest validates whole-file contents,
while per-chunk CRCs validate compressed block integrity during reads.

Minimal verification example:
1. For each component file, locate the corresponding `Digest.crc32`.
2. Compute CRC32 over the entire component file contents.
3. Compare against the decimal value in `Digest.crc32`; on mismatch, quarantine and
   rehydrate via repair/streaming.

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
- **NB format `Data.db` has NO magic number or header** -- the file starts directly with
  compressed chunk data. There are no “Header CRC32 Prefixes” in NB format.
- **NB format uses trailing chunk CRCs** -- placed after each compressed chunk, big-endian u32,
  covering compressed chunk bytes only.
- **CRC.db** (uncompressed path) holds a 4-byte int header (chunk size) + 4-byte CRC32 per chunk.
  Seek to chunk N's CRC with: `crc_file_pos = (chunk_index * 4) + 4`.
- **`Digest.crc32`** is an independent per-component file containing a single UTF-8 decimal
  CRC32 value; each component has its own digest. It does not enumerate all TOC components.
- Readers should validate all checksums on-the-fly; tools may verify digests offline.
- Fail-fast on any CRC mismatch -- do not attempt heuristic recovery in modern formats.

### References
- Cassandra 5.0.8:
  - `DataIntegrityMetadata`:
    [`org.apache.cassandra.io.util.DataIntegrityMetadata`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java)
  - `PureJavaCrc32`:
    [`org.apache.cassandra.utils.PureJavaCrc32`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/PureJavaCrc32.java)
  - `ChecksumWriter`:
    [`org.apache.cassandra.io.util.ChecksumWriter`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksumWriter.java)
    (chunk size header L43-48; appendDirect L62-89; writeFullChecksum L91-103)
  - `ChecksummedSequentialWriter`:
    [`org.apache.cassandra.io.util.ChecksummedSequentialWriter`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksummedSequentialWriter.java)
    (constructor L33-40; flushData L43-50 calling appendDirect with false)
  - `CompressedSequentialWriter`:
    [`org.apache.cassandra.io.compress.CompressedSequentialWriter`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java)
    (flushData L140-206: inline CRC after compressed chunk; digest on finalize L392-393)
  - `ChecksumType`:
    [`org.apache.cassandra.utils.ChecksumType`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/ChecksumType.java)

- CQLite implementation:
  - CRC32 computation: `crc32fast` crate (Rust standard library compatible)

For implementation details and walkthroughs, see Appendix C.

## Format/Component Checksum Matrix (Cassandra 5.0)

| Component (format)     | Header CRC32 prefix | Trailing chunk CRCs | Byte order (stored) | CRC scope | `Digest.crc32` present |
|------------------------|---------------------|---------------------|---------------------|-----------|------------------------|
| Data.db (BIG)          | no                  | no                  | n/a                 | n/a       | yes                    |
| Index.db (BIG)         | no                  | n/a                 | n/a                 | n/a       | yes                    |
| Summary.db (BIG)       | no                  | n/a                 | n/a                 | n/a       | yes                    |
| Filter.db (BIG)        | no                  | n/a                 | n/a                 | n/a       | yes                    |
| Statistics.db (BIG)    | no                  | n/a                 | n/a                 | n/a       | yes                    |
| CompressionInfo.db     | no                  | n/a                 | n/a                 | n/a       | yes                    |
| Data.db (NB)           | no                  | yes (per chunk)     | big-endian u32      | compressed chunk bytes only | yes |

Notes:
- NB `Data.db` starts directly with compressed data; no magic number, no header.
- Trailing CRCs apply only to NB `Data.db` and are big-endian u32 immediately following each compressed chunk.
- Each `Digest.crc32` covers exactly one component file (independent per-component, not a set digest).

## `Digest.crc32` Coverage

`Digest.crc32` is a per-component file written by `ChecksumWriter.writeFullChecksum()`. It holds
a single UTF-8 decimal line: the CRC32 over the full byte range of the associated component file.
Each component written by `ChecksummedSequentialWriter` or `CompressedSequentialWriter` produces
its own independent `Digest.crc32`.

For compressed components (`CompressedSequentialWriter`), the full checksum includes all
compressed data bytes **plus** all per-chunk CRC values
([`ChecksumWriter.java:74-81`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/ChecksumWriter.java)).
For uncompressed components (`ChecksummedSequentialWriter`), only data bytes are included
(per-chunk CRCs are not fed into the full checksum).

Minimal verification example:
1. Read `TOC.txt` to enumerate components.
2. For each listed component, compute CRC32 over the entire file contents.
3. Compare against entries in `Digest.crc32`; on mismatch, quarantine and rehydrate via repair/streaming.


