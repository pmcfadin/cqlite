# Appendix G: Cassandra 5.0 Compression Chunk Formats

## Overview

Cassandra 5.0 uses a chunked compression approach for Data.db files. Data is split into fixed-size chunks (default 16 KiB / 16 384 bytes) and each chunk is independently compressed. The compression metadata is stored in CompressionInfo.db, while the actual compressed data is stored in Data.db.

### Compression Architecture

**Two-File System:**
1. **CompressionInfo.db**: Metadata file containing:
   - Algorithm class name (e.g., `LZ4Compressor`, `SnappyCompressor`)
   - Option count and option key-value pairs
   - Chunk length (uncompressed chunk size; default 16 384 bytes / 16 KiB)
   - Max compressed length (version-gated at ≥ `na`, so present in every format in scope:
     `na`/`nb` BIG and `oa`/`da` BTI); the raw-vs-compressed switch, not a hint
   - Total uncompressed data length
   - Chunk count
   - Array of chunk offsets pointing into `Data.db`

2. **Data.db**: Compressed data file containing:
   - Concatenated compressed chunks (no length prefixes, no delimiters)
   - Chunk boundaries defined by offsets in `CompressionInfo.db`
   - Each chunk followed by a 4-byte CRC32 checksum (computed over the stored bytes)
   - Only LZ4 chunks carry an algorithm-specific 4-byte little-endian size prefix; Snappy, Deflate, and Zstd do not
   - A chunk stored at `>= max_compressed_length` bytes holds raw, uncompressed data

**Key Design Principle**: CompressionInfo.db acts as an index into Data.db, allowing random access to compressed chunks without scanning the entire file.

## Compression Metadata Format (CompressionInfo.db)

### Binary Layout

CompressionInfo.db contains metadata about the compressed Data.db file. The format is:

```
[Algorithm Name: UTF-8 string via writeUTF() — 2-byte BE length + bytes]
[Option Count: 4 bytes BE — number of key-value pairs]
[Option Key[i]: UTF-8 string via writeUTF()] (repeated option_count times)
[Option Value[i]: UTF-8 string via writeUTF()] (repeated option_count times)
[Chunk Length: 4 bytes BE — uncompressed chunk size, default 16384]
[Max Compressed Length: 4 bytes BE — version-gated at >= "na"; present in all formats in scope]
[Data Length: 8 bytes BE — total uncompressed file size]
[Chunk Count: 4 bytes BE]
[Chunk Offsets: 8 bytes BE * count — byte offsets into Data.db]
```

Per-chunk CRC32 checksums are stored in `Data.db` immediately after each compressed chunk, not in `CompressionInfo.db`.

**Authoritative source**: [`io/compress/CompressionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java) — `open()` lines 76–112, `writeHeader()` lines 375–398

### Field Descriptions

| Field | Type | Size | Byte Order | Description |
|-------|------|------|-----------|-------------|
| Algorithm Name | UTF-8 via `writeUTF()` | 2+N | Big-Endian length prefix | Class simple name, e.g., `"LZ4Compressor"`, `"NoopCompressor"` |
| Option Count | u32 | 4 | Big-Endian | Number of key-value option pairs (0 for most compressors) |
| Option Key[i] | UTF-8 via `writeUTF()` | 2+N each | Big-Endian length prefix | Repeated `option_count` times |
| Option Value[i] | UTF-8 via `writeUTF()` | 2+N each | Big-Endian length prefix | Repeated `option_count` times |
| Chunk Length | u32 | 4 | Big-Endian | Uncompressed chunk size; default 16 384 bytes (16 KiB) |
| Max Compressed Length | u32 | 4 | Big-Endian | Version-gated at ≥ `na`, so present in all formats in scope; `Integer.MAX_VALUE` (`0x7fffffff`) with the default `minCompressRatio=0`; a chunk stored at this length or longer is raw. Zero is corrupt — see the guard below |
| Data Length | u64 | 8 | Big-Endian | Total uncompressed file size in bytes |
| Chunk Count | u32 | 4 | Big-Endian | Number of compressed chunks |
| Chunk Offsets | u64[] | 8 each | Big-Endian | Byte offset of each chunk in `Data.db` (`count` entries) |

### Important Notes

1. **No padding field**: There is no fixed padding after the algorithm name. The bytes following the name are the 4-byte option count (u32 BE).
2. **Option count is required**: Even when there are no options, the option count (0) is written.
3. **Max compressed length is version-gated**: Its presence is gated on SSTable format version ≥ `na` (`BigFormat.java` line 401), so every format this guide covers has it. Do not write a reader that omits it.
4. **Data length is uncompressed size**: The data length field stores the total UNCOMPRESSED size, not the compressed `Data.db` size.
5. **Per-chunk CRCs are in Data.db**: CRC32 values follow each compressed chunk inline in `Data.db`. `CompressionInfo.db` stores no per-chunk CRCs.

### Example: CompressionInfo.db with LZ4

Real bytes from `test_collections/nested_collections_table` `nb-1-big-CompressionInfo.db`
(all 55 bytes of the file):

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 0d                           Algorithm name length: 13
0x02    4c 5a 34 43 6f 6d 70            "LZ4Compressor"
        72 65 73 73 6f 72
0x0f    00 00 00 00                     Option count: 0 (no key-value options)
0x13    00 00 40 00                     Chunk length: 16384 (0x4000) — the 5.0 default
0x17    7f ff ff ff                     Max compressed length: 0x7FFFFFFF (Integer.MAX_VALUE, min_compress_ratio=0)
0x1b    00 00 00 00 00 00 51 a3         Uncompressed data length: 20899
0x23    00 00 00 02                     Chunk count: 2
0x27    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x2f    00 00 00 00 00 00 21 de         Chunk 1 offset: 8670 (0x21de)
```

Total size: **55 bytes** — the file ends after the last chunk offset, with no trailing metadata
checksum (Important Note 5 above). Everything else is derived, and the corresponding `Data.db`
(11 287 bytes) confirms each derivation:

| Derived value | Formula | Value | Confirmed in `Data.db` |
|---|---|---|---|
| chunk 0 compressed payload | `8670 − 0 − 4` | 8 666 | CRC32 at offset 8666 is `2a2020b9`, matching a CRC over bytes 0–8665 |
| chunk 1 compressed payload | `11287 − 8670 − 4` | 2 613 | — |
| chunk 0 uncompressed | `min(16384, 20899 − 0)` | 16 384 | LZ4 LE prefix at offset 0 is `00 40 00 00` = 16384 |
| chunk 1 uncompressed | `min(16384, 20899 − 16384)` | 4 515 | LZ4 LE prefix at offset 8670 = 4515 |

### Example: CompressionInfo.db with Snappy

Real bytes from `test_timeseries/event_store` `nb-1-big-CompressionInfo.db` (all 58 bytes):

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 10                           Algorithm name length: 16
0x02    53 6e 61 70 70 79 43 6f         "SnappyCompressor"
        6d 70 72 65 73 73 6f 72
0x12    00 00 00 00                     Option count: 0 (no key-value options)
0x16    00 00 40 00                     Chunk length: 16384 (0x4000)
0x1a    7f ff ff ff                     Max compressed length: 0x7FFFFFFF (Integer.MAX_VALUE, min_compress_ratio=0)
0x1e    00 00 00 00 00 00 55 7c         Uncompressed data length: 21884
0x26    00 00 00 02                     Chunk count: 2
0x2a    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x32    00 00 00 00 00 00 1e 35         Chunk 1 offset: 7733 (0x1e35)
```

Total size: **58 bytes** — again ending after the last chunk offset, with no trailing checksum.
Chunk 0's compressed payload is `7733 − 0 − 4 = 7729` bytes; its inline CRC32 at `Data.db` offset
7729 is `0x001daf10`, which matches a CRC32 over those 7729 bytes (verified — see Ch. 9 for the
micro-proof).

> **Note**: `CompressionInfo.db` carries no checksums at all — neither per-chunk nor whole-file.
> Per-chunk CRC32s follow each chunk inline in `Data.db`; whole-component integrity for the
> SSTable is covered by `Digest.crc32`.

## Compressed Chunk Format in Data.db

Each compressed chunk in `Data.db` has algorithm-specific content followed by a 4-byte CRC32:

```
[Compressed Data: variable length]
[CRC32: 4 bytes — computed over compressed bytes]
```

The compressed data format varies by algorithm. The compressed length is derived from consecutive chunk offsets in `CompressionInfo.db` minus 4 (for the CRC word).

### Important Notes

1. **No explicit length prefixes in Data.db**: Chunk boundaries are defined by offsets in CompressionInfo.db
2. **CRC checksums**: Per-chunk CRC32 values are appended inline in `Data.db` immediately after each compressed chunk — they are NOT stored in `CompressionInfo.db`
3. **Chunk alignment**: Chunks start at the byte offsets specified in the chunk offset array
4. **Last chunk**: The last data-bearing chunk decompresses to fewer than `chunk_length` bytes when `data_length` is not a multiple of `chunk_length`. A trailing map entry beyond the `data_length` bound may exist and holds no data at all — see Chunk Offset Calculation note 6.
5. **No metadata CRC in CompressionInfo.db**: The file ends after the last chunk offset. Cassandra does not write a trailing metadata CRC to `CompressionInfo.db`; integrity comes from per-chunk CRCs in `Data.db`.
6. **Not every chunk is compressed**: a chunk stored at `>= max_compressed_length` bytes holds raw, uncompressed data. There is no flag — the length comparison is the encoding.

## Compression Algorithm Formats

### LZ4 Compression

**Format in Data.db:**
```
[Uncompressed Size: 4 bytes LE]
[Compressed Data: variable length]
```

**Key Details:**
- Size prefix is **little-endian** (important!)
- Size prefix represents the decompressed length in bytes
- The size prefix is part of the compressed chunk data (included in chunk offset calculation)
- Cassandra uses LZ4 block format via jpountz library (not LZ4 frame format)
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Decompression Process:**
```java
// Cassandra source: LZ4Compressor.uncompress()
final int decompressedLength =
    (input[inputOffset] & 0xFF)
    | ((input[inputOffset + 1] & 0xFF) << 8)
    | ((input[inputOffset + 2] & 0xFF) << 16)
    | ((input[inputOffset + 3] & 0xFF) << 24);

writtenLength = decompressor.decompress(input,
    inputOffset + 4,  // Skip size prefix
    inputLength - 4,   // Compressed data length
    output,
    outputOffset,
    decompressedLength);
```

**CQLite Implementation:**
```rust
// Read 4-byte little-endian size prefix
let uncompressed_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

// Validate against decompression bomb limit (128MB)
validate_decompression_size(uncompressed_size)?;

// Decompress using lz4_flex
decompress_size_prepended(data)
```

### Snappy Compression

**Format in Data.db (NB - NewBinary format):**
```
[Compressed Data: variable length] (NO size prefix)
```

**Key Details:**
- Cassandra 5.0 uses **raw Snappy** (a bare Snappy block) with no size prefix and no framing
- The uncompressed length is carried by the Snappy block itself as a leading varint, not by
  Cassandra
- The decompressed size must be cross-checked against the length the chunk map implies
  (`min(chunk_length, data_length - chunk_index * chunk_length)`)
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Decompression Process:**
```java
// Cassandra source: SnappyCompressor.uncompress() line 95
return Snappy.rawUncompress(input, inputOffset, inputLength, output, outputOffset);

// Returns the number of bytes decompressed
```

**CQLite Implementation:** exactly one decode attempt, raw Snappy, selected by the authoritative
`CompressionInfo.db` compressor name. It never tries framed-then-raw and keeps whichever
"succeeds" — a format guess can silently mis-decode an adversarial chunk into wrong bytes, so the
strict single-format decode surfaces a typed error instead (no-heuristics mandate, issue #28; the
fallback was removed by issue #1588). The advertised length is read from the block's leading varint
and rejected against the bomb limit **before** any allocation. See
`cqlite-core/src/storage/sstable/compression.rs` (`snappy_decompress_raw`).

### Deflate Compression

**Format in Data.db:**
```
[Compressed Data: variable length]  (no length prefix)
[CRC32: 4 bytes]
```

**Key Details:**
- **No 4-byte size prefix** — `DeflateCompressor.compress()` writes the deflater output directly
  with no length header
- The stream is **zlib-wrapped (RFC 1950)**, not raw deflate (RFC 1951).
  `DeflateCompressor` uses `new Deflater()` / `new Inflater()` (lines 69, 77) with no `nowrap`
  argument, so Java emits a 2-byte zlib header + deflate body + 4-byte Adler-32 trailer. Real
  chunks begin `78 9c` — verified against a Deflate-compressed corpus fixture. Decoding them as raw
  deflate fails.
- Cassandra does not call `setLevel()`, so the stream uses `Deflater`'s default level
  (`Deflater.DEFAULT_COMPRESSION`, equivalent to level 6).
- `compressArray()` returns **0** when `Deflater.needsInput()` is true (lines 113-114) — i.e. for empty
  input. That is why a degenerate empty trailing chunk stores a genuinely 0-byte payload under
  Deflate, and why inflating it is a hard error rather than a latent one. See Chunk Offset
  Calculation note 6.
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Decompression Process:**
```java
// Cassandra source: DeflateCompressor.uncompress() — no prefix; inflates all inputLength bytes
Inflater inf = inflater.get();
inf.reset();
inf.setInput(input, inputOffset, inputLength);
return inf.inflate(output, outputOffset, maxOutputLength);
```

**CQLite Implementation:** decodes with a zlib-aware decoder (`flate2::read::ZlibDecoder`), rejects
an empty chunk with a typed error rather than passing it to the decoder, and bounds the output with
`Read::take(MAX_DECOMPRESSED_SIZE + 1)` since a zlib stream declares no length. See
`cqlite-core/src/storage/sstable/compression.rs` (issue #1082).

### Zstd Compression

**Format in Data.db:**
```
[Compressed Data: variable length]  (Zstd frame format; no extra length prefix)
[CRC32: 4 bytes]
```

**Key Details:**
- **No 4-byte size prefix** — `ZstdCompressor.compress()` writes a bare Zstd frame with no extra length header
- Uses Zstd frame format with internal content checksum enabled (`ENABLE_CHECKSUM_FLAG = true`)
- Compression level 3 is the default (`ZstdCompressor.java:48`:
  `DEFAULT_COMPRESSION_LEVEL = 3`); it is settable per table via the `compression_level` option
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Decompression Process:**
```java
// Cassandra source: ZstdCompressorBase.uncompress()
long dsz = Zstd.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                    input, inputOffset, inputLength);

if (Zstd.isError(dsz)) {
    throw new IOException("Decompression failed");
}
```

**CQLite Implementation:**
```rust
// No size prefix — decompress entire chunk (bounds from CompressionInfo.db offsets)
let decompressed = decode_all(data)?;
validate_decompression_size(decompressed.len())?;
```

## Chunk Offset Calculation

To find a specific chunk in Data.db:

```
chunk_index = position_in_file / chunk_length          // position is UNCOMPRESSED

chunk_offset = chunk_offsets[chunk_index]
next_chunk_offset = chunk_offsets[chunk_index + 1]
    OR compressed_data_length (if last chunk)

compressed_length = next_chunk_offset - chunk_offset - 4   // -4 for the inline CRC word
```

Cassandra computes exactly this in `CompressionMetadata.chunkFor()`:

```java
// CompressionMetadata.java:249-252
long nextChunkOffset = idx + 8 == chunkOffsetsSize
                       ? compressedFileLength
                       : chunkOffsets.getLong(idx + 8);
return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
```

**Important Notes:**
1. **Chunk offsets** are stored as a simple array of u64 values (8 bytes each)
2. **Compressed length** is the difference of consecutive offsets **minus 4** — the delta includes
   the 4-byte CRC32 that follows each chunk inline in `Data.db`. Omitting the `− 4` feeds the CRC
   bytes to the decompressor and mis-verifies every checksum.
3. **No explicit length fields** per chunk in CompressionInfo.db - lengths are derived from offset differences
4. **Last chunk** length is `compressed_data_length - chunk_offsets[last] - 4`, where
   `compressed_data_length` is the on-disk `Data.db` size (Cassandra's `compressedFileLength`) — not
   the `data_length` field, which is the uncompressed total.
5. **Uncompressed length per chunk** is not stored either. It is
   `min(chunk_length, data_length - chunk_index * chunk_length)`; only the final data-bearing chunk
   is short.
6. **Degenerate trailing chunks**: `chunk_count` may be one greater than
   `ceil(data_length / chunk_length)`, leaving a final entry that maps to no uncompressed bytes.
   Stop iterating when `chunk_index * chunk_length >= data_length`. Compare the **logical** start
   against `data_length` — never a chunk offset, which is a compressed-file position. Cassandra
   itself never reads such a chunk because every logical position `< data_length` indexes to an
   earlier chunk. Chapter 9 covers the write-side mechanism, how often it occurs, and the
   per-compressor consequences of decompressing it anyway. Issue #2225, PR #2242.
7. **Raw (incompressible) chunks**: a chunk whose stored length is `>= max_compressed_length` was
   stored uncompressed and must **not** be handed to the decompressor
   (`CompressedSequentialWriter.flushData()` lines 159–178 on the write side;
   `CompressedChunkReader.Standard.readChunk()` lines 269–290 on the read side, which tests
   `chunk.length < maxCompressedLength` before decompressing).

## Memory Safety Considerations

### Decompression Bomb Protection

CQLite implements protection against decompression bombs by enforcing a 128MB limit:

```rust
const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

fn validate_decompression_size(uncompressed_size: usize) -> Result<()> {
    if uncompressed_size > MAX_DECOMPRESSED_SIZE {
        return Err("Decompression bomb protection: size exceeds 128MB limit");
    }
    Ok(())
}
```

The guard must run **before** allocation, not after decompression. CQLite inspects the advertised
length first for every algorithm that carries one (the LZ4 4-byte prefix, the Snappy leading
varint) and caps the reader for the wrapped streams (Deflate/Zstd) with `Read::take(limit + 1)`,
so an adversarial chunk cannot force a large allocation before any check runs
(`cqlite-core/src/storage/sstable/compression.rs`).

### Size Prefix Validation

1. For algorithms carrying a declared length (LZ4's 4-byte little-endian prefix, Snappy's leading
   varint), extract and validate it **before** calling the decompressor — the common library
   entry points pre-allocate from that value.
2. For wrapped streams with no declared length (Deflate, Zstd), bound the output side instead of
   trusting the stream.
3. Independently, cross-check the decompressed length against the value the chunk map implies:
   `min(chunk_length, data_length - chunk_index * chunk_length)`. A mismatch means corruption or a
   mis-derived chunk boundary, and is a typed error — never a fallback to another decode attempt.

### Corrupt `max_compressed_length` Guard

`max_compressed_length` selects between compressed and raw chunk storage: the reader treats a chunk
as raw when its stored length is `>= max_compressed_length`. A **zero** value therefore makes that
test true for every chunk, so a reader silently returns still-compressed bytes as if they were
plaintext instead of raising an error — it fails **open**. The per-chunk CRC32 cannot catch this,
because the CRC is computed over the genuinely-on-disk compressed bytes and matches.

Cassandra never writes zero here — the default is `Integer.MAX_VALUE` — but note that
`CompressionParams.validate()` (lines 441–455) does not reject it either: it rejects
`maxCompressedLength < 0` and `chunkLength < maxCompressedLength < Integer.MAX_VALUE`, and `0`
passes both. Zero is excluded by construction, not by validation, so a reader must treat it as a
corruption case.

Reject it once, at `CompressionInfo.db` parse time, before any chunk read, so no downstream
consumer can observe an unguarded zero:

```rust
// cqlite-core/src/storage/sstable/compression_info.rs:226-230
if max_compressed_length == 0 {
    return Err(Error::InvalidFormat(
        "max_compressed_length cannot be zero".to_string(),
    ));
}
```

CQLite fails **closed** with that typed error and repeats the guard in the point-read seek paths
(`reader/data_access/compressed_offset.rs:107`, `reader/data_access/big_promoted.rs:681`).
Issues #2524, #2529.

## Algorithm Selection in Cassandra

Cassandra stores the full Java class name in CompressionInfo.db:

| Algorithm | Class Name |
|-----------|-----------|
| LZ4 | `LZ4Compressor` |
| Snappy | `SnappyCompressor` |
| Deflate | `DeflateCompressor` |
| Zstd | `ZstdCompressor` |
| Noop | `NoopCompressor` |

CQLite normalizes these to standard names:
```rust
"LZ4Compressor" -> "LZ4"
"SnappyCompressor" -> "SNAPPY"
"DeflateCompressor" -> "DEFLATE"
"ZstdCompressor" -> "ZSTD"
```

## Byte Order Summary

| Algorithm | Size Prefix in Data.db | Byte Order | Notes |
|-----------|------------------------|-----------|-------|
| LZ4 | Yes — 4 bytes | Little-Endian | Uncompressed length prepended by Cassandra's `LZ4Compressor`, then an LZ4 **block** (not frame) |
| Snappy | No | N/A | Bare Snappy block; its own leading varint carries the length |
| Deflate | No | N/A | **zlib-wrapped (RFC 1950)** stream — begins `78 9c`; not raw RFC 1951 deflate |
| Zstd | No | N/A | Bare Zstd frame (internal content checksum enabled) |

All `CompressionInfo.db` header fields and chunk offsets are big-endian. The only little-endian
value anywhere in the compression layer is LZ4's 4-byte uncompressed-length prefix inside the chunk
payload (`LZ4Compressor.compress()` lines 120–123 write it byte-by-byte, low byte first).

## CRC Checksum Format

### Per-Chunk CRC32 (in Data.db)

Each compressed chunk in `Data.db` is immediately followed by a 4-byte CRC32 checksum:

```
[Compressed Data: variable length]
[CRC32: 4 bytes — computed over compressed bytes only]
```

- Computed using Java `java.util.zip.CRC32` (IEEE polynomial, same as `crc32()` in zlib)
- Covers the **stored** bytes of the chunk — the compressed payload, or the raw bytes for an
  incompressible chunk — and never the CRC field itself
- Written for every chunk the writer emits, including a raw chunk and a degenerate empty trailing
  chunk. An empty Deflate chunk stores a 0-byte payload with CRC `00000000` — verified on
  `cqlite-core/tests/fixtures/issue_2225/multi_partition_table/nb-1-big-{CompressionInfo,Data}.db`
  (`DeflateCompressor`, `data_length` 5681, 2 chunk offsets `[0, 3266]` where 1 is expected, final
  payload 0 bytes, stored CRC `00000000` == computed). The 5-byte LZ4 empty chunk stores
  `c622f71d`, verified across all 16 degenerate `CompressionInfo.db` files in
  `test-data/datasets/sstables/` (every one LZ4, payload `0000000000`, stored CRC == computed).
  No degenerate **Deflate** chunk exists anywhere under `test-data/` — the Deflate case is
  evidenced only by the `issue_2225` fixture above.
- Source: `CompressedSequentialWriter.flushData()`, `crcMetadata.appendDirect(toWrite, true)` (line 192)
- Next chunk offset = current offset + stored length + 4 (`chunkOffset += compressedLength + 4`,
  line 203)

`CompressionInfo.db` stores **no** per-chunk CRCs — it stores only chunk byte offsets.

**Reader note on verification cost**: Cassandra verifies a chunk CRC probabilistically, governed by
the table's `crc_check_chance` (`CompressedChunkReader.shouldCheckCrc()`, lines 63–67) — a value of
1.0 checks every chunk. CQLite validates the CRC on every chunk read.

## Practical Example: Reading an LZ4 Chunk

Given a file with:
- CompressionInfo.db showing: chunk_offsets = [0, 1024], chunk_length = 65536 (non-default;
  default is 16384), max_compressed_length = 0x7fffffff, data_length ≥ 65536
- Data.db with compressed data at offset 0

```
Bytes 0-3:      [0x00, 0x00, 0x01, 0x00]  = 0x00010000 LE = 65536 (uncompressed size)
Bytes 4-1019:   Compressed data (1016 bytes)
Bytes 1020-1023: CRC32 over bytes 0-1019 (big-endian u32)
```

Reading process:
1. Determine chunk 0 offset = 0, chunk 1 offset = 1024
2. Calculate compressed length = `1024 - 0 - 4` = **1020** bytes (the delta includes the CRC word)
3. Seek to position 0 in Data.db
4. Read 1020 bytes of compressed data, then 4 more bytes as the big-endian CRC32
5. Verify CRC32 over the 1020 compressed bytes
6. Confirm `1020 < max_compressed_length`, so the chunk is compressed rather than raw
7. Extract the 4-byte little-endian prefix = 65536 (uncompressed size); validate it against the
   decompression-bomb limit before decompressing
8. Decompress the remaining 1016 bytes using LZ4 block decompression
9. Verify decompressed size = `min(chunk_length, data_length - 0)` = 65536

## Related Documentation

- **Chapter 5**: Data.db Format and row structure
- **Chapter 6**: Index.db and Summary.db structure
- **Chapter 9**: Compression and chunking details
- **Appendix B**: Encoding cheat sheet (VInt, flags, byte order)
- **Appendix F**: Known limitations (what's not supported yet)
- **Parser**: `cqlite-core/src/storage/sstable/compression_info.rs` (header + guards),
  `cqlite-core/src/storage/sstable/chunk_decompressor.rs` (per-chunk read, raw fallback,
  size cross-check), `cqlite-core/src/storage/sstable/reader/block_io.rs` (`data_length` chunk bound)
- **Writer**: `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs` — note that
  CQLite's production write surface emits **uncompressed** SSTables and never writes a
  `CompressionInfo.db`; the compressed-write building blocks exist for fixture synthesis only and
  configuring compressed production writing returns `Error::UnsupportedFormat` (issue #1406).
  Compressed **read** support (LZ4, Snappy, Deflate, Zstd) is complete.

### Cassandra 5.0.8 Source References

- [`CompressionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java) — `open()` lines 76–112, `chunkFor()` lines 235–253 (`− 4` CRC adjustment at line 252), `writeHeader()` lines 375–398
- [`CompressedSequentialWriter.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java) — `flushData()` lines 140–206: raw-chunk fallback lines 159–178, CRC write line 192, offset advance line 203
- [`CompressedChunkReader.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/CompressedChunkReader.java) — `shouldCheckCrc()` lines 63–67, raw-vs-compressed branch line 269 (`Standard`) and line 364 (`Mmap`). Note the package: `io/util/`, not `io/compress/`
- [`schema/CompressionParams.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) — `DEFAULT_CHUNK_LENGTH = 1024 * 16` (line 47), `DEFAULT_MIN_COMPRESS_RATIO = 0.0` (line 48), `calcMaxCompressedLength()` (line 186), `validate()` (lines 441–455)
- [`LZ4Compressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/LZ4Compressor.java) — 4-byte LE uncompressed-length prefix written at lines 120–123
- [`SnappyCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/SnappyCompressor.java) — no prefix, raw Snappy block (`rawUncompress`, line 95)
- [`DeflateCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/DeflateCompressor.java) — no prefix; `new Deflater()`/`new Inflater()` (lines 69, 77) means a zlib-wrapped (RFC 1950) stream; `compressArray()` returns 0 for empty input (lines 113-114)
- [`ZstdCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/ZstdCompressor.java) — no prefix, bare Zstd frame with internal checksum; `DEFAULT_COMPRESSION_LEVEL = 3` (line 48)
- [`NoopCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/NoopCompressor.java) — passthrough compressor
- [`BigFormat.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java) — `hasMaxCompressedLength` version gate (line 401)
