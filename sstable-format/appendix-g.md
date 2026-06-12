---
title: "Appendix G: Cassandra 5.0 Compression Chunk Formats"
description: "Cassandra 5.0 uses a chunked compression approach for Data.db files. Data is split into fixed-size chunks (default 16 KiB / 16 384 bytes) and each..."
sidebar:
  label: "Appendix G: Cassandra 5.0 Compression Chunk Formats"
  order: 107
---

## Overview

Cassandra 5.0 uses a chunked compression approach for Data.db files. Data is split into fixed-size chunks (default 16 KiB / 16 384 bytes) and each chunk is independently compressed. The compression metadata is stored in CompressionInfo.db, while the actual compressed data is stored in Data.db.

### Compression Architecture

**Two-File System:**
1. **CompressionInfo.db**: Metadata file containing:
   - Algorithm class name (e.g., `LZ4Compressor`, `SnappyCompressor`)
   - Option count and option key-value pairs
   - Chunk length (uncompressed chunk size; default 16 384 bytes / 16 KiB)
   - Max compressed length (present for SSTable format version ≥ "na" / Cassandra 3.0+)
   - Total uncompressed data length
   - Chunk count
   - Array of chunk offsets pointing into `Data.db`

2. **Data.db**: Compressed data file containing:
   - Concatenated compressed chunks (no length prefixes, no delimiters)
   - Chunk boundaries defined by offsets in `CompressionInfo.db`
   - Each chunk followed by a 4-byte CRC32 checksum (computed over the compressed bytes)
   - Only LZ4 chunks carry an algorithm-specific 4-byte little-endian size prefix; Snappy, Deflate, and Zstd do not

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
[Max Compressed Length: 4 bytes BE — only present for format version >= "na" (Cassandra 3.0+)]
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
| Max Compressed Length | u32 | 4 | Big-Endian | Present only for SSTable format ≥ "na" (Cassandra 3.0+); `Integer.MAX_VALUE` when `minCompressRatio=0` |
| Data Length | u64 | 8 | Big-Endian | Total uncompressed file size in bytes |
| Chunk Count | u32 | 4 | Big-Endian | Number of compressed chunks |
| Chunk Offsets | u64[] | 8 each | Big-Endian | Byte offset of each chunk in `Data.db` (`count` entries) |

### Important Notes

1. **No padding field**: There is no fixed padding after the algorithm name. The bytes following the name are the 4-byte option count (u32 BE).
2. **Option count is required**: Even when there are no options, the option count (0) is written.
3. **Max compressed length is version-gated**: Present only for SSTable format version ≥ "na" (Cassandra 3.0+); absent in older formats. Source: `BigFormat.java` line 401.
4. **Data length is uncompressed size**: The data length field stores the total UNCOMPRESSED size, not the compressed `Data.db` size.
5. **Per-chunk CRCs are in Data.db**: CRC32 values follow each compressed chunk inline in `Data.db`. `CompressionInfo.db` stores no per-chunk CRCs.

### Example: CompressionInfo.db with LZ4

Based on the implementation test case from `compression_info_writer.rs`:

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 0d                           Algorithm name length: 13
0x02    4c 5a 34 43 6f 6d 70            "LZ4Compressor"
        72 65 73 73 6f 72
0x0f    00 00 00 00                     Option count: 0 (no key-value options)
0x13    00 01 00 00                     Chunk length: 65536 (0x10000) — non-default; default is 16384 (16 KiB)
0x17    7f ff ff ff                     Max compressed length: 0x7FFFFFFF (Integer.MAX_VALUE, minCompressRatio=0)
0x1b    00 00 00 00 00 00 3e 80         Uncompressed data length: 16000
0x23    00 00 00 02                     Chunk count: 2
0x27    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x2f    00 00 00 00 00 00 20 00         Chunk 1 offset: 8192 (0x2000)
0x37    [4-byte CRC32]                  Metadata CRC32
```

Total size: 59 bytes (55 bytes content + 4 bytes CRC)

### Example: CompressionInfo.db with Snappy

Based on the implementation test case:

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 10                           Algorithm name length: 16
0x02    53 6e 61 70 70 79 43 6f         "SnappyCompressor"
        6d 70 72 65 73 73 6f 72
0x12    00 00 00 00                     Option count: 0 (no key-value options)
0x16    00 00 40 00                     Chunk length: 16384 (0x4000)
0x1a    7f ff ff ff                     Max compressed length: 0x7FFFFFFF (Integer.MAX_VALUE, minCompressRatio=0)
0x1e    00 00 00 00 00 00 1f 40         Uncompressed data length: 8000
0x26    00 00 00 02                     Chunk count: 2
0x2a    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x32    00 00 00 00 00 00 10 00         Chunk 1 offset: 4096 (0x1000)
0x3a    [4-byte CRC32]                  Metadata CRC32
```

Total size: 62 bytes (58 bytes content + 4 bytes CRC)

> **Note**: Per-chunk CRCs are NOT stored in `CompressionInfo.db`. They follow each chunk in `Data.db`.

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
4. **Last chunk**: The last chunk may be smaller than the standard chunk size if the total data length is not evenly divisible
5. **No metadata CRC in CompressionInfo.db**: The file ends after the last chunk offset. Cassandra does not write a trailing metadata CRC to `CompressionInfo.db`; integrity comes from per-chunk CRCs in `Data.db`.

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
- Cassandra 5.0 NB format uses **raw Snappy** without a size prefix
- The uncompressed size is determined by decompression (not from metadata)
- Decompressed size is validated against chunk_length from CompressionInfo.db
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Legacy Format (pre-5.0):**
```
[Uncompressed Size: 4 bytes BE]
[Compressed Data: variable length]
```

**Decompression Process:**
```java
// Cassandra source: SnappyCompressor.uncompress()
return Snappy.rawUncompress(input, inputOffset, inputLength, output, outputOffset);

// Returns the number of bytes decompressed
```

**CQLite Implementation:**
```rust
// Try two formats:
// 1. With 4-byte size prefix (legacy)
if data.len() >= 4 {
    let uncompressed_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

    if uncompressed_size > 0 && uncompressed_size <= MAX_DECOMPRESSED_SIZE {
        let compressed_data = &data[4..];
        if let Ok(decompressed) = decoder.decompress_vec(compressed_data) {
            if decompressed.len() == uncompressed_size {
                return Ok(decompressed);
            }
        }
    }
}

// 2. Fall back to raw Snappy (no prefix) - Cassandra 5.0 NB format
let decompressed = decoder.decompress_vec(data)?;
```

### Deflate Compression

**Format in Data.db:**
```
[Compressed Data: variable length]  (no length prefix)
[CRC32: 4 bytes]
```

**Key Details:**
- **No 4-byte size prefix** — `DeflateCompressor.compress()` writes raw Deflate bytes with no length header
- Uses standard zlib Deflate format (RFC 1951 deflate stream format)
- Deflate level 6 is used by Cassandra
- 4-byte CRC32 immediately follows the compressed chunk bytes in `Data.db`

**Decompression Process:**
```java
// Cassandra source: DeflateCompressor.uncompress() — no prefix; inflates all inputLength bytes
Inflater inf = inflater.get();
inf.reset();
inf.setInput(input, inputOffset, inputLength);
return inf.inflate(output, outputOffset, maxOutputLength);
```

**CQLite Implementation:**
```rust
// No size prefix — decompress entire chunk (bounds from CompressionInfo.db offsets)
let mut decoder = DeflateDecoder::new(data);
let mut decompressed = Vec::new();
decoder.read_to_end(&mut decompressed)?;
validate_decompression_size(decompressed.len())?;
```

### Zstd Compression

**Format in Data.db:**
```
[Compressed Data: variable length]  (Zstd frame format; no extra length prefix)
[CRC32: 4 bytes]
```

**Key Details:**
- **No 4-byte size prefix** — `ZstdCompressor.compress()` writes a raw Zstd frame with no extra length header
- Uses Zstd frame format with internal content checksum enabled (`ENABLE_CHECKSUM_FLAG = true`)
- Compression level 3 is default
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
chunk_index = position_in_file / chunk_length

chunk_offset = chunk_offsets[chunk_index]
next_chunk_offset = chunk_offsets[chunk_index + 1]
    OR compressed_data_length (if last chunk)

compressed_length = next_chunk_offset - chunk_offset
```

**Important Notes:**
1. **Chunk offsets** are stored as a simple array of u64 values (8 bytes each)
2. **Compressed length** is calculated by subtracting consecutive offsets
3. **No explicit length fields** per chunk in CompressionInfo.db - lengths are derived from offset differences
4. **Last chunk** length is `compressed_data_length - chunk_offsets[last]`

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

### Size Prefix Validation

For algorithms with size prefixes:
1. Extract the prefix value
2. Validate it against the maximum before attempting decompression
3. For Snappy NB format (no prefix), validate decompressed size after decompression

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
| LZ4 | Yes — 4 bytes | Little-Endian | Uncompressed length prepended by compressor |
| Snappy | No | N/A | Raw Snappy frame |
| Deflate | No | N/A | Raw Deflate stream |
| Zstd | No | N/A | Zstd frame (internal content checksum enabled) |

## CRC Checksum Format

### Per-Chunk CRC32 (in Data.db)

Each compressed chunk in `Data.db` is immediately followed by a 4-byte CRC32 checksum:

```
[Compressed Data: variable length]
[CRC32: 4 bytes — computed over compressed bytes only]
```

- Computed using Java `java.util.zip.CRC32` (IEEE polynomial, same as `crc32()` in zlib)
- Covers the compressed bytes of the chunk — not the CRC field itself
- Applied to every chunk without exception
- Source: `CompressedSequentialWriter.flushData()`, `crcMetadata.appendDirect(toWrite, true)` (line ~192)
- Next chunk offset = current offset + compressed length + 4 (the CRC word)

`CompressionInfo.db` stores **no** per-chunk CRCs — it stores only chunk byte offsets.

**Implementation Note**: CQLite validates per-chunk CRCs during chunk reads.

## Practical Example: Reading an LZ4 Chunk

Given a file with:
- CompressionInfo.db showing: chunk_offsets = [0, 1024], chunk_length=65536 (non-default; default is 16384)
- Data.db with compressed data at offset 0

```
Bytes 0-3:      [0x00, 0x01, 0x00, 0x00]  = 0x00010000 LE = 65536 (uncompressed size)
Bytes 4-1023:   Compressed data (1020 bytes)
```

Reading process:
1. Determine chunk 0 offset = 0, chunk 1 offset = 1024
2. Calculate compressed length = 1024 - 0 = 1024 bytes
3. Seek to position 0 in Data.db
4. Read 1024 bytes of compressed data
5. Extract 4-byte LE prefix = 65536 (uncompressed size)
6. Decompress remaining 1020 bytes using LZ4
7. Verify decompressed size = 65536 matches chunk_length

## Related Documentation

- **Chapter 5**: Data.db Format and row structure
- **Chapter 6**: Index.db and Summary.db structure
- **Chapter 9**: Compression and chunking details
- **Appendix B**: Encoding cheat sheet (VInt, flags, byte order)
- **Appendix F**: Known limitations (what's not supported yet)
- **Implementation**: `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs`
- **Parser**: `cqlite-core/src/storage/sstable/compression_info.rs`

### Cassandra 5.0.8 Source References

- [`CompressionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java) — `open()` lines 76–112, `writeHeader()` lines 375–398
- [`CompressedSequentialWriter.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java) — per-chunk CRC write path, `flushData()` lines 140–206
- [`schema/CompressionParams.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) — `DEFAULT_CHUNK_LENGTH = 1024 * 16` (line 47)
- [`LZ4Compressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/LZ4Compressor.java) — 4-byte LE uncompressed-length prefix
- [`SnappyCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/SnappyCompressor.java) — no prefix, raw Snappy
- [`DeflateCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/DeflateCompressor.java) — no prefix, raw Deflate
- [`ZstdCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/ZstdCompressor.java) — no prefix, Zstd frame with internal checksum
- [`NoopCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/NoopCompressor.java) — passthrough compressor
- [`BigFormat.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java) — `hasMaxCompressedLength` version gate (line 401)
