# Appendix G: Cassandra 5.0 Compression Chunk Formats

## Overview

Cassandra 5.0 uses a chunked compression approach for Data.db files. Data is split into fixed-size chunks (typically 64KB) and each chunk is independently compressed. The compression metadata is stored in CompressionInfo.db, while the actual compressed data is stored in Data.db.

### Compression Architecture

**Two-File System:**
1. **CompressionInfo.db**: Metadata file containing:
   - Algorithm name (LZ4, Snappy, Deflate, Zstd)
   - Chunk length (uncompressed chunk size, typically 65536 bytes)
   - Array of chunk offsets pointing into Data.db
   - Optional per-chunk CRC32 checksums
   - Metadata CRC32 for integrity verification

2. **Data.db**: Compressed data file containing:
   - Concatenated compressed chunks (no length prefixes, no delimiters)
   - Chunk boundaries defined by offsets in CompressionInfo.db
   - Each chunk may have algorithm-specific size prefixes (see algorithm sections below)

**Key Design Principle**: CompressionInfo.db acts as an index into Data.db, allowing random access to compressed chunks without scanning the entire file.

## Compression Metadata Format (CompressionInfo.db)

### Binary Layout

CompressionInfo.db contains metadata about the compressed Data.db file. The format is:

```
[Algorithm Name Length: 2 bytes BE]
[Algorithm Name: variable length UTF-8]
[Padding: 4 bytes]
[Chunk Length: 4 bytes BE]
[Options: 4 bytes BE]
[Compressed Data Length: 8 bytes BE]
[Chunk Count: 4 bytes BE]
[Chunk Offsets: 8 bytes BE * count]
[Chunk CRCs: 4 bytes BE * count] (optional)
[Metadata CRC: 4 bytes BE]
```

**Implementation Reference**: `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs`

### Field Descriptions

| Field | Type | Size | Byte Order | Description |
|-------|------|------|-----------|-------------|
| Algorithm Name Length | u16 | 2 | Big-Endian | Length of algorithm name in bytes (e.g., 13 for "LZ4Compressor") |
| Algorithm Name | String | variable | UTF-8 | Full algorithm class name (e.g., "LZ4Compressor", "SnappyCompressor") |
| Padding | Fixed | 4 | - | Fixed padding (0x00000000) for 8-byte alignment |
| Chunk Length | u32 | 4 | Big-Endian | Size of uncompressed chunks (typically 65536 bytes / 64KB) |
| Options | u32 | 4 | Big-Endian | Options/flags field (typically 0x7FFFFFFF) |
| Compressed Data Length | u64 | 8 | Big-Endian | Total compressed Data.db file size in bytes |
| Chunk Count | u32 | 4 | Big-Endian | Number of compressed chunks |
| Chunk Offsets | u64[] | 8 each | Big-Endian | Byte offset of each chunk in Data.db (count entries) |
| Chunk CRCs | u32[] | 4 each | Big-Endian | Optional: CRC32 of each compressed chunk (count entries) |
| Metadata CRC | u32 | 4 | Big-Endian | CRC32 checksum of all preceding bytes |

### Important Notes

1. **Fixed 4-byte Padding**: The padding after algorithm name is NOT alignment-based, but a fixed 4-byte field (always 0x00000000)
2. **8-byte Offsets Only**: Chunk offsets are simple 8-byte values, NOT 20-byte structures with lengths
3. **Optional CRCs**: Per-chunk CRCs may be present or absent; metadata CRC is always present
4. **Compressed vs Uncompressed**: The data length field stores the total COMPRESSED size (Data.db size), not uncompressed size

### Example: CompressionInfo.db with LZ4 (No Per-Chunk CRCs)

Based on the implementation test case from `compression_info_writer.rs`:

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 0d                           Algorithm name length: 13
0x02    4c 5a 34 43 6f 6d 70            "LZ4Compressor"
        72 65 73 73 6f 72
0x0f    00 00 00 00                     Fixed padding (4 bytes)
0x13    00 01 00 00                     Chunk length: 65536 (0x10000)
0x17    7f ff ff ff                     Options: 0x7FFFFFFF
0x1b    00 00 00 00 00 00 3e 80         Compressed data length: 16000
0x23    00 00 00 02                     Chunk count: 2
0x27    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x2f    00 00 00 00 00 00 20 00         Chunk 1 offset: 8192 (0x2000)
0x37    [4-byte CRC32]                  Metadata CRC32
```

Total size: 59 bytes (55 bytes content + 4 bytes CRC)

### Example: CompressionInfo.db with Snappy (With Per-Chunk CRCs)

Based on the implementation test case:

```
Offset  Hex Bytes                       Decoded Field
------  --------------------------      -------------
0x00    00 10                           Algorithm name length: 16
0x02    53 6e 61 70 70 79 43 6f         "SnappyCompressor"
        6d 70 72 65 73 73 6f 72
0x12    00 00 00 00                     Fixed padding (4 bytes)
0x16    00 00 40 00                     Chunk length: 16384 (0x4000)
0x1a    7f ff ff ff                     Options: 0x7FFFFFFF
0x1e    00 00 00 00 00 00 1f 40         Compressed data length: 8000
0x26    00 00 00 02                     Chunk count: 2
0x2a    00 00 00 00 00 00 00 00         Chunk 0 offset: 0
0x32    00 00 00 00 00 00 10 00         Chunk 1 offset: 4096 (0x1000)
0x3a    11 22 33 44                     Chunk 0 CRC: 0x11223344
0x3e    55 66 77 88                     Chunk 1 CRC: 0x55667788
0x42    [4-byte CRC32]                  Metadata CRC32
```

Total size: 70 bytes (66 bytes content + 4 bytes CRC)

## Compressed Chunk Format in Data.db

Each compressed chunk in Data.db contains only the compressed data:

```
[Compressed Data: variable length]
```

The actual compressed data format varies by compression algorithm (see below). The compressed data size is determined by the offset difference in CompressionInfo.db's chunk offset array.

### Important Notes

1. **No explicit length prefixes in Data.db**: Chunk boundaries are defined by offsets in CompressionInfo.db
2. **CRC checksums**: Optional per-chunk CRC32 values are stored in CompressionInfo.db, not appended to Data.db chunks
3. **Chunk alignment**: Chunks start at the byte offsets specified in the chunk offset array
4. **Last chunk**: The last chunk may be smaller than the standard chunk size if the total data length is not evenly divisible
5. **Metadata CRC**: A CRC32 checksum of the entire CompressionInfo.db metadata is stored at the end of CompressionInfo.db

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
- No trailing CRC in Data.db - CRCs stored in CompressionInfo.db if present

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
- No trailing CRC in Data.db - CRCs stored in CompressionInfo.db if present

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
[Uncompressed Size: 4 bytes BE]
[Compressed Data: variable length]
```

**Key Details:**
- Size prefix is **big-endian**
- Uses standard zlib Deflate format (RFC 1951 deflate stream format)
- Deflate level 6 is used by Cassandra
- No trailing CRC in Data.db - CRCs stored in CompressionInfo.db if present

**Decompression Process:**
```java
// Cassandra source: DeflateCompressor.uncompress()
Inflater inf = inflater.get();
inf.reset();
inf.setInput(input, inputOffset, inputLength);
return inf.inflate(output, outputOffset, maxOutputLength);
```

**CQLite Implementation:**
```rust
// Extract uncompressed size (4 bytes, big-endian)
let uncompressed_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

// Validate size to prevent decompression bombs
validate_decompression_size(uncompressed_size)?;

// Decompress the actual data (skip first 4 bytes)
let compressed_data = &data[4..];
let mut decoder = DeflateDecoder::new(compressed_data);
let mut decompressed = Vec::new();
decoder.read_to_end(&mut decompressed)?;

// Verify decompressed size matches expected
if decompressed.len() != uncompressed_size {
    return Err("Deflate size mismatch");
}
```

### Zstd Compression

**Format in Data.db:**
```
[Uncompressed Size: 4 bytes BE]
[Compressed Data: variable length]
```

**Key Details:**
- Size prefix is **big-endian**
- Uses Zstd frame format with checksum enabled
- Compression level 3 is default
- No trailing CRC in Data.db - CRCs stored in CompressionInfo.db if present

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
// Extract uncompressed size (4 bytes, big-endian)
let uncompressed_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

// Validate size to prevent decompression bombs
validate_decompression_size(uncompressed_size)?;

// Decompress the actual data (skip first 4 bytes)
let compressed_data = &data[4..];
let decompressed = decode_all(compressed_data)?;

// Verify decompressed size matches expected
if decompressed.len() != uncompressed_size {
    return Err("Zstd size mismatch");
}
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
| None | `NoCompressor` or `NullCompressor` |

CQLite normalizes these to standard names:
```rust
"LZ4Compressor" -> "LZ4"
"SnappyCompressor" -> "SNAPPY"
"DeflateCompressor" -> "DEFLATE"
"ZstdCompressor" -> "ZSTD"
```

## Byte Order Summary

| Algorithm | Size Prefix | Byte Order |
|-----------|-------------|-----------|
| LZ4 | Yes | Little-Endian |
| Snappy | No (NB) / Yes (legacy) | N/A / Big-Endian |
| Deflate | Yes | Big-Endian |
| Zstd | Yes | Big-Endian |

## CRC Checksum Format

CompressionInfo.db contains two types of CRC checksums:

1. **Per-Chunk CRCs** (optional):
   - Stored in CompressionInfo.db as an array of u32 values (4 bytes each, big-endian)
   - One CRC32 value per chunk
   - Located after the chunk offset array
   - Used to validate individual compressed chunks

2. **Metadata CRC** (required):
   - Stored at the end of CompressionInfo.db (last 4 bytes)
   - CRC32 of all preceding bytes in the file
   - Uses big-endian byte order
   - Validated during CompressionInfo.db parsing

**Implementation Note**: CQLite validates the metadata CRC during parsing. Per-chunk CRC validation is optional and depends on whether the CompressionInfo.db file includes them.

## Practical Example: Reading an LZ4 Chunk

Given a file with:
- CompressionInfo.db showing: chunk_offsets = [0, 1024], chunk_length=65536
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
