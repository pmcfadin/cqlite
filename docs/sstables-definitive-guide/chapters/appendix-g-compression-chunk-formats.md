# Appendix G: Cassandra 5.0 Compression Chunk Formats

## Overview

Cassandra 5.0 uses a chunked compression approach for Data.db files. Data is split into fixed-size chunks (typically 64KB) and each chunk is independently compressed. The compression metadata is stored in CompressionInfo.db, while the actual compressed data is stored in Data.db with a 4-byte CRC checksum appended after each compressed chunk.

## Compression Metadata Format (CompressionInfo.db)

### Binary Layout

CompressionInfo.db contains metadata about the compressed Data.db file. The format is:

```
[Algorithm Name Length: 2 bytes BE]
[Algorithm Name: variable length UTF-8]
[Null Terminator: 1 byte] (optional)
[Chunk Length: 4 bytes BE]
[Data Length: 8 bytes BE]
[Number of Chunks: 4 bytes BE]
[Chunk Information: (number_of_chunks * 20 bytes)]
[Optional Compression Dictionary: variable length]
```

### Field Descriptions

| Field | Type | Size | Byte Order | Description |
|-------|------|------|-----------|-------------|
| Algorithm Name Length | u16 | 2 | Big-Endian | Length of algorithm name in bytes (e.g., 13 for "LZ4Compressor") |
| Algorithm Name | String | variable | UTF-8 | Full algorithm class name (e.g., "LZ4Compressor", "SnappyCompressor") |
| Null Terminator | u8 | 1 | - | Optional: 0x00 byte separator |
| Chunk Length | u32 | 4 | Big-Endian | Size of uncompressed chunks (typically 65536 bytes / 64KB) |
| Data Length | u64 | 8 | Big-Endian | Total uncompressed data size in bytes |
| Number of Chunks | u32 | 4 | Big-Endian | Count of compressed chunks |
| Chunk Info | struct | 20 each | - | See chunk info structure |

### Chunk Information Structure

For each chunk, 20 bytes of metadata:

```
[Chunk Offset: 8 bytes BE]
[Compressed Length: 4 bytes BE]
[Uncompressed Length: 4 bytes BE]
```

| Field | Type | Size | Byte Order | Description |
|-------|------|------|-----------|-------------|
| Chunk Offset | u64 | 8 | Big-Endian | Byte offset in the compressed Data.db file |
| Compressed Length | u32 | 4 | Big-Endian | Length of compressed chunk data (excluding CRC) |
| Uncompressed Length | u32 | 4 | Big-Endian | Length of original uncompressed data |

### Example: CompressionInfo.db with LZ4

```
Hex:                        Decoded:
00 0d                       Algorithm name length: 13
4c 5a 34 43 6f 6d 70        "LZ4Compressor"
72 65 73 73 6f 72
00                          Null terminator
00 00 40 00                 Chunk length: 16384 bytes (16KB)
00 00 00 00 00 10 00 00     Data length: 1048576 bytes (1MB)
00 00 00 01                 Number of chunks: 1
00 00 00 00 00 00 00 00     Chunk 0 offset: 0
00 00 20 00                 Chunk 0 compressed length: 8192 bytes
00 00 40 00                 Chunk 0 uncompressed length: 16384 bytes
```

## Compressed Chunk Format in Data.db

Each compressed chunk in Data.db has the structure:

```
[Compressed Data: variable length]
[CRC Checksum: 4 bytes BE]
```

The actual compressed data format varies by compression algorithm (see below).

### Important Notes

1. **CRC Checksum**: A 4-byte big-endian checksum is **appended after** the compressed chunk data
2. **No Length Prefix in Data.db**: The Data.db file does NOT contain explicit length prefixes; lengths are stored only in CompressionInfo.db
3. **Chunk Alignment**: Chunks align to the boundaries specified in the chunk offset table
4. **Last Chunk**: The last chunk may be smaller than the standard chunk size if the total data length is not evenly divisible

## Compression Algorithm Formats

### LZ4 Compression

**Format in Data.db:**
```
[Uncompressed Size: 4 bytes LE]
[Compressed Data: variable length]
[CRC Checksum: 4 bytes BE]
```

**Key Details:**
- Size prefix is **little-endian** (important!)
- Size prefix represents the decompressed length in bytes
- The size prefix is INSIDE the compressed chunk (included in CompressionInfo.compressed_length)
- Cassandra uses LZ4 frame format via jpountz library

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
[CRC Checksum: 4 bytes BE]
```

**Key Details:**
- Cassandra 5.0 NB format uses **raw Snappy** without a size prefix
- The uncompressed size is determined by decompression (not from metadata)
- Decompressed size is validated against chunk_length from CompressionInfo.db

**Legacy Format (pre-5.0):**
```
[Uncompressed Size: 4 bytes BE]
[Compressed Data: variable length]
[CRC Checksum: 4 bytes BE]
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
[CRC Checksum: 4 bytes BE]
```

**Key Details:**
- Size prefix is **big-endian**
- Uses standard zlib Deflate format (RFC 1951 deflate stream format)
- Deflate level 6 is used by Cassandra

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
[CRC Checksum: 4 bytes BE]
```

**Key Details:**
- Size prefix is **big-endian**
- Uses Zstd frame format with checksum enabled
- Compression level 3 is default

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

chunk_offset = chunks[chunk_index].offset
next_chunk_offset = chunks[chunk_index + 1].offset
    OR compressedFileLength (if last chunk)

compressed_length = next_chunk_offset - chunk_offset - 4  // Subtract CRC
```

**Important:** The 4-byte CRC checksum at the end means:
- CRC is NOT included in CompressionInfo.compressed_length
- When reading, you must account for the CRC: `actual_chunk_data_length = compressed_length`
- CRC starts at offset: `chunk_offset + compressed_length`

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

The 4-byte CRC checksum appended to each chunk:
- Uses **big-endian** byte order
- Position: `chunk_offset + compressed_length` (in Data.db)
- Not included in `CompressionInfo.compressed_length`
- Used by Cassandra for integrity verification

**CQLite Note:** Currently, CQLite does not validate CRC checksums when reading compressed chunks. This is acceptable for most use cases but could be added for stricter validation.

## Practical Example: Reading an LZ4 Chunk

Given a file with:
- CompressionInfo.db showing chunk 0: offset=0, compressed_length=1024, uncompressed_length=65536
- Data.db with compressed data at that location

```
Bytes 0-3:      [0x00, 0x01, 0x00, 0x00]  = 0x00010000 LE = 65536 (uncompressed size)
Bytes 4-1023:   Compressed data (1020 bytes)
Bytes 1024-1027: [CRC checksum]
```

Reading process:
1. Read chunk metadata: offset=0, compressed_length=1024
2. Seek to position 0 in Data.db
3. Read 1024 bytes total
4. Extract 4-byte LE prefix = 65536
5. Decompress remaining 1020 bytes using LZ4
6. Verify decompressed size = 65536
7. (Optional) Validate CRC at offset 1024

## Related Documentation

- **Chapter 5**: Data.db Format and row structure
- **Chapter 6**: Index.db and Summary.db structure
- **Appendix B**: Encoding cheat sheet (VInt, flags, byte order)
- **Appendix F**: Known limitations (what's not supported yet)
