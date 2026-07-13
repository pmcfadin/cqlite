# Compression Formats in Cassandra SSTables

## Overview

Cassandra 5.0 supports three compression algorithms:
- **LZ4** (default, recommended)
- **Snappy**
- **Deflate** (highest compression, slowest)

## Compression Block Structure

### Index Structure (CompressionInfo.db)
Located alongside Data.db:
```
[chunk_offset_1: u64]
[chunk_length_1: u32]
[chunk_offset_2: u64]
[chunk_length_2: u32]
...
```

Maps logical offsets to physical compressed chunks.

### Chunk Format
Each compressed chunk in Data.db:
```
[compressed_data: variable length]
[crc32: 4 bytes] (if CRC enabled)
```

**Chunk Parameters** (from Statistics.db):
- `chunkLength`: Max uncompressed size (typically 64KB)
- `compressionAlgorithm`: LZ4, Snappy, or Deflate
- `crcCheckChance`: Probability of CRC validation (0.0-1.0)

## LZ4 Compression

**Characteristics:**
- Very fast decompression
- Good compression ratio
- Default in Cassandra 5.0

**Rust Implementation:**
```rust
use lz4::block::decompress;

fn decompress_lz4(compressed: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    lz4::block::decompress(compressed, Some(uncompressed_size as i32))
        .map_err(|e| Error::Compression(format!("LZ4: {}", e)))
}
```

## Snappy Compression

**Characteristics:**
- Fast decompression
- Lower compression ratio than LZ4
- Widely supported

**Rust Implementation:**
```rust
use snap::raw::Decoder;

fn decompress_snappy(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder.decompress_vec(compressed)
        .map_err(|e| Error::Compression(format!("Snappy: {}", e)))
}
```

## Deflate Compression

**Characteristics:**
- Highest compression ratio
- Slower than LZ4/Snappy
- Standard zlib/gzip format

**Rust Implementation:**
```rust
use flate2::read::DeflateDecoder;
use std::io::Read;

fn decompress_deflate(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = DeflateDecoder::new(compressed);
    let mut uncompressed = Vec::new();
    decoder.read_to_end(&mut uncompressed)
        .map_err(|e| Error::Compression(format!("Deflate: {}", e)))?;
    Ok(uncompressed)
}
```

## CRC Validation

When `crcCheckChance > 0`, validate checksums:

```rust
fn validate_crc(data: &[u8], expected_crc: u32) -> Result<()> {
    let actual_crc = crc32fast::hash(data);
    if actual_crc != expected_crc {
        return Err(Error::CrcMismatch { expected_crc, actual_crc });
    }
    Ok(())
}
```

## Reading Compressed Data

### High-Level Flow
1. Parse CompressionInfo.db to build chunk index
2. For target offset in Data.db:
   - Find chunk containing offset
   - Read compressed chunk
   - Decompress chunk
   - Extract data at relative offset within decompressed buffer

### Offset Calculation
```rust
// Logical offset (from Index.db) → Physical chunk
fn find_chunk(logical_offset: u64, chunks: &[ChunkInfo]) -> Option<&ChunkInfo> {
    chunks.iter()
        .find(|chunk| {
            logical_offset >= chunk.offset 
            && logical_offset < chunk.offset + chunk.uncompressed_length
        })
}

// Offset within decompressed chunk
fn relative_offset(logical_offset: u64, chunk: &ChunkInfo) -> usize {
    (logical_offset - chunk.offset) as usize
}
```

## Zero-Copy Considerations

For optimal performance:
- **Reuse decompression buffers**: Don't allocate per-chunk
- **Share decompressed blocks**: Multiple rows may be in one chunk
- **Lazy decompression**: Only decompress chunks when needed
- **Cache hot chunks**: LRU cache for frequently accessed chunks

```rust
use bytes::Bytes;

struct ChunkCache {
    decompressed: Bytes,  // Zero-copy reference
    chunk_id: usize,
}

// Share decompressed data without copying
fn get_slice(&self, offset: usize, len: usize) -> Bytes {
    self.decompressed.slice(offset..offset + len)
}
```

## Compression Parameters

From Statistics.db `CompressionParameters`:
```json
{
  "chunk_length_in_kb": 64,
  "class": "org.apache.cassandra.io.compress.LZ4Compressor",
  "crc_check_chance": 1.0,
  "compression_level": null
}
```

**Key Values:**
- `chunk_length_in_kb`: Usually 16, 32, or 64
- `crc_check_chance`: 1.0 = always check, 0.0 = never check
- `compression_level`: Deflate-specific (1-9)

## Error Handling

Common compression errors:
- **CRC mismatch**: Data corruption (fail fast)
- **Decompression failure**: Invalid compressed data (fail fast)
- **Size mismatch**: Decompressed size != expected (fail fast)
- **Missing chunks**: CompressionInfo.db out of sync (fail fast)

All should be treated as unrecoverable errors - SSTable is corrupt.

## Performance Targets

Per PRD M1:
- Parse 1GB files in <10 seconds
- Memory usage <128MB (don't hold all decompressed chunks)
- Sub-millisecond partition lookups (cache hot chunks)

## Reference

See `cqlite-core/src/storage/sstable/reader/compression/` for implementation.

