# Appendix G Quick Reference - Compression Formats at a Glance

## One-Minute Summary

Cassandra 5.0 chunks data and compresses independently. Each algorithm wraps compressed data differently:

| Algorithm | Size Prefix | Order | Notes |
|-----------|-----------|-------|-------|
| **LZ4** | 4 bytes | **LE** | Most common; LE is critical! |
| **Snappy** | None (NB) | - | Cassandra 5.0 format; legacy has BE prefix |
| **Deflate** | 4 bytes | **BE** | Standard deflate stream |
| **Zstd** | 4 bytes | **BE** | Frame format with checksum |

All chunks end with: `[4-byte BE CRC]`

## Byte Swaps (Critical!)

```rust
// LZ4 ONLY - Little-Endian!
let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

// Deflate and Zstd - Big-Endian
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

// Snappy NB - NO PREFIX, raw Snappy
// (try legacy format first with BE prefix, fallback to raw)
```

## CompressionInfo.db Reading

```rust
// Binary layout:
[u16 BE] algorithm_name_length
[UTF-8] algorithm_name
[u8] null_terminator
[u32 BE] chunk_length
[u64 BE] data_length
[u32 BE] chunk_count

// Then for each chunk:
[u64 BE] offset          // In Data.db
[u32 BE] compressed_len  // EXCLUDES 4-byte CRC
[u32 BE] uncompressed_len
```

## Finding a Chunk in Data.db

```rust
// Given position in uncompressed file:
chunk_index = position / chunk_length
chunk_metadata = chunks[chunk_index]

// Read from Data.db:
chunk_data = read_from(
    chunk_metadata.offset,
    chunk_metadata.compressed_len
)

// Skip CRC (don't read it):
crc_location = chunk_metadata.offset + chunk_metadata.compressed_len
// (CRC is 4 bytes, but you can ignore it for decompression)
```

## Decompression Template

```rust
pub fn decompress(algorithm: &str, data: &[u8]) -> Result<Vec<u8>> {
    match algorithm {
        "LZ4" => {
            // Extract LE prefix
            let size = u32::from_le_bytes(data[0..4].try_into()?);
            assert!(size <= 128 * 1024 * 1024); // Bomb check
            lz4::decompress(&data[4..], size as usize) // Skip prefix
        }
        "SNAPPY" => {
            // Try legacy format first (BE prefix)
            if data.len() >= 4 {
                let size = u32::from_be_bytes(data[0..4].try_into()?);
                if size <= 128*1024*1024 {
                    if let Ok(result) = snappy_decode(&data[4..]) {
                        if result.len() == size as usize {
                            return Ok(result);
                        }
                    }
                }
            }
            // Fall back to raw Snappy (NB format)
            let result = snappy_decode(data)?;
            assert!(result.len() <= 128*1024*1024); // Bomb check
            Ok(result)
        }
        "DEFLATE" => {
            // Extract BE prefix
            let size = u32::from_be_bytes(data[0..4].try_into()?);
            assert!(size <= 128 * 1024 * 1024); // Bomb check
            let result = deflate_decode(&data[4..])?;
            assert_eq!(result.len(), size as usize);
            Ok(result)
        }
        "ZSTD" => {
            // Extract BE prefix
            let size = u32::from_be_bytes(data[0..4].try_into()?);
            assert!(size <= 128 * 1024 * 1024); // Bomb check
            let result = zstd_decode(&data[4..])?;
            assert_eq!(result.len(), size as usize);
            Ok(result)
        }
        _ => Err("Unknown algorithm")
    }
}
```

## Common Mistakes

1. **Using BE for LZ4**: LZ4 uses **little-endian**, not big-endian
   - `u32::from_le_bytes()` not `from_be_bytes()`

2. **Forgetting Snappy NB format**: Cassandra 5.0 has no size prefix for Snappy
   - Try legacy format, fall back to raw

3. **Including CRC in decompression**: The 4-byte CRC comes AFTER compressed data
   - Don't read it, don't pass it to decompressor
   - It's located at: `chunk_offset + compressed_length`

4. **Wrong CRC position**: CompressionInfo.db `compressed_length` doesn't include CRC
   - CRC is OUTSIDE the chunk metadata

5. **Ignoring bomb protection**: Always validate decompressed size <= 128MB
   - Use prefix size (if available) for early validation
   - Validate actual size post-decompression

## Algorithm Class Names (from CompressionInfo.db)

```
"LZ4Compressor"        -> normalize to "LZ4"
"SnappyCompressor"     -> normalize to "SNAPPY"
"DeflateCompressor"    -> normalize to "DEFLATE"
"ZstdCompressor"       -> normalize to "ZSTD"
"NoCompressor"         -> normalize to "NONE"
"NullCompressor"       -> normalize to "NONE"
```

## Files to Reference

- **Full spec**: `/docs/sstables-definitive-guide/chapters/appendix-g-compression-chunk-formats.md`
- **Research notes**: `/docs/archive/issues/COMPRESSION_CHUNK_FORMAT_RESEARCH.md`
- **CQLite code**: `/cqlite-core/src/storage/sstable/compression.rs`
- **Cassandra source**: `apache/cassandra:5.0.0` in `/src/java/org/apache/cassandra/io/compress/`

## Testing Your Implementation

```bash
# Extract SSTable from test data
cd test-data/datasets/sstables

# Dump with sstabledump (reference)
sstabledump test_basic/simple_table/*.db > reference.txt

# Decompress with your implementation
your_tool decompress test_basic/simple_table/*.db > output.txt

# Compare
diff reference.txt output.txt
```

## Size Examples

Typical CompressionInfo.db:
- ~50 bytes header
- ~20 bytes per chunk metadata
- 1 million chunks = ~20 MB overhead

Typical Data.db compression:
- LZ4: 50-70% of original
- Snappy: 50-60% of original
- Deflate: 30-50% of original
- Zstd: 25-45% of original

## When Something Breaks

1. **Decompression produces garbage**: Check byte order (especially LZ4)
2. **Size mismatch**: Validate decompressed.len() == expected
3. **Hangs on decompression**: Probably a bomb (validate size first)
4. **CRC validation fails**: Make sure you're reading CRC from correct position
5. **Snappy fails**: Try both legacy (BE prefix) and NB (raw) formats

## In 30 Seconds

1. Read 4-byte prefix (LE for LZ4, BE for others)
2. Validate size <= 128MB
3. Decompress remaining bytes using appropriate algorithm
4. Verify decompressed size matches prefix (or chunk metadata)
5. Skip the 4-byte CRC at the end

Done.
