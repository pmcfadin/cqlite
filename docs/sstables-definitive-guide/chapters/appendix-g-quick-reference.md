# Appendix G Quick Reference - Compression Formats at a Glance

## One-Minute Summary

Cassandra 5.0 chunks data and compresses independently. Each algorithm wraps compressed data differently:

| Algorithm | Size Prefix in Data.db | Order | Notes |
|-----------|------------------------|-------|-------|
| **LZ4** | 4 bytes | **LE** | Most common; LE prefix is critical! |
| **Snappy** | None | — | Raw Snappy frame |
| **Deflate** | None | — | Raw Deflate stream (no prefix) |
| **Zstd** | None | — | Zstd frame with internal content checksum |

All chunks end with: `[4-byte BE CRC]`

## Byte Swaps (Critical!)

```rust
// LZ4 ONLY — 4-byte Little-Endian uncompressed-length prefix
let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
let compressed = &data[4..];

// Snappy, Deflate, Zstd — NO PREFIX; decompress the whole chunk buffer
// (Deflate and Zstd have no length header at all)
```

## CompressionInfo.db Reading

```rust
// Binary layout (Java writeUTF = u16-BE-length-prefix + UTF-8 bytes):
[writeUTF] algorithm_name        // e.g., "LZ4Compressor"
[u32 BE]   option_count          // number of key-value pairs (0 for most)
// repeat option_count times:
[writeUTF] option_key
[writeUTF] option_value
[u32 BE]   chunk_length          // uncompressed chunk size; default 16384 (16 KiB)
[u32 BE]   max_compressed_length // ONLY for format version >= "na" (Cassandra 3.0+)
[u64 BE]   data_length           // total uncompressed size
[u32 BE]   chunk_count

// Then for each chunk (offsets only — no per-chunk lengths or CRCs):
[u64 BE]   offset                // byte offset in Data.db
// compressed_len = next_offset - offset - 4 (subtract trailing CRC)
```

## Finding a Chunk in Data.db

```rust
// Given position in uncompressed file:
chunk_index = position / chunk_length

// Offsets array from CompressionInfo.db (no per-chunk lengths stored):
offset = chunk_offsets[chunk_index]
next_offset = chunk_offsets[chunk_index + 1]  // or compressed_file_size for last
compressed_len = (next_offset - offset) - 4   // subtract 4-byte trailing CRC

// Read from Data.db:
chunk_data = read_from(offset, compressed_len)
crc = read_u32_be_from(offset + compressed_len)  // validate or skip
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
            // No prefix — decompress entire chunk buffer
            let result = deflate_decode(data)?;
            assert!(result.len() <= 128 * 1024 * 1024); // Bomb check post-decompress
            Ok(result)
        }
        "ZSTD" => {
            // No prefix — decompress entire chunk buffer
            let result = zstd_decode(data)?;
            assert!(result.len() <= 128 * 1024 * 1024); // Bomb check post-decompress
            Ok(result)
        }
        _ => Err("Unknown algorithm")
    }
}
```

## Common Mistakes

1. **Using BE for LZ4**: LZ4 uses **little-endian**, not big-endian
   - `u32::from_le_bytes()` not `from_be_bytes()`

2. **Assuming Deflate/Zstd have a size prefix**: Neither algorithm has a length prefix in Data.db
   - Decompress the entire chunk buffer directly; chunk bounds come from CompressionInfo.db offsets

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
"NoopCompressor"       -> normalize to "NOOP"
```

## Files to Reference

- **Full spec**: `/docs/sstables-definitive-guide/chapters/appendix-g-compression-chunk-formats.md`
- **Research notes**: `/docs/archive/issues/COMPRESSION_CHUNK_FORMAT_RESEARCH.md`
- **CQLite code**: `/cqlite-core/src/storage/sstable/compression.rs`
- **Cassandra source**: `apache/cassandra:5.0.8` in `/src/java/org/apache/cassandra/io/compress/`
  - [`CompressionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java)
  - [`CompressedSequentialWriter.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java)
  - [`schema/CompressionParams.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) (note: `schema/`, not `io/compress/`)

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

1. For LZ4: read 4-byte LE prefix; for Snappy/Deflate/Zstd: no prefix — use all chunk bytes
2. For LZ4: validate prefix size <= 128MB before decompressing
3. Decompress using appropriate algorithm (LZ4: skip 4-byte prefix)
4. Validate decompressed size <= 128MB (all algorithms)
5. The 4-byte CRC follows the compressed bytes in Data.db; validate or skip it

Done.
