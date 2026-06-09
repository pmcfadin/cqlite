# Appendix G: Algorithm Reference Sheet

Quick lookup for implementing Cassandra 5.0 compression decompression.

## LZ4 Format

### In Data.db

```
Byte 0-3:   Uncompressed size (Little-Endian u32)
Byte 4+:    Compressed data
Byte N:     CRC checksum (4 bytes Big-Endian)
```

### Size Extraction (CRITICAL - LITTLE-ENDIAN)

```rust
let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
```

### Decompression

```rust
use lz4_flex::decompress;

let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
if size > 128 * 1024 * 1024 { return Err("Decompression bomb"); }

let decompressed = decompress(&data[4..], size)?;
assert_eq!(decompressed.len(), size);
Ok(decompressed)
```

### Cassandra Source

- **File**: [`LZ4Compressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/LZ4Compressor.java)
- **Method**: `uncompress()` (lines 136–165); `compress()` writes 4-byte LE prefix (lines 118–134)
- **Library**: `net.jpountz.lz4:lz4-java`

---

## Snappy Format

### Cassandra 5.0 NB (NewBinary) - NO SIZE PREFIX

```
Byte 0+:    Raw compressed data (no prefix)
Byte N:     CRC checksum (4 bytes Big-Endian)
```

### Legacy Format (pre-5.0)

Cassandra versions before 5.0 also used raw Snappy without a size prefix. The legacy-format fallback path is defensive only.

```
Byte 0+:    Raw compressed data (no prefix)
Byte N:     CRC checksum (4 bytes)
```

### Decompression (Dual Format)

```rust
use snap::raw::Decoder;

let mut decoder = Decoder::new();

// Try legacy format first (Big-Endian prefix)
if data.len() >= 4 {
    let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

    if size > 0 && size <= 128*1024*1024 {
        let compressed = &data[4..];
        if let Ok(result) = decoder.decompress_vec(compressed) {
            if result.len() == size {
                return Ok(result);
            }
        }
    }
}

// Fall back to raw Snappy (NB format)
let result = decoder.decompress_vec(data)?;
if result.len() > 128*1024*1024 {
    return Err("Decompression bomb");
}
Ok(result)
```

### Cassandra Source

- **File**: [`SnappyCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/SnappyCompressor.java)
- **Method**: `uncompress()` (lines 93–108) — no prefix, raw Snappy
- **Library**: `org.xerial.snappy:snappy-java`

---

## Deflate Format

### In Data.db

```
Byte 0+:    Raw Deflate compressed data (no length prefix)
Byte N:     CRC checksum (4 bytes)
```

**No size prefix.** `DeflateCompressor.compress()` passes raw deflated bytes directly with no length header.
Chunk bounds are determined entirely by offset differences in `CompressionInfo.db`.

### Decompression

```rust
use flate2::read::DeflateDecoder;
use std::io::Read;

// No size prefix — decompress entire chunk buffer
let mut decoder = DeflateDecoder::new(data);
let mut decompressed = Vec::new();
decoder.read_to_end(&mut decompressed)?;

if decompressed.len() > 128 * 1024 * 1024 {
    return Err("Decompression bomb");
}
Ok(decompressed)
```

### Cassandra Source

- **File**: [`DeflateCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/DeflateCompressor.java)
- **Method**: `compress()` / `uncompress()` — no length prefix
- **Library**: `java.util.zip.Inflater`

---

## Zstd Format

### In Data.db

```
Byte 0+:    Zstd frame (includes internal content checksum; no extra length prefix)
Byte N:     CRC checksum (4 bytes)
```

**No size prefix.** `ZstdCompressor.compress()` writes a raw Zstd frame with `ENABLE_CHECKSUM_FLAG = true`.
Chunk bounds are determined entirely by offset differences in `CompressionInfo.db`.

### Decompression

```rust
use zstd::stream::decode_all;

// No size prefix — decompress entire chunk buffer
let decompressed = decode_all(data)?;

if decompressed.len() > 128 * 1024 * 1024 {
    return Err("Decompression bomb");
}
Ok(decompressed)
```

### Cassandra Source

- **File**: [`ZstdCompressor.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/ZstdCompressor.java)
- **Method**: `compress()` / `uncompress()` — no length prefix; Zstd frame with internal checksum
- **Library**: `com.github.luben:zstd-jni`

---

## Byte Order Comparison

```
     LZ4        Snappy     Deflate    Zstd
     ---        ------     -------   ----
LE:  [0][1]..   (none)     (none)    (none)
     prefix     no prefix  no prefix no prefix
```

LZ4 is the only algorithm with a size prefix in Data.db.

Memory layout for first 4 bytes:

### LZ4 Little-Endian Example
```
Data:           [0x78, 0x56, 0x34, 0x12]
Value:          0x12345678
u32 decode:     from_le_bytes() = 0x12345678
```

### Deflate Big-Endian Example
```
Data:           [0x12, 0x34, 0x56, 0x78]
Value:          0x12345678
u32 decode:     from_be_bytes() = 0x12345678
```

---

## CRC Checksum

### Location

```
Chunk layout in Data.db:

offset 0:        [Compressed data starts]
offset N-4:      [Last byte of compressed data]
offset N:        [CRC byte 0]
offset N+1:      [CRC byte 1]
offset N+2:      [CRC byte 2]
offset N+3:      [CRC byte 3]

Where N = CompressionInfo.compressed_length
```

### Important

- `CompressionInfo.db` stores **only byte offsets** — no per-chunk CRCs and no per-chunk compressed lengths
- Compressed length is derived: `next_offset - current_offset - 4` (the 4 is the CRC word in `Data.db`)
- CRC is **NOT** passed to the decompressor
- CRC follows the compressed bytes in `Data.db` (IEEE CRC32, Java `java.util.zip.CRC32`)

### Calculation

```
// CompressionInfo.db gives only offsets, e.g.: offsets = [0, 1028, ...]
// Derive compressed_length from offsets:
compressed_length = offsets[1] - offsets[0] - 4  = 1028 - 0 - 4 = 1024
crc_offset = offsets[0] + compressed_length = 1024

// Data.db layout:
// offset 0:    1024 bytes compressed data
// offset 1024: 4-byte CRC32
// offset 1028: next chunk starts
```

---

## CompressionInfo.db Parsing

### Header

```rust
// Java writeUTF() = u16 BE length + UTF-8 bytes (no null terminator)
let mut pos = 0;

// Algorithm class name (Java writeUTF format)
let algo_len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize;
pos += 2;
let algo = String::from_utf8(data[pos..pos+algo_len].to_vec())?;
pos += algo_len;

// Option count (4 bytes BE)
let option_count = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;

// Options (repeated option_count times — each is a Java writeUTF string)
for _ in 0..option_count {
    let key_len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize; pos += 2;
    pos += key_len;
    let val_len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize; pos += 2;
    pos += val_len;
}

// Chunk length (4 bytes BE) — default 16384
let chunk_length = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;

// Max compressed length (4 bytes BE) — only present for format >= "na" (Cassandra 3.0+)
// if has_max_compressed_length:
let max_compressed_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;

// Data length (8 bytes BE) — total uncompressed file size
let data_length = u64::from_be_bytes([
    data[pos], data[pos+1], data[pos+2], data[pos+3],
    data[pos+4], data[pos+5], data[pos+6], data[pos+7]
]);
pos += 8;

// Chunk count (4 bytes BE)
let chunk_count = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;
```

### Chunks Array

```rust
// CompressionInfo.db stores ONLY offsets — one u64 per chunk
let mut offsets = Vec::with_capacity(chunk_count as usize);
for _ in 0..chunk_count {
    let offset = u64::from_be_bytes([
        data[pos], data[pos+1], data[pos+2], data[pos+3],
        data[pos+4], data[pos+5], data[pos+6], data[pos+7]
    ]);
    pos += 8;
    offsets.push(offset);
}

// Derive compressed length from consecutive offsets:
// compressed_len[i] = offsets[i+1] - offsets[i] - 4  (subtract 4-byte CRC in Data.db)
// For the last chunk: compressed_len = compressed_file_size - offsets[last] - 4
```

---

## Common Bugs and Fixes

### Bug 1: Using BE for LZ4

```rust
// WRONG - uses Big-Endian for LZ4 (which is Little-Endian)
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

// CORRECT - uses Little-Endian for LZ4
let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
```

### Bug 2: Assuming All Algorithms Have a Size Prefix

```rust
// WRONG - Deflate and Zstd have NO prefix; this reads garbage
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
let result = deflate_decode(&data[4..])?;  // skips 4 real data bytes!

// CORRECT - no prefix for Deflate, Zstd, or Snappy (Cassandra 5.0)
let result = deflate_decode(data)?;
let result = zstd_decode(data)?;
let result = raw_snappy_decompress(data)?;
```

### Bug 3: Including CRC in Decompression

```rust
// WRONG - passes trailing CRC bytes to decompressor
let result = decompress(data)?  // data still includes 4-byte trailing CRC

// CORRECT - use derived compressed_len (excludes CRC)
// compressed_len = (next_offset - current_offset) - 4
let result = decompress(&data[..compressed_len])?;
let crc = u32::from_be_bytes(data[compressed_len..compressed_len+4].try_into()?);
```

### Bug 4: Not Validating Decompressed Size

```rust
// WRONG - no bomb protection
let result = decompress(data)?;

// CORRECT for prefix-less algorithms (Snappy, Deflate, Zstd):
let result = decompress(data)?;
if result.len() > 128 * 1024 * 1024 { return Err("Bomb"); }

// For LZ4 (has LE prefix): also validate before decompression
let size = u32::from_le_bytes(data[0..4].try_into()?);
if size > 128 * 1024 * 1024 { return Err("Bomb (pre-check)"); }
let result = lz4_decompress(&data[4..], size)?;
```

---

## Testing Template

```rust
#[test]
fn test_compression_format(algorithm: &str, data_file: &str) {
    // 1. Read compressed data from Data.db at chunk offset
    let compressed_data = std::fs::read(data_file).unwrap();

    // 2. Parse CompressionInfo.db to get metadata
    let metadata = parse_compression_info(&format!("{}-CompressionInfo.db", data_file)).unwrap();

    // 3. Decompress using algorithm-specific method
    let decompressed = match algorithm {
        "LZ4" => decompress_lz4(&compressed_data[..metadata.chunks[0].compressed_length as usize]),
        "SNAPPY" => decompress_snappy(&compressed_data[..metadata.chunks[0].compressed_length as usize]),
        "DEFLATE" => decompress_deflate(&compressed_data[..metadata.chunks[0].compressed_length as usize]),
        "ZSTD" => decompress_zstd(&compressed_data[..metadata.chunks[0].compressed_length as usize]),
        _ => panic!("Unknown algorithm"),
    }.unwrap();

    // 4. Validate size
    assert_eq!(decompressed.len() as u32, metadata.chunks[0].uncompressed_length);

    // 5. Compare against reference (sstabledump)
    let reference = std::fs::read("sstabledump-output.txt").unwrap();
    // (comparison logic)
}
```

---

## See Also

- Full specification: appendix-g-compression-chunk-formats.md
- Quick reference: appendix-g-quick-reference.md
- Research notes: /docs/archive/issues/COMPRESSION_CHUNK_FORMAT_RESEARCH.md
- CQLite source: `/cqlite-core/src/storage/sstable/compression.rs`
- [`CompressionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java)
- [`schema/CompressionParams.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) — `DEFAULT_CHUNK_LENGTH = 1024 * 16` (line 47)
- [`BigFormat.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java) — `hasMaxCompressedLength` gate (line 401)
