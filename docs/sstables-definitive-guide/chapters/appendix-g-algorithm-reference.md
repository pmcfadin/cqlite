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

- **File**: LZ4Compressor.java
- **Method**: uncompress (lines 136-165)
- **Library**: jpountz LZ4

---

## Snappy Format

### Cassandra 5.0 NB (NewBinary) - NO SIZE PREFIX

```
Byte 0+:    Raw compressed data (no prefix)
Byte N:     CRC checksum (4 bytes Big-Endian)
```

### Legacy Format (pre-5.0) - HAS SIZE PREFIX

```
Byte 0-3:   Uncompressed size (Big-Endian u32)
Byte 4+:    Compressed data
Byte N:     CRC checksum (4 bytes Big-Endian)
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

- **File**: SnappyCompressor.java
- **Method**: uncompress (lines 93-108)
- **Library**: xerial Snappy

---

## Deflate Format

### In Data.db

```
Byte 0-3:   Uncompressed size (Big-Endian u32)
Byte 4+:    Deflate compressed data
Byte N:     CRC checksum (4 bytes Big-Endian)
```

### Size Extraction (BIG-ENDIAN)

```rust
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
```

### Decompression

```rust
use flate2::read::DeflateDecoder;
use std::io::Read;

let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
if size > 128 * 1024 * 1024 { return Err("Decompression bomb"); }

let compressed = &data[4..];
let mut decoder = DeflateDecoder::new(compressed);
let mut decompressed = Vec::new();
decoder.read_to_end(&mut decompressed)?;

if decompressed.len() != size {
    return Err("Size mismatch");
}
Ok(decompressed)
```

### Cassandra Source

- **File**: DeflateCompressor.java
- **Method**: uncompress (lines 199-221)
- **Library**: Java util.zip.Inflater

---

## Zstd Format

### In Data.db

```
Byte 0-3:   Uncompressed size (Big-Endian u32)
Byte 4+:    Zstd compressed data (frame format)
Byte N:     CRC checksum (4 bytes Big-Endian)
```

### Size Extraction (BIG-ENDIAN)

```rust
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
```

### Decompression

```rust
use zstd::stream::decode_all;

let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
if size > 128 * 1024 * 1024 { return Err("Decompression bomb"); }

let compressed = &data[4..];
let decompressed = decode_all(compressed)?;

if decompressed.len() != size {
    return Err("Size mismatch");
}
Ok(decompressed)
```

### Cassandra Source

- **File**: ZstdCompressorBase.java
- **Method**: uncompress (lines 107-126)
- **Library**: luben zstd-jni

---

## Byte Order Comparison

```
     LZ4        Deflate      Zstd       Snappy
     ---        -------      ----       ------
LE:  [0][1]..   (no)         (no)       (no)
BE:  (no)       [0][1]..     [0][1]..   [0][1].. (legacy only)
```

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

- CRC is **NOT** included in CompressionInfo.compressed_length
- CRC is **NOT** passed to decompressor
- CRC byte order is Big-Endian

### Calculation

```
chunk_offset (from CompressionInfo) = 0
compressed_length (from CompressionInfo) = 1024
crc_offset = chunk_offset + compressed_length = 1024

Next chunk offset = 1024 + 4 (CRC) = 1028
```

---

## CompressionInfo.db Parsing

### Header

```rust
// At offset 0
let mut pos = 0;

// Algorithm name length (2 bytes BE)
let algo_len = u16::from_be_bytes([data[pos], data[pos+1]]);
pos += 2;

// Algorithm name (UTF-8)
let algo = String::from_utf8(data[pos..pos+algo_len].to_vec())?;
pos += algo_len;

// Null terminator
if data[pos] == 0 { pos += 1; }

// Chunk length (4 bytes BE)
let chunk_length = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;

// Data length (8 bytes BE)
let data_length = u64::from_be_bytes([
    data[pos], data[pos+1], data[pos+2], data[pos+3],
    data[pos+4], data[pos+5], data[pos+6], data[pos+7]
]);
pos += 8;

// Number of chunks (4 bytes BE)
let chunk_count = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
pos += 4;
```

### Chunks Array

```rust
// For each chunk:
for i in 0..chunk_count {
    // Chunk offset (8 bytes BE)
    let chunk_offset = u64::from_be_bytes([
        data[pos], data[pos+1], data[pos+2], data[pos+3],
        data[pos+4], data[pos+5], data[pos+6], data[pos+7]
    ]);
    pos += 8;

    // Compressed length (4 bytes BE)
    let compressed_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
    pos += 4;

    // Uncompressed length (4 bytes BE)
    let uncompressed_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
    pos += 4;
}
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

### Bug 2: Forgetting Snappy NB Format

```rust
// WRONG - assumes size prefix always exists
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
let result = snappy::decompress(&data[4..])?;

// CORRECT - tries both formats
if let Ok(result) = try_legacy_format(data) {
    return Ok(result);
}
raw_snappy_decompress(data)
```

### Bug 3: Including CRC in Decompression

```rust
// WRONG - passes CRC to decompressor
let result = decompress(&data[4..])? // includes CRC

// CORRECT - stop before CRC
let result = decompress(&data[4..chunk_size])?
let crc_offset = chunk_size;
```

### Bug 4: Not Validating Decompressed Size

```rust
// WRONG - trusts the prefix blindly
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
let result = decompress(&data[4..])?;

// CORRECT - validates both before and after
let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
if size > 128*1024*1024 { return Err("Bomb"); }
let result = decompress(&data[4..])?;
if result.len() != size { return Err("Size mismatch"); }
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
- CQLite source: /cqlite-core/src/storage/sstable/compression.rs
