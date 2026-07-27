# Compression Formats in Cassandra SSTables

## Overview

Cassandra 5.0 supports **four compression algorithms plus Noop** (the explicit
"stored raw" marker):
- **LZ4** (`LZ4Compressor`) — default, recommended
- **Snappy** (`SnappyCompressor`)
- **Deflate** (`DeflateCompressor`) — highest compression, slowest
- **Zstd** (`ZstdCompressor`)
- **Noop** (`NoopCompressor`) — chunks stored uncompressed

> **Citation**: `cqlite-core/src/storage/sstable/compression_info.rs:43-48`
> (`SUPPORTED_COMPRESSOR_NAMES`). Any other compressor name is rejected fail-fast at
> metadata-parse time — never guessed from content (no-heuristics mandate, #28).

## Compression Block Structure

### Index Structure (CompressionInfo.db)

Located alongside Data.db. It is **NOT** an array of `[offset, length]` pairs — it is a
header followed by an offsets-only array:

```
writeUTF(compressor_simple_name)   // 2-byte BE length + UTF-8 bytes
i32  option_count                  // 4 bytes BE
for each option:
    writeUTF(key)                  // 2-byte BE length + UTF-8 bytes
    writeUTF(value)                // 2-byte BE length + UTF-8 bytes
i32  chunk_length                  // 4 bytes BE — UNCOMPRESSED chunk size
i32  max_compressed_length         // 4 bytes BE — present for version >= "na" (all 5.0 files)
i64  data_length                   // 8 bytes BE — total uncompressed data length
i32  chunk_count                   // 4 bytes BE
for each chunk:
    i64 chunk_offset               // 8 bytes BE — byte offset into Data.db
```

> **Citation**: `cqlite-core/src/storage/sstable/compression_info.rs:6-20` (format doc
> block) and `:83` (`chunk_offsets`), mirroring Cassandra
> `CompressionMetadata.java:375-392`.

**There is NO per-chunk length field.** A chunk's *physical* length is the delta between
consecutive offsets (for the last chunk, `file_len - last_offset`), and **that delta
INCLUDES the trailing 4-byte CRC** — so the compressed payload is
`next_offset - this_offset - 4` bytes. Cassandra's writer advances
`chunkOffset += compressedLength + 4` (`CompressedSequentialWriter.java:203`; see the
`chunk_offsets` doc comment at `compression_info.rs:76-83`).

### Chunk Format
Each compressed-chunk record in Data.db:
```
[compressed_data: (next_offset - this_offset - 4) bytes]
[crc32: 4 bytes big-endian, over the COMPRESSED bytes]
```

The inline CRC32 is **unconditional** for `na`/`nb` — it is always present and always
read. `crc_check_chance` governs only whether Cassandra *validates* it, never whether it
is written. CQLite always validates.

> **Citation**: `cqlite-core/src/storage/sstable/chunk_decompressor.rs:275-292` —
> `u32::from_be_bytes(crc_bytes)` compared against `crc32fast::hash(&compressed_data)`
> (the *compressed* buffer, not the decompressed one). Cassandra side:
> `CompressedSequentialWriter.java:192`. Big-endian byte order was verified against 356
> real Cassandra 5.0 inline trailers (#986 / #1086).

**Chunk Parameters** (from **`CompressionInfo.db`**, NOT `Statistics.db`):
- `chunk_length`: max uncompressed chunk size. **Cassandra 5.0 default is 16 KiB**
  (`CompressionParams.DEFAULT_CHUNK_LENGTH = 1024 * 16`, cassandra-5.0.8
  `src/java/org/apache/cassandra/schema/CompressionParams.java:47`).
- `max_compressed_length`: if a compressed chunk reaches this size the chunk was stored
  uncompressed instead; equals `i32::MAX` when `min_compress_ratio=0` (the default)
  (`compression_info.rs:70-75`).
- `compressor_simple_name`: one of the five names above.
- `crc_check_chance`: a *schema* option (probability Cassandra validates the inline CRC);
  it does NOT make the CRC optional on disk.

## LZ4 Compression

**Characteristics:**
- Very fast decompression
- Good compression ratio
- Default in Cassandra 5.0

**Wire layout**: Cassandra's `LZ4Compressor` prepends a **4-byte LITTLE-endian
uncompressed-length prefix** to a **raw LZ4 block** (this is NOT the `lz4_flex`
size-prepended/varint format).

**Rust Implementation** — this repo uses the **`lz4_flex`** crate. The bare `lz4` crate is
NOT a workspace dependency, so any sample calling its `block::decompress` API cannot
compile here.

```rust
// Cassandra LZ4Compressor.decompress() lines 169-172: 4-byte LE length prefix + raw block.
fn decompress_lz4(compressed: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    if compressed.len() < 4 {
        return Err(Error::InvalidFormat("LZ4 chunk too short for length prefix".into()));
    }
    let declared = u32::from_le_bytes([
        compressed[0], compressed[1], compressed[2], compressed[3],
    ]) as usize;
    // Fail fast when the prefix disagrees with the size CompressionInfo.db implies.
    if declared != expected_size {
        return Err(Error::InvalidFormat("LZ4 length prefix mismatch".into()));
    }
    lz4_flex::decompress(&compressed[4..], declared)
        .map_err(|e| Error::storage(format!("LZ4: {}", e)))
}
```

> **Citation**: `cqlite-core/src/storage/sstable/chunk_decompressor.rs:361`
> (`decompress_lz4_chunk`), `:391` (the `u32::from_le_bytes` prefix) and `:426`
> (`lz4_flex::decompress`). Dependency: `cqlite-core/Cargo.toml:39,145` (`lz4_flex`).

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
- **ZLIB-wrapped**, not raw DEFLATE

> ### ⚠️ TRAP — this is shipped P0 #1082 (deflate-as-zlib / zstd bare-frame)
> Cassandra's `DeflateCompressor` uses `java.util.zip.Deflater`/`Inflater`, which emit a
> **zlib-wrapped** stream: a 2-byte header (`0x78 0x9c`) + DEFLATE body + 4-byte Adler-32
> trailer. Decoding it with flate2's **raw-DEFLATE** reader is **exactly the P0 bug
> #1082** — you must use flate2's **zlib** reader, `ZlibDecoder`. There is also **no** 4-byte
> uncompressed-size prefix here — that is an LZ4/Zstd convention.

**Rust Implementation:**
```rust
use flate2::read::ZlibDecoder;
use std::io::Read;

fn decompress_deflate(compressed: &[u8], chunk_size_guard: u64) -> Result<Vec<u8>> {
    if compressed.is_empty() {
        return Err(Error::storage("Invalid Deflate data: empty chunk".into()));
    }
    // No in-stream size field exists for zlib, so bound the OUTPUT rather than
    // trusting the payload (decompression-bomb guard).
    let mut decoder = ZlibDecoder::new(compressed).take(chunk_size_guard + 1);
    let mut uncompressed = Vec::new();
    decoder.read_to_end(&mut uncompressed)
        .map_err(|e| Error::storage(format!("Deflate decompression failed: {}", e)))?;
    Ok(uncompressed)
}
```

> **Citation**: `cqlite-core/src/storage/sstable/compression.rs:306` (`use
> flate2::read::ZlibDecoder`), `:313` (the explicit `#1082` trap comment) and `:324`
> (`ZlibDecoder::new(data).take(...)`).

## Zstd Compression

**Characteristics:**
- Strong ratio at good speed
- Cassandra `ZstdCompressor`
- Bare zstd frame — decode via the streaming decoder with an output bound
  (`compression.rs`, Zstd arm); no separate 4-byte length prefix is added by CQLite's
  reader beyond what the frame itself carries.

## CRC Validation

The inline CRC32 is **unconditional** for `na`/`nb` (see *Chunk Format* above): 4 bytes
**big-endian**, computed over the **COMPRESSED** bytes. `crc_check_chance` is a schema
option about *validation* frequency, not presence.

```rust
// `compressed_data` is the chunk payload BEFORE decompression — never the decompressed buffer.
fn validate_chunk_crc(compressed_data: &[u8], crc_bytes: [u8; 4]) -> Result<()> {
    let stored_crc = u32::from_be_bytes(crc_bytes);
    let computed_crc = crc32fast::hash(compressed_data);
    if stored_crc != computed_crc {
        return Err(Error::InvalidFormat(format!(
            "CRC32 mismatch: stored=0x{:08x}, computed=0x{:08x}",
            stored_crc, computed_crc
        )));
    }
    Ok(())
}
```

> **Citation**: `cqlite-core/src/storage/sstable/chunk_decompressor.rs:275-292`.

## Reading Compressed Data

### High-Level Flow
1. Parse CompressionInfo.db to build chunk index
2. For target offset in Data.db:
   - Find chunk containing offset
   - Read compressed chunk
   - Decompress chunk
   - Extract data at relative offset within decompressed buffer

### Offset Calculation

Because chunks are **fixed-size in the UNCOMPRESSED domain** (`chunk_length`), the chunk
index is arithmetic — there is no per-chunk length to search:

```rust
// Logical (uncompressed) offset → chunk index. chunk_length comes from CompressionInfo.db.
fn chunk_index(logical_offset: u64, chunk_length: u32) -> usize {
    (logical_offset / chunk_length as u64) as usize
}

// Offset within the decompressed chunk buffer.
fn relative_offset(logical_offset: u64, chunk_length: u32) -> usize {
    (logical_offset % chunk_length as u64) as usize
}

// Physical payload length: the offset DELTA minus the 4-byte trailing CRC.
// There is no stored per-chunk length field.
fn compressed_payload_len(i: usize, offsets: &[u64], data_file_len: u64) -> u64 {
    let end = offsets.get(i + 1).copied().unwrap_or(data_file_len);
    end - offsets[i] - 4
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

The **schema-level** compression options (what `DESCRIBE TABLE` shows) look like:
```json
{
  "chunk_length_in_kb": 16,
  "class": "org.apache.cassandra.io.compress.LZ4Compressor",
  "crc_check_chance": 1.0,
  "compression_level": null
}
```

**Key Values:**
- `chunk_length_in_kb`: **default 16** in Cassandra 5.0
  (`CompressionParams.DEFAULT_CHUNK_LENGTH = 1024 * 16`, cassandra-5.0.8
  `schema/CompressionParams.java:47`); 32 and 64 are common overrides.
- `crc_check_chance`: 1.0 = always validate, 0.0 = never validate. The inline CRC is on
  disk regardless.
- `compression_level`: Deflate/Zstd-specific.

**On the READ path, do not source these from `Statistics.db`.** The values the decompressor
needs (`compressor_simple_name`, `chunk_length`, `max_compressed_length`, `data_length`,
`chunk_count`, offsets) are read from **`CompressionInfo.db`**
(`cqlite-core/src/storage/sstable/compression_info.rs:62-85`). The
`option_pairs` there carry any extra options the writer recorded.

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

Implementation (verify against `origin/main` — these are the authorities for what CQLite
actually does):
- `cqlite-core/src/storage/sstable/compression_info.rs` — `CompressionInfo.db` parser,
  `SUPPORTED_COMPRESSOR_NAMES`, offsets-only chunk array.
- `cqlite-core/src/storage/sstable/chunk_decompressor.rs` — chunk record read, inline
  big-endian CRC32 validation over compressed bytes, per-algorithm chunk decode.
- `cqlite-core/src/storage/sstable/compression.rs` — algorithm dispatch; the `#1082`
  Deflate-is-zlib trap comment lives here.

Format authority for genuinely disputed on-disk questions is Apache Cassandra 5.0.8
(`CompressionMetadata.java`, `CompressedSequentialWriter.java`, `CompressionParams.java`),
plus `docs/sstables-definitive-guide/` Ch.9.

