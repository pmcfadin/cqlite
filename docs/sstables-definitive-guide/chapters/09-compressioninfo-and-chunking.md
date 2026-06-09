## CompressionInfo.db and Chunking

Explore compression algorithms, chunk sizes, offset maps, and checksums in `CompressionInfo.db`, and how chunking impacts random vs sequential IO.

### In this chapter you will learn
- What `CompressionInfo.db` contains and how it’s used
- How chunk size choices influence performance trade-offs
- How checksums are validated per chunk
- How tooling exposes chunk maps

## Compression Metadata

`CompressionInfo.db` contains algorithm class name, option count, option key-value pairs, chunk length, max compressed length (SSTable format version "na" / Cassandra 3.0+ only), total uncompressed data length, chunk count, and chunk offsets. Per-chunk CRC32 checksums live inline in `Data.db`, written immediately after each compressed chunk — `CompressionInfo.db` holds no per-chunk CRCs.

For a concise parser walkthrough, see Appendix C.

## Chunk Size Trade-offs

- Smaller chunks improve random-read locality but add metadata overhead and decompression CPU.
- Larger chunks reduce overhead and improve scans, but increase random-read amplification.

## Checksums

Per-chunk CRC32 checksums are appended inline in `Data.db` after each compressed chunk; readers enforce them for Cassandra 5.0 formats. `Digest.crc32` covers component-level integrity at a coarse level; per-chunk CRCs catch localized corruption within a chunk. `CompressionInfo.db` does not store per-chunk CRCs.

Readers enforce size and CRC expectations for modern formats. For decompressor details, see Appendix C.

## NB Format: Chunking Without Headers (Cassandra 4.x/5.x)

The "nb" (new big) format introduces a header-less Data.db structure that relies entirely on CompressionInfo.db for chunk navigation.

### Data.db Structure

**Key difference:** NB format Data.db has **no magic number or global header**. The file starts directly with compressed data:

```
Offset 0: [chunk_0_compressed_bytes]
          [crc32_chunk_0: 4 bytes, big-endian]
          [chunk_1_compressed_bytes]
          [crc32_chunk_1: 4 bytes, big-endian]
          ...
```

**Format identification:** The "nb" identifier appears only in the filename (e.g., `nb-1-big-Data.db`), not in file content.

### CompressionInfo.db Format (serialization exactness)

The compression metadata file encodes, in serialization order (`CompressionMetadata.Writer.writeHeader()`):

1. **Compressor class name** — UTF-8 string (2-byte length prefix + bytes), e.g., `"LZ4Compressor"`
2. **Option count** — 4-byte int: number of key-value option pairs
3. **Options** — repeated UTF-8 key + UTF-8 value pairs (each with 2-byte length prefix)
4. **Chunk length** — 4-byte int: uncompressed chunk size (default 16 KiB = 16 384 bytes)
5. **Max compressed length** — 4-byte int: present only for SSTable format version ≥ "na" (Cassandra 3.0+)
6. **Data length** — 8-byte long: total uncompressed file size
7. **Chunk count** — 4-byte int: number of chunks
8. **Chunk offsets** — array of 8-byte longs: byte offset of each chunk in `Data.db`

Authoritative sources:
- `org.apache.cassandra.io.compress.CompressionMetadata` (reader / writer)
- `org.apache.cassandra.schema.CompressionParams` (parameters and defaults; in `schema/`, not `io/compress/`)

Authoritative example (first 64 bytes from a real file):

```
00000000: 000d 4c5a 3443 6f6d 7072 6573 736f 7200  ..LZ4Compressor.
00000010: 0000 0000 0040 007f ffff ff00 0000 0000  .....@..........
00000020: 001e fe00 0000 0100 0000 0000 0000 00    ...............
```

Interpretation (trimmed):
- `000d` → length 13, followed by `LZ4Compressor` (UTF-8)
- `0040` → chunk length field; raw value 0x00004000 = 16 384 bytes = 16 KiB (the default)
- `007f ffff ff00 0000 0000` → total uncompressed length (u64 example)
- subsequent bytes begin the chunk map

Note: Older materials often describe the chunk map as "varint pairs"; Cassandra 5.0 uses fixed-width fields for several header values and format-dependent encodings for the map. Always consult the pinned source for exact widths.

Exact widths (NB, Cassandra 5.0):

| Field | Type/width | Endianness | Notes |
|---|---|---|---|
| compressor_name_length | u16 | big | length prefix of class name (Java `writeUTF`) |
| compressor_name | UTF-8 bytes | — | e.g., `LZ4Compressor`, `SnappyCompressor` |
| option_count | u32 | big | number of key-value option pairs |
| option_key[i] | UTF-8 string | — | repeated `option_count` times |
| option_value[i] | UTF-8 string | — | repeated `option_count` times |
| chunk_length | u32 | big | uncompressed bytes per chunk; default 16 384 (16 KiB) |
| max_compressed_length | u32 | big | present only for format version ≥ "na" (3.0+) |
| total_uncompressed_length | u64 | big | table payload size before compression |
| chunk_count | u32 | big | number of chunks |
| chunk_offsets[chunk_count] | u64 each | big | byte offset of each compressed chunk in `Data.db` |

Map encoding by format:
- NB (5.0): offsets only; per-chunk compressed length = next_offset − offset − 4 (subtract trailing CRC word in `Data.db`)
- Legacy variants: may differ; this guide focuses on NB; consult sources when targeting older formats

Chunk map (first two entries, decoded — units: bytes, endianness: big):

From `test_timeseries/event_store`:

| Entry | Offset | Length |
|-------|--------|--------|
| 0     | 0x0000 | 7,729  |
| 1     | 0x1e35 | 2,666  |

Invariants:
- Offsets are strictly increasing; lengths are positive; last chunk may be ≤ `chunk length`.

NB CRC micro-proof (same file):
```
chunk 0: start=0x0000 comp_len=7729 expected=0x001daf10 computed=0x001daf10 match=true
chunk 1: start=0x1e35 comp_len=2666 expected=0x657f7155 computed=0x657f7155 match=true
```

### Reading NB Format Files

**Required sequence:**
1. Parse `CompressionInfo.db` to get chunk map
2. For each chunk:
   - Seek to `offset` in Data.db
   - Read `length` bytes (compressed data)
   - Read next 4 bytes as CRC32 (big-endian u32)
   - Validate: compute CRC32 over compressed bytes
   - Decompress chunk
   - Parse row data from decompressed bytes

### CRC32 Algorithm

- **Implementation:** Java `java.util.zip.CRC32`
- **Polynomial:** IEEE 0x04C11DB7 (reversed: 0xEDB88320)
- **Byte order:** Big-endian
- **Scope:** Compressed chunk bytes only (not including trailing CRC)
- **Position:** Immediately after each chunk (trailing, not leading)

### Common Pitfalls

- **Don't assume Data.db has a header** - it doesn't in NB format
- **Don't treat first 4 bytes as magic number** - they're chunk data
- **Don't treat first 4 bytes as CRC prefix** - CRCs are trailing
- **Don't try to read blocks without CompressionInfo.db** - you'll read garbage sizes

### Key Takeaways
- `CompressionInfo.db` maps chunks and validates integrity for modern formats.
- Chunk length is central to random vs scan performance; choose based on workload.
- Readers must pair `CompressionInfo.db` with `Data.db` to read the right byte ranges.

### References

- `CompressionMetadata` (reader / writer): [io/compress/CompressionMetadata.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java) — `open()` lines 76–112, `writeHeader()` lines 375–398
- `CompressionParams` (defaults): [schema/CompressionParams.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) — `DEFAULT_CHUNK_LENGTH` line 47 (note: `schema/`, not `io/compress/`)
- `CompressedSequentialWriter` (chunk write + CRC): [io/compress/CompressedSequentialWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java) — `flushData()` lines 140–206
- `BigFormat` (version gate): [io/sstable/format/big/BigFormat.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java) — `hasMaxCompressedLength` line 401

For implementation details, see Appendix C.


