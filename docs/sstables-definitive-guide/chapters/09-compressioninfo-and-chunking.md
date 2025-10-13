## CompressionInfo.db and Chunking

Explore compression algorithms, chunk sizes, offset maps, and checksums in `CompressionInfo.db`, and how chunking impacts random vs sequential IO.

### In this chapter you will learn
- What `CompressionInfo.db` contains and how it’s used
- How chunk size choices influence performance trade-offs
- How checksums are validated per chunk
- How tooling exposes chunk maps

## Compression Metadata

`CompressionInfo.db` contains algorithm name, chunk length, total uncompressed length, chunk offsets, and optionally per-chunk CRCs and a metadata CRC.

For a concise parser walkthrough, see Appendix C.

## Chunk Size Trade-offs

- Smaller chunks improve random-read locality but add metadata overhead and decompression CPU.
- Larger chunks reduce overhead and improve scans, but increase random-read amplification.

## Checksums

Modern formats can record per-chunk CRCs and a metadata CRC; readers enforce them for Cassandra 5.0 formats. Digest files (`Digest.crc32`) cover component integrity at a coarse level; per-chunk CRCs catch localized corruption.

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

The compression metadata file encodes:
- compressor class name (UTF-8 string)
- chunk length (u32)
- total uncompressed length (u64)
- chunk map (offset/length pairs)

Exact field order and widths are defined in Cassandra 5.0 by `CompressionMetadata` and friends. See:
- `org.apache.cassandra.io.compress.CompressionMetadata` (reader)
- `org.apache.cassandra.io.compress.CompressionParams` (parameters)

Authoritative example (first 64 bytes from a real file):

```
00000000: 000d 4c5a 3443 6f6d 7072 6573 736f 7200  ..LZ4Compressor.
00000010: 0000 0000 0040 007f ffff ff00 0000 0000  .....@..........
00000020: 001e fe00 0000 0100 0000 0000 0000 00    ...............
```

Interpretation (trimmed):
- `000d` → length 13, followed by `LZ4Compressor` (UTF-8)
- `0040` → chunk length 64 KiB (example)
- `007f ffff ff00 0000 0000` → total uncompressed length (u64 example)
- subsequent bytes begin the chunk map

Note: Older materials often describe the chunk map as "varint pairs"; Cassandra 5.0 uses fixed-width fields for several header values and format-dependent encodings for the map. Always consult the pinned source for exact widths.

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
-- Cassandra 5.0.0:
  - `CompressionMetadata`: [org.apache/cassandra/io/compress/CompressionMetadata.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java)
  - `CompressionParams`: [org/apache/cassandra/io/compress/CompressionParams.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionParams.java)

For implementation details, see Appendix C.


