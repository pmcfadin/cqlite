## CompressionInfo.db and Chunking

Explore compression algorithms, chunk sizes, offset maps, and checksums in `CompressionInfo.db`, and how chunking impacts random vs sequential IO.

### In this chapter you will learn
- What `CompressionInfo.db` contains and how it’s used
- How chunk size choices influence performance trade-offs
- How checksums are validated per chunk
- How `max_compressed_length` selects between compressed and raw chunk storage
- Why a compressed SSTable can carry a trailing chunk that holds no data
- How Cassandra 5.0 coalesces chunk reads for scans, and when that optimization silently disappears
- How tooling exposes chunk maps

## Compression Metadata

`CompressionInfo.db` contains algorithm class name, option count, option key-value pairs, chunk
length, max compressed length, total uncompressed data length, chunk count, and chunk offsets.
The `max_compressed_length` field is gated on SSTable format version ≥ `na`, so it is present in
every format this guide covers (`na`/`nb` BIG, `oa`/`da` BTI). Per-chunk CRC32 checksums live
inline in `Data.db`, written immediately after each compressed chunk — `CompressionInfo.db` holds
no per-chunk CRCs and no trailing metadata checksum.

For a concise parser walkthrough, see Appendix C.

## Chunk Size Trade-offs

- Smaller chunks improve random-read locality but add metadata overhead and decompression CPU.
- Larger chunks reduce overhead and improve scans, but increase random-read amplification.

The server default is **16 KiB** (`CompressionParams.java:47`:
`DEFAULT_CHUNK_LENGTH = 1024 * 16`). 64 KiB is a common tuning choice, not a default, and it is a
frequent source of wrong assumptions in reader implementations. Chunk length must always be read
from `chunk_length` in `CompressionInfo.db` for the SSTable at hand — never hardcoded and never
inferred (issues #2877, #28). Cassandra also requires `chunk_length` to be a power of two, because
the chunk index is computed as `position / chunkLength` (`CompressionParams.validate()`, lines
443–448). Ch. 3 covers the sizing trade-off in operational terms.

## Scan readahead vs. per-chunk reads

A compressed reader that issues one positional read per chunk performs one syscall (or one page
fault) per `chunk_length` of logical data — at the 16 KiB default that is a poor request size for a
sequential scan. Cassandra 5.0 addresses this with a **scan-specific reader**, not with `madvise`:

- `ChunkReader.instantiateRebufferer(boolean isScan)` carries the read intent down from the caller.
  `CompressedChunkReader.instantiateRebufferer()` (line 93) resolves it as
  `new BufferManagingRebufferer.Aligned(isScan ? forScan() : this)`.
- `CompressedChunkReader.Standard` (buffered path) builds a `ScanCompressedReader` backed by a
  `ThreadLocalReadAheadBuffer`, and `forScan()` (line 241) activates it. Chunk reads are then
  served from that userspace buffer, so consecutive chunks share one large read instead of one read
  each (`ScanCompressedReader.read()`, lines 169–186).
- Buffer size comes from `compressed_read_ahead_buffer_size`, default **256 KiB**
  (`Config.java:341`). The scan reader is only constructed when that value is **greater than**
  `chunk_length` (`CompressedChunkReader.java:236–238`) — with a 256 KiB buffer and 16 KiB chunks it
  coalesces ~16 chunks per read; set the buffer at or below `chunk_length` and the optimization is
  silently absent.
- Shipped in **Cassandra 5.0.4** as CASSANDRA-15452 ("Improve disk access patterns during
  compaction and range reads", `CHANGES.txt:122`).

> **Warning: two ways the scan reader silently does not engage**
>
> 1. **ChunkCache wrapping.** `FileHandle.Builder.maybeCached()` (lines 444–448) wraps the chunk
>    reader in `ChunkCache` when the cache exists with non-zero capacity, and
>    `ChunkCache.CachingRebufferer.instantiateRebufferer(boolean isScan)`
>    (`ChunkCache.java:262–265`) **ignores `isScan` and returns `this`** — `forScan()` is never
>    called, so scans fall back to one read per chunk. Note the precondition: `ChunkCache.instance`
>    is null unless `file_cache_enabled` is true, and that setting **defaults to `false`**
>    (`Config.java:499`, `cassandra.yaml:742`). So on a default 5.0 install the trap is not armed;
>    it arms the moment an operator enables the file cache.
> 2. **mmap.** `CompressedChunkReader.Mmap` (line 321) does not override `forScan()`, so the base
>    implementation returns `this` (line 52). An mmap-backed compressed read gets no userspace scan
>    readahead at all and depends entirely on kernel readahead over the mapping. With Cassandra
>    5.0's `mmap_index_only` default, `Data.db` is buffered, so the `Standard` path (which does
>    support scan readahead) is the one normally in play — see Ch. 3.
>
> Implementers taking either lesson across: a decompressed-chunk cache and a read-coalescing layer
> must be co-designed. If a cache layer swallows the scan intent, coalescing never runs.

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
5. **Max compressed length** — 4-byte int: gated on SSTable format version ≥ `na`, so present for
   every format in scope; `Integer.MAX_VALUE` (`0x7fffffff`) with the default `min_compress_ratio = 0`
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

Interpretation (byte-exact, from `system/compaction_history` `nb-1-big-CompressionInfo.db`):

| Byte range | Bytes | Field | Value |
|---|---|---|---|
| 0x00–0x01 | `000d` | `writeUTF` length | 13 |
| 0x02–0x0e | `4c5a…736f72` | compressor name | `LZ4Compressor` |
| 0x0f–0x12 | `00000000` | option count | 0 |
| 0x13–0x16 | `00004000` | chunk length | 16 384 bytes = 16 KiB (the default) |
| 0x17–0x1a | `7fffffff` | max compressed length | `Integer.MAX_VALUE` (`min_compress_ratio = 0`) |
| 0x1b–0x22 | `0000000000001efe` | total uncompressed length | 7 934 bytes |
| 0x23–0x26 | `00000001` | chunk count | 1 |
| 0x27–0x2e | `0000000000000000` | chunk offset[0] | 0 |

The file ends after the last chunk offset: 47 bytes total for this single-chunk example. There is
no trailing metadata checksum in `CompressionInfo.db` (`CompressionMetadata.Writer.writeHeader()`
writes the header and offsets only).

Note: Older materials often describe the chunk map as "varint pairs". It is not: every field in
`CompressionInfo.db` is fixed-width big-endian, and the map is a flat `u64` offset array. Always
consult the pinned source for exact widths.

Exact widths (NB, Cassandra 5.0):

| Field | Type/width | Endianness | Notes |
|---|---|---|---|
| compressor_name_length | u16 | big | length prefix of class name (Java `writeUTF`) |
| compressor_name | UTF-8 bytes | — | e.g., `LZ4Compressor`, `SnappyCompressor` |
| option_count | u32 | big | number of key-value option pairs |
| option_key[i] | UTF-8 string | — | repeated `option_count` times |
| option_value[i] | UTF-8 string | — | repeated `option_count` times |
| chunk_length | u32 | big | uncompressed bytes per chunk; default 16 384 (16 KiB) |
| max_compressed_length | u32 | big | version-gated at ≥ `na`, so always present in scope; `0x7fffffff` by default; a stored chunk of this length or longer is raw, not compressed |
| total_uncompressed_length | u64 | big | table payload size before compression |
| chunk_count | u32 | big | number of chunks |
| chunk_offsets[chunk_count] | u64 each | big | byte offset of each compressed chunk in `Data.db` |

Map encoding:
- NB/NA (Cassandra 5.0 BIG): offsets only; per-chunk compressed length = next_offset − offset − 4
  (subtract the trailing CRC word stored inline in `Data.db`). For the last chunk, substitute the
  `Data.db` file length for `next_offset`: `compressedFileLength − offset − 4`
  (`CompressionMetadata.chunkFor()`, line 252: `new Chunk(chunkOffset, (int)(nextChunkOffset - chunkOffset - 4))`).
- `oa`/`da` BTI SSTables use the same `CompressionInfo.db` serialization — the chunk map is a
  property of the compression layer, not of the index format.

### `max_compressed_length` and the incompressible-chunk fallback

`max_compressed_length` is not a cosmetic field: it is the switch that decides whether a stored
chunk is compressed at all.

- **Writer** (`CompressedSequentialWriter.flushData()`, lines 159–178): if
  `compressedLength >= maxCompressedLength`, the writer discards the compressed output and stores
  the **raw** bytes instead. When the raw length is itself below `maxCompressedLength` (only
  possible for the final short chunk) it zero-pads up to `maxCompressedLength`; readers avoid
  reading the padding because they bound the chunk by `data_length`.
- **Reader**: a chunk whose stored length is `>= max_compressed_length` is returned as-is with no
  decompression. There is no flag byte — the length comparison *is* the encoding.
- **Default value**: `Integer.MAX_VALUE` (`0x7fffffff`). Cassandra derives it from
  `min_compress_ratio`, whose default is `0.0`
  (`CompressionParams.java:47–48`, `calcMaxCompressedLength()` at line 186), and
  `calcMinCompressRatio()` maps `Integer.MAX_VALUE` back to ratio 0 (line 198). With the default,
  no chunk can ever reach `max_compressed_length`, so the raw path is effectively disabled.

> **Warning: `max_compressed_length == 0` is corrupt metadata and fails OPEN**
>
> A zero `max_compressed_length` makes the reader's `stored_length >= max_compressed_length`
> test true for *every* chunk, so a reader silently hands back still-compressed bytes as if they
> were plaintext instead of raising an error. The per-chunk CRC32 cannot catch this: the CRC is
> computed over the genuinely-on-disk compressed bytes and matches.
>
> Cassandra never emits this value — but note that `CompressionParams.validate()`
> (lines 441–455) does **not** reject it either: it rejects `maxCompressedLength < 0` and
> `chunkLength < maxCompressedLength < Integer.MAX_VALUE`, and `0` passes both tests. Zero is
> excluded by construction (the default is `Integer.MAX_VALUE`), not by a validation rule, so a
> reader must treat it as a corruption case rather than assuming it cannot occur.
>
> **Reader requirement**: reject `max_compressed_length == 0` when parsing `CompressionInfo.db`,
> before any chunk read, so no downstream consumer can observe an unguarded zero. CQLite fails
> **closed** at parse time with a typed error
> (`cqlite-core/src/storage/sstable/compression_info.rs:226–230`,
> `Error::InvalidFormat("max_compressed_length cannot be zero")`); the same guard is repeated in
> the point-read seek paths (`reader/data_access/compressed_offset.rs:107`,
> `reader/data_access/big_promoted.rs:681`). Issues #2524, #2529.

Chunk map (first two entries, decoded — units: bytes, endianness: big):

From `test_timeseries/event_store`:

| Entry | Offset | Length |
|-------|--------|--------|
| 0     | 0x0000 | 7,729  |
| 1     | 0x1e35 | 2,666  |

Invariants:
- Offsets are strictly increasing.
- Every data-bearing chunk decompresses to `min(chunk_length, data_length − i * chunk_length)`
  bytes, so the last data-bearing chunk may be shorter than `chunk_length`.
- A stored compressed length of 0 is possible **only** for a degenerate trailing chunk whose
  logical start is already at/beyond `data_length` (see the warning below); no data-bearing chunk
  has length 0.
- `chunk_count` may exceed `ceil(data_length / chunk_length)` by one for the same reason — treat
  `data_length`, not `chunk_count`, as the authority on how much data exists.

NB CRC micro-proof (same file):
```
chunk 0: start=0x0000 comp_len=7729 expected=0x001daf10 computed=0x001daf10 match=true
chunk 1: start=0x1e35 comp_len=2666 expected=0x657f7155 computed=0x657f7155 match=true
```

### Reading NB Format Files

**Required sequence:**
1. Parse `CompressionInfo.db` to get chunk map
2. For each chunk index `i`:
   - Stop if `i * chunk_length >= data_length` — the chunk holds no uncompressed bytes
     (see the degenerate-trailing-chunk warning below)
   - Seek to `offset` in Data.db
   - Read `length` bytes (compressed data)
   - Read next 4 bytes as CRC32 (big-endian u32)
   - Validate: compute CRC32 over compressed bytes
   - Decompress chunk, unless `length >= max_compressed_length` (raw chunk — use the bytes as-is)
   - Truncate the chunk's uncompressed bytes to `min(chunk_length, data_length - i * chunk_length)`
   - Parse row data from those bytes

> **Warning: degenerate empty trailing chunks**
>
> Some compressed SSTables carry a trailing entry in the chunk map that maps to **no uncompressed
> bytes at all**: `chunk_count` is one greater than `ceil(data_length / chunk_length)`, so the last
> entry's logical start (`(chunk_count − 1) * chunk_length`) is already at or beyond `data_length`.
>
> This is a legitimate on-disk state, not corruption. `CompressedSequentialWriter.flushData()`
> unconditionally appends an offset and increments `chunkCount` for whatever is in the buffer —
> including an empty buffer — and `SequentialWriter.syncInternal()` (lines 213–217) calls
> `doFlush(0)` with no empty-buffer guard. Any `sync()` on a chunk boundary therefore emits an
> empty chunk. The common trigger is the preemptive-open path used while writing/compacting
> (`BigTableWriter.openFinalEarly()` → `dataWriter.sync()`, called from
> `SSTableRewriter.java:272`), which is why the artifact appears on *some* SSTables rather than
> all of them.
>
> **How common**: 16 of the 126 compressed SSTables in the CQLite v3.5 validation corpus have one
> (including several compacted `system_schema` tables); the other 110 — also including compacted
> generations — do not. Do **not** assume every compacted SSTable has one, and do not assume none
> does.
>
> **The bound is on the LOGICAL start, not on the chunk offset.** `chunk_offsets` are offsets into
> the *compressed* `Data.db`, while `data_length` is the *uncompressed* total — comparing the two
> is a category error. Concretely, in `test_big/wide_partition` `nb-2` the trailing chunk 113 sits
> at compressed offset 27 814 in a 27 823-byte `Data.db`, while `data_length` is 1 837 037; only
> `113 * 16384 = 1 851 392 >= 1 837 037` identifies it. Cassandra's own reader never touches such a
> chunk because every logical position `< data_length` maps to an earlier index
> (`CompressionMetadata.chunkFor()` indexes by `position / chunkLength`).
>
> **What breaks if you decompress it** depends on the compressor, so a reader must not rely on the
> decompressor to reject it:
> - **Deflate** stores a genuinely 0-byte payload — `DeflateCompressor.compressArray()` returns 0
>   when `Deflater.needsInput()` is true for empty input (line 112) — and inflating 0 bytes is a
>   hard error.
> - **LZ4** stores 5 bytes (`00 00 00 00 00`: the 4-byte little-endian length prefix `0` plus an
>   empty block) with a valid CRC32 `c622f71d`, verified in the corpus. It decompresses cleanly to
>   zero bytes, so the defect stays latent until a consumer treats a zero-length chunk as a parse
>   boundary.
>
> **Reader requirement**: bound chunk iteration by `data_length` at the single chunk-yield source,
> so every downstream consumer inherits the bound. CQLite does this in
> `cqlite-core/src/storage/sstable/reader/block_io.rs:382–394` (`logical_start >= data_length`
> → EOF), and the per-chunk decompressor derives each chunk's expected uncompressed length from
> `data_length` the same way (`chunk_decompressor.rs:197`). This is metadata-driven — no byte
> sniffing (no-heuristics mandate, issue #28). Issue #2225, PR #2242.

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
- **Don't forget the `− 4`** when deriving a chunk's compressed length from consecutive offsets;
  the delta includes the inline CRC word
- **Don't decompress all `chunk_count` entries** — stop at the `data_length` logical bound
- **Don't compare a chunk offset against `data_length`** — offsets are compressed positions,
  `data_length` is an uncompressed total
- **Don't assume `max_compressed_length` is decorative** — it selects raw vs compressed storage,
  and zero must be rejected

### Key Takeaways
- `CompressionInfo.db` maps chunks and validates integrity for modern formats.
- Chunk length is central to random vs scan performance; choose based on workload, and always read
  it from the file (default 16 KiB, not 64 KiB).
- Readers must pair `CompressionInfo.db` with `Data.db` to read the right byte ranges.
- `data_length` — not `chunk_count` — is the authority on how much uncompressed data exists.
- `max_compressed_length` is a functional switch; a zero value fails open and must be rejected at
  parse time.

### References

- `CompressionMetadata` (reader / writer): [io/compress/CompressionMetadata.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java) — `open()` lines 76–112, `chunkFor()` lines 235–253 (the `− 4` CRC adjustment at line 252), `writeHeader()` lines 375–398
- `CompressionParams` (defaults + validation): [schema/CompressionParams.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java) — `DEFAULT_CHUNK_LENGTH` line 47, `DEFAULT_MIN_COMPRESS_RATIO` line 48, `calcMaxCompressedLength()` line 186, `validate()` lines 441–455 (note: `schema/`, not `io/compress/`)
- `CompressedSequentialWriter` (chunk write + CRC + raw fallback): [io/compress/CompressedSequentialWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java) — `flushData()` lines 140–206
- `SequentialWriter` (unguarded flush on sync): [io/util/SequentialWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/SequentialWriter.java) — `syncInternal()` lines 213–217
- `CompressedChunkReader` (scan readahead, raw-chunk read): [io/util/CompressedChunkReader.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/CompressedChunkReader.java) — `instantiateRebufferer()` line 93, `ScanCompressedReader` lines 156–199, `Standard.forScan()` line 241, `Mmap` line 321 (no `forScan()` override)
- `ChunkCache` (drops the scan intent): [cache/ChunkCache.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/cache/ChunkCache.java) — `instance` gating line 53, `CachingRebufferer.instantiateRebufferer()` lines 262–265
- `Config` (defaults): [config/Config.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/config/Config.java) — `compressed_read_ahead_buffer_size` line 341, `file_cache_enabled` line 499
- `BigFormat` (version gate): [io/sstable/format/big/BigFormat.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java) — `hasMaxCompressedLength` line 401
- CASSANDRA-15452 (scan readahead, shipped 5.0.4): [CHANGES.txt](https://github.com/apache/cassandra/blob/cassandra-5.0.8/CHANGES.txt) line 122

For implementation details, see Appendix C.


