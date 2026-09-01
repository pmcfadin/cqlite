## Disk and IO Model

This chapter explains how Cassandra lays out compressed data in fixed-size chunks, how checksums and digests protect integrity, and how IO strategies (mmapped vs buffered) interact with the OS page cache. We also place the Bloom filter in the read path to avoid unnecessary disk seeks.

### In this chapter you will learn
- How compression chunks and checksums are organized
- Differences between memory-mapped and buffered IO in practice
- Why scan and point reads need separate IO plans, and how a shared mmap advice breaks one of them
- Where Bloom filters fit and how they avoid unnecessary disk reads
- The performance implications for random vs sequential reads

## Compression Chunks and Checksums

`CompressionInfo.db` records the compression algorithm, chunk length, max compressed length, total
uncompressed length, and a map of compressed chunk offsets for the corresponding `Data.db`. Every
format in scope here (`na`/`nb` BIG, `oa`/`da` BTI) stores a 4-byte CRC32 inline in `Data.db` after
each compressed chunk — so the delta between consecutive chunk offsets is the compressed payload
plus 4. `Digest.crc32` provides a coarse whole-component integrity check. Ch. 9 covers the exact
serialization and the reader invariants.

From a real `Statistics.db` (trimmed text output produced by tooling), we can see the compressor in use:

```9:14:test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt
Compressor: org.apache.cassandra.io.compress.SnappyCompressor
Compression ratio: 0.9745986747265626
TTL min: 0
TTL max: 0
First token: -9214503836773200690 (15291a77-d739-4e73-8397-b787442f3a1f)
Last token: 9222955098912132944 (079599a9-cefb-4894-9eca-e2e2b21f9582)
```


For implementation details and reader/decompressor examples, see Appendix C.

## IO Strategies

Two common strategies exist for SSTable IO:

- Memory-mapped IO (mmapped):
  - Leverages OS page cache; excellent for sequential scans and repeated hot ranges
  - Simplifies buffering in user space; kernel handles readahead and caching
  - Risks include address space pressure and GC interaction in JVM contexts

- Buffered (pread/read):
  - Explicit control over read sizes and alignment; good for targeted random reads
  - Can tune read sizes to match chunk boundaries for compressed data
  - Puts responsibility for readahead and buffering on the application/runtime

In practice, chunked compression dominates the read cost model: aligning reads to chunk boundaries reduces amplification for random lookups; sequential scans amortize decompression overhead. Cassandra’s `CompressionMetadata` and related classes define the chunk map used by the readers.

### Scan and point reads are different IO planes

The two access patterns want opposite things from the storage layer, and Cassandra treats them as
distinct plans rather than tuning one compromise:

- A **point read** touches one partition body — often a few KiB inside a single compression chunk.
  Readahead beyond that chunk is wasted bandwidth and wasted page cache.
- A **scan** walks `Data.db` in offset order. It wants the largest requests the layer will give it,
  because every chunk it reads it will also decompress and emit.

Cassandra carries this intent explicitly: `RebuffererFactory.instantiateRebufferer(boolean isScan)`
takes the read intent as a parameter, and the compressed reader resolves it into a different
underlying reader — `isScan ? forScan() : this`
(`CompressedChunkReader.java:93`). On the buffered path, `forScan()` activates a
`ScanCompressedReader` with a userspace readahead buffer (default 256 KiB) that coalesces many
16 KiB chunk reads into one large read (CASSANDRA-15452, shipped in 5.0.4). Ch. 9 documents the
mechanism and the two conditions under which it silently does not engage.

> **Warning: mmap advice is per-mapping, so a shared mapping cannot serve both planes**
>
> `madvise` advice attaches to a mapping, not to a read. `MADV_RANDOM` is the right advice for
> scattered point lookups — it suppresses kernel readahead, so a small partition body costs the
> pages it actually touches instead of a full readahead window. It is the wrong advice for a
> sequential walk, which depends on that readahead. A reader that applies `MADV_RANDOM` to one
> mapping and then serves scans from the same mapping degrades the scan to roughly page-at-a-time
> faulting.
>
> This is a real, field-measured failure mode rather than a theoretical one. In CQLite, a
> `MADV_RANDOM` point-read optimization (issue #2210) and a later change that routed the
> summary-guided scan walk onto that same source (#1940) combined into a cross-path regression:
> the compressed scan walk issued ~4–4.5 KiB block requests at ~7 000 reads/s where ~128 KiB
> readahead was expected (issue #2876). The fix (PR #2882) split the scan-side positional source
> from the point mapping — the scan side is a handle that is never `MADV_RANDOM`-advised (an `Arc`
> clone of the same mmap, not a second `mmap`/fd; CQLite advises it `MADV_WILLNEED` by default,
> issue #2824), while genuine point lookups keep the `MADV_RANDOM` source:
> `cqlite-core/src/storage/sstable/reader/mod.rs:608` builds `scan_positional_source`, and
> `POINT_MMAP_MADV_RANDOM_MIN_BYTES` (line 343) gates the advice to mappings ≥ 8 MiB. A regression
> test asserts the scan walk makes zero reads against the point source
> (`reader/read_at_point_tests.rs`,
> `summary_guided_compressed_scan_walk_avoids_point_source`).
>
> **Takeaway for implementers**: if your reader has separate point and scan paths, give them
> distinct mapping handles (or distinct advice), and add a test that asserts the scan path never
> reads through the point source. Nothing in the format makes this failure visible — the bytes are
> identical either way, only the request sizes differ.

### Cassandra 5.0 Default: `mmap_index_only`

Cassandra 5.0 changed the default `disk_access_mode` from `auto` to `mmap_index_only`
(CASSANDRA-19021; `NEWS.txt:288`, `CHANGES.txt:349`). Under `mmap_index_only`:

- **Index files** (`Index.db`, `Summary.db`, BTI trie files) are memory-mapped via
  `ioOptions.indexDiskAccessMode` (mapped to `Config.DiskAccessMode.mmap`).
- **Data.db** uses buffered I/O via `ioOptions.defaultDiskAccessMode` — not mmap.

This hybrid retains mmap’s lookup speed for index navigation while keeping `Data.db` reads
buffered, reducing address-space pressure and JVM GC interaction. Source: `IOOptions.java` and
`SortedTableReaderLoadingBuilder.java:65`. To revert to pre-5.0 behavior, set
`disk_access_mode: auto` in `cassandra.yaml`.

### ChunkCache, `file_cache_enabled`, and `file_cache_size`

Cassandra can wrap a chunk reader in a `ChunkCache` so repeated reads into the same region avoid a
re-read and re-decompression. Two conditions must both hold:

- `file_cache_enabled` must be true **and** the computed cache size must be non-zero:
  `ChunkCache.instance` is null otherwise (`ChunkCache.java:53–54`). `file_cache_enabled`
  **defaults to `false`** in Cassandra 5.0 (`Config.java:499`; commented out in
  `cassandra.yaml:742`), so the chunk cache is off on a default install.
- The reader must go through `FileHandle.Builder.maybeCached()` (`FileHandle.java:444–448`), which
  wraps only when `chunkCache != null && chunkCache.capacity() > 0`. It wraps all three reader
  flavors — `CompressedChunkReader.Mmap` (line 411), `CompressedChunkReader.Standard` (line 424),
  and `SimpleChunkReader` (line 429).

Enabling it has a non-obvious side effect on scans: `CachingRebufferer.instantiateRebufferer()`
drops the `isScan` intent, which disables the scan readahead path described in Ch. 9. The
`BufferPool` backing individual read allocations has a hard maximum of **64 KiB**
(`DiskOptimizationStrategy.java:32`: `MAX_BUFFER_SIZE = 1 << 16`), so buffered reads never exceed
that size on their own.

## Practical guidance on chunk sizes

- Server default for `chunk_length_in_kb` in Cassandra 5.0: **16 KiB** with the LZ4 compressor
  (`CompressionParams.java:47`: `DEFAULT_CHUNK_LENGTH = 1024 * 16`). This is the authoritative
  default. 64 KiB is a common tuning choice and a common wrong assumption in reader
  implementations — it is not the default.
- `chunk_length` must be a power of two: Cassandra derives the chunk index as
  `position / chunkLength` and `CompressionParams.validate()` (lines 443–448) rejects non-powers.
- 32–64 KiB can reduce metadata overhead and improve scan throughput at the cost of higher random-read amplification
- ≤16 KiB may help highly random access patterns when storage latency is high, but increases metadata and CPU overhead

Align application reads to chunk boundaries whenever possible to avoid double-decompression.

> **Note for reader implementers**: never hardcode or infer a chunk length. Read `chunk_length`
> from the SSTable's own `CompressionInfo.db` — it is authoritative per-SSTable metadata, and a
> table's `chunk_length_in_kb` can be altered after data is written, so different SSTables of the
> same table may legitimately disagree (issues #2877, #28).

## Bloom Filters and Negative Lookups

Before any disk seek, Cassandra checks a Bloom filter built from partition keys. A negative result avoids IO entirely. For positives (and false positives), the read proceeds to `Index.db`/`Summary.db` to navigate into `Data.db`.

For a brief Bloom API example, see Appendix C.

False positive rate (FPR) refresher:
- Optimal bits per key: m = −(n · ln p) / (ln 2)²
- Optimal hash functions: k = (m / n) · ln 2
- ASCII: m = - (n * ln(p)) / (ln(2))^2; k = (m / n) * ln(2)
- Example: targeting p = 1% with n = 1,000 partitions → m ≈ 9,585 bits (~1.2 KiB), k ≈ 7
- Operationally: a 1% FPR means ~1 in 100 misses still hit Index/Data, so choose p based on acceptable extra IO

### Key Takeaways
- Chunked compression bounds random-read amplification to chunk size; align reads to chunks
- Read `chunk_length` from `CompressionInfo.db`; the 5.0 default is 16 KiB, not 64 KiB
- Scan and point reads are separate IO plans; a single `MADV_RANDOM` mapping cannot serve both
- Bloom filters cut disk IO by ruling out non-existent partitions early
- Mmapped IO favors scans and hot ranges; buffered IO favors targeted random reads
- Checksums and digests provide integrity at chunk and file levels

### References
- Cassandra 5.0.8 (pinned):
  - `CompressionMetadata` — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java
  - `CompressionParams` (DEFAULT_CHUNK_LENGTH=16384, L47; `validate()` L441–455) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java#L47
  - `CompressedChunkReader` (scan-vs-point rebufferer, L93; `Standard.forScan()` L241) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/CompressedChunkReader.java#L93
  - `ChunkCache` (instance gating L53–54; `CachingRebufferer.instantiateRebufferer()` L262–265) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/cache/ChunkCache.java#L53
  - `Config` (`file_cache_enabled` L499, `compressed_read_ahead_buffer_size` L341) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/config/Config.java#L499
  - `FileHandle` (`maybeCached()` L444–448) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/FileHandle.java#L444
  - `BloomFilter` — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/bloom/BloomFilter.java
  - `IOOptions` (mmap_index_only wiring) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/IOOptions.java
  - `DiskOptimizationStrategy` (MAX_BUFFER_SIZE L32) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DiskOptimizationStrategy.java#L32
- CQLite scan/point plane split (issue #2876, PR #2882): `cqlite-core/src/storage/sstable/reader/mod.rs:343`
  (`POINT_MMAP_MADV_RANDOM_MIN_BYTES`), `:608` (`scan_positional_source`)
  
For implementation walkthroughs, see Appendix C.


