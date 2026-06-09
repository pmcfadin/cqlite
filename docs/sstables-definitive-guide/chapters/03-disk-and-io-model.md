## Disk and IO Model

This chapter explains how Cassandra lays out compressed data in fixed-size chunks, how checksums and digests protect integrity, and how IO strategies (mmapped vs buffered) interact with the OS page cache. We also place the Bloom filter in the read path to avoid unnecessary disk seeks.

### In this chapter you will learn
- How compression chunks and checksums are organized
- Differences between memory-mapped and buffered IO in practice
- Where Bloom filters fit and how they avoid unnecessary disk reads
- The performance implications for random vs sequential reads

## Compression Chunks and Checksums

`CompressionInfo.db` records the compression algorithm, chunk length, total uncompressed length, and a map of compressed chunk offsets for the corresponding `Data.db`. Modern formats may include per-chunk CRCs; `Digest.crc32` provides a coarse integrity check for the SSTable.

From a real `Statistics.db` (trimmed text output produced by tooling), we can see the compressor in use:

```9:14:/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804/nb-1-big-Statistics.db.txt
Compressor: org.apache.cassandra.io.compress.SnappyCompressor
Compression ratio: 0.9762523409965641
TTL min: 0
TTL max: 0
First token: -9216841881891618357 (4d4321e2-662b-4ba1-b75f-48e080727a52)
Last token: 9206157491929561407 (6bdb6b71-d459-402f-be40-3b4fa1067661)
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

### ChunkCache and `file_cache_size`

Cassandra optionally wraps buffered readers in a `ChunkCache` (configured via `file_cache_size`
in `cassandra.yaml`). The cache stores decompressed chunks so repeated random reads into the
same compressed region avoid re-decompression. Integration point: `FileHandle.java:444–448`
(`maybeCached()` wraps `SimpleChunkReader` with `CachingRebufferer`). The `BufferPool` backing
individual read allocations has a hard maximum of **64 KiB**
(`DiskOptimizationStrategy.java:32`: `MAX_BUFFER_SIZE = 1 << 16`).

## Practical guidance on chunk sizes

- Server default for `chunk_length_in_kb` in 5.0: **16 KiB** with LZ4 compressor (`CompressionParams.java:47`: `DEFAULT_CHUNK_LENGTH = 1024 * 16`). 64 KiB is a common tuning choice, not the default.
- 32–64 KiB can reduce metadata overhead and improve scan throughput at the cost of higher random-read amplification
- ≤16 KiB may help highly random access patterns when storage latency is high, but increases metadata and CPU overhead

Align application reads to chunk boundaries whenever possible to avoid double-decompression.

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
- Bloom filters cut disk IO by ruling out non-existent partitions early
- Mmapped IO favors scans and hot ranges; buffered IO favors targeted random reads
- Checksums and digests provide integrity at chunk and file levels

### References
- Cassandra 5.0.8 (pinned):
  - `CompressionMetadata` — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java
  - `CompressionParams` (DEFAULT_CHUNK_LENGTH=16384, L47) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/CompressionParams.java#L47
  - `BloomFilter` — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/bloom/BloomFilter.java
  - `IOOptions` (mmap_index_only wiring) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/IOOptions.java
  - `DiskOptimizationStrategy` (MAX_BUFFER_SIZE L32) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DiskOptimizationStrategy.java#L32
  
For implementation walkthroughs, see Appendix C.


