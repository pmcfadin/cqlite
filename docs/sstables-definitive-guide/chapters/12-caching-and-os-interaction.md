## Caching and OS Interaction

We compare mmapped vs buffered vs async IO and how page cache, read-ahead, and prefetching affect SSTable reads. Historical key/row caches are contrasted with current realities.

### In this chapter you will learn
- Differences among mmapped, buffered, and async IO
- Page cache behavior and read-ahead implications
- Why madvise advice forces a scan-vs-point read-plane split
- Practical defaults for most workloads

## IO Modes

- Memory-mapped (mmap): lowest syscall overhead; relies on page cache; tricky for backpressure
- Buffered (read): explicit IO, easier to bound; OS page cache still in play
- Async (AIO/epoll/tokio): concurrency-friendly; hides latency with futures

For a concrete cache/IO implementation walkthrough, see Appendix C.

### madvise is per-mapping, not per-read

`madvise` advice is a property of the mapping, so it cannot be varied per request. That makes the
choice of advice a *read-plane* decision rather than a per-call tuning knob:

- `MADV_RANDOM` suppresses kernel readahead. Correct for scattered point lookups, where a partition
  body is often a few KiB and readahead is pure waste.
- `MADV_SEQUENTIAL`/no advice leaves readahead in play. Required for an offset-ordered walk of
  `Data.db`.

A reader with both access patterns therefore needs **two mapping handles** (or two backends) — one
advised for point lookups, one unadvised for scans. Cloning a handle to the same mapping is enough;
a second `mmap` or file descriptor is not required. If a scan is served from the `MADV_RANDOM`
mapping it collapses toward page-at-a-time faulting: CQLite measured ~4–4.5 KiB block requests at
~7 000 reads/s on a compressed scan walk in exactly that configuration (issue #2876, fixed by PR
#2882 splitting the scan positional source out of the advised point mapping). Ch. 3 has the details
and the code references; the practical rule here is to assert in tests that the scan path never
reads through the point source, because the returned bytes are identical either way and only the
request sizes differ.

Cassandra reaches the same split differently: it passes the read intent down as
`instantiateRebufferer(boolean isScan)` and selects a scan-specific chunk reader with a userspace
readahead buffer instead of relying on kernel readahead at all (`CompressedChunkReader.java:93`,
CASSANDRA-15452 in 5.0.4). See Ch. 9 for how a chunk cache can silently swallow that intent.

## Practical Defaults

- Cassandra 5.0 defaults to `mmap_index_only`: index files are mmap'd; `Data.db` uses buffered
  I/O. This is the recommended baseline — see Ch. 3 for `IOOptions.java` wiring details.
- For workloads saturating `Data.db` sequentially, `disk_access_mode: mmap` re-enables full mmap;
  be aware of JVM address-space pressure on large datasets.
- Buffered async I/O suits mixed/random workloads and bounded-memory `Data.db` reads.
- Cassandra's own chunk cache is **off by default** (`file_cache_enabled = false`,
  `Config.java:499`), and enabling it disables the scan-readahead path (Ch. 9). Treat "add a cache"
  and "coalesce scan reads" as one co-design decision, not two independent wins.
- Keep Bloom enabled; summary sampling reduces seeks
- Size the scan read window as a multiple of `chunk_length` read from `CompressionInfo.db` — the
  5.0 default is 16 KiB, and Cassandra's own scan buffer defaults to 256 KiB
  (`compressed_read_ahead_buffer_size`, `Config.java:341`), i.e. ~16 chunks per read (see Ch. 9)

### Memory Pressure Handling
- Implement cache admission/eviction with size caps; evict oldest blocks first (LRU) or LFU for hot workloads.
- Under pressure, prefer dropping OS page cache (mmap) mappings over user-space caches to avoid double caching.
- Expose backpressure: throttle prefetch when cache hit rate drops below a target threshold.

### Direct IO for Large Scans
- For long sequential scans on saturated systems, direct IO can reduce page cache churn. Pair with larger read buffers (e.g., multiples of chunk length) and explicit readahead.
- Fall back to buffered IO when decompression or random access breaks large request alignment.

### Buffer Pool Sizing
- The `BufferPool` backing `RandomAccessReader` allocations has a hard ceiling of **64 KiB** per
  buffer (`DiskOptimizationStrategy.java:32`: `MAX_BUFFER_SIZE = 1 << 16`).
- Start with pool size ≈ (concurrency × average request size × 2). Bound by memory budget and
  adjust to keep allocator overhead <5% CPU.
- Align buffers to chunk size boundaries to minimize partial-chunk decompression and copies.

### Key Takeaways
- Page cache dominates; pick strategies that align with workload
- Mmap is simple and fast for scans; buffered/async helps control memory and concurrency
- madvise advice is per-mapping: give scan and point reads separate handles or separate backends
- Prefetch and block caches mitigate random-read penalties, but a cache layer that drops the scan
  intent silently cancels read coalescing

### References
- Cassandra 5.0.8 (pinned):
  - `IOOptions` (mmap_index_only wiring) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/IOOptions.java
  - `DiskOptimizationStrategy` (MAX_BUFFER_SIZE L32) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DiskOptimizationStrategy.java#L32
  - `CompressedChunkReader` (`instantiateRebufferer(isScan)` L93) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/CompressedChunkReader.java#L93
  - `Config` (`file_cache_enabled` L499, `compressed_read_ahead_buffer_size` L341) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/config/Config.java#L499
  - Reader abstractions and IO options in `org.apache.cassandra.io.*`
- CQLite scan/point plane split: issue #2876, PR #2882 —
  `cqlite-core/src/storage/sstable/reader/mod.rs:608`
  
For implementation details, see Appendix C.


