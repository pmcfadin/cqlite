## Caching and OS Interaction

We compare mmapped vs buffered vs async IO and how page cache, read-ahead, and prefetching affect SSTable reads. Historical key/row caches are contrasted with current realities.

### In this chapter you will learn
- Differences among mmapped, buffered, and async IO
- Page cache behavior and read-ahead implications
- Practical defaults for most workloads

## IO Modes

- Memory-mapped (mmap): lowest syscall overhead; relies on page cache; tricky for backpressure
- Buffered (read): explicit IO, easier to bound; OS page cache still in play
- Async (AIO/epoll/tokio): concurrency-friendly; hides latency with futures

For a concrete cache/IO implementation walkthrough, see Appendix C.

## Practical Defaults

- Cassandra 5.0 defaults to `mmap_index_only`: index files are mmap'd; `Data.db` uses buffered
  I/O. This is the recommended baseline — see Ch. 3 for `IOOptions.java` wiring details.
- For workloads saturating `Data.db` sequentially, `disk_access_mode: mmap` re-enables full mmap;
  be aware of JVM address-space pressure on large datasets.
- Buffered async I/O suits mixed/random workloads and bounded-memory `Data.db` reads.
- Keep Bloom enabled; summary sampling reduces seeks
- Tune prefetch window to match chunk sizes (see Ch. 9)

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
- Prefetch and block caches mitigate random-read penalties

### References
- Cassandra 5.0.8 (pinned):
  - `IOOptions` (mmap_index_only wiring) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/IOOptions.java
  - `DiskOptimizationStrategy` (MAX_BUFFER_SIZE L32) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/DiskOptimizationStrategy.java#L32
  - Reader abstractions and IO options in `org.apache.cassandra.io.*`
  
For implementation details, see Appendix C.


