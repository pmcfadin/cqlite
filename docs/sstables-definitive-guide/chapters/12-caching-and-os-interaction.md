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

- Use mmap for large sequential scans; buffered async for mixed/random workloads
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
- Start with pool size ≈ (concurrency × average request size × 2). Bound by memory budget and adjust to keep allocator overhead <5% CPU.
- Align buffers to chunk size boundaries to minimize partial-chunk decompression and copies.

### Key Takeaways
- Page cache dominates; pick strategies that align with workload
- Mmap is simple and fast for scans; buffered/async helps control memory and concurrency
- Prefetch and block caches mitigate random-read penalties

### References
- Cassandra 5.0.0:
  - Reader abstractions and IO options in `org.apache.cassandra.io.*`
  
For implementation details, see Appendix C.


