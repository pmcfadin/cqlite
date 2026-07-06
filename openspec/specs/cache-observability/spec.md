# cache-observability Specification

## Purpose
TBD - created by archiving change cache-observability-stats. Update Purpose after archive.
## Requirements
### Requirement: Each read cache counts evictions

Both the decompressed-chunk cache (B1) and the key→partition-offset cache (B4) SHALL maintain a
cumulative eviction counter that increments exactly once per entry evicted to stay within budget,
exposed via an `eviction_count()` accessor. The counter SHALL be a relaxed atomic so the read hot
path incurs no lock or fence for stats.

#### Scenario: N budget-forced evictions report N

- **GIVEN** a cache constructed with a byte budget that holds fewer than the entries to be inserted
- **WHEN** enough distinct entries are inserted to force exactly N LRU evictions
- **THEN** `eviction_count()` returns N
- **AND** an insert that evicts nothing (within budget) leaves `eviction_count()` unchanged

### Requirement: The public stats surface reports real chunk-cache observability

`Database::stats().memory_stats` SHALL report the live decompressed-chunk cache's real hits, misses,
evictions, resident-byte occupancy, and configured capacity. The hit rate SHALL be computed from the
real hit and miss counts (never a structurally-fixed `0.0`). When block caching is disabled or no
cache is wired, the reported numbers SHALL be real zeros (the cache's genuine activity), never a
fabricated non-zero placeholder.

#### Scenario: repeated cached reads report non-zero chunk-cache hits and hit rate

- **GIVEN** an open database over a real dataset with block caching enabled
- **WHEN** the same partition is point-read repeatedly so the chunk cache serves at least one hit
- **THEN** `memory_stats.block_cache_hits > 0`
- **AND** `memory_stats.block_cache_hit_rate() > 0.0`
- **AND** `memory_stats.total_memory_used > 0` (resident decompressed bytes)
- **AND** `memory_stats.block_cache_capacity_bytes` equals the cache's configured budget

#### Scenario: eviction count surfaces through stats

- **GIVEN** an open database whose chunk cache has evicted at least one entry under memory pressure
- **THEN** `memory_stats.block_cache_evictions` equals the cache's real `eviction_count()`

### Requirement: The public stats surface reports the aggregated key-cache observability

`Database::stats().memory_stats` SHALL report the process-level aggregate of the per-reader
key→partition-offset caches: summed hits, misses, evictions, resident bytes, and capacity bytes.
Each live reader SHALL be counted exactly once (no double counting across the by-id and by-name
reader maps). A `key_cache_hit_rate()` helper SHALL compute the rate from the aggregated hit and
miss counts. Absent/disabled caches contribute real zeros.

#### Scenario: repeated keyed point reads report real key-cache activity

- **GIVEN** an open database over a real dataset with the read caches enabled
- **WHEN** the same partition key is point-read repeatedly so the key cache serves at least one hit
- **THEN** `memory_stats.key_cache_hits > 0`
- **AND** `memory_stats.key_cache_hit_rate() > 0.0`
- **AND** the reported aggregate equals the sum of the live per-reader caches' real counters

### Requirement: Stats reporting fabricates nothing and takes no hot-path lock

Every reported cache number SHALL be read from a real live counter (relaxed atomic) or a real
aggregate over live caches; no field SHALL report a placeholder value standing in for an unknown.
Reading the stats SHALL NOT acquire any lock on the read hot path beyond the per-shard locks the
cache already uses to report occupancy.

#### Scenario: an unwired memory manager reports honest zeros

- **GIVEN** a `MemoryManager` constructed without a wired cache (`MemoryManager::new`)
- **THEN** every new cache-observability field reports `0`
- **AND** `block_cache_hit_rate()` and `key_cache_hit_rate()` return `0.0`
- **AND** no field reports a non-zero value that was not read from a real counter

