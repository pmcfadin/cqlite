# Design — cache-observability-stats (issue #1571, B5)

## Context

Two live caches exist after B1/B3/B4:

- `DecompressedChunkCache` (`cqlite-core/src/storage/cache/mod.rs`) — **shared per-manager**,
  cloned into every reader; already exposes `hit_count()`, `miss_count()`, `resident_bytes()`,
  `budget_bytes()`, `len()`. Missing: an eviction counter.
- `KeyOffsetCache` (`cqlite-core/src/storage/cache/key_offset.rs`) — **per-reader**; exposes the
  same accessors minus evictions. Missing: an eviction counter and any path to the public stats
  surface.

The public stats path is: `Database::stats()` → `DatabaseStats { storage_stats, memory_stats,
query_stats }`. `memory_stats` comes from `MemoryManager::stats()`, and `MemoryManager` already
holds an `Option<Arc<DecompressedChunkCache>>` (wired in `lib.rs` from `storage.chunk_cache()`).
B3 (#1569) uses this handle to report the chunk cache's real hits/misses/occupancy.

## Decisions

### D1 — Eviction counters are relaxed atomics incremented at the eviction site

Both caches evict inside `insert`'s `while … pop_lru()` loop. Increment a
`evictions: AtomicU64` once per successful `pop_lru`, `Ordering::Relaxed` (stats are advisory; the
hot path must not pay a fence). Expose `eviction_count()`. This mirrors the existing
hits/misses discipline exactly.

### D2 — Chunk-cache extras flow through the handle `MemoryManager` already holds

`MemoryManager::stats()` reads `eviction_count()` and `budget_bytes()` off its
`Option<Arc<DecompressedChunkCache>>` and fills the new `block_cache_evictions` /
`block_cache_capacity_bytes` fields. No new coupling — the handle is already there. When the handle
is absent (block caching disabled), the fields report real zeros (an absent cache has zero
activity), consistent with the existing block_cache_hits behavior.

### D3 — Key-cache stats are aggregated over the canonical by-id reader map, once per reader

The key cache is per-reader, so a process-level number is the **sum over live readers**.
`SSTableManager` iterates `self.readers` (the `HashMap<SSTableId, Arc<SSTableReader>>`), which holds
**each reader exactly once** (the `table_readers` name→Vec map re-references the same Arcs and would
double-count, so it is NOT used for aggregation). It reads each reader's
`key_offset_cache` snapshot (hits, misses, evictions, resident bytes, capacity bytes) and sums
them into a `KeyCacheAggregate`. Summing capacity across readers is the honest process-level
resident-budget total (each reader's cache is independently bounded).

The snapshot is read under the same per-shard locks the cache already uses (occupancy) plus relaxed
atomic loads (counters) — no new global hot-path lock (Non-goal honored).

### D4 — `Database::stats()` merges the key-cache aggregate into `memory_stats`

`MemoryManager` does not hold reader handles (the caches are per-reader and churn on refresh), so
the aggregate is captured **at stats() time** in `Database::stats()` — the async site that already
assembles `DatabaseStats` and has access to `storage`. It calls `storage.key_cache_stats()` and
writes the five `key_cache_*` fields onto the `MemoryStats` returned by `memory.stats()`. This keeps
`MemoryStats` a single flat honest surface without giving `MemoryManager` reader coupling.

### D5 — Additive-only `MemoryStats` shape

New fields are appended; no existing field renamed/removed (semver). `MemoryStats` derives
`Default`, so the additions get honest `0` defaults for constructors/tests. A `key_cache_hit_rate()`
helper mirrors `block_cache_hit_rate()`.

## Alternatives considered

- **A new top-level `CacheStats` struct on `DatabaseStats`.** Rejected: heavier public surface than
  needed and would fragment the cache numbers away from the block-cache numbers already living on
  `MemoryStats`. Additive fields on `MemoryStats` are the minimal honest surface.
- **Give `MemoryManager` a key-cache aggregator handle.** Rejected: key caches are per-reader and
  churn on `refresh`; a captured handle would go stale. Aggregating at `stats()` time reads live
  readers and is always current (D4).
- **`Option<u64>` fields to distinguish "no cache" from "zero activity".** Rejected for consistency
  with the existing `u64` block-cache fields and because a disabled/absent cache's real activity IS
  zero — reporting `0` is honest, not fabricated. (The distinction "is a cache wired at all" is
  already observable via `block_cache_capacity_bytes > 0`.)

## Test strategy (TDD, RED first)

- **Cache unit (chunk):** after forcing N evictions on a tiny-budget cache, `eviction_count() == N`.
- **Cache unit (key):** same for `KeyOffsetCache`.
- **Wiring / integration:** open a real dataset DB, do repeated point reads that hit the cache,
  then assert `Database::stats().memory_stats` reports `block_cache_hits > 0`,
  `block_cache_hit_rate() > 0`, `total_memory_used > 0`, and (for a keyed point read) the
  `key_cache_*` aggregate reflects real activity. Fails on current `main` (fields do not exist /
  key-cache never surfaced).
- **No-fabrication:** an unwired `MemoryManager` and a DB with block-caching disabled report real
  zeros across the new fields (no invented capacity).
