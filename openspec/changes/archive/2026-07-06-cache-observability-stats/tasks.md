# Tasks — cache-observability-stats (issue #1571, B5)

## 1. TDD tests first (write RED, then implement to green)
- [ ] 1.1 Unit test (chunk cache, single shard, tiny budget): force N evictions → `eviction_count() == N`;
      an insert within budget leaves it unchanged (no-fabrication).
- [ ] 1.2 Unit test (key cache, single shard, tiny budget): same eviction-count assertion.
- [ ] 1.3 `MemoryManager::stats()` unit: a wired manager reports the chunk cache's real
      evictions + capacity; an unwired manager reports honest zeros for all new fields.
- [ ] 1.4 Integration (real dataset): open DB → repeated point read of the same partition →
      `Database::stats().memory_stats` shows `block_cache_hits > 0`, `block_cache_hit_rate() > 0`,
      `total_memory_used > 0`, `block_cache_capacity_bytes > 0`, and the `key_cache_*` aggregate
      reflects real activity. Must fail on current `main`.

## 2. Eviction counters
- [ ] 2.1 `DecompressedChunkCache`: add `evictions: AtomicU64`, increment per `pop_lru`, add
      `eviction_count()`.
- [ ] 2.2 `KeyOffsetCache`: add `evictions: AtomicU64`, increment per `pop_lru`, add
      `eviction_count()` + a `pub(crate)` snapshot accessor (hits/misses/evictions/resident/capacity).

## 3. Aggregate the per-reader key caches
- [ ] 3.1 `SSTableManager::aggregate_key_cache_stats()` sums the snapshot over `self.readers`
      (each reader once — not `table_readers`).
- [ ] 3.2 `StorageEngine::key_cache_stats()` exposes it.

## 4. Surface additively through MemoryStats
- [ ] 4.1 Add fields: `block_cache_evictions`, `block_cache_capacity_bytes`,
      `key_cache_{hits,misses,evictions,resident_bytes,capacity_bytes}`; `key_cache_hit_rate()`.
- [ ] 4.2 `MemoryManager::stats()` fills the chunk-cache extras from its handle.
- [ ] 4.3 `Database::stats()` merges `storage.key_cache_stats()` into `memory_stats`.

## 5. Docs + bindings
- [ ] 5.1 Document metric names + meaning where `MemoryStats`/`DatabaseStats` are documented.
- [ ] 5.2 Additive type-stub notes in `bindings/python/python/cqlite/__init__.pyi` and
      `bindings/node/lib/index.d.ts` where the stats surface is described.

## 6. Gate
- [ ] 6.1 `openspec validate cache-observability-stats --strict` clean.
- [ ] 6.2 `scripts/agent-gate.sh --lite` PASS each fix round; `cargo +1.88.0 fmt --check` clean;
      `RUSTFLAGS="-D warnings" cargo clippy -p cqlite-core --features cli-helpers` clean.
