## Why

The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Executive summary #1, §Epic B) found that CQLite ships **two dead caching subsystems**
and a set of **decorative memory-budget config knobs** that describe a runtime that does
not exist:

1. **Per-reader block cache** — `SSTableReader.block_cache` / `block_meta_cache` /
   `CachedBlock` plus the `cache_hits` / `cache_misses` atomics
   (`reader/types.rs:270-287`, `reader/cache.rs:22-52`, wired at
   `data_access/mod.rs:538-541`). It is initialized empty and **never inserted into**, so
   `SSTableReader::get_cache_stats` reports a hit rate structurally pinned to `0.0`, and
   `estimate_memory_usage` sums an always-empty map (`cache.rs:42-52`).
2. **`MemoryManager`** (`memory/mod.rs`, ~725 lines: an LRU block cache + row cache +
   buffer pool, unit-tested) — constructed on `Database::open` and shelved; nothing on the
   read path uses its cache core. Its `stats()` / `MemoryStats` are, however,
   **public-surface-wired** through `Database::stats().memory_stats` (semver).
3. **Decorative memory config** (`config.rs:274-348`): `MemoryConfig.row_cache`,
   `query_cache`, `allocator`, and `CachePolicy::{Lfu,Arc}` — deserialized and (partly)
   validated by `Config`, but wired to nothing at runtime.

B1 (#1567, landed via PR #1831) built the **real** cache — a shared, bytes-bounded,
sharded `DecompressedChunkCache` (`cqlite-core/src/storage/cache/mod.rs`) with
`hit_count()` / `miss_count()` / `resident_bytes()` / `budget_bytes()`. With a real cache
present, the dead subsystems are pure liability: they mislead observability, carry
maintenance cost, and describe a budget that silently does nothing. This is **Epic B /
child B2 (#1568)**, capstone **Wave 3** (after B1).

**Routing: design-driven, owner-pre-decided.** The audit is the source of truth and the
owner locked the posture on 2026-07-01 ("do not re-open"): decision #1 (B1 is the real
cache), decision #12 (delete decorative config, don't document around it), and the
Block-6 constraint (AK6): `MemoryManager.stats()` is semver-wired — keep the stats *shell*
and make it report B1's *real* numbers, delete only the dead cache *core*. This change
encodes those locked decisions so the owner can approve them at Seam 1; it does not
re-litigate them.

Milestone: **M7 (perf validation)** / maintenance. No new read behavior — this is a
subtractive change plus one real budget knob and an honest stats bridge.

## What Changes

- **Delete the per-reader dead block cache**: `SSTableReader.block_cache`,
  `block_meta_cache`, `CachedBlock`, the `cache_hits` / `cache_misses` atomics, their
  `record_cache_hit` / `record_cache_miss` call sites (`data_access/mod.rs:538-541`), and
  `estimate_memory_usage`'s always-empty-map summation.
- **Delete the `MemoryManager` dead cache core** (LRU block cache, row cache, buffer pool
  internals — the parts B1 did **not** adopt as its backing store) while **preserving the
  public `MemoryManager::stats()` shell and the `MemoryStats` shape** exposed via
  `Database::stats().memory_stats`.
- **Bridge `stats()` to B1's real numbers**: the retained `MemoryStats` block-cache
  hit/miss/occupancy fields are sourced from the live `DecompressedChunkCache`
  (`hit_count()` / `miss_count()` / `resident_bytes()`), so a repeated cached read makes
  `block_cache_hit_rate()` non-zero instead of a structural `0.0`.
- **Collapse the decorative config to exactly one real knob**: keep the
  block/chunk-cache **byte budget** (`MemoryConfig.block_cache.max_size`) and wire it as
  B1's actual capacity; **delete** `MemoryConfig.row_cache`, `query_cache`, `allocator`,
  and the unused `CachePolicy::{Lfu,Arc}` variants, and remove them from
  `Config::validate()`. Record the breaking-config removals in the changelog.
- **Sweep all readers of the deleted fields** (`rg`) so nothing silently depended on them.

## Non-goals

- **NOT re-opening the B1 cache design.** The `read-cache` capability and
  `DecompressedChunkCache` are settled (owner decision #1); this change consumes B1, never
  redesigns it.
- **NOT changing the `MemoryManager::stats()` / `MemoryStats` semver surface** beyond
  making the block-cache numbers *real*. The struct shape, field names, and
  `Database::stats().memory_stats` reachability stay byte-identical; only the reported
  values become honest.
- **NOT the full honest-observability surface (B5).** B2 removes the structural-0 hit rate
  and bridges the minimal real numbers; the richer `DatabaseStats` eviction/byte-occupancy
  surface and the reader-side observability rewrite are B5's scope.
- **NOT touching B3/B4/B5 scope**: the `ChunkDecompressor` clone-on-hit cache
  (`sstable_data_manager.rs` / CLI path, B3), the key/partition-offset cache (B4), or the
  full observability rewrite (B5).
- **No change to any read RESULT.** 33-table `sstabledump` parity stays byte-for-byte
  green; this is a deletion + wiring change, not a decode change.

## Impact

- **Public surface (semver):** `MemoryManager::stats()`, `MemoryStats`, and
  `Database::stats().memory_stats` are preserved shape-compatible. Removed *config* fields
  (`row_cache`, `query_cache`, `allocator`, `CachePolicy::{Lfu,Arc}`) are a **breaking
  config-schema change** — documented in the changelog; a config that still names them
  fails to deserialize (hard error) or is rejected by `Config::validate()`, matching the
  codebase's existing config-compat policy (fail closed, no silent ignore).
- **No-heuristics mandate:** unaffected — this change removes dead machinery and wires an
  authoritative budget; no byte-content inference is introduced.
- **Memory budget (<128MB):** the retained single knob is the *real* B1 capacity, so the
  configured budget now actually bounds resident decompressed bytes (previously it bounded
  nothing).
- **Bindings (Python/Node/CLI):** no behavior change; `Database::stats()` continues to
  return the same shape, now with honest cache numbers.
