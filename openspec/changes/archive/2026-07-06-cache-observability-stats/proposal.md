## Why

The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic B,
child **B5**, #1571) found cache observability was dishonest: the reported hit rate was
structurally pinned to `0.0` (the counted cache was never written), and `estimate_memory_usage`
summed an always-empty map. B1 (#1567 / #1569) and B4 (#1570) since landed **real** caches
(`DecompressedChunkCache`, `KeyOffsetCache`) with real hit/miss counters, and B3 (#1569) wired the
chunk cache's hits/misses/occupancy through `MemoryStats`. But two honesty gaps remain before an
operator can tune these caches:

1. **No eviction counter on either cache.** The audit's observability requirement is
   hit/miss/**eviction**/occupancy; neither cache counts evictions, so churn is invisible.
2. **The B4 key cache is entirely absent from the public stats surface.** Its real
   hits/misses/occupancy/capacity are never reported through `DatabaseStats`, so a per-reader
   key-cache is un-tunable.
3. **Capacity/budget is not reported** for either cache, so a hit rate cannot be interpreted
   against the budget it was measured under.

This is **Epic B / child B5 (#1571)**, capstone Wave 3 (last in Epic B). The audit is the
**standing owner Seam-1 approval** for its children (2026-07-06 drain directive); this change does
not re-open the design decision. It is **design-driven** (which counters to expose and how to
aggregate a per-reader cache into a process-level surface has real latitude — no external oracle
dictates the stats shape), so it goes through OpenSpec.

**No-fabrication mandate (#28).** Every reported number SHALL be a real counter read from a live
cache (relaxed atomics on the hot path) or a real aggregate over live caches — never a placeholder
`0` standing in for "unknown". A genuinely idle/disabled/absent cache reports `0` because its real
activity is zero, which is honest; the change adds no field whose value is invented.

Milestone: **M7 (perf validation)** — v0.14 perf wave.

## What Changes

- **Add an eviction counter to `DecompressedChunkCache` (B1)** — a relaxed `AtomicU64` incremented
  once per LRU eviction in `insert`, exposed via `eviction_count()`. (Hits/misses/occupancy/budget
  accessors already exist.)
- **Add an eviction counter to `KeyOffsetCache` (B4)** — the same relaxed `AtomicU64` +
  `eviction_count()`, plus a `pub(crate)` snapshot accessor that reads all five real numbers
  (hits, misses, evictions, resident bytes, capacity bytes) for aggregation.
- **Aggregate the per-reader key caches** into a process-level snapshot: `SSTableManager` sums the
  live counters over its canonical by-id reader map (each reader counted once — no double count),
  surfaced through the storage layer to `Database::stats()`.
- **Surface both caches additively through the existing `MemoryStats`** (semver-additive; no field
  renamed or removed): add `block_cache_evictions`, `block_cache_capacity_bytes`, and the
  `key_cache_{hits,misses,evictions,resident_bytes,capacity_bytes}` fields, plus a
  `key_cache_hit_rate()` helper mirroring `block_cache_hit_rate()`.
- **`MemoryManager::stats()` populates the chunk-cache extras** (evictions, capacity) from its live
  chunk-cache handle; `Database::stats()` merges the key-cache aggregate into `memory_stats`.
- **Document the metric names + meaning** where `DatabaseStats` is documented, and add the new
  fields additively to the binding type stubs (`bindings/python/python/cqlite/__init__.pyi`,
  `bindings/node/lib/index.d.ts`) where the stats surface is described.

## Non-goals

- **No new public stats struct or renamed/removed field** — strictly additive to `MemoryStats`
  (semver). The `-> Result<MemoryStats>` shape of `MemoryManager::stats()` is preserved.
- **No fabricated numbers** — a field is populated only from a real counter or a real aggregate.
  A disabled/absent cache reports real zeros, never a placeholder standing in for unknown.
- **No new config knob** — the change reports existing budgets, it does not add tunables.
- **No hot-path lock for stats** — all counters are relaxed atomics; reading occupancy takes only
  the per-shard locks the cache already uses, never a new global read lock on the hot path.
- **No change to any read RESULT** — this is observation only; 33-table parity is unaffected.
- **No new external crate dependency.**

## Impact

- **Public surface (`MemoryStats`):** additive fields only; existing consumers compile unchanged.
- **Memory budget (<128MB):** unaffected — a handful of `AtomicU64`s.
- **Binding surfaces (Python/Node):** type stubs updated additively where stats are documented; no
  behavior change.
- **Concurrency:** eviction counters are relaxed atomics; no new hot-path lock.
