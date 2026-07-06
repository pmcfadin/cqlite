# Design — dead-cache-delete (issue #1568, Epic B / B2)

## Context

Anchors verified read-only on the working branch (base `main`, post-B1):

- **Per-reader dead block cache:**
  - `SSTableReader` fields `block_meta_cache: FxHashMap<u64, BlockMeta>` and
    `block_cache: FxHashMap<u64, CachedBlock>` + `cache_hits`/`cache_misses: AtomicU64`
    (`reader/types.rs:270-287`); `CachedBlock` struct (`types.rs:220`).
  - `record_cache_hit` / `record_cache_miss` / `get_cache_stats` /
    `estimate_memory_usage` (`reader/cache.rs:22-52`) — `estimate_memory_usage` sums an
    always-empty `block_cache` map.
  - Call sites: `record_cache_hit()` / `record_cache_miss()`
    (`data_access/mod.rs:538-541`), `estimate_memory_usage()` (`integrity.rs:30`),
    `block_meta_cache` init/clear (`reader/mod.rs:766,1186-1189`).
- **`MemoryManager` dead core:** `BlockCache`, `RowCache`, `BufferPool`
  (`memory/mod.rs`), `clear_caches`; `stats() -> Result<MemoryStats>`
  (`memory/mod.rs:295`); `MemoryStats` struct with `block_cache_hits/misses`,
  `row_cache_hits/misses`, `total_memory_used`, `buffer_allocations/deallocations`
  (`memory/mod.rs:435`) and `block_cache_hit_rate()` / `row_cache_hit_rate()`.
- **Public reachability (semver):** `Database` holds `memory: Arc<MemoryManager>`
  (`lib.rs:165,201,329`); `Database::stats()` returns `DatabaseStats { memory_stats:
  memory::MemoryStats, .. }` populated from `self.memory.stats()?` (`lib.rs:597-600,676`).
- **B1 real cache (consumed, not modified):** `DecompressedChunkCache`
  (`storage/cache/mod.rs`) with `hit_count()`, `miss_count()`, `resident_bytes()`,
  `budget_bytes()`, `len()`; owned by the storage/SSTable manager and shared across
  readers.
- **Decorative config:** `MemoryConfig { max_memory, block_cache: CacheConfig, row_cache:
  CacheConfig, query_cache: CacheConfig, allocator: AllocatorConfig }` (`config.rs:271-287`);
  `CachePolicy::{Lru,Lfu,Arc}` (`config.rs:335`); `block_cache.max_size` default 256MB
  (`config.rs:297`).

## Goals / non-goals

Goals: delete both dead cache subsystems and the decorative config knobs; keep the
`MemoryManager::stats()` / `MemoryStats` semver shell and make its block-cache numbers
report B1's real activity; wire exactly one real budget knob end-to-end; prove wiring from
the public surface.

Non-goals: re-opening B1's cache design; the full B5 observability surface; B3/B4; any
change to read results; changing the `MemoryStats` struct shape.

## Chosen approach

Encode the owner's three locked decisions faithfully, as a **subtractive** change plus a
minimal honest bridge:

### D1 — B1 is the real cache; the dead subsystems are deleted, not repurposed (audit decision #1)
B1 already shipped `DecompressedChunkCache` as the read cache. The per-reader
`block_cache`/`block_meta_cache`/`CachedBlock`/hit-miss atomics and the `MemoryManager`
LRU-block/row/buffer-pool core are dead weight beside it. Delete them outright rather than
trying to revive or partially retain them. Where B1 already chose part of the old
`MemoryManager` internals as its backing store, only the genuinely unused remainder is
deleted (coordinate with what B1 shipped — a compile-clean `rg` sweep of every reader of a
removed field is the check).

### D2 — Keep the `stats()` shell (semver), delete the cache core, wire real numbers (Block-6 / AK6)
`MemoryManager::stats()` / `MemoryStats` are reachable through `Database::stats().memory_stats`,
so they are a semver-frozen public surface. The struct shape, field names, and
reachability are preserved **verbatim**; only the *source* of the block-cache
hits/misses/occupancy changes — from the deleted dead counters to B1's live
`DecompressedChunkCache::{hit_count, miss_count, resident_bytes}`. Concretely, the stats
surface is threaded to the live B1 cache instance (via the storage engine that owns it) so
`Database::stats().memory_stats.block_cache_hit_rate()` becomes non-zero after a repeated
cached read instead of a structural `0.0`. The dropped row/buffer sub-counters either
report a fixed `0` for their retained fields or are removed with the deleted core — B2
keeps the fields shape-compatible; the richer honest surface is B5.

### D3 — Config source of truth: delete the decorative knobs, don't document around them (audit decision #12)
Keep exactly one real knob: `MemoryConfig.block_cache.max_size`, wired as B1's actual byte
budget so setting it demonstrably changes cache capacity (a tiny budget forces eviction).
Delete `MemoryConfig.row_cache`, `query_cache`, `allocator`, and the never-selected
`CachePolicy::{Lfu,Arc}` variants; remove them from `Config::validate()`. A config file
that still names a removed field fails **closed** (deserialize hard error / validation
rejection), matching the codebase's existing fail-closed config policy — never silently
ignored. The removals are a documented breaking-config note in the changelog.

## What this beat (rejected alternative)

The rejected alternative was the audit's NEEDS-YOU option to **keep or document around the
dead machinery** — leave the `MemoryManager` shelved, keep the decorative config knobs,
and add doc comments explaining they do nothing. The owner rejected it (decisions #1/#12):
it leaves repeated-decompress CPU on the table only if we *hadn't* built B1 (we did), and
it perpetuates a config surface that lies about the runtime — worse for embedders than a
clean breaking removal. Deleting the dead code and wiring one real knob makes the config
honest and the stats honest with no functional loss, since B1 already provides the cache
the dead subsystems only pretended to.

## Risks / mitigations

- **Hidden dependency on a "dead" field:** an `rg` sweep of every read of each removed
  field before deletion; the workspace clippy (`-D warnings`) + full gate catch any
  straggler. `block_meta_cache` in particular is init/cleared in a few places — confirm no
  live consumer reads it for correctness before removing.
- **Accidental semver break:** the change keeps `MemoryManager::stats()`, `MemoryStats`
  (shape + field names), and `Database::stats().memory_stats` reachability; a public-surface
  test asserts the shape is unchanged and the value is now real.
- **Config-compat regression:** the removed-knob behavior (hard error vs. reject) is pinned
  by a test and matched to existing policy; the breaking change is changelog-noted.
- **Read-result drift:** none expected (subtractive); 33-table parity + smoke is the
  guardrail.

## Migration / rollout

Subtractive + one wired knob. Breaking config-schema removal (documented). No read behavior
change; no `MemoryStats` shape change. Depends on B1 (#1567) having landed — it defines the
cache that backs the stats and the budget.
