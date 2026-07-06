# read-cache-cleanup Specification

## Purpose
TBD - created by archiving change dead-cache-delete. Update Purpose after archive.
## Requirements
### Requirement: The dead per-reader block cache is deleted

The system SHALL NOT contain the dead per-reader block-cache machinery on `SSTableReader`:
the `block_cache` map, the `block_meta_cache` map, the `CachedBlock` type, the
`cache_hits` / `cache_misses` atomics, their `record_cache_hit` / `record_cache_miss`
increment sites, and the `estimate_memory_usage` summation over the always-empty block
cache SHALL all be removed. No read path SHALL depend on any removed field, and the crate
SHALL compile cleanly under `RUSTFLAGS="-D warnings"` after the removal (no dead-code or
unused-field warnings suppressed by `#[allow(dead_code)]` for these members).

#### Scenario: Deleted reader cache fields have no remaining readers
- **WHEN** the source tree is searched for reads of `SSTableReader.block_cache`, `block_meta_cache`, `CachedBlock`, `cache_hits`, or `cache_misses`
- **THEN** no non-test production code references them, and the workspace builds and lints clean under `RUSTFLAGS="-D warnings"` with no `#[allow(dead_code)]` retained for these members

#### Scenario: Reader memory estimate no longer sums an empty block-cache map
- **WHEN** a reader's memory-usage estimate is computed after the removal
- **THEN** the estimate is derived without iterating the deleted `block_cache`/`block_meta_cache` maps, and the code path that formerly summed the always-empty map is gone

### Requirement: The MemoryManager dead cache core is deleted while the stats() public shell is preserved

The system SHALL delete the `MemoryManager` dead cache core — the LRU block cache, the row
cache, and the buffer pool internals not adopted by B1 as its backing store. The public
`MemoryManager::stats()` method and the `MemoryStats` type reachable through
`Database::stats().memory_stats` SHALL be preserved shape-compatibly (same method
signature, same struct field names and types, same public reachability), so this change
introduces no semver break to that surface.

#### Scenario: stats() surface shape is unchanged
- **WHEN** `Database::stats()` is called and its `memory_stats` field is inspected
- **THEN** `memory_stats` is a `MemoryStats` value with the same public field names and types and the same `block_cache_hit_rate()` accessor as before this change, and `MemoryManager::stats()` retains its `-> Result<MemoryStats>` signature

#### Scenario: Dead cache core is gone
- **WHEN** the source tree is searched for the `MemoryManager` block-cache / row-cache / buffer-pool internals that no read path used
- **THEN** those internals (and their `clear_caches`/insert/get plumbing) are removed except any component B1 explicitly adopted as its backing store, and the crate builds and lints clean under `RUSTFLAGS="-D warnings"`

### Requirement: The stats() block-cache numbers report the real B1 cache activity

The stats surface SHALL report real B1-cache numbers: the block-cache hit/miss/occupancy
values reported by `MemoryManager::stats()` (and thus `Database::stats().memory_stats`)
SHALL be sourced from the live B1 `DecompressedChunkCache` (its `hit_count()`,
`miss_count()`, and `resident_bytes()`), not from the deleted dead counters. After a chunk
is served from the B1 cache on a repeated read, the reported block-cache hit rate SHALL be
non-zero (it SHALL NOT be structurally pinned to `0.0`).

#### Scenario: Repeated cached read yields a non-zero reported hit rate
- **WHEN** a real multi-chunk SSTable fixture is opened through the public API and the identical read is issued twice so the second read is served from the B1 decompressed-chunk cache
- **THEN** `Database::stats().memory_stats.block_cache_hit_rate()` is greater than `0.0` (it reflects the real B1 hit), whereas on pre-change code the same sequence reports `0.0`

#### Scenario: Reported occupancy tracks real resident bytes
- **WHEN** the B1 cache holds one or more resident decompressed chunks after a read
- **THEN** the block-cache occupancy reported through the stats surface is derived from the B1 cache's `resident_bytes()` (a non-zero value when chunks are resident), not from the deleted always-empty reader map

### Requirement: Exactly one real memory-budget config knob is retained and wired

`MemoryConfig` SHALL retain exactly one caching knob — the block/chunk-cache byte budget
(`block_cache.max_size`) — and that knob SHALL be wired as the B1 decompressed-chunk
cache's actual byte budget, so that changing it demonstrably changes cache capacity and
eviction behavior. The decorative `MemoryConfig.row_cache`, `MemoryConfig.query_cache`,
`MemoryConfig.allocator` fields and the never-selected `CachePolicy::Lfu` and
`CachePolicy::Arc` variants SHALL be removed from the config type and from
`Config::validate()`.

#### Scenario: Setting the budget knob changes B1 cache capacity
- **WHEN** a `Database`/reader is opened with `block_cache.max_size` set to a small byte budget that holds fewer chunks than a fixture touches, and the fixture is scanned
- **THEN** the live B1 cache's `budget_bytes()` equals the configured value AND the cache evicts under the small budget (its `resident_bytes()` stays within the configured budget), demonstrating the knob is wired to real capacity

#### Scenario: A default-budget open uses the configured budget as the B1 capacity
- **WHEN** a `Database` is opened with the default `MemoryConfig`
- **THEN** the live B1 cache's `budget_bytes()` equals the configured `block_cache.max_size` (default 256MB), not an unrelated hard-coded constant

### Requirement: Removed config knobs fail closed and are documented as breaking

Removed config knobs SHALL fail closed: a configuration that names any removed field
(`row_cache`, `query_cache`, `allocator`) or any removed `CachePolicy` variant (`Lfu`,
`Arc`) SHALL NOT be silently accepted — it SHALL fail closed, either failing to
deserialize with a hard error or being rejected by
`Config::validate()`, matching the codebase's existing fail-closed config policy. The
removals SHALL be recorded as a breaking config-schema note in the changelog.

#### Scenario: A config using a removed knob is rejected
- **WHEN** a config value that still specifies `row_cache`, `query_cache`, `allocator`, or `CachePolicy::{Lfu,Arc}` is loaded/validated
- **THEN** loading fails with an explicit error (deserialize error or `Config::validate()` rejection), never a silent ignore that would suggest the knob still has effect

#### Scenario: The retained budget knob still deserializes and validates
- **WHEN** a config specifying only `max_memory` and `block_cache` (with `max_size`) is loaded and validated
- **THEN** it deserializes and passes `Config::validate()`, and its `block_cache.max_size` is the value wired into the B1 cache budget

### Requirement: Deleting the dead subsystems changes no read result

Removing the dead cache subsystems and the decorative config knobs SHALL NOT change any
query result: the 33-table `sstabledump` parity harness SHALL remain green byte-for-byte
and the smoke suite SHALL pass unchanged. The retained public binding surfaces
(Python/Node/CLI) SHALL observe no behavior change other than honest cache statistics.

#### Scenario: Parity and smoke unchanged after the deletion
- **WHEN** the full parity/smoke read suite runs after the dead subsystems and decorative config are removed and the single budget knob is wired
- **THEN** every table's rows match their `sstabledump` JSONL golden exactly, identical to pre-change behavior

