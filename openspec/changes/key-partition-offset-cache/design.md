# Design — key-partition-offset-cache (issue #1570, B4)

## Context

B1 (#1567) built `DecompressedChunkCache` (chunk bytes). B4 adds the *location* cache — the
Cassandra key-cache analogue. The two lookup functions that resolve a partition key to a location
already exist and are the exact insertion points:

- BIG: `SSTableReader::lookup_partition_with_index(&[u8]) -> Result<Option<(u64, u32)>>`
  (`partition_lookup.rs`) — probes `Index.db`.
- BTI: `SSTableReader::lookup_partition_via_bti_trie(&[u8]) -> Result<Option<u64>>`
  (`partition_lookup.rs`) — descends the `Partitions.db` trie, gauged by `TRIE_WALKS`.

A single-entry same-key memo (`bti_lookup_memo`, C3/#1574) already dedups the prune+seek trie walk
*within one point read*. B4 generalizes to a **multi-key bounded LRU** that persists *across* point
reads and covers both formats.

## Decision 1 — Key on the FULL partition-key bytes, not a digest/hash

The correctness guardrail is "a cache hit returns the SAME offset a fresh lookup would." Hashing the
key to a `u64` (as B1 does for `(sstable, chunk_index)`, where the components are already small
integers) would admit hash collisions → a hit could return a *different* key's offset. So the LRU is
keyed on the **owned raw partition-key bytes** (`Box<[u8]>`), exactly the bytes the index/trie is
keyed on. Collision-free by construction; matches the `bti_lookup_memo`'s `Box<[u8]>` key. Entries
are tiny (UUID PK = 16 bytes + 12 value bytes), so the memory cost of storing full keys is
negligible — the audit explicitly notes "entries are tiny."

## Decision 2 — Per-reader cache, not a shared manager-level cache

B1's chunk cache is manager-shared because a chunk's bytes could (in principle) be reused across
reopened readers. A *location* is meaningful only within ONE SSTable's offset domain, so a
per-reader cache is the natural scope and it makes the audit's **invalidation rule trivially
correct**: "SSTables are immutable — evict by sstable identity on reader remove/reload." A per-reader
cache dies with the reader on remove/reload, so there is never a stale-location hazard and no
cross-SSTable key namespace is needed. The reader builds its cache at `open` honoring the config
toggle (like the chunk cache's back-compat path), stored as `Arc<KeyOffsetCache>`.

## Decision 3 — Sharded `Mutex<LruCache>`, entry-count bounded (B1's concurrency rule)

`LruCache::get` mutates recency, so an `RwLock` degrades to a `Mutex`; B1's lesson is to hand-shard
`Mutex<LruCache>` (power-of-two shard count, mask instead of modulo) so the hit path takes only a
per-shard lock. B4 reuses that shape. Unlike B1 (bytes-bounded — chunk sizes vary), B4 is
**entry-count bounded**: entries are uniformly tiny, so `LruCache::new(NonZeroUsize)`'s built-in
count eviction is exactly right (no manual byte accounting). Each shard gets `capacity / shards`
(min 1) entries. Unit tests use a **single shard** for deterministic eviction ordering (the B1
lesson: multi-shard eviction is non-deterministic and flakes).

Locks are taken poison-tolerantly (`lock().unwrap_or_else(|e| e.into_inner())`), so one panicking
thread cannot wedge the cache. No `unwrap()`/`expect()` in the cache or the wiring.

## Decision 4 — Positive-only (no negative cache)

Only *present*-key resolutions are inserted. Rationale: (a) "must not fabricate hits" is trivially
satisfied — an absent key is never a stored key; (b) it avoids negative-cache invalidation
subtleties; (c) the hot path B4 targets is repeated reads of *present* keys. An absent key misses the
cache and falls through to the authoritative-absence resolution every time (still correct, just not
accelerated — which is fine, absence is already the trie/bloom fast path).

## Decision 5 — Value shape unifies BIG and BTI

Value = `PartitionLoc { data_offset: u64, data_size: u32 }`. BIG stores both (from `Index.db`). BTI
resolves only an offset (size is bounded later via the successor offset), so it stores
`data_size = 0` and its wiring reads back only `data_offset`. A reader is exactly one format, so the
two never mix within a cache; the field is unambiguous per format.

## Decision 6 — Config toggle, no new knob (B2 pattern)

The cache honors `config.memory.block_cache.enabled` (the read-cache toggle B2 established): enabled
→ a real cache sized from a small entry-count constant; disabled → `KeyOffsetCache::disabled()`, a
genuine no-op (get always misses, insert never retains, occupancy/counters report zero) so the
point-read path bypasses it. Capacity is NOT a new config field (the audit forbids a decorative
knob); it is a constructor parameter with a `DEFAULT_KEY_CACHE_ENTRIES` default.

## Decision 7 — `INDEX_PROBES` counter for BIG wiring evidence

BTI already has `TRIE_WALKS` (a hit → `== 0`). BIG has no equivalent gauge for `Index.db` probes, so
this change adds `INDEX_PROBES` to the cfg-gated `read_work_counters` module (unconditional
`record_index_probe()` call site, `#[cfg(any(test, feature = "work-counters"))]` body → zero overhead
in release, same pattern as every A5/H5 counter). It is incremented once per real
`index_reader.lookup_partition` probe in `lookup_partition_with_index`. A BIG cache hit skips the
probe → `INDEX_PROBES == 0`, the exact analogue of the BTI assertion.

## Testing strategy

- **Unit (local instance, deterministic):** eviction order (single shard, A/B/A/C cap-2 → B evicted);
  entry-count bound; hit returns the stored location; a *different* key never aliases (parity: two
  distinct keys keep distinct locations); `disabled()` no-op; poison recovery.
- **Wiring evidence (real fixture, `work-counters` feature):**
  - BTI (`test_da/simple_table`): read key A, read key B (each a `TRIE_WALKS`), then read A again with
    the counter reset → `TRIE_WALKS == 0` (the LRU hit, proven beyond the single-entry memo by the
    interleave with B).
  - BIG (`test_basic/simple_table`): call `lookup_partition_with_index(k)` twice at the reader level;
    reset `INDEX_PROBES` before the 2nd → `INDEX_PROBES == 0` and the 2nd result equals the 1st
    `(offset,size)` (parity on a hit).
- **Eviction correctness after wiring:** covered by the unit eviction test (post-eviction lookup
  falls back to a fresh resolution — the wiring calls the real resolver on a miss).

## Deferred / Non-goals

- **Global bounded cache keyed on `(sstable_id, key)` (Cassandra's key-cache model).** The per-reader
  design (Decision 2) has no global ceiling, so the worst-case aggregate resident memory is
  `N_open_readers × DEFAULT_KEY_CACHE_ENTRIES × ~80 B/entry`. B4 bounds this pragmatically by keeping
  the per-reader cap small (4096 entries → `~40 readers × 4096 × ~80 B ≈ 13 MB`, well within the
  `<128MB` budget; allocation is occupancy-proportional so idle readers cost ~nothing). A single
  process-wide bounded LRU keyed on `(sstable_id, key)` would bound the aggregate *independent* of the
  open-reader count (exactly Cassandra's key cache), but it reintroduces a cross-SSTable key namespace
  and manager-level invalidation — a larger architectural change beyond B4's audit-approved per-reader
  scope. Deferred to a follow-up issue.
