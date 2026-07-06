//! Bounded, sharded key→partition-offset cache (issue #1570, Epic B/B4).
//!
//! The Cassandra **key-cache** analogue: a small LRU that maps a partition key to
//! the location the index/trie descent produces, so a repeated hot point read can
//! seek straight to the partition and skip re-walking `Index.db` (BIG) or the
//! `Partitions.db` trie (BTI). B1's [`DecompressedChunkCache`] caches decompressed
//! *chunk bytes*; this caches *locations* — immutable facts about an immutable
//! SSTable, never row data (rows change shape under projection).
//!
//! # Design (see `openspec/changes/key-partition-offset-cache/design.md`)
//!
//! - **Keyed on the FULL raw partition-key bytes** (D1): the correctness guardrail
//!   is "a cache hit returns the SAME location a fresh lookup would." Hashing the
//!   key to a `u64` would admit collisions → a hit could return a *different* key's
//!   offset, so the LRU is keyed on the owned key bytes (`Box<[u8]>`), collision-free
//!   by construction. Entries are tiny (UUID PK = 16 bytes + a 12-byte value).
//! - **Per-reader** (D2): a location is meaningful only within one SSTable's offset
//!   domain, so each reader owns its cache. Invalidation is trivial — the cache dies
//!   with the reader on remove/reload (the audit's "SSTables are immutable, evict by
//!   sstable identity" rule), so there is never a stale-location hazard.
//! - **Sharded `Mutex<LruCache>`, entry-count bounded** (D3): `LruCache::get`
//!   mutates recency (hence a `Mutex`, not an `RwLock`), hand-sharded (power-of-two,
//!   masked) so the hit path locks exactly ONE shard, never a process-wide lock —
//!   B1's concurrency rule. Each shard's `LruCache::new(NonZeroUsize)` evicts LRU by
//!   count on its own; entries are uniform and tiny so no byte accounting is needed.
//! - **Positive-only** (D4): only *present*-key resolutions are stored, so the cache
//!   can never fabricate a hit for an absent key — it simply misses and the caller
//!   re-resolves (authoritative absence).
//! - **Poison-tolerant** (D3): every lock uses `lock().unwrap_or_else(|e|
//!   e.into_inner())` so one panicking thread cannot wedge the cache. No
//!   `unwrap()`/`expect()`.
//! - **No-heuristics** (mandate #28): the key is the authoritative partition-key
//!   bytes the index/trie is itself keyed on — never inferred from byte content.

use lru::LruCache;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default per-cache entry capacity. Entries are tiny (key bytes + a 12-byte
/// value), so a generous count stays well within the memory budget.
pub const DEFAULT_KEY_CACHE_ENTRIES: usize = 65_536;

/// Default shard count (power of two), mirroring [`DecompressedChunkCache`]'s
/// `DEFAULT_SHARDS`.
///
/// [`DecompressedChunkCache`]: super::DecompressedChunkCache
pub const DEFAULT_KEY_CACHE_SHARDS: usize = 16;

/// The resolved location of a partition, exactly what the index/trie descent
/// produces.
///
/// BIG (`Index.db`) resolves both fields. BTI resolves only `data_offset` (the
/// partition's size is bounded later via the successor offset), so a BTI wiring
/// site stores `data_size = 0` and reads back only `data_offset`. A reader is
/// exactly one format, so the two never mix within one cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLoc {
    /// Uncompressed `Data.db` offset the descent resolved.
    pub data_offset: u64,
    /// Partition byte size (BIG only; `0` for BTI, which bounds size elsewhere).
    pub data_size: u32,
}

impl PartitionLoc {
    /// Location with a known size (BIG `Index.db` resolution).
    #[inline]
    pub fn new(data_offset: u64, data_size: u32) -> Self {
        Self {
            data_offset,
            data_size,
        }
    }

    /// Location with only an offset (BTI trie resolution; `data_size = 0`).
    #[inline]
    pub fn offset_only(data_offset: u64) -> Self {
        Self {
            data_offset,
            data_size: 0,
        }
    }
}

/// A bounded, sharded key→partition-offset cache.
pub struct KeyOffsetCache {
    shards: Box<[Mutex<LruCache<Box<[u8]>, PartitionLoc>>]>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    mask: usize,
    /// When `true` this is a genuine no-op cache (honoring
    /// `config.memory.block_cache.enabled == false`): `get` always misses and
    /// `insert` never retains, so reads bypass the cache entirely.
    disabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for KeyOffsetCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyOffsetCache")
            .field("shards", &self.shards.len())
            .field("disabled", &self.disabled)
            .field("len", &self.len())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl KeyOffsetCache {
    /// Create a cache holding up to `total_entries` entries across
    /// [`DEFAULT_KEY_CACHE_SHARDS`] shards.
    pub fn with_capacity(total_entries: usize) -> Self {
        Self::with_capacity_and_shards(total_entries, DEFAULT_KEY_CACHE_SHARDS)
    }

    /// Create a cache holding up to `total_entries` entries across `shard_count`
    /// shards.
    ///
    /// `shard_count` is rounded UP to the next power of two (min 1) so shard
    /// selection can mask instead of modulo. Unit tests use `shard_count = 1` for
    /// deterministic eviction ordering; production uses [`DEFAULT_KEY_CACHE_SHARDS`].
    pub fn with_capacity_and_shards(total_entries: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1).next_power_of_two();
        // At least one entry per shard so `LruCache::new` gets a non-zero capacity
        // and the cache can always retain the entry it just resolved.
        let per_shard = (total_entries / shard_count).max(1);
        // `per_shard >= 1`, so this `NonZeroUsize` construction never fails; the
        // `unwrap_or` keeps the code panic-free without an `expect`.
        let cap = NonZeroUsize::new(per_shard).unwrap_or(NonZeroUsize::MIN);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(LruCache::new(cap)));
        }
        Self {
            shards: shards.into_boxed_slice(),
            mask: shard_count - 1,
            disabled: false,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a genuine no-op cache (honoring `block_cache.enabled == false`):
    /// reads bypass it entirely. [`get`](Self::get) always returns `None` (no
    /// counters touched), [`insert`](Self::insert) retains nothing, and
    /// `len()`/`capacity()` report `0`.
    pub fn disabled() -> Self {
        Self {
            // One dummy shard so shard indexing stays valid; nothing is ever stored.
            shards: vec![Mutex::new(LruCache::new(NonZeroUsize::MIN))].into_boxed_slice(),
            mask: 0,
            disabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Poison-tolerant lock: recover the guard if a prior holder panicked, so a
    /// single panic cannot turn the cache into a panic-for-everyone (design D3).
    #[inline]
    fn lock(
        m: &Mutex<LruCache<Box<[u8]>, PartitionLoc>>,
    ) -> std::sync::MutexGuard<'_, LruCache<Box<[u8]>, PartitionLoc>> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    fn shard_for(&self, key: &[u8]) -> &Mutex<LruCache<Box<[u8]>, PartitionLoc>> {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        let idx = (h.finish() as usize) & self.mask;
        // `idx <= mask < shards.len()`, so this index is always in bounds.
        &self.shards[idx]
    }

    /// Look up a partition location by its raw key bytes. On a hit this bumps
    /// recency and returns the stored [`PartitionLoc`]; on a miss returns `None`.
    pub fn get(&self, key: &[u8]) -> Option<PartitionLoc> {
        // A disabled cache never holds anything, so reads bypass it without
        // touching the hit/miss counters.
        if self.disabled {
            return None;
        }
        let mut guard = Self::lock(self.shard_for(key));
        match guard.get(key) {
            Some(loc) => {
                let loc = *loc;
                drop(guard);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(loc)
            }
            None => {
                drop(guard);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert `loc` under the raw partition-key bytes `key`. On a disabled cache
    /// this is a no-op. The owning shard evicts its LRU entry if it is at capacity.
    pub fn insert(&self, key: &[u8], loc: PartitionLoc) {
        if self.disabled {
            return;
        }
        let mut guard = Self::lock(self.shard_for(key));
        // `LruCache::put` evicts the least-recently-used entry when at capacity and
        // promotes the inserted key to most-recently-used.
        guard.put(Box::from(key), loc);
    }

    /// Total resident entry count across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|m| Self::lock(m).len()).sum()
    }

    /// Whether the cache currently holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The configured total entry capacity (per-shard capacity × shard count). A
    /// [`disabled`](Self::disabled) no-op cache reports `0`.
    pub fn capacity(&self) -> usize {
        if self.disabled {
            return 0;
        }
        self.shards.iter().map(|m| Self::lock(m).cap().get()).sum()
    }

    /// Cumulative cache hits (test/observability instrumentation).
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cumulative cache misses (test/observability instrumentation).
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl Default for KeyOffsetCache {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_KEY_CACHE_ENTRIES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Task 1.1: eviction order — single shard, capacity 2; insert A, insert B,
    /// access A, insert C → B (LRU) evicted, A and C survive.
    #[test]
    fn eviction_order_lru_single_shard() {
        let cache = KeyOffsetCache::with_capacity_and_shards(2, 1);
        let a = b"key-A".as_slice();
        let b = b"key-B".as_slice();
        let c = b"key-C".as_slice();

        cache.insert(a, PartitionLoc::new(10, 100));
        cache.insert(b, PartitionLoc::new(20, 200));
        // Access A so it becomes more-recently-used than B.
        assert_eq!(cache.get(a), Some(PartitionLoc::new(10, 100)));
        // Insert C: at capacity → evict the LRU, which is now B.
        cache.insert(c, PartitionLoc::new(30, 300));

        assert_eq!(
            cache.get(a),
            Some(PartitionLoc::new(10, 100)),
            "A (recently used) must survive"
        );
        assert_eq!(
            cache.get(c),
            Some(PartitionLoc::new(30, 300)),
            "C (just inserted) must survive"
        );
        assert_eq!(
            cache.get(b),
            None,
            "B (least recently used) must be evicted"
        );
        assert!(cache.len() <= 2);
    }

    /// Task 1.2: entry-count bound — inserting more distinct keys than capacity
    /// keeps the resident count within capacity after every insert.
    #[test]
    fn entry_count_bounded() {
        let cache = KeyOffsetCache::with_capacity_and_shards(4, 1);
        for i in 0..50u64 {
            let key = format!("key-{i}");
            cache.insert(key.as_bytes(), PartitionLoc::new(i, i as u32));
            assert!(
                cache.len() <= 4,
                "resident {} exceeded capacity 4 after insert {}",
                cache.len(),
                i
            );
        }
    }

    /// Task 1.3: a hit returns the stored location; a different key never aliases;
    /// a never-inserted key misses (parity / no-fabrication).
    #[test]
    fn hit_returns_stored_location_no_alias() {
        let cache = KeyOffsetCache::with_capacity_and_shards(1024, 1);
        let a = b"partition-A".as_slice();
        let b = b"partition-B".as_slice();
        cache.insert(a, PartitionLoc::new(63, 512));
        cache.insert(b, PartitionLoc::new(125, 256));

        assert_eq!(cache.get(a), Some(PartitionLoc::new(63, 512)));
        assert_eq!(cache.get(b), Some(PartitionLoc::new(125, 256)));
        assert_eq!(
            cache.get(b"never-inserted".as_slice()),
            None,
            "a never-inserted key must miss (no fabricated hit)"
        );
    }

    /// Task 1.4: `disabled()` is a genuine no-op (get misses, insert retains
    /// nothing, occupancy + counters report zero).
    #[test]
    fn disabled_cache_is_a_genuine_no_op() {
        let cache = KeyOffsetCache::disabled();
        assert_eq!(cache.capacity(), 0);

        cache.insert(b"k".as_slice(), PartitionLoc::new(1, 2));
        assert_eq!(
            cache.get(b"k".as_slice()),
            None,
            "disabled cache never retains"
        );
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
    }

    /// Task 1.5: poison recovery — a panic while holding a shard lock must not
    /// wedge the cache; subsequent get/insert recover the guard and continue.
    #[test]
    fn poisoned_lock_recovers() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::thread;

        let cache = Arc::new(KeyOffsetCache::with_capacity_and_shards(1024, 1));
        cache.insert(b"k".as_slice(), PartitionLoc::new(7, 8));

        // Poison the single shard's mutex by panicking while holding it.
        let poison_cache = Arc::clone(&cache);
        let _ = thread::spawn(move || {
            let _guard = KeyOffsetCache::lock(&poison_cache.shards[0]);
            panic!("intentional poison");
        })
        .join();

        let res = catch_unwind(AssertUnwindSafe(|| {
            let hit = cache.get(b"k".as_slice());
            cache.insert(b"k2".as_slice(), PartitionLoc::new(9, 10));
            hit
        }));
        assert_eq!(
            res.ok(),
            Some(Some(PartitionLoc::new(7, 8))),
            "cache must recover from a poisoned lock"
        );
    }

    #[test]
    fn shard_count_rounds_to_power_of_two() {
        let cache = KeyOffsetCache::with_capacity_and_shards(1024, 10);
        assert_eq!(cache.shards.len(), 16);
        assert_eq!(cache.mask, 15);
    }
}
