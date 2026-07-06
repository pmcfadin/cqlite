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
//!   B1's concurrency rule. Each shard is an `LruCache::unbounded()` whose per-shard
//!   entry cap is enforced MANUALLY on insert (`pop_lru` while over cap), mirroring
//!   B1's [`DecompressedChunkCache`]. This keeps allocation proportional to actual
//!   occupancy: an empty cache allocates no bucket array, so many concurrently-open
//!   readers do not each pre-pay a full-capacity hash table (the <128MB budget).
//!   Entries are uniform and tiny so no byte accounting is needed.
//! - **Resolution-hit only, never a MISS** (D4): the cache stores only a location a
//!   lookup actually resolved, never a definitive MISS, so it can never fabricate a
//!   hit for a key the underlying structure did not resolve. Both read paths preserve
//!   the correctness guardrail (a cache hit returns the SAME location a fresh lookup
//!   would, so any downstream check reaches the identical conclusion), but "resolved"
//!   means something slightly different for each:
//!     - **BIG = confirmed-resolution-only**: only a confirmed `Some` from the
//!       `Index.db` raw-key lookup is inserted, so an absent key is never cached and
//!       the cache simply misses (authoritative absence).
//!     - **BTI = trie-hit-including-candidates**: any trie HIT is cached, which
//!       includes prefix-collision candidates — an offset that may belong to a
//!       different or even absent key — so an absent key CAN receive a cache entry
//!       pointing at its candidate offset. This is not a correctness bug: the caller
//!       (`bti_point_lookup`) re-verifies the key bytes at the resolved offset, and a
//!       cache hit returns the SAME candidate offset a fresh descent returns, so
//!       re-verification reaches the identical conclusion (mirrors the C3
//!       `bti_lookup_memo`, which likewise stores `Some(offset)` for a candidate). A
//!       trie MISS is never cached.
//! - **Poison-tolerant** (D3): every lock uses `lock().unwrap_or_else(|e|
//!   e.into_inner())` so one panicking thread cannot wedge the cache. No
//!   `unwrap()`/`expect()`.
//! - **No-heuristics** (mandate #28): the key is the authoritative partition-key
//!   bytes the index/trie is itself keyed on — never inferred from byte content.

use lru::LruCache;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default **per-reader** key-cache entry capacity.
///
/// This cache is per-reader (design D2): each open SSTable reader owns one, so
/// the resident memory that matters for the `<128MB` budget is the **aggregate**
/// across every concurrently-open reader — `N_readers × DEFAULT_KEY_CACHE_ENTRIES
/// × bytes_per_entry` in the worst (fully-occupied) case. There is no global
/// ceiling across readers, so this per-reader cap must be chosen so a generous
/// open-reader count stays comfortably within budget.
///
/// # Aggregate-footprint math (worst / fully-occupied case)
///
/// - `bytes_per_entry ≈ 80 B`: an entry is a `Box<[u8]>` key (a typical UUID PK
///   is 16 key bytes + the 16-byte fat-pointer + heap-allocation header), the
///   [`PartitionLoc`] value (12 B), and one `LruCache` intrusive-list node
///   (`HashMap` bucket slot + two `NonNull` sibling links). Rounding generously
///   to ~80 B/entry covers allocator rounding and the map's load-factor slack.
/// - Per-reader worst case: `4096 × 80 B ≈ 320 KB` fully occupied.
/// - Aggregate for a point-read-heavy workload with ~40 open generations:
///   `40 readers × 4096 entries × 80 B ≈ 13.1 MB` — well within the `<128MB`
///   budget (leaving the vast majority of it for B1's decompressed-chunk cache
///   and the working set). Even a very generous ~128 open readers is
///   `128 × 4096 × 80 B ≈ 42 MB`, still within budget.
///
/// The previous value (`65_536`) reasoned only about the empty/idle case
/// (occupancy-proportional allocation makes an idle reader ~free) and could,
/// fully occupied across many readers, breach the budget
/// (`40 × 65_536 × 80 B ≈ 210 MB`). 4096 hot keys per reader is ample for
/// point-read locality, so the perf win is preserved while the aggregate is
/// bounded.
///
/// Each shard's bucket array still grows only with actual occupancy (unbounded
/// `LruCache` + manual eviction), so an idle/empty reader's cache costs
/// ~nothing; the cap bounds only the fully-occupied worst case.
///
/// A single **global bounded cache** keyed on `(sstable_id, key)` (Cassandra's
/// key-cache model) would bound the aggregate regardless of reader count and is
/// noted as a deferred future optimization (see `design.md` "Deferred"); it is a
/// larger architectural change beyond B4's audit-approved per-reader scope.
pub const DEFAULT_KEY_CACHE_ENTRIES: usize = 4_096;

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

/// One cache shard: an `unbounded` `LruCache` mapping the raw partition-key bytes
/// to the resolved [`PartitionLoc`], behind a `Mutex` (the hit path mutates
/// recency, so an `RwLock` would degrade to a `Mutex`). The entry-count bound is
/// enforced manually on insert so the bucket array grows only with occupancy.
type KeyShard = Mutex<LruCache<Box<[u8]>, PartitionLoc>>;

/// A bounded, sharded key→partition-offset cache.
pub struct KeyOffsetCache {
    shards: Box<[KeyShard]>,
    /// Maximum resident entries PER shard. Enforced manually in
    /// [`insert`](Self::insert) (`pop_lru` while over cap) since each shard is an
    /// `unbounded` `LruCache`. Always `>= 1` for an enabled cache; `0` when
    /// [`disabled`](Self::disabled).
    per_shard_cap: usize,
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
        // At least one entry per shard so the cache can always retain the entry it
        // just resolved. Each shard is an `unbounded` `LruCache` whose bucket array
        // grows with actual occupancy; the cap is enforced manually on insert.
        let per_shard_cap = (total_entries / shard_count).max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(LruCache::unbounded()));
        }
        Self {
            shards: shards.into_boxed_slice(),
            per_shard_cap,
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
            shards: vec![Mutex::new(LruCache::unbounded())].into_boxed_slice(),
            per_shard_cap: 0,
            mask: 0,
            disabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Poison-tolerant lock: recover the guard if a prior holder panicked, so a
    /// single panic cannot turn the cache into a panic-for-everyone (design D3).
    #[inline]
    fn lock(m: &KeyShard) -> std::sync::MutexGuard<'_, LruCache<Box<[u8]>, PartitionLoc>> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    fn shard_for(&self, key: &[u8]) -> &KeyShard {
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
    /// this is a no-op. The owning shard evicts its LRU entry(ies) if inserting
    /// pushes it over the per-shard cap.
    pub fn insert(&self, key: &[u8], loc: PartitionLoc) {
        if self.disabled {
            return;
        }
        let mut guard = Self::lock(self.shard_for(key));
        // Each shard is an `unbounded` `LruCache`, so `put` never evicts: it inserts
        // (or updates in place) and promotes the key to most-recently-used. Enforce
        // the per-shard cap manually — after the insert the new key is MRU, so
        // `pop_lru` removes the genuine least-recently-used entry. The loop tolerates
        // a cap that was lowered between inserts; normally it pops at most once.
        guard.put(Box::from(key), loc);
        while guard.len() > self.per_shard_cap {
            if guard.pop_lru().is_none() {
                break;
            }
        }
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
        // Shards are `unbounded` `LruCache`s (their `cap()` is `usize::MAX`); the
        // real bound is the manually enforced per-shard cap.
        self.per_shard_cap.saturating_mul(self.shards.len())
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
