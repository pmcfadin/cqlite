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
//!   by construction.
//! - **Per-reader** (D2): a location is meaningful only within one SSTable's offset
//!   domain, so each reader owns its cache. Invalidation is trivial — the cache dies
//!   with the reader on remove/reload (the audit's "SSTables are immutable, evict by
//!   sstable identity" rule), so there is never a stale-location hazard.
//! - **Sharded `Mutex<Shard>`, APPROXIMATE-BYTE bounded** (D3): `LruCache::get`
//!   mutates recency (hence a `Mutex`, not an `RwLock`), hand-sharded (power-of-two,
//!   masked) so the hit path locks exactly ONE shard, never a process-wide lock —
//!   B1's concurrency rule. Each shard is an `LruCache::unbounded()` whose resident
//!   footprint is bounded MANUALLY by an approximate-BYTE budget (not a raw entry
//!   count), mirroring B1's [`DecompressedChunkCache`]. Partition keys are
//!   variable-length (composite/text, up to Cassandra's ~64 KB), so bounding by
//!   entry *count* would NOT bound resident *bytes* — a count cap of 4096 with
//!   worst-case ~1 KB keys across ~40 readers is ~160 MB, breaching the `<128MB`
//!   budget. Bounding by bytes makes the aggregate footprint independent of key
//!   size (see [`DEFAULT_KEY_CACHE_BYTES`]). Each shard's bucket array still grows
//!   only with actual occupancy, so an idle/empty reader's cache costs ~nothing.
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

/// Approximate per-entry byte overhead ON TOP OF the key's own `len()` bytes.
///
/// An entry is a `Box<[u8]>` key plus its [`PartitionLoc`] value, held in an
/// `LruCache` intrusive-list node. This const covers everything except the key's
/// payload bytes (which are charged separately via `key.len()`):
/// - the `Box<[u8]>` fat pointer (16 B) + heap-allocation header/rounding for the
///   key's own allocation,
/// - the [`PartitionLoc`] value stored inline in the node (12 B),
/// - the `LruCache` node itself (a `HashMap` bucket slot + two `NonNull` sibling
///   links) and the map's load-factor slack.
///
/// Rounded generously to 64 B so the byte accounting OVER-estimates rather than
/// under-estimates true resident memory (the budget must never be silently
/// exceeded). This makes [`entry_cost`] a conservative upper bound.
const PER_ENTRY_OVERHEAD: usize = 64;

/// Approximate resident cost of caching one entry whose key is `key_len` bytes:
/// the key's own payload plus the fixed [`PER_ENTRY_OVERHEAD`]. This is the unit
/// the per-shard byte budget is enforced against, so a shard holds fewer large-key
/// entries and more small-key entries — the resident BYTES stay bounded regardless
/// of key size, which is the whole point of a byte budget over a count cap.
#[inline]
const fn entry_cost(key_len: usize) -> usize {
    key_len.saturating_add(PER_ENTRY_OVERHEAD)
}

/// Default **per-reader** key-cache BYTE budget.
///
/// This cache is per-reader (design D2): each open SSTable reader owns one, so the
/// resident memory that matters for the `<128MB` budget is the **aggregate** across
/// every concurrently-open reader. There is no global ceiling across readers, so
/// this per-reader budget must be chosen so a generous open-reader count stays
/// comfortably within budget.
///
/// # Why bytes, not an entry count (the #1570 roborev fix)
///
/// The previous bound was an entry *count* (`4096`/reader). Partition keys are
/// variable-length (composite/text keys run to Cassandra's ~64 KB limit), so a
/// count cap does NOT bound resident *bytes*: worst case `~40 readers × 4096
/// entries × ~1 KB keys ≈ 160 MB` — over the hard `<128MB` budget. The `~80 B/entry`
/// math the count cap assumed only holds for small (UUID-sized) keys. Bounding by
/// bytes makes the aggregate footprint **independent of key size**.
///
/// # Aggregate-footprint math (byte-bounded, key-size-independent)
///
/// With a per-reader budget `B` and `N` concurrently-open readers the worst-case
/// aggregate resident footprint is exactly `N × B` — regardless of individual key
/// sizes, because each reader's cache evicts to stay within `B` bytes.
///
/// - `B = 512 KiB` per reader.
/// - A point-read-heavy workload with ~40 open generations: `40 × 512 KiB ≈ 20 MB`
///   — well within `<128MB`, leaving the majority for B1's decompressed-chunk cache
///   and the working set.
/// - Even a very generous ~128 open readers is `128 × 512 KiB = 64 MB`, still within
///   budget. The footprint no longer depends on whether keys are 16 B UUIDs or 1 KB
///   composites — the byte budget bounds it either way.
///
/// Each shard's bucket array still grows only with actual occupancy (unbounded
/// `LruCache` + manual byte-budget eviction), so an idle/empty reader's cache costs
/// ~nothing; the budget bounds only the fully-occupied worst case.
///
/// A single **global bounded cache** keyed on `(sstable_id, key)` (Cassandra's
/// key-cache model) would bound the aggregate regardless of reader count and is
/// noted as a deferred future optimization (see `design.md` "Deferred"); it is a
/// larger architectural change beyond B4's audit-approved per-reader scope.
pub const DEFAULT_KEY_CACHE_BYTES: usize = 512 * 1024;

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
/// to the resolved [`PartitionLoc`], plus a running resident-byte counter. The
/// byte budget is enforced manually on insert (`pop_lru` while over budget) so the
/// bucket array grows only with occupancy.
struct Shard {
    lru: LruCache<Box<[u8]>, PartitionLoc>,
    /// Sum of [`entry_cost`] over the shard's resident entries. Maintained on every
    /// insert, in-place replacement, and eviction.
    current_bytes: usize,
}

impl Shard {
    fn new() -> Self {
        Self {
            // Used UNBOUNDED by count; the byte budget is enforced explicitly in
            // `insert` via `pop_lru`. `LruCache::unbounded` never evicts on its own.
            lru: LruCache::unbounded(),
            current_bytes: 0,
        }
    }
}

/// A bounded, sharded key→partition-offset cache.
pub struct KeyOffsetCache {
    shards: Box<[Mutex<Shard>]>,
    /// Approximate BYTE budget PER shard. Enforced manually in
    /// [`insert`](Self::insert) (`pop_lru` while over budget) since each shard is an
    /// `unbounded` `LruCache`. Always `>= entry_cost(0)` for an enabled cache so a
    /// shard can always retain one just-resolved entry; `0` when
    /// [`disabled`](Self::disabled).
    per_shard_bytes: usize,
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
            .field("per_shard_bytes", &self.per_shard_bytes)
            .field("disabled", &self.disabled)
            .field("len", &self.len())
            .field("resident_bytes", &self.resident_bytes())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl KeyOffsetCache {
    /// Create a cache with `total_budget_bytes` split across
    /// [`DEFAULT_KEY_CACHE_SHARDS`] shards.
    pub fn with_budget_bytes(total_budget_bytes: usize) -> Self {
        Self::with_budget_and_shards(total_budget_bytes, DEFAULT_KEY_CACHE_SHARDS)
    }

    /// Create a cache with `total_budget_bytes` split across `shard_count` shards.
    ///
    /// `shard_count` is rounded UP to the next power of two (min 1) so shard
    /// selection can mask instead of modulo. Unit tests use `shard_count = 1` for
    /// deterministic eviction ordering; production uses [`DEFAULT_KEY_CACHE_SHARDS`].
    pub fn with_budget_and_shards(total_budget_bytes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1).next_power_of_two();
        // At least one minimal entry per shard so the cache can always retain the
        // entry it just resolved. Each shard is an `unbounded` `LruCache` whose
        // bucket array grows with actual occupancy; the budget is enforced manually
        // on insert.
        let per_shard_bytes = (total_budget_bytes / shard_count).max(entry_cost(0));
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(Shard::new()));
        }
        Self {
            shards: shards.into_boxed_slice(),
            per_shard_bytes,
            mask: shard_count - 1,
            disabled: false,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a genuine no-op cache (honoring `block_cache.enabled == false`):
    /// reads bypass it entirely. [`get`](Self::get) always returns `None` (no
    /// counters touched), [`insert`](Self::insert) retains nothing, and
    /// `len()`/`budget_bytes()`/`resident_bytes()` all report `0`.
    pub fn disabled() -> Self {
        Self {
            // One dummy shard so shard indexing stays valid; nothing is ever stored.
            shards: vec![Mutex::new(Shard::new())].into_boxed_slice(),
            per_shard_bytes: 0,
            mask: 0,
            disabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Poison-tolerant lock: recover the guard if a prior holder panicked, so a
    /// single panic cannot turn the cache into a panic-for-everyone (design D3).
    #[inline]
    fn lock(m: &Mutex<Shard>) -> std::sync::MutexGuard<'_, Shard> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    fn shard_for(&self, key: &[u8]) -> &Mutex<Shard> {
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
        match guard.lru.get(key) {
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
    /// this is a no-op. The owning shard evicts LRU entries until it is within its
    /// per-shard byte budget (never evicting the just-inserted MRU entry).
    pub fn insert(&self, key: &[u8], loc: PartitionLoc) {
        if self.disabled {
            return;
        }
        let cost = entry_cost(key.len());
        let mut guard = Self::lock(self.shard_for(key));

        // Each shard is an `unbounded` `LruCache`, so `put` never evicts: it inserts
        // (or updates in place) and promotes the key to most-recently-used. Replacing
        // an existing key reclaims the old entry's byte weight first — the key bytes
        // are identical, so `cost` is the same for old and new, but subtract-then-add
        // keeps the counter exact and matches the sibling chunk cache's discipline.
        if guard.lru.put(Box::from(key), loc).is_some() {
            guard.current_bytes = guard.current_bytes.saturating_sub(cost);
        }
        guard.current_bytes = guard.current_bytes.saturating_add(cost);

        // Evict LRU entries until within the byte budget. The just-inserted key is
        // now most-recently-used, so `pop_lru` never targets it while other entries
        // remain. The `len() > 1` guard keeps the entry we just resolved resident
        // even if it alone exceeds the budget (documented: single oversized entry).
        while guard.current_bytes > self.per_shard_bytes && guard.lru.len() > 1 {
            match guard.lru.pop_lru() {
                Some((evicted_key, _)) => {
                    let reclaimed = entry_cost(evicted_key.len());
                    guard.current_bytes = guard.current_bytes.saturating_sub(reclaimed);
                }
                None => break,
            }
        }
    }

    /// Total resident entry count across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|m| Self::lock(m).lru.len()).sum()
    }

    /// Whether the cache currently holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total approximate resident bytes across all shards (sum of [`entry_cost`]
    /// over resident entries). A [`disabled`](Self::disabled) no-op cache reports
    /// `0`.
    pub fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|m| Self::lock(m).current_bytes)
            .sum()
    }

    /// The configured total byte budget (`per_shard_bytes × shard count`). A
    /// [`disabled`](Self::disabled) no-op cache reports `0`.
    pub fn budget_bytes(&self) -> usize {
        self.per_shard_bytes.saturating_mul(self.shards.len())
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
        Self::with_budget_bytes(DEFAULT_KEY_CACHE_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A budget that holds exactly `n` entries whose keys are `key_len` bytes each,
    /// on a single shard (so eviction is deterministic — no wall-clock, no shard
    /// spread). Used to make byte-budget eviction assertions exact.
    fn budget_for(n: usize, key_len: usize) -> usize {
        entry_cost(key_len) * n
    }

    /// Task 1.1: eviction order — single shard sized for exactly 2 entries; insert
    /// A, insert B, access A, insert C → B (LRU) evicted, A and C survive.
    #[test]
    fn eviction_order_lru_single_shard() {
        // All keys are 5 bytes ("key-X"), so 2 entries' worth of byte budget holds
        // exactly two of them.
        let cache = KeyOffsetCache::with_budget_and_shards(budget_for(2, 5), 1);
        let a = b"key-A".as_slice();
        let b = b"key-B".as_slice();
        let c = b"key-C".as_slice();

        cache.insert(a, PartitionLoc::new(10, 100));
        cache.insert(b, PartitionLoc::new(20, 200));
        // Access A so it becomes more-recently-used than B.
        assert_eq!(cache.get(a), Some(PartitionLoc::new(10, 100)));
        // Insert C: over budget → evict the LRU, which is now B.
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
        assert_eq!(cache.len(), 2);
        assert!(cache.resident_bytes() <= cache.budget_bytes());
    }

    /// Task 1.2: BYTE bound — inserting more distinct equal-size keys than the
    /// budget holds keeps resident bytes within the budget after every insert.
    #[test]
    fn resident_bytes_bounded() {
        // Budget for exactly 4 entries whose keys are 6 bytes ("key-NN").
        let key_len = 6;
        let cache = KeyOffsetCache::with_budget_and_shards(budget_for(4, key_len), 1);
        for i in 0..50u64 {
            let key = format!("key-{i:02}");
            assert_eq!(key.len(), key_len, "test assumes fixed-width keys");
            cache.insert(key.as_bytes(), PartitionLoc::new(i, i as u32));
            assert!(
                cache.resident_bytes() <= cache.budget_bytes(),
                "resident {} exceeded budget {} after insert {}",
                cache.resident_bytes(),
                cache.budget_bytes(),
                i
            );
            assert!(
                cache.len() <= 4,
                "resident count exceeded 4 after insert {i}"
            );
        }
    }

    /// Byte-budget eviction is key-SIZE aware: a large key costs more budget, so a
    /// shard sized for a handful of small keys holds fewer large keys. This is the
    /// property the count cap lacked (the #1570 fix).
    #[test]
    fn large_keys_evict_sooner_than_small_keys() {
        // Budget = room for exactly 4 small (4-byte) keys.
        let small_len = 4;
        let budget = budget_for(4, small_len);
        let cache = KeyOffsetCache::with_budget_and_shards(budget, 1);

        // Each large key costs `entry_cost(budget/2) = budget/2 + PER_ENTRY_OVERHEAD`,
        // so any two of them together exceed the shard budget (one fits, two do not).
        let big = vec![b'x'; budget / 2];
        let mut big1 = big.clone();
        big1[0] = b'a';
        let mut big2 = big.clone();
        big2[0] = b'b';
        cache.insert(&big1, PartitionLoc::new(1, 1));
        cache.insert(&big2, PartitionLoc::new(2, 2));

        // Both large keys cannot be resident at once under the byte budget, so the
        // LRU (big1) is evicted — a count cap of 4 would have kept both.
        assert!(cache.resident_bytes() <= cache.budget_bytes());
        assert_eq!(cache.get(&big1), None, "LRU large key must be evicted");
        assert_eq!(cache.get(&big2), Some(PartitionLoc::new(2, 2)));
    }

    /// Task 1.3: a hit returns the stored location; a different key never aliases;
    /// a never-inserted key misses (parity / no-fabrication).
    #[test]
    fn hit_returns_stored_location_no_alias() {
        let cache = KeyOffsetCache::with_budget_and_shards(DEFAULT_KEY_CACHE_BYTES, 1);
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

    /// Re-inserting the same key must not double-count its bytes (in-place
    /// replacement subtracts the old cost before adding the new).
    #[test]
    fn reinsert_same_key_does_not_grow_resident_bytes() {
        let cache = KeyOffsetCache::with_budget_and_shards(DEFAULT_KEY_CACHE_BYTES, 1);
        let k = b"stable-key".as_slice();
        cache.insert(k, PartitionLoc::new(1, 1));
        let after_first = cache.resident_bytes();
        cache.insert(k, PartitionLoc::new(2, 2));
        cache.insert(k, PartitionLoc::new(3, 3));
        assert_eq!(cache.len(), 1, "re-insert must not add a second entry");
        assert_eq!(
            cache.resident_bytes(),
            after_first,
            "re-inserting the same key must not grow resident bytes"
        );
        assert_eq!(cache.get(k), Some(PartitionLoc::new(3, 3)));
    }

    /// Task 1.4: `disabled()` is a genuine no-op (get misses, insert retains
    /// nothing, occupancy + byte counters report zero).
    #[test]
    fn disabled_cache_is_a_genuine_no_op() {
        let cache = KeyOffsetCache::disabled();
        assert_eq!(cache.budget_bytes(), 0);

        cache.insert(b"k".as_slice(), PartitionLoc::new(1, 2));
        assert_eq!(
            cache.get(b"k".as_slice()),
            None,
            "disabled cache never retains"
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.resident_bytes(), 0);
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

        let cache = Arc::new(KeyOffsetCache::with_budget_and_shards(
            DEFAULT_KEY_CACHE_BYTES,
            1,
        ));
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
        let cache = KeyOffsetCache::with_budget_and_shards(DEFAULT_KEY_CACHE_BYTES, 10);
        assert_eq!(cache.shards.len(), 16);
        assert_eq!(cache.mask, 15);
    }
}
