//! Shared, bytes-bounded, sharded decompressed-chunk cache (issue #1567, Epic B/B1).
//!
//! A cache of *decompressed* SSTable compression chunks keyed by authoritative
//! `(sstable identity, chunk index)`. It is the single biggest lever for
//! repeated-read latency identified by the July 2026 read-path audit: every
//! wired read site consults it before reading+decompressing, so a repeat read of
//! a resident chunk is a refcount bump instead of a disk read + decompress.
//!
//! # Design (owner decision #1, LOCKED — see
//! `openspec/changes/decompressed-chunk-cache/design.md`)
//!
//! - **Value = `Arc<[u8]>`** (D3): a hit is `Arc::clone` (refcount bump), never a
//!   chunk-sized memcpy. Insert converts the decompressed `Vec<u8>` once.
//! - **Bytes-bounded, not entry-count** (spec R1): each entry is weighed by its
//!   decompressed length; after an insert the owning shard evicts LRU entries
//!   until it is within its byte budget. A single entry larger than the budget is
//!   retained (we never evict below one live entry) — the read path must always be
//!   able to return the chunk it just produced.
//! - **Hand-sharded `Mutex<LruCache>`** (D2): `shards.len()` is a power of two.
//!   The hit path locks exactly ONE shard and calls `LruCache::get` (which mutates
//!   recency — hence a `Mutex`, not an `RwLock`; but sharded so contention is
//!   `1/N`, never a single process-wide lock). Reuses the tested `lru` crate
//!   internals; no new external dependency.
//! - **Poison-tolerant** (D2): every lock is taken with
//!   `lock().unwrap_or_else(|e| e.into_inner())` so one panicking thread cannot
//!   turn the cache into a panic-for-everyone. No `unwrap()`/`expect()` here.
//! - **No-heuristics** (mandate #28): keys are `(u64 sstable id, u64 chunk index)`
//!   derived from authoritative reader identity + chunk offsets — never inferred
//!   from decompressed byte content.
//!
//! The default is a `Box<[Mutex<Shard>]>` rather than a fixed `[Mutex<Shard>; N]`
//! array so the shard count is a constructor parameter: production uses
//! [`DEFAULT_SHARDS`], while unit tests use a single shard for deterministic
//! eviction ordering. Both remain power-of-two hand-sharded `Mutex<LruCache>` —
//! the design intent is preserved.

use lru::LruCache;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Default shard count (power of two). Production readers use this via
/// [`DecompressedChunkCache::with_budget_bytes`].
pub const DEFAULT_SHARDS: usize = 16;

/// Default total byte budget when a cache is constructed without an explicit
/// budget (mirrors `config.memory.block_cache.max_size`'s 256 MiB default).
pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Authoritative cache key: an SSTable identity hash and a chunk discriminator.
///
/// `chunk_index` carries whichever authoritative offset the wiring site uses —
/// a real chunk index (windowed scan, BTI target chunk) or an index-resolved
/// `block_offset` (BIG point read). Sites keep separate key namespaces by folding
/// a site salt into `sstable` (design D4), so numerically-overlapping values from
/// different sites never collide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    /// Stable hash of the SSTable's `file_path` (+ generation), optionally XORed
    /// with a per-site namespace salt.
    pub sstable: u64,
    /// Authoritative chunk index or index-resolved offset within `sstable`.
    pub chunk_index: u64,
    /// Extra discriminant for a size-dependent range read. The chunk-index sites
    /// (BTI, windowed scan) decompress a whole compression chunk whose bytes are
    /// fully determined by `chunk_index`, so they use `aux = 0`. The BIG
    /// point-read path (`get_cached_data`) reads an index-resolved
    /// `(block_offset, size)` byte range whose decompressed content depends on
    /// BOTH the offset AND the length: two reads at the same `block_offset` with
    /// different `size` decompress different input and MUST NOT alias. `aux`
    /// carries `size` for that path so the key is complete (roborev #1567), and
    /// because `(u64 offset, u32 size)` cannot be packed into one `u64` without
    /// loss, `aux` is a first-class key field, not folded into `chunk_index`.
    pub aux: u64,
}

impl ChunkKey {
    /// Construct a key from an sstable identity hash and a chunk discriminator
    /// (whole-chunk sites; `aux = 0`).
    #[inline]
    pub fn new(sstable: u64, chunk_index: u64) -> Self {
        Self {
            sstable,
            chunk_index,
            aux: 0,
        }
    }

    /// Construct a key with an extra discriminant (`aux`) for a size-dependent
    /// range read (the BIG point-read path keys `aux = size`).
    #[inline]
    pub fn with_aux(sstable: u64, chunk_index: u64, aux: u64) -> Self {
        Self {
            sstable,
            chunk_index,
            aux,
        }
    }
}

/// One shard: an unbounded-by-count `LruCache` plus running byte accounting.
struct Shard {
    lru: LruCache<ChunkKey, Arc<[u8]>>,
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

/// A shared, bytes-bounded, sharded decompressed-chunk cache.
pub struct DecompressedChunkCache {
    shards: Box<[Mutex<Shard>]>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    mask: usize,
    /// Per-shard byte budget (`total_budget / shards.len()`, at least 1).
    budget_per_shard: usize,
    /// When `true` this is a genuine no-op cache (issue #1568): `get` never
    /// stores/returns anything and `insert` never retains, so reads bypass the
    /// cache entirely. Built by [`disabled`](Self::disabled) when
    /// `block_cache.enabled == false`. Distinct from a zero-budget cache, which
    /// would still retain one oversized entry per shard.
    disabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for DecompressedChunkCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecompressedChunkCache")
            .field("shards", &self.shards.len())
            .field("budget_per_shard", &self.budget_per_shard)
            .field("resident_bytes", &self.resident_bytes())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl DecompressedChunkCache {
    /// Create a cache with `total_budget_bytes` split across [`DEFAULT_SHARDS`]
    /// shards.
    pub fn with_budget_bytes(total_budget_bytes: usize) -> Self {
        Self::with_budget_and_shards(total_budget_bytes, DEFAULT_SHARDS)
    }

    /// Create a cache with `total_budget_bytes` split across `shard_count` shards.
    ///
    /// `shard_count` is rounded UP to the next power of two (min 1) so shard
    /// selection can mask instead of modulo. Unit tests use `shard_count = 1` for
    /// deterministic eviction ordering; production uses [`DEFAULT_SHARDS`].
    pub fn with_budget_and_shards(total_budget_bytes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1).next_power_of_two();
        let budget_per_shard = (total_budget_bytes / shard_count).max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(Shard::new()));
        }
        Self {
            shards: shards.into_boxed_slice(),
            mask: shard_count - 1,
            budget_per_shard,
            disabled: false,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a genuine no-op cache (issue #1568): reads bypass it entirely.
    ///
    /// Used when `config.memory.block_cache.enabled == false` so the advertised
    /// toggle really disables caching instead of being decorative. [`get`](Self::get)
    /// always returns `None` (no counters touched), [`insert`](Self::insert) returns
    /// the `Arc` without retaining it, and `resident_bytes()` / `len()` /
    /// `budget_bytes()` all report `0`. A single dummy shard is allocated only so
    /// shard indexing stays valid; nothing is ever stored in it.
    pub fn disabled() -> Self {
        Self {
            shards: vec![Mutex::new(Shard::new())].into_boxed_slice(),
            mask: 0,
            budget_per_shard: 0,
            disabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Poison-tolerant lock: recover the guard if a prior holder panicked, so a
    /// single panic cannot turn the cache into a panic-for-everyone (design D2).
    #[inline]
    fn lock(m: &Mutex<Shard>) -> std::sync::MutexGuard<'_, Shard> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    fn shard_for(&self, key: &ChunkKey) -> &Mutex<Shard> {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        let idx = (h.finish() as usize) & self.mask;
        // `idx <= mask < shards.len()`, so this index is always in bounds.
        &self.shards[idx]
    }

    /// Look up a resident chunk. On a hit this bumps recency and returns an
    /// `Arc::clone` (no chunk-sized allocation). On a miss returns `None`.
    pub fn get(&self, key: &ChunkKey) -> Option<Arc<[u8]>> {
        // No-op cache (issue #1568): a disabled cache never holds anything, so
        // reads bypass it without touching the hit/miss counters.
        if self.disabled {
            return None;
        }
        let mut guard = Self::lock(self.shard_for(key));
        match guard.lru.get(key) {
            Some(v) => {
                let v = Arc::clone(v);
                drop(guard);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            None => {
                drop(guard);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert `data` under `key`, returning the resident `Arc<[u8]>`.
    ///
    /// The `Vec<u8>` is converted to `Arc<[u8]>` exactly once here; the returned
    /// handle and any subsequent [`get`](Self::get) share that one buffer. After
    /// insertion the owning shard evicts LRU entries until it is within its byte
    /// budget (never evicting the just-inserted entry).
    pub fn insert(&self, key: ChunkKey, data: Vec<u8>) -> Arc<[u8]> {
        let arc: Arc<[u8]> = Arc::from(data.into_boxed_slice());
        // No-op cache (issue #1568): return the freshly-produced buffer to the
        // caller without retaining it, so a disabled cache holds nothing.
        if self.disabled {
            return arc;
        }
        let len = arc.len();
        let mut guard = Self::lock(self.shard_for(&key));

        // Replacing an existing entry: reclaim its byte weight first.
        if let Some(old) = guard.lru.put(key, Arc::clone(&arc)) {
            guard.current_bytes = guard.current_bytes.saturating_sub(old.len());
        }
        guard.current_bytes = guard.current_bytes.saturating_add(len);

        // Evict LRU entries until within budget. The just-inserted key is now
        // most-recently-used, so `pop_lru` never targets it while other entries
        // remain. The `len() > 1` guard keeps the chunk we just produced resident
        // even if it alone exceeds the budget (documented: single oversized entry).
        while guard.current_bytes > self.budget_per_shard && guard.lru.len() > 1 {
            match guard.lru.pop_lru() {
                Some((_, evicted)) => {
                    guard.current_bytes = guard.current_bytes.saturating_sub(evicted.len());
                }
                None => break,
            }
        }
        arc
    }

    /// Total resident decompressed bytes across all shards.
    pub fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|m| Self::lock(m).current_bytes)
            .sum()
    }

    /// Total resident entry count across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|m| Self::lock(m).lru.len()).sum()
    }

    /// Whether the cache currently holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The configured total byte budget (`budget_per_shard * shard count`).
    ///
    /// A [`disabled`](Self::disabled) no-op cache reports `0` (it holds nothing).
    pub fn budget_bytes(&self) -> usize {
        self.budget_per_shard * self.shards.len()
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

impl Default for DecompressedChunkCache {
    fn default() -> Self {
        Self::with_budget_bytes(DEFAULT_BUDGET_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    /// Task 1.1: eviction order — budget holds exactly 2 equal chunks; access
    /// A, B, A, then insert C → B is evicted (LRU), A and C survive.
    #[test]
    fn eviction_order_lru() {
        // Single shard for deterministic ordering. Budget = 2 * 100 bytes.
        let cache = DecompressedChunkCache::with_budget_and_shards(200, 1);
        let a = ChunkKey::new(1, 0);
        let b = ChunkKey::new(1, 1);
        let c = ChunkKey::new(1, 2);

        cache.insert(a, chunk(0xAA, 100));
        cache.insert(b, chunk(0xBB, 100));
        // Access A so it becomes more-recently-used than B.
        assert!(cache.get(&a).is_some());
        // Insert C: over budget (300 > 200) → evict LRU, which is now B.
        cache.insert(c, chunk(0xCC, 100));

        assert!(cache.get(&a).is_some(), "A (recently used) must survive");
        assert!(cache.get(&c).is_some(), "C (just inserted) must survive");
        assert!(
            cache.get(&b).is_none(),
            "B (least recently used) must be evicted"
        );
        assert!(cache.resident_bytes() <= 200);
    }

    /// Task 1.2: byte budget — inserting more distinct chunks than the budget can
    /// hold keeps resident bytes within budget after every insert.
    #[test]
    fn byte_budget_bounded() {
        let budget = 500usize;
        let cache = DecompressedChunkCache::with_budget_and_shards(budget, 1);
        for i in 0..50u64 {
            cache.insert(ChunkKey::new(7, i), chunk(i as u8, 100));
            assert!(
                cache.resident_bytes() <= budget,
                "resident {} exceeded budget {} after insert {}",
                cache.resident_bytes(),
                budget,
                i
            );
        }
        // At 100 bytes/entry and a 500-byte budget, at most 5 entries survive.
        assert!(
            cache.len() <= 5,
            "entry count must stay bounded (got {})",
            cache.len()
        );
    }

    /// A single entry larger than the budget is retained (never evict below one
    /// live entry) — the read path must return the chunk it just produced.
    #[test]
    fn single_oversized_entry_retained() {
        let cache = DecompressedChunkCache::with_budget_and_shards(100, 1);
        let k = ChunkKey::new(1, 0);
        cache.insert(k, chunk(0xEE, 4096));
        assert!(
            cache.get(&k).is_some(),
            "oversized entry must remain resident"
        );
        assert_eq!(cache.len(), 1);
    }

    /// Task 1.3: zero-copy hit — insert once, get twice; every handle points at
    /// the SAME underlying buffer (Arc pointer identity), no chunk-sized copy.
    #[test]
    fn zero_copy_hit_pointer_identity() {
        let cache = DecompressedChunkCache::with_budget_and_shards(1 << 20, 1);
        let k = ChunkKey::new(3, 9);
        let inserted = cache.insert(k, chunk(0x42, 1024));

        let h1 = cache.get(&k).expect("first hit");
        let h2 = cache.get(&k).expect("second hit");

        // Same allocation: pointer identity, not a value clone.
        assert!(Arc::ptr_eq(&inserted, &h1));
        assert!(Arc::ptr_eq(&inserted, &h2));
        assert_eq!(h1.as_ptr(), inserted.as_ptr());
        assert_eq!(&*h1, &chunk(0x42, 1024)[..]);
    }

    /// Task 1.4: concurrency soundness — many threads read overlapping + disjoint
    /// keys under eviction pressure. Every returned buffer is complete/correct,
    /// resident bytes stay within budget, no panic.
    #[test]
    fn concurrency_soundness() {
        use std::thread;

        // Small budget vs working set so eviction runs constantly.
        let cache = Arc::new(DecompressedChunkCache::with_budget_bytes(64 * 1024));
        let chunk_len = 256usize;
        let n_keys = 512u64;

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for round in 0..2000u64 {
                    let idx = (t.wrapping_mul(31).wrapping_add(round)) % n_keys;
                    let key = ChunkKey::new(1, idx);
                    let expect_byte = idx as u8;
                    let got = match cache.get(&key) {
                        Some(v) => v,
                        None => cache.insert(key, vec![expect_byte; chunk_len]),
                    };
                    // Whatever we got must be a complete, correct chunk for `idx`
                    // (never torn/partial, never another key's bytes).
                    assert_eq!(got.len(), chunk_len);
                    assert!(got.iter().all(|&b| b == expect_byte));
                }
            }));
        }
        for h in handles {
            h.join().expect("worker thread must not panic");
        }
        assert!(
            cache.resident_bytes() <= cache.budget_bytes(),
            "resident {} exceeded budget {}",
            cache.resident_bytes(),
            cache.budget_bytes()
        );
    }

    /// Poison recovery: a panic while holding a shard lock must not wedge the
    /// cache — subsequent ops recover the guard and continue.
    #[test]
    fn poisoned_lock_recovers() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::thread;

        let cache = Arc::new(DecompressedChunkCache::with_budget_and_shards(1 << 20, 1));
        let k = ChunkKey::new(1, 0);
        cache.insert(k, chunk(0x55, 16));

        // Poison the single shard's mutex by panicking while holding it.
        let poison_cache = Arc::clone(&cache);
        let _ = thread::spawn(move || {
            let _guard = DecompressedChunkCache::lock(&poison_cache.shards[0]);
            panic!("intentional poison");
        })
        .join();

        // The cache must still serve reads/writes without panicking.
        let res = catch_unwind(AssertUnwindSafe(|| {
            let hit = cache.get(&k);
            cache.insert(ChunkKey::new(1, 1), chunk(0x66, 16));
            hit.is_some()
        }));
        assert_eq!(
            res.ok(),
            Some(true),
            "cache must recover from a poisoned lock"
        );
    }

    #[test]
    fn shard_count_rounds_to_power_of_two() {
        let cache = DecompressedChunkCache::with_budget_and_shards(1 << 20, 10);
        assert_eq!(cache.shards.len(), 16);
        assert_eq!(cache.mask, 15);
    }

    /// roborev #1567: a size-dependent range read (BIG point-read path) keyed by
    /// offset alone would return the first-cached range when the same offset is
    /// later read with a different size. The `aux` discriminant (size) makes the
    /// key complete, so a same-offset/different-size read MISSES rather than
    /// aliasing to the wrong bytes.
    #[test]
    fn ranged_key_does_not_alias_same_offset_different_size() {
        let cache = DecompressedChunkCache::with_budget_bytes(1 << 20);
        let sstable = 0xABCD_u64;
        let offset = 4096_u64;

        // Cache a 16-byte range at `offset`.
        let k16 = ChunkKey::with_aux(sstable, offset, 16);
        let v16 = cache.insert(k16, chunk(0x11, 16));

        // A read at the SAME offset but a DIFFERENT size must not hit `v16`.
        let k32 = ChunkKey::with_aux(sstable, offset, 32);
        assert!(
            cache.get(&k32).is_none(),
            "same offset with a different size must not alias the cached range"
        );

        // The original size still hits the original bytes (Arc identity).
        let again = cache.get(&k16).expect("original ranged key still resident");
        assert!(
            Arc::ptr_eq(&v16, &again),
            "same (offset,size) key returns the same buffer"
        );

        // aux == 0 (whole-chunk sites) is a distinct namespace from any sized read.
        let k0 = ChunkKey::new(sstable, offset);
        assert!(
            cache.get(&k0).is_none(),
            "whole-chunk key (aux=0) must not alias a sized range read"
        );
    }

    /// Issue #1568: `disabled()` is a genuine no-op cache. `insert` returns the
    /// produced buffer without retaining it, `get` always misses, and every
    /// occupancy/budget/counter accessor reports zero — so a read path wired to
    /// it truly bypasses caching (used when `block_cache.enabled == false`).
    #[test]
    fn disabled_cache_is_a_genuine_no_op() {
        let cache = DecompressedChunkCache::disabled();
        assert_eq!(cache.budget_bytes(), 0, "disabled cache has no budget");

        let key = ChunkKey::new(1, 0);
        let arc = cache.insert(key, chunk(0xEE, 4096));
        assert_eq!(
            arc.len(),
            4096,
            "insert still hands back the produced buffer"
        );

        assert!(cache.get(&key).is_none(), "disabled cache never retains");
        assert_eq!(cache.resident_bytes(), 0);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        // Bypass touches no counters: a disabled cache reports a structural zero.
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
    }
}
