//! Process-global, byte-bounded, sharded key→partition-offset cache (issue #2059,
//! Epic B / memory lane; Cassandra's `AutoSavingCache`/`KeyCacheKey` key-cache model).
//!
//! Replaces the per-reader [`KeyOffsetCache`](super::KeyOffsetCache) (#1570) with ONE
//! process-global instance shared by every open `SSTableReader`, so aggregate resident
//! key-offset memory is bounded by a single fixed global byte budget REGARDLESS of how
//! many readers are concurrently open — never `N_readers × per_reader_cap` (the
//! unbounded-aggregate hazard the flight `WarmTableRegistry` reintroduced by pinning one
//! `Arc<SSTableReader>` per warm generation). Post-#2412 (Summary-guided lazy BIG index)
//! a cache HIT skips the whole `Index.db` interval read; a MISS pays one interval parse
//! then populates.
//!
//! # Design (`openspec/changes/bounded-key-cache/design.md`)
//!
//! - **Global, not per-table (§A).** One instance shared by every reader. The generation
//!   identity in the key namespaces per-generation entries so one global byte budget bounds
//!   the whole process — Cassandra's single global key cache, not N per-table caches. Note
//!   the contrast with [`DecompressedChunkCache`](super::DecompressedChunkCache), which is
//!   *per-manager* and sized by the configurable `config.memory.block_cache.max_size`: this
//!   key cache is instead a true process-wide SINGLETON with a FIXED
//!   [`DEFAULT_GLOBAL_KEY_CACHE_BYTES`] budget that ignores `max_size`. That is a deliberate
//!   design decision (design §B: "no new user knob", the #2343 WS4 owner call), not an
//!   oversight — and not a resident-memory regression: occupancy is proportional to live
//!   entries, and the retired per-reader cache (#1570) already ignored `max_size` too.
//! - **Key = `(GenerationIdentity, raw partition key)` (§A, no-heuristics #28).** The
//!   identity is the authoritative inode-stable identity (device+inode+size+generation,
//!   #2345) — never a path hash (paths rebind under snapshots, #2383, but the offsets are
//!   identical across a rebind, so the identity must be rebind-stable). The partition key
//!   is the FULL raw key bytes (collision-free by construction; hashing to a `u64` would
//!   admit a cross-key alias — the #1570 D1 guardrail). Two generations sharing a
//!   partition key never collide because the identity differs.
//! - **Fail-closed on identity mismatch (§C).** A `get` supplies the querying reader's
//!   CURRENT identity; an entry keyed on a different identity is always a MISS, so a stale
//!   entry from a removed/replaced generation can never serve a location for a generation
//!   that no longer holds it.
//! - **Byte-bounded LRU, sharded, nested-by-identity (§B/§F).** Each shard holds a
//!   `HashMap<GenerationIdentity, LruCache<Box<[u8]>, _>>` — an inner per-generation LRU —
//!   plus a running resident-byte counter and a per-shard monotonic recency clock. Shard
//!   selection still hashes `(identity, key)` so a hot generation's keys spread across ALL
//!   shards (the #2052-class contention mitigation is NOT collapsed to one shard-per-identity);
//!   a HIGH shard count ([`DEFAULT_GLOBAL_KEY_CACHE_SHARDS`]) keeps the single global instance
//!   off the single-`Mutex` hot path — the hit path locks exactly ONE shard. Two structural
//!   wins over a flat `LruCache<(identity, key), _>`: (1) a `get`/lookup probes the inner LRU
//!   with a borrowed `&[u8]` (`Box<[u8]>: Borrow<[u8]>`), so the hot hit/miss path allocates
//!   NO owned key — only `insert` (which must own the key anyway) allocates; (2) invalidation
//!   is a per-shard O(1) `HashMap` removal, never a full LRU scan.
//! - **Single global byte budget across nested LRUs (§B).** The budget is still ONE aggregate
//!   byte cap, NOT a per-identity budget. Because entries live in separate inner LRUs, each
//!   entry carries a `seq` stamp from the per-shard recency clock (bumped on every get/insert);
//!   when a shard is over budget, eviction picks the globally-least-recently-used entry across
//!   ALL identities in that shard by comparing each inner LRU's tail `seq` (`peek_lru`) and
//!   `pop_lru`-ing the minimum. This is the cross-identity recency signal that stops one hot
//!   generation from starving another and keeps the byte bound aggregate, not per-identity.
//! - **Invalidation by identity (§C).** On generation removal / compaction / warm-registry
//!   evict, [`invalidate`](GlobalKeyOffsetCache::invalidate) drops ALL entries for that
//!   identity by removing its inner `HashMap` entry from each shard — O(matching-shards) O(1)
//!   removals that never scan an unrelated identity's entries. Dropped entries are counted by
//!   a DISTINCT `invalidations` counter (separate from budget `evictions`). A #2383
//!   rebind-by-inode does NOT invalidate (identity unchanged).
//! - **Poison-tolerant (§F).** Every lock uses `lock().unwrap_or_else(|e| e.into_inner())`.
//!   No `unwrap()`/`expect()`.

use lru::LruCache;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

/// The resolved location of a partition, exactly what the index/trie descent
/// produces.
///
/// # `data_size` is `0` from EVERY wiring site — neither format records a size
///
/// An earlier version of this comment claimed "BIG (`Index.db`) resolves both
/// fields". That is **false**, and it cost issue #2827 a design round. A Cassandra
/// 5.0 BIG index entry is
/// `[key][data_offset vint][promoted_index_len vint][promoted_index]` — there is no
/// partition-size field
/// (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`, "Index.db
/// Entry Format"; written by `BigTableWriter.createRowIndexEntry` at
/// `cassandra-5.0.8`). The BTI `Partitions.db` trie likewise resolves an offset
/// only. So `data_size` is `0` for BIG **and** BTI, and no caller may treat a zero
/// as "small partition" or a non-zero as guaranteed.
///
/// A partition's on-disk extent is instead MEASURED as the SUCCESSOR GAP —
/// `[data_offset, successor_offset)` via
/// [`SSTableReader::successor_partition_offset`](crate::storage::sstable::reader::SSTableReader),
/// bounding to the authoritative uncompressed data-section length for the last
/// partition. That is authoritative index layout metadata, not a heuristic: it is
/// what the single-partition seek path already uses to bound its decode window, and
/// what the #2827 access-distribution probe uses to weight an access.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLoc {
    /// Uncompressed `Data.db` offset the descent resolved.
    pub data_offset: u64,
    /// Partition byte size. **Always `0` in practice** — see the type doc: no
    /// Cassandra 5.0 index format records one. Retained so a future writer-side
    /// producer that genuinely knows a size has somewhere to put it.
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

/// The authoritative inode-stable identity of one SSTable generation, the
/// namespacing half of the cache key (design §A; mirrors the flight
/// `warm::identity::GenerationId` #2345, extended with `size` per the #2383
/// rebind gate). Two directory entries that are the same on-disk bytes share a
/// `(device, inode)`; `generation` + `size` are carried as cross-checks so a
/// recycled inode or a byte-changed generation is a DISTINCT identity — never a
/// stale hit for changed bytes.
///
/// `Copy` + `Hash` + `Eq`: stored inline in the LRU key tuple, hashed for shard
/// selection, and compared for the fail-closed mismatch check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GenerationIdentity {
    /// Filesystem device id of the `Data.db` (`stat.st_dev`).
    pub device: u64,
    /// Inode number of the `Data.db` (`stat.st_ino`).
    pub inode: u64,
    /// On-disk byte size of the `Data.db` (a cross-check; a byte-changed
    /// generation on a recycled inode is a distinct identity).
    pub size: u64,
    /// Generation number parsed from the SSTable file name (best-effort; `0`
    /// when unparseable). A cross-check on top of the authoritative inode.
    pub generation: u64,
    /// `Data.db` modification time in nanoseconds since the epoch (a cross-check
    /// that hardens the "same on-disk bytes" identity against inode RECYCLING: a
    /// deleted generation's inode reused by a NEW file of the same size + parsed
    /// generation number would otherwise alias. mtime is per-INODE, so a snapshot
    /// hardlink / #2383 rebind (same inode) shares it — rebind-stability is
    /// preserved — while a distinct file (rewritten or freshly created) differs.
    /// `0` when the mtime cannot be read. Authoritative fs metadata, never inferred
    /// from content (no-heuristics #28).
    pub mtime_ns: i128,
}

impl GenerationIdentity {
    /// Resolve the identity of a `Data.db` at `path` given its already-parsed
    /// `generation` number.
    ///
    /// `stat`s the file (following a snapshot hardlink to the real inode, so a
    /// snapshot dir resolves to the SAME identity as the live file — the #2345
    /// rebind-stable property). Returns `None` when the file cannot be `stat`ed
    /// (missing/racing removal) so the caller treats it as no-identity rather
    /// than fabricating one (no-heuristics #28).
    pub fn resolve(path: &std::path::Path, generation: u64) -> Option<Self> {
        let (device, inode, size, mtime_ns) = stat_identity(path)?;
        Some(Self {
            device,
            inode,
            size,
            generation,
            mtime_ns,
        })
    }
}

#[cfg(unix)]
fn stat_identity(path: &std::path::Path) -> Option<(u64, u64, u64, i128)> {
    use std::os::unix::fs::MetadataExt;
    // `metadata` (not `symlink_metadata`) so a snapshot hardlink resolves to the
    // SAME (device, inode) as the live file — the point of the inode-stable key.
    let md = std::fs::metadata(path).ok()?;
    let mtime_ns = md.mtime() as i128 * 1_000_000_000 + md.mtime_nsec() as i128;
    Some((md.dev(), md.ino(), md.len(), mtime_ns))
}

#[cfg(not(unix))]
fn stat_identity(path: &std::path::Path) -> Option<(u64, u64, u64, i128)> {
    // Non-unix has no stable inode identity; carry size + generation + mtime only
    // (matches the flight `warm::identity` degradation on unsupported targets).
    // CQLite's supported deployment targets are unix (macOS/Linux).
    let md = std::fs::metadata(path).ok()?;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0);
    Some((0, 0, md.len(), mtime_ns))
}

/// Approximate per-entry byte overhead ON TOP OF the key's own `len()` bytes.
///
/// In the nested structure an entry is a `Box<[u8]>` key plus an [`Entry`] value
/// ([`PartitionLoc`] + a `u64` recency `seq`), held in a per-identity `LruCache`
/// intrusive-list node. The [`GenerationIdentity`] is NO LONGER stored per entry —
/// it is the inner-LRU's `HashMap` key, held once per resident identity and
/// amortized away here. This const covers everything except the key's payload
/// bytes (charged separately via `key.len()`):
/// - the `Box<[u8]>` fat pointer + the key allocation's header/rounding,
/// - the [`Entry`] value (12 B [`PartitionLoc`] + 8 B `seq`) stored inline,
/// - the `LruCache` node (a `HashMap` bucket slot + two `NonNull` links) + slack,
/// - a generous share of the outer `HashMap<GenerationIdentity, _>` bucket slot.
///
/// Kept at 96 B (unchanged from the flat layout) so byte accounting OVER-estimates
/// true resident memory (the budget must never be silently exceeded); dropping the
/// per-entry identity makes 96 B strictly MORE conservative than before.
const PER_ENTRY_OVERHEAD: usize = 96;

/// Approximate resident cost of caching one entry whose key is `key_len` bytes.
#[inline]
const fn entry_cost(key_len: usize) -> usize {
    key_len.saturating_add(PER_ENTRY_OVERHEAD)
}

/// Default global key-cache BYTE budget (a fixed named constant inside the
/// `<128MB` envelope — no new user knob, per design §B and the #2343 WS4 decision).
///
/// Locations are tiny (~24-88 B each), so 64 MiB holds well over a million hot
/// partition locations while leaving room for the B1 decompressed-chunk cache and
/// the working set. Unlike the retired per-reader 512 KiB budget, this bounds the
/// AGGREGATE across every open reader by ONE cap — the whole point of #2059.
pub const DEFAULT_GLOBAL_KEY_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Default shard count (power of two). HIGHER than the per-reader default (#1570's
/// 16) because a single global instance concentrates ALL reader traffic onto one
/// cache, so the shard count must be high enough that the hit path never serializes
/// (design §F, the explicit #2052-class mitigation).
pub const DEFAULT_GLOBAL_KEY_CACHE_SHARDS: usize = 128;

/// A resident entry value: the resolved [`PartitionLoc`] plus a recency `seq`
/// stamp from the owning shard's monotonic clock. The `seq` gives a cross-identity
/// recency ordering so budget eviction can pick the globally-LRU entry across all
/// inner LRUs in a shard (the inner `LruCache`'s own order is per-identity only).
#[derive(Clone, Copy, Debug)]
struct Entry {
    loc: PartitionLoc,
    seq: u64,
}

/// One cache shard: a `HashMap` from [`GenerationIdentity`] to that generation's
/// `unbounded`-by-count inner `LruCache` (raw partition-key bytes → [`Entry`]),
/// plus a running resident-byte counter and a monotonic recency clock. The byte
/// budget (aggregate across ALL inner LRUs in the shard) is enforced manually on
/// insert (`evict_one` while over budget).
struct Shard {
    map: HashMap<GenerationIdentity, LruCache<Box<[u8]>, Entry>>,
    current_bytes: usize,
    /// Monotonic per-shard recency clock; each get/insert stamps the touched entry
    /// with the next value so tails can be compared across inner LRUs.
    seq: u64,
}

impl Shard {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            current_bytes: 0,
            seq: 0,
        }
    }

    /// Next recency stamp. `wrapping_add` cannot panic; at one bump/ns a `u64`
    /// takes ~584 years to wrap, so ordering is effectively total in practice.
    #[inline]
    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    /// Total resident entry count across every inner LRU in this shard.
    fn total_len(&self) -> usize {
        self.map.values().map(LruCache::len).sum()
    }

    /// Look up `key` under `identity`, bumping both the inner LRU recency and the
    /// cross-identity `seq`. Probes the inner LRU with the borrowed `&[u8]`
    /// (`Box<[u8]>: Borrow<[u8]>`), allocating NO owned key on the hot path.
    fn get(&mut self, identity: &GenerationIdentity, key: &[u8]) -> Option<PartitionLoc> {
        let seq = self.next_seq();
        let inner = self.map.get_mut(identity)?;
        let entry = inner.get_mut(key)?;
        entry.seq = seq;
        Some(entry.loc)
    }

    /// Evict the globally-least-recently-used entry across all inner LRUs (the
    /// minimum tail `seq`), returning whether one was removed. Removes an inner LRU
    /// that becomes empty so the outer `HashMap` never accumulates dead identities.
    ///
    /// O(identities-in-shard) to find the min tail — bounded by the number of
    /// generations whose keys hash into this shard (small; entries spread across
    /// all shards), and only paid under budget pressure. The just-inserted entry
    /// carries the max `seq`, so it is never chosen unless it is the sole entry.
    fn evict_one(&mut self) -> bool {
        let mut victim: Option<GenerationIdentity> = None;
        let mut min_seq = u64::MAX;
        for (id, inner) in self.map.iter() {
            if let Some((_, entry)) = inner.peek_lru() {
                if victim.is_none() || entry.seq < min_seq {
                    min_seq = entry.seq;
                    victim = Some(*id);
                }
            }
        }
        let Some(id) = victim else {
            return false;
        };
        let Some(inner) = self.map.get_mut(&id) else {
            return false;
        };
        match inner.pop_lru() {
            Some((k, _)) => {
                self.current_bytes = self.current_bytes.saturating_sub(entry_cost(k.len()));
                if inner.is_empty() {
                    self.map.remove(&id);
                }
                true
            }
            None => false,
        }
    }
}

/// A point-in-time snapshot of the global cache's real observability counters
/// (design §G). Every field is a real observed value — never a fabricated
/// placeholder. Reported through `Database::stats().memory_stats`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct GlobalKeyCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub resident_bytes: usize,
    pub capacity_bytes: usize,
}

/// The process-global, byte-bounded, sharded key→partition-offset cache.
pub struct GlobalKeyOffsetCache {
    shards: Box<[Mutex<Shard>]>,
    /// Approximate BYTE budget PER shard. `0` when [`disabled`](Self::disabled).
    per_shard_bytes: usize,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    mask: usize,
    /// When `true` this is a genuine no-op cache (honoring
    /// `block_cache.enabled == false`): `get` always misses, `insert` never
    /// retains, so reads bypass the cache entirely.
    disabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Entries evicted to stay within the byte budget (budget-driven).
    evictions: AtomicU64,
    /// Entries dropped by [`invalidate`](Self::invalidate) on generation removal —
    /// DISTINCT from budget-driven `evictions` (design §G).
    invalidations: AtomicU64,
}

impl std::fmt::Debug for GlobalKeyOffsetCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalKeyOffsetCache")
            .field("shards", &self.shards.len())
            .field("per_shard_bytes", &self.per_shard_bytes)
            .field("disabled", &self.disabled)
            .field("len", &self.len())
            .field("resident_bytes", &self.resident_bytes())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .field("evictions", &self.evictions.load(Ordering::Relaxed))
            .field("invalidations", &self.invalidations.load(Ordering::Relaxed))
            .finish()
    }
}

/// The process-global singleton, created once with the fixed budget (design §A —
/// ONE instance for the whole process). Readers obtain a shared `Arc` clone of
/// this when block caching is enabled; a disabled reader holds its own
/// [`disabled`](GlobalKeyOffsetCache::disabled) instance instead.
static GLOBAL: LazyLock<Arc<GlobalKeyOffsetCache>> = LazyLock::new(|| {
    Arc::new(GlobalKeyOffsetCache::with_budget_bytes(
        DEFAULT_GLOBAL_KEY_CACHE_BYTES,
    ))
});

impl GlobalKeyOffsetCache {
    /// The process-global singleton handle (design §A). All enabled readers share
    /// this ONE instance, so the aggregate footprint is bounded by its single cap
    /// regardless of open-reader count.
    pub fn global() -> Arc<GlobalKeyOffsetCache> {
        Arc::clone(&GLOBAL)
    }

    /// Create a cache with `total_budget_bytes` split across
    /// [`DEFAULT_GLOBAL_KEY_CACHE_SHARDS`] shards.
    pub fn with_budget_bytes(total_budget_bytes: usize) -> Self {
        Self::with_budget_and_shards(total_budget_bytes, DEFAULT_GLOBAL_KEY_CACHE_SHARDS)
    }

    /// Create a cache with `total_budget_bytes` split across `shard_count` shards.
    ///
    /// `shard_count` is rounded UP to the next power of two (min 1) so shard
    /// selection can mask instead of modulo. Unit tests use `shard_count = 1` for
    /// deterministic eviction ordering.
    pub fn with_budget_and_shards(total_budget_bytes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1).next_power_of_two();
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
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    /// Create a genuine no-op cache (honoring `block_cache.enabled == false`):
    /// reads bypass it entirely and every counter/occupancy accessor reports `0`.
    pub fn disabled() -> Self {
        Self {
            shards: vec![Mutex::new(Shard::new())].into_boxed_slice(),
            per_shard_bytes: 0,
            mask: 0,
            disabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    /// Poison-tolerant lock: recover the guard if a prior holder panicked, so a
    /// single panic cannot turn the cache into a panic-for-everyone (design §F).
    #[inline]
    fn lock(m: &Mutex<Shard>) -> std::sync::MutexGuard<'_, Shard> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    fn shard_for(&self, identity: &GenerationIdentity, key: &[u8]) -> &Mutex<Shard> {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut h);
        key.hash(&mut h);
        let idx = (h.finish() as usize) & self.mask;
        // `idx <= mask < shards.len()`, so this index is always in bounds.
        &self.shards[idx]
    }

    /// Look up a partition location by `(identity, key)`. A hit bumps recency and
    /// returns the stored [`PartitionLoc`]; a miss — INCLUDING an entry stored
    /// under a DIFFERENT identity for the same raw key (fail-closed, design §C) —
    /// returns `None`.
    pub fn get(&self, identity: GenerationIdentity, key: &[u8]) -> Option<PartitionLoc> {
        if self.disabled {
            return None;
        }
        let mut guard = Self::lock(self.shard_for(&identity, key));
        // Fail-closed by construction: the inner LRU is selected by `identity`, so a
        // lookup under a different identity for the same raw key never sees the wrong
        // generation's entries. Probes with the borrowed `&[u8]` — no owned-key alloc.
        let found = guard.get(&identity, key);
        drop(guard);
        match found {
            Some(loc) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(loc)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert `loc` under `(identity, key)`. A no-op on a disabled cache. The
    /// owning shard evicts LRU entries until within its per-shard byte budget
    /// (never evicting the just-inserted MRU entry).
    pub fn insert(&self, identity: GenerationIdentity, key: &[u8], loc: PartitionLoc) {
        if self.disabled {
            return;
        }
        let cost = entry_cost(key.len());
        let mut guard = Self::lock(self.shard_for(&identity, key));

        let seq = guard.next_seq();
        let inner = guard
            .map
            .entry(identity)
            .or_insert_with(LruCache::unbounded);
        let replaced = inner.put(key.into(), Entry { loc, seq }).is_some();
        if replaced {
            // In-place replacement: the key bytes are identical, so `cost` matches;
            // subtract-then-add keeps the counter exact (sibling-cache discipline).
            guard.current_bytes = guard.current_bytes.saturating_sub(cost);
        }
        guard.current_bytes = guard.current_bytes.saturating_add(cost);

        // Evict the globally-LRU entry across all identities until within budget,
        // never evicting the just-inserted MRU (it carries the max `seq`, so it is
        // only picked when it is the sole resident entry — then `total_len() > 1`
        // stops us, retaining one oversized entry, matching the flat-layout policy).
        let mut evicted_here: u64 = 0;
        while guard.current_bytes > self.per_shard_bytes && guard.total_len() > 1 {
            if !guard.evict_one() {
                break;
            }
            evicted_here += 1;
        }
        drop(guard);
        if evicted_here > 0 {
            self.evictions.fetch_add(evicted_here, Ordering::Relaxed);
        }
    }

    /// Drop ALL entries for `identity` (generation removal / compaction /
    /// warm-registry evict, design §C). Returns the number of entries dropped and
    /// records them on the DISTINCT `invalidations` counter (separate from budget
    /// `evictions`). A no-op on a disabled cache. A #2383 rebind does NOT call this
    /// (the identity is unchanged across a rebind, so entries survive).
    pub fn invalidate(&self, identity: GenerationIdentity) -> u64 {
        if self.disabled {
            return 0;
        }
        let mut dropped: u64 = 0;
        for shard in self.shards.iter() {
            let mut guard = Self::lock(shard);
            // O(1) removal of this identity's whole inner LRU — no scan of unrelated
            // identities' entries. We iterate ONLY the removed inner LRU to reclaim
            // its bytes and count its drops; other identities are never touched.
            if let Some(inner) = guard.map.remove(&identity) {
                let mut reclaimed = 0usize;
                let mut n: u64 = 0;
                for (k, _) in inner.iter() {
                    reclaimed = reclaimed.saturating_add(entry_cost(k.len()));
                    n += 1;
                }
                guard.current_bytes = guard.current_bytes.saturating_sub(reclaimed);
                dropped += n;
            }
        }
        if dropped > 0 {
            self.invalidations.fetch_add(dropped, Ordering::Relaxed);
        }
        dropped
    }

    /// Drop EVERY entry (a full flush), returning the number dropped and recording
    /// them on the `invalidations` counter. Used by a whole-dataset drop and by
    /// tests needing a cold-cache starting point in the shared process-global cache.
    /// A no-op on a disabled cache.
    pub fn invalidate_all(&self) -> u64 {
        if self.disabled {
            return 0;
        }
        let mut dropped: u64 = 0;
        for shard in self.shards.iter() {
            let mut guard = Self::lock(shard);
            dropped = dropped.saturating_add(guard.total_len() as u64);
            guard.map.clear();
            guard.current_bytes = 0;
        }
        if dropped > 0 {
            self.invalidations.fetch_add(dropped, Ordering::Relaxed);
        }
        dropped
    }

    /// Total resident entry count across all shards (summed over every inner LRU).
    pub fn len(&self) -> usize {
        self.shards.iter().map(|m| Self::lock(m).total_len()).sum()
    }

    /// Whether the cache currently holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total approximate resident bytes across all shards. A disabled cache
    /// reports `0`.
    pub fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|m| Self::lock(m).current_bytes)
            .sum()
    }

    /// The configured total byte budget (`per_shard_bytes × shard count`). A
    /// disabled cache reports `0`.
    pub fn budget_bytes(&self) -> usize {
        self.per_shard_bytes.saturating_mul(self.shards.len())
    }

    /// Cumulative cache hits.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cumulative cache misses.
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Cumulative entries evicted to stay within budget (budget-driven).
    pub fn eviction_count(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Cumulative entries dropped by generation invalidation (distinct from
    /// budget-driven evictions, design §G).
    pub fn invalidation_count(&self) -> u64 {
        self.invalidations.load(Ordering::Relaxed)
    }

    /// A point-in-time snapshot of the real observability counters (design §G),
    /// reported through `Database::stats().memory_stats`.
    pub(crate) fn snapshot(&self) -> GlobalKeyCacheSnapshot {
        GlobalKeyCacheSnapshot {
            hits: self.hit_count(),
            misses: self.miss_count(),
            evictions: self.eviction_count(),
            invalidations: self.invalidation_count(),
            resident_bytes: self.resident_bytes(),
            capacity_bytes: self.budget_bytes(),
        }
    }
}

impl Default for GlobalKeyOffsetCache {
    fn default() -> Self {
        Self::with_budget_bytes(DEFAULT_GLOBAL_KEY_CACHE_BYTES)
    }
}

/// Tests live in a sibling file so this module stays inside the campsite-rule
/// source target (#1116).
#[cfg(test)]
#[path = "global_key_offset_tests.rs"]
mod tests;
