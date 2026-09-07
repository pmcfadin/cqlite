//! Memory management for CQLite.
//!
//! Issue #1568 (Epic B / B2) deleted the dead `MemoryManager` cache core — the
//! LRU block cache, the row cache, and the buffer pool that no read path used —
//! now that B1 (#1567) ships the real
//! [`DecompressedChunkCache`](crate::storage::cache::DecompressedChunkCache).
//! What remains is the **semver-frozen** public stats shell:
//! [`MemoryManager::stats`] and [`MemoryStats`], reachable through
//! `Database::stats().memory_stats`. Its block-cache numbers are now sourced
//! from the live B1 cache (real hits/misses/occupancy) instead of the deleted
//! always-zero counters, so a repeated cached read yields a non-zero
//! [`MemoryStats::block_cache_hit_rate`] rather than a structural `0.0`.

use std::sync::Arc;

use crate::storage::cache::DecompressedChunkCache;
#[cfg(feature = "state_machine")]
use crate::Value;
use crate::{Config, Result};

/// Memory manager: the retained public stats shell over the real B1 cache.
///
/// It holds an optional handle to the shared
/// [`DecompressedChunkCache`](crate::storage::cache::DecompressedChunkCache).
/// When present (the production path — [`MemoryManager::with_chunk_cache`]),
/// [`stats`](Self::stats) reports that cache's real activity. When absent
/// ([`new`](Self::new), used where no cache is wired), the block-cache numbers
/// report zero.
#[derive(Debug)]
pub struct MemoryManager {
    /// The live shared decompressed-chunk cache backing the stats surface, if
    /// this manager was wired to one.
    chunk_cache: Option<Arc<DecompressedChunkCache>>,
}

impl MemoryManager {
    /// Create a memory manager not wired to a cache.
    ///
    /// Retained for callers (and tests) that construct a manager without a live
    /// storage engine; its [`stats`](Self::stats) reports zero block-cache
    /// activity. Production opens use [`with_chunk_cache`](Self::with_chunk_cache).
    pub fn new(_config: &Config) -> Result<Self> {
        Ok(Self { chunk_cache: None })
    }

    /// Create a memory manager whose stats surface reports the real activity of
    /// the shared B1 decompressed-chunk cache (issue #1568).
    pub fn with_chunk_cache(chunk_cache: Arc<DecompressedChunkCache>) -> Self {
        Self {
            chunk_cache: Some(chunk_cache),
        }
    }

    /// Get memory statistics.
    ///
    /// The block-cache hit/miss counts and occupancy (`total_memory_used`) are
    /// sourced from the live B1 [`DecompressedChunkCache`] when wired (issue
    /// #1568): `hit_count()` / `miss_count()` / `resident_bytes()`. The retained
    /// row-cache and buffer-pool sub-fields report a fixed `0` — the richer
    /// honest surface is Epic B / B5. The `-> Result<MemoryStats>` signature is
    /// preserved for semver compatibility with `Database::stats()`.
    ///
    /// [`DecompressedChunkCache`]: crate::storage::cache::DecompressedChunkCache
    pub fn stats(&self) -> Result<MemoryStats> {
        let mut stats = MemoryStats::default();
        if let Some(cache) = &self.chunk_cache {
            stats.block_cache_hits = cache.hit_count();
            stats.block_cache_misses = cache.miss_count();
            stats.block_cache_evictions = cache.eviction_count();
            stats.block_cache_capacity_bytes = cache.budget_bytes();
            stats.total_memory_used = cache.resident_bytes();
        }
        Ok(stats)
    }
}

/// Estimate the logical size, in bytes, of a single CQL value.
///
/// The single estimator reused by the SELECT executor's byte-bounded result
/// budget (issue #1582). It is a *logical* content estimate and deliberately
/// does not model container overhead (HashMap slots, `Arc`/`String` capacity),
/// which is why the executor's byte budget sits well below the process memory
/// target to leave headroom for that overhead.
///
/// `state_machine`-gated: its only consumer is the (feature-gated) query engine
/// (`query::result_budget`), so under a minimal `--no-default-features` build it
/// would otherwise be dead code.
#[cfg(feature = "state_machine")]
pub(crate) fn estimate_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        // Zero content bytes: the sentinel's payload IS the empty buffer
        // (issue #3805).
        Value::Empty(_) => 0,
        Value::Boolean(_) => 1,
        Value::Integer(_) => 4,
        Value::BigInt(_) => 8,
        Value::Counter(_) => 8,
        Value::Float(_) => 8,
        Value::Text(s) => s.len(),
        Value::Blob(b) => b.len(),
        Value::Timestamp(_) => 8,
        Value::Date(_) => 4,
        Value::Time(_) => 8,
        Value::Uuid(_) => 16,
        Value::Inet(bytes) => bytes.len(),
        Value::Json(json) => json.to_string().len(),
        Value::List(items) => items.iter().map(estimate_value_size).sum(),
        Value::Map(map) => map
            .iter()
            .map(|(k, v)| estimate_value_size(k) + estimate_value_size(v))
            .sum(),
        Value::TinyInt(_) => 1,
        Value::SmallInt(_) => 2,
        Value::Float32(_) => 4,
        Value::Set(items) => items.iter().map(estimate_value_size).sum(),
        Value::Tuple(items) => items.iter().map(estimate_value_size).sum(),
        Value::Udt(udt) => udt
            .fields
            .iter()
            .map(|f| f.value.as_ref().map_or(0, estimate_value_size))
            .sum(),
        Value::Frozen(boxed_value) => estimate_value_size(boxed_value),
        Value::Varint(data) => data.len(),
        Value::Decimal { unscaled, .. } => 4 + unscaled.len(), // scale + unscaled data
        Value::Duration { .. } => 12,                          // 3 * 4 bytes
        Value::Tombstone(_) => 16,                             // timestamp + type + optional TTL
    }
}

/// Memory statistics.
///
/// Public-surface-wired (semver) through `Database::stats().memory_stats`. The
/// field names and types are preserved verbatim (issue #1568, AK6); only the
/// *source* of the block-cache numbers changed — they now reflect the real B1
/// [`DecompressedChunkCache`](crate::storage::cache::DecompressedChunkCache).
///
/// # Independent sampling (advisory observability)
///
/// The `block_cache_*` fields and the `key_cache_*` fields are sampled
/// **independently and at different instants**: the block-cache figures come from
/// the [`MemoryManager`] (its shared B1 chunk cache), while the key-cache figures
/// are aggregated from the storage engine's per-reader B4 caches
/// (`SSTableManager::aggregate_key_cache_stats`) in a separate step. Under
/// concurrent read load the two groups can therefore reflect slightly different
/// moments in time, so within a single `memory_stats` snapshot the block-cache vs
/// key-cache figures are **not guaranteed to be mutually coherent**. Treat this as
/// advisory observability (trends, rough ratios), not a transactionally-consistent
/// point-in-time view of both caches.
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Block cache hits (real B1 cache hit count when wired).
    pub block_cache_hits: u64,

    /// Block cache misses (real B1 cache miss count when wired).
    pub block_cache_misses: u64,

    /// Block cache evictions: entries the B1 decompressed-chunk cache evicted to
    /// stay within its byte budget (issue #1571, B5). Real `eviction_count()`
    /// when wired; `0` (honest — no cache, no evictions) otherwise. High churn
    /// relative to hits signals an undersized `block_cache_capacity_bytes`.
    pub block_cache_evictions: u64,

    /// Block cache configured byte budget (issue #1571, B5): the B1 cache's
    /// `budget_bytes()` when wired, so a hit rate can be read against the budget
    /// it was measured under; `0` when no cache is wired / block caching disabled.
    pub block_cache_capacity_bytes: usize,

    /// Key cache hits (issue #1571/#2059). A hit lets a repeated point read skip the
    /// `Index.db` interval parse. Since #2059 the key cache is ONE process-global
    /// instance shared by every open reader, so these counters are PROCESS-GLOBAL: they
    /// aggregate activity across ALL `Database` instances in the process, not one
    /// reader's slice (a semantic change from the retired per-reader counters). Real
    /// counter; `0` before any reader touches the cache.
    pub key_cache_hits: u64,

    /// Key cache misses (issue #1571/#2059). Process-global, like
    /// [`key_cache_hits`](Self::key_cache_hits) — summed across all `Database`
    /// instances in the process.
    pub key_cache_misses: u64,

    /// Key cache evictions: entries evicted from the process-global key cache to
    /// stay within its byte budget (issue #1571/#2059) — DISTINCT from
    /// [`key_cache_invalidations`](Self::key_cache_invalidations).
    pub key_cache_evictions: u64,

    /// Key cache invalidations: entries dropped from the process-global key cache on
    /// generation removal / compaction / warm-registry evict (issue #2059) — a
    /// distinct counter from budget-driven [`key_cache_evictions`](Self::key_cache_evictions).
    pub key_cache_invalidations: u64,

    /// Key cache resident bytes: approximate resident footprint of the process-global
    /// key cache (issue #1571/#2059).
    pub key_cache_resident_bytes: usize,

    /// Key cache capacity bytes: the process-global key cache's fixed configured byte
    /// budget (issue #1571/#2059), or `0` when block caching is disabled.
    pub key_cache_capacity_bytes: usize,

    /// Row cache hits. Retained for shape compatibility; the row cache was
    /// deleted (issue #1568), so this reports `0` (full surface is Epic B / B5).
    pub row_cache_hits: u64,

    /// Row cache misses. Retained for shape compatibility; reports `0`.
    pub row_cache_misses: u64,

    /// Total memory used. Now the B1 cache's resident decompressed bytes
    /// (`resident_bytes()`) when wired (issue #1568).
    pub total_memory_used: usize,

    /// Buffer pool allocations. Retained for shape compatibility; the buffer
    /// pool was deleted (issue #1568), so this reports `0`.
    pub buffer_allocations: u64,

    /// Buffer pool deallocations. Retained for shape compatibility; reports `0`.
    pub buffer_deallocations: u64,
}

impl MemoryStats {
    /// Calculate block cache hit rate.
    pub fn block_cache_hit_rate(&self) -> f64 {
        let total = self.block_cache_hits + self.block_cache_misses;
        if total > 0 {
            self.block_cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Calculate the aggregate key-cache hit rate (issue #1571, B5) from the real
    /// summed hit and miss counts. Returns `0.0` when there has been no key-cache
    /// activity (honest — not a structural pin).
    pub fn key_cache_hit_rate(&self) -> f64 {
        let total = self.key_cache_hits + self.key_cache_misses;
        if total > 0 {
            self.key_cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Calculate row cache hit rate.
    pub fn row_cache_hit_rate(&self) -> f64 {
        let total = self.row_cache_hits + self.row_cache_misses;
        if total > 0 {
            self.row_cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache::{ChunkKey, DecompressedChunkCache};

    #[test]
    fn stats_report_zero_without_a_wired_cache() {
        let config = Config::default();
        let manager = MemoryManager::new(&config).expect("construct manager");

        let stats = manager.stats().expect("stats");
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(stats.block_cache_misses, 0);
        assert_eq!(stats.total_memory_used, 0);
        assert_eq!(stats.block_cache_hit_rate(), 0.0);
        // Issue #1571 (B5): every new cache-observability field reports an honest
        // zero when no cache is wired — never a fabricated placeholder.
        assert_eq!(stats.block_cache_evictions, 0);
        assert_eq!(stats.block_cache_capacity_bytes, 0);
        assert_eq!(stats.key_cache_hits, 0);
        assert_eq!(stats.key_cache_misses, 0);
        assert_eq!(stats.key_cache_evictions, 0);
        // Issue #2059: the invalidations counter is distinct from evictions.
        assert_eq!(stats.key_cache_invalidations, 0);
        assert_eq!(stats.key_cache_resident_bytes, 0);
        assert_eq!(stats.key_cache_capacity_bytes, 0);
        assert_eq!(stats.key_cache_hit_rate(), 0.0);
    }

    #[test]
    fn stats_bridge_reports_real_b1_cache_numbers() {
        // A wired manager reports the live B1 cache's hits/misses/occupancy.
        let cache = Arc::new(DecompressedChunkCache::with_budget_bytes(1 << 20));
        let manager = MemoryManager::with_chunk_cache(Arc::clone(&cache));

        let key = ChunkKey::new(1, 0);
        cache.insert(key, vec![0xAB; 4096]); // resident bytes now non-zero
        assert!(cache.get(&key).is_some()); // one hit
        assert!(cache.get(&ChunkKey::new(1, 1)).is_none()); // one miss

        let stats = manager.stats().expect("stats");
        assert_eq!(stats.block_cache_hits, 1);
        assert_eq!(stats.block_cache_misses, 1);
        assert_eq!(stats.total_memory_used, cache.resident_bytes());
        assert!(
            stats.total_memory_used > 0,
            "occupancy tracks resident bytes"
        );
        assert!(
            stats.block_cache_hit_rate() > 0.0,
            "a hit makes the reported rate non-zero (not structural 0.0)"
        );
        // Issue #1571 (B5): capacity is the real budget; no eviction yet.
        assert_eq!(stats.block_cache_capacity_bytes, cache.budget_bytes());
        assert_eq!(stats.block_cache_evictions, 0);
    }

    #[test]
    fn stats_bridge_reports_real_b1_eviction_count() {
        // Issue #1571 (B5): a wired manager surfaces the chunk cache's real
        // eviction count. Tiny budget on a single shard forces deterministic
        // evictions.
        let cache = Arc::new(DecompressedChunkCache::with_budget_and_shards(200, 1));
        let manager = MemoryManager::with_chunk_cache(Arc::clone(&cache));
        for i in 0..5u64 {
            cache.insert(ChunkKey::new(1, i), vec![i as u8; 100]);
        }
        let stats = manager.stats().expect("stats");
        assert_eq!(stats.block_cache_evictions, cache.eviction_count());
        assert!(
            stats.block_cache_evictions > 0,
            "over-budget inserts must have evicted"
        );
        assert_eq!(stats.block_cache_capacity_bytes, cache.budget_bytes());
    }
}
