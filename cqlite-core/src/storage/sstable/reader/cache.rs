//! Block caching implementation for SSTable reader

use std::sync::atomic::Ordering;

use super::types::{BlockMeta, CachedBlock, SSTableReader};

impl SSTableReader {
    /// Calculate current cache hit rate from atomic counters
    pub(crate) fn calculate_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Increment cache hit counter (thread-safe)
    /// Currently unused as caching is not yet implemented (always cache miss)
    #[allow(dead_code)]
    pub(crate) fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache miss counter (thread-safe)
    pub(crate) fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get cache statistics for reporting
    pub fn get_cache_stats(&self) -> (u64, u64, f64) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let rate = self.calculate_cache_hit_rate();
        (hits, misses, rate)
    }

    /// Estimate current memory usage of the reader
    pub(crate) fn estimate_memory_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let block_cache_size = self
            .block_cache
            .values()
            .map(|block| block.data.len() + std::mem::size_of::<CachedBlock>())
            .sum::<usize>();
        let meta_cache_size = self.block_meta_cache.len() * std::mem::size_of::<BlockMeta>();

        base_size + block_cache_size + meta_cache_size
    }

    /// Clear block caches and log statistics
    #[allow(dead_code)]
    pub(crate) fn clear_caches(&mut self) -> (usize, usize) {
        let cache_entries = self.block_cache.len();
        let meta_entries = self.block_meta_cache.len();

        self.block_cache.clear();
        self.block_meta_cache.clear();

        (cache_entries, meta_entries)
    }
}
