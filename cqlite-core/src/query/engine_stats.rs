//! Lock-free per-query statistics for the query engine (issue #1595, Epic F/F5).
//!
//! The engine previously held `QueryStats` behind an `Arc<RwLock<_>>` and took a
//! **write lock 2–3× per query** just to bump counters — a process-wide
//! serialization point on the hot path with no functional need. This module
//! replaces that with a struct of `AtomicU64` counters bumped with
//! [`Ordering::Relaxed`]: the counters are independent totals, no reader depends
//! on a happens-before relationship between two different counters, and the
//! public snapshot is inherently a racy point-in-time read (as it was under the
//! old lock). Relaxed `fetch_add` is exact under concurrency (no lost updates),
//! which is the only invariant the audit requires ("counts must remain correct").
//!
//! The public [`QueryStats`] snapshot shape is unchanged; derived values
//! (`avg_execution_time_us`, `cache_hit_ratio`) are computed at read time in
//! [`AtomicQueryStats::snapshot`].

use std::sync::atomic::{AtomicU64, Ordering};

use super::QueryStats;

/// Lock-free query statistics accumulator held by the query engine.
///
/// Every field is an independent monotonic counter incremented with relaxed
/// ordering. [`snapshot`](Self::snapshot) reads them into the public
/// [`QueryStats`] view, computing the derived averages/ratios at read time.
#[derive(Debug, Default)]
pub(crate) struct AtomicQueryStats {
    /// Total queries executed.
    total_queries: AtomicU64,
    /// Queries that resulted in errors.
    error_queries: AtomicU64,
    /// Number of plan-cache hits (numerator of `cache_hit_ratio`).
    cache_hits: AtomicU64,
    /// Total rows affected by write operations.
    rows_affected: AtomicU64,
    /// Sum of recorded execution times, in microseconds. Divided by
    /// `total_queries` at read time to derive `avg_execution_time_us`.
    exec_time_us_sum: AtomicU64,
}

impl AtomicQueryStats {
    /// Count one executed query.
    pub(crate) fn record_query(&self) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one query that resulted in an error.
    pub(crate) fn record_error(&self) {
        self.error_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one plan-cache hit.
    pub(crate) fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the outcome of a materialized query: add its execution time (µs)
    /// and the rows it affected.
    pub(crate) fn record_execution(&self, exec_time_us: u64, rows_affected: u64) {
        self.exec_time_us_sum
            .fetch_add(exec_time_us, Ordering::Relaxed);
        self.rows_affected
            .fetch_add(rows_affected, Ordering::Relaxed);
    }

    /// Read a consistent-shaped point-in-time [`QueryStats`] snapshot.
    ///
    /// Derived values are computed here: `avg_execution_time_us` is the mean
    /// recorded execution time over all queries, and `cache_hit_ratio` is the
    /// fraction of queries served from the plan cache. Both are `0` when no
    /// queries have been recorded.
    pub(crate) fn snapshot(&self) -> QueryStats {
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let exec_time_us_sum = self.exec_time_us_sum.load(Ordering::Relaxed);

        let (avg_execution_time_us, cache_hit_ratio) = if total_queries == 0 {
            (0, 0.0)
        } else {
            (
                exec_time_us_sum / total_queries,
                // Clamp the numerator: `total_queries` and `cache_hits` are
                // read as independent `Relaxed` atomics, so a concurrent
                // snapshot could observe `cache_hits > total_queries` and yield
                // a ratio > 1.0, violating the documented 0.0..=1.0 contract.
                cache_hits.min(total_queries) as f64 / total_queries as f64,
            )
        };

        QueryStats {
            total_queries,
            error_queries: self.error_queries.load(Ordering::Relaxed),
            avg_execution_time_us,
            cache_hit_ratio,
            rows_affected: self.rows_affected.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn empty_snapshot_is_all_zero() {
        let snap = AtomicQueryStats::default().snapshot();
        assert_eq!(snap.total_queries, 0);
        assert_eq!(snap.error_queries, 0);
        assert_eq!(snap.rows_affected, 0);
        assert_eq!(snap.avg_execution_time_us, 0);
        assert_eq!(snap.cache_hit_ratio, 0.0);
    }

    #[test]
    fn derived_values_have_expected_shape() {
        let stats = AtomicQueryStats::default();
        // 4 queries, 1 cache hit, executions summing to 400us over 4 queries.
        for _ in 0..4 {
            stats.record_query();
        }
        stats.record_cache_hit();
        stats.record_execution(100, 2);
        stats.record_execution(300, 3);

        let snap = stats.snapshot();
        assert_eq!(snap.total_queries, 4);
        assert_eq!(snap.rows_affected, 5);
        // avg = exec_time_us_sum / total_queries = 400 / 4
        assert_eq!(snap.avg_execution_time_us, 100);
        // ratio = cache_hits / total_queries = 1 / 4, and is within [0, 1] and > 0.
        assert!((0.0..=1.0).contains(&snap.cache_hit_ratio));
        assert!(snap.cache_hit_ratio > 0.0);
        assert!((snap.cache_hit_ratio - 0.25).abs() < f64::EPSILON);
    }

    /// Concurrent increments must not lose updates: the snapshot totals must
    /// equal the exact number of issued events. This asserts only counter *sums*
    /// against known issued counts — no wall-clock window — so it cannot flake on
    /// a timing boundary.
    #[test]
    fn concurrent_increments_are_exact() {
        const THREADS: u64 = 16;
        const PER_THREAD: u64 = 5_000;

        let stats = Arc::new(AtomicQueryStats::default());
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        stats.record_query();
                        stats.record_error();
                        stats.record_cache_hit();
                        stats.record_execution(2, 3);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }

        let snap = stats.snapshot();
        let issued = THREADS * PER_THREAD;
        assert_eq!(snap.total_queries, issued);
        assert_eq!(snap.error_queries, issued);
        assert_eq!(snap.rows_affected, issued * 3);
        // avg = (issued * 2) / issued == 2; ratio = issued hits / issued == 1.0
        assert_eq!(snap.avg_execution_time_us, 2);
        assert!((snap.cache_hit_ratio - 1.0).abs() < f64::EPSILON);
    }
}
