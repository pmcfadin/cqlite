//! Bounded warm-cache observability counters (issue #2310, WS4 #2343).
//!
//! Spec Requirement 6: emit warm-cache **hit**, **miss**, **evict**, and
//! **refresh-outcome** (unchanged / rebuilt-delta / fail-closed-retained)
//! counters, riding the EXISTING observability contract with NO new config knob,
//! environment variable, or ticket field.
//!
//! Two surfaces, one source of truth:
//!
//! * **Process/registry-scoped atomics** ([`WarmMetrics`]) — cheap, always-on,
//!   feature-independent. The registry increments them; tests and the #2289/#1494
//!   bench harness read a [`WarmMetricsSnapshot`] to PROVE warm behavior (e.g. a
//!   warm hit opened ZERO readers). This is the always-compiled "work-done probe"
//!   the spec's second-query-zero-parse scenarios assert against.
//! * **OpenTelemetry counters** — mirrored via `cqlite_core::observability`, a
//!   no-op when its feature is off, so cardinality stays bounded (the only
//!   attribute is the fixed outcome label — never tickets, keys, or paths).

use std::sync::atomic::{AtomicU64, Ordering};

use cqlite_core::observability::{self as obs, catalog, AttrValue};

/// The refresh outcome recorded per warm-handle lookup that (re)built state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The probed generation set matched the cached set — a warm hit, no rebuild.
    Unchanged,
    /// The generation set changed; only the delta was rebuilt (added generations
    /// opened, removed dropped, unchanged kept).
    RebuiltDelta,
    /// A rebuild failed (an added generation would not open); the PREVIOUSLY warm
    /// set was retained fully intact (fail-closed, mirrors #1749).
    FailClosedRetained,
}

impl RefreshOutcome {
    /// Bounded attribute label for the outcome (fixed set — cardinality-safe).
    const fn label(self) -> &'static str {
        match self {
            RefreshOutcome::Unchanged => "unchanged",
            RefreshOutcome::RebuiltDelta => "rebuilt_delta",
            RefreshOutcome::FailClosedRetained => "fail_closed_retained",
        }
    }
}

/// Process/registry-scoped atomic counters for the warm cache.
///
/// Held behind an `Arc` on the registry so clones (and the owning
/// [`crate::service::CqliteFlightService`]) share one set. All `Relaxed` — these
/// are monotonic diagnostic counters, never a synchronization edge.
#[derive(Debug, Default)]
pub struct WarmMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    evicts: AtomicU64,
    refresh_unchanged: AtomicU64,
    refresh_rebuilt: AtomicU64,
    refresh_fail_closed: AtomicU64,
    /// Number of `SSTableReader::open` calls the registry performed — the
    /// "work-done" probe. A warm hit performs ZERO opens (spec Requirement 2).
    reader_opens: AtomicU64,
}

impl WarmMetrics {
    /// Record a warm hit (the generation set was unchanged; state served from
    /// cache with zero reader-open/parse).
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        obs::add_counter(catalog::WARM_CACHE_HITS, 1, &[]);
    }

    /// Record a warm miss (no cached entry, or a rebuild was required).
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        obs::add_counter(catalog::WARM_CACHE_MISSES, 1, &[]);
    }

    /// Record `n` evictions (LRU or removed-on-disk).
    pub fn record_evicts(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.evicts.fetch_add(n, Ordering::Relaxed);
        obs::add_counter(catalog::WARM_CACHE_EVICTS, n, &[]);
    }

    /// Record a refresh outcome.
    pub fn record_refresh(&self, outcome: RefreshOutcome) {
        let counter = match outcome {
            RefreshOutcome::Unchanged => &self.refresh_unchanged,
            RefreshOutcome::RebuiltDelta => &self.refresh_rebuilt,
            RefreshOutcome::FailClosedRetained => &self.refresh_fail_closed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        obs::add_counter(
            catalog::WARM_CACHE_REFRESH,
            1,
            &[(
                catalog::attr::WARM_REFRESH_OUTCOME,
                AttrValue::StaticStr(outcome.label()),
            )],
        );
    }

    /// Record `n` reader opens performed during a (re)build.
    pub fn record_reader_opens(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.reader_opens.fetch_add(n, Ordering::Relaxed);
    }

    /// A consistent-enough snapshot of the counters for tests/benches.
    pub fn snapshot(&self) -> WarmMetricsSnapshot {
        WarmMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evicts: self.evicts.load(Ordering::Relaxed),
            refresh_unchanged: self.refresh_unchanged.load(Ordering::Relaxed),
            refresh_rebuilt_delta: self.refresh_rebuilt.load(Ordering::Relaxed),
            refresh_fail_closed_retained: self.refresh_fail_closed.load(Ordering::Relaxed),
            reader_opens: self.reader_opens.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time read of [`WarmMetrics`], for tests and the bench harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WarmMetricsSnapshot {
    /// Warm hits (unchanged generation set served from cache).
    pub hits: u64,
    /// Warm misses (fresh build or rebuild required).
    pub misses: u64,
    /// Total evictions (LRU + removed-on-disk).
    pub evicts: u64,
    /// Refresh outcomes = unchanged.
    pub refresh_unchanged: u64,
    /// Refresh outcomes = rebuilt-delta.
    pub refresh_rebuilt_delta: u64,
    /// Refresh outcomes = fail-closed-retained.
    pub refresh_fail_closed_retained: u64,
    /// Total `SSTableReader::open` calls — the work-done probe (0 on a warm hit).
    pub reader_opens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_increment() {
        let m = WarmMetrics::default();
        assert_eq!(m.snapshot(), WarmMetricsSnapshot::default_zero());
        m.record_hit();
        m.record_miss();
        m.record_evicts(2);
        m.record_reader_opens(3);
        m.record_refresh(RefreshOutcome::RebuiltDelta);
        let s = m.snapshot();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.evicts, 2);
        assert_eq!(s.reader_opens, 3);
        assert_eq!(s.refresh_rebuilt_delta, 1);
    }

    #[test]
    fn zero_evicts_and_opens_are_noops() {
        let m = WarmMetrics::default();
        m.record_evicts(0);
        m.record_reader_opens(0);
        let s = m.snapshot();
        assert_eq!(s.evicts, 0);
        assert_eq!(s.reader_opens, 0);
    }

    impl WarmMetricsSnapshot {
        fn default_zero() -> Self {
            Self {
                hits: 0,
                misses: 0,
                evicts: 0,
                refresh_unchanged: 0,
                refresh_rebuilt_delta: 0,
                refresh_fail_closed_retained: 0,
                reader_opens: 0,
            }
        }
    }
}
