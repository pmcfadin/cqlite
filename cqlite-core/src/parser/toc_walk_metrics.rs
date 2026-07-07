//! Minimal process-wide counter for Statistics.db Table-of-Contents (TOC) walks
//! (issue #1658, epic #1606).
//!
//! The metadata stack re-walks the Statistics.db TOC several times during a
//! single SSTable open — once to locate the `HEADER` (SerializationHeader)
//! offset (`enhanced_statistics_parser::header::parse_statistics_toc_for_header_offset`)
//! and again inside `repair_metadata::stats_component_bounds`, which
//! `read_table_counts` and `parse_stats_extras` each invoke while building
//! `SSTableStatistics`. This counter lets the A5 cold-open bench
//! (`benches/open.rs`) MEASURE that redundancy so a fix can be scoped against a
//! real number rather than a guess. It is NOT a decoding heuristic: nothing in
//! the parse path reads the count, so it has no effect on correctness (#28).
//!
//! The instrumentation is gated behind `cli-helpers` (roborev #1658): the
//! counter is only read by the cli-helpers-gated bench, so production builds
//! (default features) carry zero overhead — `record_toc_walk()` compiles to an
//! empty inline no-op.

/// Record that a Statistics.db TOC was walked once. Called at each TOC-walk site.
/// Gated behind cli-helpers so production builds carry zero overhead (roborev #1658).
#[inline]
#[cfg(feature = "cli-helpers")]
pub(crate) fn record_toc_walk() {
    gated::TOC_WALKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// No-op in production builds (when cli-helpers is disabled).
#[inline]
#[cfg(not(feature = "cli-helpers"))]
pub(crate) fn record_toc_walk() {
    // Empty — zero overhead in default/production builds.
}

// The counter and its accessors are only compiled when cli-helpers is enabled
// (the bench that reads them is also gated on cli-helpers).
#[cfg(feature = "cli-helpers")]
mod gated {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Process-wide count of Statistics.db TOC walks since the last reset.
    pub(super) static TOC_WALKS: AtomicU64 = AtomicU64::new(0);

    /// Read the current TOC-walk count (process-wide, since the last reset).
    pub fn toc_walk_count() -> u64 {
        TOC_WALKS.load(Ordering::Relaxed)
    }

    /// Reset the TOC-walk counter to zero and return the value it held. Lets a
    /// caller (e.g. the cold-open bench) measure the walks of a single open in
    /// isolation: reset, do one open, then read `toc_walk_count()`.
    pub fn reset_toc_walk_count() -> u64 {
        TOC_WALKS.swap(0, Ordering::Relaxed)
    }
}

#[cfg(feature = "cli-helpers")]
pub use gated::{reset_toc_walk_count, toc_walk_count};

// When cli-helpers is disabled, provide stub functions so the bench module
// (which is itself gated on cli-helpers) still compiles if accidentally reached.
#[cfg(not(feature = "cli-helpers"))]
pub fn toc_walk_count() -> u64 {
    0
}

#[cfg(not(feature = "cli-helpers"))]
pub fn reset_toc_walk_count() -> u64 {
    0
}
