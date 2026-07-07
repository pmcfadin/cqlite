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
//! The increment is a single `Relaxed` atomic add — negligible on the open path.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide count of Statistics.db TOC walks since the last reset.
static TOC_WALKS: AtomicU64 = AtomicU64::new(0);

/// Record that a Statistics.db TOC was walked once. Called at each TOC-walk site.
#[inline]
pub(crate) fn record_toc_walk() {
    TOC_WALKS.fetch_add(1, Ordering::Relaxed);
}

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
