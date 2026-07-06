//! Cfg-gated multi-generation streaming-merge resident-rows probe (issue #1579,
//! Epic D / D3).
//!
//! # Why this exists
//!
//! D3 makes `scan_stream` over more than one SSTable generation feed each
//! reconciled partition from the k-way merger STRAIGHT into the bounded output
//! channel, instead of collecting the ENTIRE reconciled table into a `Vec`,
//! sorting it, and only then dribbling it down the channel. A correct answer
//! never proves *how much memory* the merge held resident — the same rows come
//! out either way. This probe makes the peak resident set observable so the
//! O(window) claim is pinned the no-heuristics way: observe the *work* (rows the
//! streaming producer held resident at once), not just the result.
//!
//! # The counter
//!
//! - [`record_resident`] raises a process-global high-water mark to `n` — the
//!   number of rows the streaming producer had materialized-but-not-yet-drained
//!   at one instant. The streaming driver reports each stepped partition's row
//!   count (its resident window is one partition at a time). The pre-D3 eager
//!   path reported `merged.len()` — the whole reconciled table — so the peak was
//!   proportional to the row count and the D3 memory guard
//!   (`peak_resident` FLAT across fixture sizes) flips red without the streaming
//!   rework.
//!
//! # Zero overhead in release (same pattern as [`crate::query::agg_stream_probe`])
//!
//! [`record_resident`] is an **unconditional** `#[inline(always)]` public
//! function; its body is `#[cfg(any(test, feature = "work-counters"))]`. In a
//! default/release build (no `work-counters`, no `cfg(test)`) the body is empty
//! and optimizes away — no atomic is even linked. The getter + [`reset`] are
//! `#[cfg(any(test, feature = "work-counters"))]`: in-crate unit tests reach them
//! via the `test` cfg; integration tests in `tests/` enable the `work-counters`
//! feature (they compile the lib WITHOUT its `test` cfg).

#[cfg(any(test, feature = "work-counters"))]
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-global high-water mark of rows the streaming multi-generation merge
/// held resident at one instant since the last [`reset`] (test/feature builds
/// only).
#[cfg(any(test, feature = "work-counters"))]
static PEAK_RESIDENT: AtomicU64 = AtomicU64::new(0);

/// Record that the streaming multi-generation merge held `n` rows resident at
/// one instant, raising the process-global high-water mark. Called
/// unconditionally by the streaming merge driver; the body compiles to a no-op
/// in a release build.
#[inline(always)]
pub fn record_resident(n: u64) {
    #[cfg(any(test, feature = "work-counters"))]
    PEAK_RESIDENT.fetch_max(n, Ordering::Relaxed);
    #[cfg(not(any(test, feature = "work-counters")))]
    let _ = n;
}

/// Peak resident rows the streaming multi-generation merge held at one instant
/// since the last [`reset`]. Streaming holds one partition at a time, so this is
/// FLAT across fixture sizes; the pre-D3 eager path reported the whole table.
#[cfg(any(test, feature = "work-counters"))]
pub fn peak_resident() -> u64 {
    PEAK_RESIDENT.load(Ordering::Relaxed)
}

/// Clear the process-global high-water mark. Integration tests call this before a
/// measured stream so a stale value cannot satisfy a later assertion; because the
/// global is shared, callers serialize on the shared test mutex (the counter-test
/// convention).
#[cfg(any(test, feature = "work-counters"))]
pub fn reset() {
    PEAK_RESIDENT.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Exercises the max/get/reset contract on the process-global. Serialized so it
    // never overlaps another test mutating the same global.
    #[test]
    #[serial]
    fn peak_resident_high_water_and_reset() {
        reset();
        assert_eq!(peak_resident(), 0);
        record_resident(5);
        record_resident(3); // lower value does not lower the high-water mark
        assert_eq!(peak_resident(), 5);
        record_resident(9);
        assert_eq!(peak_resident(), 9);
        reset();
        assert_eq!(peak_resident(), 0);
    }
}
