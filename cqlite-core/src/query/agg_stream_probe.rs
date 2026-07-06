//! Cfg-gated aggregate-buffering probe (issue #1578, Epic D / D2).
//!
//! # Why this exists
//!
//! D2 makes a GROUP-BY-free aggregate (`COUNT`/`MIN`/`MAX`/`SUM`/`AVG` with no
//! `GROUP BY`) fold the scan stream into an O(1) accumulator instead of buffering
//! the whole table into a `Vec<QueryRow>` before aggregating. A correct answer
//! never proves *how much memory* the aggregate held — `COUNT(*)` returns the
//! same number whether it streamed one row at a time or materialized the table.
//! This probe makes the buffering observable so the O(1) claim is pinned the
//! no-heuristics way: observe the *work* (rows resident in the aggregate input
//! buffer), not just the result.
//!
//! # The counter
//!
//! - [`record_buffered_rows`] adds the number of rows a single **buffered**
//!   aggregate input carried (the length of the `Vec<QueryRow>` fed into the
//!   materializing `execute_aggregation`). The streaming fold path never calls
//!   it, so a GROUP-BY-free aggregate driven by the fold records `0` regardless
//!   of table size. On `main`/pre-D2 a `COUNT(*)` over N rows buffered all N and
//!   recorded N — so the D2 memory guard (`buffered_rows() == 0` for a global
//!   aggregate; flat across fixture sizes) flips red without the fold.
//!
//! # Zero overhead in release (same pattern as [`crate::storage::sstable::read_work_counters`])
//!
//! [`record_buffered_rows`] is an **unconditional** `#[inline(always)]` public
//! function; its body is `#[cfg(any(test, feature = "work-counters"))]`. In a
//! default/release build (no `work-counters`, no `cfg(test)`) the body is empty
//! and optimizes away — no atomic is even linked. The getter + [`reset`] are
//! `#[cfg(any(test, feature = "work-counters"))]`: in-crate unit tests reach them
//! via the `test` cfg; integration tests in `tests/` enable the `work-counters`
//! feature (they compile the lib WITHOUT its `test` cfg).

#[cfg(any(test, feature = "work-counters"))]
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-global count of rows fed into the *buffered* aggregate input since the
/// last [`reset`] (test/feature builds only).
#[cfg(any(test, feature = "work-counters"))]
static BUFFERED_ROWS: AtomicU64 = AtomicU64::new(0);

/// Record that a buffered aggregate input carried `n` rows (`AGG_BUFFERED_ROWS`;
/// consumer D2). Called unconditionally by the materializing `execute_aggregation`;
/// the body compiles to a no-op in a release build.
#[inline(always)]
pub fn record_buffered_rows(n: u64) {
    #[cfg(any(test, feature = "work-counters"))]
    BUFFERED_ROWS.fetch_add(n, Ordering::Relaxed);
    #[cfg(not(any(test, feature = "work-counters")))]
    let _ = n;
}

/// Rows fed into the buffered aggregate input since the last [`reset`]
/// (`AGG_BUFFERED_ROWS`). A GROUP-BY-free aggregate served by the D2 streaming
/// fold records `0`.
#[cfg(any(test, feature = "work-counters"))]
pub fn buffered_rows() -> u64 {
    BUFFERED_ROWS.load(Ordering::Relaxed)
}

/// Clear the process-global counter. Integration tests call this before a measured
/// query so a stale value cannot satisfy a later assertion; because the global is
/// shared, callers serialize on the shared test mutex (the counter-test convention).
#[cfg(any(test, feature = "work-counters"))]
pub fn reset() {
    BUFFERED_ROWS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Exercises the add/get/reset contract on the process-global. Serialized so it
    // never overlaps another test mutating the same global.
    #[test]
    #[serial]
    fn buffered_rows_round_trip_and_reset() {
        reset();
        assert_eq!(buffered_rows(), 0);
        record_buffered_rows(5);
        record_buffered_rows(7);
        assert_eq!(buffered_rows(), 12);
        reset();
        assert_eq!(buffered_rows(), 0);
    }
}
