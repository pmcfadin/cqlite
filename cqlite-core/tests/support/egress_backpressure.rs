//! Shared "how many rows park a merge producer" oracle for the egress
//! backpressure fixtures (issue #2820 review round 2).
//!
//! # Why this module exists
//!
//! Five integration binaries (#2316 ×2, #2370, #2419 ×2) and one in-crate test
//! module needed the SAME premise — *the smallest fixture for which every
//! producer is GUARANTEED to be parked in `SyncSender::send` with nothing
//! received* — and each wrote it out by hand:
//!
//! ```text
//! probe.rows_in_full_channel(rows_cap) + probe.batch_emit_rows + 1
//! ```
//!
//! Deriving it from the shipped constants was the point (`egress_batch`'s probe
//! exists precisely so the premise cannot rot when the batch size, the ramp or
//! the row budget moves), and six verbatim copies reintroduced exactly that
//! drift ONE LEVEL UP: the second term is `batch_emit_rows`, the SATURATED batch
//! size, which after review round 2 is wrong for any run whose adaptive row
//! capacity was squeezed below it — the correct term is the run's own
//! `batch_limit_ceiling`. Fixing that in six places is the failure mode; the sum
//! now lives once, in `EgressBatchProbe::rows_that_park_the_producer`, and this
//! module is the thin per-fixture wrapper that resolves the capacity and applies
//! the historical partition floor.
//!
//! Included by `#[path = "support/egress_backpressure.rs"] mod ...;` — the same
//! shape as `support/os_thread_budget.rs`, because cargo builds each
//! `tests/*.rs` as its own binary and there is no shared test crate.

/// The historical partition-count floor of these fixtures. Kept so a fixture
/// sized purely for backpressure also stays a genuinely MULTI-PARTITION scan —
/// the "the merge does not run to completion" half of what they prove.
pub const MULTI_PARTITION_FLOOR: usize = 400;

/// Rows per input SSTable so that every producer of a merge started NOW is
/// guaranteed to park in `send`.
///
/// `rows_cap` is resolved from the LIVE adaptive budget for the merge this
/// fixture is about to start (`active_merge_count() + 1`, i.e. counting itself),
/// never from the 256 constant: under concurrency the snapshot a new merge
/// receives is smaller, and a fixture sized for 256 would then be sized for a
/// channel that no longer exists.
///
/// A literal would silently rot: pre-#2820 "> 256" meant "past a 256-ENTRY
/// channel", and the channel is now bounded in MESSAGES whose batches are
/// bounded by the row capacity.
pub fn rows_that_park_the_producer() -> i32 {
    let probe = cqlite_core::storage::write_engine::merge::merge_egress_batch_probe();
    let rows_cap = cqlite_core::storage::write_engine::merge::egress_channel_capacity_for(
        cqlite_core::storage::write_engine::merge::active_merge_count() + 1,
    );
    probe
        .rows_that_park_the_producer(rows_cap)
        .max(MULTI_PARTITION_FLOOR) as i32
}
