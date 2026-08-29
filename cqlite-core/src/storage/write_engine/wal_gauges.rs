//! WAL observability gauges for the write engine (issue #1707, AI7 of epic #1686).
//!
//! Two readings an operator needs and the engine already holds, so nothing here
//! stats a file or samples anything:
//!
//! * [`record_wal_size`] — the size the [`super::wal::WriteAheadLog`] TRACKS, emitted
//!   at the same post-write seam as the memtable gauges so "memtable growing / WAL
//!   growing" are two readings taken at one instant;
//! * [`record_wal_replay_duration`] — how long the replay at engine open took.
//!
//! A sibling file, not more lines in `mod.rs`, per the campsite rule (#1116): that
//! file is far over the source target already.

use std::time::Duration;

use crate::observability::{self as obs, catalog};

/// Emit the current WAL size (bytes). No-op when the `observability` feature is off
/// or no meter is installed.
pub(super) fn record_wal_size(size: u64) {
    obs::record_gauge(catalog::WAL_SIZE, size as i64, &[]);
}

/// Emit the duration of the WAL replay performed at engine open.
///
/// A free function because replay runs during CONSTRUCTION, before a `WriteEngine`
/// value exists. See [`catalog::WAL_REPLAY_DURATION`] for why this is a histogram in
/// base-unit seconds rather than an `i64` gauge (a sub-second replay would truncate
/// to a fabricated `0`), and why it is recorded even when there was nothing to
/// replay and even when replay found corruption.
pub(super) fn record_wal_replay_duration(elapsed: Duration) {
    obs::record_histogram(catalog::WAL_REPLAY_DURATION, elapsed.as_secs_f64(), &[]);
}
