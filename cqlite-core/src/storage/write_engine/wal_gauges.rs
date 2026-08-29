//! WAL observability gauges for the write engine (issue #1707, AI7 of epic #1686).
//!
//! Two readings an operator needs and the engine already holds, so nothing here
//! stats a file or samples anything:
//!
//! * [`record_wal_size`] — the size the [`super::wal::WriteAheadLog`] TRACKS, emitted
//!   at the same post-write seam as the memtable gauges so "memtable growing / WAL
//!   growing" are two readings taken at one instant;
//! * [`record_wal_recovery_duration`] — how long WAL recovery at engine open took.
//!
//! A sibling file, not more lines in `mod.rs`, per the campsite rule (#1116): that
//! file is far over the source target already.
//!
//! # Why the WAL gauge is emitted through the engine's paired `record_size_gauges`
//!
//! [`record_wal_size`] and the memtable gauges are emitted by ONE engine helper
//! called at all THREE seams — the sync write, the async write, and after the
//! post-flush WAL truncate — so the two readings are taken at one instant and
//! neither can be wired into a code path the other is missing from.
//!
//! That pairing exists because the unpaired version failed in both directions
//! (issue #1707): the WAL gauge originally had a SINGLE call site inside the sync
//! `write_into_memtable`, while `write_async_inner` duplicates that logic and
//! emitted only the memtable gauges — so an async-API caller got no
//! `cqlite.wal.size` series AT ALL — and the post-flush truncate emitted nothing, so
//! a sync caller's gauge only ever CLIMBED. The operator doc reads a monotonic climb
//! as "flushes are not keeping up", so a flush that worked perfectly manufactured an
//! alarm. The healthy shape the doc promises is a saw-tooth, and the post-truncate
//! emission is the falling edge of it.

use std::time::Duration;

use crate::observability::{self as obs, catalog};

/// Emit the current WAL size (bytes). No-op when the `observability` feature is off
/// or no meter is installed.
pub(super) fn record_wal_size(size: u64) {
    obs::record_gauge(catalog::WAL_SIZE, size as i64, &[]);
}

/// Emit the duration of the WAL recovery performed at engine open — the CRC
/// validation scan run while opening the log plus the replay of its entries.
///
/// A free function because recovery runs during CONSTRUCTION, before a
/// `WriteEngine` value exists. See [`catalog::WAL_RECOVERY_DURATION`] for why this is
/// a histogram in base-unit seconds rather than an `i64` gauge (a sub-second
/// recovery would truncate to a fabricated `0`), and why it is recorded even when
/// there was nothing to recover and even when replay found corruption.
pub(super) fn record_wal_recovery_duration(elapsed: Duration) {
    obs::record_histogram(catalog::WAL_RECOVERY_DURATION, elapsed.as_secs_f64(), &[]);
}
