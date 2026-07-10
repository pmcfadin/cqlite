//! Cooperative cancellation for long-running synchronous scans (issue #2264).
//!
//! The compaction streaming read (`stream_all_partitions_for_compaction`) walks
//! a whole Data.db on a detached producer thread. For an index-less
//! (Summary.db-absent) SSTable that walk fully materialises every partition in
//! one uninterruptible pass — so a Flight `do_get` whose client has disconnected
//! keeps burning CPU until a coarse ~1–2 min backstop reaps it, ignoring the
//! cancellation the transport already received.
//!
//! [`ScanCancel`] is a cheap, cloneable flag the scan polls at a bounded interval
//! (every N partitions). It is deliberately a bare `AtomicBool` — the scan runs
//! on a plain `std::thread` with no async runtime, so it cannot poll an
//! async-wakeable token. The Flight layer bridges its own
//! `tokio_util::CancellationToken`-backed `CancelFlag` onto one of these so a
//! single `cancel()` trips both the async channel-race (PR #2282) AND this
//! synchronous scan poll.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A cheap, cloneable cooperative-cancellation flag for synchronous scans.
///
/// All clones share one `AtomicBool`, so cancelling any clone is observed by a
/// scan polling another. A default (never-cancelled) flag makes every threaded
/// call site a no-op, so non-cancellable callers pass `ScanCancel::default()`.
#[derive(Clone, Debug, Default)]
pub struct ScanCancel(Arc<AtomicBool>);

impl ScanCancel {
    /// Create a fresh, un-cancelled flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Return `Err(Error::Cancelled)` if cancellation has been requested, else
    /// `Ok(())`. The polling helper scan loops call at a bounded interval.
    pub fn check(&self) -> crate::Result<()> {
        if self.is_cancelled() {
            Err(crate::Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_uncancelled_and_cancels() {
        let c = ScanCancel::new();
        assert!(!c.is_cancelled());
        assert!(c.check().is_ok());
        c.cancel();
        assert!(c.is_cancelled());
        assert!(matches!(c.check(), Err(crate::Error::Cancelled)));
    }

    #[test]
    fn clones_share_state() {
        let a = ScanCancel::new();
        let b = a.clone();
        a.cancel();
        assert!(b.is_cancelled(), "cancel on one clone is seen by another");
    }

    #[test]
    fn default_is_never_cancelled() {
        let c = ScanCancel::default();
        assert!(!c.is_cancelled());
        assert!(c.check().is_ok());
    }
}
