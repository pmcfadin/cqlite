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

    /// Cooperative scan CHECKPOINT: poll for cancellation (issues #2264/#2346)
    /// **and** yield to the async runtime every [`YIELD_STRIDE`]-th call
    /// (issue #1695).
    ///
    /// # Why the yield belongs here
    ///
    /// `query.max_execution_time` is enforced by ONE `tokio::time::timeout` at the
    /// query-engine chokepoint (`query::engine::deadline`). A `timeout` can only
    /// elapse when the future it wraps becomes `Pending`: the read path does long
    /// stretches of SYNCHRONOUS work per poll (stitch a data section, parse a
    /// block, decode a partition), so without a yield point the wrapper could not
    /// fire until the whole scan had finished — the budget would be a placebo on
    /// exactly the runaway full scan it exists to bound.
    ///
    /// This carries NO deadline knowledge: the scan never reads a clock and never
    /// learns the budget (the mandate of issue #1695 is one wrapper at the
    /// chokepoint, never ad-hoc clock checks in the scan loop). It only makes the
    /// walk interruptible at the SAME audited cadence at which it is already
    /// cancellable.
    ///
    /// `tick` is the loop's own counter (partition/entry/chunk index); the
    /// cancellation flag is polled on EVERY call (a relaxed atomic load), the
    /// runtime yield happens on every `YIELD_STRIDE`-th one.
    pub async fn checkpoint(&self, tick: usize) -> crate::Result<()> {
        self.check()?;
        if tick % YIELD_STRIDE == 0 {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// [`Self::checkpoint`] for a loop whose iterations are individually coarse
    /// (a whole block / data section), where every iteration is a correct yield
    /// point.
    pub async fn checkpoint_now(&self) -> crate::Result<()> {
        self.check()?;
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Runtime-yield stride for [`ScanCancel::checkpoint`] — the same 256-iteration
/// cadence the cancellation polls already used (#2264/#2346), so neither
/// cancellation latency nor scan throughput changes measurably: one task
/// reschedule per 256 decoded partitions/entries/chunks.
pub const YIELD_STRIDE: usize = 256;

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

    /// A checkpoint on an UNCANCELLED flag must return `Ok` and (at a stride
    /// boundary) hand control back to the runtime — the property the chokepoint
    /// timeout depends on (issue #1695).
    #[tokio::test]
    async fn checkpoint_yields_at_the_stride_and_relays_cancellation() {
        let c = ScanCancel::new();
        // Stride boundary: yields, still Ok.
        assert!(c.checkpoint(0).await.is_ok());
        // Off-stride: no yield, still Ok.
        assert!(c.checkpoint(1).await.is_ok());
        assert!(c.checkpoint_now().await.is_ok());

        c.cancel();
        // Cancellation is polled on EVERY call, on and off stride, and takes
        // precedence over the yield.
        for tick in [0usize, 1, YIELD_STRIDE, YIELD_STRIDE + 1] {
            assert!(matches!(
                c.checkpoint(tick).await,
                Err(crate::Error::Cancelled)
            ));
        }
        assert!(matches!(
            c.checkpoint_now().await,
            Err(crate::Error::Cancelled)
        ));
    }

    /// The checkpoint's yield is what lets a `tokio::time::timeout` around a
    /// synchronous-looking scan loop actually elapse (issue #1695): without it the
    /// loop would run to completion inside one poll.
    #[tokio::test]
    async fn checkpoint_makes_a_scan_loop_interruptible_by_timeout() {
        let c = ScanCancel::new();
        let out = tokio::time::timeout(std::time::Duration::from_millis(1), async {
            // A loop with NO yield point of its own, exactly like a decode walk.
            for tick in 0..usize::MAX {
                c.checkpoint(tick).await?;
            }
            Ok::<(), crate::Error>(())
        })
        .await;
        assert!(
            out.is_err(),
            "a loop whose only yield point is the checkpoint MUST be interruptible              by an enclosing timeout"
        );
    }

    #[test]
    fn default_is_never_cancelled() {
        let c = ScanCancel::default();
        assert!(!c.is_cancelled());
        assert!(c.check().is_ok());
    }
}
