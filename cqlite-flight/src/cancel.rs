//! Cooperative cancellation for the Flight merge path (issue #1473).
//!
//! The `do_get` merge is CPU-bound and runs inside `tokio::task::spawn_blocking`.
//! `spawn_blocking` closures are NOT cancelled when the driving async future is
//! dropped, so a client that disconnects mid-`do_get` would otherwise leave the
//! merge running to completion — pinning a blocking-pool thread (the default
//! pool is 512 threads and can saturate under churn) and holding its working
//! memory.
//!
//! [`CancelFlag`] is a cheap, cloneable cooperative-cancellation flag threaded
//! into the merge loop; the loop polls it between partition steps and aborts
//! early (returning a clean `Aborted` status) when it is set. [`CancelGuard`]
//! cancels its flag on `Drop` unless disarmed, so holding the guard inside the
//! `do_get` future makes a future-drop (client disconnect) cancel the in-flight
//! merge deterministically.
//!
//! Backing (issue #2264): the flag wraps a [`tokio_util::sync::CancellationToken`]
//! rather than a bare `AtomicBool`. The polling API ([`CancelFlag::is_cancelled`])
//! is unchanged for the between-step merge checks, but the token additionally
//! exposes an ASYNC-WAKEABLE [`CancelFlag::cancelled`] future. The streaming sink
//! races that future against the bounded-channel backpressure send
//! (`ChannelSink::emit`), so a client disconnect wakes a producer otherwise parked
//! forever in `blocking_send` — a bare `AtomicBool` has no waker and could only be
//! observed between merge steps, never while blocked in a full channel.

use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// A cheap, cloneable cooperative-cancellation flag.
///
/// All clones share one [`CancellationToken`], so cancelling any clone is observed
/// by the merge loop polling another AND wakes anyone awaiting [`Self::cancelled`].
#[derive(Clone, Debug, Default)]
pub struct CancelFlag(CancellationToken);

impl CancelFlag {
    /// Create a fresh, un-cancelled flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Idempotent.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Whether cancellation has been requested. This is the between-step polling
    /// API the merge loop uses; unchanged in semantics from the `AtomicBool`.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// An owned future that resolves when this flag is cancelled (issue #2264).
    ///
    /// Used by the streaming sink to race a client disconnect against a blocked
    /// backpressure send, so a producer parked in `blocking_send` on a full
    /// channel is woken by the cancellation rather than blocking forever. Owned
    /// (not borrowed) so it can be moved into a `tokio::select!` on a blocking
    /// thread's local runtime handle without borrowing `self`.
    pub fn cancelled(&self) -> WaitForCancellationFutureOwned {
        self.0.clone().cancelled_owned()
    }

    /// Arm a [`CancelGuard`] that cancels this flag when dropped (unless
    /// disarmed first). Wire it into a future so a future-drop cancels the flag.
    pub fn drop_guard(&self) -> CancelGuard {
        CancelGuard {
            flag: self.clone(),
            armed: true,
        }
    }
}

/// RAII guard that cancels its [`CancelFlag`] on `Drop` unless [`disarm`]ed.
///
/// [`disarm`]: CancelGuard::disarm
#[derive(Debug)]
pub struct CancelGuard {
    flag: CancelFlag,
    armed: bool,
}

impl CancelGuard {
    /// Disarm the guard so a normal (non-cancelled) completion does NOT cancel
    /// the flag. Call after the guarded work finishes successfully.
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.flag.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_starts_uncancelled_and_cancels() {
        let flag = CancelFlag::new();
        assert!(!flag.is_cancelled());
        flag.cancel();
        assert!(flag.is_cancelled());
    }

    #[test]
    fn clones_share_state() {
        let a = CancelFlag::new();
        let b = a.clone();
        a.cancel();
        assert!(b.is_cancelled(), "cancel on one clone is seen by another");
    }

    #[test]
    fn armed_guard_cancels_on_drop() {
        let flag = CancelFlag::new();
        {
            let _guard = flag.drop_guard();
            assert!(!flag.is_cancelled(), "not cancelled while guard is held");
        }
        assert!(flag.is_cancelled(), "dropping an armed guard cancels");
    }

    #[test]
    fn disarmed_guard_does_not_cancel_on_drop() {
        let flag = CancelFlag::new();
        {
            let mut guard = flag.drop_guard();
            guard.disarm();
        }
        assert!(
            !flag.is_cancelled(),
            "a disarmed guard must not cancel on drop"
        );
    }
}
