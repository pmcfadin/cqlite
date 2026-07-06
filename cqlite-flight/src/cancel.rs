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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A cheap, cloneable cooperative-cancellation flag.
///
/// All clones share one atomic, so cancelling any clone is observed by the
/// merge loop polling another. `Relaxed` ordering is sufficient: this is a
/// best-effort "stop soon" signal, not a lock guarding other memory.
#[derive(Clone, Debug, Default)]
pub struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
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
