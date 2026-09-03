//! Test-only rendezvous plumbing for [`WarmTableRegistry`] (campsite split,
//! epic #1116 / issue #3940).
//!
//! Three `#[cfg(test)]` hooks, each a `Mutex<Option<Arc<dyn Fn()>>>` field on
//! the registry (declared there, with the doc that says what each one is FOR):
//!
//! | hook | fires |
//! |---|---|
//! | `swap_barrier` | in `rebuild`, past the probe + open, before the swap lock |
//! | `open_barrier` | at the top of each `open_added` iteration, BEFORE its cancel check |
//! | `open_parse_barrier` | inside the coalesced real-open closure, immediately before the Index.db open+parse |
//!
//! One discipline applies to all three `run_*`: clone the `Arc` OUT of the lock
//! and invoke the hook WITHOUT holding it, so a hook that re-enters the registry
//! (or blocks on another thread that does) cannot self-deadlock on the hook slot.
//! `None` is a no-op, and none of this exists in a non-test build.

use std::sync::{Arc, PoisonError};

use super::WarmTableRegistry;

impl WarmTableRegistry {
    /// Install the test-only swap rendezvous (see the field doc).
    pub(crate) fn set_swap_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .swap_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the swap rendezvous if one is installed.
    pub(super) fn run_swap_barrier(&self) {
        Self::run_hook(&self.swap_barrier);
    }

    /// Install the test-only per-open rendezvous (see the field doc).
    pub(crate) fn set_open_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .open_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the per-open rendezvous if one is installed. Visible to
    /// [`super::super::rebuild`]'s `open_added` (campsite split).
    pub(in crate::warm) fn run_open_barrier(&self) {
        Self::run_hook(&self.open_barrier);
    }

    /// Install the test-only pre-parse rendezvous (see the field doc).
    pub(crate) fn set_open_parse_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .open_parse_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the pre-parse rendezvous if one is installed. Visible to
    /// [`super::super::rebuild`]'s `open_added` (campsite split).
    pub(in crate::warm) fn run_open_parse_barrier(&self) {
        Self::run_hook(&self.open_parse_barrier);
    }

    /// Clone the installed hook OUT of `slot` and invoke it with the slot lock
    /// RELEASED (see the module doc). A poisoned slot is recovered rather than
    /// panicked on, matching the registry's `lock_inner` discipline.
    fn run_hook(slot: &std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>) {
        let hook = slot.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if let Some(f) = hook {
            f();
        }
    }
}
