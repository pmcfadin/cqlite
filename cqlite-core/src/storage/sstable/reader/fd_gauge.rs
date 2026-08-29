//! Reader-reported open-descriptor accounting for [`catalog::READER_FDS_OPEN`]
//! (issue #1707, AI7 of epic #1686).
//!
//! # Why the readers report it, and NOT `/proc`
//!
//! `cqlite.proc.fds` already samples `/proc/self/fd` every ~2s (issue #2419) — the
//! whole process, Linux only, sockets and WAL included. This gauge answers the
//! narrower question an operator actually needs when a read path is close to
//! `EMFILE`: how many descriptors are the SSTable READERS holding right now? The
//! readers know that exactly, at the moment it changes, on every platform — so it is
//! reported, never sampled and never inferred from a byte pattern or a directory
//! listing.
//!
//! # It counts DESCRIPTORS, so several honest zeros exist
//!
//! An mmap-backed source holds a MAPPING, not a descriptor (the fd is closed right
//! after `mmap`), and an `Arc` clone of an existing handle is not a new descriptor.
//! Both contribute NOTHING here rather than a plausible-looking number: the #2314
//! authoritative-data rule forbids inventing a value nobody measured, and an
//! inflated fd count would send an operator hunting a limit they are nowhere near.
//!
//! # One atomic op per change, and the increment cannot be skipped
//!
//! [`OpenFdGauge::minted`] is the ONLY constructor and it increments; `Drop`
//! decrements. The type has a private unit field, so it cannot be built by a struct
//! literal that forgets the increment (the same structural guarantee
//! `read_path_probe::BlockingScanTaskGuard` uses), and it is deliberately NOT
//! `Clone`/`Copy` — cloning it would decrement twice for one descriptor.
//!
//! Ordering is `AcqRel` on both sides so a reader thread that observes the
//! decrement also observes everything the closing thread did before releasing the
//! handle; the gauge emission then reports the value the same atomic op returned,
//! never a separate `load` (a load-then-record pair can report a value that was
//! never current under concurrency).

use std::sync::atomic::{AtomicI64, Ordering};

use crate::observability::catalog;

/// Descriptors the SSTable readers currently hold open, process-wide.
///
/// `i64` because the gauge is `i64`, and because a signed type makes an unpaired
/// decrement show up as a NEGATIVE reading — visibly wrong — instead of wrapping to
/// a huge positive one that reads like genuine fd pressure.
static READER_FDS_OPEN: AtomicI64 = AtomicI64::new(0);

/// One open file descriptor's presence in [`catalog::READER_FDS_OPEN`].
///
/// Stored BESIDE the handle it accounts for, so its lifetime is the descriptor's:
/// every close path — a clean drop, an early return, an unwind — decrements exactly
/// once, with no `close`-site bookkeeping to forget.
#[derive(Debug)]
pub(crate) struct OpenFdGauge(());

impl OpenFdGauge {
    /// Account one descriptor that was JUST opened, and report the new level.
    ///
    /// Call this only where an `open(2)` really happened — never for an `Arc` clone,
    /// a memory mapping, or a handle another `OpenFdGauge` already accounts for.
    pub(crate) fn minted() -> Self {
        // The value the increment itself produced (+1 on the previous), so the
        // reported level is one that was genuinely current.
        let now = READER_FDS_OPEN.fetch_add(1, Ordering::AcqRel) + 1;
        record(now);
        Self(())
    }
}

impl Drop for OpenFdGauge {
    fn drop(&mut self) {
        let now = READER_FDS_OPEN.fetch_sub(1, Ordering::AcqRel) - 1;
        record(now);
    }
}

/// Emit the gauge. No-op (and zero-cost) when the `observability` feature is off or
/// no meter provider is installed.
fn record(level: i64) {
    crate::observability::record_gauge(catalog::READER_FDS_OPEN, level, &[]);
}

/// The current level, for tests and for a caller that wants the reading without
/// waiting for the next change.
#[cfg(test)]
pub(crate) fn open_fds() -> i64 {
    READER_FDS_OPEN.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_guard_increments_on_mint_and_decrements_on_drop() {
        // Relative, never absolute: sibling tests in this binary hold their own
        // readers' descriptors concurrently, so only the DELTA is this test's.
        let before = open_fds();
        {
            let _a = OpenFdGauge::minted();
            assert_eq!(open_fds(), before + 1);
            let _b = OpenFdGauge::minted();
            assert_eq!(open_fds(), before + 2);
        }
        assert_eq!(
            open_fds(),
            before,
            "both guards decremented — an unpaired increment would leak the level \
             upward for the rest of the process"
        );
    }

    #[test]
    fn an_unwind_still_decrements() {
        let before = open_fds();
        let unwound = std::panic::catch_unwind(|| {
            let _g = OpenFdGauge::minted();
            panic!("simulated failure while a descriptor is held");
        });
        assert!(unwound.is_err());
        assert_eq!(
            open_fds(),
            before,
            "an unwinding open path must not leave the descriptor counted forever"
        );
    }
}
