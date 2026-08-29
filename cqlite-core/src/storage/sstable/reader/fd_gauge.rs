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
//! [`OpenFdGauge::minted`] is the only PRODUCTION constructor and it increments;
//! `Drop` decrements. The type's single field is private, so it cannot be built by a
//! struct literal that forgets the increment (the same structural guarantee
//! `read_path_probe::BlockingScanTaskGuard` uses), and it is deliberately NOT
//! `Clone`/`Copy` — cloning it would decrement twice for one descriptor.
//!
//! Ordering is `AcqRel` on both sides so a reader thread that observes the
//! decrement also observes everything the closing thread did before releasing the
//! handle; the gauge emission then reports the value the same atomic op returned,
//! never a separate `load` (a load-then-record pair can report a value that was
//! never current under concurrency).
//!
//! # What that does and does NOT promise (issue #1707)
//!
//! It promises that every value REPORTED was the true level at some instant — no
//! reading is fabricated, and none is a stale re-read of a counter that has since
//! moved. It does NOT promise the last reported value is the CURRENT level: the
//! atomic op and the gauge emission are two steps, so two threads transitioning at
//! once (A `fetch_add`→1, B `fetch_sub`→0) can emit in the opposite order to the one
//! they transitioned in, leaving `1` as the last reported value until the NEXT
//! transition corrects it. The window is one emission and it is self-healing, which
//! is why there is deliberately no `Mutex` here: serialising every open and close of
//! a descriptor to tighten a gauge would be paying real contention on the read path
//! for a reading that is already eventually right, and the pre-existing
//! `SSTABLES_OPEN` counter has the byte-for-byte identical shape — one lock here
//! would leave two patterns for one problem. Read the gauge as a level with a
//! transition-latency of one event, not as a serialised ledger.

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
///
/// The single field is PRIVATE, so this cannot be built by a struct literal that
/// forgets the increment (the same structural guarantee
/// `read_path_probe::BlockingScanTaskGuard` uses), and the type is deliberately not
/// `Clone`/`Copy` — cloning it would decrement twice for one descriptor.
#[derive(Debug)]
pub(crate) struct OpenFdGauge {
    /// The counter this guard accounts into. Always [`READER_FDS_OPEN`] in a
    /// production build, where [`Self::minted`] is the only constructor; a
    /// TEST-LOCAL counter under `cfg(test)` via [`Self::minted_into`].
    counter: &'static AtomicI64,
}

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
        Self {
            counter: &READER_FDS_OPEN,
        }
    }

    /// TEST-ONLY: account into `counter` instead of the process-global one, and emit
    /// nothing (a test-local counter has no metric series behind it).
    ///
    /// # Why the counter has to be injectable to test this at all
    ///
    /// The pairing properties — mint increments, `Drop` decrements, an UNWIND still
    /// decrements — are statements about ONE guard, but [`READER_FDS_OPEN`] is
    /// process-global and sibling unit tests in this binary open and close their own
    /// readers CONCURRENTLY. Sampling that global before and after does not isolate
    /// this test's delta; it just reads a foreign thread's transition as this test's
    /// own, which is a race that really fired (a gate run observed `before = 24`,
    /// `after = 23`). Widening the assertion to a range or a direction would not fix
    /// it — the reading is still one a sibling can move. Giving the guard its own
    /// counter removes the shared channel instead of trying to filter it, and still
    /// exercises the same increment / `Drop` / unwind logic.
    #[cfg(test)]
    pub(crate) fn minted_into(counter: &'static AtomicI64) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for OpenFdGauge {
    fn drop(&mut self) {
        let now = self.counter.fetch_sub(1, Ordering::AcqRel) - 1;
        // Emit only for the process-global counter — a test-local one has no series.
        // In a production build `minted` is the only constructor, so this compares a
        // `&'static` against the very address it was initialised with.
        if std::ptr::eq(self.counter, &READER_FDS_OPEN) {
            record(now);
        }
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

    /// A counter of this test's OWN, so the assertions can be exact equalities. See
    /// [`OpenFdGauge::minted_into`] for why the process-global counter cannot carry
    /// these properties.
    static PAIRING_FDS: AtomicI64 = AtomicI64::new(0);
    /// A second private counter, so the two tests cannot perturb each other even
    /// when the harness runs them on different threads at the same time.
    static UNWIND_FDS: AtomicI64 = AtomicI64::new(0);

    #[test]
    fn a_guard_increments_on_mint_and_decrements_on_drop() {
        assert_eq!(PAIRING_FDS.load(Ordering::Acquire), 0);
        {
            let _a = OpenFdGauge::minted_into(&PAIRING_FDS);
            assert_eq!(PAIRING_FDS.load(Ordering::Acquire), 1);
            let _b = OpenFdGauge::minted_into(&PAIRING_FDS);
            assert_eq!(PAIRING_FDS.load(Ordering::Acquire), 2);
        }
        assert_eq!(
            PAIRING_FDS.load(Ordering::Acquire),
            0,
            "both guards decremented — an unpaired increment would leak the level \
             upward for the rest of the process"
        );
    }

    #[test]
    fn an_unwind_still_decrements() {
        assert_eq!(UNWIND_FDS.load(Ordering::Acquire), 0);
        let unwound = std::panic::catch_unwind(|| {
            let _g = OpenFdGauge::minted_into(&UNWIND_FDS);
            assert_eq!(UNWIND_FDS.load(Ordering::Acquire), 1);
            panic!("simulated failure while a descriptor is held");
        });
        assert!(unwound.is_err());
        assert_eq!(
            UNWIND_FDS.load(Ordering::Acquire),
            0,
            "an unwinding open path must not leave the descriptor counted forever"
        );
    }

    #[test]
    fn the_production_constructor_accounts_into_the_process_global_counter() {
        // The injected-counter tests above would still pass if `minted()` accounted
        // somewhere else entirely, so pin the wiring itself: hold a guard from the
        // PRODUCTION constructor and require the global level to be at least 1 while
        // it lives. A lower bound, because concurrent siblings hold descriptors too —
        // this asserts the wiring, never the level (the pairing properties are the
        // deterministic tests above).
        let _g = OpenFdGauge::minted();
        assert!(
            open_fds() >= 1,
            "minted() must account into READER_FDS_OPEN, the counter the \
             cqlite.reader.fds.open gauge reports"
        );
    }
}
