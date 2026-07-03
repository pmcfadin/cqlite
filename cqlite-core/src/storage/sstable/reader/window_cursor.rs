//! Sliding decompressed-byte window helpers (issue #1589).
//!
//! Both the user-facing windowed scan (`scan_stream_windowed`) and the streaming
//! compaction driver (`data_access::compaction`) keep a `Vec<u8>` of decompressed
//! bytes, parse CONFIRMED partitions from the FRONT, and refill from the tail as
//! new chunks arrive. Consuming the front with `window.drain(0..consumed)` memmoves
//! the ENTIRE residual tail on every confirmed partition — Θ(P·W) bytes moved per
//! window for P tiny partitions over a W-byte residual (the pathological case is a
//! partition-dense table: thousands of one-row partitions packed into one window).
//!
//! This module hosts the test-only byte-movement [`probe`] that measures that cost
//! (issue #1589), and — after the fix — the [`WindowCursor`] cursor that reduces it
//! to Θ(W) per window.

/// Test-only probe (issue #1589) for the sliding window's byte-movement cost.
///
/// Records the total bytes physically memmoved by the window's consume/compact path
/// and the total decompressed bytes appended, so a guard test can prove that a
/// partition-dense scan moves O(appended) bytes total — each byte at most ~once per
/// window — rather than the Θ(P·W) the per-partition front-drain moved. Compiled
/// ONLY under the non-default `scan-offload-probe` feature, so it adds zero cost and
/// no public surface in normal/release builds; both the windowed scan and the
/// streaming compaction driver feed this one counter, so it covers both sites.
#[cfg(feature = "scan-offload-probe")]
#[doc(hidden)]
pub mod probe {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static ARMED: AtomicBool = AtomicBool::new(false);
    static BYTES_MEMMOVED: AtomicUsize = AtomicUsize::new(0);
    static BYTES_APPENDED: AtomicUsize = AtomicUsize::new(0);

    /// Arm the probe and reset the counters. Call before driving a scan/compaction.
    pub fn arm() {
        BYTES_MEMMOVED.store(0, Ordering::SeqCst);
        BYTES_APPENDED.store(0, Ordering::SeqCst);
        ARMED.store(true, Ordering::SeqCst);
    }

    /// Disarm the probe (restores the production no-op state).
    pub fn disarm() {
        ARMED.store(false, Ordering::SeqCst);
    }

    /// Record `n` bytes physically memmoved while consuming/compacting the window.
    pub(crate) fn note_bytes_memmoved(n: usize) {
        if ARMED.load(Ordering::Relaxed) {
            BYTES_MEMMOVED.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Record `n` bytes appended (decompressed) into the window, if armed.
    pub(crate) fn note_bytes_appended(n: usize) {
        if ARMED.load(Ordering::Relaxed) {
            BYTES_APPENDED.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Total bytes physically memmoved by the window during the armed run.
    pub fn recorded_bytes_memmoved() -> usize {
        BYTES_MEMMOVED.load(Ordering::Relaxed)
    }

    /// Total decompressed bytes appended into the window during the armed run.
    pub fn recorded_bytes_appended() -> usize {
        BYTES_APPENDED.load(Ordering::Relaxed)
    }
}
