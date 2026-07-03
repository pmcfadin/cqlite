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
//! (issue #1589), and the [`WindowCursor`] cursor that reduces it to Θ(W) per
//! window.

/// A decompressed-byte window with a FRONT CURSOR (issue #1589).
///
/// Invariant: `start <= buf.len()`. The logical window (the bytes not yet consumed)
/// is `buf[start..]`. [`consume`](Self::consume) advances `start` with NO memmove;
/// [`refill`](Self::refill) reclaims the consumed prefix `buf[..start]` with a
/// SINGLE compaction — once per refill, not once per confirmed partition — then
/// appends the new chunk. So each byte is physically moved at most once per refill
/// it survives (Θ(W) per window over the whole window lifetime), replacing the
/// per-partition `window.drain(0..consumed)` that moved Θ(P·W).
///
/// Window sizing / backpressure semantics are unchanged: [`as_slice`](Self::as_slice)
/// yields exactly the same unconsumed bytes the prior `Vec` did after each drain;
/// only the physical byte movement differs.
pub(crate) struct WindowCursor {
    buf: Vec<u8>,
    start: usize,
}

impl WindowCursor {
    /// A new, empty window.
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::new(),
            start: 0,
        }
    }

    /// The unconsumed bytes — the logical window the parser reads. Cursor-relative.
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    /// Number of unconsumed bytes remaining in the logical window.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.buf.len() - self.start
    }

    /// Whether the logical window is empty (all appended bytes consumed).
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.start >= self.buf.len()
    }

    /// Consume `n` bytes from the FRONT of the logical window by advancing the
    /// cursor — NO memmove. `n` is clamped to the unconsumed length, exactly
    /// matching the prior `drain(0..n.min(len))` semantics (a parser reporting
    /// `consumed > remaining` cannot advance the cursor past the window end).
    #[inline]
    pub(crate) fn consume(&mut self, n: usize) {
        // `capped <= self.len()` and `start + self.len() == buf.len()`, so the
        // cursor never runs past the buffer; no overflow (capped <= buf.len()).
        let capped = n.min(self.len());
        self.start += capped;
    }

    /// Append a freshly decompressed `chunk`. First reclaims the already-consumed
    /// prefix `buf[..start]` with a SINGLE compaction (one memmove of the residual
    /// unconsumed tail), resetting the cursor to 0, THEN extends with `chunk`. The
    /// residual tail is therefore moved at most once PER REFILL — never once per
    /// confirmed partition (issue #1589).
    pub(crate) fn refill(&mut self, chunk: &[u8]) {
        if self.start > 0 {
            // Residual (unconsumed) bytes this single compaction moves. `copy_within`
            // + `truncate` moves exactly `residual` bytes — the same work a
            // `drain(0..start)` would do, but done once per refill, not per partition.
            let residual = self.len();
            #[cfg(feature = "scan-offload-probe")]
            probe::note_bytes_memmoved(residual);
            self.buf.copy_within(self.start.., 0);
            self.buf.truncate(residual);
            self.start = 0;
        }
        #[cfg(feature = "scan-offload-probe")]
        probe::note_bytes_appended(chunk.len());
        self.buf.extend_from_slice(chunk);
    }
}

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

#[cfg(test)]
mod tests {
    use super::WindowCursor;

    #[test]
    fn consume_advances_cursor_without_memmove() {
        let mut w = WindowCursor::new();
        w.refill(b"abcdefgh");
        assert_eq!(w.as_slice(), b"abcdefgh");
        assert_eq!(w.len(), 8);
        assert!(!w.is_empty());
        w.consume(3);
        assert_eq!(w.as_slice(), b"defgh");
        assert_eq!(w.len(), 5);
        w.consume(5);
        assert!(w.is_empty());
        assert_eq!(w.len(), 0);
        assert_eq!(w.as_slice(), b"");
    }

    #[test]
    fn consume_clamps_to_remaining() {
        let mut w = WindowCursor::new();
        w.refill(b"xyz");
        // A parser reporting more consumed than remaining must not run past the end
        // (mirrors the prior `drain(0..take.min(window.len()))` clamp).
        w.consume(100);
        assert!(w.is_empty());
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn refill_compacts_consumed_prefix_and_preserves_residual() {
        let mut w = WindowCursor::new();
        w.refill(b"aabbccdd");
        w.consume(6); // consume "aabbcc", residual "dd"
        assert_eq!(w.as_slice(), b"dd");
        w.refill(b"eeff"); // compact residual "dd" once, then append "eeff"
        assert_eq!(w.as_slice(), b"ddeeff");
        assert_eq!(w.len(), 6);
        // A second consume + refill continues from the compacted base.
        w.consume(2); // "dd"
        w.refill(b"gg");
        assert_eq!(w.as_slice(), b"eeffgg");
    }

    #[test]
    fn refill_from_empty_and_fully_consumed_windows() {
        let mut w = WindowCursor::new();
        // Refill into an empty window: no residual to compact.
        w.refill(b"first");
        w.consume(5); // fully consumed
        assert!(w.is_empty());
        // Refill after full consumption: compaction removes everything, then appends.
        w.refill(b"second");
        assert_eq!(w.as_slice(), b"second");
        assert_eq!(w.len(), 6);
    }
}
