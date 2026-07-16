//! Sliding decompressed-byte window helpers (issue #1589; enum-backed
//! borrow API added issue #1644 K5 stage 2, design D1).
//!
//! Both the user-facing windowed scan (`scan_stream_windowed`) and the streaming
//! compaction driver (`data_access::compaction`) keep a window of decompressed
//! bytes, parse CONFIRMED partitions from the FRONT, and refill from the tail as
//! new chunks arrive. This module hosts the test-only byte-movement [`probe`]
//! (issue #1589), the [`WindowCursor`] cursor that bounds per-window memmove to
//! Θ(W), and (issue #1644) the [`Bytes`]-native borrow path that lets a value
//! fully inside one chunk be materialized as a refcounted view instead of a copy.

use bytes::Bytes;
use std::ops::Range;

/// A payload materialized through the window borrow API (issue #1644, D1).
///
/// Both variants carry a [`Bytes`] payload; the distinction exists so the
/// window-borrow work-probe can record how many bytes were physically COPIED
/// (`Owned`) vs BORROWED (zero-copy refcount view, `Borrowed`) — the #1644
/// acceptance proof (scan-window-borrow spec, "bytes copied into values").
#[derive(Debug, Clone)]
pub(crate) enum ValueBytes {
    /// A refcounted view of the parent chunk's `Bytes` — no allocation.
    Borrowed(Bytes),
    /// A standalone copy: either the range straddled a chunk boundary (no
    /// single parent `Bytes` to slice — correctness over borrow, D1), or the
    /// window had no active `Bytes` backing (e.g. the `Vec<u8>`-based
    /// compaction driver, which never adopted the #1940 `Bytes` substrate).
    Owned(Bytes),
}

impl ValueBytes {
    /// Take the payload, discarding the borrowed/owned distinction.
    pub(crate) fn into_bytes(self) -> Bytes {
        match self {
            ValueBytes::Borrowed(b) | ValueBytes::Owned(b) => b,
        }
    }
}

/// Shared pointer-range check (issue #1644, D1): is `sub` a subslice of
/// `base`? If so, calls `on_borrow(offset, len)` to produce a zero-copy
/// [`ValueBytes::Borrowed`]; otherwise falls back to an owned copy of `sub`.
///
/// Used by BOTH [`WindowCursor::borrow_subslice`] (against `self.as_slice()`,
/// when the window's backing is `Backing::Borrowed`) and
/// [`super::value_borrow::borrow_active`] (against the active window's raw
/// `Bytes`, installed via a thread-local for the streaming decode call graph)
/// — the ONE implementation of the pointer-range check, per D1's "localize the
/// borrow/copy decision to one place". The check is a pointer-VALUE
/// comparison only (`as usize` casts, never a dereference outside `sub`'s own
/// valid range), so it can only ever choose borrow-vs-copy, never affect
/// memory safety: the copy fallback dereferences only `sub` itself, a slice
/// the caller already legitimately holds.
pub(crate) fn borrow_bytes_subslice(
    base: &[u8],
    sub: &[u8],
    on_borrow: impl FnOnce(usize, usize) -> ValueBytes,
) -> ValueBytes {
    if sub.is_empty() {
        return ValueBytes::Owned(Bytes::new());
    }
    let base_start = base.as_ptr() as usize;
    let base_end = base_start + base.len();
    let sub_start = sub.as_ptr() as usize;
    let sub_end = sub_start.saturating_add(sub.len());
    if sub_start >= base_start && sub_end <= base_end {
        on_borrow(sub_start - base_start, sub.len())
    } else {
        #[cfg(feature = "scan-offload-probe")]
        probe::note_bytes_copied_into_value(sub.len());
        ValueBytes::Owned(Bytes::copy_from_slice(sub))
    }
}

/// The window's backing storage (issue #1644, D1).
enum Backing {
    /// The logical window IS exactly one chunk's `Bytes` (a cursor over it,
    /// `start` tracks consumption). The steady state: the previous chunk was
    /// fully consumed before the next arrived, so `refill_owned` REPLACED the
    /// backing with the incoming `Bytes` by move + refcount — no copy.
    Borrowed(Bytes),
    /// A partition straddled a chunk boundary, leaving a residual unconsumed
    /// tail that had to be concatenated with the next chunk into an owned
    /// buffer — the pre-#1644 copy path. Also the ONLY state the `Vec<u8>`-
    /// sourced compaction driver ever uses (it has no `Bytes` to move).
    Stitched(Vec<u8>),
}

/// A decompressed-byte window with a FRONT CURSOR (issue #1589) and a
/// [`Bytes`]-native borrow path (issue #1644).
///
/// Invariant: `start <= backing.len()`. The logical window (the bytes not yet
/// consumed) is `backing[start..]`. [`consume`](Self::consume) advances `start`
/// with NO memmove; a refill ([`refill`](Self::refill) /
/// [`refill_owned`](Self::refill_owned)) reclaims the consumed prefix with a
/// SINGLE compaction — once per refill, not once per confirmed partition —
/// then appends the new chunk. Each byte is therefore physically moved at most
/// once per refill it survives.
///
/// [`as_slice`](Self::as_slice) yields exactly the same unconsumed bytes
/// regardless of backing — the parser's scanning/boundary logic is UNCHANGED;
/// only value-materialization call sites use [`borrow`](Self::borrow) /
/// [`borrow_subslice`](Self::borrow_subslice).
pub(crate) struct WindowCursor {
    backing: Backing,
    start: usize,
}

impl WindowCursor {
    /// A new, empty window.
    pub(crate) fn new() -> Self {
        Self {
            backing: Backing::Stitched(Vec::new()),
            start: 0,
        }
    }

    /// A cheap, immutable SNAPSHOT view over `bytes` alone — used by
    /// [`super::value_borrow::ActiveWindowGuard`] to install the current
    /// window's active `Bytes` (issue #1644, D1) as a small standalone
    /// `WindowCursor` a decode site can call [`borrow_subslice`](Self::borrow_subslice)
    /// on, without holding a live reference to the real scanning window (whose
    /// `start` cursor keeps advancing as decode proceeds — the snapshot's own
    /// `start` stays 0, matching `bytes`'s full extent).
    pub(crate) fn from_borrowed_bytes(bytes: Bytes) -> Self {
        Self {
            backing: Backing::Borrowed(bytes),
            start: 0,
        }
    }

    #[inline]
    fn backing_slice(&self) -> &[u8] {
        match &self.backing {
            Backing::Borrowed(b) => b,
            Backing::Stitched(v) => v,
        }
    }

    /// The unconsumed bytes — the logical window the parser reads. Cursor-relative.
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.backing_slice()[self.start..]
    }

    /// Number of unconsumed bytes remaining in the logical window.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.backing_slice().len() - self.start
    }

    /// Whether the logical window is empty (all appended bytes consumed).
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.start >= self.backing_slice().len()
    }

    /// Consume `n` bytes from the FRONT of the logical window by advancing the
    /// cursor — NO memmove. `n` is clamped to the unconsumed length, exactly
    /// matching the prior `drain(0..n.min(len))` semantics (a parser reporting
    /// `consumed > remaining` cannot advance the cursor past the window end).
    #[inline]
    pub(crate) fn consume(&mut self, n: usize) {
        let capped = n.min(self.len());
        self.start += capped;
    }

    /// The current window's `Bytes` backing when it is a single, un-stitched
    /// chunk (issue #1644, D1) — `None` when the window is
    /// [`Backing::Stitched`] (a straddle occurred, or this window has never
    /// received a `Bytes`-sourced refill, e.g. the compaction driver). A clone
    /// is a refcount bump, not a copy: cheap to call once per partition-parse
    /// to install the active borrow source (`value_borrow`).
    pub(crate) fn active_bytes(&self) -> Option<Bytes> {
        match &self.backing {
            Backing::Borrowed(b) => Some(b.clone()),
            Backing::Stitched(_) => None,
        }
    }

    /// Borrow the byte range `range` (relative to the LOGICAL window — the same
    /// coordinate space as `as_slice()`) as a [`ValueBytes`] (issue #1644, D1).
    ///
    /// - [`Backing::Borrowed`] → a zero-copy [`Bytes::slice`] view (refcount
    ///   bump only).
    /// - [`Backing::Stitched`] → an owned copy: a straddling value has no
    ///   single parent `Bytes` to slice from, so correctness takes the copy
    ///   (D1 — never sacrifice correctness to avoid a copy).
    pub(crate) fn borrow(&self, range: Range<usize>) -> ValueBytes {
        let abs_start = self.start + range.start;
        let abs_end = self.start + range.end;
        match &self.backing {
            Backing::Borrowed(b) => ValueBytes::Borrowed(b.slice(abs_start..abs_end)),
            Backing::Stitched(v) => {
                #[cfg(feature = "scan-offload-probe")]
                probe::note_bytes_copied_into_value(abs_end.saturating_sub(abs_start));
                ValueBytes::Owned(Bytes::copy_from_slice(&v[abs_start..abs_end]))
            }
        }
    }

    /// Borrow `sub` — a byte slice a caller holds that is EXPECTED to be a
    /// subslice of `self.as_slice()` — as a [`ValueBytes`] (issue #1644, D1).
    ///
    /// Delegates to [`borrow_bytes_subslice`], the shared pointer-range check
    /// also used by [`super::value_borrow`] (the active-window decode-borrow
    /// source), against `self.as_slice()`.
    pub(crate) fn borrow_subslice(&self, sub: &[u8]) -> ValueBytes {
        borrow_bytes_subslice(self.as_slice(), sub, |offset, len| {
            self.borrow(offset..offset + len)
        })
    }

    /// Reclaim the consumed prefix (single compaction) so a fresh chunk can be
    /// appended at offset 0, returning an owned buffer holding ONLY the
    /// unconsumed residual at offset 0. Shared by both refill entry points below.
    ///
    /// When the backing is already an owned [`Backing::Stitched`] `Vec`, the
    /// residual is compacted IN PLACE (`copy_within` + `truncate`), REUSING the
    /// existing allocation — no fresh `Vec` per refill (the pre-#1644
    /// allocation pattern). A [`Backing::Borrowed`] chunk has no reusable owned
    /// `Vec`, so its residual is copied into a fresh one.
    fn take_residual_buf(&mut self) -> Vec<u8> {
        let start = self.start;
        let residual_len = self.len();
        #[cfg(feature = "scan-offload-probe")]
        if start > 0 {
            probe::note_bytes_memmoved(residual_len);
        }
        match std::mem::replace(&mut self.backing, Backing::Stitched(Vec::new())) {
            Backing::Stitched(mut buf) => {
                if start > 0 {
                    buf.copy_within(start.., 0);
                }
                buf.truncate(residual_len);
                buf
            }
            Backing::Borrowed(b) => {
                let mut buf = Vec::with_capacity(residual_len);
                buf.extend_from_slice(&b[start..]);
                buf
            }
        }
    }

    /// Append a freshly decompressed `chunk` sourced as an OWNED, refcounted
    /// [`Bytes`] (issue #1644 — the #1940 substrate; `scan_stream_windowed`'s
    /// Bytes-sourced IO half).
    ///
    /// When the window is fully consumed (`self.is_empty()`, the steady state:
    /// the previous chunk was drained before this one arrived), REPLACES the
    /// backing directly with `chunk` — a move + refcount bump, NOT a copy —
    /// switching to [`Backing::Borrowed`] so subsequent `borrow`/`borrow_subslice`
    /// calls for this chunk are zero-copy. Otherwise (a residual straddling
    /// tail survives) stitches: copies the residual + `chunk` into one owned
    /// `Vec<u8>` ([`Backing::Stitched`]) — a straddling value has no single
    /// parent `Bytes`, so correctness requires the copy fallback (D1).
    pub(crate) fn refill_owned(&mut self, chunk: Bytes) {
        #[cfg(feature = "scan-offload-probe")]
        probe::note_bytes_appended(chunk.len());
        if self.is_empty() {
            self.backing = Backing::Borrowed(chunk);
            self.start = 0;
            return;
        }
        let mut residual = self.take_residual_buf();
        residual.reserve(chunk.len());
        residual.extend_from_slice(&chunk);
        self.backing = Backing::Stitched(residual);
        self.start = 0;
    }

    /// Append a freshly decompressed `chunk` sourced as a borrowed `&[u8]`
    /// (the `Vec<u8>`-based compaction driver, which never adopted the #1940
    /// `Bytes` substrate). ALWAYS copies — there is no `Bytes` to move — so the
    /// window ends up [`Backing::Stitched`] regardless of prior state. Like the
    /// pre-#1644 path it compacts the residual IN PLACE and appends into the
    /// reused buffer (see [`take_residual_buf`](Self::take_residual_buf)) — no
    /// fresh allocation per refill. See [`refill_owned`](Self::refill_owned) for
    /// the Bytes-sourced zero-copy-refill fast path.
    pub(crate) fn refill(&mut self, chunk: &[u8]) {
        #[cfg(feature = "scan-offload-probe")]
        probe::note_bytes_appended(chunk.len());
        let mut residual = self.take_residual_buf();
        residual.reserve(chunk.len());
        residual.extend_from_slice(chunk);
        self.backing = Backing::Stitched(residual);
        self.start = 0;
    }
}

/// Test-only probe (issue #1589) for the sliding window's byte-movement cost;
/// extended (issue #1644) with a bytes-copied-into-VALUES counter — the
/// scan-window-borrow acceptance proof (bytes-copied-into-values ≈ 0 on the
/// non-straddling borrow path).
///
/// Records the total bytes physically memmoved by the window's consume/compact
/// path, the total decompressed bytes appended, and the total bytes copied
/// (rather than borrowed) into a materialized value. Compiled ONLY under the
/// non-default `scan-offload-probe` feature, so it adds zero cost and no public
/// surface in normal/release builds.
#[cfg(feature = "scan-offload-probe")]
#[doc(hidden)]
pub mod probe {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static ARMED: AtomicBool = AtomicBool::new(false);
    static BYTES_MEMMOVED: AtomicUsize = AtomicUsize::new(0);
    static BYTES_APPENDED: AtomicUsize = AtomicUsize::new(0);
    static BYTES_COPIED_INTO_VALUES: AtomicUsize = AtomicUsize::new(0);

    /// Arm the probe and reset the counters. Call before driving a scan/compaction.
    pub fn arm() {
        BYTES_MEMMOVED.store(0, Ordering::SeqCst);
        BYTES_APPENDED.store(0, Ordering::SeqCst);
        BYTES_COPIED_INTO_VALUES.store(0, Ordering::SeqCst);
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

    /// Record `n` bytes COPIED (not borrowed) into a materialized value's
    /// `Bytes` payload — the #1644 window-borrow work-probe.
    pub(crate) fn note_bytes_copied_into_value(n: usize) {
        if ARMED.load(Ordering::Relaxed) {
            BYTES_COPIED_INTO_VALUES.fetch_add(n, Ordering::Relaxed);
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

    /// Total bytes copied (rather than borrowed) into a materialized value
    /// during the armed run — should stay ≈0 for a non-straddling text-heavy
    /// scan (issue #1644 acceptance proof).
    pub fn recorded_bytes_copied_into_values() -> usize {
        BYTES_COPIED_INTO_VALUES.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // ---- issue #1644 (K5 stage 2): Bytes-native borrow path ----

    #[test]
    fn refill_owned_on_empty_window_moves_not_copies() {
        let mut w = WindowCursor::new();
        let chunk = Bytes::from_static(b"abcdefgh");
        let chunk_ptr = chunk.as_ptr();
        w.refill_owned(chunk);
        assert_eq!(w.as_slice(), b"abcdefgh");
        // The window's active Bytes IS the same allocation (a refcount bump on
        // refill, not a copy) — same base pointer.
        assert_eq!(w.active_bytes().unwrap().as_ptr(), chunk_ptr);
    }

    #[test]
    fn borrow_within_borrowed_backing_is_zero_copy_view() {
        let mut w = WindowCursor::new();
        let chunk = Bytes::from_static(b"0123456789");
        let chunk_ptr = chunk.as_ptr();
        w.refill_owned(chunk);
        let borrowed = w.borrow(2..5);
        match borrowed {
            ValueBytes::Borrowed(b) => {
                assert_eq!(&b[..], b"234");
                // Same underlying allocation as the source chunk (a `slice`
                // view), not a fresh allocation.
                assert_eq!(b.as_ptr(), unsafe { chunk_ptr.add(2) });
            }
            ValueBytes::Owned(_) => panic!("expected a borrowed (zero-copy) view"),
        }
    }

    #[test]
    fn borrow_after_consume_is_cursor_relative() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"0123456789"));
        w.consume(4); // logical window is now "456789"
        let borrowed = w.borrow(0..2);
        match borrowed {
            ValueBytes::Borrowed(b) => assert_eq!(&b[..], b"45"),
            ValueBytes::Owned(_) => panic!("expected borrowed"),
        }
    }

    #[test]
    fn straddling_refill_falls_back_to_owned_copy() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"aabbcc"));
        w.consume(4); // residual "cc" straddles into the next chunk
        w.refill_owned(Bytes::from_static(b"ddee")); // stitches: "cc" + "ddee"
        assert_eq!(w.as_slice(), b"ccddee");
        let borrowed = w.borrow(0..6);
        match borrowed {
            ValueBytes::Owned(b) => assert_eq!(&b[..], b"ccddee"),
            ValueBytes::Borrowed(_) => panic!("a stitched window must copy, never borrow"),
        }
    }

    #[test]
    fn borrow_subslice_matches_active_window_by_pointer() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"hello world"));
        let sub = &w.as_slice()[0..5];
        let sub_copy = sub.to_vec(); // detach the borrow-checker view for later compare
        match w.borrow_subslice(sub) {
            ValueBytes::Borrowed(b) => assert_eq!(&b[..], &sub_copy[..]),
            ValueBytes::Owned(_) => panic!("expected a zero-copy borrowed view"),
        }
    }

    #[test]
    fn borrow_subslice_of_unrelated_buffer_falls_back_to_copy() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"hello world"));
        let unrelated = b"not in the window".to_vec();
        match w.borrow_subslice(&unrelated) {
            ValueBytes::Owned(b) => assert_eq!(&b[..], &unrelated[..]),
            ValueBytes::Borrowed(_) => panic!("an unrelated buffer must never borrow"),
        }
    }

    #[test]
    fn active_bytes_is_none_for_stitched_window() {
        let mut w = WindowCursor::new();
        w.refill(b"abc"); // the &[u8] path always stitches
        assert!(w.active_bytes().is_none());
    }

    #[test]
    fn active_bytes_is_some_for_borrowed_window() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"abc"));
        assert!(w.active_bytes().is_some());
    }
}
