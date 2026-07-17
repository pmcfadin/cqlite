//! Active-window decode borrow source (issue #1644, K5 stage 2, design D1).
//!
//! [`WindowCursor::borrow`]/[`borrow_subslice`](super::window_cursor::WindowCursor)
//! need a handle to the CURRENT window to decide borrow-vs-copy, but the
//! scalar-value decode call graph (`row_decoder/raw_value.rs`,
//! `row_decoder/cell_value.rs`, `parsing/raw_type_value.rs`,
//! `parsing/complex_column.rs`, `row_decoder/udt.rs`, and the comparator family)
//! is hundreds of `&[u8]`-in/`&[u8]`-out nom-style combinators several call
//! levels below the one place ([`super::scan_stream_windowed`]'s
//! `drain_scan_window`) that owns the [`WindowCursor`]. Design D1 explicitly
//! rejects threading `(&Bytes, offset)` provenance through every one of those
//! signatures — "hundreds of decode signatures ... the copy fallback is
//! unavoidable regardless" — in favor of localizing the decision to one place.
//!
//! This module is THE localization mechanism: an [`ActiveWindowGuard`] installs
//! the current window's `Bytes` backing (when [`Backing::Borrowed`]) in a
//! `thread_local!` for the duration of exactly one
//! `parse_one_partition_with_timestamps` call, and [`borrow_active`] lets any
//! leaf decode site — WITHOUT any new parameter on its own signature — turn a
//! `&[u8]` it already holds (guaranteed, by ordinary slicing, to be a subslice
//! of the window's backing whenever a window is active) into a zero-copy
//! [`Bytes`] view via [`WindowCursor::borrow_subslice`]'s pointer-range check.
//!
//! # Why a thread-local is safe here
//! The entire windowed-scan parse (`drain_scan_window_blocking`) runs
//! SYNCHRONOUSLY on a single dedicated `spawn_blocking` thread: there is no
//! `.await` between installing the guard and dropping it, and no other task
//! interleaves partition-parsing work on that thread while the guard is live.
//! So "thread-local for the scope of one synchronous call" behaves exactly like
//! an implicit extra parameter threaded through every frame — WITHOUT touching
//! any of those frames' signatures. The RAII guard restores the previous value
//! on drop (supporting nested/sequential partition parses safely), and every
//! consumer degrades to a SAFE (never memory-unsafe, never silently wrong —
//! only less zero-copy) owned copy when no window is active: the compaction
//! driver, `get()`/point-lookup decode, and any other non-windowed caller never
//! installs a guard, so [`borrow_active`] always copies for them, unchanged
//! from pre-#1644 behavior.

use super::window_cursor::WindowCursor;
use bytes::Bytes;
use std::cell::RefCell;

thread_local! {
    /// A SNAPSHOT `WindowCursor` over the current streaming-scan window's
    /// active `Bytes` backing (via [`WindowCursor::from_borrowed_bytes`]),
    /// installed for the duration of ONE partition parse by
    /// [`ActiveWindowGuard`]. `None` when no window is active (compaction /
    /// point-lookup / any non-windowed decode path) OR when the real window is
    /// currently STITCHED (a straddling partition — no single parent `Bytes`
    /// exists, so every decode call naturally falls back to a copy via
    /// [`WindowCursor::borrow_subslice`]'s pointer-range check, per D1's
    /// correctness-over-borrow policy).
    static ACTIVE_WINDOW: RefCell<Option<WindowCursor>> = const { RefCell::new(None) };
}

/// RAII guard installing the active decode-borrow source for its scope,
/// restoring the previous value on drop.
pub(crate) struct ActiveWindowGuard {
    previous: Option<WindowCursor>,
}

impl ActiveWindowGuard {
    /// Install a snapshot of `window`'s current `Bytes` backing (via
    /// [`WindowCursor::active_bytes`] — `None` when stitched) as the active
    /// borrow source for the returned guard's scope.
    pub(crate) fn install(window: &WindowCursor) -> Self {
        let snapshot = window.active_bytes().map(WindowCursor::from_borrowed_bytes);
        let previous = ACTIVE_WINDOW.with(|w| w.replace(snapshot));
        Self { previous }
    }
}

impl Drop for ActiveWindowGuard {
    fn drop(&mut self) {
        ACTIVE_WINDOW.with(|w| *w.borrow_mut() = self.previous.take());
    }
}

/// Materialize `sub` — a byte slice a decode site already holds, ordinarily a
/// subslice of the active window's logical bytes — as a [`Bytes`] payload
/// (issue #1644).
///
/// Zero-copy (a refcounted view, via [`WindowCursor::borrow_subslice`]'s
/// pointer-range check) when a window is active AND `sub`'s address range lies
/// within it; an owned copy otherwise (no active window, a stitched straddle,
/// or `sub` sourced from an unrelated buffer). The bounds check is a
/// pointer-VALUE comparison only — it can only ever choose borrow-vs-copy,
/// never affect memory safety, since the copy path dereferences only `sub`
/// itself (a slice the caller already legitimately holds).
pub(crate) fn borrow_active(sub: &[u8]) -> Bytes {
    ACTIVE_WINDOW.with(|w| match w.borrow().as_ref() {
        Some(snapshot) => snapshot.borrow_subslice(sub).into_bytes(),
        None => {
            #[cfg(feature = "scan-offload-probe")]
            super::window_cursor::probe::note_bytes_copied_into_value(sub.len());
            Bytes::copy_from_slice(sub)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrow_active_without_a_guard_copies() {
        let data = b"no active window".to_vec();
        let b = borrow_active(&data);
        assert_eq!(&b[..], &data[..]);
    }

    #[test]
    fn borrow_active_within_installed_window_is_zero_copy() {
        let mut w = WindowCursor::new();
        w.refill_owned(Bytes::from_static(b"hello borrowed world"));
        let guard = ActiveWindowGuard::install(&w);
        let sub = &w.as_slice()[0..5];
        let b = borrow_active(sub);
        assert_eq!(&b[..], b"hello");
        assert_eq!(b.as_ptr(), sub.as_ptr());
        drop(guard);
        // After the guard drops, the same call falls back to a copy.
        let owned = borrow_active(sub);
        assert_eq!(&owned[..], b"hello");
    }

    #[test]
    fn borrow_active_for_stitched_window_copies() {
        let mut w = WindowCursor::new();
        w.refill(b"stitched, no bytes backing");
        let _guard = ActiveWindowGuard::install(&w);
        let sub = &w.as_slice()[0..8];
        let b = borrow_active(sub);
        assert_eq!(&b[..], b"stitched");
    }

    #[test]
    fn nested_guards_restore_previous_on_drop() {
        let mut outer = WindowCursor::new();
        outer.refill_owned(Bytes::from_static(b"outer-window-bytes"));
        let outer_guard = ActiveWindowGuard::install(&outer);
        {
            let mut inner = WindowCursor::new();
            inner.refill_owned(Bytes::from_static(b"inner"));
            let _inner_guard = ActiveWindowGuard::install(&inner);
            let sub = &inner.as_slice()[0..5];
            assert_eq!(&borrow_active(sub)[..], b"inner");
        }
        // Outer window is restored after the inner guard drops.
        let sub = &outer.as_slice()[0..5];
        let b = borrow_active(sub);
        assert_eq!(&b[..], b"outer");
        assert_eq!(b.as_ptr(), sub.as_ptr());
        drop(outer_guard);
    }
}
