//! Issue #3782 — the buffer-extent contract every block-emit parse must STATE.
//!
//! # Why this is a type and not a defaulted flag
//!
//! The block-emit paths have no `NeedMore`/refill vocabulary (unlike
//! `drive_partition_sliding`, which decides the same question from its
//! authoritative `at_final_chunk`), so tolerance cannot be decided inside the
//! parse: only the CALLER knows whether more bytes can still arrive.
//!
//! That question used to be answered by a builder flag defaulting to `false` —
//! the DATA-LOSING direction. A new parse site therefore inherited the tolerant
//! break silently, and that is exactly how the BTI (`da`) full scan came to
//! truncate: `bti_scan_with_metadata_cancellable` stitched the whole data
//! section and then parsed it with a defaulted parser, so a corrupt row ended
//! the scan and returned `Ok` with 120 of 468 rows (measured on a real `da`
//! fixture with one compressed byte flipped). Six stitched-buffer sites were
//! audited for the same shape after roborev job 48 named one of them.
//!
//! Passing the extent EXPLICITLY at the parse call makes that class
//! unrepresentable: a new call site cannot compile without answering the
//! question, and the answer sits beside the buffer it describes rather than
//! several builder calls away.
/// `pub` only because `V5CompressedLegacyParser` is re-exported (`doc(hidden)`)
/// for integration tests, so a `pub fn` taking this type needs it at least as
/// visible. Every production caller is in-crate. A downstream caller can still
/// state either variant — what it can no longer do is INHERIT the lossy one by
/// saying nothing, which is the defect this type exists for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferExtent {
    /// The buffer holds EVERY byte of the extent being decoded — the whole
    /// stitched data section, a partition slice already proven fully consumed,
    /// or a standalone buffer with no continuation. No further bytes can arrive
    /// to finish a row, so a row that fails to decode is truncation or
    /// corruption — DATA LOSS — and MUST be reported.
    Complete,
    /// The buffer is a chunk-covering WINDOW whose tail may cut a row that
    /// continues in the next chunk. A decode failure at the tail is the
    /// ordinary straddling-row case and stays tolerant; refusing here would
    /// break a legitimate, load-bearing control flow (the point readers use it
    /// as their straddle protocol) rather than fix a defect.
    Window,
}

impl BufferExtent {
    /// `true` only for [`BufferExtent::Complete`] — the affirmative value, so a
    /// permissive branch is never keyed on "not the bad one".
    pub fn is_complete(self) -> bool {
        matches!(self, BufferExtent::Complete)
    }
}
