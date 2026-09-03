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
/// # Contract — the caller MUST state which of these its buffer is
///
/// This type is `pub` because it is a REQUIRED parameter of public methods on
/// [`V5CompressedLegacyParser`](super::V5CompressedLegacyParser) (re-exported
/// `doc(hidden)` for integration tests, issue #166): a public method taking an
/// unnameable type cannot be called at all, so the type is re-exported beside
/// the parser (`storage::sstable::reader::BufferExtent`).
///
/// Choosing the WRONG variant is a correctness bug in both directions, and
/// neither direction is safe to guess for the caller:
///
/// * [`BufferExtent::Complete`] on a buffer that is really a WINDOW turns every
///   legitimate straddling row — a row whose bytes continue in the next chunk —
///   into a hard error. The point readers use the tolerant break as their
///   straddle protocol, so this direction is a false refusal on healthy data.
/// * [`BufferExtent::Window`] on a buffer that is really COMPLETE silently
///   drops the remainder of the extent on any decode error: DATA LOSS, reported
///   as `Ok`. That is the defect issue #3782 removed.
///
/// Hence there is deliberately **no** defaulted setter and **no** wrapper that
/// picks a variant: the previous `with_complete_buffer(bool)` builder flag
/// defaulted to the data-losing answer, and a new parse site inherited it
/// silently. A two-variant enum that must be written at the call site keeps the
/// answer beside the buffer it describes. Do NOT reintroduce a default — the
/// earlier "narrow the public surface" review nit was about the defaulted
/// boolean setter, not about this self-documenting required parameter.
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
    ///
    /// Stays `pub(crate)`: the enum has to be NAMEABLE from outside (it is a
    /// public parameter), but out-of-crate callers construct variants rather
    /// than inspect them, so the predicate is not widened with the type.
    pub(crate) fn is_complete(self) -> bool {
        matches!(self, BufferExtent::Complete)
    }
}

/// Issue #3928 — the ONE piece of state that decides whether an undecodable
/// partition HEADER is refused, computed once per walk and consulted at every
/// header arm.
///
/// # Why this exists, and why it replaced a mechanism rather than extending one
///
/// The header arms reached this shape over three review rounds, each moving the
/// boundary by one case: the arm consulted NOTHING (a `tracing::warn!` and
/// `offset += 1`, unconditionally); then a per-site byte-count test
/// (`remaining < 2`, `data.len() < 2`) plus a per-CALL boolean (`bounded`); and
/// each of those turned out to be wrong at its own edges — the byte count
/// because one surviving byte is the first byte of a truncated header and not a
/// tail, the call-wide boolean because a row-body window introduces no
/// uncertainty until its endpoint is REACHED, so partition 0's own header was
/// being excused before the window had done anything.
///
/// Three rounds in one predicate is this repository's signal to replace the
/// mechanism. Both edges are edges of ONE question, so the question is asked
/// once, in one place:
///
/// > Can a byte still arrive, or is this walk's progress no longer attributable?
///
/// * **Can a byte still arrive** — [`BufferExtent`], stated by the caller.
/// * **Is progress attributable** — has every byte before this offset been
///   consumed as a complete structure, so that "a partition starts here" is a
///   promise the walk actually made? It has, until something stops the walk
///   somewhere it was not promised a boundary.
///
/// A byte count answers NEITHER, which is why it is gone from the decision and
/// survives only in the diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::storage::sstable::reader::parsing::row_decoder) struct HeaderTolerance {
    extent: BufferExtent,
    attributable: bool,
}

impl HeaderTolerance {
    /// The block-emit walk, which is handed its extent explicitly. Progress is
    /// attributable from the start: the walk begins at a partition boundary the
    /// caller vouches for.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn for_extent(
        extent: BufferExtent,
    ) -> Self {
        Self {
            extent,
            attributable: true,
        }
    }

    /// The sliding drivers, whose `at_final_chunk` IS the "no further bytes can
    /// arrive" fact — an authoritative property of the window, not a guess about
    /// the bytes. They have no row-body bound, so their progress is always
    /// attributable: each call starts at a partition boundary the caller
    /// advanced to by a confirmed consumed-byte count.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn for_final_chunk(
        at_final_chunk: bool,
    ) -> Self {
        Self::for_extent(if at_final_chunk {
            BufferExtent::Complete
        } else {
            BufferExtent::Window
        })
    }

    /// The walk has been stopped by its caller's #954 row-body END BOUND, i.e.
    /// SHORT of the extent and mid-partition by construction. Every later offset
    /// in this call is therefore unattributable — the bytes there were never
    /// claimed to begin a partition — so tolerance takes over from here on.
    ///
    /// Call this AT the stop, never at the start of the call: that ordering is
    /// the whole of finding C2. A window supplied at the call is not yet an
    /// uncertainty; reaching its endpoint is.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn bounded_out(&mut self) {
        self.attributable = false;
    }

    /// Must an undecodable partition header here be REPORTED rather than
    /// resynchronised past?
    ///
    /// Affirmative in both conjuncts, so a permissive answer is never keyed on
    /// "not the bad one": the buffer must be PROVEN complete AND the walk must
    /// still be attributable.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn refuses(self) -> bool {
        self.extent.is_complete() && self.attributable
    }

    /// Which conjunct is tolerating, for a diagnostic. Never used to decide.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn why_tolerant(
        self,
    ) -> &'static str {
        match (self.extent.is_complete(), self.attributable) {
            (false, _) => {
                "the buffer is a chunk-covering WINDOW, so a header may still be \
                           completed by the next chunk"
            }
            (true, false) => {
                "this walk was stopped by its row-body end bound, so offsets past \
                              that bound were never claimed to begin a partition"
            }
            (true, true) => {
                "nothing is tolerating: this walk is unbounded over a proven-complete \
                             buffer"
            }
        }
    }
}
