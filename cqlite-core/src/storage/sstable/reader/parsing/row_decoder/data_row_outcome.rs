//! Issue #3809 (Finding 1) — the DISCRIMINATED outcome of a data-row policy hook.
//!
//! # Why one `Err` channel was not enough
//!
//! Issue #3782 gave [`SlidingPartitionPolicy::on_data_row`] an error channel
//! meaning *the row FAILED TO DECODE*, and made the DRIVER — never the policy —
//! decide tolerance from its authoritative `at_final_chunk`: at the final chunk no
//! further bytes can arrive, so the failure is truncation/corruption and is
//! refused; mid-window it is the ordinary straddling-row case and is answered with
//! `NeedMore`. That is exactly right for its subject, which is a
//! BYTES-AVAILABILITY question.
//!
//! Issue #3809 needs the opposite disposition for a different subject. Its
//! clustering-identity check
//! (`CompactionRowData::require_tombstone_clustering_identity`) fires only AFTER
//! the row DECODED successfully: a row deletion reached the builder having
//! recovered fewer clustering values than its table declares, so emitting it would
//! hand the merge a deletion that identifies no row. **No amount of refilling can
//! make a short clustering become full-arity**, so "maybe more bytes will arrive"
//! is not merely unhelpful there, it is semantically inapplicable — and on every
//! entry point that declares [`BufferExtent::Window`](super::BufferExtent::Window)
//! (the point and promoted readers, the block-by-block scans, the windowed emit)
//! `at_final_chunk` is `false`, so a single undiscriminated `Err` channel would
//! convert the refusal into a refill request and silently truncate the read: the
//! very tolerant-tail data loss both issues exist to remove.
//!
//! # Why a distinct OUTCOME rather than a kind test at the driver
//!
//! Two shapes were available. A kind test at the driver (`if err.is_refusal()`)
//! would need a discriminable marker on [`crate::error::Error`], i.e. either a new
//! public variant — a public-surface change nothing in this repo detects the drift
//! of (#3366), and one that would retitle #3809's deliberately-chosen
//! `Corruption` telemetry bucket — or a message-text test, which the no-heuristics
//! mandate (#28) forbids outright.
//!
//! A distinct outcome fits this trait instead, for three reasons:
//!
//! * The trait ALREADY owns a policy-outcome enum of exactly this shape
//!   ([`MarkerOutcome`](super::MarkerOutcome)), where the policy reports what it
//!   found and the driver decides how to advance.
//! * It makes the data-losing default UNREPRESENTABLE, which is the argument
//!   [`BufferExtent`](super::BufferExtent) itself is built on. Had the hook kept a
//!   `Result`, a future `?` inside a policy body — the most natural thing to write
//!   — would route a refusal into the TOLERATED channel silently, and that is the
//!   defaulted-flag defect #3782 removed, reintroduced one layer up. With no
//!   `Result` in the signature, `?` does not compile and every failure site must
//!   NAME which of the two it is.
//! * Both dispositions stay literally true and are readable side by side, so the
//!   trait no longer carries two contracts on one channel.
//!
//! The cost is stated rather than hidden: a policy that acquires a genuine
//! plumbing error (neither a decode failure nor a refusal) has no `Err` to return
//! and must classify it. No policy has one today — all three route a
//! `parse_row_data_*` `Result` and nothing else — and being forced to choose is
//! the point.

use crate::error::Error;

/// What a [`SlidingPartitionPolicy::on_data_row`](super::SlidingPartitionPolicy::on_data_row)
/// call found. FOUR outcomes on ONE channel: the two failures are separate
/// variants because the driver must treat them differently and cannot tell them
/// apart from an [`Error`] value.
#[derive(Debug)]
pub(super) enum DataRowOutcome {
    /// The row decoded and was handled; continue the row loop at this offset.
    Decoded(usize),
    /// The policy DECLINES the row with no error to report. Unchanged pre-#3782
    /// behaviour: end-of-partition on the final chunk, else `NeedMore`.
    ///
    /// `dead_code`-allowed, and the reason is worth recording rather than
    /// silencing: NO production policy declines TODAY. Both classify their one
    /// failure since #3782 (`DecodeFailed`), so the decline path is reached only by
    /// the driver's [`StubPolicy`](super::partition_driver) harness. That was
    /// already true on `main` and merely INVISIBLE there, because the hook returned
    /// `Option<usize>` and `None` is a std variant no lint can call unconstructed.
    /// It is kept because the DRIVER's disposition of it is load-bearing contract —
    /// distinct from both failures, pinned by test (f) — and deleting the variant
    /// would delete that behaviour along with its test.
    #[allow(dead_code)]
    Declined,
    /// The row FAILED TO DECODE, with the decoder's error preserved (issue #3782).
    /// A BYTES-AVAILABILITY answer: the policy does NOT decide tolerance, the
    /// driver does, from `at_final_chunk` — refused at a proven-complete buffer,
    /// tolerated as a straddling row mid-window.
    DecodeFailed(Error),
    /// The row DECODED but MUST NOT be emitted (issue #3809): a semantic refusal.
    /// The driver propagates it UNCONDITIONALLY — `at_final_chunk` is not
    /// consulted, because the question is not about bytes and refilling the window
    /// cannot change the answer.
    Refused(Error),
}
