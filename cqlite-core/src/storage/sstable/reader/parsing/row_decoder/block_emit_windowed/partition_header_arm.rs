//! Issue #3928 — the ONE partition-HEADER arm of the block-emit walk.
//!
//! Split out of `block_emit_windowed.rs` (campsite rule, epic #1116) when that
//! arm gained a tolerance DECISION: it now answers three different things
//! (a decoded header, the end of a window, a resynchronisation) and the choice
//! between the last two rests on the caller's [`BufferExtent`], so it is a
//! responsibility rather than a few inline bounds checks.
//!
//! # What changed, and why the tolerant answer was unsound
//!
//! Both arms used to `tracing::warn!` and then `offset += 1` unconditionally.
//! On a buffer the caller has PROVEN complete that has two consequences, and the
//! second is the bad one:
//!
//! 1. the malformed partition is silently DROPPED, and
//! 2. the one-byte resync can land on MISALIGNED bytes that parse as a plausible
//!    header — so the walk INVENTS a partition that does not exist.
//!
//! Measured on a real Cassandra 5.0 `da` fixture with ONE compressed byte
//! flipped so the first partition's `DeletionTime` discriminator reads `0xFF`
//! (which Cassandra's own `DeletionTime.Serializer.deserialize` throws on,
//! cassandra-5.0.8 `DeletionTime.java:222-230`): `distinct_partition_keys`
//! answered `Ok` with **5** partition keys where the pristine fixture has **3**
//! — 2 FABRICATED — and `get_all_entries` answered `Ok` with **0** of 468 rows.
//! Both are reported as success. The count moves in BOTH directions, which is
//! why no count-based check and no total-comparing parity oracle can see this.
//!
//! `BufferExtent::Complete` is the authoritative discriminator, exactly as it is
//! for the row arm #3782 fixed: no further bytes can arrive, so a header that
//! does not decode is truncation or corruption and both are DATA LOSS. On a
//! `Window` the SAME condition is the ordinary straddle — a header split across
//! the chunk boundary — and stays tolerant.

//! # Reachable only from a CONFIRMED boundary (issue #3928 round 5)
//!
//! This arm is entered ONLY where the walk has been promised a partition starts:
//! the block start (the caller's own offset 0), the byte after an
//! `END_OF_PARTITION` the row loop just consumed, or an offset a
//! `peek_is_partition_header` confirmed. That property is what makes the
//! `Ready`-then-unparseable refusal below SAFE rather than merely justified, and
//! it is maintained by the four breaks in `block_emit_windowed.rs` that leave the
//! cursor at an UNCONFIRMED position, each of which now ENDS THE WALK instead of
//! falling through to here:
//!
//! * the #954 row-body bound (finding B1) — nothing further was requested;
//! * the tolerant ROW break — the straddle protocol, so the cursor is MID-ROW;
//! * the three range-tombstone MARKER failures (finding I2) — the cursor is ON
//!   the marker. These `return` the MARKER's own error at EVERY extent, which
//!   delivers the same property a fortiori (a `return` cannot fall through to
//!   here at all) and is issue #3721's decision, not this one's — see
//!   "Reconciling #3721 and #3928" below.
//!
//! # Why that matters, measured
//!
//! `Ready` proves only that enough bytes for a CANDIDATE header exist at this
//! offset — not that the offset IS a boundary. Readiness reads `data[1]` as the
//! key length and never inspects byte 0, and any oa/da discriminator byte with
//! bit 7 set that is not exactly `0x80` fails the full parse. So row payload
//! classifies `Ready`-then-`Err` at a mid-row offset, and while the row break
//! fell through to here, the (correct-at-a-boundary) unconditional refusal
//! REJECTED HEALTHY WINDOWS: measured on the real `da` fixture
//! `test_da.wide_table` with its own gates, **294096 of 1857615** healthy window
//! prefixes refused, every one through this arm. The ASCII-payload `da` fixtures
//! showed 0 of 57548 and 0 of 112255 — the arm is simply unreachable in them, so
//! they could not have detected it.
//!
//! Both directions are pinned in `issue_3928_corrupt_header_refusal.rs`:
//! `a_windowed_block_read_refuses_a_complete_but_structurally_invalid_header`
//! (refuse at a boundary) and `no_healthy_window_prefix_is_ever_refused` (never
//! refuse a healthy window).
//!
//! # Reconciling #3721 and #3928 (rebase onto PR #3814)
//!
//! Both issues are refuse-don't-swallow fixes to the SAME walk, and at the four
//! breaks above they were written independently and appeared to disagree. They do
//! not, once the two questions are separated:
//!
//! * **#3721 asks WHETHER a failure is reported to the caller.** Only it can
//!   tell, and only at the site that holds the error: an
//!   [`Error::ColumnDecode`] is a per-column decode failure the caller owns the
//!   tolerance decision for (`column_decode_error::end_of_partition_or_bail`),
//!   while a range-marker failure is corruption and never a terminator
//!   (`range_marker_error`).
//! * **#3928 asks WHERE a failure that is NOT reported leaves the cursor**, and
//!   hence whether this arm may be entered from it.
//!
//! So they compose per site rather than competing:
//!
//! * the THREE MARKER sites `return` unconditionally (#3721). #3928's I2
//!   invariant is satisfied *more* strictly than by its own proposed
//!   `break 'partitions`, because a `return` ends the walk at every extent and
//!   can never reach this arm. Gating those returns on `extent.is_complete()` —
//!   which is what #3928 round 5 wrote, before #3721 landed — was MEASURED to
//!   reintroduce #3721's defect: the scan passes `Window` even with the whole
//!   partition buffered, so all three `d9_select_marker_parse_failure` cases went
//!   back to `Ok` with the marker and every later row silently dropped. And
//!   nothing #3928 measured argues the other way: all 294096 healthy-window
//!   refusals above arrived through the ROW break, none through a marker site.
//! * the ROW break keeps #3721's `end_of_partition_or_bail(..)?` (so a
//!   `ColumnDecode` still reaches the caller — `issue_3721_bti_point_read_absence`
//!   pins that) and then `break 'partitions` rather than a plain `break` (#3928),
//!   because an `Ok` from that call establishes only "not `ColumnDecode`" and
//!   consumes nothing: the cursor is where the failed row parse STARTED, which is
//!   a boundary in neither direction.
//!
//! ## Residuals, both of them #3721's and both still open
//!
//! 1. A marker refusal is `Error::Corruption`, which
//!    `column_decode_error::indexed_walk_falls_back` does not recognise (it
//!    matches `is_column_decode` only), so an index-NARROWED read fails instead
//!    of retracting. The fix is a TYPED retraction signal — matching message text
//!    would violate the no-heuristics rule (#28) — not tolerance, which would
//!    merely make the narrowed read silently short.
//! 2. A `Window` that cuts a VALID marker's body is refused rather than treated
//!    as the straddle it is. That IS decidable structurally, by the move #1741
//!    made for headers (`partition_header_readiness`) applied to the marker's own
//!    framing: every byte present and still failing is corruption, absent bytes
//!    are a straddle. It is new parse-side machinery rather than a reconciliation
//!    of two existing changes, so it is named here and not invented.
//!
//! Until (1) or (2) lands, the accepted direction at a marker is a conservative
//! false refusal on a narrowed read — recoverable, and loud — over a silent short
//! answer from `SELECT`, which is the defect itself.

use super::super::buffer_extent::HeaderTolerance;
use super::super::*;

/// What the block-emit walk should do with the bytes at a candidate partition
/// start.
pub(in crate::storage::sstable::reader::parsing::row_decoder) enum HeaderStep {
    /// A structurally-valid header: the partition key, the offset of the first
    /// row/marker, and the partition-level deletion.
    Parsed(RowKey, usize, Option<(i64, i32)>),
    /// Fewer bytes remain than even a header needs. Only reachable on a
    /// [`BufferExtent::Window`] (a `Complete` buffer refuses instead), where it
    /// is the ordinary end of the window.
    EndOfBlock,
    /// The header is undecodable and more bytes may still arrive, so the caller
    /// skips ONE byte and re-tries. Only reachable on a
    /// [`BufferExtent::Window`].
    Resync,
}

impl V5CompressedLegacyParser {
    /// Decode the partition header at `offset`, or say what the walk should do
    /// instead.
    ///
    /// Tolerance is decided by ONE piece of state, [`HeaderTolerance`], which the
    /// caller owns and threads in: "can a byte still arrive, or is this walk's
    /// progress no longer attributable?" See that type for why it replaced the
    /// per-site byte count plus per-call boolean this arm used to consult, and
    /// for the two edges each of those got wrong.
    ///
    /// Two consequences worth stating here, at the arm:
    ///
    /// * partition 0's own header is refused on a BOUNDED call too. A row-body
    ///   window supplied at the call is not yet an uncertainty; reaching its
    ///   endpoint is, and `HeaderTolerance::bounded_out` is called there
    ///   (finding C2).
    /// * `row_body_window.is_some() && partition_index == 0` — which reads as
    ///   that property and was the obvious patch — is NOT what this does, and
    ///   was measured to be wrong: `partition_index` is incremented at the END of
    ///   the windowed partition's arm, so at the very offset `body_end` leaves
    ///   behind it already reads `1`, and the refusal fired on correct input
    ///   (`regression_1741c_tests::range_tombstone_before_slice_is_shadowed_on_windowed_read`,
    ///   observed: `only 1 byte(s) remain … (partition=1)`, the byte being that
    ///   partition's own `END_OF_PARTITION` marker). Tracking the STOP rather
    ///   than inferring it from a partition index is what makes both edges right
    ///   at once.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn block_partition_header(
        &self,
        data: &[u8],
        offset: usize,
        tolerance: HeaderTolerance,
        partition_index: usize,
    ) -> Result<HeaderStep> {
        let refuse = tolerance.refuses();
        // Issue #3928 fix round 3 (B2) — the oa/da DeletionTime SIZING RULE is
        // CALLED, never re-derived.
        //
        // This arm used to compute its own minimum,
        // `if has_uint_deletion_time() { 1 } else { 12 }`, with no look at the
        // discriminator. That is the PRE-#1741 rule: only the LIVE sentinel
        // (`0x80`) is one byte, and a DELETED oa/da partition carries the full
        // 12-byte form. So a legitimate deleted header STRADDLING a
        // `BufferExtent::Window` passed the 1-byte minimum, failed the full
        // parse, and returned `Resync` — discarding a header byte of HEALTHY
        // data, the direction `buffer_extent.rs` records as being as much a
        // defect as a swallow.
        //
        // #1741 fixed exactly that, inside `partition_header_readiness`, which
        // peeks `data[2 + key_len]` to size the form and answers `Incomplete`
        // when the discriminator itself is absent. The two sliding drivers route
        // through it; this arm did not, so the two implementations of one rule
        // DRIFTED — the driver path got the fix and this one kept the bug. It now
        // asks the same classifier, and the pair is pinned together in
        // `regression_1741k_tests.rs`.
        //
        // The classifier also owns the key-length limits, so this arm no longer
        // carries its own copy of the 255-byte cap.
        //
        // `remaining` survives for the DIAGNOSTICS only: no decision is made from
        // a byte count (fix round 2), and the sub-two-byte case an earlier
        // revision handled separately is simply `Incomplete` to the classifier.
        let remaining = data.len().saturating_sub(offset);
        // `get(..).unwrap_or_default()` rather than `&data[offset..]`: this is
        // reachable from `row_decoder` now, so an out-of-range offset must read
        // as an empty buffer (which the classifier answers `Incomplete`) and
        // never as a slice panic.
        let head = data.get(offset..).unwrap_or_default();
        match self.partition_header_readiness(head) {
            // The header — or, for an oa/da DELETED partition, its full 12-byte
            // DeletionTime — is not entirely present.
            //
            // On a WINDOW that is the ordinary straddle: the walk ends here and
            // the caller refills. On an UNBOUNDED, proven-complete buffer no
            // further bytes can arrive, so the partition Cassandra wrote here was
            // truncated away and reporting a clean end of block would DROP it —
            // the same verdict the drivers' arm reaches from the same
            // classification (finding C1), and the reason the two stitched walks
            // agree (`data_access/mod.rs:249` vs `:288` hand both the SAME
            // stitched `Complete` buffer).
            PartitionHeaderReadiness::Incomplete => {
                if refuse {
                    return Err(undecodable_partition_header(
                        offset,
                        &format!(
                            "the header is INCOMPLETE — {remaining} byte(s) remain, too few to \
                             hold its declared key AND the DeletionTime for its live/deleted \
                             form (partition={partition_index}){}",
                            structural_note_at(data, offset)
                        ),
                    ));
                }
                tracing::debug!(
                    "V5CompressedLegacy: partition header at offset {offset} is INCOMPLETE \
                     ({remaining} byte(s) remain), stopping — tolerated because {}",
                    tolerance.why_tolerant()
                );
                return Ok(HeaderStep::EndOfBlock);
            }
            // A zero or over-long declared key length — decidable from two bytes,
            // so no further byte can change the verdict.
            //
            // NOTE: no heuristic validation of `flags` (issues #258, #28); the
            // classifier reads the LENGTH only. Issue #164's concern — that a
            // block's trailing bytes must not be assumed to begin another
            // partition just because `offset < data.len()` — is answered by this
            // arm's three-way classification rather than by a hand-rolled
            // minimum.
            PartitionHeaderReadiness::Malformed => {
                let detail = format!(
                    "CQLite READ a key length of {} here, which it rejects as zero or \
                     over-long, with {remaining} byte(s) available (partition={partition_index}){}",
                    head.get(1).copied().unwrap_or(0),
                    structural_note_at(data, offset)
                );
                if refuse {
                    return Err(undecodable_partition_header(offset, &detail));
                }
                tracing::warn!(
                    "V5CompressedLegacy: Skipping malformed partition header at offset {offset} \
                     ({detail}) — resynchronising because {}",
                    tolerance.why_tolerant()
                );
                return Ok(HeaderStep::Resync);
            }
            // Every header byte is present, so the full parse below cannot fail
            // from truncation.
            PartitionHeaderReadiness::Ready => {}
        }

        match self.parse_partition_header_full(data, offset) {
            Ok((key, next_offset, deletion)) => Ok(HeaderStep::Parsed(key, next_offset, deletion)),
            Err(e) => {
                // Issue #3928 fix round 4 — REPORTED UNCONDITIONALLY, at every
                // extent, exactly as the sliding drivers report it.
                //
                // `Ready` is an AFFIRMATIVE guarantee from
                // `partition_header_readiness` that every header byte is present:
                // the key length, the key, and the `DeletionTime` for its
                // live/deleted form, sized by PEEKING the discriminator (#1741).
                // So **a `Ready` header cannot straddle**, and that is what
                // decides this arm rather than the extent.
                //
                // The tolerant break here used to be justified by the point
                // readers' straddle protocol. That justification does not reach
                // this arm: the break protects a straddling ROW, and AC1 licenses
                // tolerance only where "a header can legitimately straddle" —
                // which a `Ready` header provably cannot. No later chunk can
                // repair an invalid oa/da deletion-time discriminator, so
                // resynchronising past it drops a real partition and can invent
                // misaligned ones.
                //
                // Measured on the real `da` fixture with its first partition's
                // discriminator flipped to `0xFF` (a byte Cassandra's own
                // `DeletionTime.Serializer.deserialize` throws on) parsed under a
                // `BufferExtent::Window`: `Ok` with 401 of 468 rows — **180 LOST
                // and 113 FABRICATED**. Pinned by
                // `a_windowed_block_read_refuses_a_complete_but_structurally_invalid_header`.
                //
                // The corpus cannot evidence this arm in either direction (AC3's
                // counters record 0 arrivals here across 126 tables), which is
                // why the absence of a measured loss was never a reason to
                // tolerate it.
                //
                // This arm composes NO message: `e` is `scan_partition_header`'s
                // own, and after `Ready` it can realistically only be the
                // `oa`/`da` invalid-IS_LIVE-byte rejection, which does not rest
                // on the key-length model — so no #3999 caveat is owed here.
                // Rewording that error's own text belongs to #3999 with the model
                // it asserts.
                Err(e)
            }
        }
    }
}

/// The refusal a partition header raises where the caller has PROVEN no further
/// bytes can arrive.
///
/// Deliberately NOT shared with `partition_driver.rs` /
/// `compaction_stream.rs`, which reach the same decision from `at_final_chunk`
/// and own their own `ParseStep`/`PartitionStreamStep` vocabularies — the same
/// call the #3782 row arm made when it spelled the rule out in each driver
/// rather than inventing one abstraction over three different advance
/// protocols. What must not diverge is the DECISION, and that is one predicate
/// per driver over its own authoritative signal.
fn undecodable_partition_header(offset: usize, detail: &str) -> Error {
    Error::corruption(format!(
        "V5CompressedLegacy: refusing an undecodable partition header at offset {offset} \
         ({detail}); this walk is UNBOUNDED over a PROVEN-COMPLETE buffer, so no further \
         bytes can arrive to finish this header. Skipping a byte to resynchronise would \
         DROP this partition and can land on misaligned bytes that parse as a plausible \
         header, INVENTING a partition that does not exist (issue #3928). \
         NOTE (#3999): CQLite reads the partition key length as ONE byte after a 'flags' \
         byte, while Cassandra writes an unsigned 2-byte big-endian length and no flags \
         byte (SortedTablePartitionWriter.start -> ByteBufferUtil.writeWithShortLength), \
         so the two models agree only for keys under 256 bytes; for a longer key this \
         refusal may be CQLite's model and not corruption."
    ))
}

/// Issue #3928 — name the leading byte's STRUCTURAL class when it is one this arm
/// should never have been handed.
///
/// Round 1 (I2) added this because the row loop broke with `offset` still ON a
/// range-tombstone marker it could not parse or feed, and the outer partition
/// loop then re-entered HERE — so the diagnostic read "undecodable partition
/// header" for a MARKER failure and sent the next reader to the wrong arm. That
/// route is GONE as of round 5: all three marker sites now `return` the marker's
/// own error at every extent (issue #3721), so this arm is reachable only from a
/// CONFIRMED partition boundary.
///
/// The notes are kept because a boundary can still legitimately be REACHED with
/// a marker or END_OF_PARTITION byte at it on malformed input (two consecutive
/// end markers, say), and naming that is still the difference between one read
/// and a debugging session. What they no longer describe is a fall-through.
fn structural_note_at(data: &[u8], offset: usize) -> &'static str {
    let Some(&leading) = data.get(offset) else {
        return "";
    };
    if V5CompressedLegacyParser::is_end_of_partition(leading) {
        " — NOTE: this leading byte is an END_OF_PARTITION marker (0x01), so the walk \
         re-entered the header arm at a marker rather than at a partition start"
    } else if V5CompressedLegacyParser::is_range_tombstone_marker(leading) {
        " — NOTE: this leading byte carries IS_MARKER, i.e. it is a RANGE-TOMBSTONE \
         marker and not a partition header. Since #3928 round 5 the row loop's marker \
         failures can no longer leave the cursor here (they return the marker's own error \
         at every extent), so this indicates a partition BOUNDARY whose first byte is a \
         marker — malformed input, not a fall-through"
    } else {
        ""
    }
}
