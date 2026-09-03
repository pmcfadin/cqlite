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
                // Issue #3928: the error's own kind is preserved and returned on
                // a proven-complete buffer — the same rule, and the same reason,
                // as the row arm's `return Err(e)` (#3782).
                //
                // This arm composes NO message: `e` is `scan_partition_header`'s
                // own, and under the validation above it can realistically only
                // be the `oa`/`da` invalid-IS_LIVE-byte rejection, which does
                // not rest on the key-length model. Its two key-length messages
                // are already excluded here (a zero or over-long length, and a
                // key running past the buffer, are what the validation just
                // checked), and rewording THEM belongs to **#3999** with the
                // model they assert — see `undecodable_partition_header`.
                if refuse {
                    return Err(e);
                }
                tracing::warn!(
                    "V5CompressedLegacy: Failed to parse partition header at offset {offset} \
                     (partition={partition_index}): {e}. Resynchronising because {}.",
                    tolerance.why_tolerant()
                );
                Ok(HeaderStep::Resync)
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

/// Issue #3928 fix round 1, I2 — name the leading byte's STRUCTURAL class when it
/// is one this arm should never have been handed.
///
/// The row loop breaks with `offset` still ON a range-tombstone marker it could
/// not parse or feed (`block_emit_windowed.rs`'s three marker breaks), and the
/// outer partition loop then re-enters HERE. Without this the diagnostic reads
/// "undecodable partition header" for a MARKER failure and sends the next reader
/// to the wrong arm.
///
/// Diagnostics only: the refusal itself is unchanged, and moving the marker
/// breaks to report their own failure is a different arm and a different issue.
fn structural_note_at(data: &[u8], offset: usize) -> &'static str {
    let Some(&leading) = data.get(offset) else {
        return "";
    };
    if V5CompressedLegacyParser::is_end_of_partition(leading) {
        " — NOTE: this leading byte is an END_OF_PARTITION marker (0x01), so the walk \
         re-entered the header arm at a marker rather than at a partition start"
    } else if V5CompressedLegacyParser::is_range_tombstone_marker(leading) {
        " — NOTE: this leading byte carries IS_MARKER, i.e. it is a RANGE-TOMBSTONE \
         marker and not a partition header. The previous partition's row loop ended at a \
         marker it could not parse or feed, leaving the cursor on it; the failure to \
         investigate is in the MARKER arm, not here"
    } else {
        ""
    }
}
