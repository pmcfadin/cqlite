//! Issue #3928 — the ONE partition-HEADER arm of the two SLIDING drivers.
//!
//! `drive_partition_sliding` (buffered, one whole partition per call) and
//! `stream_partition_body_incremental` (row-granular, one structure per call)
//! had this arm spelled out twice, verbatim: the same
//! [`PartitionHeaderReadiness`] classification, the same re-classification after
//! a parse failure, and the same byte resync — each expressed in its own advance
//! vocabulary (`ParseStep::Emitted(1)` / `PartitionStreamStep::Consumed(1)`,
//! which are the SAME resync wearing two names).
//!
//! What must not diverge between them is the DECISION, so the decision lives
//! here and each driver only translates the answer into its own vocabulary. The
//! block-emit walk deliberately keeps its own arm
//! (`block_emit_windowed/partition_header_arm.rs`): its discriminator is the
//! caller-stated [`BufferExtent`], not `at_final_chunk`, and it has a partition
//! index to report.
//!
//! # Why the tolerant answer was unsound at the final chunk
//!
//! Both arms tolerated unconditionally. At the final chunk no further bytes can
//! arrive, so a header that does not classify or does not parse is truncation or
//! corruption — and the one-byte resync then has two effects, the second worse
//! than the first: the partition is silently DROPPED, and the resync can land on
//! MISALIGNED bytes that parse as a plausible header, INVENTING a partition that
//! does not exist.
//!
//! Measured on a real Cassandra 5.0 `da` fixture with ONE compressed byte
//! flipped so the first partition's `DeletionTime` discriminator reads `0xFF` —
//! a value Cassandra's own `DeletionTime.Serializer.deserialize` throws on
//! (cassandra-5.0.8 `DeletionTime.java:222-230`) — both compaction surfaces
//! answered `Ok` with **404 of 468** rows and both partition-key surfaces
//! answered `Ok` with **5** keys where the fixture has **3**: 64 rows lost and 2
//! partitions invented, reported as success. Compaction would write that back to
//! disk. The count moves in BOTH directions, so no count-based check and no
//! total-comparing parity oracle can see it.
//!
//! `at_final_chunk` is the authoritative discriminator, exactly as it is for the
//! row arm #3782 fixed: it is a property of the sliding window, not a guess
//! about the bytes. Mid-stream the SAME conditions stay tolerant, because a
//! window the driver can still advance out of may legitimately be positioned
//! mid-structure.

use super::super::buffer_extent::HeaderTolerance;
use super::super::*;

/// Issue #3999 — every refusal derived from CQLite's partition-key-LENGTH model
/// must carry this, because that model is known to differ from Cassandra's for
/// keys of 256 bytes or more.
///
/// Cassandra writes the Data.db partition key as
/// `ByteBufferUtil.writeWithShortLength(key.getKey(), writer)` inside
/// `SortedTablePartitionWriter.start` (cassandra-5.0.8
/// `SortedTablePartitionWriter.java:97-105`), i.e. `out.writeShort(length)` — an
/// unsigned 16-bit BIG-ENDIAN length, with NO flags byte. CQLite reads byte 0 as
/// "flags" and byte 1 as a one-byte length, so the two models coincide only
/// while the high byte is `0x00`, i.e. for keys under 256 bytes. For a 256-byte
/// key Cassandra writes `0x0100`, CQLite reads a length of `0`, and a refusal
/// here would be CQLite's own model rather than corruption.
///
/// Correcting the model touches `partition_header_readiness`,
/// `scan_partition_header`, `peek_is_partition_header`,
/// `parse_partition_header_full` and `FORMAT_MAX_KEY_SIZE`, and needs a
/// 256-byte-key fixture the corpus does not have — a different family, tracked
/// as **#3999**. Until then the refusal STAYS (loud beats silent, and #3928's
/// whole subject is that the silent answer both dropped and invented
/// partitions) and it SAYS SO, so an operator who hits a long key is pointed at
/// the right issue instead of at their disk.
const KEY_LENGTH_MODEL_CAVEAT: &str = "NOTE (#3999): CQLite reads that length as ONE byte \
     after a 'flags' byte, while Cassandra writes an unsigned 2-byte big-endian length and no \
     flags byte (SortedTablePartitionWriter.start -> ByteBufferUtil.writeWithShortLength), so \
     the two models agree only for keys under 256 bytes; for a longer key this refusal may be \
     CQLite's model and not corruption.";

/// What a sliding driver should do with the bytes at its window front.
pub(in crate::storage::sstable::reader::parsing::row_decoder) enum DriverHeader {
    /// A structurally-valid header: the partition key, the offset of the first
    /// row/marker, and the partition-level deletion.
    Parsed(RowKey, usize, Option<(i64, i32)>),
    /// The header (or, for an `oa`/`da` deleted partition, its 12-byte
    /// `DeletionTime`) is split across the chunk boundary. Unreachable at the
    /// final chunk.
    NeedMore,
    /// The window front is not a decodable header and more bytes may still
    /// arrive, so the driver consumes ONE byte and re-tries. Unreachable at the
    /// final chunk.
    Resync,
    /// There is NOTHING here: an EMPTY buffer at the final chunk. The drivers
    /// translate it to `ParseStep::Done` / `PartitionStreamStep::AllDone`, i.e.
    /// clean completion of the walk.
    ///
    /// Reachable ONLY for an empty buffer since finding C1 — a non-empty
    /// incomplete header at the final chunk is a truncated partition and is
    /// refused. Both drivers early-return on `data.is_empty()` before calling
    /// this arm, so no caller reaches this variant today; it exists so the arm
    /// is total, because "there is nothing here" is not corruption.
    Done,
}

/// What CQLite READ as the partition key length, when there were enough bytes to
/// read one at all (issue #3928 / #3999).
///
/// With a single byte present NO length has been read, so the diagnostic must
/// not name one — asserting a length nobody read is the same class of false
/// statement #3999 is about, one field over.
fn read_length_note(data: &[u8]) -> String {
    match data.get(1) {
        Some(&low) => format!(" (CQLite READ a key length of {low} from byte 1)"),
        None => " (too short for CQLite to have read a key length at all)".to_string(),
    }
}

impl V5CompressedLegacyParser {
    /// Classify and (on success) parse the partition header at the front of
    /// `data`.
    ///
    /// `at_final_chunk` is the driver's authoritative "no further bytes can
    /// arrive" fact, and it is converted at this boundary into the same
    /// [`HeaderTolerance`] the block-emit arm consults, so both arms ask ONE
    /// question with one answer. A driver has no row-body bound, so its progress
    /// is always attributable: every call starts at a partition boundary the
    /// caller advanced to by a CONFIRMED consumed-byte count.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn driver_partition_header(
        &self,
        data: &[u8],
        at_final_chunk: bool,
    ) -> Result<DriverHeader> {
        // #1741 (roborev HIGH): size the header need-more decision correctly for
        // the oa/da DeletionTime form via the authoritative discriminator peek,
        // so a deleted header split across a NON-FINAL chunk returns `NeedMore`
        // instead of being mis-parsed and skipped (which desynced the scan and,
        // on compaction, dropped a partition tombstone).
        let tolerance = HeaderTolerance::for_final_chunk(at_final_chunk);
        match self.partition_header_readiness(data) {
            // `Malformed` is decidable from TWO bytes (a zero or over-long
            // declared key length), so no additional byte can rescue it — and at
            // the final chunk none can arrive at all.
            PartitionHeaderReadiness::Malformed => {
                if tolerance.refuses() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: refusing an unreadable partition header at the \
                         FINAL chunk (leading bytes {:02x?}, {} byte(s) in window): CQLite \
                         READ a partition key length of {} and rejects it as zero or \
                         over-long, and no further bytes can arrive to change what was read. \
                         Skipping a byte to resynchronise would DROP this partition and can \
                         land on misaligned bytes that parse as a plausible header, INVENTING \
                         a partition that does not exist (issue #3928). \
                         {KEY_LENGTH_MODEL_CAVEAT}",
                        &data[..data.len().min(2)],
                        data.len(),
                        usize::from(data[1])
                    )));
                }
                Ok(DriverHeader::Resync)
            }
            // `Incomplete` means the header could not be COMPLETED from the
            // bytes present — the declared key runs past the buffer, its
            // `DeletionTime` does, or there are fewer than the two bytes any
            // header needs.
            //
            // Mid-stream that is the ordinary straddle: more bytes can arrive,
            // so `NeedMore`. At the final chunk none can, and then EVERY
            // non-empty case is a header Cassandra wrote whose bytes were
            // truncated away. `DriverHeader::Done` is read by BOTH drivers as
            // CLEAN COMPLETION (`ParseStep::Done` / `PartitionStreamStep::AllDone`),
            // so it is truthful only where there is genuinely NOTHING here.
            //
            // Issue #3928 round 1 kept the one-byte case tolerant, reasoning that
            // fewer than two bytes cannot be a partition. Finding C1: that byte
            // can be the surviving FIRST byte of a truncated partition-key
            // length, so the partition IS lost — and `block_emit`'s walk already
            // refused the identical bytes, leaving the driver path and the
            // block-emit path disagreeing about one file. Measured pre-fix: the
            // driver answered `Done` over ONE byte of a header declaring a
            // 16-byte key while both block walks answered `Err`.
            //
            // So the byte COUNT decides nothing (it never did — see
            // `HeaderTolerance`); emptiness does. And an empty buffer cannot
            // reach here from either driver today: both early-return on
            // `data.is_empty()` before this call. The arm is kept total anyway,
            // because a function that refuses "there is nothing here" as
            // corruption would be wrong in isolation, and this one is
            // `pub(in row_decoder)`.
            //
            // No caller legitimately leaves a non-empty stub at the final chunk:
            // every `Emitted`/`PartitionDone` consumed-count includes the
            // structural terminator and the callers advance by exactly it
            // (`scan_stream_windowed.rs`'s `window.consume(take)`,
            // `drain_compaction_window`), and the AC3 counters recorded ZERO
            // arrivals here at the final chunk across 126 tables / 148 SSTables
            // on four surfaces.
            PartitionHeaderReadiness::Incomplete => {
                if !tolerance.refuses() {
                    return Ok(DriverHeader::NeedMore);
                }
                if data.is_empty() {
                    return Ok(DriverHeader::Done);
                }
                Err(Error::corruption(format!(
                    "V5CompressedLegacy: refusing a TRUNCATED partition header at the FINAL \
                     chunk: {} byte(s) are present{}, too few to complete the header, and no \
                     further bytes can arrive. The partition Cassandra wrote here was \
                     truncated away, so reporting completion would DROP it and answer Ok \
                     with one fewer partition than the file holds (issue #3928). \
                     {KEY_LENGTH_MODEL_CAVEAT}",
                    data.len(),
                    read_length_note(data)
                )))
            }
            PartitionHeaderReadiness::Ready => match self.parse_partition_header_full(data, 0) {
                Ok((key, after_header, deletion)) => {
                    Ok(DriverHeader::Parsed(key, after_header, deletion))
                }
                // Defense-in-depth: `Ready` guarantees the DeletionTime is fully
                // present, so a parse failure here cannot be truncation. On a
                // non-final chunk only re-request bytes if the header is somehow
                // still incomplete (a `NeedMore` on a complete buffer would loop
                // for ever).
                //
                // Issue #3928: the error is REPORTED, with its own kind
                // preserved, exactly as the row arm reports its own (#3782) —
                // and here it is reported REGARDLESS of `at_final_chunk`, which
                // is the one place this issue's rule is STRONGER than #3782's.
                //
                // Why that is sound rather than over-eager: `Ready` is an
                // AFFIRMATIVE guarantee from `partition_header_readiness` that
                // every header byte — the key length, the key, and the full
                // `DeletionTime` for its live/deleted form (the discriminator is
                // PEEKED to size it, #1741) — is already present. So a failure
                // here "is a genuine STRUCTURAL rejection ... never truncation"
                // (that classifier's own contract, quoted from
                // `row_framing.rs`), and no additional byte can change the
                // verdict. AC1's tolerance carve-out is for a header that "can
                // legitimately straddle" the window; a `Ready` header provably
                // cannot.
                //
                // Measured: on a real `da` fixture whose FIRST partition's
                // `DeletionTime` discriminator was flipped to a value
                // Cassandra's own reader throws on, that header sits in chunk 0
                // of 5 — so `at_final_chunk` is FALSE there —  and
                // `stream_all_partitions_for_compaction` answered `Ok` with 404
                // of 468 rows. An `at_final_chunk`-gated refusal would have left
                // that silent loss in place on the surface compaction WRITES
                // BACK.
                //
                // The `Incomplete` re-probe is kept as defence in depth: it can
                // only fire if the classifier disagrees with itself between the
                // two calls, and a `NeedMore` is the conservative answer there.
                Err(e) => {
                    if !at_final_chunk
                        && self.partition_header_readiness(data)
                            == PartitionHeaderReadiness::Incomplete
                    {
                        return Ok(DriverHeader::NeedMore);
                    }
                    Err(e)
                }
            },
        }
    }
}
