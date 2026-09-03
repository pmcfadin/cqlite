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

use super::super::*;

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
    /// The header is incomplete AND this is the final chunk: the walk is over
    /// (trailing bytes shorter than a header, which is how a truncated tail
    /// presents). Preserved verbatim from both drivers' pre-#3928 behaviour —
    /// this issue changed the MALFORMED and UNPARSEABLE answers, not this one.
    Done,
}

impl V5CompressedLegacyParser {
    /// Classify and (on success) parse the partition header at the front of
    /// `data`.
    ///
    /// `at_final_chunk` decides tolerance and nothing else does: the parse
    /// cannot know whether more bytes can arrive, so the driver — which owns the
    /// sliding window — states it.
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
        match self.partition_header_readiness(data) {
            // `Malformed` is decidable from TWO bytes (a zero or over-long
            // declared key length), so no additional byte can rescue it — and at
            // the final chunk none can arrive at all.
            PartitionHeaderReadiness::Malformed => {
                if at_final_chunk {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: malformed partition header at the FINAL chunk \
                         (leading bytes {:02x?}, {} byte(s) in window): the declared partition \
                         key length is zero or over-long and no further bytes can arrive to \
                         change that. Skipping a byte to resynchronise would DROP this \
                         partition and can land on misaligned bytes that parse as a plausible \
                         header, INVENTING a partition that does not exist (issue #3928)",
                        &data[..data.len().min(2)],
                        data.len()
                    )));
                }
                Ok(DriverHeader::Resync)
            }
            PartitionHeaderReadiness::Incomplete => Ok(if at_final_chunk {
                DriverHeader::Done
            } else {
                DriverHeader::NeedMore
            }),
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
