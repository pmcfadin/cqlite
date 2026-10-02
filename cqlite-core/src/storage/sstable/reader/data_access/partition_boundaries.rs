//! Partition-BOUNDARY enumeration over `Data.db` (issue #1103; rewritten
//! streaming + boundary-keyed for issue #4197 roborev job 124).
//!
//! One job: report, in on-disk order, where every partition STARTS in the
//! decompressed data section and which raw partition key its header carries —
//! the `(data_position, raw_key)` domain `Index.db` and the BTI
//! `Partitions.db` trie encode. Two consumers:
//!
//! * the verifier's BTI identity cross-check (`verify.rs`), which resolves a
//!   trie leaf's payload back to a raw key through this map, and
//! * `cqlite rebuild` (`write_engine::rebuild::boundaries`), which has no
//!   authoritative boundary source to trust at all — regenerating `Index.db`
//!   is the whole point of the tool.
//!
//! # Two properties this module exists to hold (roborev job 124)
//!
//! 1. **Bounded memory.** The walk rides the EXISTING streaming compaction
//!    driver ([`SSTableReader::stream_all_partitions_for_compaction`]) instead
//!    of `stitch_all_chunks`, so the decompressed data section is NEVER fully
//!    resident: peak is one chunk + one in-flight structure (the driver
//!    advances its window cursor per ROW — issue #2299), plus the returned
//!    boundary vector itself. The pre-#4197 implementation materialised the
//!    whole section into one `Vec<u8>` first, which made
//!    `rebuild_components`'s peak heap O(file size) while spec R6 claimed
//!    O(one partition).
//! 2. **Boundary-keyed, never key-keyed.** Each enumerated entry is one
//!    on-disk partition HEADER. The pre-#4197 implementation deduped by raw
//!    partition key (`HashSet<Vec<u8>>`), so a `Data.db` carrying the same key
//!    at two different offsets — the corruption Cassandra's `Verifier` reports
//!    as "Key out of order" for a non-increasing `(token, key)` step — was
//!    silently UNDER-enumerated rather than refused. That is exactly the
//!    hazard `partition_verify_scan` already documents fixing under issue
//!    #1282; this module keys on the boundary for the same reason. The
//!    duplicate is surfaced to the caller, which decides: `rebuild` refuses
//!    (`RefusalReason::DataCorrupt`), the verifier's multiset compare reports
//!    the mismatch.
//!
//! # What the boundary signal IS
//!
//! Not a byte-pattern search (issue #28, spec R2.3): the streaming driver
//! tells us. [`CompactionPartitionState::header_parsed`] flips `false -> true`
//! exactly when `stream_partition_body_incremental` has decoded one partition
//! HEADER, and that header began at the window-front offset recorded before
//! the call. That offset is the same `partition_start` the buffered
//! `parse_block_for_compaction_emit_with_offset` reports, so both paths define
//! the boundary in one place.
//!
//! One deliberate behavioural delta from the buffered predecessor: it recorded
//! a partition only once a ROW of it was emitted, so a partition consisting of
//! a header plus an END_OF_PARTITION marker and nothing else was not
//! enumerated at all. A header on disk IS a partition (Cassandra's `Index.db`
//! carries an entry for it), so it is now enumerated. Consumers already handle
//! a partition that reconciles to no rows — `rebuild`'s pass 2 skips it
//! (`decode_one_partition` → `Ok(None)`), which is why its key-range
//! population is drawn from the pass-2 call site alone (issue #4197 F6).

use super::super::SSTableReader;
use crate::storage::sstable::reader::parsing::CompactionPartitionState;
use crate::storage::sstable::reader::window_cursor::WindowCursor;
use crate::types::RowKey;
use crate::Result;

/// The partition-boundary observer threaded through the streaming compaction
/// driver (`data_access::compaction`), plus the driver's running LOGICAL offset
/// bookkeeping.
///
/// `front` is the absolute offset, in the DECOMPRESSED data section, of the
/// driver window's front byte — i.e. the total number of bytes the driver has
/// consumed so far. It is maintained whether or not a `sink` is installed (the
/// cost is one add per consumed structure), because the whole point is that the
/// offset is derived from the driver's own confirmed consumption rather than
/// re-derived by a second walk.
///
/// `sink` is `None` for an ordinary compaction stream (the k-way merge producer
/// wants rows, not boundaries) and `Some` only for the boundary walk below.
pub(crate) struct PartitionBoundaryObserver<'a> {
    front: u64,
    #[allow(clippy::type_complexity)]
    sink: Option<&'a mut (dyn FnMut(u64, &RowKey) -> Result<()> + Send + 'a)>,
}

impl<'a> PartitionBoundaryObserver<'a> {
    /// An observer that tracks the logical front offset but reports no
    /// boundaries — the ordinary compaction-stream case.
    pub(crate) fn inactive() -> Self {
        Self {
            front: 0,
            sink: None,
        }
    }

    /// An observer that reports every partition boundary to `sink` as
    /// `(data_position, raw_partition_key)`.
    pub(crate) fn reporting(
        sink: &'a mut (dyn FnMut(u64, &RowKey) -> Result<()> + Send + 'a),
    ) -> Self {
        Self {
            front: 0,
            sink: Some(sink),
        }
    }

    /// Whether a caller is actually collecting boundaries. The driver consults
    /// this to route a boundary walk AWAY from the index-walk fallback, which
    /// reports no decompressed-data-section offsets at all (see
    /// [`SSTableReader::stream_all_partitions_for_compaction_observed`]'s
    /// non-stitching branch) rather than report fabricated ones (issue #28).
    pub(crate) fn is_reporting(&self) -> bool {
        self.sink.is_some()
    }

    /// The logical offset of the driver window's front byte — the start offset
    /// of whatever structure the driver is about to parse.
    pub(crate) fn front(&self) -> u64 {
        self.front
    }

    /// Consume `n` bytes from the driver's window, advancing the logical front
    /// by the number of bytes the window ACTUALLY consumed.
    ///
    /// Reads the clamped amount back out of the window rather than trusting
    /// `n`: [`WindowCursor::consume`] clamps to the remaining length, so a
    /// decoder reporting `consumed > remaining` must not be allowed to push the
    /// logical offset past the bytes the driver really walked (every later
    /// boundary would then be reported at a wrong absolute offset).
    pub(crate) fn consume(&mut self, window: &mut WindowCursor, n: usize) {
        let before = window.len();
        window.consume(n);
        self.front += (before - window.len()) as u64;
    }

    /// Report one confirmed partition boundary, if a caller is collecting.
    pub(crate) fn note_partition_start(&mut self, start: u64, key: &RowKey) -> Result<()> {
        if let Some(sink) = self.sink.as_mut() {
            sink(start, key)?;
        }
        Ok(())
    }

    /// The `false -> true` header-parsed transition check the driver runs after
    /// every `stream_partition_body_incremental` call: when `was_header_parsed`
    /// was `false` and the state now reports a parsed header, a partition
    /// started at `start` carrying `state`'s key.
    pub(crate) fn note_if_partition_started(
        &mut self,
        was_header_parsed: bool,
        start: u64,
        state: &CompactionPartitionState,
    ) -> Result<()> {
        if !was_header_parsed && state.header_parsed() {
            self.note_partition_start(start, state.partition_key())?;
        }
        Ok(())
    }
}

impl SSTableReader {
    /// Return every on-disk partition's `(data_position, raw_partition_key)`
    /// in on-disk order, where `data_position` is the partition's start offset
    /// in the DECOMPRESSED data section (issue #1103).
    ///
    /// `data_position` is exactly the value a BTI `Partitions.db` leaf encodes
    /// as
    /// [`BtiPartitionLocation::DataOffset`](crate::storage::sstable::bti::parser::BtiPartitionLocation::DataOffset)
    /// (and the `data_position` recovered from a `RowsOffset` entry via
    /// [`resolve_rows_db_entry`](crate::storage::sstable::bti::parser::resolve_rows_db_entry)),
    /// and the offset `Index.db` records for the partition. The verifier's BTI
    /// cross-check resolves each leaf PAYLOAD back to its raw partition key
    /// through this map, so it catches a corruption that keeps the emitted trie
    /// prefix but rewrites the payload to point at a DIFFERENT partition.
    ///
    /// # One entry per BOUNDARY, not per distinct key
    ///
    /// The returned keys are NOT deduplicated: a `Data.db` in which the same
    /// partition key appears at two different offsets yields two entries, in
    /// on-disk order. That is a corruption (Cassandra's `Verifier` flags a
    /// non-increasing `(token, key)` step), and the caller is the right place
    /// to classify it — `rebuild` refuses with
    /// `RefusalReason::DataCorrupt`, the verifier's multiset compare reports
    /// the mismatch. Deduplicating here, as this function did before issue
    /// #4197's roborev round, silently DROPPED the second occurrence and
    /// under-enumerated the file (the same hazard
    /// [`SSTableReader::partition_verify_scan`] documents fixing under issue
    /// #1282).
    ///
    /// # Memory
    ///
    /// Streaming: bounded by one chunk + one in-flight structure + the
    /// returned vector, never by the size of the data section (spec R6). See
    /// the module doc.
    ///
    /// `schema`, when supplied, takes priority over the reader's own
    /// header-derived resolution (`get_table_schema`'s "Strategy 0"; issue
    /// #4197). This matters whenever the reader's usual fallbacks cannot
    /// resolve one on their own — e.g. `cqlite rebuild` walking boundaries
    /// while the sibling `Statistics.db` (the source `get_table_schema`'s
    /// header-derived Strategy 1 reads from at OPEN time) is itself the
    /// component being regenerated, or is temporarily relocated for repair-
    /// field recovery (spec R4.2). Every pre-existing caller passes `None`
    /// and is unaffected.
    pub async fn distinct_partition_keys_with_positions(
        &self,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut result: Vec<(u64, Vec<u8>)> = Vec::new();
        {
            let mut sink = |start: u64, key: &RowKey| -> Result<()> {
                result.push((start, key.as_bytes().to_vec()));
                Ok(())
            };
            // The rows themselves are irrelevant here — each one is dropped as
            // soon as the driver hands it over, which is what keeps the walk's
            // peak bounded by a single structure. Decoding them is not wasted
            // work that could be skipped: it is how the driver establishes
            // where the NEXT structure (and so the next partition header)
            // begins, and it is the same decode that proves `Data.db` is
            // structurally sound (rebuild spec R5).
            self.stream_all_partitions_for_compaction_observed(
                schema,
                &self.scan_cancel,
                PartitionBoundaryObserver::reporting(&mut sink),
                |_row| Ok(std::ops::ControlFlow::Continue(())),
            )
            .await?;
        }
        Ok(result)
    }
}
