//! The arm-independent ROW SOURCE seam for the read-path drive loop (issue
//! #3058).
//!
//! `MergeProducer`'s row loop (`producer_stream::drive_row_source`) owns
//! batching, the `max_batch_bytes` budget, `CancelFlag` polling, `ScanProgress`
//! accounting, the token filter, and the predicate/projection application. Two
//! structurally different sources feed it:
//!
//! * the k-way merge (`StreamingMerger` → `RowStepper`), used whenever ≥2
//!   sources must be reconciled, and
//! * the single-generation scan (`crate::bypass::ScanRowSource`), used when the
//!   fail-closed bypass predicate selects it.
//!
//! Both are expressed as a [`RowSource`] yielding [`SourceStep`]s so the loop
//! exists in exactly ONE place — duplicating it is how the two arms would drift
//! apart on batching, cancellation or LIMIT semantics.
//!
//! Lives in its own module so `producer_stream.rs` stays under the campsite
//! file-size target (epic #1116).

use cqlite_core::query::{build_row_from_scan_cached, PartitionKeyCache, QueryRow};
use cqlite_core::storage::write_engine::merge::{MergeEntry, StreamingStep};
use cqlite_core::storage::write_engine::DecoratedKey;
use cqlite_core::types::ScanRow;
use cqlite_core::RowKey;

use crate::producer::{MergeProducer, ProducerError};
use crate::producer_stream::RowStepper;

/// One increment from a row SOURCE — the arm-independent shape of the drive
/// loop's input (issue #3058).
///
/// The k-way merge arm and the single-source scan arm produce structurally
/// different rows, but the loop around them (token filter, partition
/// accounting, predicate, projection, `batch_size`/`max_batch_bytes` batching,
/// `LIMIT`, cancellation, sub-phase timing) is IDENTICAL and must stay defined
/// in exactly one place — duplicating it is how the two arms would drift.
pub(crate) enum SourceStep {
    /// A row increment: the partition it belongs to, plus a row that has NOT
    /// been materialized yet (see [`PendingRow`]).
    Row(DecoratedKey, PendingRow),
    /// A partition boundary carrying no row (an empty/all-purged partition that
    /// still counts as scanned on the merge arm).
    ///
    /// KNOWN ACCOUNTING DIFFERENCE between the arms (issue #3058, roborev;
    /// tracked for closure by #3098). Only the MERGE source emits this: the k-way
    /// merger surfaces `StreamingStep::PartitionEnd` for a partition whose rows
    /// were all suppressed (a partition deletion, a range tombstone, expired
    /// TTLs), so `drive_row_source` calls `meter.record_partition()` for it. The
    /// single-generation scan source CANNOT: the walk emits only SURVIVING rows,
    /// so a fully-suppressed partition produces no message at all and the source
    /// never learns it existed — surfacing it would need a new
    /// partition-boundary signal threaded out of two core walks.
    ///
    /// Consequence, stated rather than left silent: `ScanProgress`'s
    /// `partitions_scanned` can be LOWER on the fast arm than on the merge arm by
    /// exactly the number of fully-suppressed partitions. `rows_scanned` is
    /// UNAFFECTED (it counts materialized rows, and a suppressed partition
    /// materializes none) and no emitted row, value or order differs — this is a
    /// progress-counter difference only. Pinned by
    /// `bypass::tests::progress_accounting_difference_between_the_arms_is_the_documented_one`.
    PartitionEnd(DecoratedKey),
    /// The source is exhausted.
    Complete,
}

/// A row that is materialized ONLY after the token filter admits its partition
/// (issue #3058).
///
/// Deliberately lazy: `drive_row_source` evaluates the token filter BEFORE
/// materializing, exactly as the pre-#3058 loop did, so a token-excluded
/// partition costs no row construction (and a decode error inside one cannot
/// surface for a partition the split does not own).
pub(crate) enum PendingRow {
    /// A reconciled k-way merge entry (multi-source arm).
    Merged(Box<MergeEntry>),
    /// A single-generation, read-shadowed scan row (single-source fast arm).
    Scanned(RowKey, ScanRow),
}

/// Abstraction over the row SOURCE the drive loop pulls from (issue #3058).
pub(crate) trait RowSource {
    /// Advance the source by one increment (or report completion).
    fn next_step(&mut self) -> Result<SourceStep, cqlite_core::Error>;
}

/// Adapts the k-way merge [`RowStepper`] to the arm-independent [`RowSource`],
/// so the merge arm keeps its existing (test-doubled) stepper seam while the
/// loop itself is shared.
pub(crate) struct MergeRowSource<'a> {
    stepper: &'a mut dyn RowStepper,
}

impl<'a> MergeRowSource<'a> {
    pub(crate) fn new(stepper: &'a mut dyn RowStepper) -> Self {
        Self { stepper }
    }
}

impl RowSource for MergeRowSource<'_> {
    fn next_step(&mut self) -> Result<SourceStep, cqlite_core::Error> {
        Ok(match self.stepper.step_row()? {
            StreamingStep::ClusterGroup { key, row } => {
                SourceStep::Row(key, PendingRow::Merged(row))
            }
            StreamingStep::PartitionEnd { key } => SourceStep::PartitionEnd(key),
            StreamingStep::Complete => SourceStep::Complete,
        })
    }
}

impl MergeProducer {
    /// Materialize one [`PendingRow`] into a [`QueryRow`], or `None` when the
    /// row must be suppressed from output (issue #3058).
    ///
    /// The two arms suppress the same shapes through different mechanisms, and
    /// that equivalence is proven by the forced-path differential, not asserted
    /// here:
    /// * MERGE arm — `entry_to_row` drops `RowData::Tombstone` and applies the
    ///   `has_live_data_cell` / row-marker liveness visibility rule
    ///   (#2374/#2789) against the request's `now_secs`.
    /// * SCAN arm — the single-generation decoder has ALREADY applied read
    ///   shadowing (partition/range/row/cell tombstones + TTL expiry at the
    ///   request's pinned `now`); a row tombstone reaches here as
    ///   `ScanRow::Marker` and `build_row_from_scan_cached` suppresses it via
    ///   `row.into_cells()?` (issue #505). Collections already arrive COLLAPSED
    ///   in the single-generation shape `assemble_read_cells` mirrors, so no
    ///   reassembly is needed (and `assemble_cols` is therefore not consulted:
    ///   the projection is applied downstream by the Arrow encode over
    ///   `self.columns`, exactly as it is for the merge arm, which also builds
    ///   its row with an EMPTY projection).
    pub(crate) fn materialize_pending(
        &self,
        key: &DecoratedKey,
        pending: PendingRow,
        pk_cache: &mut PartitionKeyCache,
        assemble_cols: Option<&std::collections::HashSet<String>>,
    ) -> Result<Option<QueryRow>, ProducerError> {
        match pending {
            PendingRow::Merged(entry) => {
                self.entry_to_row(&key.key, *entry, pk_cache, assemble_cols, self.now_secs)
            }
            PendingRow::Scanned(row_key, scan_row) => Ok(build_row_from_scan_cached(
                row_key,
                scan_row,
                &[],
                Some(&self.schema),
                pk_cache,
            )),
        }
    }
}
