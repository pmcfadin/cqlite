//! SSTable maintenance and STCS compaction for the write engine.
//!
//! Extracted verbatim from `write_engine/mod.rs` (issue #1120, epic #1116) as a
//! behavior-preserving split. Owns the incremental K-way merge state machine
//! (`maintenance_step`), candidate scanning, startup orphan sweeps, atomic
//! input deletion, and the public `MaintenanceReport` type. `WriteEngine`'s
//! fields are reachable here because this is a sibling module in the same crate.

use super::merge;
use super::mutation::{DecoratedKey, PartitionTombstone, RangeTombstone};
// `Mutation` itself is no longer named directly outside tests (issue #1668
// stage 5c-iv part 3 removed the `Vec<Mutation>` accumulator this file used
// to build) — only `mod tests` (`use super::*`) still constructs one
// directly, so this import is test-only to avoid an unused-import error in
// the non-test build.
#[cfg(test)]
use super::mutation::Mutation;
use super::{CompactionStats, KWayMerger, MergePolicy, WriteEngine};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::writer::data_writer::{StaticOpsTracker, StreamingPartitionSession};
use crate::storage::sstable::writer::stats_fold;
use crate::storage::sstable::writer::StatisticsMetadata;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Per-partition streaming state (issue #1668, stage 5c-iv part 3) — mirrors
/// `KWayMerger::merge()`'s per-partition state machine (stage 5c-iv part 2):
/// a bounded `clustering_key: None` prefix (partition/range-tombstone
/// carriers, static-row carrier) resolves first; a
/// [`StreamingPartitionSession`] opens only once the first `Some(ck)` row (or
/// an unclustered table's sole row) arrives, since only then do we have
/// everything `begin_streaming_partition` needs upfront (the partition
/// tombstone and range-tombstone list). Because `StreamingPartitionSession`
/// owns everything it needs (no lifetime — issue #1668 stage 5c-iv part 3's
/// whole reason for existing over `IncrementalPartitionWriter`), EITHER
/// variant can be stashed on [`ActiveMerge::pending_partition`] and survive
/// returning to the maintenance scheduler between `maintenance_step_inner`
/// calls: resuming is just "keep calling methods on it," never a
/// capture/reconstitute conversion.
pub(crate) enum PartitionStreamState {
    /// Still resolving the bounded prefix — no on-disk partition exists yet
    /// (no header has been written), so there is nothing to pause beyond
    /// these plain owned accumulators.
    Prefix {
        partition_tombstone: Option<PartitionTombstone>,
        range_tombstones: Vec<RangeTombstone>,
        static_tracker: StaticOpsTracker,
        static_first_ts: i64,
        saw_carrier_or_static: bool,
        /// This partition's stats fold accumulated so far — see the
        /// `Streaming` variant's field of the same name for why this is
        /// threaded through the state rather than folded straight into the
        /// writer's own `stats`.
        partition_stats: StatisticsMetadata,
        /// Mirrors `KWayMerger::merge()`'s loop-local `row_count`: one static
        /// carrier mutation increments this by 1 EACH time (not once per
        /// merged output row — issue #1668 stage 5c-iv part 2's row-count
        /// parity finding), carried forward into `Streaming::row_count` once
        /// the first `Some(ck)` row opens the session.
        row_count: u64,
    },
    /// A real on-disk partition is open (header + any rows fed so far are
    /// already in the writer's buffer) and must be finished — via
    /// `finish_streaming_partition` + `complete_partition_incremental` —
    /// before anything else can happen to this partition.
    Streaming {
        session: StreamingPartitionSession,
        /// This partition's stats fold accumulated so far (mirrors
        /// `KWayMerger::merge()`'s `partition_stats` — the writer's `stats`
        /// field cannot be folded into directly while a session that
        /// borrows `self.data_writer` per-call is open across calls, so it
        /// is threaded alongside the session instead).
        partition_stats: StatisticsMetadata,
        row_count: u64,
        partition_tombstone: Option<PartitionTombstone>,
    },
}

impl PartitionStreamState {
    /// A fresh, empty prefix accumulator — the starting state for any new
    /// partition (never resumed from a pause).
    fn fresh() -> Self {
        PartitionStreamState::Prefix {
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            static_tracker: StaticOpsTracker::new(),
            static_first_ts: 0,
            saw_carrier_or_static: false,
            partition_stats: StatisticsMetadata::new(),
            row_count: 0,
        }
    }

    /// This partition's stats-fold accumulator, regardless of which variant
    /// is currently active — every mutation (carrier, static, or clustering
    /// row) folds through the SAME chokepoint before classification, mirroring
    /// `KWayMerger::merge()`'s single fold point (issue #1668 stage 5c-iv
    /// part 2).
    fn partition_stats_mut(&mut self) -> &mut StatisticsMetadata {
        match self {
            PartitionStreamState::Prefix {
                partition_stats, ..
            } => partition_stats,
            PartitionStreamState::Streaming {
                partition_stats, ..
            } => partition_stats,
        }
    }
}

// `StaticOpsTracker`/`StreamingPartitionSession` do not implement `Debug`
// (the former holds a non-`Debug` cell-value map; the latter is a plain
// bookkeeping struct never printed in production), so `ActiveMerge`'s
// `#[derive(Debug)]` (used only incidentally — no code actually formats an
// `ActiveMerge`, confirmed by grep) needs a hand-written `Debug` for this
// enum rather than pulling `Debug` onto either of those types just to
// satisfy a derive nothing exercises.
impl std::fmt::Debug for PartitionStreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionStreamState::Prefix {
                saw_carrier_or_static,
                ..
            } => f
                .debug_struct("PartitionStreamState::Prefix")
                .field("saw_carrier_or_static", saw_carrier_or_static)
                .finish_non_exhaustive(),
            PartitionStreamState::Streaming { row_count, .. } => f
                .debug_struct("PartitionStreamState::Streaming")
                .field("row_count", row_count)
                .finish_non_exhaustive(),
        }
    }
}

/// Maintenance report from a maintenance_step() call (M5.2, Issue #384)
#[derive(Debug, Clone)]
pub struct MaintenanceReport {
    /// Time spent in this maintenance step
    pub time_spent: Duration,
    /// Completed merge output files (if any merge completed)
    pub completed_merges: Vec<PathBuf>,
    /// Number of rows merged in this step
    pub rows_merged: u64,
    /// Number of bytes written in this step
    pub bytes_written: u64,
    /// Whether there is pending compaction work
    pub pending_compaction: bool,
    /// SSTables DROPPED WHOLE by the fully-expired fast path in the merge that
    /// completed this step (issue #1388), distinct from the merged inputs: each was
    /// proven fully expired by authoritative `Statistics.db` metadata and
    /// overlap-safe, so it was excluded from the K-way merger (never read/decoded)
    /// and its components were reclaimed after the merged output published. Empty
    /// when nothing was dropped. Paths are input Data.db paths.
    pub dropped_whole: Vec<PathBuf>,
}

/// Active merge state for incremental compaction (M5.2, Issue #384)
#[derive(Debug)]
pub(crate) struct ActiveMerge {
    /// K-way merger performing the compaction
    pub(crate) merger: KWayMerger,
    /// Output SSTable writer (writes to `tmp_dir/keyspace/table/`)
    pub(crate) writer: crate::storage::sstable::writer::SSTableWriter,
    /// Input SSTable paths being merged (these remain intact until atomic rename succeeds)
    pub(crate) input_paths: Vec<PathBuf>,
    /// Root of the temporary directory tree used for this compaction output.
    ///
    /// The SSTableWriter appends `keyspace/table/` to this path, so component
    /// files land at `tmp_dir/keyspace/table/nb-{gen}-big-*.{ext}`.
    ///
    /// After `writer.finish()` the files are atomically renamed to the final
    /// SSTable directory. Only then are the inputs deleted.
    ///
    /// Invariant: if the process crashes before the renames complete, `tmp_dir`
    /// may contain partial output but the input SSTables remain intact.
    pub(crate) tmp_dir: PathBuf,
    /// Final SSTable directory (`data_dir/keyspace/table/`)
    ///
    /// Stored here so `finalize_merge_async` doesn't have to recompute it.
    pub(crate) sstable_dir: PathBuf,
    /// Number of rows merged so far (updated per partition)
    pub(crate) rows_merged: u64,
    /// Total bytes read from input SSTables (approximate: sum of Data.db file sizes)
    pub(crate) bytes_read: u64,
    /// When this merge started
    pub(crate) started_at: Instant,
    /// Effective compaction schema (#850): the configured schema augmented with
    /// any static columns that appear in the input SSTables' SerializationHeaders
    /// but were dropped from the current schema. Used to convert merged entries to
    /// mutations so the writer still emits the static-row prelude (static-column
    /// presence is read from the input headers, not the current schema only).
    pub(crate) effective_schema: TableSchema,
    /// SSTables DROPPED WHOLE for this compaction (issue #1388): proven fully
    /// expired by authoritative `Statistics.db` metadata and overlap-safe, EXCLUDED
    /// from `input_paths` (never read into the merger). Reclaimed in
    /// `finalize_merge_async` AFTER the merged output publishes, via the same
    /// component-delete path as the merged inputs, and surfaced in the
    /// `MaintenanceReport`. Empty when nothing was dropped.
    pub(crate) dropped_whole: Vec<PathBuf>,
    /// Mid-partition budget-check resumption state (issue #1668, stage 4 —
    /// the "Q4 unlock"). `Some((key, remaining_rows, mutations_so_far))` when
    /// a partition's cluster-group drain was PAUSED because the budget was
    /// exceeded before the whole partition could be converted+written;
    /// `None` when no partition is mid-drain (the common case — and the
    /// ONLY case before this stage, when only whole partitions were ever
    /// paused between, never within one).
    ///
    /// - `.0` the partition key (needed both to resume draining and for the
    ///   eventual `write_partition` call).
    /// - `.1` rows `KWayMerger::step()` ALREADY fully reconciled for this
    ///   partition but not yet POPPED off `StreamingMerger`'s queue
    ///   (extracted via `StreamingMerger::into_paused_state`, restored via
    ///   `StreamingMerger::resume`) — nothing here is re-computed by a
    ///   resumed call, and nothing is lost.
    /// - `.2` this partition's [`PartitionStreamState`] (issue #1668, stage
    ///   5c-iv part 3): either still resolving the bounded carrier/static
    ///   prefix, or an already-open `StreamingPartitionSession` with rows
    ///   fed so far. Fed incrementally as cluster groups arrive — the
    ///   writer no longer waits for the whole partition to accumulate as a
    ///   `Vec<Mutation>` before writing anything (that was stage 5c-iv part
    ///   2's change for `KWayMerger::merge`; this is the same change for the
    ///   background maintenance loop). Resuming a paused partition is just
    ///   "keep feeding this same value" — no re-computation, no lost rows,
    ///   and (unlike the pre-stage-5c-iv-part-3 `Vec<Mutation>` design) no
    ///   growing in-memory buffer for a wide partition either.
    pub(crate) pending_partition: Option<(
        DecoratedKey,
        VecDeque<merge::MergeEntry>,
        PartitionStreamState,
    )>,
}

impl WriteEngine {
    /// Set the merge policy for background compaction (M5.2, Issue #383)
    ///
    /// # Arguments
    ///
    /// * `policy` - Merge policy implementation (e.g., STCS, LCS, TWCS)
    pub fn set_merge_policy(&mut self, policy: Box<dyn MergePolicy>) -> Result<()> {
        self.merge_policy = Some(policy);
        Ok(())
    }

    /// Return cumulative compaction statistics (M5.2, Issue #474)
    ///
    /// Returns a snapshot of the lifetime totals accumulated across all compaction
    /// cycles that have completed since the `WriteEngine` was created. The snapshot
    /// is cheaply cloneable and safe to inspect from any thread (no lock required,
    /// because `WriteEngine` itself is not `Sync`).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = engine.maintenance_stats();
    /// println!(
    ///     "Completed {} compactions, merged {} rows, wrote {} bytes",
    ///     stats.compactions_completed,
    ///     stats.rows_merged,
    ///     stats.bytes_written,
    /// );
    /// ```
    pub fn maintenance_stats(&self) -> CompactionStats {
        self.cumulative_stats.clone()
    }

    /// Perform incremental maintenance work (M5.2, Issue #384)
    ///
    /// This method performs background compaction work within a time budget.
    /// It can be called repeatedly from a background thread or task scheduler
    /// to make incremental progress on compaction.
    ///
    /// ## Runtime contexts
    ///
    /// This is a synchronous method, but its internal async-to-sync bridge is
    /// runtime-aware (see [`merge::block_on_async`]), so it is safe to call from
    /// **either** a plain synchronous context **or** from within an active Tokio
    /// runtime — including `#[tokio::main]`/`#[tokio::test]` worker threads and
    /// `async fn` callers. Prior to Issue #587 calling it from inside a runtime
    /// panicked with "Cannot start a runtime from within a runtime" once a merge
    /// had input SSTables to read. The sync signature is preserved so the CLI and
    /// Python bindings can keep calling it directly. (The Node binding wraps it in
    /// `spawn_blocking`, which remains correct.)
    ///
    /// ## Behavior
    ///
    /// 1. If no active merge exists, consult the merge policy for work
    /// 2. If merge work is available, start a new merge
    /// 3. Process the active merge until budget is exhausted
    /// 4. Return progress report
    ///
    /// ## Invariants
    ///
    /// - Budget is honored within 10% tolerance
    /// - At least one CLUSTER GROUP is processed per call (minimum progress
    ///   guarantee; issue #1668 stage 4 loosened this from "at least one
    ///   partition" — a single oversized partition no longer blocks the
    ///   budget for its entire duration)
    /// - Merge state is preserved across calls for resumption, INCLUDING a
    ///   partition whose cluster-group drain was paused mid-way
    ///   (`ActiveMerge::pending_partition`, issue #1668 stage 4) — resuming
    ///   never re-computes or loses a row, and the writer always still
    ///   receives one partition's mutations in one `write_partition` call.
    ///
    /// ## Budget Enforcement
    ///
    /// The budget is honored within approximately 10% tolerance, checked
    /// BETWEEN CLUSTER GROUPS (issue #1668 stage 4) rather than only between
    /// whole partitions — a fat partition can now yield control back to the
    /// budget check partway through its own drain instead of running to
    /// completion regardless of elapsed time. The tolerance ensures forward
    /// progress on each call while remaining responsive to time constraints.
    ///
    /// # Arguments
    ///
    /// * `budget` - Maximum time to spend in this call
    ///
    /// # Returns
    ///
    /// A report containing progress metrics and whether more work is pending.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - Merge policy returns an error
    /// - SSTable reading or writing fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    ///
    /// // Background compaction loop
    /// loop {
    ///     let report = engine.maintenance_step(Duration::from_millis(100))?;
    ///
    ///     if !report.pending_compaction {
    ///         // No more work, sleep or exit
    ///         break;
    ///     }
    ///
    ///     // Log progress
    ///     println!("Merged {} rows in {:?}", report.rows_merged, report.time_spent);
    /// }
    /// ```
    #[tracing::instrument(name = "compaction.maintenance_step", level = "debug", skip(self))]
    pub fn maintenance_step(&mut self, budget: Duration) -> Result<MaintenanceReport> {
        // Budget requested for this step (issue #1037). Compared with the
        // consumed budget below (the scheduler honors a ~10% tolerance).
        crate::observability::record_histogram(
            crate::observability::catalog::COMPACTION_BUDGET_REQUESTED,
            budget.as_secs_f64(),
            &[],
        );

        let result = self.maintenance_step_inner(budget);

        // Budget consumed + lifetime-throughput counters (issue #1037). Recorded
        // for every step (even a no-op one) so the budget-tolerance signal is
        // complete; rows-merged is per-step and feeds the throughput rate when
        // combined with COMPACTION_DURATION at finalize.
        if let Ok(report) = &result {
            use crate::observability::{self as obs, catalog};
            obs::record_histogram(
                catalog::COMPACTION_BUDGET_CONSUMED,
                report.time_spent.as_secs_f64(),
                &[],
            );
            obs::add_counter(catalog::COMPACTION_ROWS_MERGED, report.rows_merged, &[]);
            obs::record_gauge(catalog::COMPACTION_LAG, self.l0_count as i64, &[]);
        }

        crate::observability::record_result("compaction", result)
    }

    fn maintenance_step_inner(&mut self, budget: Duration) -> Result<MaintenanceReport> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        let start = Instant::now();
        let mut report = MaintenanceReport {
            time_spent: Duration::from_secs(0),
            completed_merges: Vec::new(),
            rows_merged: 0,
            bytes_written: 0,
            pending_compaction: false,
            dropped_whole: Vec::new(),
        };

        // If no merge policy is set, no maintenance work to do
        let merge_policy = match &self.merge_policy {
            Some(policy) => policy,
            None => {
                report.time_spent = start.elapsed();
                return Ok(report);
            }
        };

        // If no active merge exists, check if we should start one
        if self.active_merge.is_none() {
            // SCOPE TO THIS TABLE (#935 branch review): `scan_sstable_candidates`
            // walks the whole `data_dir` recursively, so it can include SSTables
            // of OTHER keyspaces/tables. This WriteEngine is single-table
            // (`config.schema`) and always publishes output to
            // `data_dir/keyspace/table/`, so restrict the candidate set to THIS
            // table's directory BEFORE any policy or purge-safety decision.
            // Otherwise a full compaction of this table is misclassified as
            // partial whenever a foreign table's SSTable exists under `data_dir`
            // (`selected_set != candidate_set`), which both lets the policy see
            // foreign-table inputs and disables tombstone purging that is actually
            // safe. Every published SSTable for this table lives under
            // `table_dir`, so the scoping never drops a real input.
            let table_dir = self
                .config
                .data_dir
                .join(&self.config.schema.keyspace)
                .join(&self.config.schema.table);
            let candidates: Vec<PathBuf> = self
                .scan_sstable_candidates()?
                .into_iter()
                .filter(|p| p.starts_with(&table_dir))
                .collect();
            let selected = merge_policy.select_merge(&candidates)?;

            if !selected.is_empty() {
                // Overlap-safety gate for tombstone purging (#921 finding 1): a
                // compaction may purge tombstones ONLY when it spans EVERY
                // candidate SSTable for the table (a major/full compaction).
                // Otherwise a tombstone could be purged while a non-included
                // overlapping SSTable still holds data it shadows, resurrecting
                // that data on the next read. A partial selection (the common
                // background-compaction case) is therefore purge-UNSAFE: it
                // retains tombstones. Compare as sets so input ordering does not
                // affect the decision.
                let selected_set: std::collections::HashSet<&PathBuf> = selected.iter().collect();
                let candidate_set: std::collections::HashSet<&PathBuf> =
                    candidates.iter().collect();
                let purge_safe = !candidate_set.is_empty() && selected_set == candidate_set;

                // Overlap-aware partial-compaction purging (#935): when this is a
                // PARTIAL compaction (some candidate SSTables are NOT included),
                // compute the min write timestamp across those non-included
                // SSTables. A tombstone older than every one of them shadows
                // nothing outside the set and can be purged even here. For a full
                // compaction (`purge_safe == true`) there are no non-included
                // SSTables, so the bound is `None` and the merger uses its +inf
                // full-compaction fast path. `candidates` is already scoped to
                // this table's directory (see above), so the non-included set is
                // exactly this table's outside SSTables.
                // The non-included (outside) overlapping set for this table. Empty
                // for a full compaction (`purge_safe == true`). Used both for the
                // #935 overlap-purge bound below AND for the #1388 fully-expired
                // drop-set overlap gate (see `start_merge`).
                let non_included: Vec<PathBuf> = candidates
                    .iter()
                    .filter(|p| !selected_set.contains(*p))
                    .cloned()
                    .collect();
                let max_purgeable_timestamp = if purge_safe {
                    None
                } else {
                    merge::compute_max_purgeable_timestamp(&non_included)
                };

                // Start a new merge. `non_included` is threaded through so
                // `start_merge` can compute the fully-expired drop-set (issue #1388)
                // with the correct overlap gate.
                self.start_merge(selected, purge_safe, max_purgeable_timestamp, non_included)?;
            } else {
                // No work selected by policy
                report.time_spent = start.elapsed();
                report.pending_compaction = false;
                return Ok(report);
            }
        }

        // Process active merge within budget.
        //
        // Stage 4 (#1668, the "Q4 unlock"): the budget is checked BETWEEN
        // CLUSTER GROUPS, not only between whole partitions. Stage 5c-iv part
        // 3 changed WHAT gets stashed when a partition is too fat to finish
        // within one call's remaining budget: instead of accumulating a
        // growing `Vec<Mutation>` (the whole-partition buffering this issue
        // exists to eliminate), each cluster group now streams straight into
        // a `StreamingPartitionSession` as it arrives (mirroring
        // `KWayMerger::merge`'s stage 5c-iv part 2 change), and the SESSION
        // ITSELF — which owns everything it needs, no lifetime — is what
        // gets stashed on `ActiveMerge.pending_partition` and resumed
        // unchanged on the NEXT `maintenance_step` call. Nothing is
        // re-computed, nothing is lost, and the writer still receives one
        // partition as one on-disk unit (opened once, finished once), so
        // output stays byte-identical (see `#921`). The minimum-progress
        // guarantee remains "at least one cluster group per call."
        let budget_tolerance = budget.mul_f32(1.1); // 10% tolerance
        let mut partitions_processed = 0u64;

        /// Outcome of draining one partition's cluster groups, bounded by
        /// the mid-partition budget check.
        enum DrainOutcome {
            /// The partition fully drained; finalize whatever
            /// `PartitionStreamState` it ended in. Boxed: `Streaming` embeds
            /// a `StreamingPartitionSession` (clippy::large_enum_variant) —
            /// boxing keeps `DrainOutcome` itself cheap to move regardless.
            Ready(DecoratedKey, Box<PartitionStreamState>),
            /// Budget exceeded mid-drain; progress was stashed on
            /// `ActiveMerge.pending_partition` for the next call.
            Paused,
            /// No more partitions in any run.
            MergeComplete,
        }

        while let Some(merge) = &mut self.active_merge {
            // Resume a partition paused by a PRIOR call's budget check, if
            // any (issue #1668, stage 4). `raw_remaining` are rows `step()`
            // already reconciled but not yet popped; `stream_state` is this
            // partition's `PartitionStreamState` from EARLIER in this same
            // partition (this or a prior call) — possibly still resolving
            // the carrier/static prefix, or an already-open session with
            // rows fed so far.
            let (resume_state, mut stream_state): (
                Option<(DecoratedKey, VecDeque<merge::MergeEntry>)>,
                PartitionStreamState,
            ) = match merge.pending_partition.take() {
                Some((key, rows, state)) => (Some((key, rows)), state),
                None => (None, PartitionStreamState::fresh()),
            };

            // #850: convert with the effective compaction schema (DECODE
            // time — needed to identify/purge-evaluate a dropped column's
            // cells) so any static column re-added from the input headers is
            // preserved. Cloned ONCE here (before `stream` borrows
            // `merge.merger`) so per-row conversion below never needs to
            // borrow `self`/`merge` while that borrow is alive.
            let decode_schema = merge.effective_schema.clone();
            // ENCODE time: the writer's OWN schema (dropped columns already
            // stripped, if this compaction drops any) — required by every
            // `begin_streaming_partition`/`feed_streaming_row`/
            // `feed_streaming_static_row` call. Using `decode_schema` here
            // instead would repeat the exact #1019 regression
            // `KWayMerger::merge` hit in stage 5c-iv part 2 (a header/row
            // encoding mismatch that silently produced zero readable rows) —
            // see that stage's fix for the full incident writeup.
            let write_schema = merge.writer.schema().clone();
            let schema_has_static = write_schema.columns.iter().any(|c| c.is_static);
            let mut stream = merge::StreamingMerger::resume(&mut merge.merger, resume_state);
            // True once THIS call has popped at least one cluster group —
            // guarantees forward progress within a call even when resuming
            // an already-in-progress `stream_state` from a prior call
            // (mirrors the pre-stage-4 "always process at least one
            // partition" floor, now at cluster-group granularity).
            let mut progressed_this_call = false;

            let outcome = loop {
                if (partitions_processed > 0 || progressed_this_call)
                    && start.elapsed() >= budget_tolerance
                {
                    break DrainOutcome::Paused;
                }
                match stream.step_streaming()? {
                    merge::StreamingStep::ClusterGroup { key, row } => {
                        progressed_this_call = true;
                        // Skip metadata-only entries (#886/#899 branch-review):
                        // they carry complex/range deletion metadata through
                        // the merge stream but have no writer-emittable
                        // content yet. See `MergeEntry::is_metadata_only_no_op`.
                        if row.is_metadata_only_no_op() {
                            continue;
                        }
                        let mutation =
                            merge::KWayMerger::merge_entry_to_mutation(*row, &decode_schema)?;
                        // Single fold point for EVERY mutation of this
                        // partition, mirroring `KWayMerger::merge`'s stage
                        // 5c-iv part 2 design — folds unconditionally,
                        // regardless of classification, before it happens.
                        stats_fold::fold_mutation_stats(
                            stream_state.partition_stats_mut(),
                            &mutation,
                        );

                        if let PartitionStreamState::Streaming {
                            session, row_count, ..
                        } = &mut stream_state
                        {
                            merge.writer.feed_streaming_row(session, &mutation)?;
                            *row_count += 1;
                            continue;
                        }

                        // Still in the (bounded) None-keyed prefix.
                        let is_partition_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_some()
                            && mutation.row_tombstone.is_none()
                            && mutation.range_tombstones.is_empty();
                        let is_range_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_none()
                            && mutation.row_tombstone.is_none()
                            && !mutation.range_tombstones.is_empty();
                        // A `clustering_key: None` mutation is the resolved
                        // static-row carrier ONLY when the schema actually
                        // declares static columns — see
                        // `KWayMerger::merge`'s identical gate (issue #1668
                        // stage 5c-iv part 2) for the unclustered-table
                        // rationale.
                        let is_static_carrier =
                            mutation.clustering_key.is_none() && schema_has_static;

                        if is_partition_only || is_range_only || is_static_carrier {
                            if let PartitionStreamState::Prefix {
                                partition_tombstone,
                                range_tombstones,
                                static_tracker,
                                static_first_ts,
                                saw_carrier_or_static,
                                row_count,
                                ..
                            } = &mut stream_state
                            {
                                if is_partition_only {
                                    *partition_tombstone = mutation.partition_tombstone;
                                } else if is_range_only {
                                    range_tombstones
                                        .extend(mutation.range_tombstones.iter().cloned());
                                } else {
                                    if !*saw_carrier_or_static {
                                        *static_first_ts = mutation.timestamp_micros;
                                    }
                                    static_tracker.feed(&mutation, &write_schema, None);
                                }
                                *saw_carrier_or_static = true;
                                *row_count += 1;
                            }
                            continue;
                        }

                        // First Some(ck) row (or, for an unclustered table,
                        // its sole `clustering_key: None` row): the prefix
                        // is now COMPLETE. Take `stream_state` by value to
                        // open the session and transition to `Streaming`.
                        let prev =
                            std::mem::replace(&mut stream_state, PartitionStreamState::fresh());
                        let (
                            partition_tombstone,
                            range_tombstones,
                            static_tracker,
                            static_first_ts,
                            partition_stats,
                            mut row_count,
                        ) = match prev {
                            PartitionStreamState::Prefix {
                                partition_tombstone,
                                range_tombstones,
                                static_tracker,
                                static_first_ts,
                                partition_stats,
                                row_count,
                                ..
                            } => (
                                partition_tombstone,
                                range_tombstones,
                                static_tracker,
                                static_first_ts,
                                partition_stats,
                                row_count,
                            ),
                            // Provably unreachable — the `if let
                            // PartitionStreamState::Streaming` guard above
                            // already `continue`d whenever `stream_state` was
                            // `Streaming`. Handled without panicking (no
                            // unwrap/expect/unreachable!() in library code):
                            // just restore it and skip this cluster group
                            // rather than assert something the compiler
                            // cannot itself prove impossible here.
                            already_streaming @ PartitionStreamState::Streaming { .. } => {
                                stream_state = already_streaming;
                                continue;
                            }
                        };

                        let mut session = merge.writer.begin_streaming_partition(
                            &key,
                            partition_tombstone.as_ref(),
                            &range_tombstones,
                        )?;
                        if schema_has_static {
                            let merged = static_tracker.finish();
                            merge.writer.feed_streaming_static_row(
                                &mut session,
                                &merged,
                                static_first_ts,
                            )?;
                        }
                        merge.writer.feed_streaming_row(&mut session, &mutation)?;
                        row_count += 1;
                        stream_state = PartitionStreamState::Streaming {
                            session,
                            partition_stats,
                            row_count,
                            partition_tombstone,
                        };
                    }
                    merge::StreamingStep::PartitionEnd { key } => {
                        break DrainOutcome::Ready(
                            key,
                            Box::new(std::mem::replace(
                                &mut stream_state,
                                PartitionStreamState::fresh(),
                            )),
                        );
                    }
                    merge::StreamingStep::Complete => break DrainOutcome::MergeComplete,
                }
            };

            match outcome {
                DrainOutcome::Ready(key, state) => {
                    partitions_processed += 1;

                    match *state {
                        PartitionStreamState::Streaming {
                            session,
                            partition_stats,
                            row_count,
                            partition_tombstone,
                        } => {
                            if let Some(merge) = &mut self.active_merge {
                                let (offset, blocks, emit) =
                                    merge.writer.finish_streaming_partition(session)?;
                                merge.writer.complete_partition_incremental(
                                    &key,
                                    partition_tombstone.as_ref(),
                                    offset,
                                    &blocks,
                                    emit,
                                    &partition_stats,
                                )?;
                                merge.rows_merged += row_count;
                            }
                            report.rows_merged += row_count;
                        }
                        PartitionStreamState::Prefix {
                            partition_tombstone,
                            range_tombstones,
                            static_tracker,
                            static_first_ts,
                            saw_carrier_or_static,
                            partition_stats,
                            row_count,
                        } => {
                            // Truly empty partition (every entry was
                            // metadata-only-no-op) — skip entirely, matching
                            // the original `mutations.is_empty()` skip
                            // (#886 branch-review).
                            if !saw_carrier_or_static {
                                continue;
                            }
                            // No `Some(ck)` row ever arrived, but a
                            // range/partition tombstone or a static value
                            // survived — still emittable (#933/#1072),
                            // matching `KWayMerger::merge`'s `None if
                            // saw_carrier_or_static` branch exactly.
                            if let Some(merge) = &mut self.active_merge {
                                let mut session = merge.writer.begin_streaming_partition(
                                    &key,
                                    partition_tombstone.as_ref(),
                                    &range_tombstones,
                                )?;
                                if schema_has_static {
                                    let merged = static_tracker.finish();
                                    merge.writer.feed_streaming_static_row(
                                        &mut session,
                                        &merged,
                                        static_first_ts,
                                    )?;
                                }
                                let (offset, blocks, emit) =
                                    merge.writer.finish_streaming_partition(session)?;
                                merge.writer.complete_partition_incremental(
                                    &key,
                                    partition_tombstone.as_ref(),
                                    offset,
                                    &blocks,
                                    emit,
                                    &partition_stats,
                                )?;
                                merge.rows_merged += row_count;
                            }
                            report.rows_merged += row_count;
                        }
                    }
                }
                DrainOutcome::Paused => {
                    // Stash progress for the NEXT maintenance_step call
                    // (issue #1668, stage 4). `into_paused_state` is `Some`
                    // whenever ANY row was popped for the in-progress
                    // partition — guaranteed here because `progressed_this_call`
                    // (or a non-empty `resume_state`, which also seeds
                    // `stream`'s partition key) must be true to reach `Paused`.
                    if let Some((key, raw_remaining)) = stream.into_paused_state() {
                        if let Some(merge) = &mut self.active_merge {
                            merge.pending_partition = Some((key, raw_remaining, stream_state));
                        }
                    }
                    break;
                }
                DrainOutcome::MergeComplete => {
                    // Merge is complete - finalize and clean up
                    // Use blocking call to handle async finalization
                    self.finalize_merge_blocking(&mut report)?;
                    break;
                }
            }
        }

        // Check if more work is pending
        report.pending_compaction = self.active_merge.is_some();
        report.time_spent = start.elapsed();

        Ok(report)
    }

    #[tracing::instrument(name = "compaction.scan_candidates", level = "debug", skip(self))]
    fn scan_sstable_candidates(&self) -> Result<Vec<PathBuf>> {
        let mut candidates = Vec::new();

        if !self.config.data_dir.exists() {
            return Ok(candidates);
        }

        Self::scan_data_files(
            &self.config.data_dir,
            &mut candidates,
            crate::storage::sstable::MAX_SSTABLE_SCAN_DEPTH,
        )?;
        Ok(candidates)
    }

    /// Recursively scan for Data.db files
    fn scan_data_files(dir: &Path, candidates: &mut Vec<PathBuf>, depth: usize) -> Result<()> {
        for entry in std::fs::read_dir(dir)
            .map_err(|e| Error::Storage(format!("Failed to read data directory: {}", e)))?
        {
            let entry = entry
                .map_err(|e| Error::Storage(format!("Failed to read directory entry: {}", e)))?;

            let path = entry.path();
            let filename = path.file_name().unwrap_or_default().to_string_lossy();

            // Only consider Data.db files
            if filename.starts_with("nb-") && filename.ends_with("-big-Data.db") {
                // Honor the TOC.txt publication barrier (Issue #591). A Data.db
                // without a sibling TOC.txt is NOT a published SSTable: it is
                // either a crash-interrupted partial rename or a deferred-delete
                // orphan whose TOC was removed first while its data file stayed
                // pinned by an open/mapped reader (Windows). Feeding such a file
                // to the merger would re-compact an unpublished input and could
                // produce garbled output, so it is skipped here just as the
                // read path discovers SSTables by TOC.txt. The startup orphan
                // sweep reclaims the leftover components.
                let base = filename.trim_end_matches("-Data.db");
                let toc_path = path.with_file_name(format!("{base}-TOC.txt"));
                if toc_path.exists() {
                    candidates.push(path);
                } else {
                    tracing::debug!(
                        "scan_data_files: skipping unpublished SSTable (no TOC.txt): {:?}",
                        path
                    );
                }
            } else if depth > 0 && path.is_dir() {
                Self::scan_data_files(&path, candidates, depth - 1)?;
            }
        }
        Ok(())
    }

    /// Delete all component files for an SSTable (M5.2 helper)
    pub(crate) fn delete_sstable_files(&self, data_path: &Path) -> Result<()> {
        Self::delete_sstable_files_static(data_path)
    }

    /// Static helper that deletes all component files for an SSTable given the
    /// Data.db path.  Called from both `delete_sstable_files` and the startup
    /// orphan sweep, which runs before `self` is fully constructed.
    ///
    /// ## Deferred-delete / Windows policy (Issue #591)
    ///
    /// `TOC.txt` is removed **first**. TOC.txt is the publication barrier — both
    /// the read path (`SSTableManager`) and the compaction candidate scan
    /// (`scan_data_files`, since #591) treat a Data.db without a sibling TOC.txt
    /// as unpublished. Removing TOC.txt first therefore *unpublishes* the SSTable
    /// atomically, before any data component is touched, so it can never be
    /// observed (no duplicate rows, never re-fed to the merger) even if the
    /// remaining components cannot be removed yet.
    ///
    /// The remaining components are then deleted **best-effort**: a failure on
    /// any one of them (most plausibly a Windows sharing violation when a
    /// concurrent reader still has the file open or memory-mapped) is logged but
    /// does NOT abort the rest or fail the operation. Such a leftover is a
    /// harmless orphan — invisible because its TOC.txt is gone — and is reclaimed
    /// by [`Self::sweep_orphaned_partial_sstables`] on the next engine startup,
    /// by which time the reader's handle has been released. This is the
    /// "deferred delete" half of the policy; Unix removes the inode immediately
    /// while any mapping keeps the bytes alive until it is dropped.
    pub(crate) fn delete_sstable_files_static(data_path: &Path) -> Result<()> {
        // Extract base path: nb-{gen}-big
        let filename = data_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Storage("Invalid SSTable path".to_string()))?;

        let base = filename
            .strip_suffix("-Data.db")
            .ok_or_else(|| Error::Storage("Invalid Data.db filename".to_string()))?;

        let parent_dir = data_path.parent().ok_or_else(|| {
            Error::Storage(format!(
                "Data.db path has no parent directory: {:?}",
                data_path
            ))
        })?;

        // TOC.txt FIRST — the publication barrier (Issue #591). Once it is gone
        // the SSTable is unpublished regardless of whether the data components
        // can be removed. Remaining components follow, best-effort.
        let components = [
            "TOC.txt",
            "Data.db",
            "Index.db",
            "Summary.db",
            "Statistics.db",
            "CompressionInfo.db",
            // CRC.db is the per-chunk CRC for uncompressed BIG SSTables
            // (Issue #1197); without it deletion/compaction would leave an
            // orphan file. Best-effort like the other optional components.
            "CRC.db",
            "Filter.db",
            "Digest.crc32",
        ];

        let mut failures: Vec<String> = Vec::new();
        for component in &components {
            let component_path = parent_dir.join(format!("{}-{}", base, component));
            if component_path.exists() {
                match std::fs::remove_file(&component_path) {
                    Ok(()) => tracing::debug!("Deleted compaction input: {:?}", component_path),
                    Err(e) => {
                        // Best-effort: do not abort. A leftover data component
                        // whose TOC.txt is already gone is an invisible orphan
                        // reclaimed by the startup sweep (Issue #591).
                        tracing::warn!(
                            "Deferred delete of {:?}: {} (component left as orphan; \
                             unpublished via TOC.txt removal, reclaimed on next startup)",
                            component_path,
                            e
                        );
                        failures.push(format!("{:?}: {}", component_path, e));
                    }
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            // Surface a non-fatal error so callers can log it. The SSTable is
            // already unpublished (TOC.txt removed first), so callers treat this
            // as a deferred reclamation, not a correctness failure.
            Err(Error::Storage(format!(
                "Deferred delete left {} orphaned component(s) (unpublished, reclaimed on \
                 next startup): {}",
                failures.len(),
                failures.join("; ")
            )))
        }
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::test_support::{create_test_schema, flush_n_sstables_sync};
    use crate::storage::write_engine::WriteEngineConfig;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::TempDir;

    // Mock merge policy that selects specific files for testing
    #[derive(Debug)]
    #[allow(dead_code)] // Used in multiple test functions below
    struct TestMergePolicy {
        files_to_select: Vec<PathBuf>,
    }

    impl MergePolicy for TestMergePolicy {
        fn select_merge(&self, _candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
            Ok(self.files_to_select.clone())
        }
    }

    #[test]
    fn test_set_merge_policy() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Should succeed now (was previously returning error)
        let policy = Box::new(crate::storage::write_engine::STCSPolicy::default());
        engine.set_merge_policy(policy).unwrap();

        // With policy set but no SSTables, should return quickly with no work
        let report = engine
            .maintenance_step(std::time::Duration::from_millis(100))
            .unwrap();
        assert!(!report.pending_compaction);
        assert_eq!(report.rows_merged, 0);
    }

    // M5.2 maintenance_step() tests (Issue #384)

    #[test]
    fn test_maintenance_step_no_policy() {
        // Without a merge policy, maintenance_step should do nothing.
        // Since #1619 makes STCS the default, disable auto_compaction so this
        // test still validates the None branch (no-policy -> no work).
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        config.auto_compaction = false;

        let mut engine = WriteEngine::new(config).unwrap();

        // Call maintenance_step without setting a policy
        let report = engine.maintenance_step(Duration::from_millis(100)).unwrap();

        // Should return immediately with no work done
        assert_eq!(report.rows_merged, 0);
        assert_eq!(report.bytes_written, 0);
        assert_eq!(report.completed_merges.len(), 0);
        assert!(!report.pending_compaction);
        assert!(report.time_spent < Duration::from_millis(50));
    }

    #[test]
    fn test_maintenance_step_with_closed_engine() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Close the engine
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.close())
            .unwrap();

        // maintenance_step should fail on closed engine
        let result = engine.maintenance_step(Duration::from_millis(100));
        assert!(result.is_err());
        match result {
            Err(Error::InvalidInput(msg)) => {
                assert!(msg.contains("closed"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_maintenance_report_creation() {
        let report = MaintenanceReport {
            time_spent: Duration::from_millis(250),
            completed_merges: vec![PathBuf::from("data/nb-5-big-Data.db")],
            rows_merged: 1000,
            bytes_written: 1024 * 1024,
            pending_compaction: true,
            dropped_whole: Vec::new(),
        };

        assert_eq!(report.time_spent.as_millis(), 250);
        assert_eq!(report.completed_merges.len(), 1);
        assert_eq!(report.rows_merged, 1000);
        assert_eq!(report.bytes_written, 1024 * 1024);
        assert!(report.pending_compaction);
    }

    #[test]
    fn test_scan_sstable_candidates_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        let candidates = engine.scan_sstable_candidates().unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_scan_sstable_candidates_with_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Create dummy SSTable files. Each Data.db needs a sibling TOC.txt to
        // count as a *published* SSTable (the publication barrier, Issue #591) —
        // a Data.db without TOC.txt is an unpublished partial/orphan and must be
        // skipped by the candidate scan.
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(data_dir.join("nb-1-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-1-big-TOC.txt"), b"").unwrap();
        std::fs::write(data_dir.join("nb-2-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-2-big-TOC.txt"), b"").unwrap();
        std::fs::write(data_dir.join("nb-3-big-Index.db"), b"").unwrap(); // Not a Data.db
        std::fs::write(data_dir.join("other-file.txt"), b"").unwrap(); // Not an SSTable
                                                                       // An unpublished Data.db (no TOC.txt) must NOT be picked up (Issue #591).
        std::fs::write(data_dir.join("nb-4-big-Data.db"), b"").unwrap();

        let candidates = engine.scan_sstable_candidates().unwrap();

        // Should only find the two PUBLISHED Data.db files (TOC.txt present);
        // nb-4 is excluded because it has no TOC.txt.
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .all(|p| p.to_string_lossy().contains("Data.db")));
        assert!(
            !candidates
                .iter()
                .any(|p| p.to_string_lossy().contains("nb-4-big")),
            "unpublished Data.db (no TOC.txt) must be excluded (Issue #591)"
        );
    }

    #[test]
    fn test_delete_sstable_files() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Create dummy SSTable component files
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let components = [
            "nb-5-big-Data.db",
            "nb-5-big-Index.db",
            "nb-5-big-Summary.db",
            "nb-5-big-Statistics.db",
        ];

        for component in &components {
            std::fs::write(data_dir.join(component), b"dummy").unwrap();
        }

        // Verify files exist
        for component in &components {
            assert!(data_dir.join(component).exists());
        }

        // Delete SSTable files
        let data_path = data_dir.join("nb-5-big-Data.db");
        engine.delete_sstable_files(&data_path).unwrap();

        // Verify files are deleted
        for component in &components {
            assert!(!data_dir.join(component).exists());
        }
    }

    /// Issue #591: deletion removes TOC.txt FIRST so the SSTable is unpublished
    /// before any data component is touched. This guarantees the read path and
    /// the compaction candidate scan stop seeing it immediately, even if a data
    /// component cannot be removed yet (e.g. pinned by a mapped reader on
    /// Windows).
    #[test]
    fn test_delete_removes_toc_first_unpublishing_atomically() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // A full published SSTable component set including TOC.txt.
        for comp in &[
            "nb-7-big-Data.db",
            "nb-7-big-Index.db",
            "nb-7-big-Statistics.db",
            "nb-7-big-TOC.txt",
        ] {
            std::fs::write(data_dir.join(comp), b"x").unwrap();
        }

        let data_path = data_dir.join("nb-7-big-Data.db");
        WriteEngine::delete_sstable_files_static(&data_path).unwrap();

        // Everything gone on the happy path.
        assert!(!data_dir.join("nb-7-big-TOC.txt").exists());
        assert!(!data_path.exists());

        // And critically: scan_data_files (the compaction candidate discovery)
        // never surfaces a Data.db without a TOC.txt, so a deferred-delete orphan
        // is not re-fed to the merger. Recreate a TOC-less leftover to prove it.
        std::fs::write(data_dir.join("nb-8-big-Data.db"), b"x").unwrap();
        let mut candidates = Vec::new();
        WriteEngine::scan_data_files(&data_dir, &mut candidates, 1).unwrap();
        assert!(
            candidates.is_empty(),
            "a Data.db without a sibling TOC.txt must NOT be a compaction candidate \
             (publication barrier, Issue #591); got {:?}",
            candidates
        );

        // Add the matching TOC.txt and it becomes a valid candidate again.
        std::fs::write(data_dir.join("nb-8-big-TOC.txt"), b"x").unwrap();
        let mut candidates = Vec::new();
        WriteEngine::scan_data_files(&data_dir, &mut candidates, 1).unwrap();
        assert_eq!(
            candidates.len(),
            1,
            "a published Data.db (TOC.txt present) must be discovered"
        );
    }

    #[test]
    fn test_maintenance_step_with_policy_no_work() {
        // Policy that returns empty selection (no work to do)
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Set a policy that selects nothing
        let policy = TestMergePolicy {
            files_to_select: vec![],
        };
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // Call maintenance_step - policy selects no work
        let report = engine.maintenance_step(Duration::from_millis(100)).unwrap();

        // Should return with no work done
        assert_eq!(report.rows_merged, 0);
        assert_eq!(report.bytes_written, 0);
        assert_eq!(report.completed_merges.len(), 0);
        assert!(!report.pending_compaction);
    }

    #[test]
    fn test_maintenance_step_budget_honored() {
        // Test that budget is approximately honored
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Set a policy that selects nothing
        let policy = TestMergePolicy {
            files_to_select: vec![],
        };
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // Call with small budget - policy selects no work, should return quickly
        let budget = Duration::from_millis(10);
        let report = engine.maintenance_step(budget).unwrap();

        // Should return quickly when there's no compaction work
        assert!(
            report.time_spent < budget.mul_f32(1.5),
            "Time spent {:?} exceeded budget {:?} by >50%",
            report.time_spent,
            budget
        );
    }

    #[test]
    fn test_maintenance_stats_initial_zero() {
        // Before any maintenance work, all stats should be zero
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        let stats = engine.maintenance_stats();
        assert_eq!(stats.compactions_completed, 0);
        assert_eq!(stats.sstables_merged_in, 0);
        assert_eq!(stats.sstables_produced, 0);
        assert_eq!(stats.bytes_read, 0);
        assert_eq!(stats.bytes_written, 0);
        assert_eq!(stats.rows_merged, 0);
        assert_eq!(stats.total_time, Duration::ZERO);
    }

    #[test]
    fn test_stcs_selects_expected_group_by_size() {
        // Verify that STCSPolicy groups four same-sized SSTables into one candidate set.
        // We do this without actually running a merge (just test the policy selection).
        let policy = crate::storage::write_engine::STCSPolicy::default();

        // Create 4 temp files of equal size to satisfy min_threshold=4
        let temp_dir = TempDir::new().unwrap();
        let mut paths = Vec::new();
        for i in 1..=4 {
            let path = temp_dir.path().join(format!("nb-{}-big-Data.db", i));
            // 60 MB each (above min_sstable_size threshold)
            let size_bytes = 60 * 1024 * 1024u64;
            let file = std::fs::File::create(&path).unwrap();
            file.set_len(size_bytes).unwrap();
            paths.push(path);
        }

        // Policy should select all 4 as a candidate group
        let selected = policy.select_merge(&paths).unwrap();
        assert_eq!(
            selected.len(),
            4,
            "STCS should select all 4 same-sized SSTables as one compaction group"
        );

        // All selected paths should be from our input set
        for sel in &selected {
            assert!(
                paths.contains(sel),
                "Selected path {:?} not in input set",
                sel
            );
        }
    }

    #[test]
    fn test_stcs_does_not_select_below_threshold() {
        // With only 3 SSTables, STCS (min_threshold=4) should select nothing.
        let policy = crate::storage::write_engine::STCSPolicy::default();

        let temp_dir = TempDir::new().unwrap();
        let mut paths = Vec::new();
        for i in 1..=3 {
            let path = temp_dir.path().join(format!("nb-{}-big-Data.db", i));
            let file = std::fs::File::create(&path).unwrap();
            file.set_len(60 * 1024 * 1024).unwrap();
            paths.push(path);
        }

        let selected = policy.select_merge(&paths).unwrap();
        assert!(
            selected.is_empty(),
            "STCS should NOT select when fewer than min_threshold SSTables exist"
        );
    }

    #[test]
    fn test_maintenance_step_compacts_sstables_atomically() {
        // Create an engine, flush 4 SSTables, then run maintenance_step with STCS.
        // After the step: input files must be gone, output file must exist,
        // and maintenance_stats() must reflect the completed compaction.
        //
        // Uses a sync wrapper so maintenance_step's internal block_on works without
        // nesting inside a pre-existing async runtime.
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Use a LOW min_sstable_size so small test files pass bucket grouping
        let policy = crate::storage::write_engine::STCSPolicy::new(
            4,   // min_threshold
            32,  // max_threshold
            0.5, // bucket_low
            1.5, // bucket_high
            0,   // min_sstable_size = 0 so tiny files group together
        )
        .unwrap();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Flush 4 distinct SSTables (sync helper creates its own single-threaded runtime)
        let input_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(input_paths.len(), 4, "Expected 4 flushed SSTables");

        // Verify all input Data.db files exist before compaction
        for p in &input_paths {
            assert!(
                p.exists(),
                "Input file {:?} should exist before compaction",
                p
            );
        }

        // Attach the policy and run maintenance
        engine.set_merge_policy(Box::new(policy)).unwrap();
        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();

        // The report must indicate a completed merge
        assert_eq!(
            report.completed_merges.len(),
            1,
            "Expected exactly 1 completed merge, got: {:?}",
            report.completed_merges
        );
        // bytes_written is u64 and always non-negative, so no assertion needed here.

        // The merged output file must exist in the final SSTable directory
        let merged_path = &report.completed_merges[0];
        assert!(
            merged_path.exists(),
            "Merged output file {:?} must exist after compaction",
            merged_path
        );

        // All input files must be gone (consumed by compaction)
        for p in &input_paths {
            assert!(
                !p.exists(),
                "Input file {:?} should have been deleted after compaction",
                p
            );
        }

        // maintenance_stats() must reflect the operation
        let stats = engine.maintenance_stats();
        assert_eq!(
            stats.compactions_completed, 1,
            "compactions_completed must be 1"
        );
        assert_eq!(
            stats.sstables_merged_in, 4,
            "Should have consumed 4 input SSTables"
        );
        assert_eq!(stats.sstables_produced, 1, "sstables_produced must be 1");
        // bytes_written may be 0 if the merged output is empty (reader/writer compatibility),
        // but total_time must be non-zero
        assert!(stats.total_time > Duration::ZERO, "total_time must be > 0");
    }

    /// Stage 4 (#1668, the "Q4 unlock"): the mid-partition budget check fires
    /// BETWEEN CLUSTER GROUPS, not only between whole partitions. A synthetic
    /// FAT partition (many clustering rows split across 2 input SSTables, so
    /// a real K-way merge reconciles them into ONE partition) drained with a
    /// near-zero budget must PAUSE mid-drain — proven directly by observing
    /// `ActiveMerge::pending_partition` populated after the FIRST call: with
    /// only ONE partition (`id = 1`) in the whole merge, a pause is ONLY
    /// possible mid-partition, never between partitions. The pre-stage-4
    /// design always finished a whole partition within a single call
    /// regardless of budget, so this also proves the fat partition's drain
    /// spans MULTIPLE `maintenance_step` calls, and that no row is lost or
    /// duplicated across the pause/resume cycle.
    #[test]
    fn test_maintenance_step_pauses_mid_partition_on_tiny_budget() {
        use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
        use crate::storage::write_engine::mutation::{
            CellOperation, ClusteringKey, PartitionKey, TableId,
        };
        use crate::types::Value;
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        // A schema with ONE clustering column so many rows can share ONE
        // partition key (id=1) — the "fat partition" fixture.
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const ROWS_PER_FLUSH: i32 = 15;
        const TOTAL_ROWS: u64 = (ROWS_PER_FLUSH * 2) as u64;

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Two flushes, ONE shared partition key (id=1), DISJOINT clustering
        // ranges — a real K-way merge reconciles them into ONE fat partition.
        let mut input_paths = Vec::new();
        for flush_idx in 0..2 {
            for i in 0..ROWS_PER_FLUSH {
                let ck = flush_idx * ROWS_PER_FLUSH + i;
                let table_id = TableId::new("test_ks", "test_table");
                let pk = PartitionKey::single("id", Value::Integer(1));
                let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
                let ops = vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(format!("row-{ck}")),
                }];
                let mutation = Mutation::new(
                    table_id,
                    pk,
                    Some(ck_key),
                    ops,
                    1_000_000 + i64::from(ck),
                    None,
                );
                engine.write(mutation).unwrap();
            }
            let info = rt.block_on(engine.flush()).unwrap().unwrap();
            input_paths.push(info.data_path);
        }
        assert_eq!(input_paths.len(), 2, "expected 2 flushed SSTables");

        let policy = crate::storage::write_engine::STCSPolicy::new(
            2,   // min_threshold
            32,  // max_threshold
            0.5, // bucket_low
            1.5, // bucket_high
            0,   // min_sstable_size = 0 so tiny files group together
        )
        .unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // A near-zero budget: `budget_tolerance` (budget * 1.1) is ~1ns, so
        // the elapsed-time check trips on the very next iteration after the
        // first cluster group is popped — any realistic per-iteration
        // overhead vastly exceeds 1ns.
        let tiny_budget = Duration::from_nanos(1);
        let first_report = engine.maintenance_step(tiny_budget).unwrap();

        // With only ONE partition (id=1) across the whole merge, a pause is
        // ONLY possible mid-partition — prove it directly.
        let paused_mid_partition = engine
            .active_merge
            .as_ref()
            .and_then(|m| m.pending_partition.as_ref())
            .is_some();
        assert!(
            paused_mid_partition,
            "expected ActiveMerge::pending_partition to be populated after a \
             tiny-budget call against a single fat partition — the mid-partition \
             budget check (issue #1668 stage 4) must have fired DURING the \
             partition's cluster-group drain, not just between partitions"
        );
        assert!(
            first_report.pending_compaction,
            "compaction must still be pending after a tiny-budget call"
        );
        assert_eq!(
            first_report.completed_merges.len(),
            0,
            "the fat partition must not have finished writing in one tiny-budget call"
        );

        // Keep calling (generous budget now) until the merge completes.
        let mut calls = 1u32;
        let mut total_rows_merged = first_report.rows_merged;
        let mut final_report = first_report;
        while final_report.pending_compaction {
            final_report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
            total_rows_merged += final_report.rows_merged;
            calls += 1;
            assert!(
                calls < 10_000,
                "compaction never completed — possible stall/starvation"
            );
        }
        assert!(
            calls > 1,
            "expected the fat partition's drain to span MULTIPLE maintenance_step \
             calls (mid-partition pause/resume) — got just {calls} call(s), which \
             would only be possible if the pre-stage-4 (whole-partition-per-call) \
             behavior were still in effect"
        );

        // Completeness: every one of the TOTAL_ROWS distinct clustering rows
        // for partition id=1 must have been merged exactly once — proving no
        // row was lost or duplicated across the pause/resume cycle.
        assert_eq!(
            total_rows_merged, TOTAL_ROWS,
            "expected all {TOTAL_ROWS} clustering rows to be merged exactly \
             once across the paused/resumed drain"
        );
        assert_eq!(
            final_report.completed_merges.len(),
            1,
            "expected exactly one completed merge output"
        );
        let stats = engine.maintenance_stats();
        assert_eq!(
            stats.sstables_merged_in, 2,
            "must have consumed both input SSTables"
        );
    }

    /// Shared fixture for the byte-identity proof below: builds the SAME
    /// 2-generation fat-partition fixture as
    /// `test_maintenance_step_pauses_mid_partition_on_tiny_budget` (a fresh,
    /// independent `WriteEngine`/temp dir each call — deterministic content,
    /// so two calls produce byte-identical INPUT SSTables too), compacts it
    /// under `budget`, and returns the produced output's raw Data.db bytes.
    fn compact_fat_partition_fixture_with_budget(budget: Duration) -> Vec<u8> {
        use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
        use crate::storage::write_engine::mutation::{
            CellOperation, ClusteringKey, PartitionKey, TableId,
        };
        use crate::types::Value;
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const ROWS_PER_FLUSH: i32 = 15;

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        for flush_idx in 0..2 {
            for i in 0..ROWS_PER_FLUSH {
                let ck = flush_idx * ROWS_PER_FLUSH + i;
                let table_id = TableId::new("test_ks", "test_table");
                let pk = PartitionKey::single("id", Value::Integer(1));
                let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
                let ops = vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(format!("row-{ck}")),
                }];
                let mutation = Mutation::new(
                    table_id,
                    pk,
                    Some(ck_key),
                    ops,
                    1_000_000 + i64::from(ck),
                    None,
                );
                engine.write(mutation).unwrap();
            }
            rt.block_on(engine.flush()).unwrap();
        }

        let policy = crate::storage::write_engine::STCSPolicy::new(2, 32, 0.5, 1.5, 0).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();

        let mut report = engine.maintenance_step(budget).unwrap();
        let mut calls = 1u32;
        while report.pending_compaction {
            report = engine.maintenance_step(budget).unwrap();
            calls += 1;
            assert!(
                calls < 100_000,
                "compaction never completed — possible stall/starvation"
            );
        }
        assert_eq!(
            report.completed_merges.len(),
            1,
            "expected exactly one completed merge output"
        );
        std::fs::read(&report.completed_merges[0]).expect("read compacted Data.db")
    }

    /// Byte-identity proof (issue #1668, stage 5c-iv part 3): pausing and
    /// resuming a partition's drain mid-stream — via
    /// `StreamingPartitionSession` stashed on
    /// `ActiveMerge::pending_partition` across many `maintenance_step`
    /// calls, instead of the pre-stage-5c-iv-part-3 growing `Vec<Mutation>`
    /// — must produce EXACTLY the same Data.db bytes as a single unbroken
    /// drain. Same rigor as every `IncrementalPartitionWriter`/
    /// `StreamingPartitionSession` byte-identity test in `data_writer/tests/`,
    /// but exercised through the REAL production entry point
    /// (`WriteEngine::maintenance_step`) rather than the session type
    /// directly — proving the INTEGRATION (budget checks,
    /// `PartitionStreamState` transitions, resume plumbing) introduces no
    /// divergence of its own.
    #[test]
    fn test_maintenance_paused_and_unpaused_compaction_produce_byte_identical_output() {
        // Generous budget: the fat partition drains in ONE
        // `maintenance_step` call (no pause) — the unbroken-batch baseline.
        let unpaused_bytes = compact_fat_partition_fixture_with_budget(Duration::from_secs(60));

        // Near-zero budget: forces the SAME fat partition's drain to pause
        // and resume across MANY `maintenance_step` calls (issue #1668 stage
        // 4's mid-partition budget check).
        let paused_bytes = compact_fat_partition_fixture_with_budget(Duration::from_nanos(1));

        assert_eq!(
            unpaused_bytes, paused_bytes,
            "pausing and resuming mid-partition must produce byte-identical \
             Data.db output to a single unbroken drain"
        );
    }

    /// #935 branch-review regression: `scan_sstable_candidates` walks the whole
    /// `data_dir` recursively, so a foreign keyspace/table's SSTable sitting under
    /// `data_dir` must NOT be treated as a candidate for this table's compaction.
    /// Before the fix the foreign SSTable inflated `candidate_set`, so a full
    /// compaction of this table was misclassified as partial (the policy could
    /// also see the foreign input). After the fix candidates are scoped to
    /// `data_dir/keyspace/table/`, so only this table's SSTables are merged and
    /// the foreign file is left untouched.
    #[test]
    fn test_maintenance_step_ignores_foreign_table_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let policy = crate::storage::write_engine::STCSPolicy::new(
            4,   // min_threshold
            32,  // max_threshold
            0.5, // bucket_low
            1.5, // bucket_high
            0,   // min_sstable_size = 0 so tiny files group together
        )
        .unwrap();

        let data_dir = temp_dir.path().join("data");
        let config = WriteEngineConfig::new(data_dir.clone(), temp_dir.path().join("wal"), schema);

        let mut engine = WriteEngine::new(config).unwrap();

        // Flush 4 SSTables for THIS table (data/test_ks/test_table/).
        let input_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(input_paths.len(), 4, "Expected 4 flushed SSTables");

        // Plant a foreign keyspace/table SSTable under the same data_dir, with a
        // sibling TOC.txt so it passes the publication barrier and would be
        // discovered by the recursive scan.
        let foreign_dir = data_dir.join("other_ks").join("other_tbl");
        std::fs::create_dir_all(&foreign_dir).unwrap();
        let foreign_data = foreign_dir.join("nb-1-big-Data.db");
        std::fs::write(&foreign_data, b"not a real sstable").unwrap();
        std::fs::write(foreign_dir.join("nb-1-big-TOC.txt"), b"Data.db\nTOC.txt\n").unwrap();

        engine.set_merge_policy(Box::new(policy)).unwrap();
        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();

        // The merge must complete using ONLY this table's 4 inputs.
        assert_eq!(
            report.completed_merges.len(),
            1,
            "Expected exactly 1 completed merge, got: {:?}",
            report.completed_merges
        );
        let stats = engine.maintenance_stats();
        assert_eq!(
            stats.sstables_merged_in, 4,
            "Only this table's 4 SSTables must be merged; the foreign SSTable must be excluded"
        );

        // The foreign SSTable must be left completely untouched.
        assert!(
            foreign_data.exists(),
            "Foreign-table SSTable {:?} must not be consumed by this table's compaction",
            foreign_data
        );

        // This table's inputs are consumed as usual.
        for p in &input_paths {
            assert!(
                !p.exists(),
                "Input file {:?} should have been deleted after compaction",
                p
            );
        }
    }

    #[test]
    fn test_maintenance_stats_accumulate_across_cycles() {
        // Run two compaction cycles and verify that stats accumulate.
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let policy = crate::storage::write_engine::STCSPolicy::new(
            4, 32, 0.5, 1.5, 0, // min_sstable_size=0 for small test files
        )
        .unwrap();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // First cycle: flush 4, compact
        flush_n_sstables_sync(&mut engine, 4);
        engine.maintenance_step(Duration::from_secs(60)).unwrap();

        let stats_after_first = engine.maintenance_stats();
        assert_eq!(stats_after_first.compactions_completed, 1);

        // Second cycle: flush 4 more, compact again
        // Row IDs must not collide with the first cycle so each cycle produces 4 SSTables.
        // flush_n_sstables_sync uses batch * 100 + row, so offset the start batch.
        // We re-use the helper but note generation counter now starts at a higher value,
        // so the output SSTable won't conflict with input paths from cycle 1.
        flush_n_sstables_sync(&mut engine, 4);
        engine.maintenance_step(Duration::from_secs(60)).unwrap();

        let stats_after_second = engine.maintenance_stats();
        assert_eq!(
            stats_after_second.compactions_completed, 2,
            "Stats must accumulate across compaction cycles"
        );
        assert_eq!(
            stats_after_second.sstables_merged_in, 8,
            "Should have consumed 8 total input SSTables (2 cycles × 4 each)"
        );
        assert_eq!(
            stats_after_second.sstables_produced, 2,
            "Should have produced 2 output SSTables"
        );
        assert!(
            stats_after_second.total_time >= stats_after_first.total_time,
            "Cumulative total_time must only increase"
        );
    }

    #[test]
    fn test_maintenance_step_inputs_intact_on_unwriteable_tmp_dir() {
        // Failure injection: make the data_dir read-only so creating the tmp
        // compaction directory fails. All input SSTables must remain intact.
        //
        // Note: This test relies on filesystem permissions and is skipped when
        // running as root (where permissions are not enforced).

        // Skip if running as root (CI containers sometimes run as root)
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // Try /proc/self first (Linux), fall back to checking euid via libc
            let is_root = std::fs::metadata("/proc/self")
                .map(|m| m.uid() == 0)
                .unwrap_or_else(|_| {
                    // On macOS, /proc/self doesn't exist; use a writable sentinel
                    false
                });
            // Also check by trying to write to /etc/cqlite-test-root-check
            let is_root_macos = std::fs::write("/etc/cqlite-test-root-check", b"")
                .map(|_| {
                    let _ = std::fs::remove_file("/etc/cqlite-test-root-check");
                    true
                })
                .unwrap_or(false);
            if is_root || is_root_macos {
                // Running as root — permission denial won't work; skip.
                return;
            }
        }

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Flush 4 SSTables so STCS can select them
        let input_paths = flush_n_sstables_sync(&mut engine, 4);
        for p in &input_paths {
            assert!(
                p.exists(),
                "Input file {:?} should exist before failure test",
                p
            );
        }

        // Make data_dir read-only so creating tmp dir fails
        let data_dir = temp_dir.path().join("data");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                &data_dir,
                std::fs::Permissions::from_mode(0o555), // read+execute, no write
            )
            .unwrap();
        }

        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // maintenance_step should fail because it cannot create the tmp directory
        let result = engine.maintenance_step(Duration::from_secs(60));

        // Restore permissions before asserting (so TempDir can clean up)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        assert!(
            result.is_err(),
            "maintenance_step should return an error when the tmp dir cannot be created"
        );

        // All input files must still exist (atomicity guarantee)
        for p in &input_paths {
            assert!(
                p.exists(),
                "Input file {:?} must remain intact after failed compaction",
                p
            );
        }

        // Stats must NOT have incremented (no successful compaction)
        let stats = engine.maintenance_stats();
        assert_eq!(
            stats.compactions_completed, 0,
            "compactions_completed must not increment on failure"
        );
    }

    #[test]
    fn test_no_tmp_dir_remains_after_successful_merge() {
        // After a successful compaction, the .compaction-tmp-* directory must be cleaned up.
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();
        flush_n_sstables_sync(&mut engine, 4);

        engine.set_merge_policy(Box::new(policy)).unwrap();
        engine.maintenance_step(Duration::from_secs(60)).unwrap();

        // Scan data_dir for any leftover .compaction-tmp-* directories
        let data_dir = temp_dir.path().join("data");
        let leftover_tmp: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".compaction-tmp-")
            })
            .collect();

        assert!(
            leftover_tmp.is_empty(),
            "No .compaction-tmp-* directories should remain after successful compaction, \
             found: {:?}",
            leftover_tmp.iter().map(|e| e.path()).collect::<Vec<_>>()
        );
    }

    /// Issue #1619 WIRING EVIDENCE: `WriteEngine::new` must install a default
    /// STCS policy so `maintenance_step` compacts WITHOUT any `set_merge_policy`
    /// call. This test uses ONLY the public constructor — that is the whole
    /// point of the fix (STCS on by default). Before the fix `merge_policy` was
    /// hard-coded to `None`, so `rows_merged == 0` and no L0 reduction occurred.
    #[test]
    fn test_maintenance_step_default_policy_compacts_via_public_ctor() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Default config: auto_compaction = true (no set_merge_policy call).
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Flush 4 distinct L0 SSTables (>= min_threshold = 4). The tiny test
        // files are all below DEFAULT_MIN_SSTABLE_SIZE, so STCS groups them via
        // the "both small" rule into one eligible bucket.
        let input_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(input_paths.len(), 4, "Expected 4 flushed SSTables");

        let before = engine.scan_sstable_candidates().unwrap().len();
        assert_eq!(before, 4, "Expected 4 L0 SSTables before compaction");

        // NO set_merge_policy call — the public ctor must have wired STCS.
        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();

        assert!(
            report.rows_merged > 0,
            "default STCS policy must merge rows via the public ctor (rows_merged = {})",
            report.rows_merged
        );

        let after = engine.scan_sstable_candidates().unwrap().len();
        assert!(
            after < before,
            "on-disk L0 SSTable count must drop after compaction (before = {}, after = {})",
            before,
            after
        );
    }

    /// Issue #1619 OFF-SWITCH: with `auto_compaction = false`, `WriteEngine::new`
    /// installs NO policy, so `maintenance_step` is a no-op even with enough L0
    /// SSTables to trigger a compaction. Proves the documented off-switch works.
    #[test]
    fn test_maintenance_step_auto_compaction_disabled_is_noop() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        config.auto_compaction = false;

        let mut engine = WriteEngine::new(config).unwrap();

        let input_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(input_paths.len(), 4, "Expected 4 flushed SSTables");

        let before = engine.scan_sstable_candidates().unwrap().len();
        assert_eq!(before, 4, "Expected 4 L0 SSTables before compaction");

        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();

        assert_eq!(
            report.rows_merged, 0,
            "off-switch: no policy means no rows merged"
        );
        assert!(
            !report.pending_compaction,
            "off-switch: no policy means no pending compaction"
        );

        let after = engine.scan_sstable_candidates().unwrap().len();
        assert_eq!(
            after, before,
            "off-switch: L0 SSTable count must be unchanged (before = {}, after = {})",
            before, after
        );
    }

    /// Issue #1619 AH1: `Config.storage.compaction` must be non-decorative.
    /// A `CompactionConfig` with `auto_compaction = false` mapped onto the
    /// WriteEngineConfig must disable the default policy end-to-end (no rows
    /// merged, no L0 reduction) — proving the config wiring reaches behavior.
    #[test]
    fn test_compaction_config_disables_default_policy() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let compaction = crate::config::CompactionConfig {
            auto_compaction: false,
        };
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_compaction_config(&compaction);
        assert!(
            !config.auto_compaction,
            "config mapping must disable compaction"
        );

        let mut engine = WriteEngine::new(config).unwrap();
        flush_n_sstables_sync(&mut engine, 4);
        let before = engine.scan_sstable_candidates().unwrap().len();

        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
        assert_eq!(report.rows_merged, 0, "disabled config: no rows merged");

        let after = engine.scan_sstable_candidates().unwrap().len();
        assert_eq!(after, before, "disabled config: L0 count unchanged");
    }

    // Startup orphan-sweep coverage lives in `write_engine::sweep` (issue #1393),
    // which owns the sweep implementation and its thorough acceptance tests
    // (true-orphan removal, never-delete-live-data, non-fatal surfaced failures,
    // idempotence, and the crash-mid-compaction e2e).
}
