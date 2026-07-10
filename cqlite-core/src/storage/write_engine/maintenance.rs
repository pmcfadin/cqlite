//! SSTable maintenance and STCS compaction for the write engine.
//!
//! Extracted verbatim from `write_engine/mod.rs` (issue #1120, epic #1116) as a
//! behavior-preserving split. Owns the incremental K-way merge state machine
//! (`maintenance_step`), candidate scanning, startup orphan sweeps, atomic
//! input deletion, and the public `MaintenanceReport` type. `WriteEngine`'s
//! fields are reachable here because this is a sibling module in the same crate.

use super::merge;
// `Mutation` is named in production again (issue #1383 fix): a partition's
// surviving clustering rows are BUFFERED as `Vec<Mutation>` in
// `PartitionStreamState` and written in one session at partition end, once
// the full range-tombstone set is known — see that type's doc.
use super::mutation::{DecoratedKey, Mutation, PartitionTombstone, RangeTombstone};
use super::{CompactionStats, KWayMerger, MergePolicy, WriteEngine};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::writer::data_writer::StaticOpsTracker;
use crate::storage::sstable::writer::stats_fold;
use crate::storage::sstable::writer::StatisticsMetadata;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Per-partition streaming state (issue #1668, stage 5c-iv part 3; buffering
/// reworked for the issue #1383 range-tombstone-boundary fix) — mirrors
/// `KWayMerger::merge()`'s per-partition loop state exactly.
///
/// A bounded `clustering_key: None` prefix (partition/range-tombstone
/// carriers, static-row carrier) is accumulated as it arrives; every
/// surviving `Some(ck)` clustering row is BUFFERED into `buffered_rows`
/// rather than fed to a writer session as it arrives. The
/// [`StreamingPartitionSession`] is opened only at `StreamingStep::PartitionEnd`
/// — once the partition's COMPLETE, coalesced range-tombstone set is known —
/// and every buffered row is fed through it in one pass, then finished.
///
/// This is required for correctness (issue #1383): a range tombstone's
/// coalesced marker is only surfaced by `StreamingMerger` once its CLOSE
/// bound is parsed, which — for a range covering rows in a different (e.g.
/// newer) generation — is strictly AFTER those rows have already streamed
/// (issue #1668 stage 5d's "range tombstones can arrive AFTER the rows they
/// cover" finding). Opening the session on the first `Some(ck)` row therefore
/// fixed an often-EMPTY range-tombstone set, and every late range-only marker
/// mutation was then dropped as a `feed_streaming_row` no-op — silently
/// losing the shadowing/boundary. Buffering until the full set is known lets
/// `feed_streaming_row` shadow every covered row and interleave the markers
/// correctly, exactly as `KWayMerger::merge()`'s `buffered_rows` does for the
/// direct-call path.
///
/// The whole struct (including `buffered_rows`) owns everything it needs, so
/// it can be stashed on [`ActiveMerge::pending_partition`] and survive a
/// mid-partition budget pause between `maintenance_step_inner` calls: the
/// budget check still fires between cluster groups (each iteration buffers one
/// group cheaply, then may pause), so a fat partition still yields control —
/// only the WRITE is deferred to partition end. The buffer is
/// whole-partition-width for range-tombstone-bearing partitions (a genuinely
/// bounded-memory streaming write there needs the out-of-scope two-pass
/// reader; the reader already materializes the decompressed section anyway),
/// while `StreamingMerger`'s own reconciliation stays row-streamed — the
/// memory bound the dhat proof actually exercises.
pub(crate) struct PartitionStreamState {
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
    static_tracker: StaticOpsTracker,
    static_first_ts: i64,
    saw_carrier_or_static: bool,
    /// This partition's stats fold accumulated so far. Threaded through the
    /// state rather than folded straight into the writer's own `stats`
    /// because the writer is borrowed per-call only at partition end.
    partition_stats: StatisticsMetadata,
    /// Mirrors `KWayMerger::merge()`'s loop-local `row_count`: incremented
    /// once per buffered clustering row AND once per static-row carrier (not
    /// for pure partition/range-tombstone carriers — issue #1668 stage 5c-iv
    /// part 2's row-count parity finding), so it equals the number of
    /// emittable rows regardless of pauses.
    row_count: u64,
    /// Surviving `Some(ck)` clustering-row mutations, buffered in arrival
    /// (clustering) order and written in one session at partition end (see
    /// the type doc for why buffering is required).
    buffered_rows: Vec<Mutation>,
}

impl PartitionStreamState {
    /// A fresh, empty accumulator — the starting state for any new partition
    /// (never resumed from a pause).
    fn fresh() -> Self {
        PartitionStreamState {
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            static_tracker: StaticOpsTracker::new(),
            static_first_ts: 0,
            saw_carrier_or_static: false,
            partition_stats: StatisticsMetadata::new(),
            row_count: 0,
            buffered_rows: Vec::new(),
        }
    }

    /// This partition's stats-fold accumulator — every mutation (carrier,
    /// static, or clustering row) folds through the SAME chokepoint before
    /// classification, mirroring `KWayMerger::merge()`'s single fold point
    /// (issue #1668 stage 5c-iv part 2).
    fn partition_stats_mut(&mut self) -> &mut StatisticsMetadata {
        &mut self.partition_stats
    }
}

// `StaticOpsTracker`/`StreamingPartitionSession` do not implement `Debug`
// (the former holds a non-`Debug` cell-value map; the latter is a plain
// bookkeeping struct never printed in production), so `ActiveMerge`'s
// `#[derive(Debug)]` (used only incidentally — no code actually formats an
// `ActiveMerge`, confirmed by grep) needs a hand-written `Debug` here rather
// than pulling `Debug` onto `StaticOpsTracker` just to satisfy a derive
// nothing exercises.
impl std::fmt::Debug for PartitionStreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStreamState")
            .field("saw_carrier_or_static", &self.saw_carrier_or_static)
            .field("row_count", &self.row_count)
            .field("buffered_rows", &self.buffered_rows.len())
            .finish_non_exhaustive()
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
    ///   partition's raw-entry reconciliation state (carrier accumulator,
    ///   in-progress cluster, already-reconciled-but-not-yet-popped rows —
    ///   issue #1668 stage 5d widened this from a plain
    ///   `VecDeque<MergeEntry>` once `StreamingMerger` stopped buffering a
    ///   whole partition via `KWayMerger::step`). Extracted via
    ///   `StreamingMerger::into_paused_state`, restored via
    ///   `StreamingMerger::resume` — nothing here is re-computed by a
    ///   resumed call, and nothing is lost. Held OPAQUELY: this file never
    ///   inspects a field of it.
    /// - `.2` this partition's [`PartitionStreamState`] (issue #1668, stage
    ///   5c-iv part 3; buffering reworked for the issue #1383 fix): the
    ///   accumulated partition tombstone / range-tombstone set / static row
    ///   plus the buffered surviving clustering rows seen so far. The writer
    ///   session is opened only at `PartitionEnd` (once the full
    ///   range-tombstone set is known — see [`PartitionStreamState`]'s doc),
    ///   so a paused partition stashes plain owned accumulators here.
    ///   Resuming is just "keep buffering into this same value" — no
    ///   re-computation, no lost rows. The buffer is whole-partition-width
    ///   only for range-tombstone-bearing partitions (the documented
    ///   residual); the budget pause still fires between cluster groups, so a
    ///   fat partition still yields control mid-drain.
    pub(crate) pending_partition: Option<(
        DecoratedKey,
        merge::PartitionReconcileCheckpoint,
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
        // CLUSTER GROUPS, not only between whole partitions. When a partition
        // is too fat to finish within one call's remaining budget, the
        // accumulated `PartitionStreamState` (carrier/static prefix + the
        // buffered surviving clustering rows so far) is stashed on
        // `ActiveMerge.pending_partition` and resumed unchanged on the NEXT
        // `maintenance_step` call. The writer session is opened only at
        // `PartitionEnd`, once the partition's FULL range-tombstone set is
        // known (issue #1383 — see `PartitionStreamState`'s doc for why a late
        // range tombstone would otherwise drop a boundary), so the writer
        // still receives one partition as one on-disk unit (opened once,
        // finished once) and output stays byte-identical (see `#921`).
        // Nothing is re-computed across a pause, nothing is lost. The
        // minimum-progress guarantee remains "at least one cluster group per
        // call"; the budget still fires between cluster groups, so a fat
        // partition yields control mid-drain even though its WRITE is
        // deferred to the end.
        let budget_tolerance = budget.mul_f32(1.1); // 10% tolerance
        let mut partitions_processed = 0u64;

        /// Outcome of draining one partition's cluster groups, bounded by
        /// the mid-partition budget check.
        enum DrainOutcome {
            /// The partition fully drained; write the buffered
            /// `PartitionStreamState` now (open session, feed static + rows,
            /// finish). Boxed to keep `DrainOutcome` cheap to move regardless
            /// of `PartitionStreamState`'s size (clippy::large_enum_variant).
            Ready(DecoratedKey, Box<PartitionStreamState>),
            /// Budget exceeded mid-drain; progress was stashed on
            /// `ActiveMerge.pending_partition` for the next call.
            Paused,
            /// No more partitions in any run.
            MergeComplete,
        }

        while let Some(merge) = &mut self.active_merge {
            // Resume a partition paused by a PRIOR call's budget check, if
            // any (issue #1668, stage 4). `resume_state` is
            // `StreamingMerger`'s own raw-entry reconciliation checkpoint
            // (issue #1668 stage 5d); `stream_state` is this partition's
            // `PartitionStreamState` from EARLIER in this same partition
            // (this or a prior call) — the carrier/static prefix plus the
            // clustering rows buffered so far (no session is open yet; it
            // opens only at PartitionEnd, see that type's doc).
            let (resume_state, mut stream_state): (
                Option<(DecoratedKey, merge::PartitionReconcileCheckpoint)>,
                PartitionStreamState,
            ) = match merge.pending_partition.take() {
                Some((key, checkpoint, state)) => (Some((key, checkpoint)), state),
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
                    merge::StreamingStep::ClusterGroup { key: _, row } => {
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

                        // Classify the (None-keyed) carriers and the static
                        // row; everything else is a real clustering row that
                        // is BUFFERED (see `PartitionStreamState`'s doc). No
                        // writer session is opened here — a range tombstone
                        // can still arrive AFTER some rows are buffered (issue
                        // #1383), so the session opens only at PartitionEnd,
                        // once the full range-tombstone set is resolved.
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

                        if is_partition_only {
                            stream_state.partition_tombstone = mutation.partition_tombstone;
                            stream_state.saw_carrier_or_static = true;
                            continue;
                        }
                        if is_range_only {
                            stream_state
                                .range_tombstones
                                .extend(mutation.range_tombstones.iter().cloned());
                            stream_state.saw_carrier_or_static = true;
                            continue;
                        }
                        if is_static_carrier {
                            // Roborev blocker #2 (issue #1668): only the
                            // resolved static-row carrier increments
                            // `row_count` — a pure partition/range-tombstone
                            // carrier emits a marker/header deletion, not a
                            // row.
                            if !stream_state.saw_carrier_or_static {
                                stream_state.static_first_ts = mutation.timestamp_micros;
                            }
                            stream_state
                                .static_tracker
                                .feed(&mutation, &write_schema, None);
                            stream_state.saw_carrier_or_static = true;
                            stream_state.row_count += 1;
                            continue;
                        }

                        // A real clustering row (or an unclustered table's
                        // sole `clustering_key: None` row): buffer it for the
                        // single PartitionEnd write.
                        stream_state.buffered_rows.push(mutation);
                        stream_state.row_count += 1;
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
                    let state = *state;

                    // Truly empty partition (every entry was
                    // metadata-only-no-op, and no carrier/static survived) —
                    // skip entirely, matching `KWayMerger::merge`'s
                    // `buffered_rows.is_empty() && !saw_carrier_or_static`
                    // skip. A partition with only carriers/statics but no
                    // `Some(ck)` row is still emittable (#933/#1072).
                    if !state.buffered_rows.is_empty() || state.saw_carrier_or_static {
                        // Open the session NOW, with the partition tombstone,
                        // the COMPLETE coalesced range-tombstone set, the
                        // static row, and every buffered clustering row all
                        // known (issue #1383): `feed_streaming_row` shadows
                        // every covered row and interleaves the markers in
                        // clustering order.
                        if let Some(merge) = &mut self.active_merge {
                            let mut session = merge.writer.begin_streaming_partition(
                                &key,
                                state.partition_tombstone.as_ref(),
                                &state.range_tombstones,
                            )?;
                            if schema_has_static {
                                let merged = state.static_tracker.finish();
                                merge.writer.feed_streaming_static_row(
                                    &mut session,
                                    &merged,
                                    state.static_first_ts,
                                )?;
                            }
                            for mutation in &state.buffered_rows {
                                merge.writer.feed_streaming_row(&mut session, mutation)?;
                            }
                            let (offset, blocks, emit) =
                                merge.writer.finish_streaming_partition(session)?;
                            merge.writer.complete_partition_incremental(
                                &key,
                                state.partition_tombstone.as_ref(),
                                offset,
                                &blocks,
                                emit,
                                &state.partition_stats,
                            )?;
                            merge.rows_merged += state.row_count;
                        }
                        report.rows_merged += state.row_count;
                    }
                }
                DrainOutcome::Paused => {
                    // Stash progress for the NEXT maintenance_step call
                    // (issue #1668, stage 4). `into_paused_state` is `Some`
                    // whenever ANY row was popped for the in-progress
                    // partition — guaranteed here because `progressed_this_call`
                    // (or a non-empty `resume_state`, which also seeds
                    // `stream`'s partition key) must be true to reach `Paused`.
                    if let Some((key, checkpoint)) = stream.into_paused_state() {
                        if let Some(merge) = &mut self.active_merge {
                            merge.pending_partition = Some((key, checkpoint, stream_state));
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

    /// Roborev blocker #2 (issue #1668): `PartitionStreamState::Prefix`'s
    /// carrier-classification block must NOT count a pure partition-
    /// tombstone carrier toward `row_count` — mirroring
    /// `KWayMerger::merge`'s reference implementation exactly (only the
    /// resolved static-row carrier increments it). Before the fix, EVERY
    /// carrier classification (partition-only, range-only, static)
    /// incremented `row_count`, inflating `report.rows_merged` (and the
    /// `COMPACTION_ROWS_MERGED` observability counter) whenever a
    /// partition's carrier prefix included a pure tombstone.
    ///
    /// Fixture: one partition (`id = 1`) with a CLUSTERED schema (so a
    /// `clustering_key: None` carrier always sorts strictly before every
    /// `Some(ck)` row — `SchemaOrderedEntry`'s `(None, Some(_)) =>
    /// Ordering::Less`, unconditional on `run_index` — unlike an unclustered
    /// table where a real row ALSO carries `clustering_key: None` and could
    /// race the carrier on a `run_index` tie-break). Source 1 carries the
    /// partition tombstone alongside one real row (`ck=0`, postdating the
    /// tombstone, so it is NOT itself shadowed — this keeps source 1 non-
    /// empty, avoiding any confound from a truly zero-row source); source 2
    /// carries a second real row (`ck=1`, also postdating the tombstone).
    /// Compacting the two must merge to exactly 2 counted rows — the
    /// re-emitted tombstone carrier is a marker/header deletion, not a row.
    #[test]
    fn test_maintenance_step_does_not_count_partition_tombstone_carrier_as_a_row() {
        use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
        use crate::storage::write_engine::mutation::{
            CellOperation, ClusteringKey, PartitionKey, PartitionTombstone, TableId,
        };
        use crate::types::Value;

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
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let table_id = TableId::new(&schema.keyspace, &schema.table);
        let pk = PartitionKey::single("id", Value::Integer(1));
        let row_mutation = |ck: i32, ts: i64| {
            Mutation::new(
                table_id.clone(),
                pk.clone(),
                Some(ClusteringKey::single("ck", Value::Integer(ck))),
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(format!("row-{ck}")),
                }],
                ts,
                None,
            )
        };

        // `local_deletion_time` must be a REALISTIC (near-"now") GC-clock
        // timestamp, not a tiny literal — otherwise this full/overlap-safe
        // 2-source compaction correctly gc-grace-purges the tombstone
        // outright (it would be`local_deletion_time < gc_before`, decades
        // expired), never reaching the row-count classification this test
        // targets at all.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i32;

        // Source 1: the partition tombstone (deletion_time=100) alongside
        // ck=0 (ts=5_000, postdates the tombstone — survives).
        let mut ck0_with_tombstone = row_mutation(0, 5_000);
        ck0_with_tombstone.partition_tombstone = Some(PartitionTombstone {
            deletion_time: 100,
            local_deletion_time: now_secs,
        });
        engine.write(ck0_with_tombstone).unwrap();
        rt.block_on(engine.flush()).unwrap().unwrap();

        // Source 2: ck=1 (ts=6_000, also postdates the tombstone — survives).
        engine.write(row_mutation(1, 6_000)).unwrap();
        rt.block_on(engine.flush()).unwrap().unwrap();

        // `min_sstable_size` large enough that both tiny files count as
        // "small" and bucket together regardless of their size ratio.
        let policy =
            crate::storage::write_engine::STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // `MaintenanceReport::rows_merged` is per-CALL, not cumulative
        // (`test_maintenance_paused_and_unpaused_compaction_produce_byte_identical_output`'s
        // `total_rows_merged` pattern above) — a merge can finish its own
        // work in one call yet still report `pending_compaction: true` for
        // an unrelated reason, so sum across every call rather than trusting
        // only the last one.
        let mut report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
        let mut total_rows_merged = report.rows_merged;
        let mut calls = 1u32;
        while report.pending_compaction {
            report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
            total_rows_merged += report.rows_merged;
            calls += 1;
            assert!(calls < 100_000, "compaction never completed");
        }

        assert_eq!(
            total_rows_merged, 2,
            "only the 2 real surviving rows (ck=0, ck=1) must be counted — \
             the re-emitted partition-tombstone carrier is a marker, not a row"
        );
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
    /// resuming a partition's drain mid-stream — via the
    /// `PartitionStreamState` (carrier/static prefix + buffered clustering
    /// rows) stashed on `ActiveMerge::pending_partition` across many
    /// `maintenance_step` calls — must produce EXACTLY the same Data.db bytes
    /// as a single unbroken drain. Same rigor as every
    /// `IncrementalPartitionWriter`/`StreamingPartitionSession` byte-identity
    /// test in `data_writer/tests/`, but exercised through the REAL
    /// production entry point (`WriteEngine::maintenance_step`) rather than
    /// the session type directly — proving the INTEGRATION (budget checks,
    /// buffering/resume plumbing, the single PartitionEnd write) introduces no
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
