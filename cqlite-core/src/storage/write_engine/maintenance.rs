//! SSTable maintenance and STCS compaction for the write engine.
//!
//! Extracted verbatim from `write_engine/mod.rs` (issue #1120, epic #1116) as a
//! behavior-preserving split. Owns the incremental K-way merge state machine
//! (`maintenance_step`), candidate scanning, startup orphan sweeps, atomic
//! input deletion, and the public `MaintenanceReport` type. `WriteEngine`'s
//! fields are reachable here because this is a sibling module in the same crate.

use super::merge;
use super::{CompactionStats, KWayMerger, MergePolicy, WriteEngine};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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
    /// - At least one partition is processed per call (minimum progress guarantee)
    /// - Merge state is preserved across calls for resumption
    ///
    /// ## Budget Enforcement
    ///
    /// The budget is honored within approximately 10% tolerance. This tolerance
    /// exists to avoid interrupting partition processing mid-stream, which would
    /// require complex state management to resume. The tolerance ensures forward
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
    #[tracing::instrument(name = "compaction.maintenance_step", skip(self))]
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

        // Process active merge within budget
        let budget_tolerance = budget.mul_f32(1.1); // 10% tolerance
        let mut partitions_processed = 0;

        while let Some(merge) = &mut self.active_merge {
            // Check budget (but always process at least one partition)
            if partitions_processed > 0 && start.elapsed() >= budget_tolerance {
                break;
            }

            // Process one partition from the merge
            let step = merge.merger.step()?;

            match step {
                merge::MergeStep::Partition { key, rows } => {
                    partitions_processed += 1;

                    // Convert MergeEntry rows to Mutation format
                    // (collect into a vec first to release the borrow on merge)
                    let entries_vec: Vec<_> = rows.into_iter().collect();

                    // Now we can call self methods without conflict.
                    // Skip metadata-only entries (#886/#899 branch-review): they
                    // carry complex/range deletion metadata through the merge
                    // stream but have no writer-emittable content yet, so writing
                    // them would produce a phantom live empty (pure-PK) row at
                    // timestamp 0. See `MergeEntry::is_metadata_only_no_op`.
                    // #850: convert with the effective compaction schema so any
                    // static column re-added from the input headers is preserved
                    // (partition-key decoding is identical; only static columns
                    // differ). Falls back to the config schema if (impossibly) no
                    // active merge is present.
                    let conversion_schema = self
                        .active_merge
                        .as_ref()
                        .map(|m| m.effective_schema.clone())
                        .unwrap_or_else(|| self.config.schema.clone());
                    let mutations = entries_vec
                        .into_iter()
                        .filter(|entry| !entry.is_metadata_only_no_op())
                        .map(|entry| {
                            merge::KWayMerger::merge_entry_to_mutation(entry, &conversion_schema)
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // If every merged row was metadata-only, the partition has no
                    // writer-emittable content. Skip `write_partition` to avoid a
                    // phantom EMPTY partition (header/end marker + Index/Filter/
                    // Summary/statistics registration) in the output SSTable, and
                    // do not count it as an output partition or row (#886
                    // branch-review).
                    if mutations.is_empty() {
                        continue;
                    }

                    // Count rows actually written (skipped metadata-only entries
                    // produce no row, so they must not inflate the stats). A pure
                    // range-tombstone carrier (#933) or a pure partition-tombstone
                    // carrier (#1072) emits a marker / partition-header deletion,
                    // not a row — exclude them, matching KWayMerger::merge.
                    let row_count = mutations
                        .iter()
                        .filter(|m| {
                            let is_range_only = m.operations.is_empty()
                                && m.partition_tombstone.is_none()
                                && m.row_tombstone.is_none()
                                && !m.range_tombstones.is_empty();
                            let is_partition_only = m.operations.is_empty()
                                && m.partition_tombstone.is_some()
                                && m.row_tombstone.is_none()
                                && m.range_tombstones.is_empty();
                            !(is_range_only || is_partition_only)
                        })
                        .count() as u64;

                    // Write partition to output SSTable
                    // Re-borrow active_merge to write
                    if let Some(merge) = &mut self.active_merge {
                        merge.writer.write_partition(key, mutations)?;
                        merge.rows_merged += row_count;
                    }

                    // Update stats
                    report.rows_merged += row_count;
                }
                merge::MergeStep::Complete => {
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

    #[tracing::instrument(name = "compaction.scan_candidates", skip(self))]
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
                    log::debug!(
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
                    Ok(()) => log::debug!("Deleted compaction input: {:?}", component_path),
                    Err(e) => {
                        // Best-effort: do not abort. A leftover data component
                        // whose TOC.txt is already gone is an invisible orphan
                        // reclaimed by the startup sweep (Issue #591).
                        log::warn!(
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
        // Without a merge policy, maintenance_step should do nothing
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

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

    // Startup orphan-sweep coverage lives in `write_engine::sweep` (issue #1393),
    // which owns the sweep implementation and its thorough acceptance tests
    // (true-orphan removal, never-delete-live-data, non-fatal surfaced failures,
    // idempotence, and the crash-mid-compaction e2e).
}
