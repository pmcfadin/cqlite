//! `KWayMerger`'s per-partition reconcile and partition-floor shadowing (split out of
//! `merge/mod.rs`, #4658).

use super::*;

#[cfg(feature = "write-support")]
impl<S: TraceSink> KWayMerger<S> {
    pub(super) fn merge_partition_rows(
        &mut self,
        rows: Vec<MergeEntry>,
    ) -> Result<Vec<MergeEntry>> {
        use std::collections::BTreeMap;

        // Tombstone-purge accounting (issue #1037). Accumulated at the ACTUAL
        // gc/overlap-safe purge decision points (not derived from an input-vs-
        // output entry-count diff, which both missed cell/complex purges and
        // mis-counted last-write-wins collapse as a purge). See the comment on
        // `COMPACTION_TOMBSTONES_PURGED` emission at the end of this method.
        let mut purges = PurgeCounts::default();

        // Overlap-safety gate for tombstone purging (#921 finding 1, #935):
        // decide BOTH the effective gc_grace cutoff and the overlap-aware
        // max-purgeable timestamp this cluster is reconciled with.
        //
        // - FULL compaction (`purge_safe == true`): no non-included overlapping
        //   SSTable exists, so the overlap bound is `+inf` (`i64::MAX`) — every
        //   gc-purgeable tombstone passes the overlap gate, exactly as #845.
        // - PARTIAL compaction with an overlap bound (#935): purge a tombstone
        //   only when its own deletion timestamp is STRICTLY LESS THAN the min
        //   write timestamp of every non-included overlapping SSTable, proving it
        //   shadows nothing outside the set.
        // - PARTIAL compaction without a bound (#921 default): retain every
        //   tombstone — collapse the cutoff to `None` (purge no-op).
        //
        // Issue #933 builds `clustered_rows` (and splits out range-tombstone
        // carriers) AFTER this gate, below.
        let (effective_gc_before, max_purgeable_timestamp) = self.effective_gc_settings();

        // Partition-scoped tombstone-carrier pre-scan (issue #1668, stage 1).
        // A standalone read-only first pass extracts the partition-level
        // markers — the range-tombstone carriers (#933) and the MAX
        // partition-deletion floor (#1072) — out of the buffered partition into
        // `PartitionCarriers`. This preserves the exact prior behavior:
        //
        //   * Issue #933: range-tombstone carriers are partition-level markers
        //     spanning a clustering range — NOT per-`(pk, ck)` rows — so routing
        //     them through `reconcile_cluster` (which collapses a cluster to ONE
        //     entry) would lose every range but one. Collected here, then
        //     canonicalized (below), then used to (a) shadow covered cells per
        //     cluster and (b) re-emit the survivors so the writer persists them.
        //   * Issue #1072: the synthetic partition-tombstone carriers yield the
        //     MAX `markedForDeleteAt` across ALL sources (with the winning mfda's
        //     localDeletionTime) — the partition's OUTERMOST shadow floor. The
        //     partition key is the same for every entry in this call
        //     (`merge_partition_rows` is per partition), so one floor covers all.
        //
        // Carriers are NOT pushed into `clustered_rows`; the buffering pass below
        // skips them and keeps only the per-`(pk, ck)` rows, exactly as before.
        let carriers::PartitionCarriers {
            mut range_tombstones,
            range_tombstone_run_indices,
            max_partition_deletion,
            partition_delete_key,
            partition_delete_run_index,
        } = carriers::scan_partition_carriers(&rows, S::ENABLED);

        // Carrier rows are split out before cluster reconciliation: emit their marker
        // observations here (their source generation is otherwise lost on rebuild).
        if S::ENABLED {
            if let Some(run_indices) = range_tombstone_run_indices.as_ref() {
                for ((_, range), run_index) in range_tombstones.iter().zip(run_indices.iter()) {
                    self.trace.tombstone(TombstoneRecord {
                        kind: TombstoneKind::Range,
                        run_index: *run_index,
                        clustering: None,
                        column: None,
                        cell_path: None,
                        deletion_time: range.deletion_time,
                        local_deletion_time: range.local_deletion_time,
                        range_start: Some(range.start.clone()),
                        range_end: Some(range.end.clone()),
                        droppable_at_now: effective_gc_before
                            .is_some_and(|gc| i64::from(range.local_deletion_time as u32) < gc),
                    });
                }
            }
            if let (Some((deletion_time, local_deletion_time)), Some(run_index)) =
                (max_partition_deletion, partition_delete_run_index)
            {
                self.trace.tombstone(TombstoneRecord {
                    kind: TombstoneKind::Partition,
                    run_index,
                    clustering: None,
                    column: None,
                    cell_path: None,
                    deletion_time,
                    local_deletion_time,
                    range_start: None,
                    range_end: None,
                    droppable_at_now: effective_gc_before
                        .is_some_and(|gc| i64::from(local_deletion_time as u32) < gc),
                });
            }
        }

        let original_range_tombstones = range_tombstone_run_indices.as_ref().map(|run_indices| {
            range_tombstones
                .iter()
                .zip(run_indices.iter())
                .map(|((key, range), run_index)| (key.clone(), range.clone(), *run_index))
                .collect::<Vec<_>>()
        });

        let mut clustered_rows: BTreeMap<Option<ClusteringKey>, Vec<MergeEntry>> = BTreeMap::new();

        // Row-count reconciliation (issue #2163): rows entering the reconcile
        // boundary. `rows` is moved by the loop below, so snapshot the count here.
        // Emitted once per merge alongside `rows_out` (`merged.len()`) so the
        // in/out delta is exactly the rows removed by reconciliation.
        let rows_in = rows.len() as u64;

        for row in rows {
            // Skip the partition-scoped carriers already captured above so only
            // per-`(pk, ck)` rows enter `clustered_rows` (mirrors the original
            // inline `continue`s exactly: partition-deletion carrier first, then
            // range-marker carrier).
            if row.is_partition_delete_carrier() && row.partition_deletion.is_some() {
                continue;
            }
            if carriers::is_range_marker_carrier(&row) {
                continue;
            }
            clustered_rows
                .entry(row.clustering_key.clone())
                .or_default()
                .push(row);
        }

        // Coalesce the cross-SSTable union into a NON-OVERLAPPING canonical
        // sequence per partition (newest deletion wins per covered segment). The
        // writer emits each range as an independent open/close marker pair and the
        // reader pairs them with a single pending-start, so overlapping re-emitted
        // ranges would corrupt the persisted ranges on read-back (roborev #959
        // High #1). This also subsumes the old identical-bounds dedup.
        Self::coalesce_range_tombstones(&mut range_tombstones, &self.schema);

        // Keep the source generation aligned with the canonical ranges. Exact
        // matches cover the common case; the containment fallback preserves
        // provenance when coalescing split an overlapping range into segments.
        let range_source_runs = original_range_tombstones.as_ref().map(|original| {
            range_tombstones
                .iter()
                .map(|(key, canonical)| {
                    original
                        .iter()
                        .find(|(source_key, source, _)| {
                            source_key.key == key.key && source == canonical
                        })
                        .or_else(|| {
                            original.iter().find(|(source_key, source, _)| {
                                source_key.key == key.key
                                    && source.deletion_time == canonical.deletion_time
                                    && source.local_deletion_time == canonical.local_deletion_time
                                    && Self::range_tombstone_contains_range(
                                        source,
                                        canonical,
                                        &self.schema,
                                    )
                            })
                        })
                        .map(|(_, _, run_index)| *run_index)
                })
                .collect::<Vec<_>>()
        });

        let mut merged = Vec::new();
        for (ck, cluster_rows) in clustered_rows {
            let (reconciled, trace_metadata) =
                Self::reconcile_cluster_with_overlap_counted_traced_with_metadata(
                    ck,
                    cluster_rows,
                    &self.schema.dropped_columns,
                    effective_gc_before,
                    max_purgeable_timestamp,
                    // #1382: TTL expiry uses the merger's pinned evaluation instant,
                    // the SAME `now` that drove `compute_gc_before` so expiry and
                    // gc-grace purging agree deterministically.
                    self.now_secs,
                    &mut purges,
                    &mut self.trace,
                );
            if let Some(entry) = reconciled {
                // Issue #933: shadow cells covered by a range tombstone. The merge
                // must do this per-cell (not just per-row at the writer) because a
                // reconciled row's simple cells lose their individual writetimes
                // when converted to a mutation — only the merge still sees each
                // `CellData.timestamp`.
                if let Some(shadowed) = Self::apply_range_shadowing_traced(
                    entry,
                    &range_tombstones,
                    &self.schema,
                    trace_metadata.winner_runs.as_ref(),
                    range_source_runs.as_deref(),
                    effective_gc_before,
                    &mut self.trace,
                ) {
                    // Issue #1072: apply the partition deletion as the OUTERMOST
                    // floor — after range shadowing — dropping every cell/row
                    // whose timestamp is `<= pmfda` (per-cell `<=`, so strictly-
                    // newer cells survive). A whole-row covered by the partition
                    // floor contributes nothing (the re-emitted partition
                    // tombstone covers it).
                    if let Some(survivor) = Self::apply_partition_shadowing_traced(
                        shadowed,
                        max_partition_deletion,
                        trace_metadata.winner_runs.as_ref(),
                        partition_delete_run_index,
                        effective_gc_before,
                        &mut self.trace,
                    ) {
                        reconcile_cluster::trace_entry_winners(
                            &survivor,
                            trace_metadata.winner_runs.as_ref(),
                            &mut self.trace,
                        );
                        merged.push(survivor);
                    }
                }
            }
        }

        // Issue #1072: a range tombstone older-or-equal to the partition floor is
        // subsumed by the partition deletion and must not be re-emitted (it would
        // be redundant). A strictly-newer range marker survives. Apply BEFORE the
        // range re-emit loop below.
        if let Some((pmfda, _)) = max_partition_deletion {
            range_tombstones.retain(|(_, rt)| rt.deletion_time > pmfda);
        }

        // Re-emit the surviving range tombstones as carrier entries so the writer
        // persists the markers (otherwise the shadowing above would resurrect the
        // covered rows from a non-compacted SSTable — issue #933 / roborev #959
        // High #2). gc_grace purges a marker only when BOTH conditions hold,
        // matching the cell/row/complex purge paths (issue #1061):
        //   1. the marker's `localDeletionTime` is strictly below `gcBefore`
        //      (gc-grace expired — coordinate with #845), AND
        //   2. the marker's own deletion timestamp is strictly below
        //      `max_purgeable_timestamp` (the min write timestamp of every
        //      non-included overlapping SSTable — #935), proving it shadows
        //      nothing outside the compaction set.
        // For a full / overlap-safe compaction `max_purgeable_timestamp` is
        // `i64::MAX`, which lets every gc-purgeable marker through unchanged for
        // any realistic deletion timestamp (the same strict-`<`-against-`i64::MAX`
        // convention the cell/row/complex gates use above). For a PARTIAL
        // compaction with a finite overlap bound,
        // condition 2 RETAINS a marker at/above the bound — without it the
        // marker could be dropped while still shadowing data in a non-included
        // SSTable, resurrecting that data (issue #1061).
        for (key, rt) in range_tombstones {
            if let Some(gc_before) = effective_gc_before {
                if i64::from(rt.local_deletion_time as u32) < gc_before
                    && rt.deletion_time < max_purgeable_timestamp
                {
                    // Count the range-tombstone marker purge (issue #1037). The
                    // DROP is now overlap-safe: gated on gc_before AND the
                    // overlap bound, like the other tombstone categories (#1061).
                    purges.range_tombstones += 1;
                    continue;
                }
            }
            // Retained (non-purged) range-tombstone marker carried into the output
            // (issue #2163): a tombstone marker emitted, not purged.
            purges.emitted += 1;
            merged.push(
                MergeEntry::new(
                    usize::MAX,
                    key,
                    None,
                    rt.deletion_time,
                    RowData::Live { cells: Vec::new() },
                )
                .with_range_deletion(rt),
            );
        }

        // Issue #1072: re-emit the surviving partition tombstone so even a
        // tombstone-only partition (no rows in any source) still emits a partition
        // deletion — and the deletion keeps shadowing older rows in non-compacted
        // SSTables. gc_grace purges the marker only when BOTH conditions hold,
        // IDENTICAL to the range-marker re-emit gate / #1061 idiom:
        //   1. the partition's `localDeletionTime` is strictly below `gcBefore`
        //      (gc-grace expired), AND
        //   2. its own `markedForDeleteAt` is strictly below
        //      `max_purgeable_timestamp` (the min write timestamp of every
        //      non-included overlapping SSTable — #935), proving it shadows
        //      nothing outside the compaction set.
        // For a full / overlap-safe compaction `max_purgeable_timestamp` is
        // `i64::MAX` so any realistic deletion passes condition 2. For a PARTIAL
        // compaction without an overlap bound `max_purgeable_timestamp` is
        // `i64::MIN`, so condition 2 never fires and the tombstone is always
        // retained (never resurrect).
        if let (Some((pmfda, pldt)), Some(key)) = (max_partition_deletion, partition_delete_key) {
            let purge = match effective_gc_before {
                Some(gc_before) => {
                    i64::from(pldt as u32) < gc_before && pmfda < max_purgeable_timestamp
                }
                None => false,
            };
            if purge {
                purges.partition_tombstones += 1;
            } else {
                // Retained partition tombstone marker carried into the output
                // (issue #2163): a tombstone marker emitted, not purged.
                purges.emitted += 1;
                merged.push(
                    MergeEntry::new(
                        usize::MAX,
                        key,
                        None,
                        pmfda,
                        RowData::Tombstone {
                            deletion_time: pmfda,
                            local_deletion_time: pldt,
                        },
                    )
                    .with_partition_deletion((pmfda, pldt)),
                );
            }
        }

        // Sort merged rows by clustering key for output order
        merged.sort_by(|a, b| match (&a.clustering_key, &b.clustering_key) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(ck_a), Some(ck_b)) => {
                // Use schema-aware comparison if available
                ck_a.compare(ck_b, &self.schema).unwrap_or_else(|e| {
                    tracing::warn!(
                        "Schema-aware clustering key comparison failed, using fallback: {}",
                        e
                    );
                    ck_a.cmp(ck_b)
                })
            }
        });

        // Emit the count of tombstones GENUINELY PURGED during this merge (issue
        // #1037).
        //
        // `COMPACTION_TOMBSTONES_PURGED` now counts ONLY true gc_grace /
        // overlap-safe purges, accumulated at the exact decision points where a
        // tombstone is dropped because it is safe to drop:
        //   - cell tombstones (simple + complex-element) removed by the Step 3c
        //     gc/overlap retain in `reconcile_cluster_with_overlap_counted`;
        //   - row tombstones cleared by that same gc/overlap gate;
        //   - complex-deletion markers removed by that gate;
        //   - range-tombstone markers dropped by the gc-grace gate in the
        //     re-emit loop above.
        // It explicitly does NOT count last-write-wins reconciliation collapse
        // (e.g. two duplicate row tombstones merging to one), which is ordinary
        // deduplication, not a purge. When no gc cutoff applies (partial
        // compaction without an overlap bound) the purge stages are no-ops and
        // this counter stays at zero.
        let purged = purges.total();
        if purged > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_PURGED,
                purged,
                &[],
            );
        }

        // Correctness / silent-miss signals (issue #2163), emitted once per merge
        // alongside the purge total — never per row/cell. `suppressed` (live data
        // shadowed by a tombstone) and `emitted` (tombstone markers retained) move
        // independently of `tombstones_purged`; the row-count pair lets a dashboard
        // see reconciliation drop ratio + volume.
        if purges.suppressed > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_SUPPRESSED,
                purges.suppressed,
                &[],
            );
        }
        if purges.emitted > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_EMITTED,
                purges.emitted,
                &[],
            );
        }
        if rows_in > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::MERGE_ROWS_IN,
                rows_in,
                &[],
            );
            crate::observability::add_counter(
                crate::observability::catalog::MERGE_ROWS_OUT,
                merged.len() as u64,
                &[],
            );
        }

        Ok(merged)
    }

    /// Shadow the cells / row / metadata of a reconciled cluster entry that are
    /// covered by the partition-level deletion (issue #1072 — the OUTERMOST floor).
    ///
    /// Drops every DATA cell whose own `timestamp` is `<= pmfda` (the `<=` lets the
    /// partition deletion win an equal-ts tie, like #498/#933). Clustering-key
    /// pseudo-cells are retained whenever any data cell survives so the row keeps
    /// its key columns for read-back. A row whose every data cell is shadowed AND
    /// whose row-marker liveness is `<= pmfda` (judged on the MARKER's own
    /// timestamp, #3094) produces nothing. A row tombstone / coexisting row deletion /
    /// complex deletion older-or-equal to the floor is subsumed; a strictly-newer
    /// one survives. `None` floor (`max_partition_deletion == None`) is the common
    /// case and returns the entry untouched.
    pub(super) fn apply_partition_shadowing(
        entry: MergeEntry,
        max_partition_deletion: Option<(i64, i32)>,
    ) -> Option<MergeEntry> {
        let mut sink = NoTrace;
        // `gc_before_secs: None` is inert here — `NoTrace::ENABLED` is
        // `false`, so the `T::ENABLED` guard below skips the trace call
        // (and hence the `droppable_at_now` computation) entirely.
        Self::apply_partition_shadowing_traced(
            entry,
            max_partition_deletion,
            None,
            None,
            None,
            &mut sink,
        )
    }

    /// Trace-enabled partition-floor shadowing.  The no-trace wrapper above is
    /// retained for the existing streaming and unit-test callers.
    #[allow(clippy::too_many_arguments)]
    fn apply_partition_shadowing_traced<T: TraceSink>(
        entry: MergeEntry,
        max_partition_deletion: Option<(i64, i32)>,
        winner_runs: Option<&std::collections::HashMap<reconcile::CellKey, usize>>,
        partition_source_run: Option<usize>,
        gc_before_secs: Option<i64>,
        trace: &mut T,
    ) -> Option<MergeEntry> {
        let Some((pmfda, partition_ldt)) = max_partition_deletion else {
            return Some(entry);
        };

        // Capture the cell versions that the partition floor removes before
        // consuming `entry.row_data` below.  The carrier's source generation
        // is not present in this reconciled entry, so the current entry's
        // The reconciled row has no carrier field, so use the source generation
        // captured by the partition pre-scan for the deciding marker.
        if T::ENABLED {
            if let RowData::Live { cells } = &entry.row_data {
                let ck_names: std::collections::HashSet<&str> = entry
                    .clustering_key
                    .as_ref()
                    .map(|ck| ck.columns.iter().map(|(name, _)| name.as_str()).collect())
                    .unwrap_or_default();
                let shadowed: Vec<CellData> = cells
                    .iter()
                    .filter(|cell| {
                        !ck_names.contains(cell.column.as_str()) && cell.timestamp <= pmfda
                    })
                    .cloned()
                    .collect();
                if !shadowed.is_empty() {
                    reconcile_cluster::trace_entry_shadowed(
                        &entry,
                        &shadowed,
                        winner_runs,
                        trace,
                        TombstoneKind::Partition,
                        partition_source_run.unwrap_or(entry.run_index),
                        pmfda,
                        partition_ldt,
                        reconcile_cluster::droppable_at_now(gc_before_secs, partition_ldt),
                    );
                }
            }
        }

        // A complex (collection) deletion older-or-equal to the partition floor is
        // subsumed; one strictly newer must survive.
        let surviving_complex: Vec<ComplexDeletion> = entry
            .complex_deletions
            .into_iter()
            .filter(|cd| cd.marked_for_delete_at > pmfda)
            .collect();

        // A coexisting row deletion (#932) older-or-equal to the floor is subsumed;
        // a newer one is preserved.
        let surviving_row_del = entry.row_deletion.filter(|(dt, _)| *dt > pmfda);

        match entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                // A row tombstone older-or-equal to the partition floor is subsumed
                // by the partition deletion; a strictly-newer one survives.
                if deletion_time > pmfda {
                    let mut rebuilt = MergeEntry::new(
                        entry.run_index,
                        entry.key,
                        entry.clustering_key,
                        deletion_time,
                        RowData::Tombstone {
                            deletion_time,
                            local_deletion_time,
                        },
                    );
                    if !surviving_complex.is_empty() {
                        rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                    }
                    Some(rebuilt)
                } else if !surviving_complex.is_empty() {
                    Some(
                        MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            entry.clustering_key,
                            entry.timestamp,
                            RowData::Live { cells: Vec::new() },
                        )
                        .with_complex_deletions(surviving_complex),
                    )
                } else {
                    None
                }
            }
            RowData::Live { cells } => {
                // Identify clustering pseudo-cells (kept when any data cell
                // survives). A static / unclustered row has no clustering key.
                let ck_names: std::collections::HashSet<String> = entry
                    .clustering_key
                    .as_ref()
                    .map(|ck| ck.columns.iter().map(|(n, _)| n.clone()).collect())
                    .unwrap_or_default();
                let is_data = |c: &CellData| !ck_names.contains(&c.column);

                let kept: Vec<CellData> = cells
                    .into_iter()
                    .filter(|c| !is_data(c) || c.timestamp > pmfda)
                    .collect();
                let has_data = kept.iter().any(is_data);

                // #3094: judged on the MARKER's OWN ts — see `marker_survives_floor`.
                let liveness = entry.row_liveness;
                let marker_live = liveness.marker_survives_floor(entry.timestamp, pmfda);

                if !has_data && !marker_live {
                    if let Some((dt, ldt)) = surviving_row_del {
                        return Some(MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            entry.clustering_key,
                            dt,
                            RowData::Tombstone {
                                deletion_time: dt,
                                local_deletion_time: ldt,
                            },
                        ));
                    }
                    if !surviving_complex.is_empty() {
                        return Some(
                            MergeEntry::new(
                                entry.run_index,
                                entry.key,
                                entry.clustering_key,
                                entry.timestamp,
                                RowData::Live { cells: Vec::new() },
                            )
                            .with_complex_deletions(surviving_complex),
                        );
                    }
                    return None;
                }

                let row_ts = if has_data {
                    kept.iter()
                        .filter(|c| is_data(c))
                        .map(|c| c.timestamp)
                        .max()
                        .unwrap_or(entry.timestamp)
                } else {
                    entry.timestamp
                };

                let mut rebuilt = MergeEntry::new(
                    entry.run_index,
                    entry.key,
                    entry.clustering_key,
                    row_ts,
                    RowData::Live { cells: kept },
                );
                // Issue #2374/#2789: carry the primary-key liveness marker forward
                // when it survives the partition floor, so a key-only live row
                // (INSERT with no/all-null regular columns) that coexists with an
                // older covering partition tombstone stays VISIBLE through the read
                // path. Dropped (default) when the marker did not survive the floor.
                rebuilt = rebuilt.with_row_liveness(if marker_live {
                    entry.row_liveness
                } else {
                    Default::default()
                });
                if !surviving_complex.is_empty() {
                    rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                }
                if let Some((dt, ldt)) = surviving_row_del {
                    rebuilt = rebuilt.with_row_deletion(dt, ldt);
                }
                Some(rebuilt)
            }
        }
    }
}
