//! `KWayMerger`'s builder methods, the incremental `step`, and the heap refill (split out of
//! `merge/mod.rs`, #4658).

use super::*;

#[cfg(feature = "write-support")]
impl<S: TraceSink> KWayMerger<S> {
    /// Mark this merge as overlap-safe for tombstone purging (#921 finding 1).
    ///
    /// Set `true` ONLY when the compaction inputs provably span EVERY SSTable
    /// for the table (a major/full compaction), so no non-included overlapping
    /// SSTable can hold data shadowed by a purged tombstone. When `false` (the
    /// default) the gc_grace purge stage is a strict no-op — tombstones are
    /// retained — which can never resurrect data in a partial compaction.
    #[must_use]
    pub fn with_purge_safe(mut self, purge_safe: bool) -> Self {
        self.purge_safe = purge_safe;
        self
    }

    /// Set the read-time TTL evaluation instant (`now`, epoch seconds) for this
    /// merge (issue #2374/#2789), enabling `expire_ttl_cells` on a READ merge
    /// built through a `now`-less constructor (e.g. the warm
    /// [`new_from_readers`](KWayMerger::new_from_readers) path). `gc_before_secs`
    /// stays `None` so NO tombstone is gc-purged — a read reflects deletions, it
    /// does not collect them. A compaction WRITE path threads `now` through its
    /// constructor instead and never calls this.
    #[must_use]
    pub fn with_now_secs(mut self, now_secs: Option<i64>) -> Self {
        self.now_secs = now_secs;
        self
    }

    /// Set the gc_grace cutoff (`gcBefore`, GC-clock seconds) for this merge
    /// (issue #4193). Like [`Self::with_now_secs`], this lets a caller that
    /// built a merger through a gc-less constructor (a point-read builder, or
    /// [`KWayMerger::new_from_readers`]) configure it as a full compaction
    /// after the fact — the `explain` surface's only production caller drives
    /// a traced point-read merger with the SAME `now`/`gc_before`/`purge_safe`
    /// posture the untraced compaction path uses (design.md §D3), so the
    /// `winner` set the trail records is provably the read result. `None`
    /// disables gc-grace purging (the default).
    #[must_use]
    pub fn with_gc_before_secs(mut self, gc_before_secs: Option<i64>) -> Self {
        self.gc_before_secs = gc_before_secs;
        self
    }

    /// Supply the overlap-aware max-purgeable timestamp for a PARTIAL compaction
    /// (#935, parity with Cassandra `CompactionController.maxPurgeableTimestamp`).
    ///
    /// `max_purgeable_timestamp` is the MINIMUM write timestamp (`markedForDeleteAt`,
    /// micros) across every NON-INCLUDED overlapping SSTable for the table (their
    /// `Statistics.db` min-timestamp bound). With it set, the gc_grace purge stage
    /// additionally purges a tombstone in a partial compaction when the tombstone's
    /// own deletion timestamp is STRICTLY LESS THAN this bound — proving it shadows
    /// nothing outside the compaction set. `None` keeps the conservative #921
    /// behavior (a partial compaction does not purge). Ignored when `purge_safe`
    /// is `true` (a full compaction already has no non-included overlap, so the
    /// effective bound is `+inf`).
    #[must_use]
    pub fn with_max_purgeable_timestamp(mut self, max_purgeable_timestamp: Option<i64>) -> Self {
        self.max_purgeable_timestamp = max_purgeable_timestamp;
        self
    }

    /// Borrow the decision-trail sink after driving the merger.
    #[must_use]
    pub fn trace_sink(&self) -> &S {
        &self.trace
    }

    /// Mutably borrow the decision-trail sink while driving a merger.
    #[must_use]
    pub fn trace_sink_mut(&mut self) -> &mut S {
        &mut self.trace
    }

    /// Extract the caller-owned decision-trail sink after the merge completes.
    ///
    /// The input runs and heap are dropped with the merger.  This is the
    /// bounded hand-off used by explain callers: drive `step()` to
    /// `MergeStep::Complete`, then take the collected records without a second
    /// merge or a clone of the sink vectors.
    #[must_use]
    pub fn into_trace_sink(self) -> S {
        self.trace
    }

    /// Perform one merge step (one partition)
    ///
    /// Returns the next merged partition, or Complete if the merge is done.
    /// This allows incremental merging for better memory control.
    ///
    /// # Returns
    ///
    /// - `MergeStep::Partition` - Next merged partition with all its rows
    /// - `MergeStep::Complete` - Merge is complete
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    #[tracing::instrument(name = "merger.step", level = "debug", skip(self))]
    pub fn step(&mut self) -> Result<MergeStep> {
        // Initialize heap on first call
        if self.heap.is_empty() && self.current_partition.is_none() {
            self.initialize_heap()?;
        }

        // If heap is empty, merge is complete
        if self.heap.is_empty() {
            return Ok(MergeStep::Complete);
        }

        // Collect all rows for the next partition
        let mut partition_rows = Vec::new();
        let mut partition_key: Option<DecoratedKey> = None;

        while let Some(Reverse(wrapped)) = self.heap.peek() {
            // Check if we've moved to a new partition
            if let Some(ref current_key) = partition_key {
                if &wrapped.entry.key != current_key {
                    // Partition boundary - stop here
                    break;
                }
            } else {
                // First entry of new partition
                partition_key = Some(wrapped.entry.key.clone());
            }

            // Pop entry from heap
            let Reverse(wrapped) = self
                .heap
                .pop()
                .ok_or_else(|| Error::InvalidInput("Merge heap unexpectedly empty".to_string()))?;
            let entry = wrapped.entry;
            // Read the Copy `run_index` before moving `entry` (issue #1664: the
            // entry is already owned, so move it into partition_rows instead of
            // cloning).
            let run_index = entry.run_index;

            // Add to partition rows
            partition_rows.push(entry);

            // Refill heap from the run we just consumed from
            self.refill_heap(run_index)?;
        }

        if let Some(key) = partition_key {
            // Merge cells within this partition (last-write-wins)
            let merged_rows = self.merge_partition_rows(partition_rows)?;
            Ok(MergeStep::Partition {
                key,
                rows: merged_rows,
            })
        } else {
            Ok(MergeStep::Complete)
        }
    }

    /// Initialize the heap with the first entry from each run
    pub(super) fn initialize_heap(&mut self) -> Result<()> {
        for run_index in 0..self.runs.len() {
            self.refill_heap(run_index)?;
        }
        Ok(())
    }

    /// Refill the heap from a specific run
    pub(super) fn refill_heap(&mut self, run_index: usize) -> Result<()> {
        if run_index >= self.runs.len() {
            return Ok(());
        }

        let run = &mut self.runs[run_index];
        if !run.is_exhausted() {
            if let Some(entry) = run.advance()? {
                // Move the owned entry into the heap, paired with the
                // schema-aware comparator's schema (issue #1668, stage 5c-i).
                // Issue #1664: `advance()` returns the OWNED front entry, so we
                // move it in instead of the former peek+clone+discard-advance.
                self.heap
                    .push(Reverse(schema_order::SchemaOrderedEntry::new(
                        entry,
                        self.schema_arc.clone(),
                    )));
            }
        }

        Ok(())
    }

    /// Merge rows within a single partition using **per-cell reconcile**
    /// (Cassandra `org.apache.cassandra.db.rows.Cells#reconcile`).
    ///
    /// The pre-#533 implementation selected a single whole winning `MergeEntry`
    /// per clustering key, which DROPPED columns when two SSTables shared the same
    /// `(pk, ck)` but carried DISJOINT columns (e.g. A→{name}, B→{score} merged to
    /// only B's column). This now reconciles cell-by-cell so disjoint columns from
    /// every input survive (Issue #533).
    ///
    /// Algorithm per clustering-key group:
    ///   1. **Effective row deletion** — among `RowData::Tombstone` entries take the
    ///      max `deletion_time` (`row_del`). A row tombstone shadows any cell whose
    ///      `timestamp <= row_del`.
    ///   2. **Per-column cell reconcile** — across all `RowData::Live` entries, for
    ///      each column name pick the winning cell by:
    ///        - higher `timestamp` wins (last-write-wins);
    ///        - at EQUAL timestamp a cell tombstone (`Value::Tombstone(CellTombstone)`)
    ///          beats a live value (same rule as #498, applied per cell);
    ///        - otherwise the existing winner is kept (stable; heap routing already
    ///          ordered inputs by run_index so the first-seen at a tie is the newer
    ///          file).
    ///   3. **Row-tombstone shadowing per cell** — drop any reconciled cell whose
    ///      `timestamp <= row_del`. The `<=` makes the tombstone win at equal ts,
    ///      consistent with #498. Cells written strictly AFTER `row_del` survive.
    ///   4. **Build the merged result** — if any cells survive, emit a `Live`
    ///      entry whose row timestamp is the max surviving cell timestamp; else if a
    ///      row tombstone was present, emit a `Tombstone` entry at `row_del` so the
    ///      row stays shadowed downstream; else emit nothing.
    ///
    /// Overlap-safety gate for tombstone purging (#921 finding 1, #935):
    /// decide BOTH the effective gc_grace cutoff and the overlap-aware
    /// max-purgeable timestamp every cluster in this merger's output is
    /// reconciled with. Constant for this `KWayMerger`'s whole lifetime
    /// (derived only from its own `purge_safe`/`gc_before_secs`/
    /// `max_purgeable_timestamp` fields, set once at construction) — callers
    /// may compute it once and reuse it across every partition, rather than
    /// re-deriving it per partition.
    ///
    /// Extracted from [`Self::merge_partition_rows`] (issue #1668 stage 5d)
    /// so the streaming reconciliation path (`streaming.rs`) can reuse the
    /// EXACT same derivation without duplicating it.
    ///
    /// - FULL compaction (`purge_safe == true`): no non-included overlapping
    ///   SSTable exists, so the overlap bound is `+inf` (`i64::MAX`) — every
    ///   gc-purgeable tombstone passes the overlap gate, exactly as #845.
    /// - PARTIAL compaction with an overlap bound (#935): purge a tombstone
    ///   only when its own deletion timestamp is STRICTLY LESS THAN the min
    ///   write timestamp of every non-included overlapping SSTable, proving
    ///   it shadows nothing outside the set.
    /// - PARTIAL compaction without a bound (#921 default): retain every
    ///   tombstone — collapse the cutoff to `None` (purge no-op).
    pub(super) fn effective_gc_settings(&self) -> (Option<i64>, i64) {
        if self.purge_safe {
            (self.gc_before_secs, i64::MAX)
        } else if let Some(bound) = self.max_purgeable_timestamp {
            (self.gc_before_secs, bound)
        } else {
            (None, i64::MIN)
        }
    }
}
