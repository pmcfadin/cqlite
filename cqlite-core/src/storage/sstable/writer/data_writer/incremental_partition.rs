//! Incremental partition-write entry point (issue #1668, stage 5c-iv, PART 1
//! — build + prove, not yet wired to any production caller).
//!
//! `write_partition`/`write_partition_with_index_blocks` (`partition.rs`)
//! need the WHOLE partition's `&[Mutation]` slice upfront: they sort it, run
//! `collect_static_operations` over it, build `rows` via
//! `merge_clustering_rows`, then interleave rows with range-tombstone markers
//! via a full re-sort. [`IncrementalPartitionWriter`] proves the SAME Data.db
//! bytes can be produced from a partition fed ONE PIECE AT A TIME:
//!
//! 1. [`DataWriter::begin_partition_incremental`] — writes the header
//!    immediately (the partition tombstone, if any, is known upfront from the
//!    caller — the eventual merge-side caller has it from the stage-1
//!    carrier pre-scan). Builds and sorts the (typically few) range-
//!    tombstone markers ONCE via `marker_merge::partition_item_cmp` (stage
//!    5c-iii) — NOT the whole per-row items list.
//! 2. [`IncrementalPartitionWriter::feed_static_row`] — called AT MOST ONCE,
//!    right after `begin_partition_incremental`, BEFORE any clustering row.
//!    Compaction's own merge reconciliation guarantees a static-row carrier
//!    (if any) is the ONLY `clustering_key: None` entry in a partition's
//!    reconciled `Vec<MergeEntry>` and ALWAYS sorts first — proven directly
//!    in `streaming.rs`'s `static_row_carrier_always_sorts_first_regardless_
//!    of_partition_width` test. So the caller resolves statics via
//!    [`super::static_ops::StaticOpsTracker`] (stage 5c-ii) BEFORE streaming
//!    any `Some(ck)` row, without buffering the clustering-row tail.
//! 3. [`IncrementalPartitionWriter::feed_row`] — called once per clustering
//!    row, ALREADY in clustering order (the caller's streaming source
//!    guarantees this — stage 5c-i's schema-aware heap, modulo the stage-5d
//!    `clustered_rows` fix). Emits any PENDING sorted markers that sort
//!    strictly before this row (the 5c-iii merge-step logic, driven one row
//!    at a time instead of via a bulk interleave), then the row itself.
//! 4. [`IncrementalPartitionWriter::finish`] — emits any remaining markers,
//!    the `END_OF_PARTITION` byte, closes the last promoted-index block, and
//!    flushes.
//!
//! Promoted-index block tracking (Cassandra `BigFormatPartitionWriter`
//! parity) is reproduced VERBATIM from `write_partition_with_index_blocks`'s
//! inline loop body — moved, not duplicated — via
//! [`IncrementalPartitionWriter::emit_item`], the ONE place that both the
//! static-row/row/marker emission paths funnel through.
//!
//! NOT yet called by any production path: `SSTableWriter::write_partition`
//! still calls `write_partition_with_index_blocks` (the whole-slice path).
//! Wiring `maintenance.rs`/`KWayMerger::merge()` to this incremental entry
//! point is stage 5c-iv, PART 2 — only after this module is proven correct
//! in isolation (see this module's tests).

// Stage 5c-iv part 1 (#1668) is deliberately UNWIRED: no production caller
// constructs an `IncrementalPartitionWriter` yet (that is part 2), so a
// normal (non-test) build sees every item here as unreachable. Matches the
// crate's existing convention for proof-only surface pending production
// wiring (see `streaming.rs`'s stage-2 / `schema_order.rs`'s stage-5b
// history). Exercised directly by this module's own tests.
#![allow(dead_code)]

use super::*;

/// A single range-tombstone bound marker, sorted once in
/// [`DataWriter::begin_partition_incremental`] and drained as rows arrive
/// (issue #1668, stage 5c-iv). A dedicated `Copy` struct — rather than
/// storing `PartitionItem::Marker` variants directly — so converting a
/// stored marker into the `PartitionItem` `emit_item` expects is a plain,
/// always-valid field copy with no other-variant case to (unreachably)
/// panic on.
#[derive(Clone, Copy)]
struct SortableMarker<'r> {
    bound: &'r ClusteringBound,
    is_open: bool,
    deletion_time: i64,
    local_deletion_time: i32,
}

impl<'r> SortableMarker<'r> {
    fn as_partition_item(self) -> PartitionItem<'r> {
        PartitionItem::Marker {
            bound: self.bound,
            is_open: self.is_open,
            deletion_time: self.deletion_time,
            local_deletion_time: self.local_deletion_time,
        }
    }
}

/// Incremental partition-write session (issue #1668, stage 5c-iv).
///
/// Holds `&'w mut DataWriter` (so its own methods can call the writer's
/// existing `pub(super)` emission helpers directly) plus the running,
/// partition-scoped state `write_partition_with_index_blocks` otherwise
/// keeps as plain local variables inside its `for item in items` loop.
pub(crate) struct IncrementalPartitionWriter<'w, 'r> {
    writer: &'w mut DataWriter,
    partition_offset: u64,
    partition_buf_start: usize,
    prev_unfiltered_size: u64,
    partition_floor: Option<i64>,
    range_tombstones: &'r [RangeTombstone],
    /// The (typically few) range-tombstone markers, sorted ONCE via the
    /// SAME comparator `sort_partition_items` uses (stage 5c-iii) — drained
    /// as rows arrive via [`Self::feed_row`], never re-sorted.
    sorted_markers: Vec<SortableMarker<'r>>,
    marker_cursor: usize,
    emit: PartitionEmitCounts,
    /// `None` until the first post-header/post-static-row item is about to
    /// be written — matches `write_partition_with_index_blocks`'s
    /// `block_start_buf_offset`, which excludes the header AND the static
    /// row from block 0's byte range.
    block_start_buf_offset: Option<usize>,
    blocks: Vec<PromotedIndexBlock>,
    current_block_first_ck: Option<Vec<u8>>,
    current_block_last_ck: Option<Vec<u8>>,
    current_block_oss50: Option<Option<Vec<u8>>>,
}

impl DataWriter {
    /// Begin an incremental partition write (issue #1668, stage 5c-iv).
    ///
    /// Writes the partition header immediately (the caller supplies
    /// `partition_tombstone` upfront — known before any row arrives, from
    /// the stage-1 carrier pre-scan on the merge side) and pre-sorts the
    /// (small) `range_tombstones` list ONCE. Returns a session that must be
    /// driven through `feed_static_row` (at most once, if the schema has any
    /// static column), then `feed_row` (one call per clustering row, in
    /// clustering order), then `finish`.
    pub(crate) fn begin_partition_incremental<'w, 'r>(
        &'w mut self,
        key: &DecoratedKey,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &'r [RangeTombstone],
        schema: &TableSchema,
    ) -> Result<IncrementalPartitionWriter<'w, 'r>> {
        let partition_offset = self.position + self.buffer.len() as u64;
        let partition_buf_start = self.buffer.len();
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let prev_unfiltered_size = (self.buffer.len() - header_start) as u64;
        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);

        let mut sorted_markers: Vec<SortableMarker<'r>> =
            Vec::with_capacity(range_tombstones.len() * 2);
        for rt in range_tombstones {
            sorted_markers.push(SortableMarker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            sorted_markers.push(SortableMarker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }
        sorted_markers.sort_by(|a, b| {
            marker_merge::partition_item_cmp(&a.as_partition_item(), &b.as_partition_item(), schema)
        });

        Ok(IncrementalPartitionWriter {
            writer: self,
            partition_offset,
            partition_buf_start,
            prev_unfiltered_size,
            partition_floor,
            range_tombstones,
            sorted_markers,
            marker_cursor: 0,
            emit: PartitionEmitCounts::default(),
            block_start_buf_offset: None,
            blocks: Vec::new(),
            current_block_first_ck: None,
            current_block_last_ck: None,
            current_block_oss50: None,
        })
    }
}

impl<'w, 'r> IncrementalPartitionWriter<'w, 'r> {
    /// Write the static-row prelude — called AT MOST ONCE, before any
    /// `feed_row` call, when the schema declares any static column. `merged`
    /// is the ALREADY-RESOLVED static ops (e.g. via
    /// `static_ops::StaticOpsTracker::finish`, stage 5c-ii); pass an empty
    /// slice for "schema has statics but this partition writes none" (the
    /// writer still emits the minimal empty form, matching
    /// `write_partition_with_index_blocks` exactly). `first_mutation_ts` is
    /// the fallback liveness timestamp used only when `merged` is non-empty
    /// but carries no derivable liveness (mirrors
    /// `mutations.first().map(|m| m.timestamp_micros).unwrap_or(0)` in the
    /// whole-slice path).
    pub(crate) fn feed_static_row(
        &mut self,
        merged: &[StaticMergedOp],
        first_mutation_ts: i64,
        schema: &TableSchema,
    ) -> Result<()> {
        if merged.is_empty() {
            let static_size = self.writer.write_empty_static_row(0, schema)? as u64;
            self.prev_unfiltered_size += static_size;
        } else {
            let (latest_ts, ttl) =
                static_liveness_from_ops(merged).unwrap_or((first_mutation_ts, None));
            let (static_size, static_cells) = self
                .writer
                .write_static_row_with_prev_size(merged, latest_ts, ttl, schema, 0)?;
            self.prev_unfiltered_size += static_size as u64;
            self.emit.rows += 1;
            self.emit.columns += static_cells;
        }
        Ok(())
    }

    /// Feed one clustering-key group's mutation (issue #1668, stage 5c-iv).
    ///
    /// For compaction, a "cluster group" is already a single, fully
    /// reconciled `Mutation` (the merge layer collapses to one entry per
    /// distinct clustering key), so this treats `mutation` as its own
    /// singleton group via `merge_row_group(&[mutation], ..)` — the SAME
    /// function `merge_clustering_rows` calls per adjacency-group in the
    /// whole-slice path. Emits any PENDING sorted markers that sort strictly
    /// before this row first, then the row itself (skipping emission
    /// entirely if the row is fully shadowed — matches
    /// `merge_clustering_rows`'s `if let Some(row) = ...` skip).
    pub(crate) fn feed_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        let clustering_key = mutation.clustering_key.as_ref();
        let mut shadow_floor = self.partition_floor;
        for rt in self.range_tombstones {
            if range_tombstone_covers(rt, clustering_key, schema) {
                shadow_floor =
                    Some(shadow_floor.map_or(rt.deletion_time, |f| f.max(rt.deletion_time)));
            }
        }

        // Emit every pending marker that sorts strictly before this row
        // (issue #1220 tie-break: a Row and a Marker never compare Equal at
        // the same clustering value — see marker_merge's module doc — so
        // "strictly before" is unambiguous here).
        let row_item = PartitionItem::Row(RowWrite {
            clustering_key,
            liveness_ts: None,
            ttl_seconds: None,
            row_deletion: None,
            ops: Vec::new(),
            complex_element_ops: Vec::new(),
        });
        while let Some(marker) = self.sorted_markers.get(self.marker_cursor).copied() {
            if marker_merge::partition_item_cmp(&marker.as_partition_item(), &row_item, schema)
                == std::cmp::Ordering::Greater
            {
                break;
            }
            self.emit_next_marker_or_boundary(marker, schema)?;
        }

        if let Some(row) = DataWriter::merge_row_group(&[mutation], schema, false, shadow_floor) {
            self.emit_item(PartitionItem::Row(row), schema)?;
        }
        Ok(())
    }

    /// Emit `marker` (the item at `self.marker_cursor`), coalescing it with
    /// its immediately-following pair into a single `PartitionItem::Boundary`
    /// when they form one (issue #1220 / roborev blocker #1 on #1668):
    /// `write_partition`/`write_partition_with_index_blocks` run every
    /// emitted marker through `coalesce_boundaries`, but draining
    /// `sorted_markers` one at a time (instead of coalescing the whole
    /// pre-sorted `Vec` up front) skipped that step entirely — two adjacent
    /// range tombstones with different deletion times would otherwise be
    /// persisted as two separate bound markers instead of Cassandra's single
    /// boundary marker.
    ///
    /// Safe to decide using ONLY `marker`'s own already-known sort position
    /// (the caller's row-test in [`Self::feed_row`], or unconditionally in
    /// [`Self::finish`]): a coalescible close+open pair always shares the
    /// EXACT SAME clustering point with EQUAL sort weight (see
    /// `marker_merge::sort_class`), so both bounds always compare identically
    /// against any given row — advancing past both here can never skip past
    /// a row that should have been emitted first.
    fn emit_next_marker_or_boundary(
        &mut self,
        marker: SortableMarker<'r>,
        schema: &TableSchema,
    ) -> Result<()> {
        if !marker.is_open {
            if let Some(next) = self.sorted_markers.get(self.marker_cursor + 1).copied() {
                if next.is_open {
                    if let Some((kind, clustering)) =
                        partition::boundary_kind_for(marker.bound, next.bound, schema)
                    {
                        self.marker_cursor += 2;
                        return self.emit_item(
                            PartitionItem::Boundary {
                                kind,
                                clustering,
                                end_deletion_time: marker.deletion_time,
                                end_local_deletion_time: marker.local_deletion_time,
                                start_deletion_time: next.deletion_time,
                                start_local_deletion_time: next.local_deletion_time,
                            },
                            schema,
                        );
                    }
                }
            }
        }
        self.marker_cursor += 1;
        self.emit_item(marker.as_partition_item(), schema)
    }

    /// Finalize the partition: emit any remaining markers, `END_OF_PARTITION`,
    /// close the last promoted-index block, and flush.
    pub(crate) fn finish(
        mut self,
        schema: &TableSchema,
    ) -> Result<(u64, Vec<PromotedIndexBlock>, PartitionEmitCounts)> {
        while self.marker_cursor < self.sorted_markers.len() {
            let marker = self.sorted_markers[self.marker_cursor];
            self.emit_next_marker_or_boundary(marker, schema)?;
        }

        self.writer.buffer.push(END_OF_PARTITION);

        if let (Some(first), Some(last)) = (
            self.current_block_first_ck.take(),
            self.current_block_last_ck.take(),
        ) {
            let current_buf_offset = self.writer.buffer.len() - self.partition_buf_start;
            let block_start = self.block_start_buf_offset.unwrap_or(current_buf_offset);
            let block_bytes = (current_buf_offset - block_start) as u64;
            if block_bytes > 0 {
                self.blocks.push(PromotedIndexBlock {
                    first_name: first,
                    last_name: last,
                    offset: block_start as u64,
                    width: block_bytes,
                    oss50_separator: self.current_block_oss50.take().flatten(),
                });
            }
        }

        self.writer.flush_partition()?;
        Ok((self.partition_offset, self.blocks, self.emit))
    }

    /// Write one item (row or marker) and update promoted-index-block
    /// tracking — moved VERBATIM (not duplicated) from
    /// `write_partition_with_index_blocks`'s `for item in items { .. }` loop
    /// body, the ONE place both the row and marker emission paths funnel
    /// through.
    ///
    /// Generic over its OWN lifetime `'a`, independent of the struct's `'r`
    /// (which is `range_tombstones`' lifetime): a `Row` item borrows from
    /// the CURRENT `feed_row` call's `mutation` argument, an entirely
    /// different (and shorter-lived) object than `range_tombstones` — the
    /// item is fully consumed (its bytes written) before this call returns,
    /// so it never needs to outlive that.
    fn emit_item<'a>(&mut self, item: PartitionItem<'a>, schema: &TableSchema) -> Result<()> {
        let ck_bytes: Vec<u8> = match &item {
            PartitionItem::Row(row) => {
                if let Some(ck) = row.clustering_key {
                    serialize_clustering_prefix_for_index(ck, schema)
                        .unwrap_or_else(|_| empty_clustering_prefix_for_index())
                } else {
                    empty_clustering_prefix_for_index()
                }
            }
            PartitionItem::Marker { bound, is_open, .. } => {
                serialize_marker_bound_prefix_for_index(bound, *is_open, schema)
                    .unwrap_or_else(|_| marker_bound_prefix_for_index(bound, *is_open))
            }
            PartitionItem::Boundary {
                kind, clustering, ..
            } => serialize_boundary_prefix_for_index(*kind, clustering, schema)
                .unwrap_or_else(|_| boundary_prefix_for_index_fallback(*kind)),
        };

        let partition_buf_start = self.partition_buf_start;
        if self.current_block_first_ck.is_none() {
            self.current_block_first_ck = Some(ck_bytes.clone());
            let ck_values: Option<Vec<Value>> = match &item {
                PartitionItem::Row(row) => row
                    .clustering_key
                    .filter(|ck| !ck.columns.is_empty())
                    .map(|ck| ck.columns.iter().map(|(_, v)| v.clone()).collect()),
                PartitionItem::Marker { .. } | PartitionItem::Boundary { .. } => None,
            };
            let is_reversed: Vec<bool> = schema
                .clustering_keys
                .iter()
                .map(|c| c.order == crate::schema::ClusteringOrder::Desc)
                .collect();
            self.current_block_oss50 = Some(ck_values.and_then(|vals| {
                crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order(
                    &vals,
                    &is_reversed,
                )
                .ok()
            }));
            // Lazily anchor the FIRST block's start now, at the first
            // post-header/post-static-row item — matches
            // `write_partition_with_index_blocks`'s `block_start_buf_offset`
            // initialization point exactly.
            self.block_start_buf_offset
                .get_or_insert(self.writer.buffer.len() - partition_buf_start);
        }
        self.current_block_last_ck = Some(ck_bytes.clone());

        self.prev_unfiltered_size = match item {
            PartitionItem::Row(row) => {
                self.emit.rows += 1;
                let (bytes, cells) = self.writer.write_merged_row_with_prev_size(
                    &row,
                    schema,
                    self.prev_unfiltered_size,
                )?;
                self.emit.columns += cells;
                bytes as u64
            }
            PartitionItem::Marker {
                bound,
                is_open,
                deletion_time,
                local_deletion_time,
            } => self.writer.write_range_bound(
                bound,
                is_open,
                deletion_time,
                local_deletion_time,
                schema,
                self.prev_unfiltered_size,
            )? as u64,
            PartitionItem::Boundary {
                kind,
                clustering,
                end_deletion_time,
                end_local_deletion_time,
                start_deletion_time,
                start_local_deletion_time,
            } => self.writer.write_range_boundary(
                kind,
                clustering,
                end_deletion_time,
                end_local_deletion_time,
                start_deletion_time,
                start_local_deletion_time,
                schema,
                self.prev_unfiltered_size,
            )? as u64,
        };

        let current_buf_offset = self.writer.buffer.len() - partition_buf_start;
        let block_start = self.block_start_buf_offset.unwrap_or(current_buf_offset);
        let block_bytes = (current_buf_offset - block_start) as u64;

        if block_bytes >= COLUMN_INDEX_SIZE_BYTES {
            self.blocks.push(PromotedIndexBlock {
                first_name: self
                    .current_block_first_ck
                    .take()
                    .unwrap_or_else(empty_clustering_prefix_for_index),
                last_name: self
                    .current_block_last_ck
                    .take()
                    .unwrap_or_else(empty_clustering_prefix_for_index),
                offset: block_start as u64,
                width: block_bytes,
                oss50_separator: self.current_block_oss50.take().flatten(),
            });
            self.block_start_buf_offset = Some(current_buf_offset);
            self.current_block_first_ck = None;
            self.current_block_last_ck = None;
            self.current_block_oss50 = None;
        }
        Ok(())
    }
}
