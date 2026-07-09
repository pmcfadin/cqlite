//! Cross-call resumable incremental partition-write session (issue #1668,
//! stage 5c-iv part 3).
//!
//! [`super::incremental_partition::IncrementalPartitionWriter`] (stage 5c-iv
//! part 1) proves a partition can be streamed one row at a time instead of
//! buffered as a whole `&[Mutation]` slice — but it BORROWS `&'w mut
//! DataWriter` and the caller's `&'r [RangeTombstone]` for its whole
//! lifetime, so it cannot survive returning to a caller that must give
//! control back to an outer scheduler mid-partition (`WriteEngine::
//! maintenance_step`'s budget-driven pause/resume, issue #1668 stage 4) and
//! reconstitute it later — the borrow, and the unfinished on-disk partition
//! (no `END_OF_PARTITION`/index registration yet), cannot outlive that
//! function call.
//!
//! [`StreamingPartitionSession`] is a NEW, parallel type (the existing
//! borrow-based session is UNTOUCHED — `KWayMerger::merge` keeps using it
//! exactly as stage 5c-iv part 2 left it) that eliminates every lifetime
//! parameter with one change: `writer: &mut DataWriter` moves from a STORED
//! field to a PER-CALL PARAMETER on `feed_row`/`feed_static_row`/`finish`,
//! borrowed fresh from `SSTableWriter.data_writer` for just that one call.
//! `range_tombstones`/`sorted_markers`' bounds are cloned into owned storage
//! instead of borrowed (deliberately: both are documented as "typically
//! few" per partition — the same bounded-cost class as the static-row/
//! carrier prefix, not something that scales with partition width). The
//! result has no lifetime at all, so it can sit as a plain field on
//! `ActiveMerge` across `maintenance_step_inner` calls: pausing is just "stop
//! calling methods on it," resuming is just "keep calling methods on it" —
//! no capture/reconstitute conversion step is needed.
//!
//! The emission logic itself (marker draining, promoted-index-block
//! tracking) is intentionally a parallel copy of
//! [`super::incremental_partition`]'s, not a shared abstraction over both —
//! unifying them would require also changing the already-tested borrow-based
//! session's signature, which risks the behavior stage 5c-iv parts 1/2
//! already proved. Kept as an explicit, flagged duplication rather than
//! reworking tested code.

use super::*;

/// Owned range-tombstone bound marker — same shape as
/// [`super::incremental_partition`]'s private `SortableMarker`, but OWNS a
/// clone of the [`ClusteringBound`] instead of borrowing it, so the whole
/// session (including its markers) is lifetime-free.
#[derive(Clone)]
struct OwnedSortableMarker {
    bound: ClusteringBound,
    is_open: bool,
    deletion_time: i64,
    local_deletion_time: i32,
}

impl OwnedSortableMarker {
    fn as_partition_item(&self) -> PartitionItem<'_> {
        PartitionItem::Marker {
            bound: &self.bound,
            is_open: self.is_open,
            deletion_time: self.deletion_time,
            local_deletion_time: self.local_deletion_time,
        }
    }
}

/// Cross-call resumable incremental partition-write session (issue #1668,
/// stage 5c-iv part 3). See the module doc for why this exists alongside
/// [`super::incremental_partition::IncrementalPartitionWriter`] rather than
/// replacing it.
pub(crate) struct StreamingPartitionSession {
    partition_offset: u64,
    partition_buf_start: usize,
    prev_unfiltered_size: u64,
    partition_floor: Option<i64>,
    range_tombstones: Vec<RangeTombstone>,
    sorted_markers: Vec<OwnedSortableMarker>,
    marker_cursor: usize,
    emit: PartitionEmitCounts,
    block_start_buf_offset: Option<usize>,
    blocks: Vec<PromotedIndexBlock>,
    current_block_first_ck: Option<Vec<u8>>,
    current_block_last_ck: Option<Vec<u8>>,
    current_block_oss50: Option<Option<Vec<u8>>>,
}

impl DataWriter {
    /// Begin a cross-call resumable incremental partition write (issue
    /// #1668, stage 5c-iv part 3). Same upfront work as
    /// [`Self::begin_partition_incremental`] (writes the header immediately,
    /// pre-sorts the range-tombstone markers once) but clones
    /// `range_tombstones` and every marker's bound into owned storage so the
    /// returned session carries no borrow of `self` or of the caller's slice.
    pub(crate) fn begin_streaming_partition(
        &mut self,
        key: &DecoratedKey,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &[RangeTombstone],
        schema: &TableSchema,
    ) -> Result<StreamingPartitionSession> {
        let partition_offset = self.position + self.buffer.len() as u64;
        let partition_buf_start = self.buffer.len();
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let prev_unfiltered_size = (self.buffer.len() - header_start) as u64;
        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);

        let mut sorted_markers: Vec<OwnedSortableMarker> =
            Vec::with_capacity(range_tombstones.len() * 2);
        for rt in range_tombstones {
            sorted_markers.push(OwnedSortableMarker {
                bound: rt.start.clone(),
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            sorted_markers.push(OwnedSortableMarker {
                bound: rt.end.clone(),
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }
        sorted_markers.sort_by(|a, b| {
            marker_merge::partition_item_cmp(&a.as_partition_item(), &b.as_partition_item(), schema)
        });

        Ok(StreamingPartitionSession {
            partition_offset,
            partition_buf_start,
            prev_unfiltered_size,
            partition_floor,
            range_tombstones: range_tombstones.to_vec(),
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

impl StreamingPartitionSession {
    /// Write the static-row prelude — see
    /// [`super::incremental_partition::IncrementalPartitionWriter::feed_static_row`]
    /// for the full contract (identical here, minus the borrowed `writer`
    /// field becoming a per-call parameter).
    pub(crate) fn feed_static_row(
        &mut self,
        writer: &mut DataWriter,
        merged: &[StaticMergedOp],
        first_mutation_ts: i64,
        schema: &TableSchema,
    ) -> Result<()> {
        if merged.is_empty() {
            let static_size = writer.write_empty_static_row(0, schema)? as u64;
            self.prev_unfiltered_size += static_size;
        } else {
            let (latest_ts, ttl) =
                static_liveness_from_ops(merged).unwrap_or((first_mutation_ts, None));
            let (static_size, static_cells) =
                writer.write_static_row_with_prev_size(merged, latest_ts, ttl, schema, 0)?;
            self.prev_unfiltered_size += static_size as u64;
            self.emit.rows += 1;
            self.emit.columns += static_cells;
        }
        Ok(())
    }

    /// Feed one clustering-key group's mutation — see
    /// [`super::incremental_partition::IncrementalPartitionWriter::feed_row`]
    /// for the full contract (identical here, minus the borrowed `writer`
    /// field becoming a per-call parameter).
    pub(crate) fn feed_row(
        &mut self,
        writer: &mut DataWriter,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        let clustering_key = mutation.clustering_key.as_ref();
        let mut shadow_floor = self.partition_floor;
        for rt in &self.range_tombstones {
            if range_tombstone_covers(rt, clustering_key, schema) {
                shadow_floor =
                    Some(shadow_floor.map_or(rt.deletion_time, |f| f.max(rt.deletion_time)));
            }
        }

        let row_item = PartitionItem::Row(RowWrite {
            clustering_key,
            liveness_ts: None,
            ttl_seconds: None,
            row_deletion: None,
            ops: Vec::new(),
            complex_element_ops: Vec::new(),
        });
        while let Some(marker) = self.sorted_markers.get(self.marker_cursor).cloned() {
            if marker_merge::partition_item_cmp(&marker.as_partition_item(), &row_item, schema)
                == std::cmp::Ordering::Greater
            {
                break;
            }
            self.marker_cursor += 1;
            self.emit_item(writer, marker.as_partition_item(), schema)?;
        }

        if let Some(row) = DataWriter::merge_row_group(&[mutation], schema, false, shadow_floor) {
            self.emit_item(writer, PartitionItem::Row(row), schema)?;
        }
        Ok(())
    }

    /// Finalize the partition — see
    /// [`super::incremental_partition::IncrementalPartitionWriter::finish`]
    /// for the full contract (identical here, minus the borrowed `writer`
    /// field becoming a per-call parameter; `self` is consumed either way).
    pub(crate) fn finish(
        mut self,
        writer: &mut DataWriter,
        schema: &TableSchema,
    ) -> Result<(u64, Vec<PromotedIndexBlock>, PartitionEmitCounts)> {
        while self.marker_cursor < self.sorted_markers.len() {
            let marker = self.sorted_markers[self.marker_cursor].clone();
            self.marker_cursor += 1;
            self.emit_item(writer, marker.as_partition_item(), schema)?;
        }

        writer.buffer.push(END_OF_PARTITION);

        if let (Some(first), Some(last)) = (
            self.current_block_first_ck.take(),
            self.current_block_last_ck.take(),
        ) {
            let current_buf_offset = writer.buffer.len() - self.partition_buf_start;
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

        writer.flush_partition()?;
        Ok((self.partition_offset, self.blocks, self.emit))
    }

    /// Write one item (row or marker) and update promoted-index-block
    /// tracking — moved VERBATIM from
    /// [`super::incremental_partition::IncrementalPartitionWriter::emit_item`]
    /// (itself moved verbatim from `write_partition_with_index_blocks`),
    /// minus the borrowed `writer` field becoming a per-call parameter.
    fn emit_item<'a>(
        &mut self,
        writer: &mut DataWriter,
        item: PartitionItem<'a>,
        schema: &TableSchema,
    ) -> Result<()> {
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
            self.block_start_buf_offset
                .get_or_insert(writer.buffer.len() - partition_buf_start);
        }
        self.current_block_last_ck = Some(ck_bytes.clone());

        self.prev_unfiltered_size = match item {
            PartitionItem::Row(row) => {
                self.emit.rows += 1;
                let (bytes, cells) = writer.write_merged_row_with_prev_size(
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
            } => writer.write_range_bound(
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
            } => writer.write_range_boundary(
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

        let current_buf_offset = writer.buffer.len() - partition_buf_start;
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
