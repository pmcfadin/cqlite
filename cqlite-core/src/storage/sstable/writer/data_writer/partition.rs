//! Partition-level emission: partition header, top-level partition write, and the empty static-row prelude.
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, flag constants, and crate imports
//! re-exported from `data_writer/mod.rs`. No emitted bytes change.

use super::*;

/// One unfiltered to emit inside a partition body, in clustering order: a regular
/// row, a single range-tombstone BOUND marker, or a coalesced range-tombstone
/// BOUNDARY marker (issue #1220).
///
/// Lifted to module scope (from the two per-function local enums it replaced) so
/// the marker-coalescing pass ([`coalesce_boundaries`]) is shared verbatim by both
/// the plain [`DataWriter::write_partition`] and the wide-partition
/// [`DataWriter::write_partition_with_index_blocks`] emitters — they must produce
/// byte-identical bodies, boundary coalescing included.
pub(crate) enum PartitionItem<'a> {
    Row(RowWrite<'a>),
    Marker {
        bound: &'a ClusteringBound,
        is_open: bool,
        deletion_time: i64,
        local_deletion_time: i32,
    },
    /// A coalesced boundary: closes the previous range and opens the next at the
    /// same `clustering` point. `end_*` is the closing range's deletion (primary),
    /// `start_*` is the opening range's deletion (secondary).
    Boundary {
        kind: u8,
        clustering: &'a ClusteringKey,
        end_deletion_time: i64,
        end_local_deletion_time: i32,
        start_deletion_time: i64,
        start_local_deletion_time: i32,
    },
}

/// Sort `items` into clustering order (rows + bound markers), schema-aware.
/// The comparator itself (`marker_merge::partition_item_cmp`) is SHARED with
/// [`marker_merge::merge_rows_and_markers`] (issue #1668, stage 5c-iii) — the
/// incremental alternative that merges already-sorted rows with the
/// (typically few) range-tombstone markers instead of re-sorting the whole
/// combined sequence from scratch. Both paths must always agree.
fn sort_partition_items(items: &mut [PartitionItem], schema: &TableSchema) {
    items.sort_by(|a, b| marker_merge::partition_item_cmp(a, b, schema));
}

/// The BOUNDARY kind for an adjacent close+open bound pair at the same clustering
/// point, or `None` when the two bounds do NOT form a boundary (issue #1220).
///
/// Cassandra coalesces two adjacent range tombstones into a single boundary only
/// when the closing bound and the opening bound meet at the same clustering value
/// with COMPLEMENTARY inclusivity (exactly one of them inclusive) — otherwise the
/// ranges either overlap (both inclusive) or leave a gap (both exclusive) and stay
/// separate bounds. Returns the boundary kind ordinal plus the shared clustering.
fn boundary_kind_for<'a>(
    close: &'a ClusteringBound,
    open: &'a ClusteringBound,
    schema: &TableSchema,
) -> Option<(u8, &'a ClusteringKey)> {
    let same = |a: &ClusteringKey, b: &ClusteringKey| {
        a.compare(b, schema)
            .map(|o| o == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    };
    match (close, open) {
        (ClusteringBound::Exclusive(c1), ClusteringBound::Inclusive(c2)) if same(c1, c2) => {
            Some((EXCL_END_INCL_START_BOUNDARY, c2))
        }
        (ClusteringBound::Inclusive(c1), ClusteringBound::Exclusive(c2)) if same(c1, c2) => {
            Some((INCL_END_EXCL_START_BOUNDARY, c1))
        }
        _ => None,
    }
}

/// Coalesce each adjacent (close BOUND, open BOUND) pair that meets at the same
/// clustering point with complementary inclusivity into a single BOUNDARY item
/// (issue #1220). After [`sort_partition_items`], the closing bound of one range
/// and the opening bound of the next sit at the same sort position (equal class,
/// clustering and weight) with the close first, so no row can fall between them;
/// this single forward pass merges them into one two-deletion-time boundary.
fn coalesce_boundaries<'a>(
    items: Vec<PartitionItem<'a>>,
    schema: &TableSchema,
) -> Vec<PartitionItem<'a>> {
    let mut out: Vec<PartitionItem<'a>> = Vec::with_capacity(items.len());
    let mut iter = items.into_iter().peekable();
    while let Some(item) = iter.next() {
        if let PartitionItem::Marker {
            bound: close_bound,
            is_open: false,
            deletion_time: end_dt,
            local_deletion_time: end_ldt,
        } = &item
        {
            if let Some(PartitionItem::Marker {
                bound: open_bound,
                is_open: true,
                deletion_time: start_dt,
                local_deletion_time: start_ldt,
            }) = iter.peek()
            {
                if let Some((kind, clustering)) = boundary_kind_for(close_bound, open_bound, schema)
                {
                    let boundary = PartitionItem::Boundary {
                        kind,
                        clustering,
                        end_deletion_time: *end_dt,
                        end_local_deletion_time: *end_ldt,
                        start_deletion_time: *start_dt,
                        start_local_deletion_time: *start_ldt,
                    };
                    // The open marker's fields are already copied into `boundary`;
                    // drop it from the stream.
                    iter.next();
                    out.push(boundary);
                    continue;
                }
            }
        }
        out.push(item);
    }
    out
}

impl DataWriter {
    /// Write a complete partition (partition key + all rows)
    ///
    /// # Arguments
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `mutations` - All mutations for this partition (must be in clustering order)
    /// * `schema` - Table schema for column metadata
    /// * `partition_tombstone` - Optional partition-level tombstone
    /// * `range_tombstones` - Range tombstones for this partition (must be in clustering order)
    ///
    /// # Returns
    /// File offset where this partition starts (for Index.db)
    pub fn write_partition(
        &mut self,
        key: &DecoratedKey,
        mutations: &[Mutation],
        schema: &TableSchema,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &[RangeTombstone],
    ) -> Result<u64> {
        // File offset of this partition = bytes already flushed (`position`) plus
        // whatever is currently buffered. In streaming mode `buffer` is empty at
        // the start of each partition (flushed + cleared by the previous call),
        // so this is `position`; in in-memory mode `position` is 0 and `buffer`
        // holds all prior partitions, matching the legacy `buffer.len()`.
        let partition_offset = self.position + self.buffer.len() as u64;

        // Write partition header (with optional tombstone)
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let mut prev_unfiltered_size = (self.buffer.len() - header_start) as u64;

        // SSTables must be internally reconciled: Cassandra's read path and
        // compaction only reconcile rows against deletions from OTHER sources
        // (memtables / other sstables) — a row shadowed by a partition or
        // range tombstone in the SAME sstable is served live. Cassandra's own
        // flush drops shadowed data, and so must we (Issue #716/#717).
        // `partition_floor` is the shadow timestamp from the partition
        // tombstone; per-row floors additionally account for covering range
        // tombstones.
        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);

        // Cassandra's SerializationHeader.hasStatic() returns true whenever the schema
        // declares any static column — and both the writer and reader unconditionally
        // emit/consume a static-row prelude in that case.  We must do the same.
        let schema_has_static = schema.columns.iter().any(|c| c.is_static);

        if schema_has_static {
            // Collect static-column operations from ALL mutations in this partition,
            // regardless of whether the mutation also carries a clustering key.
            // Last-write-wins by timestamp_micros when the same column appears twice.
            // Static cells shadowed by the partition tombstone are dropped.
            let merged = collect_static_operations(mutations, schema, partition_floor);

            if merged.is_empty() {
                // Schema declares statics but this partition writes none.
                // Cassandra still expects the prelude; emit the minimal empty form.
                //
                // Issue #821 (finding #2): a static row hard-codes
                // previousUnfilteredSize = 0 and does NOT become the "previous
                // unfiltered" for the chain. The running value (the partition
                // header size) is carried forward by ADDING the static row's
                // bytes, so the first regular row sees its own offset from the
                // partition start (verified against real Cassandra "nb"
                // SSTables: header + static_row_size).
                let static_size = self.write_empty_static_row(0, schema)? as u64;
                prev_unfiltered_size += static_size;
            } else {
                // Issue #1018 (roborev HIGH): derive the static row's liveness
                // timestamp + TTL from the SURVIVING merged ops (each carrying its
                // own per-cell writetime after the per-cell shadow floor), NOT from
                // the mutations that merely cleared the floor on their row max — a
                // static write whose per-cell ts is `<= partition_floor` is already
                // dropped from `merged`, so it cannot leak its writetime here.
                // `!merged.is_empty()` guarantees `Some`.
                let (latest_ts, ttl) = static_liveness_from_ops(&merged).unwrap_or((
                    mutations.first().map(|m| m.timestamp_micros).unwrap_or(0),
                    None,
                ));

                // Issue #764: pass the per-op merged static ops (each carrying its
                // own originating timestamp + local_deletion_time) so a surviving
                // older static delete keeps its own LDT instead of inheriting the
                // newest static mutation's value.
                //
                // Issue #821 (finding #2): hard-code prev_size = 0 for the static
                // row and carry the running chain value forward by adding the
                // static row's serialized bytes (see comment above).
                let (static_size, _static_cells) =
                    self.write_static_row_with_prev_size(&merged, latest_ts, ttl, schema, 0)?;
                prev_unfiltered_size += static_size as u64;
            }
        }

        // Merge all mutations sharing a clustering key into a single row each
        // (Issue #716/#717: writing one row per mutation produced duplicate
        // rows with equal clustering — e.g. an INSERT row plus a phantom
        // tombstone-carrier row — which is invalid in the OA format). Rows
        // shadowed by the partition tombstone or a covering range tombstone
        // are dropped during the merge.
        let rows = self.merge_clustering_rows(
            mutations,
            schema,
            schema_has_static,
            partition_floor,
            range_tombstones,
        );

        // Interleave merged rows with range tombstone bound markers in
        // clustering order. Cassandra requires every unfiltered (row or
        // marker) of a partition to appear in clustering order; with equal
        // clustering values, inclusive-start/exclusive-end bounds sort before
        // the row and inclusive-end/exclusive-start bounds sort after it
        // (ClusteringPrefix.Kind.comparedToClustering).
        let mut items: Vec<PartitionItem> = rows.into_iter().map(PartitionItem::Row).collect();
        for rt in range_tombstones {
            items.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            items.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }

        sort_partition_items(&mut items, schema);
        // Issue #1220: coalesce each adjacent close+open bound pair sharing a
        // boundary point into a single two-deletion-time BOUNDARY marker, exactly
        // as Cassandra emits them.
        let items = coalesce_boundaries(items, schema);

        for item in items {
            prev_unfiltered_size = match item {
                PartitionItem::Row(row) => {
                    let (bytes, _cells) =
                        self.write_merged_row_with_prev_size(&row, schema, prev_unfiltered_size)?;
                    bytes as u64
                }
                PartitionItem::Marker {
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                } => self.write_range_bound(
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
                PartitionItem::Boundary {
                    kind,
                    clustering,
                    end_deletion_time,
                    end_local_deletion_time,
                    start_deletion_time,
                    start_local_deletion_time,
                } => self.write_range_boundary(
                    kind,
                    clustering,
                    end_deletion_time,
                    end_local_deletion_time,
                    start_deletion_time,
                    start_local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
            };
        }

        // Write end-of-partition marker
        self.buffer.push(END_OF_PARTITION);

        // Streaming mode: flush this partition to disk and clear the scratch so
        // only one partition is ever resident in memory. No-op in memory mode.
        self.flush_partition()?;

        Ok(partition_offset)
    }

    /// Write a complete partition and collect promoted index blocks for wide partitions.
    ///
    /// Same as [`write_partition`] but also tracks `IndexInfo` block boundaries
    /// for partitions whose uncompressed data exceeds `COLUMN_INDEX_SIZE_BYTES` (64 KiB).
    /// The returned `Vec<PromotedIndexBlock>` contains **one entry per 64 KiB block**;
    /// the caller (SSTableWriter) passes this to `IndexWriter::add_partition_with_promoted`.
    ///
    /// Block sampling rules (mirrors `BigFormatPartitionWriter`):
    /// - A new block is opened whenever the bytes written since the last boundary ≥ 64 KiB.
    /// - The last open block is closed when `END_OF_PARTITION` is reached.
    /// - Only rows with clustering keys contribute `firstName`/`lastName`; tables without
    ///   clustering keys produce blocks with empty prefix bytes (`[0x00]` = header-only VInt).
    /// - A promoted index is only meaningful when 2+ blocks result; the caller checks this.
    pub fn write_partition_with_index_blocks(
        &mut self,
        key: &DecoratedKey,
        mutations: &[Mutation],
        schema: &TableSchema,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &[RangeTombstone],
    ) -> Result<(u64, Vec<PromotedIndexBlock>, PartitionEmitCounts)> {
        // File offset of this partition
        let partition_offset = self.position + self.buffer.len() as u64;

        // Issue #851: tally the rows/cells actually emitted below so Statistics
        // is fed from the single source of truth (this emitter) instead of a
        // parallel re-derivation that kept diverging from Data.db.
        let mut emit = PartitionEmitCounts::default();

        // Note the absolute buffer position at the start of partition data.
        // For streaming mode this is always `self.position` (buffer is empty at start).
        // For in-memory mode this is `self.buffer.len()` (position == 0).
        let partition_buf_start = self.buffer.len();

        // Write partition header (with optional tombstone)
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let mut prev_unfiltered_size = (self.buffer.len() - header_start) as u64;

        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);
        let schema_has_static = schema.columns.iter().any(|c| c.is_static);

        if schema_has_static {
            let merged = collect_static_operations(mutations, schema, partition_floor);

            if merged.is_empty() {
                // Issue #821 (finding #2): static row hard-codes prev_size = 0 and
                // is skipped by the prev-size chain; carry the running value
                // forward by adding the static row's bytes (see the non-indexed
                // path in `write_partition` for the rationale + Cassandra anchor).
                let static_size = self.write_empty_static_row(0, schema)? as u64;
                prev_unfiltered_size += static_size;
            } else {
                // Issue #1018 (roborev HIGH): derive liveness/TTL from the SURVIVING
                // merged ops (per-cell writetimes after the shadow floor), not from
                // mutations that only cleared the floor on their row max. See the
                // non-indexed path above for the rationale.
                let (latest_ts, ttl) = static_liveness_from_ops(&merged).unwrap_or((
                    mutations.first().map(|m| m.timestamp_micros).unwrap_or(0),
                    None,
                ));

                // Issue #764: same per-op LDT preservation as the non-indexed path.
                // Issue #821 (finding #2): prev_size = 0 for the static row; chain
                // carries forward by adding the static row's serialized bytes.
                let (static_size, static_cells) =
                    self.write_static_row_with_prev_size(&merged, latest_ts, ttl, schema, 0)?;
                prev_unfiltered_size += static_size as u64;
                // A non-empty static prelude is one physical row. Issue #851
                // (review): count the static cells the writer ACTUALLY serialized
                // (returned by the write path), not `merged.len()` — a merged
                // static null write is skipped by `write_static_cells`, so it
                // must not be counted as an emitted column.
                emit.rows += 1;
                emit.columns += static_cells;
            }
        }

        let rows = self.merge_clustering_rows(
            mutations,
            schema,
            schema_has_static,
            partition_floor,
            range_tombstones,
        );

        let mut items: Vec<PartitionItem> = rows.into_iter().map(PartitionItem::Row).collect();
        for rt in range_tombstones {
            items.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            items.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }

        sort_partition_items(&mut items, schema);
        // Issue #1220: coalesce adjacent close+open bound pairs into BOUNDARY
        // markers so the wide-partition body matches the plain path byte-for-byte.
        let items = coalesce_boundaries(items, schema);

        // ── Promoted index block tracking ────────────────────────────────
        // Mirrors Cassandra's BigFormatPartitionWriter:
        // - Start block at position 0 (immediately after partition header).
        // - Close block + open a new one whenever accumulated bytes ≥ 64 KiB.
        // - The last block is closed at END_OF_PARTITION.
        //
        // `block_start_buf_offset`: absolute position in `self.buffer` where
        //   the current block began (relative to `partition_buf_start`).
        // `current_block_first_ck`: serialized ClusteringPrefix of the first item
        //   in the current block (empty bytes → "header only" VInt 0x00 for no-CK tables).
        // `current_block_last_ck`: serialized ClusteringPrefix of the most-recent item.

        let mut blocks: Vec<PromotedIndexBlock> = Vec::new();
        let mut block_start_buf_offset = self.buffer.len() - partition_buf_start;
        let mut current_block_first_ck: Option<Vec<u8>> = None;
        let mut current_block_last_ck: Option<Vec<u8>> = None;
        // OSS50 byte-comparable separator (first unfiltered's clustering) for the
        // BTI Rows.db row-index trie (issue #910). `None` until the first item.
        let mut current_block_oss50: Option<Option<Vec<u8>>> = None;

        for item in items {
            // Promoted-index `firstName`/`lastName` ClusteringPrefix bytes for this
            // item (Issue #1186). These are serialized in Cassandra's IndexInfo form
            // (`ClusteringPrefix.serializer.serialize`), which prepends the
            // `Kind.ordinal()` byte — NOT the values-only Data.db row form.
            //
            // The kind byte differs by unfiltered type:
            // - A ROW clustering name is always kind CLUSTERING (0x04); a single
            //   `int` clustering yields the Cassandra-exact 6 bytes `04 00 <int>`.
            // - A range-tombstone MARKER name carries its actual BOUND kind ordinal
            //   (INCL_START_BOUND=1 / EXCL_END_BOUND=0 / INCL_END_BOUND=6 /
            //   EXCL_START_BOUND=7), computed identically to `write_range_bound`,
            //   NOT 0x04 (roborev MEDIUM). The fallbacks produce an empty values
            //   header of the correct kind.
            let ck_bytes: Vec<u8> = match &item {
                PartitionItem::Row(row) => {
                    if let Some(ck) = row.clustering_key {
                        serialize_clustering_prefix_for_index(ck, schema)
                            .unwrap_or_else(|_| empty_clustering_prefix_for_index())
                    } else {
                        empty_clustering_prefix_for_index() // no clustering key
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

            if current_block_first_ck.is_none() {
                current_block_first_ck = Some(ck_bytes.clone());
                // OSS50 byte-comparable separator from the first unfiltered's
                // clustering values (issue #910). Markers and no-CK rows yield
                // None (no row-index separator). Encoding errors degrade to None
                // — the partition then keeps a direct Data.db offset rather than
                // emitting a separator the reader cannot reconstruct.
                let ck_values: Option<Vec<Value>> = match &item {
                    PartitionItem::Row(row) => row
                        .clustering_key
                        .filter(|ck| !ck.columns.is_empty())
                        .map(|ck| ck.columns.iter().map(|(_, v)| v.clone()).collect()),
                    // A marker / boundary yields no row-index separator: the
                    // promoted-index row trie keys off concrete row clusterings,
                    // not tombstone bound names.
                    PartitionItem::Marker { .. } | PartitionItem::Boundary { .. } => None,
                };
                // Per-column clustering ORDER (ASC/DESC). For a DESC column the
                // OSS50 separator bytes must be the REVERSED byte-comparable form
                // (Cassandra `ReversedType`/`ByteSource.invert`: complement every
                // byte) so that descending value order maps to ascending byte
                // order — otherwise the strict-ascending separator check rejects
                // the trie and the wide partition silently falls back to a direct
                // Data.db offset (roborev MEDIUM, issue #910 follow-up).
                let is_reversed: Vec<bool> = schema
                    .clustering_keys
                    .iter()
                    .map(|c| c.order == crate::schema::ClusteringOrder::Desc)
                    .collect();
                current_block_oss50 = Some(ck_values.and_then(|vals| {
                    crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order(
                        &vals,
                        &is_reversed,
                    )
                    .ok()
                }));
            }
            current_block_last_ck = Some(ck_bytes.clone());

            // Write the item
            prev_unfiltered_size = match item {
                PartitionItem::Row(row) => {
                    // One physical row. Issue #851 (review): count the cells the
                    // writer ACTUALLY serialized (returned by the write path),
                    // not `row.ops.len()` — `write_merged_cells` skips null-valued
                    // writes, so a row whose only write is null contributes a live
                    // row with zero columns, matching Data.db exactly.
                    emit.rows += 1;
                    let (bytes, cells) =
                        self.write_merged_row_with_prev_size(&row, schema, prev_unfiltered_size)?;
                    emit.columns += cells;
                    bytes as u64
                }
                PartitionItem::Marker {
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                } => self.write_range_bound(
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
                PartitionItem::Boundary {
                    kind,
                    clustering,
                    end_deletion_time,
                    end_local_deletion_time,
                    start_deletion_time,
                    start_local_deletion_time,
                } => self.write_range_boundary(
                    kind,
                    clustering,
                    end_deletion_time,
                    end_local_deletion_time,
                    start_deletion_time,
                    start_local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
            };

            // Bytes written in the current block so far
            let current_buf_offset = self.buffer.len() - partition_buf_start;
            let block_bytes = (current_buf_offset - block_start_buf_offset) as u64;

            if block_bytes >= COLUMN_INDEX_SIZE_BYTES {
                // Close this block and start a new one
                blocks.push(PromotedIndexBlock {
                    first_name: current_block_first_ck
                        .take()
                        .unwrap_or_else(empty_clustering_prefix_for_index),
                    last_name: current_block_last_ck
                        .take()
                        .unwrap_or_else(empty_clustering_prefix_for_index),
                    offset: block_start_buf_offset as u64,
                    width: block_bytes,
                    oss50_separator: current_block_oss50.take().flatten(),
                });
                block_start_buf_offset = current_buf_offset;
                // first/last reset for next block
                current_block_first_ck = None;
                current_block_last_ck = None;
                current_block_oss50 = None;
            }
        }

        // Write end-of-partition marker
        self.buffer.push(END_OF_PARTITION);

        // Close the final block (if any items were written)
        if let (Some(first), Some(last)) = (current_block_first_ck, current_block_last_ck) {
            let current_buf_offset = self.buffer.len() - partition_buf_start;
            let block_bytes = (current_buf_offset - block_start_buf_offset) as u64;
            if block_bytes > 0 {
                blocks.push(PromotedIndexBlock {
                    first_name: first,
                    last_name: last,
                    offset: block_start_buf_offset as u64,
                    width: block_bytes,
                    oss50_separator: current_block_oss50.take().flatten(),
                });
            }
        }

        self.flush_partition()?;

        Ok((partition_offset, blocks, emit))
    }

    /// Write an empty static-row prelude.
    ///
    /// Required by Cassandra whenever the schema has any static column, even
    /// when this particular partition writes no static cells.
    ///
    /// Binary form:
    /// ```text
    /// [0x80]              ← row_flags = ROW_HAS_EXTENDED_FLAGS only
    /// [0x01]              ← extended_flags = EXTENDED_IS_STATIC
    /// [row_size: VUInt]   ← size of (prev_size VInt + bitmap)
    /// [prev_size: VUInt]
    /// [bitmap: VUInt]     ← all-missing bitmap: (1 << N) - 1 for N static cols
    ///                       (encoded via write_column_subset with empty present set)
    /// ```
    pub(super) fn write_empty_static_row(
        &mut self,
        prev_size: u64,
        schema: &TableSchema,
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        // flags = only HAS_EXTENDED_FLAGS; no timestamp, no TTL, no deletion,
        // no HAS_ALL_COLUMNS, no HAS_COMPLEX_DELETION.
        let flags: u8 = ROW_HAS_EXTENDED_FLAGS;
        self.buffer.push(flags);
        self.buffer.push(EXTENDED_IS_STATIC);

        // Build the row body: just prev_size VInt + column bitmap (all missing).
        let mut body = Vec::new();

        // Column bitmap: "all columns missing" for every static column.
        // write_column_subset with an empty present_set.
        let static_columns = self.static_columns(schema);
        let empty_present: std::collections::HashSet<&str> = std::collections::HashSet::new();
        self.write_column_subset(&mut body, &static_columns, &empty_present)?;

        let prev_size_vint_len = unsigned_len(prev_size);
        let row_body_size = prev_size_vint_len as u64 + body.len() as u64;

        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        encode_unsigned(prev_size, &mut self.buffer);
        self.buffer.extend_from_slice(&body);

        Ok(self.buffer.len() - start_len)
    }

    /// Write partition header
    ///
    /// Format (V5CompressedLegacy / Cassandra BigFormat):
    /// ```text
    /// [key_length: u16 BE]           ← Partition key length (2-byte unsigned short)
    /// [key_bytes]                    ← Raw partition key bytes
    /// [local_deletion_time: i32 BE]  ← i32::MAX for LIVE (DeletionTime.LIVE)
    /// [deletion_timestamp: i64 BE]   ← i64::MIN for LIVE (DeletionTime.LIVE)
    /// ```
    ///
    /// Note: Cassandra uses `ByteBufferUtil.writeWithShortLength()` for the key,
    /// which is a 2-byte BE unsigned short. There is NO separate flags byte.
    /// DeletionTime.LIVE uses sentinel values (Integer.MAX_VALUE, Long.MIN_VALUE).
    pub(super) fn write_partition_header(
        &mut self,
        key: &DecoratedKey,
        tombstone: Option<&PartitionTombstone>,
    ) -> Result<()> {
        // Partition key length (u16 BE, matching Cassandra's writeWithShortLength)
        if key.key.len() > 65535 {
            return Err(Error::InvalidInput(format!(
                "Partition key too large: {} bytes (max 65535)",
                key.key.len()
            )));
        }
        self.buffer
            .write_all(&(key.key.len() as u16).to_be_bytes())?;

        // Partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Partition deletion info.
        //
        // Issue #1741: the on-disk `DeletionTime` layout is format-specific. `da`
        // (BTI, oa `hasUIntDeletionTime`) uses `DeletionTime.Serializer`:
        //   LIVE    = 1 byte `0x80` (IS_LIVE_DELETION);
        //   DELETED = markedForDeleteAt (i64 BE) + localDeletionTime (u32 BE) = 12 bytes.
        // The legacy na/`nb` layout (`legacySerializer`) is always 12 bytes:
        //   localDeletionTime (i32 BE) + markedForDeleteAt (i64 BE), LIVE encoded as
        //   (Integer.MAX_VALUE, Long.MIN_VALUE). Writing the `nb` layout into a `da`
        //   file makes the reader's oa branch misread the LIVE sentinel as a
        //   tombstone (its first byte `0x7F` != `0x80`), which the read-side
        //   shadowing then treats as a partition delete — so `da` MUST use the oa
        //   layout. Authority: DeletionTime.java (Serializer / legacySerializer),
        //   BigFormat.java:409 (`hasUIntDeletionTime`).
        match (self.oa_partition_deletion, tombstone) {
            (true, Some(ts)) => {
                // oa DELETED: markedForDeleteAt (i64) then localDeletionTime (u32).
                self.buffer.write_all(&ts.deletion_time.to_be_bytes())?;
                self.buffer
                    .write_all(&(ts.local_deletion_time as u32).to_be_bytes())?;
            }
            (true, None) => {
                // oa LIVE: single IS_LIVE_DELETION byte.
                self.buffer.write_all(&[0x80])?;
            }
            (false, Some(ts)) => {
                // nb DELETED: localDeletionTime (i32) then markedForDeleteAt (i64).
                self.buffer
                    .write_all(&ts.local_deletion_time.to_be_bytes())?;
                self.buffer.write_all(&ts.deletion_time.to_be_bytes())?;
            }
            (false, None) => {
                // nb LIVE: DeletionTime.LIVE sentinel (Integer.MAX_VALUE, Long.MIN_VALUE).
                self.buffer.write_all(&i32::MAX.to_be_bytes())?;
                self.buffer.write_all(&i64::MIN.to_be_bytes())?;
            }
        }

        Ok(())
    }
}
