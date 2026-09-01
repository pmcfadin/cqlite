//! The [`SlidingPartitionPolicy`] for the streaming-scan / compaction-read
//! `(TableId, RowKey, ScanRow, row_timestamp)` path (issue #1640 K1), split out of
//! `block_emit_windowed.rs` to keep that file under the campsite file-size target
//! (epic #1116).

use super::*;

/// Issue #1640 (K1): [`SlidingPartitionPolicy`] for the streaming-scan /
/// compaction-read `(TableId, RowKey, ScanRow, row_timestamp)` path
/// (`parse_block_with_timestamps` family). Read-side shadowing is active only
/// when the parser was built for a user-facing query read (`read_shadowing`);
/// physical consumers leave `shadow` `None` and see every on-disk row.
pub(super) struct TimestampPolicy<'a> {
    parser: &'a V5CompressedLegacyParser,
    table_id: TableId,
    partition_key: RowKey,
    /// Issue #1741 per-partition read-side shadow; `None` for physical reads.
    shadow: Option<PartitionShadow>,
    /// Issue #480 static cells accumulated for merge into each clustering row.
    /// Issue #1642 (K3): positional `RowCells`, matching the decoder emit.
    static_cells: RowCells,
    /// Issue #3095: the static row's own write timestamp, carried so a
    /// static-only partition's synthesized row reports the SAME authoritative
    /// timestamp rule (`row_write_timestamp`) every other row does.
    static_row_ts: i64,
    /// Issue #3095: whether this partition yielded at least one CLUSTERING row —
    /// Cassandra's `partition.hasNext()`, which counts clustering rows only
    /// because the static row is delivered out of band by `partition.staticRow()`
    /// (`db/rows/BaseRowIterator.java`).
    emitted_clustering_row: bool,
}

impl<'a> TimestampPolicy<'a> {
    pub(super) fn new(parser: &'a V5CompressedLegacyParser) -> Self {
        Self {
            parser,
            table_id: TableId::new(format!("{}.{}", parser.keyspace, parser.table_name)),
            partition_key: RowKey::new(Vec::new()),
            shadow: None,
            static_cells: Vec::new(),
            static_row_ts: 0,
            emitted_clustering_row: false,
        }
    }
}

#[cfg(test)]
impl TimestampPolicy<'_> {
    /// Test seam (issue #3095): install a live STATIC row for the current partition
    /// exactly as `on_data_row`'s `is_static` branch does, so
    /// [`TimestampPolicy::on_partition_close`]'s `complete` / `read_shadowing` /
    /// visible-row decisions can be asserted DIRECTLY rather than only end-to-end.
    pub(super) fn seed_static_for_test(
        &mut self,
        partition_key: RowKey,
        schema: &TableSchema,
        row_ts: i64,
    ) {
        let static_column = schema
            .columns
            .iter()
            .find(|c| c.is_static)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "s".to_string());
        self.partition_key = partition_key;
        self.static_cells = vec![(Arc::from(static_column.as_str()), Value::text("static-val"))];
        self.static_row_ts = row_ts;
    }

    /// Test seam: mark that the partition produced a VISIBLE clustering row.
    pub(super) fn note_visible_row_for_test(&mut self) {
        self.emitted_clustering_row = true;
    }
}

impl SlidingPartitionPolicy for TimestampPolicy<'_> {
    type Row = (TableId, RowKey, ScanRow, i64);

    fn on_partition_open(
        &mut self,
        partition_key: RowKey,
        partition_deletion: Option<(i64, i32)>,
        schema: &TableSchema,
        _pending: &mut Vec<Self::Row>,
    ) {
        self.partition_key = partition_key;
        // Issue #3095 (roborev BLOCKER): RESET every partition-scoped field here.
        // Not defensive style — required for correctness: the driver calls this
        // hook again when a mid-partition `NeedMore` makes the caller refill and
        // RE-PARSE the same partition from its start (its `pending` rows are
        // discarded), and a policy instance may be reused across partitions. Left
        // un-reset, `emitted_clustering_row` from an earlier partition/attempt would
        // permanently suppress a LATER static-only partition's row, and
        // `static_cells` would leak one partition's static values into the next.
        self.static_cells = Vec::new();
        self.static_row_ts = 0;
        self.emitted_clustering_row = false;
        // Issue #1741 (F2): reuse the parser's once-per-read `now_secs`, not a
        // per-block wall-clock sample, so all blocks of one scan share one `now`.
        self.shadow = self.parser.read_shadowing.then(|| {
            PartitionShadow::open(
                self.parser.now_secs,
                partition_deletion,
                clustering_reversed_flags(schema),
            )
        });
    }

    fn on_range_marker(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        _pending: &mut Vec<Self::Row>,
    ) -> MarkerOutcome {
        // Issue #1741: on the user-facing scan path decode the marker and feed
        // the range-tombstone FSM so covered rows are shadowed; the physical path
        // (shadow == None) only advances past it.
        if let Some(sh) = self.shadow.as_mut() {
            match self
                .parser
                .parse_range_tombstone_marker_full(data, offset, schema)
            {
                Ok((bv, bk, dp, ds, next_offset)) => {
                    if sh.feed_range_marker(bv, bk, dp, ds).is_err() {
                        return MarkerOutcome::Stop;
                    }
                    MarkerOutcome::Advanced(next_offset)
                }
                Err(_) => MarkerOutcome::Stop,
            }
        } else {
            match self
                .parser
                .skip_range_tombstone_marker(data, offset, schema)
            {
                Ok(next_offset) => MarkerOutcome::Advanced(next_offset),
                Err(_) => MarkerOutcome::Stop,
            }
        }
    }

    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> Result<Option<usize>> {
        match self.parser.parse_row_data_with_offset(
            data,
            offset,
            Some(schema),
            reader,
            false,
            resolution,
            self.shadow.as_ref(),
        ) {
            Ok((
                mut cells,
                _row_cell_meta,
                row_header_opt,
                next_offset,
                is_static,
                _complex_meta,
            )) => {
                // Issue #932 (K1): the ONE row-write-timestamp coexistence rule.
                let row_ts = row_write_timestamp(&row_header_opt);

                if is_static {
                    // Issue #1741 (Finding 1): drop a shadowed/expired static row's
                    // cells so a surviving clustering row does not resurface stale
                    // static data. No-op when shadowing is off.
                    let static_hidden = self.shadow.as_ref().is_some_and(|sh| {
                        row_header_opt
                            .as_ref()
                            .is_some_and(|h| sh.row_hidden(h, &[]))
                    });
                    self.static_cells = if static_hidden { Vec::new() } else { cells };
                    self.static_row_ts = row_ts;
                } else {
                    // Issue #1741: hide rows shadowed by a partition/range tombstone
                    // or expired by TTL (matching a Cassandra SELECT). No-op on the
                    // physical path (shadow == None).
                    let hidden = self.shadow.as_ref().is_some_and(|sh| {
                        row_header_opt.as_ref().is_some_and(|h| {
                            let clustering = if sh.needs_clustering() {
                                extract_clustering_values(&cells, schema)
                            } else {
                                Vec::new()
                            };
                            sh.row_hidden(h, &clustering)
                        })
                    });

                    // Issue #505/#932 row-tombstone display rule + issue #480/#1642
                    // static merge. Issue #3095: on a user-facing SELECT read the
                    // tombstone decision is taken over the row's OWN cells FIRST, so a
                    // static value cannot revive a row-tombstoned row; PHYSICAL
                    // consumers (compaction) keep the historical order and stay
                    // byte-identical. See `build_display_row_read_path`.
                    let row_value = if self.shadow.is_some() {
                        build_display_row_read_path(
                            cells,
                            &self.static_cells,
                            row_header_opt.as_ref(),
                            schema,
                        )
                    } else {
                        merge_static_cells(&mut cells, &self.static_cells);
                        build_display_row(cells, row_header_opt.as_ref(), schema)
                    };

                    if !hidden {
                        // Issue #3095 (roborev + rust-reviewer BLOCKER): Cassandra's
                        // `partition.hasNext()` is evaluated over the ALREADY-FILTERED
                        // `RowIterator` (`UnfilteredRowIterators.filter`), so a purged
                        // row does NOT count as a row. A `ScanRow::Marker` is a pure
                        // row tombstone that every user-facing consumer suppresses
                        // (`filter_tombstone` / `build_row_from_scan`'s `into_cells`),
                        // strictly AFTER this point — counting it would make a
                        // partition whose ONLY clustering rows are deleted emit
                        // nothing, where Cassandra (and the k-way merge arm) return
                        // the static row. Count only a VISIBLE row.
                        self.emitted_clustering_row |= row_is_visible(&row_value);
                        pending.push((
                            self.table_id.clone(),
                            self.partition_key.clone(),
                            row_value,
                            row_ts,
                        ));
                    }
                }
                Ok(Some(next_offset))
            }
            // Issue #3721: a per-column decode failure reaches the caller instead of
            // being folded into the `Ok(None)` end-of-partition signal, which would
            // return a SHORT partition from a successful scan.
            Err(e) if column_decode_error::is_column_decode(&e) => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// Issue #3095: Cassandra's static-content-on-an-empty-partition rule.
    ///
    /// `SelectStatement.processPartition()` (cassandra-5.0.8, L1099-1120): when
    /// `!partition.hasNext()` — no CLUSTERING rows — and the out-of-band
    /// `partition.staticRow()` is not empty, the query returns EXACTLY ONE result
    /// row whose clustering and REGULAR columns are null
    /// (`default: result.add((ByteBuffer) null)`) and whose STATIC columns carry
    /// the static row's values. That branch `return`s, so it is mutually exclusive
    /// with the per-row loop — which is why this fires only when no clustering row
    /// was emitted for the partition.
    ///
    /// Gated on `read_shadowing`, i.e. user-facing SELECT reads only: a PHYSICAL
    /// consumer (`verify`, `get_all_entries`, compaction, delta-scan) must see
    /// exactly the on-disk unfiltereds, so it never gets this synthesized row and
    /// sstabledump/compaction parity is byte-unchanged.
    ///
    /// `self.static_cells` is already empty when the static row was shadowed by a
    /// partition tombstone or expired by its own TTL (#1741 Finding 1), so a stale
    /// static row can never resurface here. The clustering-restriction half of
    /// `returnStaticContentOnPartitionWithNoRows()` is enforced downstream: the
    /// row carries NULL clustering and NULL regular columns, so any restriction on
    /// one of those (the only way `queriesFullPartitions()` becomes false) rejects
    /// it under the reader's/producer's three-valued predicate rule, which keeps a
    /// row only when the predicate is definitely True.
    fn on_partition_close(
        &mut self,
        schema: &TableSchema,
        pending: &mut Vec<Self::Row>,
        complete: bool,
    ) {
        // `complete` (issue #3095): synthesize ONLY at a STRUCTURALLY-confirmed
        // partition end. A truncated/corrupt body (buffer exhausted with no
        // END_OF_PARTITION, an unrepresentable range marker, an unparseable row) was
        // only PARTLY observed, so "this partition yielded no clustering row" is not
        // knowable — fail closed rather than invent a row. This is the SAME guard
        // the two `block_emit*` decode sites apply via their own `partition_complete`,
        // so all three paths now agree and the hook's doc is literally true.
        if !complete
            || !self.parser.read_shadowing
            || self.emitted_clustering_row
            || self.static_cells.is_empty()
        {
            return;
        }
        // Cells are non-empty and hold at least one static (non-key) column, so
        // the shared display rule yields `ScanRow::Row`; the header is not needed
        // (a shadowed/tombstoned static row already emptied `static_cells`).
        let row_value = build_display_row(std::mem::take(&mut self.static_cells), None, schema);
        pending.push((
            self.table_id.clone(),
            self.partition_key.clone(),
            row_value,
            self.static_row_ts,
        ));
    }
}
