use super::*;

// Issue #1116 (campsite rule): the DELTA-SCAN emit shape. Declared here rather
// than in the parent `mod.rs` because it is an arm of THIS walk; gated at the
// `mod` so the whole file compiles only with the feature, exactly as the
// function it holds was gated.
#[cfg(feature = "delta-scan")]
mod delta_emit;

impl V5CompressedLegacyParser {
    /// Parse decompressed block into (TableId, RowKey, ScanRow) entries
    ///
    /// # Arguments
    /// * `data` - Decompressed block bytes
    /// * `schema` - Optional table schema for type-aware parsing
    /// * `reader` - Reference to SSTableReader for value parsing
    ///
    /// # Returns
    /// * `Ok(Vec<(TableId, RowKey, ScanRow)>)` - Parsed entries
    /// * `Err(Error)` - Parse error with context
    pub fn parse_block(
        &self,
        data: &[u8],
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let mut results = Vec::new();
        self.parse_block_emit(data, extent, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Parse a block and return both row values and per-cell write metadata.
    ///
    /// Identical to [`parse_block`] but the returned vector carries a fourth element:
    /// the per-cell `CellWriteMetadata` map (column name → metadata). Used by the
    /// executor when `ProjectionFlags::include_cell_metadata` is set (i.e. when the
    /// query contains `WRITETIME(col)` or `TTL(col)` expressions).
    pub fn parse_block_with_cell_metadata(
        &self,
        data: &[u8],
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<ParsedBlockWithMeta> {
        let mut results = Vec::new();
        self.parse_block_emit_with_metadata(data, extent, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Internal streaming variant of `parse_block_with_cell_metadata`.
    fn parse_block_emit_with_metadata<F>(
        &self,
        data: &[u8],
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            (TableId, RowKey, ScanRow, HashMap<String, CellWriteMetadata>),
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        // Issue #1046: build the header→schema column resolution ONCE per block,
        // reused across every partition/row below — zero per-row schema-lookup alloc.
        let resolution = RowColumnResolution::build(schema, reader);

        let mut offset = 0;
        let mut partition_index = 0;
        // Issue #1741 (F2): read-time TTL clock — captured ONCE per parser (== once per
        // read/scan operation) in `V5CompressedLegacyParser::new`, reused for every
        // block/partition here so a scan crossing an expiration-second boundary decides
        // all rows with the SAME `now`.
        let now_secs = self.now_secs;
        // Issue #1741: authoritative per-clustering-column reversal flags (DESC),
        // built ONCE per block so range-tombstone coverage compares clustering
        // prefixes in physical storage order. Bounded by clustering arity.
        let clustering_reversed = clustering_reversed_flags(schema);

        while offset < data.len() {
            // Parse partition header: returns (RowKey, next_data_offset, deletion)
            let (partition_key, next_data_offset, partition_deletion) =
                match self.parse_partition_header_full(data, offset) {
                    Ok(ph) => ph,
                    // Issue #3928: on a PROVEN-COMPLETE buffer no further bytes
                    // can arrive to finish this header, so the bare `break` was a
                    // silent truncation of the whole REMAINDER of the walk — and
                    // when the FIRST partition's header is the undecodable one,
                    // of everything. Measured on a real `da` fixture whose first
                    // partition's `DeletionTime` discriminator was flipped to a
                    // value Cassandra's own reader throws on: this route
                    // (`bti_scan_with_metadata_cancellable` → here) answered `Ok`
                    // with **0 of 468 rows**, which is the 0-rows-when-present
                    // failure. Mid-window the break STAYS: a header can
                    // legitimately straddle a chunk-covering window's tail.
                    Err(e) => {
                        if extent.is_complete() {
                            return Err(e);
                        }
                        break;
                    }
                };

            let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));
            offset = next_data_offset;
            partition_index += 1;

            // Issue #1741: per-partition read-side shadowing (WRITETIME/TTL projection
            // path), active ONLY for user-facing query reads (`self.read_shadowing`).
            // Un-gated; hides partition/range-tombstone-shadowed and TTL-expired rows
            // to match a Cassandra SELECT. `None` for physical consumers.
            let mut shadow = self.read_shadowing.then(|| {
                PartitionShadow::open(now_secs, partition_deletion, clustering_reversed.clone())
            });

            // Issue #1642 (K3): static cells accumulate as a positional `RowCells`
            // vector, matching the decoder's positional emit. Metadata stays keyed.
            let mut static_cells: RowCells = Vec::new();
            let mut static_cell_meta: HashMap<String, CellWriteMetadata> = HashMap::new();
            let mut row_count = 0;
            // Issue #3095: Cassandra's `partition.hasNext()` (clustering rows only)
            // plus proof this call saw the partition's END — see the
            // static-only-partition emission after the loop.
            let mut emitted_clustering_row = false;
            let mut partition_complete = false;

            loop {
                if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                    offset += 1;
                    partition_complete = true;
                    break;
                }

                if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                    // Issue #1741: when shadowing is active, decode + feed the
                    // range-tombstone FSM so covered rows are shadowed; otherwise
                    // (physical read) only advance past the marker.
                    if let Some(sh) = shadow.as_mut() {
                        match self.parse_range_tombstone_marker_full(data, offset, schema) {
                            Ok((bv, bk, dp, ds, next_offset)) => {
                                // Issue #3721: the marker is FRAMED (`next_offset`
                                // is bound, so the body continues) and the FSM
                                // refuses only an unrepresentable bound kind — a
                                // `break` here reported `Ok` while dropping the
                                // tombstone and every later row of the partition.
                                if let Err(e) = sh.feed_range_marker(bv, bk, dp, ds) {
                                    return Err(range_marker_error::range_marker_refused(
                                        e,
                                        &partition_index,
                                        offset,
                                        next_offset,
                                    ));
                                }
                                offset = next_offset;
                                continue;
                            }
                            // Issue #3721 (roborev job 78): a marker that
                            // cannot be PARSED is corruption here, never a
                            // framing terminator — this block is fully buffered,
                            // so no refill can complete it, and `break` reported
                            // `Ok` with the tombstone AND every later row of the
                            // partition gone. See `range_marker_error`'s docs.
                            Err(cause) => {
                                return Err(
                                    range_marker_error::unparseable_marker_in_buffered_block(
                                        cause,
                                        &partition_index,
                                        offset,
                                    ),
                                )
                            }
                        }
                    }
                    match self.skip_range_tombstone_marker(data, offset, schema) {
                        Ok(next_offset) => {
                            offset = next_offset;
                            continue;
                        }
                        // Same decision on the PHYSICAL path (no shadowing).
                        Err(cause) => {
                            return Err(range_marker_error::unparseable_marker_in_buffered_block(
                                cause,
                                &partition_index,
                                offset,
                            ))
                        }
                    }
                }

                match self.parse_row_data_with_offset(
                    data,
                    offset,
                    Some(schema),
                    reader,
                    true,
                    &resolution,
                    shadow.as_ref(),
                ) {
                    Ok((
                        mut cells,
                        row_cell_meta_opt,
                        row_header_opt,
                        next_offset,
                        is_static,
                        _complex_meta,
                    )) => {
                        let mut row_cell_meta = row_cell_meta_opt.unwrap_or_default();
                        offset = next_offset;
                        row_count += 1;

                        if is_static {
                            // Issue #1741 (Finding 1): drop a static row's cells/metadata
                            // when the static row is itself shadowed by the partition
                            // tombstone or expired by its own TTL, so a surviving
                            // clustering row does not resurface the stale static value.
                            // Static rows have no clustering key (empty clustering); no-op
                            // when shadowing is off.
                            let static_hidden = shadow.as_ref().is_some_and(|sh| {
                                row_header_opt
                                    .as_ref()
                                    .is_some_and(|h| sh.row_hidden(h, &[]))
                            });
                            if static_hidden {
                                static_cells = Vec::new();
                                static_cell_meta = HashMap::new();
                            } else {
                                static_cells = cells;
                                static_cell_meta = row_cell_meta;
                            }
                        } else {
                            // Merge static metadata into the clustering row
                            // (clustering-row-wins; positional, issue #1642). The CELL
                            // merge happens below, AFTER the row-tombstone decision on
                            // a read path (issue #3095).
                            for (k, v) in &static_cell_meta {
                                row_cell_meta.entry(k.clone()).or_insert_with(|| v.clone());
                            }

                            // Issue #1741: hide partition/range-tombstone-shadowed and
                            // TTL-expired rows (matching a Cassandra SELECT). Active
                            // only for query reads (shadow is `Some`). A row reduced to
                            // only its primary key by per-cell filtering is hidden here
                            // too: the dropped cells still fold into the row aggregate,
                            // so `row_hidden` sees the whole row as shadowed/expired.
                            let hidden = shadow.as_ref().is_some_and(|sh| {
                                row_header_opt.as_ref().is_some_and(|h| {
                                    let clustering = if sh.needs_clustering() {
                                        extract_clustering_values(&cells, schema)
                                    } else {
                                        Vec::new()
                                    };
                                    sh.row_hidden(h, &clustering)
                                })
                            });

                            // Issue #505/#932 row-tombstone display rule. Issue #3095:
                            // on a user-facing SELECT read the decision is taken over
                            // the row's OWN cells FIRST so a static value cannot revive
                            // a row-tombstoned row; physical consumers keep the
                            // historical order. See `build_display_row_read_path`.
                            let row_value = if shadow.is_some() {
                                build_display_row_read_path(
                                    cells,
                                    &static_cells,
                                    row_header_opt.as_ref(),
                                    schema,
                                )
                            } else {
                                merge_static_cells(&mut cells, &static_cells);
                                build_display_row(cells, row_header_opt.as_ref(), schema)
                            };

                            if !hidden {
                                // Issue #3095: only a VISIBLE row counts (see
                                // `row_is_visible`) — a suppressed `ScanRow::Marker`
                                // must not hide a static-only partition's row.
                                emitted_clustering_row |= row_is_visible(&row_value);
                                match emit((
                                    table_id.clone(),
                                    partition_key.clone(),
                                    row_value,
                                    row_cell_meta,
                                ))? {
                                    std::ops::ControlFlow::Continue(()) => {}
                                    std::ops::ControlFlow::Break(()) => return Ok(()),
                                }
                            }
                        }

                        if offset >= data.len() {
                            break;
                        }

                        if self.peek_is_partition_header(data, offset) {
                            tracing::debug!(
                                "V5CompressedLegacy: Partition {} detected at offset {} after {} rows",
                                partition_index + 1, offset, row_count
                            );
                            partition_complete = true;
                            break;
                        }
                    }
                    Err(e) => {
                        // Issue #3721: `Err` here is normally the end-of-partition
                        // signal; a per-column decode failure is NOT, and only
                        // `column_decode_error` decides which is which.
                        // Issue #3782 composes with it: the tolerance decision belongs
                        // to the CALLER's declared extent, never to this parse. When the
                        // buffer is COMPLETE no further bytes can arrive, so the
                        // discrimination above is authoritative and a column decode
                        // failure is data loss. When it is INCOMPLETE the same failure is
                        // the ordinary straddling-row case a refill fixes, so the
                        // tolerant break stays — measured on #3782: over 42 well-formed
                        // corpus tables the tolerant path fires 614 times, every one of
                        // them with an incomplete extent and none with a complete one.
                        // #3782 FIRST: a proven-complete buffer makes ANY failure data
                        // loss. Then #3721's discrimination for the incomplete case, so a
                        // column decode failure still reaches the caller that owns the
                        // tolerance decision rather than being swallowed here.
                        if extent.is_complete() {
                            return Err(e);
                        }
                        column_decode_error::end_of_partition_or_bail(
                            e,
                            partition_index,
                            row_count,
                            offset,
                        )?;
                        break;
                    }
                }
            }

            // Issue #3095: Cassandra's static-content-on-an-empty-partition rule
            // (`SelectStatement.processPartition()`, cassandra-5.0.8 L1099-1120) on
            // the WRITETIME/TTL-projection decode, so a `SELECT … WRITETIME(s)`
            // returns the same result SHAPE as a plain `SELECT *`. Same guards and
            // rationale as the primary site in `block_emit_windowed.rs`: user-facing
            // SELECT reads only (`read_shadowing`), a CONFIRMED-complete partition,
            // no clustering row emitted, and a live static row.
            //
            // Carries the SAME stated residual as that site: `partition_complete` is
            // established PER BLOCK, so a static-only partition whose
            // `END_OF_PARTITION` byte lands in the next decompressed block yields 0
            // rows instead of 1 — fail-closed, and absent on the sliding-window path,
            // which has an explicit `at_final_chunk` contract.
            if self.read_shadowing
                && partition_complete
                && !emitted_clustering_row
                && !static_cells.is_empty()
            {
                let row_value = build_display_row(static_cells, None, schema);
                match emit((
                    table_id.clone(),
                    partition_key.clone(),
                    row_value,
                    static_cell_meta,
                ))? {
                    std::ops::ControlFlow::Continue(()) => {}
                    std::ops::ControlFlow::Break(()) => return Ok(()),
                }
            }
        }

        Ok(())
    }

    /// Streaming variant of [`parse_block`]: invokes `emit` for each parsed
    /// `(TableId, RowKey, ScanRow)` entry instead of collecting them into a `Vec`,
    /// so callers can forward rows into a bounded channel without materializing
    /// the whole block at once (issue #790). Returning `ControlFlow::Break` from
    /// `emit` stops parsing early — used when the streaming consumer is dropped.
    pub fn parse_block_emit<F>(
        &self,
        data: &[u8],
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, ScanRow)) -> Result<std::ops::ControlFlow<()>>,
    {
        // Whole-block decode: no within-partition row-body window narrowing.
        self.parse_block_emit_windowed(data, extent, schema, reader, None, emit)
    }
}
