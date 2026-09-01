use super::*;

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
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let mut results = Vec::new();
        self.parse_block_emit(data, schema, reader, |entry| {
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
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<ParsedBlockWithMeta> {
        let mut results = Vec::new();
        self.parse_block_emit_with_metadata(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Internal streaming variant of `parse_block_with_cell_metadata`.
    fn parse_block_emit_with_metadata<F>(
        &self,
        data: &[u8],
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
                    Err(_) => break,
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
                                        partition_index,
                                        offset,
                                        next_offset,
                                    ));
                                }
                                offset = next_offset;
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                    match self.skip_range_tombstone_marker(data, offset, schema) {
                        Ok(next_offset) => {
                            offset = next_offset;
                            continue;
                        }
                        Err(_) => break,
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

    /// Delta-scan variant of [`parse_block_emit_with_metadata`] (Epic #696, Issue #698).
    ///
    /// Identical in parsing strategy to [`parse_block_emit_with_metadata`] but emits
    /// **static rows separately** instead of merging them into the first clustering row.
    /// The emit closure receives five values per row:
    ///
    /// ```text
    /// (partition_key, cells, cell_meta, row_liveness_ts, is_static)
    /// ```
    ///
    /// - `cells`            — column-name → decoded `Value` (including clustering cols).
    /// - `cell_meta`        — column-name → `CellWriteMetadata` (writetime + TTL).
    /// - `row_liveness_ts`  — `Some(ts_µs)` when the row was created with `INSERT` and
    ///   carries a primary-key liveness timestamp (`HAS_TIMESTAMP` flag).  `None` for
    ///   `UPDATE`-only rows (no pk liveness).
    /// - `is_static`        — `true` for static-column rows (emit as `StaticUpsert`).
    ///
    /// Row tombstones (rows with `HAS_DELETION`) are emitted with a non-empty `cell_meta`
    /// and an empty `cells` map; callers must detect them via a missing row-liveness
    /// timestamp combined with `row_header_is_deletion = true` in `cell_meta`.
    ///
    /// Note: Range tombstone markers are *skipped* in this version — they are emitted as
    /// errors by the delta-scan caller per Issue #699 scope boundaries.
    // ComplexColumnMeta is intentionally restricted to the reader module; the
    // closure bound here is not part of the public API surface.
    #[allow(private_bounds)]
    #[cfg(feature = "delta-scan")]
    pub fn parse_block_emit_delta<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            (
                RowKey,
                HashMap<String, Value>,
                HashMap<String, CellWriteMetadata>,
                Option<i64>, // row-level liveness timestamp (HAS_TIMESTAMP), µs
                bool,        // is_static
                bool,        // is_row_tombstone
                Option<i64>, // marked_for_delete_at (row tombstone deletion time, or None)
                // --- Issue #699 tombstone extensions ---
                Option<(Vec<Value>, bool, Vec<Value>, bool, i64)>, // range tombstone info: (start_values, start_inclusive, end_values, end_inclusive, deleted_at)
                bool,                                              // is_partition_tombstone
                // --- Issue #700 DS4 collection extensions ---
                HashMap<String, ComplexColumnMeta>, // per-column complex collection metadata
                // --- Issue #702 TTL liveness expiry ---
                Option<i64>, // liveness expires_at in microseconds (from HAS_TTL ldt, epoch-s * 1_000_000)
            ),
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy delta-scan requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        // Issue #1046: build the header→schema column resolution ONCE per block.
        let resolution = RowColumnResolution::build(schema, reader);

        let mut offset = 0;
        let mut partition_index = 0;

        while offset < data.len() {
            let (partition_key, next_data_offset, partition_deletion) = self
                .parse_partition_header_full(data, offset)
                .map_err(|e| {
                    Error::corruption(format!(
                        "delta-scan: partition-header parse error at offset {} in {}.{}: {}",
                        offset, self.keyspace, self.table_name, e
                    ))
                })?;

            offset = next_data_offset;
            partition_index += 1;
            let mut row_count = 0;

            // ----------------------------------------------------------------
            // Issue #699: emit PartitionDelete if the partition header carried
            // a tombstone (markedForDeleteAt != LIVE sentinel).
            // ----------------------------------------------------------------
            if let Some((deleted_at, _partition_ldt)) = partition_deletion {
                match emit((
                    partition_key.clone(),
                    HashMap::new(),
                    HashMap::new(),
                    None,
                    false,
                    false,
                    Some(deleted_at),
                    None,
                    true,           // is_partition_tombstone
                    HashMap::new(), // no collection metadata for tombstones
                    None,           // no liveness TTL expiry for tombstones
                ))? {
                    std::ops::ControlFlow::Continue(()) => {}
                    std::ops::ControlFlow::Break(()) => return Ok(()),
                }
            }

            // Buffer for in-flight range tombstone start bound.
            //
            // A range tombstone in Cassandra SSTable format is represented as a pair of
            // adjacent "range tombstone markers":
            //   INCL_START_BOUND (kind 1) or EXCL_START_BOUND (kind 7)  ← start
            //   INCL_END_BOUND   (kind 6) or EXCL_END_BOUND   (kind 0)  ← end
            //
            // Or as a single "boundary" marker (kind 2 or 5) that encodes both the end
            // of the previous range and the start of the next range simultaneously (used
            // when two ranges share a clustering-key boundary point).
            //
            // We buffer the start bound here and emit a RangeDelete when the end arrives.
            //
            // Tuple: (start_values, start_inclusive, deleted_at)
            let mut pending_range_start: Option<(Vec<Value>, bool, i64)> = None;

            loop {
                if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                    offset += 1;
                    break;
                }

                if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                    // Issue #699: Decode the range tombstone marker and emit RangeDelete.
                    let (
                        bound_values,
                        bound_kind,
                        deleted_at_primary,
                        deleted_at_secondary,
                        next_offset,
                    ) = self
                        .parse_range_tombstone_marker_full(data, offset, schema)
                        .map_err(|e| {
                            Error::corruption(format!(
                                "delta-scan: range-tombstone-marker parse error in partition {} \
                                     at offset {} in {}.{}: {}",
                                partition_index, offset, self.keyspace, self.table_name, e
                            ))
                        })?;
                    offset = next_offset;

                    // Decode bound kind into start/end semantics.
                    //
                    // ClusteringPrefix.Kind ordinals (ClusteringBoundOrBoundary.java):
                    //   0 = EXCL_END_BOUND              → end,   exclusive  (<  ck)
                    //   1 = INCL_START_BOUND             → start, inclusive  (>= ck)
                    //   2 = EXCL_END_INCL_START_BOUNDARY → end excl + start incl (2 del times)
                    //   5 = INCL_END_EXCL_START_BOUNDARY → end incl + start excl (2 del times)
                    //   6 = INCL_END_BOUND               → end,   inclusive  (<= ck)
                    //   7 = EXCL_START_BOUND             → start, exclusive  (>  ck)
                    match bound_kind {
                        1 | 7 => {
                            // Simple start bound: buffer and wait for the matching end.
                            let is_inclusive = bound_kind == 1; // 1=INCL_START, 7=EXCL_START
                            pending_range_start =
                                Some((bound_values, is_inclusive, deleted_at_primary));
                        }
                        0 | 6 => {
                            // Simple end bound: pair with buffered start and emit RangeDelete.
                            let is_end_inclusive = bound_kind == 6; // 6=INCL_END, 0=EXCL_END
                            let (start_values, start_inclusive, _start_del) =
                                pending_range_start.take().unwrap_or_else(|| {
                                    // End bound with no preceding start bound — treat as
                                    // open (unbounded) start.  Hard-error policy: we faithfully
                                    // represent this as an open bound rather than dropping it.
                                    (Vec::new(), false, deleted_at_primary)
                                });
                            // Cassandra puts the authoritative markedForDeleteAt on both
                            // bounds of a simple range (they are the same value); use the
                            // end bound's primary deletion time.
                            let range_info = Some((
                                start_values,
                                start_inclusive,
                                bound_values,
                                is_end_inclusive,
                                deleted_at_primary,
                            ));
                            match emit((
                                partition_key.clone(),
                                HashMap::new(),
                                HashMap::new(),
                                None,
                                false,
                                false,
                                Some(deleted_at_primary),
                                range_info,
                                false,          // is_partition_tombstone
                                HashMap::new(), // no collection metadata for tombstones
                                None,           // no liveness TTL expiry for tombstones
                            ))? {
                                std::ops::ControlFlow::Continue(()) => {}
                                std::ops::ControlFlow::Break(()) => return Ok(()),
                            }
                        }
                        2 => {
                            // EXCL_END_INCL_START_BOUNDARY (kind 2):
                            //   primary   = end of the previous range, exclusive
                            //   secondary = start of the new range, inclusive
                            //
                            // Close the pending range (if any) first.
                            if let Some((start_values, start_inclusive, _)) =
                                pending_range_start.take()
                            {
                                let range_info = Some((
                                    start_values,
                                    start_inclusive,
                                    bound_values.clone(),
                                    false, // EXCL_END
                                    deleted_at_primary,
                                ));
                                match emit((
                                    partition_key.clone(),
                                    HashMap::new(),
                                    HashMap::new(),
                                    None,
                                    false,
                                    false,
                                    Some(deleted_at_primary),
                                    range_info,
                                    false,
                                    HashMap::new(), // no collection metadata for tombstones
                                    None,           // no liveness TTL expiry for tombstones
                                ))? {
                                    std::ops::ControlFlow::Continue(()) => {}
                                    std::ops::ControlFlow::Break(()) => return Ok(()),
                                }
                            }
                            // Open new range starting at bound_values (inclusive).
                            let new_del_at = deleted_at_secondary.unwrap_or(deleted_at_primary);
                            pending_range_start = Some((bound_values, true, new_del_at));
                        }
                        5 => {
                            // INCL_END_EXCL_START_BOUNDARY (kind 5):
                            //   primary   = end of the previous range, inclusive
                            //   secondary = start of the new range, exclusive
                            if let Some((start_values, start_inclusive, _)) =
                                pending_range_start.take()
                            {
                                let range_info = Some((
                                    start_values,
                                    start_inclusive,
                                    bound_values.clone(),
                                    true, // INCL_END
                                    deleted_at_primary,
                                ));
                                match emit((
                                    partition_key.clone(),
                                    HashMap::new(),
                                    HashMap::new(),
                                    None,
                                    false,
                                    false,
                                    Some(deleted_at_primary),
                                    range_info,
                                    false,
                                    HashMap::new(), // no collection metadata for tombstones
                                    None,           // no liveness TTL expiry for tombstones
                                ))? {
                                    std::ops::ControlFlow::Continue(()) => {}
                                    std::ops::ControlFlow::Break(()) => return Ok(()),
                                }
                            }
                            // Open new range starting at bound_values (exclusive).
                            let new_del_at = deleted_at_secondary.unwrap_or(deleted_at_primary);
                            pending_range_start = Some((bound_values, false, new_del_at));
                        }
                        unknown => {
                            return Err(Error::corruption(format!(
                                "delta-scan: unknown range tombstone bound kind {} at offset {} \
                                 in {}.{} (partition key {:?}) — cannot represent faithfully \
                                 (no-heuristics mandate, issue #28)",
                                unknown, offset, self.keyspace, self.table_name, partition_key.0
                            )));
                        }
                    }

                    continue;
                }

                match self.parse_row_data_with_offset(
                    data,
                    offset,
                    Some(schema),
                    reader,
                    true,
                    &resolution,
                    // Delta-scan is a physical consumer: no read-side shadowing.
                    None,
                ) {
                    Ok((
                        cells,
                        row_cell_meta_opt,
                        row_header_opt,
                        next_offset,
                        is_static,
                        complex_meta,
                    )) => {
                        let cell_meta = row_cell_meta_opt.unwrap_or_default();
                        // DS4 (Issue #700): pass ComplexColumnMeta to the emit closure so the
                        // delta-scan caller can set `replaced` and surface element tombstone counts.
                        let col_meta_map = complex_meta.unwrap_or_default();
                        offset = next_offset;
                        row_count += 1;

                        let (
                            row_liveness_ts,
                            is_row_tombstone,
                            marked_for_delete_at,
                            liveness_expires_at_micros,
                        ) = if let Some(ref h) = row_header_opt {
                            // Convert epoch-seconds liveness expiry to epoch-microseconds
                            // (Issue #702: delta-scan CellMeta.expires_at).
                            let liveness_exp = h
                                .liveness_expires_at_seconds
                                .map(|s| s.saturating_mul(1_000_000));
                            (
                                h.timestamp,
                                h.is_row_tombstone(),
                                h.marked_for_delete_at,
                                liveness_exp,
                            )
                        } else {
                            (None, false, None, None)
                        };

                        // Delta-scan emit boundary (issue #1334): the delta
                        // consumer keys cells by `String`; materialise the interned
                        // `Arc<str>` names here. This is the cold delta path, not
                        // the user-facing read hot path.
                        let cells: HashMap<String, Value> =
                            cells.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
                        match emit((
                            partition_key.clone(),
                            cells,
                            cell_meta,
                            row_liveness_ts,
                            is_static,
                            is_row_tombstone,
                            marked_for_delete_at,
                            None,                       // range_info (not a range tombstone)
                            false,                      // is_partition_tombstone
                            col_meta_map,               // DS4 collection metadata
                            liveness_expires_at_micros, // Issue #702: TTL liveness expiry
                        ))? {
                            std::ops::ControlFlow::Continue(()) => {}
                            std::ops::ControlFlow::Break(()) => return Ok(()),
                        }

                        if offset >= data.len() {
                            break;
                        }

                        if self.peek_is_partition_header(data, offset) {
                            tracing::debug!(
                                "V5CompressedLegacy delta-scan: Partition {} detected at offset {} after {} rows",
                                partition_index + 1, offset, row_count
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        return Err(Error::corruption(format!(
                            "delta-scan: row parse error in partition {} at offset {} in {}.{}: {}",
                            partition_index, offset, self.keyspace, self.table_name, e
                        )));
                    }
                }
            }

            let _ = row_count; // acknowledged for logging purposes

            // Finding 3: dangling-range guard.
            // A well-formed SSTable always pairs every start marker with a matching
            // end marker (or a boundary marker that closes the range) before the
            // end-of-partition byte.  If we reach here with `pending_range_start`
            // still set, the SSTable is corrupt — the range was opened but never
            // closed.  Silently discarding it would violate the no-heuristics mandate
            // (issue #28); return a corruption error naming the partition.
            if let Some((start_vals, start_incl, start_del_at)) = pending_range_start {
                return Err(Error::corruption(format!(
                    "delta-scan: partition {} in {}.{} (key {:?}) has an unclosed range \
                     tombstone start bound (values={:?}, inclusive={}, deleted_at={}) with \
                     no matching end marker — corrupt SSTable (no-heuristics mandate, issue #28)",
                    partition_index,
                    self.keyspace,
                    self.table_name,
                    partition_key.0,
                    start_vals,
                    start_incl,
                    start_del_at,
                )));
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
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, ScanRow)) -> Result<std::ops::ControlFlow<()>>,
    {
        // Whole-block decode: no within-partition row-body window narrowing.
        self.parse_block_emit_windowed(data, schema, reader, None, emit)
    }
}
