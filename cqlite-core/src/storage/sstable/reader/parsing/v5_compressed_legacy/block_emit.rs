use super::*;

impl V5CompressedLegacyParser {
    /// Parse decompressed block into (TableId, RowKey, Value) entries
    ///
    /// # Arguments
    /// * `data` - Decompressed block bytes
    /// * `schema` - Optional table schema for type-aware parsing
    /// * `reader` - Reference to SSTableReader for value parsing
    ///
    /// # Returns
    /// * `Ok(Vec<(TableId, RowKey, Value)>)` - Parsed entries
    /// * `Err(Error)` - Parse error with context
    pub fn parse_block(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
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
            (TableId, RowKey, Value, HashMap<String, CellWriteMetadata>),
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

        let mut offset = 0;
        let mut partition_index = 0;

        while offset < data.len() {
            // Parse partition header: returns (RowKey, next_data_offset)
            let (partition_key, next_data_offset) = match self.parse_partition_header(data, offset)
            {
                Ok(ph) => ph,
                Err(_) => break,
            };

            let table_id = TableId(format!("{}.{}", self.keyspace, self.table_name));
            offset = next_data_offset;
            partition_index += 1;

            let mut static_cells: HashMap<String, Value> = HashMap::new();
            let mut static_cell_meta: HashMap<String, CellWriteMetadata> = HashMap::new();
            let mut row_count = 0;

            loop {
                if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                    offset += 1;
                    break;
                }

                if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                    match self.skip_range_tombstone_marker(data, offset, schema) {
                        Ok(next_offset) => {
                            offset = next_offset;
                            continue;
                        }
                        Err(_) => break,
                    }
                }

                match self.parse_row_data_with_offset(data, offset, Some(schema), reader, true) {
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
                            static_cells = cells;
                            static_cell_meta = row_cell_meta;
                        } else {
                            // Merge static cells / metadata into clustering row
                            for (k, v) in &static_cells {
                                cells.entry(k.clone()).or_insert_with(|| v.clone());
                            }
                            for (k, v) in &static_cell_meta {
                                row_cell_meta.entry(k.clone()).or_insert_with(|| v.clone());
                            }

                            let row_tombstone =
                                row_header_opt.as_ref().filter(|h| h.is_row_tombstone());

                            // Issue #932: a HAS_DELETION row may ALSO carry surviving
                            // cells (written strictly after the row deletion). For the
                            // user-facing scan those cells — all newer than the
                            // deletion — display as a live row; the deletion shadows
                            // only already-absent older cells, so it has no display
                            // effect here. Emit a pure `Tombstone` ONLY when no
                            // non-primary-key cell survives.
                            let has_data_cell = row_has_non_key_cell(&cells, schema);
                            let row_value = if row_tombstone.is_some() && !has_data_cell {
                                row_tombstone
                                    .map(|h| h.row_tombstone())
                                    .unwrap_or(Value::Null)
                            } else if cells.is_empty() {
                                Value::Null
                            } else {
                                let mut map_entries: Vec<(Value, Value)> = cells
                                    .into_iter()
                                    .map(|(name, value)| (Value::Text(name), value))
                                    .collect();
                                map_entries.sort_by(|a, b| {
                                    let a_key = if let Value::Text(s) = &a.0 {
                                        s.as_str()
                                    } else {
                                        ""
                                    };
                                    let b_key = if let Value::Text(s) = &b.0 {
                                        s.as_str()
                                    } else {
                                        ""
                                    };
                                    a_key.cmp(b_key)
                                });
                                Value::Map(map_entries)
                            };

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

                        if offset >= data.len() {
                            break;
                        }

                        if self.peek_is_partition_header(data, offset) {
                            log::debug!(
                                "V5CompressedLegacy: Partition {} detected at offset {} after {} rows",
                                partition_index + 1, offset, row_count
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy: Row parse error in partition {} at offset {}: {}",
                            partition_index,
                            offset,
                            e
                        );
                        break;
                    }
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

                match self.parse_row_data_with_offset(data, offset, Some(schema), reader, true) {
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
                                .map(|s| (s as i64).saturating_mul(1_000_000));
                            (
                                h.timestamp,
                                h.is_row_tombstone(),
                                h.marked_for_delete_at,
                                liveness_exp,
                            )
                        } else {
                            (None, false, None, None)
                        };

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
                            log::debug!(
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
    /// `(TableId, RowKey, Value)` entry instead of collecting them into a `Vec`,
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
        F: FnMut((TableId, RowKey, Value)) -> Result<std::ops::ControlFlow<()>>,
    {
        // Whole-block decode: no within-partition row-body window narrowing.
        self.parse_block_emit_windowed(data, schema, reader, None, emit)
    }

    /// Within-partition clustering-slice variant of [`parse_block_emit`] (Issue
    /// #954, Epic #951).
    ///
    /// When `row_body_window` is `Some((body_start, body_end))` (byte offsets
    /// into `data`, in the SAME domain as `data` indices) the FIRST partition's
    /// row-body parse is bounded to that window:
    ///   - after the partition header is decoded, the row cursor is fast-forwarded
    ///     to `max(after_header, body_start)` (skipping rows that precede the
    ///     requested clustering slice), and
    ///   - the row loop stops once the cursor reaches `body_end` (skipping rows
    ///     after the slice).
    ///
    /// `body_start`/`body_end` are the byte extent of the row-index block(s) that
    /// the authoritative BTI row index resolved as covering the requested
    /// clustering range, so this decodes O(matched rows + index-block slack)
    /// rather than the whole partition. The post-scan `evaluate_leaf` backstop
    /// trims the block-granularity over-read, so the returned rows are a SUPERSET
    /// of the exact slice and the final query output is byte-identical.
    ///
    /// The start fast-forward is only applied when the schema has NO static
    /// columns (the caller enforces this): a static row precedes the clustering
    /// rows and must be merged into each clustering row, so skipping past it would
    /// drop static values. The end bound is always safe to apply.
    ///
    /// Every row this method actually decodes bumps the `work_counters`
    /// `rows_decoded` counter so a test can prove the decode was bounded to the
    /// slice. With `row_body_window == None` this is byte-for-byte
    /// [`parse_block_emit`]'s original behaviour, and the counter is still bumped
    /// (the seek path reports its full-partition decode too).
    pub fn parse_block_emit_windowed<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        row_body_window: Option<(usize, usize)>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, Value)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        // V5CompressedLegacy format stores cells WITHOUT column names,
        // relying on schema to interpret the binary data. Schema is REQUIRED.
        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        log::debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );
        log::debug!(
            "V5CompressedLegacy: Schema has {} columns",
            schema.columns.len()
        );
        for (i, col) in schema.columns.iter().enumerate() {
            log::debug!("  Column {}: {} ({})", i, col.name, col.data_type);
        }
        log::debug!(
            "V5CompressedLegacy: First 64 bytes of data: {}",
            hex::encode(&data[..std::cmp::min(64, data.len())])
        );
        debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );

        let mut emitted: usize = 0;
        let mut offset = 0;
        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        // Cassandra partition key size limits (used in header validation)
        // - CASSANDRA_MAX_KEY_SIZE: 64KB limit per Apache Cassandra specification
        // - FORMAT_MAX_KEY_SIZE: u8 max value - V5CompressedLegacy format limitation
        const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
        const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 max value - format limitation

        // Parse ALL partitions in block (Issue #2 fix: previously only parsed one partition)
        let mut partition_index = 0;
        let mut skipped_partitions = 0;
        while offset < data.len() {
            log::debug!(
                "V5CompressedLegacy: === PARTITION {} at offset {} (block size: {}) ===",
                partition_index,
                offset,
                data.len()
            );

            // CRITICAL FIX (Issue #164): Validate partition header format before attempting parse
            //
            // Most compressed blocks contain EXACTLY ONE partition. After parsing the first
            // partition's row data and trailing VInt, we should NOT assume there's another
            // partition just because offset < data.len().
            //
            // Partition header format validation:
            // - Byte 0: Flags (typically 0x00, sometimes has partition-level flags)
            // - Byte 1: Partition key length (u8, typically 16 for UUID)
            // - Bytes 2+: Partition key data
            //
            // If we don't see a valid partition header structure, we've reached the end
            // of partitions in this block (remaining bytes are likely padding or metadata).
            if offset >= data.len() {
                break; // End of block
            }

            // Check if this looks like a partition header (flags byte + reasonable key length)
            // Partition keys can be up to 64KB per Cassandra spec (composite keys, text, etc.)
            if offset + 2 > data.len() {
                log::debug!(
                    "V5CompressedLegacy: Not enough bytes for partition header at offset {} (need 2, have {}), stopping",
                    offset,
                    data.len() - offset
                );
                break;
            }

            let flags = data[offset];
            let key_len = data[offset + 1] as usize;

            // Validate partition header:
            // - Key length must be non-zero and within format's limit (u8 max = 255 bytes)
            //   Note: Cassandra spec allows 64KB keys, but V5CompressedLegacy format uses u8 length
            // - Must have enough bytes for the header (size depends on format version)
            //
            // VG3: oa format (hasUIntDeletionTime) uses a compact DeletionTime:
            //   LIVE = 1 byte; DELETED = 12 bytes.  The minimum is therefore 1 byte.
            // nb format always uses 12 bytes (4 + 8).
            // NOTE: No heuristic validation of flags (Issue #258, #28 no-heuristics mandate)
            let deletion_time_min = if self.has_uint_deletion_time() { 1 } else { 12 };
            let header_min_size = 1 + 1 + key_len + deletion_time_min;
            if key_len == 0
                || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE)
                || offset + header_min_size > data.len()
            {
                log::warn!(
                    "V5CompressedLegacy: Skipping malformed partition header at offset {} \
                     (flags=0x{:02x}, key_len={}, need {} bytes, have {}, partition={}): header validation failed",
                    offset,
                    flags,
                    key_len,
                    header_min_size,
                    data.len() - offset,
                    partition_index
                );
                // Try to skip to next potential partition boundary
                skipped_partitions += 1;
                offset += 1; // Minimal forward progress to avoid infinite loop
                continue; // Skip this partition, try next
            }

            // Try to parse partition header
            match self.parse_partition_header(data, offset) {
                Ok((partition_key, new_offset)) => {
                    let header_size = new_offset - offset;
                    offset = new_offset;

                    // Issue #954: when a within-partition clustering-slice window is
                    // supplied, fast-forward the row cursor of the FIRST partition to
                    // the first row-index block covering the slice (skipping rows
                    // before it). Applied only to `partition_index == 0` because the
                    // seek decodes exactly one target partition. The end bound
                    // (`body_end`) stops the row loop below. The caller guarantees a
                    // start fast-forward is only requested when the schema has no
                    // static columns, so this never skips a static prefix.
                    let row_body_end = match row_body_window {
                        Some((body_start, body_end)) if partition_index == 0 => {
                            if body_start > offset && body_start <= data.len() {
                                offset = body_start;
                            }
                            Some(body_end)
                        }
                        _ => None,
                    };

                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Parsed partition key: {} bytes (header consumed {} bytes, now at offset {})",
                        partition_index,
                        partition_key.0.len(),
                        header_size,
                        offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Row data starts at offset {}, remaining: {} bytes",
                        partition_index,
                        offset,
                        data.len() - offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Row data hex (first 128 bytes): {}",
                        partition_index,
                        hex::encode(&data[offset..std::cmp::min(offset + 128, data.len())])
                    );

                    debug!(
                        "V5CompressedLegacy: Parsed partition key: {} bytes, now at offset {}",
                        partition_key.0.len(),
                        offset
                    );

                    // Parse ALL rows in this partition (Issue #166 fix: multi-row partition support)
                    //
                    // V5CompressedLegacy partitions can contain multiple rows with different clustering keys.
                    // We use structural parsing (peek_is_partition_header) to detect partition boundaries,
                    // not flag value heuristics (Issue #258, #28 no-heuristics mandate).
                    // We parse rows in a loop until we encounter:
                    // - End of block (offset >= data.len())
                    // - END_OF_PARTITION marker (flags == 0x01, Issue #229 fix)
                    // - Next partition header (detected via peek_is_partition_header)
                    // - Parse error (invalid row data)

                    // Issue #480 FIX: Static cell handling
                    //
                    // Cassandra static rows are stored once per partition (before clustering rows).
                    // They should NOT be emitted as separate result entries — instead their column
                    // values must be merged into each clustering row that follows in the partition.
                    //
                    // We accumulate static cells here and inject them into every clustering row.
                    let mut static_cells: HashMap<String, Value> = HashMap::new();
                    let mut row_count = 0;
                    loop {
                        // Issue #954: stop at the clustering-slice end bound. The
                        // row-index block extent (`body_end`) is the authoritative
                        // upper byte bound of the rows that may fall in the requested
                        // clustering range; rows past it are outside the slice, so we
                        // stop decoding (the post-scan backstop already trims any
                        // block-granularity over-read within the window).
                        if let Some(body_end) = row_body_end {
                            if offset >= body_end {
                                break;
                            }
                        }

                        // Issue #229 FIX: Check for END_OF_PARTITION marker BEFORE attempting row parse
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 102, 730-732):
                        // When END_OF_PARTITION (0x01) is set in the flags byte, nothing follows.
                        // The partition is complete and we should move to the next partition.
                        if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                            log::debug!(
                                "V5CompressedLegacy: Partition {} complete via END_OF_PARTITION marker at offset {} ({} rows parsed)",
                                partition_index, offset, row_count
                            );
                            offset += 1; // Skip the END_OF_PARTITION marker byte
                            break; // Move to next partition
                        }

                        // Issue #229 FIX: Check for range tombstone marker
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 103, 735-738):
                        // When IS_MARKER (0x02) is set, this is a range tombstone boundary, not a row.
                        // We skip these markers for now (full implementation would parse deletion ranges).
                        if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                            log::debug!(
                                "V5CompressedLegacy: Range tombstone marker at offset {} (partition {}), skipping",
                                offset, partition_index
                            );
                            // Skip the marker - for now, just advance past it
                            // A full implementation would parse ClusteringBoundOrBoundary and deletion times
                            match self.skip_range_tombstone_marker(data, offset, schema) {
                                Ok(next_offset) => {
                                    offset = next_offset;
                                    continue; // Continue to next row/marker
                                }
                                Err(e) => {
                                    log::debug!(
                                        "V5CompressedLegacy: Failed to skip range tombstone marker at offset {}: {}",
                                        offset, e
                                    );
                                    break; // Can't parse marker, end partition
                                }
                            }
                        }

                        match self.parse_row_data_with_offset(
                            data,
                            offset,
                            Some(schema),
                            reader,
                            false,
                        ) {
                            Ok((
                                mut cells,
                                _row_cell_meta,
                                row_header_opt,
                                next_offset,
                                is_static,
                                _complex_meta,
                            )) => {
                                // Update offset to point to the next row or partition
                                offset = next_offset;
                                row_count += 1;

                                log::debug!(
                                    "V5CompressedLegacy: Partition {} Row {} - Parsed {} cells, now at offset {} (is_static={})",
                                    partition_index,
                                    row_count,
                                    cells.len(),
                                    offset,
                                    is_static
                                );

                                if let Some(ref header) = row_header_opt {
                                    log::debug!(
                                        "V5CompressedLegacy: Row {} metadata - timestamp={:?}, ttl={:?}, deletion={:?}",
                                        row_count,
                                        header.timestamp, header.ttl, header.local_deletion_time
                                    );
                                }

                                debug!(
                                    "V5CompressedLegacy: Parsed {} cells from row {} (is_static={})",
                                    cells.len(),
                                    row_count,
                                    is_static
                                );

                                // Issue #480 FIX: Static row handling
                                //
                                // Static rows are stored once per partition and contain values for
                                // STATIC columns (e.g. `static_data TEXT STATIC`). They must NOT
                                // be emitted as standalone result rows. Instead, store the static
                                // column values and merge them into each subsequent clustering row.
                                if is_static {
                                    log::debug!(
                                        "V5CompressedLegacy: Partition {} - Storing {} static cells for merging into clustering rows",
                                        partition_index,
                                        cells.len()
                                    );
                                    static_cells = cells;
                                    // Do NOT push to results — static rows are not result rows
                                    // Continue to next row/marker in partition
                                } else {
                                    // Merge static cells into this clustering row (Issue #480)
                                    for (k, v) in &static_cells {
                                        cells.entry(k.clone()).or_insert_with(|| v.clone());
                                    }

                                    // Convert cells HashMap to Value::Map (required by SelectExecutor)
                                    //
                                    // Issue #505: Row tombstones are detected via HAS_DELETION in the
                                    // row header.  When present, emit a proper `Value::Tombstone`
                                    // carrying the authoritative `markedForDeleteAt` (microseconds) so
                                    // the compaction merger can apply tombstone-shadowing semantics
                                    // rather than treating the absent cells as a live empty row.
                                    let row_tombstone =
                                        row_header_opt.as_ref().filter(|h| h.is_row_tombstone());

                                    // Issue #932: a HAS_DELETION row carrying
                                    // surviving (strictly-newer) cells displays as a
                                    // live row for the user-facing scan — the
                                    // deletion shadows only already-absent older
                                    // cells. Emit a pure Tombstone only when no
                                    // non-primary-key cell survives.
                                    let has_data_cell = row_has_non_key_cell(&cells, schema);
                                    let row_value = if row_tombstone.is_some() && !has_data_cell {
                                        if let Some(h) = row_tombstone {
                                            log::debug!(
                                                "V5CompressedLegacy: Partition {} Row {} - emitting Tombstone(deletion_time={})",
                                                partition_index, row_count, h.row_tombstone_deletion_time()
                                            );
                                            h.row_tombstone()
                                        } else {
                                            Value::Null
                                        }
                                    } else if cells.is_empty() {
                                        warn!(
                                            "V5CompressedLegacy: No cells extracted for {}.{} partition {} row {} (partition key: {} bytes)",
                                            self.keyspace,
                                            self.table_name,
                                            partition_index,
                                            row_count,
                                            partition_key.0.len()
                                        );
                                        Value::Null
                                    } else {
                                        // Convert HashMap<String, Value> to Vec<(Value, Value)> for Value::Map.
                                        //
                                        // Sort alphabetically by column name to guarantee a deterministic
                                        // ordering across independent parse calls.  HashMap iteration order
                                        // is randomized per-instance in Rust, so two separate calls to
                                        // stitch_and_parse_all_chunks (e.g. get() then scan()) would
                                        // otherwise produce Vec orderings that compare as unequal even
                                        // though they hold the same data.
                                        //
                                        // Alphabetical is not schema column order, but the query layer
                                        // (executor.rs:storage_data_to_query_row) accesses columns by name
                                        // (not position), so this ordering does not affect query correctness
                                        // or sstabledump parity.
                                        //
                                        // NON-BLOCKING-3 (Issue #516/517): A future improvement could use
                                        // serialization-header order (reader.header.columns) rather than
                                        // alphabetical, matching Cassandra's on-disk column order exactly.
                                        // That would require threading the column order through ParsedRow
                                        // to this call site.
                                        let mut map_entries: Vec<(Value, Value)> = cells
                                            .into_iter()
                                            .map(|(name, value)| (Value::Text(name), value))
                                            .collect();
                                        map_entries.sort_by(|a, b| {
                                            let a_key = if let Value::Text(s) = &a.0 {
                                                s.as_str()
                                            } else {
                                                ""
                                            };
                                            let b_key = if let Value::Text(s) = &b.0 {
                                                s.as_str()
                                            } else {
                                                ""
                                            };
                                            a_key.cmp(b_key)
                                        });
                                        Value::Map(map_entries)
                                    };

                                    // Issue #954: count each clustering row actually
                                    // decoded out of Data.db so a slice query can be
                                    // proven to decode O(matched rows + index block),
                                    // not the whole partition. Counted at the row
                                    // grain (here), distinct from the per-partition
                                    // `partitions_decoded`. Gated `not(tombstones)`
                                    // because the mutator is compiled only there.
                                    #[cfg(not(feature = "tombstones"))]
                                    crate::storage::sstable::work_counters::add_rows_decoded(1);

                                    match emit((
                                        table_id.clone(),
                                        partition_key.clone(),
                                        row_value,
                                    ))? {
                                        std::ops::ControlFlow::Continue(()) => emitted += 1,
                                        // Consumer dropped (streaming receiver gone): stop parsing.
                                        std::ops::ControlFlow::Break(()) => return Ok(()),
                                    }
                                }

                                // Check if we're at the end of the partition
                                if offset >= data.len() {
                                    debug!(
                                        "V5CompressedLegacy: Partition {} complete: {} rows parsed (end of block)",
                                        partition_index, row_count
                                    );
                                    break; // End of block
                                }

                                // CRITICAL FIX (Issue #166): NO HEURISTICS - Try-parse approach
                                //
                                // Instead of guessing based on byte patterns (e.g., checking if flags <= 0x20
                                // or validating key_len ranges), we ACTUALLY TRY TO PARSE the next structure.
                                //
                                // Why heuristics fail:
                                // - Row with small value (e.g., boolean=0x0A) can look like key_len
                                // - Row flags=0x00 or 0x20 pass "<= 0x20" checks meant for partitions
                                // - Any byte-pattern guessing will eventually fail on edge cases
                                //
                                // The only reliable approach: try to parse as partition header.
                                // If that succeeds, it's a partition. If it fails, continue with rows.
                                if self.peek_is_partition_header(data, offset) {
                                    debug!(
                                        "V5CompressedLegacy: Partition {} complete: {} rows parsed (next partition detected at offset {})",
                                        partition_index, row_count, offset
                                    );
                                    break; // Next partition starts here
                                }

                                // Peek failed - not a partition header, so continue parsing rows
                                debug!(
                                    "V5CompressedLegacy: Partition {} - Continuing to row {} at offset {} (peek confirmed this is NOT a partition header)",
                                    partition_index, row_count + 1, offset
                                );
                            }
                            Err(e) => {
                                // End of valid data in partition
                                debug!(
                                    "V5CompressedLegacy: Partition {} ended after {} rows: {}",
                                    partition_index, row_count, e
                                );
                                if row_count == 0 {
                                    // If we couldn't parse even one row, log as error
                                    log::error!(
                                        "V5CompressedLegacy: Partition {} - Failed to parse first row at offset {}: {}",
                                        partition_index, offset, e
                                    );
                                }
                                break; // End of valid data in partition
                            }
                        }
                    }

                    partition_index += 1;
                }
                Err(e) => {
                    log::warn!(
                        "V5CompressedLegacy: Failed to parse partition header at offset {} \
                         (partition={}): {}. Attempting to continue to next partition.",
                        offset,
                        partition_index,
                        e
                    );
                    // Try to skip forward to find next partition
                    skipped_partitions += 1;
                    offset += 1;
                    continue; // Skip this partition, try next
                }
            }
        }

        if skipped_partitions > 0 {
            log::warn!(
                "V5CompressedLegacy: Successfully parsed {} entries, skipped {} malformed partitions",
                emitted,
                skipped_partitions
            );
        }

        debug!(
            "V5CompressedLegacy: Parsed {} total entries from block",
            emitted
        );

        Ok(())
    }

    /// Parse all partitions in a decompressed block, returning per-row timestamps.
    ///
    /// This is the compaction-specific variant of [`parse_block`].  It returns
    /// `(TableId, RowKey, Value, row_timestamp_micros)` so that the k-way merger
    /// can perform timestamp-accurate last-write-wins ordering rather than
    /// falling back to `SystemTime::now()`.
    ///
    /// Row tombstones are emitted as `Value::Tombstone(RowTombstone)` with their
    /// actual `deletion_time`.  Cell tombstones within live rows are stored as
    /// `Value::Tombstone(CellTombstone)` inside the `Value::Map`, again carrying
    /// the cell-level deletion timestamp.
    ///
    /// The `row_timestamp_micros` in the returned tuple is the row-level write
    /// timestamp decoded from the `HAS_TIMESTAMP` field in the row header
    /// (`min_timestamp + delta`).  For row tombstones the same timestamp also
    /// appears in `TombstoneInfo::deletion_time`.
    ///
    /// Normal user-facing scan/get paths should use [`parse_block`] instead.
    /// (Issue #505)
    pub fn parse_block_with_timestamps(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, Value, i64)>> {
        // Thin wrapper that collects the streaming emit variant into a Vec, so
        // every existing caller/test is byte-for-byte unchanged (issue #827).
        let mut results: Vec<(TableId, RowKey, Value, i64)> = Vec::new();
        self.parse_block_with_timestamps_emit(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Streaming variant of [`parse_block_with_timestamps`]: invokes `emit` for
    /// each parsed `(TableId, RowKey, Value, row_timestamp_micros)` entry rather
    /// than collecting into a `Vec`, so the compaction read path can forward
    /// rows into a bounded channel without materialising the whole block at once
    /// (issue #827). Returning `ControlFlow::Break` from `emit` stops parsing
    /// early — used when the streaming consumer is dropped.
    ///
    /// The tombstone/timestamp semantics are byte-identical to
    /// [`parse_block_with_timestamps`] (Issue #505/#533): a row tombstone is
    /// emitted as `Value::Tombstone` carrying its `markedForDeleteAt`, and the
    /// fourth tuple element is the row write timestamp for live rows.
    pub fn parse_block_with_timestamps_emit<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, Value, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        let mut offset = 0;
        let mut skipped_partitions = 0;

        // Wrap `emit` so a Break is observable here as well as inside the
        // one-partition parser (which stops its inner row loop on Break). This
        // lets the outer loop terminate promptly when the consumer is dropped.
        // `Cell` so the wrapping closure can borrow it shared while the outer
        // loop also reads it between calls.
        let broke = std::cell::Cell::new(false);
        let mut tracking_emit = |entry| -> Result<std::ops::ControlFlow<()>> {
            let flow = emit(entry)?;
            if matches!(flow, std::ops::ControlFlow::Break(())) {
                broke.set(true);
            }
            Ok(flow)
        };

        while offset < data.len() {
            match self.parse_one_partition_with_timestamps(
                &data[offset..],
                Some(schema),
                reader,
                // The whole block is present; never request a refill. A trailing
                // parse failure is terminal here (matches the legacy
                // `Err(_) => break` behaviour of the original loop).
                true,
                &mut tracking_emit,
            )? {
                ParseStep::Emitted(consumed) => {
                    if consumed == 0 {
                        // Defensive: avoid an infinite loop on a zero-byte
                        // partition (should not happen — a header is >= 2 bytes).
                        skipped_partitions += 1;
                        offset += 1;
                    } else {
                        offset += consumed;
                    }
                }
                // `at_final_chunk = true` collapses NeedMore into Done: there is
                // no further chunk to append, so a truncated tail is end-of-data.
                ParseStep::NeedMore | ParseStep::Done => break,
            }
            // Propagate an early Break from `emit` (consumer dropped).
            if broke.get() {
                break;
            }
        }

        if skipped_partitions > 0 {
            log::warn!(
                "V5CompressedLegacy (compaction): skipped {} malformed partitions",
                skipped_partitions
            );
        }

        Ok(())
    }

    /// Parse exactly ONE partition from the front of `data`, emitting each row
    /// via `emit`, and report how the parse terminated (issue #827).
    ///
    /// This isolates the body of the outer partition loop so the sliding-window
    /// compaction driver can drain one partition at a time and `drain(0..consumed)`
    /// from its window between calls. The crucial distinction over the legacy
    /// monolithic loop is `NeedMore` vs `Done`:
    ///
    /// - [`ParseStep::Emitted(consumed)`] — a full partition was parsed and
    ///   terminated by an END_OF_PARTITION marker or a confirmed next-partition
    ///   header. `consumed` bytes may be drained from the window.
    /// - [`ParseStep::NeedMore`] — `data` is (possibly) truncated mid-partition
    ///   and `!at_final_chunk`, so the caller must append the next chunk and
    ///   retry. NEVER returned when `at_final_chunk` is true.
    /// - [`ParseStep::Done`] — genuine end of partitions, or (when
    ///   `at_final_chunk`) a trailing truncation that cannot be resolved by more
    ///   data. Terminal.
    ///
    /// `at_final_chunk` flips a mid-partition parse failure between a refill
    /// request (`NeedMore`) and a terminal stop. The legacy code conflated
    /// parse-error with end-of-partitions (`Err(_) => break`); doing that
    /// mid-stream would silently drop every partition after a chunk boundary, so
    /// we return `NeedMore` whenever the buffer may simply be truncated and we
    /// are not yet at the final chunk.
    pub fn parse_one_partition_with_timestamps<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        at_final_chunk: bool,
        emit: &mut F,
    ) -> Result<ParseStep>
    where
        F: FnMut((TableId, RowKey, Value, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        const CASSANDRA_MAX_KEY_SIZE: usize = 65536;
        const FORMAT_MAX_KEY_SIZE: usize = 255;

        // A partition header is at least flags(1) + key_len(1). If we cannot even
        // read those two bytes, we are truncated: request more unless final.
        if data.len() < 2 {
            return Ok(if at_final_chunk {
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let key_len = data[1] as usize;
        let header_min_size = 1 + 1 + key_len + 4 + 8;

        // Invalid header shape (zero/over-long key) → malformed; advance by one
        // byte so the outer loop can resynchronise. Returning Emitted(1) here
        // mirrors the legacy `offset += 1; continue` skip-a-byte recovery.
        if key_len == 0 || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE) {
            return Ok(ParseStep::Emitted(1));
        }

        // Header declared but not fully present in `data` → truncated mid-header.
        if header_min_size > data.len() {
            return Ok(if at_final_chunk {
                // No more bytes will ever arrive; the legacy loop treated this
                // as the end of parseable partitions.
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let (partition_key, mut offset) = match self.parse_partition_header(data, 0) {
            Ok(v) => v,
            Err(_) => {
                // Fixed header bytes are present but did not parse: skip a byte
                // to resynchronise — matching the legacy
                // `Err(_) => { offset += 1; continue }` recovery.
                return Ok(ParseStep::Emitted(1));
            }
        };

        let mut static_cells: HashMap<String, Value> = HashMap::new();

        // Finding 1 (#827): buffer this partition's emitted rows locally and only
        // forward them to the external `emit` once the partition is CONFIRMED
        // complete (a `ParseStep::Emitted` return). If the buffer is truncated
        // mid-partition (`NeedMore`) after one or more rows were parsed, we must
        // emit NOTHING and let the caller refill and re-parse this partition from
        // its start — otherwise the already-forwarded rows would be re-emitted on
        // the retry, duplicating them in the streaming compaction output.
        //
        // The buffer is bounded by ONE partition's rows (the documented
        // `max_partition_size` bound), not the whole file, so memory stays
        // bounded as required by the #827 deliverable.
        let mut pending: Vec<(TableId, RowKey, Value, i64)> = Vec::new();

        // Flush the buffered rows to the external `emit`, honouring an early
        // `Break`. Returns the `ParseStep` to surface to the caller: on `Break`
        // we still report the bytes consumed for this (complete) partition so the
        // caller drains correctly, but stop forwarding the remaining buffered
        // rows. `flushed_break` becomes true so the driver can stop promptly.
        macro_rules! flush_and_emitted {
            ($consumed:expr, $pending:expr, $emit:expr) => {{
                for entry in $pending.drain(..) {
                    match $emit(entry)? {
                        std::ops::ControlFlow::Continue(()) => {}
                        std::ops::ControlFlow::Break(()) => break,
                    }
                }
                Ok(ParseStep::Emitted($consumed))
            }};
        }

        loop {
            // END_OF_PARTITION (0x01): partition complete, consume the marker.
            if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                offset += 1;
                return flush_and_emitted!(offset, pending, emit);
            }

            // Consumed everything but never saw END_OF_PARTITION: the partition
            // may continue in the next chunk. On NeedMore emit NOTHING (drop the
            // buffered rows) so the caller can refill and re-parse from the start
            // without duplicating already-buffered rows (Finding 1).
            if offset >= data.len() {
                if at_final_chunk {
                    return flush_and_emitted!(offset, pending, emit);
                }
                return Ok(ParseStep::NeedMore);
            }

            if Self::is_range_tombstone_marker(data[offset]) {
                match self.skip_range_tombstone_marker(data, offset, schema) {
                    Ok(next_offset) => {
                        offset = next_offset;
                        continue;
                    }
                    Err(_) => {
                        // Marker body truncated? request more unless final.
                        if at_final_chunk {
                            return flush_and_emitted!(offset, pending, emit);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                }
            }

            match self.parse_row_data_with_offset(data, offset, Some(schema), reader, false) {
                Ok((
                    mut cells,
                    _row_cell_meta,
                    row_header_opt,
                    next_offset,
                    is_static,
                    _complex_meta,
                )) => {
                    offset = next_offset;

                    // For a row tombstone the authoritative timestamp is
                    // markedForDeleteAt (HAS_TIMESTAMP is absent for pure row
                    // deletes). For a live row it is the row write timestamp.
                    // Both the merger tuple `row_ts` and the emitted
                    // Value::Tombstone must agree, so resolve once here (#505).
                    let row_tombstone = row_header_opt.as_ref().filter(|h| h.is_row_tombstone());
                    // Issue #932: a HAS_DELETION row may ALSO carry a liveness
                    // timestamp (surviving cells written strictly after the
                    // deletion). Prefer the liveness timestamp as the row
                    // timestamp so those cells inherit it and are NOT shadowed by
                    // the older row deletion during reconcile; fall back to
                    // markedForDeleteAt only for a PURE row tombstone (which has no
                    // HAS_TIMESTAMP).
                    let row_ts = row_header_opt
                        .as_ref()
                        .and_then(|h| h.timestamp)
                        .or_else(|| row_tombstone.map(|h| h.row_tombstone_deletion_time()))
                        .unwrap_or(0);

                    if is_static {
                        static_cells = cells;
                    } else {
                        for (k, v) in &static_cells {
                            cells.entry(k.clone()).or_insert_with(|| v.clone());
                        }

                        // Row tombstone → Value::Tombstone(markedForDeleteAt).
                        // Issue #932: unless the row ALSO carries surviving cells
                        // (newer than the deletion), in which case it displays as a
                        // live row — the deletion shadows only already-absent older
                        // cells.
                        let has_data_cell = row_has_non_key_cell(&cells, schema);
                        let row_value = if row_tombstone.is_some() && !has_data_cell {
                            row_tombstone
                                .map(|h| h.row_tombstone())
                                .unwrap_or(Value::Null)
                        } else if cells.is_empty() {
                            Value::Null
                        } else {
                            let mut map_entries: Vec<(Value, Value)> = cells
                                .into_iter()
                                .map(|(name, value)| (Value::Text(name), value))
                                .collect();
                            map_entries.sort_by(|a, b| {
                                let a_key = if let Value::Text(s) = &a.0 {
                                    s.as_str()
                                } else {
                                    ""
                                };
                                let b_key = if let Value::Text(s) = &b.0 {
                                    s.as_str()
                                } else {
                                    ""
                                };
                                a_key.cmp(b_key)
                            });
                            Value::Map(map_entries)
                        };

                        // Finding 1: buffer the row instead of forwarding it now.
                        // It is flushed to `emit` only when the partition is
                        // confirmed complete (a `flush_and_emitted!` return). A
                        // mid-partition `NeedMore` discards `pending` and the
                        // caller re-parses from the partition start.
                        pending.push((table_id.clone(), partition_key.clone(), row_value, row_ts));
                    }

                    if offset >= data.len() {
                        // End of the buffer without an explicit END_OF_PARTITION:
                        // the partition may continue in the next chunk.
                        if at_final_chunk {
                            return flush_and_emitted!(offset, pending, emit);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    if self.peek_is_partition_header(data, offset) {
                        // Next partition starts here — current one is complete.
                        return flush_and_emitted!(offset, pending, emit);
                    }
                }
                Err(_) => {
                    // A row failed to parse. The legacy loop unconditionally
                    // `break`s here (end-of-partition). Mid-stream that may
                    // instead be a row straddling the chunk boundary, so request
                    // more bytes unless this is the final chunk.
                    if at_final_chunk {
                        return flush_and_emitted!(offset, pending, emit);
                    }
                    return Ok(ParseStep::NeedMore);
                }
            }
        }
    }
}
