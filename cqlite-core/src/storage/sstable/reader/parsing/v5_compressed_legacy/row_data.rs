use super::*;

impl V5CompressedLegacyParser {
    /// Parse row data (header + cells) and return cells with new offset
    ///
    /// V5CompressedLegacy format stores cells WITHOUT column names in schema column order.
    /// Schema is REQUIRED to determine which column each value belongs to.
    ///
    /// Returns: `ParsedRow` = `(cells, row_header, new_offset, is_static)` where
    /// `is_static` is `true` when the row's `EXTENDED_IS_STATIC` flag was set.
    /// Static rows must be merged into clustering rows by the caller, not emitted directly.
    pub(super) fn parse_row_data_with_offset(
        &self,
        data: &[u8],
        offset: usize,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        want_cell_metadata: bool,
    ) -> Result<ParsedRow> {
        self.parse_row_data_with_offset_impl(data, offset, schema, reader, want_cell_metadata, None)
    }

    /// Implementation of [`Self::parse_row_data_with_offset`] with an optional
    /// per-column complex-element collector for the compaction read path
    /// (epic #899). When `compaction_complex_out` is `Some`, every complex
    /// column's per-element cells + complex deletion are captured into it
    /// alongside the normal collapsed-`Value` cells. On user-facing reads it is
    /// `None` and behavior is byte-unchanged.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn parse_row_data_with_offset_impl(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        want_cell_metadata: bool,
        mut compaction_complex_out: Option<&mut CompactionComplexColumns>,
    ) -> Result<ParsedRow> {
        let mut cells = HashMap::new();
        // Parallel per-cell write metadata map (populated alongside `cells`).
        // Only allocated when the caller actually needs WRITETIME/TTL metadata
        // (i.e. `want_cell_metadata == true`).  On the normal read path this stays
        // `None` so that zero HashMap allocations or inserts occur per cell.
        let mut cell_meta: Option<HashMap<String, CellWriteMetadata>> = if want_cell_metadata {
            Some(HashMap::new())
        } else {
            None
        };

        // DS4 (Issue #700): Per-column complex collection metadata.  Only allocated when
        // want_cell_metadata is true (same gate as cell_meta to avoid hot-path overhead).
        let mut complex_col_meta: Option<HashMap<String, ComplexColumnMeta>> = if want_cell_metadata
        {
            Some(HashMap::new())
        } else {
            None
        };

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy: Schema required for {}.{} (cells stored without column names)",
                self.keyspace, self.table_name
            ))
        })?;

        debug!(
            "V5CompressedLegacy: Starting row data parse at offset {} with {} schema columns",
            offset,
            schema.columns.len()
        );

        // ISSUE #213 FIX: Correct parsing order for V5CompressedLegacy format
        //
        // The CORRECT format from Cassandra UnfilteredSerializer.java is:
        //   1. [row_flags: u8]
        //   2. [extended_flags: u8 if 0x80 set]
        //   3. [clustering_prefix: variable]  ← BEFORE row_size!
        //   4. [row_size: VInt]
        //   5. [prev_size: VInt]
        //   6. [row_body: timestamp, ttl, deletion, bitmap, cells]
        //
        // The previous code parsed row_size BEFORE clustering, which caused
        // clustering key bytes to be misinterpreted as row_size (often 0).

        // Step 1: Parse row flags (1-2 bytes)
        let (row_flags, extended_flags, flags_size) = self.parse_row_flags(data, offset)?;
        offset += flags_size;

        // Issue #258 fix: Check if this is a static row (no clustering prefix)
        // Per Cassandra UnfilteredSerializer.java lines 114-122, 190-191:
        // Static rows have the IS_STATIC bit (0x01) set in extended flags and
        // do NOT have a clustering prefix - skip directly to row_size.
        let is_static = extended_flags
            .map(|ef| (ef & EXTENDED_IS_STATIC) != 0)
            .unwrap_or(false);

        // Step 2: Parse clustering prefix BEFORE row_size (Issue #213 fix)
        // This is the critical change - clustering comes AFTER flags but BEFORE row_size
        // EXCEPT for static rows which have no clustering prefix at all.
        let (clustering_values, offset) = if !is_static {
            self.parse_clustering_prefix(data, offset, schema)?
        } else {
            log::debug!(
                "V5CompressedLegacy: Static row detected (extended_flags=0x{:02x}), skipping clustering prefix",
                extended_flags.unwrap_or(0)
            );
            (vec![], offset)
        };

        log::debug!(
            "V5CompressedLegacy: Parsed {} clustering values after flags, now at offset {} (is_static={})",
            clustering_values.len(),
            offset,
            is_static
        );

        // Issue #229 FIX: Add clustering key values to cells HashMap
        //
        // Cassandra stores clustering keys separately from regular columns, but they
        // must be included in the result for proper query output. Without this fix,
        // tables with clustering keys show fallback column names because the clustering
        // values weren't being added to the cells HashMap.
        for (i, ck) in schema.clustering_keys.iter().enumerate() {
            if i < clustering_values.len() {
                cells.insert(ck.name.clone(), clustering_values[i].clone());
            }
        }

        // Step 3: Parse row metadata (row_size, prev_size, timestamps, etc.)
        //
        // CRITICAL (Issue #237): Save offset where row_size VInt STARTS.
        // The row_size value is measured from AFTER this VInt is consumed.
        // Formula: next_offset = (row_metadata_offset + row_size_vint_len) + row_size
        // This offset is right after the clustering prefix (which was already parsed).
        let row_metadata_offset = offset;
        let (row_header, row_size) =
            self.parse_row_metadata(data, offset, row_flags, extended_flags)?;

        // CRITICAL VALIDATION: row_size must be reasonable
        //
        // In V5CompressedLegacy format, row_size should never exceed the block size (typically 16KB).
        // If row_size is unreasonably large, it indicates either:
        // 1. Partition tombstone or deletion marker (no actual row data)
        // 2. Format parsing error (landed at wrong offset)
        // 3. Corrupted data
        //
        // In all cases, we should skip this partition rather than panic.
        const MAX_REASONABLE_ROW_SIZE: u64 = 1_000_000; // 1MB max (very generous)
        if row_size > MAX_REASONABLE_ROW_SIZE {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Unreasonably large row_size={} at offset {} (max: {}). Likely partition tombstone or format error.",
                row_size,
                offset,
                MAX_REASONABLE_ROW_SIZE
            )));
        }

        // Row payloads can span multiple compressed chunks in V5CompressedLegacy format.
        // The reader has already stitched all chunks together (see get_all_entries()),
        // so row_size is valid across chunk boundaries. We MUST NOT validate against
        // individual chunk sizes as rows naturally span chunks in Cassandra's format.
        // This is NOT corruption - it's the intended file layout.

        log::debug!(
            "V5CompressedLegacy: Parsed row metadata at offset {}: header_size={} bytes, row_size={} bytes, timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, row_header.header_size, row_size, row_header.timestamp, row_header.ttl, row_header.local_deletion_time
        );

        // Row tombstone detection (Issue #191, Phase 2) + coexistence (Issue #932).
        //
        // A row with ROW_HAS_DELETION (0x10) carries a row-level deletion. HISTORICALLY
        // such a row had NO cells, so the parser skipped cell parsing entirely. But a
        // row deletion can COEXIST with surviving cells (cells written strictly after
        // the deletion — issue #932): the writer emits HAS_DELETION + HAS_TIMESTAMP +
        // the surviving cells. We must therefore skip cell parsing ONLY when there are
        // no cell bytes after the row header; otherwise fall through to the normal cell
        // loop so the surviving cells are read (the row header still carries the
        // deletion via `local_deletion_time` / `marked_for_delete_at`).
        //
        // Calculate offset after row data (based on row_size from header).
        //
        // CRITICAL FIX (Issue #237): row_size is measured from AFTER the row_size VInt,
        // not from where it starts. This matches Cassandra's getFilePointer() semantics:
        //   next_position = row_size_value + position_after_reading_row_size_vint
        let after_row_offset =
            (row_metadata_offset + row_header.row_size_vint_len) + row_size as usize;
        // Cell data (if any) begins right after the row header. When that start
        // reaches the row boundary there are no cells — a pure row tombstone.
        let cell_data_start = row_metadata_offset + row_header.header_size;
        let has_cell_bytes = cell_data_start < after_row_offset;

        if row_header.local_deletion_time.is_some() && !has_cell_bytes {
            log::debug!(
                "V5CompressedLegacy: Pure row tombstone (deletion_time={:?}), skipping cell parsing",
                row_header.local_deletion_time
            );

            // Validate we have enough data
            if after_row_offset > data.len() {
                let remaining = data
                    .len()
                    .saturating_sub(row_metadata_offset + row_header.row_size_vint_len);
                return Err(Error::corruption(format!(
                    "V5CompressedLegacy: Not enough bytes for row data at offset {} (need {}, have {})",
                    row_metadata_offset + row_header.row_size_vint_len,
                    row_size,
                    remaining
                )));
            }

            let next_offset = after_row_offset;
            log::debug!(
                "V5CompressedLegacy: Skipped tombstoned row, next offset = {}",
                next_offset
            );

            // Return empty cells for tombstoned row (no cell metadata, no complex meta)
            return Ok((
                cells,
                cell_meta,
                Some(row_header),
                next_offset,
                is_static,
                None,
            ));
        }

        // Advance offset past row metadata to start of cell data
        let mut offset = offset + row_header.header_size;

        log::debug!(
            "V5CompressedLegacy: Cell data starts at offset {}, first 32 bytes: {}",
            offset,
            hex::encode(&data[offset..std::cmp::min(offset + 32, data.len())])
        );

        // Cell flags validation: First byte should be valid cell flags (0x00-0x1F) for simple cells
        // Common flags: 0x00 (basic cell), 0x08 (USE_ROW_TIMESTAMP), 0x04 (HAS_EMPTY_VALUE)
        // Deleted cells have 0x01 (IS_DELETED), expiring cells have 0x02 (IS_EXPIRING)
        //
        // NOTE: For complex columns (non-frozen collections), the first byte is a VInt for the
        // cell count, which may have values > 0x1F. This is normal and not an error.
        // The validation below is only accurate for tables with all simple cells.
        if offset < data.len() {
            let first_byte = data[offset];
            if first_byte <= 0x1F {
                debug!(
                    "V5CompressedLegacy: Valid cell flags 0x{:02x} at offset {} after row header",
                    first_byte, offset
                );
            } else {
                debug!(
                    "V5CompressedLegacy: First byte 0x{:02x} at offset {} (> 0x1F) - may be VInt for complex column cell count",
                    first_byte, offset
                );
            }
        }

        // CRITICAL: V5CompressedLegacy format stores cells WITHOUT column names
        // or column IDs in the binary data. Cells appear in SCHEMA DEFINITION ORDER
        // (the order columns were defined in CREATE TABLE), NOT alphabetical order.
        //
        // NULL/missing columns are handled by:
        // - Checking for cell marker (0x08) before attempting to parse
        // - If no marker found or parse fails, column is NULL (not present)
        // - Continue to next column in schema order
        //
        // This implementation uses schema definition order directly, which is the
        // correct approach per Cassandra 5.0 SerializationHeader semantics.

        // CRITICAL FIX (Issue #164): Filter out partition keys and clustering keys!
        // The schema.columns list contains ALL columns (including keys), but cells
        // are only stored for REGULAR columns. Partition/clustering keys are part
        // of the row key and do NOT have cell data.
        let partition_key_names: std::collections::HashSet<_> = schema
            .partition_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        let clustering_key_names: std::collections::HashSet<_> = schema
            .clustering_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();

        // CRITICAL FIX (Issue #191): Use serialization header column order, not schema order
        // Cassandra 5.0 V5CompressedLegacy stores cells in the order defined by Statistics.db
        // serialization header (alphabetical by ColumnIdentifier/comparator), NOT CQL schema order.
        // We must iterate reader.header.columns directly to align binary layout with logical columns.
        //
        // Issue #702 FIX: For tables with BOTH static and regular columns, Cassandra's
        // missing_columns_bitmap is relative to the column group of the current row kind:
        //   - Static rows:  bitmap covers only static columns
        //   - Regular rows: bitmap covers only regular columns
        // Including the wrong group shifts all bitmap indices, causing columns to be
        // silently absent or misread.  Filter columns_in_order to the matching kind.
        // Each entry pairs the supplied schema `Column` (drives column identity /
        // order) with the AUTHORITATIVE on-disk SerializationHeader marshal type
        // (`ColumnInfo.column_type`). The on-disk marshal type is the no-heuristics
        // source of truth (issue #28) used to decide complex-ness and to decode
        // complex values — e.g. `UserType(...)` for a non-frozen multicell UDT
        // (issue #1081), or `FrozenType(UserType(...))` for a frozen UDT whose
        // supplied short form carries no field defs (issue #1080).
        //
        // `schema` is `None` for a column present on disk but ABSENT from the
        // supplied schema — a DROPPED / evolved-away column.  Such a column is NOT
        // emitted, but its bytes MUST still be consumed from the cell stream so the
        // trailing columns stay byte-aligned (issue #1080 Part 2: the gen-1 header
        // carries dropped tuple/UDT columns ahead of `survivor`).  We therefore keep
        // dropped columns in iteration order (driven by the header) rather than
        // filtering them out, which would silently misalign every following column.
        //
        // `header_type` is `None` only on the header-empty fallback path (synthetic
        // SSTables) where the supplied schema type is all we have; on that path
        // every iterated column is schema-present by construction.
        struct ColumnToParse<'a> {
            schema: Option<&'a crate::schema::Column>,
            header_type: Option<&'a str>,
        }

        let columns_in_order: Vec<ColumnToParse> = if !reader.header.columns.is_empty() {
            // Issue #1046: resolve each header column to its schema `Column` by an
            // allocation-free linear scan of `schema.columns` rather than building
            // (and immediately discarding) a `HashMap<String, &Column>` per row.
            //
            // The old map cost one `String` clone per schema column PLUS one
            // HashMap allocation on EVERY parsed row — the dominant per-row
            // allocation-count site in the read/scan hot path (dhat: the single
            // largest call-site for a full scan, scaling linearly with
            // rows × schema-columns). The map was local to this function and never
            // escaped, and it was queried at most once per header column, so a
            // borrowing `iter().find()` is byte-for-byte equivalent: `HashMap::get`
            // and `find(|c| c.name == name)` both return the column whose name
            // matches exactly. Cassandra schemas have a handful of columns, so the
            // linear scan is also cheaper in practice than a per-row hash build —
            // and it allocates nothing, which is the point of this change.
            let resolve_schema = |name: &str| -> Option<&crate::schema::Column> {
                schema.columns.iter().find(|col| col.name == name)
            };

            // Iterate serialization header columns in exact order (skipping keys,
            // and filtering to match the current row's static/regular kind).
            reader
                .header
                .columns
                .iter()
                .filter(|col_info| {
                    !col_info.is_primary_key
                        && !col_info.is_clustering
                        && col_info.is_static == is_static
                })
                .map(|col_info| ColumnToParse {
                    schema: resolve_schema(&col_info.name),
                    header_type: Some(col_info.column_type.as_str()),
                })
                .collect()
        } else {
            // Fallback to schema order when header is empty (shouldn't happen for real SSTables)
            log::warn!("V5CompressedLegacy: reader.header.columns is empty, falling back to schema order (may cause column misalignment)");
            schema
                .columns
                .iter()
                .filter(|col| {
                    !partition_key_names.contains(col.name.as_str())
                        && !clustering_key_names.contains(col.name.as_str())
                        && col.is_static == is_static // Issue #702: match row kind
                })
                .map(|col| ColumnToParse {
                    schema: Some(col),
                    header_type: None,
                })
                .collect()
        };

        // Filter columns by missing_columns_bitmap when present.
        // The bitmap indicates which columns are MISSING (bit=1 → absent).
        // We only parse cells for columns that are actually present in the data.
        // The bitmap is indexed by the ON-DISK column order (header order), which is
        // exactly `columns_in_order` (dropped columns retained), so the index
        // alignment is preserved.
        let columns_to_parse: Vec<ColumnToParse> = match row_header.missing_columns_bitmap {
            Some(bitmap) => {
                let total_columns = columns_in_order.len();
                let filtered: Vec<_> = columns_in_order
                    .into_iter()
                    .enumerate()
                    .filter(|(idx, _)| {
                        // Bitmap only covers the first 64 columns (u64).
                        // Columns beyond index 63 are not represented in the
                        // bitmap and are treated as present.
                        *idx >= 64 || (bitmap & (1u64 << idx)) == 0
                    })
                    .map(|(_, col)| col)
                    .collect();
                log::debug!(
                    "V5CompressedLegacy: Column bitmap 0x{:X} filters {} → {} columns",
                    bitmap,
                    total_columns,
                    filtered.len()
                );
                filtered
            }
            None => columns_in_order,
        };

        log::debug!("V5CompressedLegacy: Parsing {} cells in SERIALIZATION HEADER ORDER starting at offset {} (row header was {} bytes)", columns_to_parse.len(), offset, row_header.header_size);
        log::debug!(
            "V5CompressedLegacy: Column order: {:?}",
            columns_to_parse
                .iter()
                .map(|c| c.schema.map(|s| s.name.as_str()).unwrap_or("<dropped>"))
                .collect::<Vec<_>>()
        );
        log::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        // Issue #221: Check if row has complex deletion info for non-frozen collections
        let has_complex_deletion = (row_flags & ROW_HAS_COMPLEX_DELETION) != 0;
        if has_complex_deletion {
            log::debug!("V5CompressedLegacy: Row has HAS_COMPLEX_DELETION flag (0x40) set");
        }

        for (col_idx, ctp) in columns_to_parse.iter().enumerate() {
            let header_type: Option<&str> = ctp.header_type;

            // A column present on disk but ABSENT from the supplied schema is a
            // DROPPED column (issue #1080 Part 2): its bytes MUST be consumed to
            // keep the trailing columns aligned, but it is NOT emitted. We decode
            // it with a synthetic Column whose `data_type` is the AUTHORITATIVE
            // on-disk header marshal string (the only type metadata we have), then
            // discard the value. `emit` gates every cell/metadata insertion below.
            let emit = ctp.schema.is_some();
            // Synthetic column used ONLY to consume a DROPPED column's bytes; built
            // from the on-disk header marshal type. Initialized lazily (deferred
            // binding) so the common schema-present path pays no allocation — the
            // dropped-column path is rare. The holder outlives the borrow below.
            let dropped_column_holder;
            let column: &crate::schema::Column = match ctp.schema {
                Some(c) => c,
                None => {
                    dropped_column_holder = crate::schema::Column {
                        name: format!("__dropped_col_{col_idx}"),
                        data_type: header_type.unwrap_or("blob").to_string(),
                        nullable: true,
                        default: None,
                        is_static,
                    };
                    &dropped_column_holder
                }
            };

            // Issue #1081: the AUTHORITATIVE complex-ness / complex-decode type.
            // Prefer the on-disk SerializationHeader marshal type (carries
            // `UserType(...)` for a non-frozen UDT, which the supplied schema's
            // bare short form cannot express); fall back to the supplied schema
            // type only on the header-empty path (no-heuristics: both are
            // authoritative metadata, never guessed from bytes — issue #28). For a
            // dropped column the synthetic `column.data_type` IS the header marshal
            // type, so this resolves identically either way.
            let complex_type: &str = header_type.unwrap_or(&column.data_type);

            if offset >= data.len() {
                log::debug!(
                    "V5CompressedLegacy: Reached end of data at column {} ('{}'), parsed {}/{} cells",
                    col_idx,
                    column.name,
                    cells.len(),
                    columns_to_parse.len()
                );
                break;
            }

            // Issue #221: Branch based on column type - complex columns need special parsing
            // Issue #693: simple columns return 4-tuple including cell timestamp / expiration;
            //             complex columns return 2-tuple and inherit the row-level timestamp.
            if Self::is_complex_column(complex_type) {
                log::debug!(
                    "V5CompressedLegacy: Column '{}' is complex (non-frozen collection / multicell UDT), using parse_complex_column",
                    column.name
                );
                // Epic #899: on the compaction read path collect per-element
                // cells + the real complex deletion into `compaction_complex_out`
                // (otherwise this is the user-facing read path: collapsed value
                // only, byte-unchanged). Elements inherit the row liveness
                // timestamp when they carry USE_ROW_TIMESTAMP.
                let parse_result = if compaction_complex_out.is_some() {
                    let row_ts = row_header.timestamp.unwrap_or(0);
                    let mut element_buf = Vec::new();
                    self.parse_complex_column_inner(
                        data,
                        offset,
                        column,
                        complex_type,
                        has_complex_deletion,
                        row_ts,
                        Some(&mut element_buf),
                    )
                    .map(|(value, new_offset, col_meta)| {
                        if emit {
                            if let Some(ref mut out) = compaction_complex_out {
                                // Capture the whole-collection collapsed value for the
                                // byte-neutral Phase A output path (roborev #863).
                                out.insert(
                                    column.name.clone(),
                                    (col_meta.complex_deletion, element_buf, value.clone()),
                                );
                            }
                        }
                        (value, new_offset, col_meta)
                    })
                } else {
                    self.parse_complex_column(
                        data,
                        offset,
                        column,
                        complex_type,
                        has_complex_deletion,
                        reader,
                    )
                };
                match parse_result {
                    Ok((value, new_offset, col_meta)) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✓ Complex column {} '{}' = {:?}, consumed {} bytes",
                            col_idx, column.name, value, new_offset - offset
                        );
                        // Normal read-path (WRITETIME/TTL queries): use the row-level liveness
                        // timestamp unchanged.  Cassandra's WRITETIME(non_frozen_collection) on
                        // the standard read path returns the row timestamp, not per-element max.
                        // The delta-scan path computes its own max-element-writetime from
                        // ComplexColumnMeta (stored separately below) and never reads this field
                        // for collection columns.  Do NOT mutate this with max_element_writetime
                        // here — that would silently change WRITETIME(col) on the ordinary path
                        // (roborev Finding 1).
                        if emit {
                            if let Some(ref mut meta_map) = cell_meta {
                                let row_ts = row_header.timestamp.unwrap_or(0);
                                meta_map.insert(
                                    column.name.clone(),
                                    CellWriteMetadata {
                                        write_timestamp_micros: row_ts,
                                        expiration: None,
                                    },
                                );
                            }
                            // DS4 (Issue #700): Store ComplexColumnMeta for delta-scan callers.
                            if let Some(ref mut ccm_map) = complex_col_meta {
                                ccm_map.insert(column.name.clone(), col_meta);
                            }
                            cells.insert(column.name.clone(), value);
                        }
                        offset = new_offset;
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✗ Complex column {} '{}' at offset {} FAILED: {}",
                            col_idx, column.name, offset, e
                        );
                        break;
                    }
                }
            } else {
                match self.parse_cell_value_schema_order(data, offset, column, header_type, reader)
                {
                    Ok((value, cell_own_ts, cell_exp, new_offset)) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✓ Column {} '{}' ({}) = {:?}, consumed {} bytes",
                            col_idx,
                            column.name,
                            column.data_type,
                            value,
                            new_offset - offset
                        );
                        // Only compute and store per-cell metadata when the caller requested it.
                        // On the normal read hot-path (want_cell_metadata == false), cell_meta is
                        // None and this entire block is skipped — zero allocations per cell.
                        // `emit` is false for a DROPPED column (issue #1080 Part 2): we still
                        // advanced `offset` to consume its bytes, but emit no cell/metadata.
                        if emit {
                            if let Some(ref mut meta_map) = cell_meta {
                                // Resolve effective write timestamp:
                                // use cell's own timestamp when present, else row-level liveness timestamp.
                                let effective_ts = cell_own_ts
                                    .unwrap_or_else(|| row_header.timestamp.unwrap_or(0));
                                // Resolve expiration: cell-level wins; fall back to row-level TTL when
                                // the cell used USE_ROW_TTL (cell_exp is None in that case).
                                // USE_ROW_TTL path: row_header.ttl is the row-level TTL in seconds.
                                // row_header.local_deletion_time is the corresponding expires_at (seconds).
                                let row_level_exp =
                                    match (row_header.ttl, row_header.local_deletion_time) {
                                        (Some(ttl_s), Some(ldt_s)) => Some(CellExpiration {
                                            ttl_seconds: ttl_s,
                                            expires_at_seconds: ldt_s as i64,
                                        }),
                                        _ => None,
                                    };
                                let effective_exp = cell_exp.or(row_level_exp);
                                meta_map.insert(
                                    column.name.clone(),
                                    CellWriteMetadata {
                                        write_timestamp_micros: effective_ts,
                                        expiration: effective_exp,
                                    },
                                );
                            }
                            cells.insert(column.name.clone(), value);
                        }
                        offset = new_offset;
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✗ Column {} '{}' ({}) at offset {} FAILED: {}",
                            col_idx,
                            column.name,
                            column.data_type,
                            offset,
                            e
                        );
                        // CRITICAL FIX: Stop parsing remaining columns when we hit an error
                        // The offset doesn't advance here, but we exit the loop cleanly
                        // rather than continuing with invalid offset
                        break;
                    }
                }
            }
        }

        log::debug!(
            "V5CompressedLegacy: Parsed {}/{} columns (missing columns are NULL)",
            cells.len(),
            columns_to_parse.len()
        );
        log::debug!(
            "V5CompressedLegacy: Cells HashMap keys: {:?}",
            cells.keys().collect::<Vec<_>>()
        );

        debug!("V5CompressedLegacy: Parsed total of {} cells", cells.len());

        // Calculate offset after cell data (based on row_size from header)
        //
        // CRITICAL (Issue #237): row_size is measured from AFTER the row_size VInt,
        // not from where it starts. This matches Cassandra's getFilePointer() semantics:
        //   next_position = row_size_value + position_after_reading_row_size_vint
        //
        // Formula: (row_metadata_offset + row_size_vint_len) + row_size
        //
        // There is NO trailing field in V5CompressedLegacy format - the next partition/row
        // starts immediately after row_size bytes from this position.
        let row_size_counted_from = row_metadata_offset + row_header.row_size_vint_len;
        let after_cells_offset = row_size_counted_from + row_size as usize;

        // Validate we have enough data
        if after_cells_offset > data.len() {
            let remaining = data.len().saturating_sub(row_size_counted_from);
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Not enough bytes for row data at offset {} (need {}, have {})",
                row_size_counted_from, row_size, remaining
            )));
        }

        // No trailing field - next partition/row starts immediately
        let next_offset = after_cells_offset;

        debug!(
            "V5CompressedLegacy: Row complete - row_size={} bytes, next offset = {} (counted from {}, is_static={})",
            row_size, next_offset, row_size_counted_from, is_static
        );

        Ok((
            cells,
            cell_meta,
            Some(row_header),
            next_offset,
            is_static,
            complex_col_meta,
        ))
    }

    /// Test-only helper that parses the cell header (flags + conditional temporal
    /// metadata) and returns the offset at which the value bytes begin.
    ///
    /// This mirrors the logic in `parse_cell_value_schema_order` for the conditional
    /// sections (Steps 1-3), but stops before the value parse.  It is used by the
    /// S1 audit verification tests (Issue #623) to confirm that:
    ///   - USE_ROW_TIMESTAMP (0x08) causes the timestamp VInt to be ABSENT
    ///   - USE_ROW_TTL (0x10) without IS_EXPIRING causes LDT/TTL to be ABSENT
    ///
    /// Returns `(flags, value_start_offset)`.
    #[cfg(test)]
    pub(super) fn parse_cell_header_end_offset(
        &self,
        data: &[u8],
        start_offset: usize,
    ) -> Result<(u8, usize)> {
        const CELL_IS_DELETED: u8 = 0x01;
        const CELL_IS_EXPIRING: u8 = 0x02;
        const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
        const CELL_USE_ROW_TTL: u8 = 0x10;

        if start_offset >= data.len() {
            return Err(Error::corruption(
                "cell_header_end_offset: no flags byte".to_string(),
            ));
        }
        let flags = data[start_offset];
        let mut offset = start_offset + 1;

        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        // Step 1: skip timestamp VInt if not using row timestamp.
        // Skip-only: byte advancement is identical for vint/vuint, but use the
        // UNSIGNED variant to match the writer encoding (roborev #863).
        if !use_row_timestamp {
            let (remaining, _ts_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse timestamp VInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 2: skip LDT VUInt if not using row TTL and (deleted or expiring)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse LDT VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 3: skip TTL VUInt if not using row TTL and expiring
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse TTL VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }

        Ok((flags, offset))
    }
}
