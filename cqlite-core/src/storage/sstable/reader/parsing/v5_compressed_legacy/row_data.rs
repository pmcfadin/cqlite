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
            // Build lookup map from schema for column details
            let schema_map: HashMap<String, &crate::schema::Column> = schema
                .columns
                .iter()
                .map(|col| (col.name.clone(), col))
                .collect();

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
                    schema: schema_map.get(&col_info.name).copied(),
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

    /// Parse a single cell value WITHOUT column name (schema-order format)
    ///
    /// Cell format in V5CompressedLegacy follows Cassandra 5.0 cell serialization:
    /// - First byte: Cell flags (bitset, valid range: 0x00-0x1F)
    ///   - 0x01 = IS_DELETED_MASK (tombstone)
    ///   - 0x02 = IS_EXPIRING_MASK (has TTL)
    ///   - 0x04 = HAS_EMPTY_VALUE_MASK (no value bytes)
    ///   - 0x08 = USE_ROW_TIMESTAMP_MASK (use row timestamp)
    ///   - 0x10 = USE_ROW_TTL_MASK (use row TTL)
    /// - Conditional timestamp/TTL/deletion fields (based on flags)
    /// - Value data (if HAS_EMPTY_VALUE not set)
    ///
    /// See CASSANDRA_5_CELL_DESERIALIZATION_FORMAT.md for complete specification.
    ///
    /// Returns: `(value, cell_own_timestamp, expiration, new_offset)` where:
    /// - `cell_own_timestamp`: the cell's own decoded timestamp in µs, or `None`
    ///   when the cell inherits the row-level timestamp (`USE_ROW_TIMESTAMP` flag).
    /// - `expiration`: TTL / localDeletionTime pair when the cell is expiring, or
    ///   `None` when the cell has no TTL.
    pub(super) fn parse_cell_value_schema_order(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        // AUTHORITATIVE on-disk SerializationHeader marshal type for this column
        // (issue #1080). Used as a fallback to resolve a `frozen<udt>` whose
        // supplied schema short form carries no field defs and no UdtRegistry is
        // wired: the header type is the full `FrozenType(UserType(ks,name,fields))`
        // marshal string, decoded structurally rather than guessed. `None` on the
        // header-empty synthetic path and on internal recursive calls.
        header_type: Option<&str>,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<(Value, Option<i64>, Option<CellExpiration>, usize)> {
        // Cell flag constants (from Cassandra 5.0 Cell.Serializer)
        const CELL_IS_DELETED: u8 = 0x01;
        const CELL_IS_EXPIRING: u8 = 0x02;
        const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
        const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
        const CELL_USE_ROW_TTL: u8 = 0x10;

        // Read cell flags byte
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Cell '{}': unexpected end at flags byte",
                column.name
            )));
        }
        let flags = data[offset];

        // CRITICAL FIX (Issue #191): Validate flags are in valid range (0x00-0x1F)
        // Bits 0x20, 0x40, 0x80 are row-level flags and should NEVER appear in cell flags.
        // If we see these bits, the offset is misaligned (reading row data at cell position).
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Cell '{}': invalid cell flags 0x{:02x} at offset {} (bits 0x20/0x40/0x80 indicate offset misalignment)",
                column.name, flags, offset
            )));
        }

        offset += 1;

        // Decode flags
        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
        let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        log::debug!(
            "V5CompressedLegacy: Cell '{}' flags=0x{:02x} (deleted={}, expiring={}, empty={}, use_row_ts={}, use_row_ttl={})",
            column.name, flags, is_deleted, is_expiring, has_empty_value, use_row_timestamp, use_row_ttl
        );

        // === PHASE 2: Parse conditional fields between flags and value ===
        // Based on Cassandra 5.0 Cell.Serializer format specification

        // Step 1: Read timestamp (if not using row timestamp)
        // Issue #505: capture the actual cell timestamp so deleted cells can carry it
        // in a Value::Tombstone.
        //
        // Fix #629 (C2): Cell timestamp delta is UNSIGNED VInt per Cassandra
        // SerializationHeader.java:165: out.writeUnsignedVInt(timestamp - stats.minTimestamp).
        let mut cell_timestamp: Option<i64> = None;
        if !use_row_timestamp {
            let (remaining, timestamp_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse timestamp delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let absolute_ts = self.min_timestamp.wrapping_add(timestamp_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' timestamp_delta={} (min_timestamp={}) absolute={}",
                column.name,
                timestamp_delta,
                self.min_timestamp,
                absolute_ts,
            );
            cell_timestamp = Some(absolute_ts);
        }

        // Step 2: Read localDeletionTime (if deleted or expiring, and not using row TTL)
        // Captured as absolute epoch-seconds for CellExpiration.expires_at_seconds.
        let mut cell_local_deletion_time: Option<i64> = None;
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, deletion_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse localDeletionTime delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let abs_ldt = self
                .min_local_deletion_time
                .wrapping_add(deletion_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' deletion_delta={} (min_local_deletion_time={}) abs_ldt={}",
                column.name,
                deletion_delta,
                self.min_local_deletion_time,
                abs_ldt
            );
            cell_local_deletion_time = Some(abs_ldt);
        }

        // Step 3: Read TTL (if expiring and not using row TTL)
        // Captured as absolute TTL seconds for CellExpiration.ttl_seconds.
        let mut cell_ttl_seconds: Option<i32> = None;
        if !use_row_ttl && is_expiring {
            let (remaining, ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse TTL delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            // Absolute TTL = min_ttl + delta (seconds).  Clamp to i32 range for the
            // CellExpiration.ttl_seconds field (Cassandra caps TTL at ~630M seconds).
            let abs_ttl = self.min_ttl.unwrap_or(0).wrapping_add(ttl_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' ttl_delta={} (min_ttl={:?}) abs_ttl={}",
                column.name,
                ttl_delta,
                self.min_ttl,
                abs_ttl
            );
            cell_ttl_seconds = Some(abs_ttl.min(i32::MAX as i64) as i32);
        }

        // Build per-cell expiration metadata (used when the flag is set).
        // Available at both return sites below — the tombstone path also uses
        // cell_timestamp so we compute expiration here before the tombstone check.
        let cell_expiration: Option<CellExpiration> =
            match (is_expiring, cell_local_deletion_time, cell_ttl_seconds) {
                (true, Some(expires_at), Some(ttl_secs)) => Some(CellExpiration {
                    ttl_seconds: ttl_secs,
                    expires_at_seconds: expires_at,
                }),
                // use_row_ttl path: expiration info comes from the row header (caller handles it).
                _ => None,
            };

        // Step 4: Cell path for complex columns (multi-cell collections/UDTs)
        // For now, skip this - we'll add in a future iteration when we handle complex columns.
        // Simple columns (int, text, boolean, uuid, etc.) don't have cell paths.

        // === End of Phase 2 conditional field parsing ===

        // CRITICAL: Inverted logic for HAS_EMPTY_VALUE_MASK
        // Flag NOT set (0x04 absent) = cell HAS value → read value bytes
        // Flag SET (0x04 present) = cell has NO value → return empty/null immediately
        let has_value = !has_empty_value;

        // Handle deleted cells (tombstones)
        // According to Cassandra 5.0 Cell.Serializer, deleted cells:
        // 1. Have IS_DELETED flag set
        // 2. May have deletion metadata (timestamp, localDeletionTime)
        // 3. Do NOT have value data (even if HAS_EMPTY_VALUE not set)
        //
        // Issue #505: emit Value::Tombstone(CellTombstone) so that the compaction
        // merger can apply cell-level shadowing semantics.  The actual deletion
        // timestamp is carried in the tombstone for timestamp-based LWW ordering.
        if is_deleted {
            let deletion_time = cell_timestamp.unwrap_or(0);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' is tombstone (deleted), returning Tombstone(deletion_time={})",
                column.name, deletion_time
            );
            return Ok((
                Value::Tombstone(TombstoneInfo {
                    deletion_time,
                    tombstone_type: TombstoneType::CellTombstone,
                    // On-disk `localDeletionTime` (GC clock, seconds) for the cell
                    // tombstone; `0` when not surfaced (#873).
                    local_deletion_time: cell_local_deletion_time.unwrap_or(0),
                    ttl: None,
                    range_start: None,
                    range_end: None,
                }),
                cell_timestamp,
                cell_expiration,
                offset,
            ));
        }

        // Handle empty cells (no value bytes to read)
        if !has_value {
            log::debug!(
                "V5CompressedLegacy: Cell '{}' has HAS_EMPTY_VALUE flag, returning empty value",
                column.name
            );
            // Issue #1077: the empty (zero-length) value MUST decode to the empty
            // value of the column's DECLARED type — never blindly `Text("")`.
            // An empty `blob` is `Blob([])` (sstabledump renders `"0x"`), an empty
            // text/ascii/varchar is `Text("")`. Mirrors the clustering-key EMPTY
            // handling above; fixed-width types should not normally carry an empty
            // value, so treat that as NULL with a warning.
            let empty_value = match column.data_type.to_lowercase().as_str() {
                "text" | "varchar" | "ascii" => Value::Text(String::new()),
                "blob" => Value::Blob(Vec::new()),
                _ => {
                    log::warn!(
                        "V5CompressedLegacy: EMPTY value for cell '{}' (type {}), treating as NULL",
                        column.name,
                        column.data_type
                    );
                    Value::Null
                }
            };
            return Ok((empty_value, cell_timestamp, cell_expiration, offset));
        }

        // At this point, we have a live cell with value data
        // The value parsing logic below is unchanged from the original implementation

        // Parse based on column type (data_type is a String with CQL type name)
        // CRITICAL: Normalize type name to lowercase for case-insensitive matching
        // Schema may provide "TEXT", "INT", etc. (uppercase) while match arms use lowercase
        let normalized_type = column.data_type.to_lowercase();
        let value = match normalized_type.as_str() {
            "boolean" => {
                // Boolean: [0x08][u8 value]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at boolean value",
                        column.name
                    )));
                }
                let bool_byte = data[offset];
                offset += 1;
                Value::Boolean(bool_byte != 0)
            }

            "int" => {
                // Integer (i32): fixed-width 4 bytes (no length prefix in Cassandra 5.0)
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for int, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let int_val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Integer(int_val)
            }

            "text" | "varchar" | "ascii" => {
                // Text: [VInt len][text bytes]
                // V5CompressedLegacy uses VInt length encoding for variable-length types
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse text length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for text, only {} available",
                        column.name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': invalid UTF-8 in text value: {}",
                        column.name, e
                    ))
                })?;

                offset += text_len;
                Value::Text(text)
            }

            "uuid" | "timeuuid" => {
                // UUID/TimeUUID: fixed-width 16 bytes (no length prefix in Cassandra 5.0 writer)
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 16 bytes for UUID, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                offset += 16;
                Value::Uuid(uuid_bytes)
            }

            "decimal" => {
                // Decimal: [VInt total_len][i32 scale][unscaled bytes]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at decimal length",
                        column.name
                    )));
                }

                let (remaining, total_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse decimal length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let total_len = total_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + total_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for decimal, only {} available",
                        column.name,
                        total_len,
                        data.len() - offset
                    )));
                }

                // First 4 bytes: scale (i32 BE)
                if total_len < 4 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': decimal length {} too small for scale",
                        column.name, total_len
                    )));
                }
                let scale = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);

                // Remaining bytes: unscaled value
                let unscaled = data[offset + 4..offset + total_len].to_vec();
                offset += total_len;

                Value::Decimal { scale, unscaled }
            }

            "bigint" => {
                // BigInt: fixed-width 8 bytes (no length prefix in Cassandra 5.0)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for bigint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::BigInt(val)
            }

            "counter" => {
                // Counter cells can arrive in two formats:
                //
                // 1. Real Cassandra CounterContext: [VInt length][header_size:i16][indices][shards]
                //    The counter value is the sum of all shard counts.
                //
                // 2. CQLite writer format (raw i64): [VInt(8)][8 bytes big-endian i64]
                //    The writer serialises Value::Counter as a plain 8-byte integer with a
                //    length prefix of 8, identical to how BigInt is written.
                //
                // We try CounterContext first and fall back to the raw-i64 interpretation
                // when the length prefix equals exactly 8 (the size of a raw i64).

                // Read the VInt length prefix.
                let (remaining, context_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse counter context length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let context_len = context_len as usize;
                let len_bytes_consumed = data[offset..].len() - remaining.len();
                offset += len_bytes_consumed;

                log::debug!(
                    "V5CompressedLegacy: Counter '{}' context_len={} (len prefix: {} bytes)",
                    column.name,
                    context_len,
                    len_bytes_consumed
                );

                if offset + context_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for counter context, only {} available",
                        column.name,
                        context_len,
                        data.len() - offset
                    )));
                }

                // Try the full CounterContext parse first.
                match Self::parse_counter_context(data, offset, &column.name) {
                    Ok((total, consumed)) if consumed == context_len => {
                        // Successfully parsed a proper CounterContext.
                        offset += consumed;
                        log::debug!(
                            "V5CompressedLegacy: Counter '{}' value={} (CounterContext), total consumed {} bytes",
                            column.name,
                            total,
                            len_bytes_consumed + context_len
                        );
                        Value::Counter(total)
                    }
                    _ if context_len == 8 => {
                        // A real Cassandra CounterContext is at minimum 36 bytes
                        // (2 header + 2 indices + 32 body for 1 shard), so
                        // context_len == 8 can only be produced by the CQLite writer
                        // which serialises Counter as a raw big-endian i64.
                        // This intentionally swallows any parse_counter_context error
                        // for 8-byte payloads, which is safe since a valid
                        // CounterContext can never be 8 bytes.
                        //
                        // Bounds already verified by the context_len check above.
                        let val = i64::from_be_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ]);
                        offset += 8;
                        log::debug!(
                            "V5CompressedLegacy: Counter '{}' value={} (raw i64 fallback), total consumed {} bytes",
                            column.name,
                            val,
                            len_bytes_consumed + 8
                        );
                        Value::Counter(val)
                    }
                    Err(e) => return Err(e),
                    Ok((_, consumed)) => {
                        return Err(Error::corruption(format!(
                            "Counter '{}': VInt length ({}) doesn't match parsed context size ({})",
                            column.name, context_len, consumed
                        )));
                    }
                }
            }

            "double" => {
                // Double: 8 bytes, f64 big-endian (NO length prefix)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for double, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let val = f64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Float(val)
            }

            "timestamp" => {
                // Timestamp: 8 bytes, i64 milliseconds big-endian (NO length prefix, per Cassandra spec)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for timestamp, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Timestamp(millis)
            }

            "date" => {
                // Date: [VInt len=4][i32 BE days]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at date length",
                        column.name
                    )));
                }

                let (remaining, date_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse date length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let date_len = date_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if date_len != 4 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected date length 4, got {}",
                        column.name, date_len
                    )));
                }

                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for date, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let stored = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Value::Date(days_since_epoch)
            }

            "duration" => {
                // Duration: [VInt len][months VInt][days VInt][nanos VInt]
                // Format: Variable-length encoding with 3 VInt components
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at duration length",
                        column.name
                    )));
                }

                let (remaining, duration_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let duration_len = duration_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + duration_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for duration, only {} available",
                        column.name,
                        duration_len,
                        data.len() - offset
                    )));
                }

                // Parse three VInt components from the duration_len bytes
                let duration_bytes = &data[offset..offset + duration_len];

                // Parse months (signed VInt)
                let (remaining, months) = parse_vint(duration_bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration months: {:?}",
                        column.name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse days (signed VInt)
                let (remaining, days) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration days: {:?}",
                        column.name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse nanoseconds (signed VInt)
                let (remaining, nanos) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration nanos: {:?}",
                        column.name, e
                    ))
                })?;

                // Verify we consumed all duration bytes
                if !remaining.is_empty() {
                    warn!(
                        "V5CompressedLegacy: Duration '{}' has {} extra bytes after parsing",
                        column.name,
                        remaining.len()
                    );
                }

                offset += duration_len;
                Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                }
            }

            "float" => {
                // Float: 4 bytes, f32 big-endian (NO length prefix, fixed size)
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for float, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Float(val as f64) // Convert f32 to f64 for storage
            }

            "smallint" | "short" => {
                // SmallInt: [VInt len=2][i16 BE]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at smallint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse smallint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 2 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected smallint length 2, got {}",
                        column.name, len
                    )));
                }

                if offset + 2 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 2 bytes for smallint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = i16::from_be_bytes([data[offset], data[offset + 1]]);
                offset += 2;
                Value::SmallInt(val)
            }

            "tinyint" | "byte" => {
                // TinyInt: [VInt len=1][i8]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at tinyint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse tinyint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 1 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected tinyint length 1, got {}",
                        column.name, len
                    )));
                }

                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 1 byte for tinyint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = data[offset] as i8;
                offset += 1;
                Value::TinyInt(val)
            }

            "time" => {
                // Time: [VInt len=8][i64 BE nanoseconds since midnight]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at time length",
                        column.name
                    )));
                }
                let (remaining, time_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse time length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let time_len = time_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;
                if time_len != 8 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected time length 8, got {}",
                        column.name, time_len
                    )));
                }
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for time value, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let nanos = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Time(nanos)
            }

            "inet" => {
                // Inet: [VInt len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at inet length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse inet length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 4 && len != 16 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': invalid inet length {}, expected 4 or 16",
                        column.name, len
                    )));
                }

                if offset + len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for inet, only {} available",
                        column.name,
                        len,
                        data.len() - offset
                    )));
                }

                let bytes = data[offset..offset + len].to_vec();
                offset += len;
                Value::Inet(bytes)
            }

            // Complex types: frozen, tuple, UDT
            type_str if type_str.starts_with("frozen<") => {
                // Frozen types: unwrap inner type and route to appropriate parser
                let inner_type = self.extract_frozen_inner_type(type_str)?;

                log::debug!(
                    "V5CompressedLegacy: Parsing frozen type '{}' -> inner type '{}'",
                    type_str,
                    inner_type
                );

                // Route to appropriate frozen collection parser
                let (inner_value, new_offset) = if inner_type.starts_with("list<") {
                    let element_type = self.extract_collection_element_type(&inner_type, "list")?;
                    self.parse_frozen_list_value(data, offset, &element_type, column, _reader)?
                } else if inner_type.starts_with("set<") {
                    let element_type = self.extract_collection_element_type(&inner_type, "set")?;
                    self.parse_frozen_set_value(data, offset, &element_type, column, _reader)?
                } else if inner_type.starts_with("map<") {
                    let (key_type, value_type) = self.extract_map_types(&inner_type)?;
                    self.parse_frozen_map_value(
                        data,
                        offset,
                        &key_type,
                        &value_type,
                        column,
                        _reader,
                    )?
                } else if Self::is_udt_type(&column.data_type) {
                    // Frozen UDT - parse using UDT parser
                    // The column.data_type contains the full Cassandra type string including UserType
                    log::debug!(
                        "V5CompressedLegacy: Parsing frozen UDT column '{}' type='{}'",
                        column.name,
                        column.data_type
                    );

                    // Parse UDT definition from the type string
                    let udt_def = Self::parse_udt_type_definition(&column.data_type)?;

                    // First read the VInt-prefixed blob length
                    let (remaining, blob_len_raw) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen UDT '{}': failed to parse blob length: {:?}",
                            column.name, e
                        ))
                    })?;
                    if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}': blob_len {} exceeds maximum {}",
                            column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                        )));
                    }
                    let blob_len = blob_len_raw as usize;
                    let bytes_consumed = data[offset..].len() - remaining.len();
                    offset += bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}': need {} bytes but only {} available",
                            column.name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    // Parse UDT value from the blob
                    let udt_data = &data[offset..offset + blob_len];
                    let (udt_value, _) = self.parse_udt_value(udt_data, 0, &udt_def, column)?;
                    offset += blob_len;

                    (udt_value, offset)
                } else if let Some(udt_def) = self
                    .udt_registry
                    .as_ref()
                    .and_then(|reg| reg.get_udt(&self.keyspace, &inner_type).cloned())
                {
                    // frozen<short_udt_name>: look up the concrete UDT definition in the
                    // registry (Issue #502).  This handles type strings like
                    // `frozen<person>` where "person" is a registered UDT rather than a
                    // collection or a full marshal-format UserType string.
                    log::debug!(
                        "V5CompressedLegacy: Resolving frozen UDT '{}' via registry for column '{}'",
                        inner_type,
                        column.name,
                    );

                    // Read VUInt-prefixed blob length (same framing as tuple and
                    // marshal-format UDT cells).
                    let (remaining, blob_len_raw) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): failed to parse blob length: {:?}",
                            inner_type, column.name, e
                        ))
                    })?;
                    if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): blob_len {} exceeds maximum {}",
                            inner_type, column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                        )));
                    }
                    let blob_len = blob_len_raw as usize;
                    let len_bytes_consumed = data[offset..].len() - remaining.len();
                    offset += len_bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): need {} bytes but only {} available",
                            inner_type,
                            column.name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    let udt_data = &data[offset..offset + blob_len];
                    let (udt_value, _) = self.parse_udt_value(udt_data, 0, &udt_def, column)?;
                    offset += blob_len;

                    (udt_value, offset)
                } else if let Some(ht) =
                    header_type.filter(|ht| Self::marshal_is_top_level_frozen_udt(ht))
                {
                    // Issue #1080: NO UdtRegistry is wired and the supplied schema
                    // short form `frozen<person_type>` carries no field defs, but
                    // the AUTHORITATIVE on-disk SerializationHeader marshal type for
                    // this column is the full
                    // `FrozenType(UserType(ks,hexname,field:Type,...))`. Decode the
                    // UDT STRUCTURALLY from that header type (no guessing, issue #28)
                    // rather than dropping the column (which also broke the Err→break
                    // loop, silently losing all trailing columns).
                    self.decode_frozen_udt_from_header_type(data, offset, ht, column)?
                } else {
                    // Detect bare identifiers that look like unregistered UDT names.
                    // A bare identifier has no '<' (not a container or tuple) and does not
                    // match any known CQL primitive type.  If we reach this branch with
                    // such an identifier it means the UDT was not in the registry — return
                    // an actionable schema error rather than silently producing a Blob.
                    //
                    // Legitimate fall-through types handled below:
                    //   • tuple<...>  (contains '<')
                    //   • known primitives: int, text, uuid, boolean, blob, float, double,
                    //     decimal, varint, bigint, counter, timestamp, date, time, duration,
                    //     inet, smallint, tinyint, varchar, ascii, timeuuid
                    const KNOWN_PRIMITIVES: &[&str] = &[
                        "int",
                        "bigint",
                        "counter",
                        "smallint",
                        "tinyint",
                        "text",
                        "varchar",
                        "ascii",
                        "uuid",
                        "timeuuid",
                        "boolean",
                        "blob",
                        "float",
                        "double",
                        "decimal",
                        "varint",
                        "timestamp",
                        "date",
                        "time",
                        "duration",
                        "inet",
                    ];
                    let is_container = inner_type.contains('<');
                    let is_primitive = KNOWN_PRIMITIVES.contains(&inner_type.as_str());
                    if !is_container && !is_primitive {
                        // Bare identifier that is neither a container nor a primitive —
                        // this is an unregistered UDT name.
                        return Err(Error::schema(format!(
                            "frozen<{inner}>: UDT '{inner}' not found in registry for keyspace '{}'; \
                             register it before reading",
                            self.keyspace,
                            inner = inner_type,
                        )));
                    }
                    // Non-collection / primitive frozen type — recurse normally.
                    // The recursive call now returns 4 elements; we only need value + offset.
                    let mut inner_column = column.clone();
                    inner_column.data_type = inner_type.clone();
                    let (inner_val, _inner_ts, _inner_exp, inner_off) = self
                        .parse_cell_value_schema_order(
                            data,
                            offset,
                            &inner_column,
                            None,
                            _reader,
                        )?;
                    (inner_val, inner_off)
                };

                offset = new_offset;

                // Wrap in Frozen
                Value::Frozen(Box::new(inner_value))
            }

            type_str if type_str.starts_with("tuple<") => {
                // Tuple types: parse fixed number of elements
                self.parse_tuple_value(data, &mut offset, type_str, column, _reader)?
            }

            // Non-frozen collections: list, set, map
            // TODO(Issue #162, Task 3): Multi-cell collection parsing
            //
            // Collections in V5CompressedLegacy are stored as MULTIPLE CELLS with path identifiers,
            // NOT as single blob values. The current single-cell parser cannot handle this.
            //
            // Format (from sstabledump analysis):
            //   {"name": "scores", "deletion_info": {...}},  // Collection tombstone
            //   {"name": "scores", "path": ["uuid1"], "value": 23},  // Element 1
            //   {"name": "scores", "path": ["uuid2"], "value": 99},  // Element 2
            //
            // Required implementation:
            //   1. Parse cell path (clustering key bytes) for each collection element
            //   2. Detect collection tombstone cell (has deletion_info, no path/value)
            //   3. Read N element cells (each with path + value)
            //   4. Aggregate elements into Value::List/Set/Map based on column type
            //   5. Handle different path encodings:
            //      - list<T>: path is UUID bytes (timeuuid for ordering)
            //      - set<T>: path is serialized element value (key), value is empty
            //      - map<K,V>: path is serialized key, value is serialized value
            //
            // This is a fundamental architectural change requiring cell-level parsing
            // before column-level aggregation. For now, return stub to unblock downstream work.
            type_str
                if type_str.starts_with("list<")
                    || type_str.starts_with("set<")
                    || type_str.starts_with("map<") =>
            {
                warn!(
                    "V5CompressedLegacy: Non-frozen collection '{}' type '{}' requires multi-cell parsing (not yet implemented). \
                     Collections are stored as multiple cells with path identifiers, requiring cell-level aggregation. \
                     Returning empty collection as placeholder. See Issue #162 Task 3 for implementation plan.",
                    column.name, column.data_type
                );

                // Return empty collection based on type
                if type_str.starts_with("list<") {
                    Value::List(Vec::new())
                } else if type_str.starts_with("set<") {
                    Value::Set(Vec::new())
                } else {
                    Value::Map(Vec::new())
                }
            }

            // Issue #1080 / roborev job 1363: marshal-form frozen UDT. When the
            // schema is DERIVED FROM the on-disk header (rather than supplied as a
            // CQL short form), `column.data_type` is the authoritative marshal
            // string `org.apache.cassandra.db.marshal.FrozenType(...UserType...)`,
            // which does NOT start with CQL `frozen<` and so misses the arm above.
            // Decode it structurally from that marshal type (same authoritative
            // path as the supplied-schema header fallback) instead of blobbing it.
            // `marshal_is_top_level_frozen_udt` accepts ONLY a top-level
            // `FrozenType(UserType(...))` (NOT a frozen collection that contains a
            // UDT, e.g. `FrozenType(ListType(UserType(...)))` — roborev 1365), and a
            // non-frozen top-level UDT is routed to the complex branch by
            // `is_complex_column`, so reaching here means a single-cell frozen UDT
            // → wrap in `Value::Frozen` (consistent with the CQL `frozen<` arm).
            _ if Self::marshal_is_top_level_frozen_udt(&column.data_type) => {
                let (udt_value, new_offset) = self.decode_frozen_udt_from_header_type(
                    data,
                    offset,
                    &column.data_type,
                    column,
                )?;
                offset = new_offset;
                Value::Frozen(Box::new(udt_value))
            }

            // TODO(Issue #162): UDT parsing requires schema registry access
            // For now, UDTs fall through to blob. Future implementation will:
            // - Extract UDT name from type_str
            // - Look up UDT definition in schema registry
            // - Parse fields according to UDT schema
            // - Return Value::Udt(UdtValue)

            // Default: treat as VInt-length-prefixed blob
            // CRITICAL: V5CompressedLegacy format uses VInt encoding for blob/bytes lengths,
            // NOT simple u8 length prefix. This allows blobs > 255 bytes.
            _ => {
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at blob length (type: {})",
                        column.name, column.data_type
                    )));
                }

                // Parse blob length as unsigned VInt (can be > 255 bytes)
                let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse blob length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let blob_len = blob_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for blob, only {} available (type: {})",
                        column.name,
                        blob_len,
                        data.len() - offset,
                        column.data_type
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }
        };

        Ok((value, cell_timestamp, cell_expiration, offset))
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
