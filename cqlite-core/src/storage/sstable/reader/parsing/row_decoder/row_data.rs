use super::*;

// Issue #3721: the per-column decode-failure policy lives in `column_decode_error`.
use super::column_decode_error::{column_decode_failure, dispatch_type, row_body_exhausted};

impl V5CompressedLegacyParser {
    /// Parse row data (header + cells) and return cells with new offset
    ///
    /// V5CompressedLegacy format stores cells WITHOUT column names in schema column order.
    /// Schema is REQUIRED to determine which column each value belongs to.
    ///
    /// Returns: `ParsedRow` = `(cells, row_header, new_offset, is_static)` where
    /// `is_static` is `true` when the row's `EXTENDED_IS_STATIC` flag was set.
    /// Static rows must be merged into clustering rows by the caller, not emitted directly.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn parse_row_data_with_offset(
        &self,
        data: &[u8],
        offset: usize,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        want_cell_metadata: bool,
        resolution: &RowColumnResolution,
        // Issue #1741 (Finding 1): read-side shadow for per-cell tombstone/TTL
        // filtering. `Some` only on user-facing SELECT reads (`read_shadowing`);
        // `None` for every physical consumer (compaction / delta-scan / tests), which
        // stay byte-unchanged.
        shadow: Option<&PartitionShadow>,
    ) -> Result<ParsedRow> {
        self.parse_row_data_with_offset_impl(
            data,
            offset,
            schema,
            reader,
            want_cell_metadata,
            None,
            resolution,
            shadow,
        )
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
        resolution: &RowColumnResolution,
        shadow: Option<&PartitionShadow>,
    ) -> Result<ParsedRow> {
        // Issue #1642 (K3): positional cell vector, built directly in
        // serialization-header (schema) column order. Determinism comes from
        // CONSTRUCTION — the emit path no longer allocates a per-row `HashMap` nor
        // alphabetically re-sorts each row. The interned column-name handle
        // (issue #1334) is a schema-owned `Arc<str>` shared across every cell/row,
        // so populating a cell with its name stays an `Arc::clone` refcount bump,
        // NOT a per-cell heap `String` allocation. Pre-sized to the on-disk data
        // column count PLUS the clustering-key count so the common case does not
        // reallocate — clustering-key cells are pushed FIRST (below), before the
        // data columns, so sizing to data columns alone reallocates on a clustered
        // table (issue #1642).
        let mut cells: RowCells =
            Vec::with_capacity(resolution.columns_for(false).len() + resolution.clustering_len());
        // Parallel per-cell write metadata map (populated alongside `cells`).
        // Only allocated when the caller actually needs WRITETIME/TTL metadata
        // (i.e. `want_cell_metadata == true`).  On the normal read path this stays
        // `None` so that zero HashMap allocations or inserts occur per cell.
        let mut cell_meta: Option<HashMap<String, CellWriteMetadata>> = if want_cell_metadata {
            // Issue #3058: explicit "a per-row cell-write-metadata map was
            // allocated" marker (see `storage::read_path_probe`), so the Flight
            // fast path can PROVE it builds none rather than infer it.
            crate::storage::read_path_probe::record_cell_metadata_map();
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
            tracing::debug!(
                "V5CompressedLegacy: Static row detected (extended_flags=0x{:02x}), skipping clustering prefix",
                extended_flags.unwrap_or(0)
            );
            (vec![], offset)
        };

        tracing::debug!(
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
        for (i, _ck) in schema.clustering_keys.iter().enumerate() {
            if i < clustering_values.len() {
                // Issue #1334: reuse the interned clustering-key name handle
                // (an `Arc::clone`) rather than cloning the schema `String`.
                if let Some(name) = resolution.clustering_name(i) {
                    cells.push((Arc::clone(name), clustering_values[i].clone()));
                }
            }
        }

        // Issue #1741 (Finding 1): per-cell shadow context for this row. `Some` only
        // on the user-facing SELECT path. `covering` is the deletion (µs) that shadows
        // any data older than it (partition tombstone folded with the open range
        // tombstone when this row's clustering falls inside it); `now` is the read
        // clock for per-cell TTL expiry. A data cell / whole collection whose
        // effective write ts <= `covering`, or which is TTL-expired at `now`, is
        // dropped below (no new per-cell allocation — the insert is made conditional).
        let cell_ctx: Option<(Option<i64>, i64)> =
            shadow.map(|sh| sh.cell_context(&clustering_values));

        // Step 3: Parse row metadata (row_size, prev_size, timestamps, etc.)
        //
        // CRITICAL (Issue #237): Save offset where row_size VInt STARTS.
        // The row_size value is measured from AFTER this VInt is consumed.
        // Formula: next_offset = (row_metadata_offset + row_size_vint_len) + row_size
        // This offset is right after the clustering prefix (which was already parsed).
        let row_metadata_offset = offset;
        let (mut row_header, row_size) =
            self.parse_row_metadata(data, offset, row_flags, extended_flags)?;

        // STRUCTURAL BOUND on row_size — the AUTHORITATIVE invariant, not a guess
        // (no-heuristics, #28 / #2436). The former arbitrary `MAX_REASONABLE_ROW_SIZE
        // = 1_000_000` cap made the driver fold its `Err` into a `None` and SILENTLY
        // DROP every legit >1 MB single-cell `text`/`blob` row → `Ok(0 rows)` for a
        // genuinely-written partition (#2436); a row body has no 1 MB limit. `data`
        // is the fully-materialised parse unit, so a row body cannot claim more bytes
        // than remain after its `row_size` VInt (overflow-safe vs `offset + row_size`).
        // RETURNS `Err` on genuine truncation at the parser's OWN return value; the
        // driver may still swallow that `Err` into a `None` on the final chunk (#2481).
        let row_body_start = row_metadata_offset + row_header.row_size_vint_len;
        let available = data.len().saturating_sub(row_body_start) as u64;
        if row_size > available {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: row_size={} at offset {} exceeds available data \
                 ({} bytes remain after the row_size VInt) — truncated or corrupt row",
                row_size, offset, available
            )));
        }

        // Row payloads can span multiple compressed chunks in V5CompressedLegacy format.
        // The reader has already stitched all chunks together (see get_all_entries()),
        // so row_size is valid across chunk boundaries. We MUST NOT validate against
        // individual chunk sizes as rows naturally span chunks in Cassandra's format.
        // This is NOT corruption - it's the intended file layout.

        tracing::debug!(
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
            tracing::debug!(
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
            tracing::debug!(
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

        tracing::debug!(
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

        // CRITICAL FIX (Issue #191): cells are stored in SERIALIZATION HEADER column
        // order (Statistics.db serialization header — alphabetical by
        // ColumnIdentifier/comparator), NOT CQL schema order. Partition/clustering
        // keys carry no cell data and are excluded; dropped columns (present on disk
        // but absent from the supplied schema) are RETAINED in order so their bytes
        // are consumed and trailing columns stay byte-aligned (issue #1080 Part 2).
        //
        // Issue #702: the on-disk column group differs by row kind, so the resolution
        // exposes a separate ordering for static vs regular rows.
        //
        // Issue #1046 (the true hoist): this header→schema resolution is CONSTANT for
        // every row in the block, so it is built ONCE (in `RowColumnResolution::build`
        // at the top of the per-block driver) and reused here. The per-row body now
        // performs ZERO schema-lookup allocations — no per-row `HashMap`, no per-row
        // `String` clone, no per-row `Vec` of columns.
        let columns_in_order: &[ColumnToParse] = resolution.columns_for(is_static);

        // Apply the missing_columns_bitmap INLINE (no per-row `Vec`): a column at
        // on-disk index `idx` is present iff `idx >= 64` (beyond the u64 bitmap,
        // treated present) or its bit is clear. The bitmap is indexed by the ON-DISK
        // column order, which is exactly `columns_in_order` (dropped columns retained),
        // so the index alignment is preserved.
        let missing_bitmap = row_header.missing_columns_bitmap;
        let is_present = |idx: usize| -> bool {
            match missing_bitmap {
                Some(bitmap) => idx >= 64 || (bitmap & (1u64 << idx)) == 0,
                None => true,
            }
        };

        tracing::debug!("V5CompressedLegacy: Parsing cells in SERIALIZATION HEADER ORDER starting at offset {} (row header was {} bytes, {} on-disk columns, bitmap={:?})", offset, row_header.header_size, columns_in_order.len(), missing_bitmap);
        tracing::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        // Issue #221: Check if row has complex deletion info for non-frozen collections
        let has_complex_deletion = (row_flags & ROW_HAS_COMPLEX_DELETION) != 0;
        if has_complex_deletion {
            tracing::debug!("V5CompressedLegacy: Row has HAS_COMPLEX_DELETION flag (0x40) set");
        }

        // Issue #1741 read-side shadowing aggregate. Computed from scalars the cell
        // parser already returns (no new allocation, no extra HashMap): the max data
        // cell write timestamp, the max expiring-cell expiry, and whether any data
        // cell is live-forever. Stashed on `row_header` after the loop and consulted
        // by the read emit paths to hide partition/range-tombstone-shadowed and
        // TTL-expired rows. Kept off the write/compaction reconciliation path.
        let mut agg_max_cell_ts: Option<i64> = None;
        let mut agg_max_expires_at: Option<i64> = None;
        let mut agg_has_live_forever = false;
        // #3094: PRESENCE of a tombstone cell — never a timestamp (`has_shadow_evidence`).
        let mut agg_has_deleted_cell = false;

        for (col_idx, ctp) in columns_in_order.iter().enumerate() {
            // Skip columns marked MISSING by the row's bitmap (inline, no per-row
            // allocation). `col_idx` is the ON-DISK column index — exactly what the
            // bitmap is indexed by — so this is identical to the prior pre-filtered
            // `columns_to_parse` Vec, just without materializing it.
            if !is_present(col_idx) {
                continue;
            }
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

            // Issue #3721: bytes exhausted with a column the row declares PRESENT still
            // outstanding is TRUNCATION, never a row boundary — see `row_body_exhausted`.
            if offset >= data.len() {
                return Err(row_body_exhausted(
                    column,
                    header_type,
                    (offset, data.len()),
                    (col_idx, columns_in_order.len(), cells.len()),
                ));
            }

            // Issue #221: Branch based on column type - complex columns need special parsing
            // Issue #693: simple columns return 4-tuple including cell timestamp / expiration;
            //             complex columns return 2-tuple and inherit the row-level timestamp.
            if ctp.is_complex {
                tracing::debug!(
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
                        // Compaction / delta read path: NO per-element filtering (it
                        // needs every element for reconciliation) — byte-unchanged.
                        None,
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
                    // Issue #1741 (per-element filtering): on the user-facing SELECT
                    // read path (`cell_ctx.is_some()`), thread the covering deletion +
                    // read clock + row-liveness ts into the element loop so shadowed /
                    // TTL-expired elements are skipped from the emitted container.
                    // `None` when no shadow context is active (tombstone-free reads and
                    // physical consumers) — byte-unchanged.
                    let element_filter = cell_ctx.map(|(cover, now)| ElementShadow {
                        cover,
                        now,
                        row_ts: row_header.timestamp,
                        // Issue #1741: the effective row expiry a USE_ROW_TTL
                        // collection element inherits — computed EXACTLY like the
                        // scalar USE_ROW_TTL cell path (see the `None if use_row_ttl`
                        // arm below): the pk-liveness localExpirationTime
                        // (`liveness_expires_at_seconds`, year-2106-safe i64 from
                        // HAS_TTL), falling back to the row `local_deletion_time`
                        // re-read UNSIGNED on oa/da when the GC clock is post-2038.
                        row_expires_at: row_header.liveness_expires_at_seconds.or_else(|| {
                            row_header.local_deletion_time.map(|s| {
                                if self.has_uint_deletion_time() {
                                    (s as u32) as i64
                                } else {
                                    s as i64
                                }
                            })
                        }),
                        // Issue #2038 (round 3): row-liveness TTL seconds, paired
                        // with `row_expires_at` above so a `USE_ROW_TTL` collection
                        // element's per-cell-metadata expiry can be resolved
                        // EXACTLY like the scalar `USE_ROW_TTL` cell path's
                        // `(row_header.ttl, row_expiry)` pairing (~line 726 below).
                        row_ttl_seconds: row_header.ttl,
                    });
                    self.parse_complex_column(
                        data,
                        offset,
                        column,
                        complex_type,
                        has_complex_deletion,
                        reader,
                        element_filter,
                    )
                };
                match parse_result {
                    Ok((value, new_offset, col_meta)) => {
                        tracing::debug!(
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
                            // Issue #1741 (Finding 2): the collection's effective max
                            // write ts is the newest of its element-level timestamps and
                            // the row liveness ts inherited by USE_ROW_TIMESTAMP elements.
                            // Folding `max_element_writetime` (not just the row ts) means a
                            // collection element NEWER than a partition/range tombstone
                            // keeps the row visible even when the row ts predates the
                            // tombstone. `max_element_writetime` folds ALL live elements
                            // (including shadow-dropped ones), so a wholly-shadowed
                            // collection still contributes its element ts here.
                            let coll_eff_ts = col_meta
                                .max_element_writetime
                                .max(row_header.timestamp.unwrap_or(i64::MIN));
                            // Issue #1741 (per-element filtering): the shadowed/expired
                            // elements have ALREADY been skipped from `value` inside
                            // `parse_complex_column_inner`. The collection now reads as
                            // ABSENT (null) iff the read-side filter EMPTIED it (every
                            // element was shadowed/expired: `shadow_filtered_element_count
                            // > 0` AND the container is empty). A collection empty for any
                            // OTHER reason (genuinely empty / all element-tombstones, where
                            // `shadow_filtered_element_count == 0`) keeps its prior
                            // behavior, so tombstone-free reads and physical consumers are
                            // byte-unchanged. `cover == None` never filters an element, so
                            // a non-shadowing read never treats a collection as absent.
                            let collection_absent = col_meta.shadow_filtered_element_count > 0
                                && Self::complex_value_is_empty(&value);
                            // Fold the collection into the ROW aggregate REGARDLESS of
                            // absence (so a wholly-shadowed collection is still recognised
                            // as shadowed by `row_hidden`, and Finding 2's newest-element ts
                            // keeps a surviving row visible). Its expiring elements
                            // contribute their expiry; `has_live_forever_element` is already
                            // computed POST-filter, so a wholly-shadowed collection never
                            // sets live-forever.
                            if coll_eff_ts != i64::MIN {
                                agg_max_cell_ts = Some(
                                    agg_max_cell_ts.map_or(coll_eff_ts, |m| m.max(coll_eff_ts)),
                                );
                            }
                            if let Some(e) = col_meta.max_element_expires_at {
                                agg_max_expires_at =
                                    Some(agg_max_expires_at.map_or(e, |m| m.max(e)));
                            }
                            if col_meta.has_live_forever_element {
                                agg_has_live_forever = true;
                            }
                            if !collection_absent {
                                if let Some(ref mut meta_map) = cell_meta {
                                    let row_ts = row_header.timestamp.unwrap_or(0);
                                    // Issue #2038: surface the collection's expiry so
                                    // `TTL(non_frozen_collection/UDT)` is not always
                                    // `null` (the complex-cell analogue of the scalar
                                    // #1743 fix at ~line 736 below). Authoritative,
                                    // no-heuristics: `visible_uniform_expiration` is
                                    // `Some` ONLY when every VISIBLE element shares the
                                    // identical explicit expiry (the `ExpiryHomogeneity`
                                    // tracker in `complex_column.rs` — roborev Medium
                                    // finding); a mixed/heterogeneous collection, or one
                                    // with no expiring element, stays `None` rather than
                                    // over-approximating with a single element's TTL.
                                    let expiration = col_meta.visible_uniform_expiration.clone();
                                    meta_map.insert(
                                        column.name.clone(),
                                        CellWriteMetadata {
                                            write_timestamp_micros: row_ts,
                                            expiration,
                                        },
                                    );
                                }
                                // DS4 (Issue #700): Store ComplexColumnMeta for delta-scan.
                                if let Some(ref mut ccm_map) = complex_col_meta {
                                    ccm_map.insert(column.name.clone(), col_meta);
                                }
                                // Issue #1334/#1642: interned name handle (Arc::clone),
                                // pushed positionally in schema column order.
                                cells.push((Arc::clone(&ctp.name), value));
                            }
                        }
                        offset = new_offset;
                    }
                    Err(e) => {
                        // Issue #3721: PROPAGATE (rule: `column_decode_error`); the
                        // two arms above share it, so compaction sees it too.
                        let n = &column.name;
                        let ty = dispatch_type(&column.data_type, Some(complex_type));
                        return Err(column_decode_failure(n, &ty, offset, e));
                    }
                }
            } else {
                // Issue #1741: peek the cell flags (offset points at the flags byte)
                // to detect USE_ROW_TTL (0x10), which makes a cell with no explicit
                // expiry inherit the ROW's expiry rather than being live-forever.
                let cell_flags = data.get(offset).copied().unwrap_or(0);
                // Issue #3721: there is NO end-of-cells sentinel, so a flags byte
                // that is not a cell's is a DECODE FAILURE, not a loop exit — see
                // `column_decode_error`, "The cell-flags byte: NOT a terminator"
                // (authority, and the rejected mask-to-`0x1F` alternative).
                match self.parse_cell_value_schema_order(
                    data,
                    offset,
                    column,
                    header_type,
                    // J1 (issue #1635): the precomputed per-column dispatch tag,
                    // resolved once per block — no per-cell `to_lowercase`.
                    Some(&ctp.kind),
                    reader,
                ) {
                    Ok((value, cell_own_ts, cell_exp, new_offset)) => {
                        tracing::debug!(
                            "V5CompressedLegacy:   ✓ Column {} '{}' ({}) = {:?}, consumed {} bytes",
                            col_idx,
                            column.name,
                            column.data_type,
                            value,
                            new_offset - offset
                        );
                        // `emit` is false for a DROPPED column (issue #1080 Part 2): we still
                        // advanced `offset` to consume its bytes, but emit no cell/metadata.
                        if emit {
                            // Issue #1741 read-side shadow aggregate (scalars only, no
                            // allocation) + the #3094 DELETED-CELL-reads-NULL drop. Runs
                            // BEFORE the metadata block, which moves `cell_exp`. `tomb` is
                            // the BROAD shape (no tombstone of any kind is live data); the
                            // #3094 drop is narrow — CELL tombstones only.
                            let eff_ts = cell_own_ts.or(row_header.timestamp);
                            let tomb = matches!(value, Value::Tombstone(_));
                            let mut dropped = PartitionShadow::cell_tombstone_dropped(
                                cell_ctx.is_some(),
                                PartitionShadow::is_cell_tombstone(&value),
                            );
                            if tomb {
                                // #3094: PRESENCE only — a tombstone is neither liveness
                                // nor a timestamp for the row's shadow maximum.
                                agg_has_deleted_cell = true;
                            } else {
                                // A USE_ROW_TTL cell inherits the ROW's expiry. For a
                                // TTL-bearing INSERT that expiry is the pk-liveness
                                // localExpirationTime (`liveness_expires_at_seconds`,
                                // from HAS_TTL); `local_deletion_time` is set only by
                                // HAS_DELETION (row tombstone), so it is the fallback.
                                let use_row_ttl = (cell_flags & 0x10) != 0;
                                let eff_exp: Option<i64> = match cell_exp.as_ref() {
                                    Some(e) => Some(e.expires_at_seconds),
                                    None if use_row_ttl => {
                                        // Liveness expiry is already the year-2106-safe
                                        // i64 (unsigned-reinterpreted at decode, #1741 F1).
                                        // The `local_deletion_time` fallback is stored as
                                        // the `(u32) as i32` on-disk representation, so on
                                        // oa/da a post-2038 GC clock must be re-read UNSIGNED
                                        // here too rather than sign-extended negative.
                                        row_header.liveness_expires_at_seconds.or_else(|| {
                                            row_header.local_deletion_time.map(|s| {
                                                if self.has_uint_deletion_time() {
                                                    (s as u32) as i64
                                                } else {
                                                    s as i64
                                                }
                                            })
                                        })
                                    }
                                    None => None,
                                };
                                // Issue #1741 (Finding 1): per-cell shadow/TTL filter — drop
                                // this cell from the EMITTED map when its effective write ts
                                // is shadowed by the covering deletion (`eff_ts <= cover`) or
                                // it is TTL-expired at `now`. `cell_ctx` is `None` for
                                // physical reads → never drops → byte-unchanged.
                                if let Some((cover, now)) = cell_ctx {
                                    dropped = PartitionShadow::cell_shadowed_or_expired(
                                        cover, now, eff_ts, eff_exp,
                                    );
                                }
                                // Fold this cell into the ROW aggregate driving `row_hidden`.
                                // A dropped (shadowed/expired) cell STILL contributes its ts
                                // + expiry, so a row reduced to nothing reads as shadowed/
                                // expired rather than as an empty/truncated parse — but it is
                                // NEVER counted live-forever (it is not live).
                                agg_max_cell_ts =
                                    PartitionShadow::fold_max(agg_max_cell_ts, eff_ts);
                                agg_max_expires_at =
                                    PartitionShadow::fold_max(agg_max_expires_at, eff_exp);
                                // A non-expiring cell that SURVIVED is live-forever.
                                if eff_exp.is_none() && !dropped {
                                    agg_has_live_forever = true;
                                }
                            }
                            if dropped {
                                offset = new_offset;
                                continue;
                            }
                            // Only compute and store per-cell metadata when the caller requested it.
                            // On the normal read hot-path (want_cell_metadata == false), cell_meta is
                            // None and this entire block is skipped — zero allocations per cell.
                            if let Some(ref mut meta_map) = cell_meta {
                                // Resolve effective write timestamp:
                                // use cell's own timestamp when present, else row-level liveness timestamp.
                                let effective_ts = cell_own_ts
                                    .unwrap_or_else(|| row_header.timestamp.unwrap_or(0));
                                // Resolve expiration: explicit per-cell TTL wins;
                                // else a USE_ROW_TTL (0x10) cell inherits the ROW's
                                // expiry (issue #1743). For a statement-level `USING
                                // TTL` INSERT that expiry is the pk-liveness
                                // localExpirationTime (`liveness_expires_at_seconds`,
                                // from row `HAS_TTL`) — NOT `local_deletion_time`, the
                                // GC-grace clock set only by a row tombstone
                                // (`HAS_DELETION`), `None` for a plain TTL INSERT (so
                                // the old pairing produced `None` → `TTL(col)` null).
                                // Mirrors the #1741 shadow-path expiry resolution,
                                // incl. the oa/da unsigned LDT fallback reinterpret.
                                let use_row_ttl = (cell_flags & 0x10) != 0;
                                let row_level_exp = if use_row_ttl {
                                    let row_expiry =
                                        row_header.liveness_expires_at_seconds.or_else(|| {
                                            row_header.local_deletion_time.map(|s| {
                                                if self.has_uint_deletion_time() {
                                                    (s as u32) as i64
                                                } else {
                                                    s as i64
                                                }
                                            })
                                        });
                                    match (row_header.ttl, row_expiry) {
                                        (Some(ttl_s), Some(exp_s)) => Some(CellExpiration {
                                            ttl_seconds: ttl_s,
                                            expires_at_seconds: exp_s,
                                        }),
                                        _ => None,
                                    }
                                } else {
                                    None
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
                            // Issue #1334/#1642: interned name handle (Arc::clone),
                            // pushed positionally in schema column order.
                            cells.push((Arc::clone(&ctp.name), value));
                        }
                        offset = new_offset;
                    }
                    Err(e) => {
                        // Issue #3721: PROPAGATE (see `column_decode_error`); the
                        // `break` this replaces described only its mechanism, and a
                        // clean loop exit IS a truncated row.
                        let ty = dispatch_type(&column.data_type, header_type);
                        return Err(column_decode_failure(&column.name, &ty, offset, e));
                    }
                }
            }
        }

        // Issue #1741: stash the read-side shadowing aggregate onto the header. #3094:
        // a decoded tombstone cell rides as PRESENCE (`has_deleted_data_cell`) — it
        // defeats the `i64::MIN` fail-safe without ever raising the row max.
        row_header.max_data_cell_timestamp = agg_max_cell_ts;
        row_header.max_data_cell_expires_at = agg_max_expires_at;
        row_header.has_live_forever_data_cell = agg_has_live_forever;
        row_header.has_deleted_data_cell = agg_has_deleted_cell;

        tracing::debug!(
            "V5CompressedLegacy: Parsed {}/{} on-disk columns (missing columns are NULL)",
            cells.len(),
            columns_in_order.len()
        );
        tracing::debug!(
            "V5CompressedLegacy: Cell column names (positional order): {:?}",
            cells.iter().map(|(name, _)| name).collect::<Vec<_>>()
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
