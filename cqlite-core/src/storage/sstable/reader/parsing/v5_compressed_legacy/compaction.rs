use super::*;

impl V5CompressedLegacyParser {
    /// Build a [`CompactionRow`] from a parsed row's pieces (epic #899).
    ///
    /// `cells` is the collapsed column→value map (simple columns plus the
    /// collapsed `Value` for each complex column); `cell_meta` carries per-simple
    /// -cell write timestamps / TTLs; `complex` carries the per-element capture
    /// for the complex columns. The complex columns are split out of `cells` (the
    /// collapsed complex `Value` is dropped in favour of the per-element cells).
    ///
    /// A row tombstone produces [`CompactionRowData::Tombstone`]; an empty row
    /// (no cells, no tombstone) produces an empty `Live`.
    fn build_compaction_row_data(
        &self,
        cells: HashMap<String, Value>,
        cell_meta: Option<HashMap<String, CellWriteMetadata>>,
        complex: CompactionComplexColumns,
        row_header_opt: &Option<RowHeader>,
        row_ts: i64,
        schema: &TableSchema,
    ) -> crate::storage::sstable::reader::compaction_row::CompactionRowData {
        use crate::storage::sstable::reader::compaction_row::{
            CompactionRowData, ComplexColumn, SimpleCell,
        };

        // Issue #932: a row with `HAS_DELETION` may ALSO carry surviving cells
        // (cells written strictly after the row deletion). The row deletion is
        // captured here either as the coexisting `row_deletion` on a `Live` row
        // (when data cells survive) or as a pure `Tombstone` (when only the
        // deletion remains). The decision is made AFTER building the cell sets so
        // we can tell whether any NON-clustering data cell survived.
        let row_deletion: Option<(i64, i32)> = row_header_opt
            .as_ref()
            .filter(|h| h.is_row_tombstone())
            .map(|h| {
                (
                    h.row_tombstone_deletion_time(),
                    // localDeletionTime in SECONDS (GC-grace clock). Preserve the
                    // far-future [2^31, 2^32) encoding via wrapping `as u32 as i32`.
                    h.local_deletion_time.unwrap_or(0),
                )
            });

        // Build complex columns (sorted by name for deterministic output, mirroring
        // the collapsed-value path's column ordering).
        let mut complex_cols: Vec<ComplexColumn> = complex
            .into_iter()
            .map(
                |(column, (complex_deletion, elements, collapsed_value))| ComplexColumn {
                    column,
                    complex_deletion,
                    elements,
                    collapsed_value,
                },
            )
            .collect();
        complex_cols.sort_by(|a, b| a.column.cmp(&b.column));

        // Simple cells are every collapsed cell whose column is NOT a complex
        // column. Per-cell timestamp / ttl / local-deletion-time come from
        // `cell_meta` when present, else inherit the row timestamp.
        let complex_names: std::collections::HashSet<&str> =
            complex_cols.iter().map(|c| c.column.as_str()).collect();

        let mut simple_cells: Vec<SimpleCell> = cells
            .into_iter()
            .filter(|(name, _)| !complex_names.contains(name.as_str()))
            .map(|(column, value)| {
                let (timestamp, ttl, local_deletion_time) =
                    match cell_meta.as_ref().and_then(|m| m.get(&column)) {
                        Some(meta) => {
                            let ttl = meta.expiration.as_ref().map(|e| e.ttl_seconds as u32);
                            let ldt = meta
                                .expiration
                                .as_ref()
                                .map(|e| e.expires_at_seconds as u32 as i32);
                            (meta.write_timestamp_micros, ttl, ldt)
                        }
                        None => (row_ts, None, None),
                    };
                SimpleCell {
                    column,
                    value,
                    timestamp,
                    ttl,
                    local_deletion_time,
                }
            })
            .collect();
        simple_cells.sort_by(|a, b| a.column.cmp(&b.column));

        // Issue #932: a row deletion either COEXISTS with surviving data cells
        // (kept as `Live { row_deletion: Some(..) }`) or — when no NON-primary-key
        // cell and no complex element survives — is a pure row tombstone (kept as
        // `Tombstone`, preserving the #912 clustering-prefix capture). The earlier
        // code always took the `Tombstone` branch, DROPPING surviving cells and
        // letting older cells of other columns resurrect in a partial compaction.
        if let Some((deletion_time, local_deletion_time)) = row_deletion {
            let primary_key: std::collections::HashSet<&str> = schema
                .partition_keys
                .iter()
                .map(|k| k.name.as_str())
                .chain(schema.clustering_keys.iter().map(|c| c.name.as_str()))
                .collect();
            let has_simple_data = simple_cells
                .iter()
                .any(|c| !primary_key.contains(c.column.as_str()));
            let has_complex_data = complex_cols
                .iter()
                .any(|c| !c.elements.is_empty() || c.complex_deletion.is_some());

            if !has_simple_data && !has_complex_data {
                // Pure row tombstone: rebuild the clustering prefix in schema
                // order from the surfaced clustering pseudo-cells (#912). A
                // missing clustering column falls back to the `None` bucket.
                let mut clustering: Vec<(String, Value)> =
                    Vec::with_capacity(schema.clustering_keys.len());
                for ck in &schema.clustering_keys {
                    match simple_cells.iter().find(|c| c.column == ck.name) {
                        Some(c) => clustering.push((ck.name.clone(), c.value.clone())),
                        None => {
                            clustering.clear();
                            break;
                        }
                    }
                }
                return CompactionRowData::Tombstone {
                    deletion_time,
                    local_deletion_time,
                    clustering,
                };
            }
        }

        CompactionRowData::Live {
            simple: simple_cells,
            complex: complex_cols,
            row_deletion,
        }
    }

    /// Parse all partitions in a decompressed block into per-element
    /// [`CompactionRow`]s (epic #899, compaction-only). Thin Vec wrapper over
    /// [`Self::parse_block_for_compaction_emit`].
    pub fn parse_block_for_compaction(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<crate::storage::sstable::reader::compaction_row::CompactionRow>> {
        let mut results = Vec::new();
        self.parse_block_for_compaction_emit(data, schema, reader, |row| {
            results.push(row);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Streaming per-element compaction variant of
    /// [`Self::parse_block_with_timestamps_emit`]: emits a [`CompactionRow`]
    /// (per-element complex cells + real complex deletion) per row (epic #899).
    pub fn parse_block_for_compaction_emit<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            crate::storage::sstable::reader::compaction_row::CompactionRow,
        ) -> Result<std::ops::ControlFlow<()>>,
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

        let broke = std::cell::Cell::new(false);
        let mut tracking_emit = |row| -> Result<std::ops::ControlFlow<()>> {
            let flow = emit(row)?;
            if matches!(flow, std::ops::ControlFlow::Break(())) {
                broke.set(true);
            }
            Ok(flow)
        };

        while offset < data.len() {
            match self.parse_one_partition_for_compaction(
                &data[offset..],
                Some(schema),
                reader,
                true,
                &mut tracking_emit,
            )? {
                ParseStep::Emitted(consumed) => {
                    if consumed == 0 {
                        skipped_partitions += 1;
                        offset += 1;
                    } else {
                        offset += consumed;
                    }
                }
                ParseStep::NeedMore | ParseStep::Done => break,
            }
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

    /// Like [`Self::parse_block_for_compaction_emit`] but also reports, for every
    /// emitted [`CompactionRow`], the byte offset within `data` at which that
    /// row's partition begins.
    ///
    /// `data` is the WHOLE decompressed Data.db data section (header stripped),
    /// so the reported offset is the partition's absolute decompressed-Data.db
    /// position — i.e. the value a BTI `Partitions.db` leaf encodes as
    /// `BtiPartitionLocation::DataOffset`. The verifier uses this to resolve a
    /// `DataOffset` payload back to its raw partition key by IDENTITY
    /// (issue #1103), closing the same-count wrong-payload corruption gap.
    pub fn parse_block_for_compaction_emit_with_offset<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            usize,
            crate::storage::sstable::reader::compaction_row::CompactionRow,
        ) -> Result<std::ops::ControlFlow<()>>,
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

        let broke = std::cell::Cell::new(false);

        while offset < data.len() {
            // Capture the partition-start offset BEFORE parsing this partition so
            // every row emitted for it is tagged with the same authoritative
            // decompressed-Data.db position.
            let partition_start = offset;
            let mut tagging_emit = |row| -> Result<std::ops::ControlFlow<()>> {
                let flow = emit(partition_start, row)?;
                if matches!(flow, std::ops::ControlFlow::Break(())) {
                    broke.set(true);
                }
                Ok(flow)
            };

            match self.parse_one_partition_for_compaction(
                &data[offset..],
                Some(schema),
                reader,
                true,
                &mut tagging_emit,
            )? {
                ParseStep::Emitted(consumed) => {
                    if consumed == 0 {
                        skipped_partitions += 1;
                        offset += 1;
                    } else {
                        offset += consumed;
                    }
                }
                ParseStep::NeedMore | ParseStep::Done => break,
            }
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

    /// Per-element compaction counterpart of
    /// [`Self::parse_one_partition_with_timestamps`] (epic #899). Identical
    /// sliding-window / `ParseStep` / buffering semantics, but emits a
    /// [`CompactionRow`] carrying per-element complex cells and the real complex
    /// deletion instead of the collapsed `(RowKey, Value, ts)` tuple.
    pub fn parse_one_partition_for_compaction<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        at_final_chunk: bool,
        emit: &mut F,
    ) -> Result<ParseStep>
    where
        F: FnMut(
            crate::storage::sstable::reader::compaction_row::CompactionRow,
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        use crate::storage::sstable::reader::compaction_row::CompactionRow;

        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        const CASSANDRA_MAX_KEY_SIZE: usize = 65536;
        const FORMAT_MAX_KEY_SIZE: usize = 255;

        if data.len() < 2 {
            return Ok(if at_final_chunk {
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let key_len = data[1] as usize;
        let header_min_size = 1 + 1 + key_len + 4 + 8;

        if key_len == 0 || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE) {
            return Ok(ParseStep::Emitted(1));
        }

        if header_min_size > data.len() {
            return Ok(if at_final_chunk {
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let (partition_key, mut offset, partition_deletion) =
            match self.parse_partition_header_full(data, 0) {
                Ok(v) => v,
                Err(_) => return Ok(ParseStep::Emitted(1)),
            };

        // Issue #1074: on the COMPACTION path the static row is emitted as its own
        // partition-level `CompactionRow` (Cassandra's `Row.staticRow`), NOT folded
        // into the clustering rows. The user-facing read paths
        // (`parse_block_emit_*` / `parse_one_partition_with_timestamps`) still fold
        // statics into each row so a `SELECT` surfaces the static columns per row;
        // compaction must preserve the partition/clustering-row separation instead.
        let mut pending: Vec<CompactionRow> = Vec::new();

        // Issue #1072: a partition-level tombstone in this SSTable must shadow OLDER
        // live rows in OTHER SSTables during a cross-generation compaction merge.
        // Surface it as a synthetic partition-deletion `CompactionRow` (mirroring the
        // `RangeMarker` carrier) so the merge can apply the partition floor and
        // re-emit the surviving tombstone. Pushed exactly once per partition — even
        // when the partition has zero rows (tombstone-only case) — because this
        // header parse runs once per partition emit.
        if let Some((mfda, ldt)) = partition_deletion {
            use crate::storage::sstable::reader::compaction_row::CompactionRowData;
            pending.push(CompactionRow {
                key: partition_key.clone(),
                row_timestamp: mfda,
                row_data: CompactionRowData::PartitionDelete {
                    deletion_time: mfda,
                    local_deletion_time: ldt,
                },
            });
        }

        // In-flight range tombstone start bound (issue #933). A range tombstone is
        // a pair of adjacent bound markers (start then end), or a sequence of
        // boundary markers that close one range and open the next. We buffer the
        // open start here and emit a complete `CompactionRowData::RangeMarker` when
        // the matching end/boundary arrives. Local to this call so it is re-derived
        // from the partition start on every sliding-window re-parse (the `pending`
        // rows are only flushed on a clean `Emitted`).
        //
        // Tuple: (start bound, deletion_time µs, local_deletion_time s).
        let mut pending_range_start: Option<(
            crate::storage::sstable::reader::compaction_row::CompactionBound,
            i64,
            i32,
        )> = None;

        // Build a `CompactionBound` from decoded clustering prefix values. The
        // writer emits an OPEN bound (Bottom/Top) as an inclusive bound with zero
        // clustering values, so an empty prefix decodes to Bottom (start) / Top
        // (end). A non-empty prefix pairs each value with its schema clustering
        // column name (positionally; bounds may be PREFIX-length).
        let make_bound = |values: Vec<Value>, inclusive: bool, is_start: bool| {
            use crate::storage::sstable::reader::compaction_row::CompactionBound;
            if values.is_empty() {
                return if is_start {
                    CompactionBound::Bottom
                } else {
                    CompactionBound::Top
                };
            }
            let pairs: Vec<(String, Value)> = values
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    let name = schema
                        .clustering_keys
                        .get(i)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| format!("ck{i}"));
                    (name, v)
                })
                .collect();
            if inclusive {
                CompactionBound::Inclusive(pairs)
            } else {
                CompactionBound::Exclusive(pairs)
            }
        };

        macro_rules! flush_and_emitted {
            ($consumed:expr, $pending:expr, $emit:expr) => {{
                for row in $pending.drain(..) {
                    match $emit(row)? {
                        std::ops::ControlFlow::Continue(()) => {}
                        std::ops::ControlFlow::Break(()) => break,
                    }
                }
                Ok(ParseStep::Emitted($consumed))
            }};
        }

        loop {
            if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                offset += 1;
                return flush_and_emitted!(offset, pending, emit);
            }

            if offset >= data.len() {
                if at_final_chunk {
                    return flush_and_emitted!(offset, pending, emit);
                }
                return Ok(ParseStep::NeedMore);
            }

            if Self::is_range_tombstone_marker(data[offset]) {
                // Issue #933: parse the range-tombstone bound marker and pair it
                // into a complete `RangeMarker` so the merge can shadow covered
                // cells AND re-emit the surviving marker (previously SKIPPED, which
                // dropped the tombstone entirely).
                let (bound_values, bound_kind, (mfda_p, ldt_p), secondary, next_offset) =
                    match self.parse_range_tombstone_marker_with_ldt(data, offset, schema) {
                        Ok(v) => v,
                        Err(_) => {
                            // Truncated marker body at a chunk boundary (or corrupt
                            // at the final chunk): request more / flush, exactly as
                            // the prior skip path did.
                            if at_final_chunk {
                                return flush_and_emitted!(offset, pending, emit);
                            }
                            return Ok(ParseStep::NeedMore);
                        }
                    };
                offset = next_offset;

                use crate::storage::sstable::reader::compaction_row::{
                    CompactionBound, CompactionRowData,
                };

                // Emit a complete range tombstone given a (start, deletion) and an
                // end bound + the end's deletion time/ldt.
                let mut emit_range =
                    |start: CompactionBound, end: CompactionBound, dt: i64, ldt: i32| {
                        pending.push(CompactionRow {
                            key: partition_key.clone(),
                            row_timestamp: dt,
                            row_data: CompactionRowData::RangeMarker {
                                start,
                                end,
                                deletion_time: dt,
                                local_deletion_time: ldt,
                            },
                        });
                    };

                // ClusteringPrefix.Kind ordinals:
                //   0 EXCL_END, 1 INCL_START, 2 EXCL_END_INCL_START_BOUNDARY,
                //   5 INCL_END_EXCL_START_BOUNDARY, 6 INCL_END, 7 EXCL_START.
                match bound_kind {
                    1 | 7 => {
                        // Simple start bound: buffer until the matching end.
                        let start = make_bound(bound_values, bound_kind == 1, true);
                        pending_range_start = Some((start, mfda_p, ldt_p));
                    }
                    0 | 6 => {
                        // Simple end bound: close the buffered start.
                        let end = make_bound(bound_values, bound_kind == 6, false);
                        let start = pending_range_start
                            .take()
                            .map(|(s, _, _)| s)
                            .unwrap_or(CompactionBound::Bottom);
                        emit_range(start, end, mfda_p, ldt_p);
                    }
                    2 | 5 => {
                        // Boundary marker: primary closes the previous range, the
                        // secondary opens the next. kind 2 = EXCL_END + INCL_START,
                        // kind 5 = INCL_END + EXCL_START.
                        let end_inclusive = bound_kind == 5;
                        if let Some((start, _, _)) = pending_range_start.take() {
                            let end = make_bound(bound_values.clone(), end_inclusive, false);
                            emit_range(start, end, mfda_p, ldt_p);
                        }
                        let (new_mfda, new_ldt) = secondary.unwrap_or((mfda_p, ldt_p));
                        let new_start = make_bound(bound_values, bound_kind == 2, true);
                        pending_range_start = Some((new_start, new_mfda, new_ldt));
                    }
                    _ => {
                        // Unknown bound kind: skip it rather than mis-parse the
                        // partition (the offset already advanced past the marker).
                    }
                }
                continue;
            }

            // Compaction mode: capture per-column complex elements and request
            // per-cell metadata so simple cells carry per-cell timestamps/TTLs.
            let mut complex_capture: CompactionComplexColumns = HashMap::new();
            match self.parse_row_data_with_offset_impl(
                data,
                offset,
                Some(schema),
                reader,
                true,
                Some(&mut complex_capture),
            ) {
                Ok((
                    cells,
                    cell_meta,
                    row_header_opt,
                    next_offset,
                    // Issue #1074: the on-disk static flag no longer changes the
                    // emit path — static and clustering rows each become their own
                    // `CompactionRow` (see below) and the writer decides static
                    // placement from the schema, not from on-disk folding.
                    _is_static,
                    _complex_meta,
                )) => {
                    offset = next_offset;

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

                    // Issue #1074: emit BOTH static and clustering rows as their own
                    // `CompactionRow`. A static row carries no clustering prefix, so
                    // it decodes to the merge's `None` bucket and reconciles
                    // independently of the clustering rows — a clustering-row delete
                    // can no longer shadow the static cell, a static-only partition is
                    // no longer dropped, and the static cell keeps its OWN write
                    // timestamp (instead of inheriting a clustering row's). The merge
                    // re-emits it as a `clustering_key: None` mutation and the writer
                    // rebuilds the partition static prelude via
                    // `collect_static_operations` (static-ness decided by the schema,
                    // not by on-disk folding).
                    let row_data = self.build_compaction_row_data(
                        cells,
                        cell_meta,
                        complex_capture,
                        &row_header_opt,
                        row_ts,
                        schema,
                    );

                    pending.push(CompactionRow {
                        key: partition_key.clone(),
                        row_timestamp: row_ts,
                        row_data,
                    });

                    if offset >= data.len() {
                        if at_final_chunk {
                            return flush_and_emitted!(offset, pending, emit);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    if self.peek_is_partition_header(data, offset) {
                        return flush_and_emitted!(offset, pending, emit);
                    }
                }
                Err(_) => {
                    if at_final_chunk {
                        return flush_and_emitted!(offset, pending, emit);
                    }
                    return Ok(ParseStep::NeedMore);
                }
            }
        }
    }
}
