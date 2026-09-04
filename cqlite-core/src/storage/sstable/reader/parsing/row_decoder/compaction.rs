use super::*;
// The discriminated data-row policy outcome (issue #3809). Imported EXPLICITLY
// rather than through `row_decoder/mod.rs`'s glob because that file sits at the
// campsite-rule size limit and this costs no line there.
use super::partition_driver::DataRowOutcome;

// Issue #3809 (campsite #1116): the per-element row BUILDER, split out of this
// file so the clustering-identity invariant and its tests have room. A child
// module rather than a sibling so this split costs no line in `row_decoder/mod.rs`
// (that file is itself at its size limit); `build_compaction_row_data` is
// `pub(super)` purely so its former host still calls it.
#[path = "compaction_row_build.rs"]
mod compaction_row_build;

impl V5CompressedLegacyParser {
    /// Parse all partitions in a decompressed block into per-element
    /// [`CompactionRow`]s (epic #899, compaction-only). Thin Vec wrapper over
    /// [`Self::parse_block_for_compaction_emit`].
    pub fn parse_block_for_compaction(
        &self,
        data: &[u8],
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Vec<crate::storage::sstable::reader::compaction_row::CompactionRow>> {
        let mut results = Vec::new();
        self.parse_block_for_compaction_emit(data, extent, schema, reader, |row| {
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
        extent: BufferExtent,
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
                // Issue #3782: the caller's DECLARED extent, not a hard-coded
                // `true`. `at_final_chunk` is what makes a row decode error
                // fatal here, so baking it in meant this entry point silently
                // ASSUMED a complete buffer — the mirror of the defaulted flag
                // #3782 removed, and a window handed in would have produced
                // false refusals.
                extent.is_complete(),
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
            tracing::warn!(
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
        extent: BufferExtent,
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
        let mut partition_index: usize = 0;

        let broke = std::cell::Cell::new(false);

        while offset < data.len() {
            // Cooperative cancellation (issue #2264): poll the reader's cancel
            // token at a bounded interval so a compressed, index-less SSTable's
            // compaction stream (a Flight `do_get`) abandons promptly on client
            // disconnect rather than draining the whole window under the backstop.
            if partition_index & 0xFF == 0 {
                reader.scan_cancel.check()?;
            }
            partition_index += 1;
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
                // Issue #3782: the caller's DECLARED extent (see the sibling
                // emit entry point).
                extent.is_complete(),
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
            tracing::warn!(
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
    /// deletion instead of the collapsed `(RowKey, ScanRow, ts)` tuple.
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
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        // Issue #1640 (K1): thin adapter over the single sliding-window partition
        // driver. `CompactionPolicy` supplies the three per-consumer hooks
        // (partition-tombstone emit, range-marker bound pairing, per-element
        // CompactionRow build); the driver owns the framing skeleton + ParseStep
        // + pending buffering this function used to hand-roll.
        let mut policy = CompactionPolicy::new(self);
        self.drive_partition_sliding(data, schema, reader, at_final_chunk, &mut policy, |row| {
            emit(row)
        })
    }

    /// STRUCTURE-ONLY sibling of
    /// [`parse_one_partition_for_compaction`](Self::parse_one_partition_for_compaction)
    /// (issue #3058): drives the SAME sliding-window partition framing and returns
    /// the SAME [`ParseStep`], but builds no rows at all.
    ///
    /// This is what the coverage check
    /// (`SSTableReader::partition_slice_fully_consumed`) needs: its verdict is
    /// `Emitted(consumed) && consumed == slice.len()`, i.e. purely the byte
    /// framing, and every row it used to build was handed to a no-op closure. On
    /// the token-scoped single-source read path that row-building was the last
    /// remaining per-row `CellWriteMetadata` allocation (spec R3), and on the
    /// merge arm it was pure waste — the structural verdict is byte-identical
    /// because consumption never depended on the discarded rows.
    ///
    /// It also runs no row-BUILD invariant, so a partition this reports fully
    /// consumed may still be REFUSED by the real per-element read — see the
    /// DIVERGENCE note on [`CompactionPolicy::structure_only`] (issue #3809).
    pub fn parse_one_partition_structure_only(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        at_final_chunk: bool,
    ) -> Result<ParseStep> {
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }
        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (structure check) requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;
        let mut policy = CompactionPolicy::structure_only(self);
        self.drive_partition_sliding(data, schema, reader, at_final_chunk, &mut policy, |_row| {
            Ok(std::ops::ControlFlow::Continue(()))
        })
    }
}

/// Issue #1640 (K1): [`SlidingPartitionPolicy`] for the per-element compaction
/// read path (epic #899). Compaction is a PHYSICAL consumer: no read-side
/// shadowing (it reconciles tombstones itself across generations), the static
/// row is emitted as its own `CompactionRow` (issue #1074, not merged), and
/// range-tombstone markers are paired into `RangeMarker` rows (issue #933).
pub(super) struct CompactionPolicy<'a> {
    parser: &'a V5CompressedLegacyParser,
    /// STRUCTURE-ONLY mode (issue #3058): decode each row far enough to advance
    /// the offset, but build NO `CompactionRow`, NO per-cell `CellWriteMetadata`
    /// map and NO per-column complex-element map.
    ///
    /// Used by the coverage check `partition_slice_fully_consumed`, whose rows are
    /// fed to a no-op emit and thrown away — building them was pure waste, and on
    /// the token-scoped single-source read path (`summary_scan::query_rows`) it was
    /// the last thing allocating a per-row metadata `HashMap` on a path whose whole
    /// point is not to (spec R3). Consumption/`ParseStep` are byte-driven and
    /// therefore identical in both modes, so the structural verdict is unchanged.
    ///
    /// DIVERGENCE, stated because it is not nothing (issue #3809): building no row
    /// also runs none of the row-BUILD invariants, so the #3809 row-deletion
    /// clustering-identity refusal
    /// (`CompactionRowData::require_tombstone_clustering_identity`) cannot fire
    /// here. `partition_slice_fully_consumed` can therefore report a partition
    /// structurally fine that the real per-element compaction read of the same
    /// bytes REFUSES. That is deliberate — the coverage check's question is byte
    /// framing, and answering it must not depend on row semantics — but the two
    /// verdicts are not interchangeable.
    structure_only: bool,
    partition_key: RowKey,
    /// Issue #933: in-flight range-tombstone start bound
    /// `(bound, markedForDeleteAt µs, localDeletionTime s)`, re-derived from the
    /// partition start on every sliding-window re-parse.
    pending_range_start: Option<(
        crate::storage::sstable::reader::compaction_row::CompactionBound,
        i64,
        i32,
    )>,
}

impl<'a> CompactionPolicy<'a> {
    pub(super) fn new(parser: &'a V5CompressedLegacyParser) -> Self {
        Self {
            parser,
            structure_only: false,
            partition_key: RowKey::new(Vec::new()),
            pending_range_start: None,
        }
    }

    /// A policy that verifies STRUCTURE only — see [`Self::structure_only`].
    pub(super) fn structure_only(parser: &'a V5CompressedLegacyParser) -> Self {
        Self {
            structure_only: true,
            ..Self::new(parser)
        }
    }

    /// The partition key decoded on `on_partition_open` (issue #2299: the
    /// resumable streaming driver in `compaction_stream` reads it back to carry
    /// across a chunk-straddling refill).
    pub(super) fn partition_key(&self) -> &RowKey {
        &self.partition_key
    }

    /// The in-flight range-tombstone start bound (issue #2299: carried across
    /// resumed streaming calls — see `CompactionPartitionState::pending_range_start`).
    pub(super) fn pending_range_start(
        &self,
    ) -> &Option<(
        crate::storage::sstable::reader::compaction_row::CompactionBound,
        i64,
        i32,
    )> {
        &self.pending_range_start
    }

    /// Restore the partition key on a resumed streaming call (issue #2299).
    pub(super) fn set_partition_key(&mut self, key: RowKey) {
        self.partition_key = key;
    }

    /// Restore the in-flight range-tombstone start bound on a resumed streaming
    /// call (issue #2299).
    pub(super) fn set_pending_range_start(
        &mut self,
        pending: Option<(
            crate::storage::sstable::reader::compaction_row::CompactionBound,
            i64,
            i32,
        )>,
    ) {
        self.pending_range_start = pending;
    }

    /// Build a `CompactionBound` from decoded clustering-prefix values. An empty
    /// prefix is an OPEN bound (Bottom for a start, Top for an end); a non-empty
    /// prefix pairs each value with its schema clustering-column name
    /// (positionally; bounds may be PREFIX-length).
    fn make_bound(
        &self,
        schema: &TableSchema,
        values: Vec<Value>,
        inclusive: bool,
        is_start: bool,
    ) -> crate::storage::sstable::reader::compaction_row::CompactionBound {
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
    }
}

impl SlidingPartitionPolicy for CompactionPolicy<'_> {
    type Row = crate::storage::sstable::reader::compaction_row::CompactionRow;

    fn on_partition_open(
        &mut self,
        partition_key: RowKey,
        partition_deletion: Option<(i64, i32)>,
        _schema: &TableSchema,
        pending: &mut Vec<Self::Row>,
    ) {
        use crate::storage::sstable::reader::compaction_row::{CompactionRow, CompactionRowData};
        self.partition_key = partition_key;
        // Issue #1072: surface a partition-level tombstone as a synthetic
        // partition-delete `CompactionRow` (pushed once per partition, even for a
        // tombstone-only partition) so the merge can apply the partition floor.
        if let Some((mfda, ldt)) = partition_deletion {
            pending.push(CompactionRow {
                key: self.partition_key.clone(),
                row_timestamp: mfda,
                row_data: CompactionRowData::PartitionDelete {
                    deletion_time: mfda,
                    local_deletion_time: ldt,
                },
            });
        }
    }

    fn on_range_marker(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        pending: &mut Vec<Self::Row>,
    ) -> MarkerOutcome {
        use crate::storage::sstable::reader::compaction_row::{
            CompactionBound, CompactionRow, CompactionRowData,
        };
        // Issue #933: parse the range-tombstone bound marker and pair it into a
        // complete `RangeMarker` so the merge can shadow covered cells AND
        // re-emit the surviving marker.
        let (bound_values, bound_kind, (mfda_p, ldt_p), secondary, next_offset) = match self
            .parser
            .parse_range_tombstone_marker_with_ldt(data, offset, schema)
        {
            Ok(v) => v,
            // Issue #3721 (roborev job 16): a marker PARSE failure is NOT
            // automatically a chunk boundary, and answering `Stop` here made it
            // one — on the FINAL chunk the driver converted `Stop` into a
            // SUCCESSFUL partition completion, so a corrupt or truncated marker
            // was dropped and this policy's rows were still WRITTEN. That
            // resurrects every row the tombstone shadowed, durably, on disk: the
            // same harm as the unrecognised bound kind below (#3808), one arm up.
            //
            // The cause is PRESERVED in the outcome (never discarded and
            // re-synthesised — the parser's own message is what makes the
            // condition actionable) and the FINAL-vs-non-final decision is left
            // to the driver, the only holder of the chunking state: a non-final
            // chunk may simply have cut the marker body in half, which is the one
            // explanation a refill can fix. Nothing here inspects bytes to guess
            // whether more data exists (issue #28).
            Err(cause) => {
                let partition = format!("key 0x{}", hex::encode(self.partition_key.as_bytes()));
                return MarkerOutcome::Unparseable(range_marker_error::range_marker_unparseable(
                    cause, &partition, offset,
                ));
            }
        };

        // ClusteringPrefix.Kind ordinals:
        //   0 EXCL_END, 1 INCL_START, 2 EXCL_END_INCL_START_BOUNDARY,
        //   5 INCL_END_EXCL_START_BOUNDARY, 6 INCL_END, 7 EXCL_START.
        match bound_kind {
            1 | 7 => {
                // Simple start bound: buffer until the matching end.
                let start = self.make_bound(schema, bound_values, bound_kind == 1, true);
                self.pending_range_start = Some((start, mfda_p, ldt_p));
            }
            0 | 6 => {
                // Simple end bound: close the buffered start.
                let end = self.make_bound(schema, bound_values, bound_kind == 6, false);
                let start = self
                    .pending_range_start
                    .take()
                    .map(|(s, _, _)| s)
                    .unwrap_or(CompactionBound::Bottom);
                pending.push(CompactionRow {
                    key: self.partition_key.clone(),
                    row_timestamp: mfda_p,
                    row_data: CompactionRowData::RangeMarker {
                        start,
                        end,
                        deletion_time: mfda_p,
                        local_deletion_time: ldt_p,
                    },
                });
            }
            2 | 5 => {
                // Boundary marker: primary closes the previous range, the
                // secondary opens the next. kind 2 = EXCL_END + INCL_START,
                // kind 5 = INCL_END + EXCL_START.
                let end_inclusive = bound_kind == 5;
                if let Some((start, _, _)) = self.pending_range_start.take() {
                    let end = self.make_bound(schema, bound_values.clone(), end_inclusive, false);
                    pending.push(CompactionRow {
                        key: self.partition_key.clone(),
                        row_timestamp: mfda_p,
                        row_data: CompactionRowData::RangeMarker {
                            start,
                            end,
                            deletion_time: mfda_p,
                            local_deletion_time: ldt_p,
                        },
                    });
                }
                let (new_mfda, new_ldt) = secondary.unwrap_or((mfda_p, ldt_p));
                let new_start = self.make_bound(schema, bound_values, bound_kind == 2, true);
                self.pending_range_start = Some((new_start, new_mfda, new_ldt));
            }
            unknown => {
                // Issue #3808: an unrecognised `ClusteringPrefix.Kind` ordinal is
                // NOT skippable here, and skipping it was the worst instance of the
                // #3721 swallow: this policy's output is WRITTEN, so omitting an
                // unrepresentable deletion marker resurrects the rows it shadowed
                // durably, on disk. `row_framing` returns `bound_kind` unvalidated,
                // so an arbitrary byte reaches this match; giving it a permissive
                // default meaning is the byte-pattern inference issue #28 forbids.
                // Both sibling readers of the same byte already refuse
                // (`partition_shadow::feed_range_marker`, delta-scan
                // `block_emit`), so this arm was the lone fail-open.
                //
                // The marker PARSED — `next_offset` is bound and the partition body
                // continues there — so this is `Refused`, never the `Unparseable`
                // a marker PARSE failure earns above (which has no resume point and
                // so is still refillable on a non-final chunk). Both are terminal on
                // the final chunk; neither can be silently completed.
                let partition = format!("key 0x{}", hex::encode(self.partition_key.as_bytes()));
                return MarkerOutcome::Refused(range_marker_error::range_marker_refused(
                    Error::corruption(format!(
                        "compaction: unknown range tombstone bound kind {unknown} — cannot \
                         represent faithfully (no-heuristics mandate, issue #28)"
                    )),
                    &partition,
                    offset,
                    next_offset,
                ));
            }
        }
        MarkerOutcome::Advanced(next_offset)
    }

    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> DataRowOutcome {
        use crate::storage::sstable::reader::compaction_row::CompactionRow;
        // Structure-only (issue #3058): advance over the row WITHOUT allocating a
        // per-cell metadata map, a complex-element map or a `CompactionRow` — the
        // caller discards rows and only reads the byte consumption.
        if self.structure_only {
            return match self.parser.parse_row_data_with_offset_impl(
                data,
                offset,
                Some(schema),
                reader,
                false,
                None,
                resolution,
                None,
            ) {
                Ok((_cells, _meta, _hdr, next_offset, _is_static, _complex)) => {
                    DataRowOutcome::Decoded(next_offset)
                }
                // Issue #3782: preserve the decode error — the POLICY never decides
                // tolerance, the driver does. The structural coverage check that drives
                // this arm passes `at_final_chunk = false`, so the driver still answers
                // `NeedMore` (⇒ "not fully consumed") there; the error only becomes
                // terminal where no more bytes can arrive.
                //
                // Issue #3721, on why folding it into `Ok(None)` was wrong here
                // specifically: this path exists to measure byte CONSUMPTION, and a row
                // whose column decode failed has an UNKNOWN consumption (the failing
                // parser returns no offset). Answering `Ok(None)` reported "end of
                // partition" for a row that is really there — the same silent truncation,
                // one level up.
                //
                // Structure-only builds NO row, so it raises no #3809 refusal —
                // see the DIVERGENCE note on `structure_only`.
                Err(e) => DataRowOutcome::DecodeFailed(e),
            };
        }
        // Compaction mode: capture per-column complex elements and request
        // per-cell metadata so simple cells carry per-cell timestamps/TTLs.
        let mut complex_capture: CompactionComplexColumns = HashMap::new();
        match self.parser.parse_row_data_with_offset_impl(
            data,
            offset,
            Some(schema),
            reader,
            true,
            Some(&mut complex_capture),
            resolution,
            // Compaction is a physical consumer: no read-side shadowing.
            None,
        ) {
            Ok((cells, cell_meta, row_header_opt, next_offset, is_static, _complex_meta)) => {
                // Issue #932 (K1): the ONE row-write-timestamp coexistence rule.
                let row_ts = row_write_timestamp(&row_header_opt);

                // Issue #1074: emit BOTH static and clustering rows as their own
                // `CompactionRow` (static-ness is decided by the schema in the
                // writer, not by on-disk folding).
                //
                // Issue #3809: `is_static` is threaded in because it is what makes
                // an EMPTY tombstone clustering legitimate. A build failure is
                // reported as `Refused`, NEVER as `DecodeFailed`: the decode above
                // already returned `Ok`, so the row is fully framed and no refill
                // can change the verdict — routing it through #3782's
                // driver-decides-tolerance channel would convert it into a
                // straddle/refill request on every `BufferExtent::Window` entry
                // point and silently truncate the read.
                let row_data = match self.parser.build_compaction_row_data(
                    cells,
                    cell_meta,
                    complex_capture,
                    &row_header_opt,
                    row_ts,
                    schema,
                    is_static,
                ) {
                    Ok(row_data) => row_data,
                    Err(e) => return DataRowOutcome::Refused(e),
                };

                pending.push(CompactionRow {
                    key: self.partition_key.clone(),
                    row_timestamp: row_ts,
                    row_data,
                });
                DataRowOutcome::Decoded(next_offset)
            }
            // Issue #3782: preserve the decode error. Swallowing it here is what
            // made compaction emit MORE rows than the source while losing real
            // partitions — a loss it would then write back to disk. This is the
            // BYTES half; the row-BUILD refusal above is the semantic half (#3809).
            //
            // Issue #3721 names the arm: this is the compaction / elements-out read,
            // the caller of `parse_row_data_with_offset_impl` with
            // `compaction_complex_out = Some(..)`. Handing back `Ok(None)` for a
            // column that failed to decode let compaction WRITE OUT a row missing
            // that column and every later one — the read defect turned into durable
            // data loss.
            Err(e) => DataRowOutcome::DecodeFailed(e),
        }
    }
}
