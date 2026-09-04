use super::buffer_extent::HeaderTolerance;
use super::*;

use range_marker_error::unparseable_marker_in_buffered_block as unparseable;

// Issue #3928: the partition-HEADER arm, split out when it gained a tolerance
// DECISION (campsite rule, epic #1116). Declared here rather than in the parent
// `mod.rs` because it is used by this walk alone — the two sliding drivers reach
// the same decision from their own `at_final_chunk`.
pub(super) mod partition_header_arm;
use partition_header_arm::HeaderStep;

impl V5CompressedLegacyParser {
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
        extent: BufferExtent,
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        row_body_window: Option<(usize, usize)>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, ScanRow)) -> Result<std::ops::ControlFlow<()>>,
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

        // Issue #1046: build the header→schema column resolution ONCE per block.
        let resolution = RowColumnResolution::build(schema, reader);

        tracing::debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );
        tracing::debug!(
            "V5CompressedLegacy: Schema has {} columns",
            schema.columns.len()
        );
        for (i, col) in schema.columns.iter().enumerate() {
            tracing::debug!("  Column {}: {} ({})", i, col.name, col.data_type);
        }
        tracing::debug!(
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
        // Issue #1741 (F2): read-time TTL clock — captured ONCE per parser (== once per
        // read/scan operation) in `V5CompressedLegacyParser::new`. The windowed scan
        // driver reuses this parser across every block; sampling `self.now_secs` here
        // (not the wall clock per block) keeps a boundary-crossing scan consistent.
        let now_secs = self.now_secs;
        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        // Parse ALL partitions in block (Issue #2 fix: previously only parsed one partition)
        let mut partition_index = 0;
        let mut skipped_partitions = 0;
        // Issue #3928 (finding C2): the ONE header-tolerance state for this
        // walk. It starts ATTRIBUTABLE — including on a call that supplied a
        // `row_body_window`, because a window is not yet an uncertainty; it
        // becomes one only when its endpoint is REACHED, which is where
        // `bounded_out()` is called. Before that, partition 0's own header is as
        // attributable as on an unbounded walk.
        let tolerance = HeaderTolerance::for_extent(extent);
        // Issue #3928 (finding B1): LABELLED so the row-body bound can terminate
        // the WHOLE walk, not just the row loop — see the bound check below.
        'partitions: while offset < data.len() {
            // Cooperative cancellation (issue #2264): an uncompressed, index-less
            // SSTable is returned to the scan as ONE contiguous block, so this loop
            // is the 400k+-partition hot loop that the compaction streaming read
            // (and thus a Flight `do_get`) spends its whole time in. Poll the
            // reader's cancel token at a bounded interval so a disconnected client
            // abandons the walk within milliseconds instead of running to
            // completion under the coarse ~1–2 min backstop. Every 256 partitions
            // keeps the relaxed-atomic load negligible against the per-partition
            // parse cost.
            if partition_index & 0xFF == 0 {
                reader.scan_cancel.check()?;
            }
            tracing::debug!(
                "V5CompressedLegacy: === PARTITION {} at offset {} (block size: {}) ===",
                partition_index,
                offset,
                data.len()
            );

            // Issue #3928: the partition-HEADER arm — bounds, the issue-#164
            // structural validation, and the real parse — lives in ONE place
            // (`partition_header_arm.rs`), because on a PROVEN-COMPLETE buffer it
            // must REFUSE rather than skip a byte and resynchronise: the resync
            // both drops the partition and can invent one out of misaligned
            // bytes. `?` propagates that refusal; `Resync`/`EndOfBlock` are only
            // reachable on a `BufferExtent::Window`.
            match self.block_partition_header(data, offset, tolerance, partition_index)? {
                HeaderStep::Parsed(partition_key, new_offset, partition_deletion) => {
                    let header_size = new_offset - offset;
                    offset = new_offset;

                    // Issue #1741: per-partition read-side shadowing, active ONLY for
                    // user-facing query reads (`self.read_shadowing`). Captures the
                    // partition-level deletion and tracks the open range tombstone as
                    // rows are walked, so partition/range-tombstone-shadowed and
                    // TTL-expired rows are hidden (matching Cassandra SELECT). `None`
                    // for physical consumers (verify / get_all_entries) which must see
                    // every on-disk row. Un-gated (not behind write-support).
                    let mut shadow = self.read_shadowing.then(|| {
                        PartitionShadow::open(
                            now_secs,
                            partition_deletion,
                            clustering_reversed_flags(schema),
                        )
                    });

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
                                // Issue #1741 (Finding 1): a range tombstone that
                                // OPENS before `body_start` can cover rows inside the
                                // requested clustering slice. Fast-forwarding straight
                                // to `body_start` would skip those markers, so the open
                                // range would never be fed into `PartitionShadow` and a
                                // slice read could return rows a full scan correctly
                                // hides. When shadowing is active, replay the markers
                                // from the partition body start up to `body_start` into
                                // the shadow FSM first. If priming cannot faithfully
                                // reconstruct the state (unrepresentable marker or an
                                // undecodable row), fall back to a full-partition decode
                                // from the body start rather than skip the markers: the
                                // post-scan clustering backstop still trims the rows
                                // before the slice, so correctness holds and only the
                                // fast path is lost. When shadowing is off (physical
                                // read) there is nothing to prime — keep the byte-for-
                                // byte fast-forward (no-RT fast path unaffected).
                                //
                                // Issue #1741 (Finding 2): priming decodes the pre-window
                                // prefix, regressing the row-index fast-forward from
                                // O(slice) to O(prefix+slice) on a wide partition. Pay it
                                // ONLY when a range tombstone can actually reach the slice.
                                // Partition-deletion shadowing needs NO prefix decode (it is
                                // captured from the header into `shadow`). Range tombstones:
                                // when the SSTable's authoritative EncodingStats prove there
                                // are no deletions (hence no range tombstones), keep the
                                // O(slice) `offset = body_start` fast-forward and skip
                                // priming. Priming itself is now marker-ONLY — it skips row
                                // bodies via framing (no cell decode), feeding only the RT
                                // markers into the FSM — so even when it runs it never
                                // re-decodes the prefix cells.
                                let primed = match shadow.as_mut() {
                                    Some(_) if !self.sstable_may_have_range_tombstones() => true,
                                    Some(sh) => self.prime_shadow_before_window(
                                        data, offset, body_start, schema, sh,
                                    ),
                                    None => true,
                                };
                                if primed {
                                    offset = body_start;
                                }
                            }
                            Some(body_end)
                        }
                        _ => None,
                    };

                    tracing::debug!(
                        "V5CompressedLegacy: Partition {} - Parsed partition key: {} bytes (header consumed {} bytes, now at offset {})",
                        partition_index,
                        partition_key.0.len(),
                        header_size,
                        offset
                    );
                    tracing::debug!(
                        "V5CompressedLegacy: Partition {} - Row data starts at offset {}, remaining: {} bytes",
                        partition_index,
                        offset,
                        data.len() - offset
                    );
                    tracing::debug!(
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
                    // Issue #1642 (K3): positional `RowCells`, matching the decoder emit.
                    let mut static_cells: RowCells = Vec::new();
                    let mut row_count = 0;
                    // Issue #3095: Cassandra's `partition.hasNext()` — clustering
                    // rows only (the static row is delivered out of band by
                    // `partition.staticRow()`), plus proof that this call really
                    // saw the partition's END. A partition body that is only
                    // PARTIALLY present in `data` must never be mistaken for an
                    // empty one, so the static-only row below is emitted solely
                    // when the walk reached `END_OF_PARTITION` or the next
                    // partition header.
                    let mut emitted_clustering_row = false;
                    let mut partition_complete = false;
                    loop {
                        // Issue #954: stop at the clustering-slice end bound. The
                        // row-index block extent (`body_end`) is the authoritative
                        // upper byte bound of the rows that may fall in the requested
                        // clustering range; rows past it are outside the slice, so we
                        // stop decoding (the post-scan backstop already trims any
                        // block-granularity over-read within the window).
                        if let Some(body_end) = row_body_end {
                            if offset >= body_end {
                                // Issue #3928 (finding B1): TERMINATE THE WHOLE
                                // WALK, not just this row loop.
                                //
                                // The bound is the end of what the caller ASKED
                                // to read (the row-index block extent the
                                // authoritative BTI/promoted index resolved for
                                // the requested clustering range). Reaching it
                                // used to break only the inner loop, so the outer
                                // partition loop then walked the unrequested
                                // remainder byte-by-byte as candidate headers.
                                // That both read partitions nobody asked for and
                                // could FABRICATE one from misaligned row payload
                                // — AC2's own subject — and on a wide partition it
                                // could rescan the same remainder per call.
                                //
                                // Measured on the PRISTINE BIG fixture with a
                                // window ending at offset 62: the walk emitted 99
                                // FOREIGN partition keys, i.e. the whole table,
                                // for a request scoped to ~32 bytes. Pinned by
                                // `a_bounded_walk_stops_at_its_bound_and_reads_no_further_partition`.
                                //
                                // Nothing after this point in the arm is owed to
                                // the caller: the #3095 static-only emission
                                // below is already gated on
                                // `row_body_end.is_none()`, and a bounded read's
                                // rows are trimmed by the post-scan clustering
                                // backstop either way.
                                break 'partitions;
                            }
                        }

                        // Issue #229 FIX: Check for END_OF_PARTITION marker BEFORE attempting row parse
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 102, 730-732):
                        // When END_OF_PARTITION (0x01) is set in the flags byte, nothing follows.
                        // The partition is complete and we should move to the next partition.
                        if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                            tracing::debug!(
                                "V5CompressedLegacy: Partition {} complete via END_OF_PARTITION marker at offset {} ({} rows parsed)",
                                partition_index, offset, row_count
                            );
                            offset += 1; // Skip the END_OF_PARTITION marker byte
                            partition_complete = true;
                            break; // Move to next partition
                        }

                        // Issue #229 FIX: Check for range tombstone marker
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 103, 735-738):
                        // When IS_MARKER (0x02) is set, this is a range tombstone boundary, not a row.
                        if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                            // Issue #1741: when read-side shadowing is active, decode
                            // the marker (bounds + deletion time) and feed the
                            // range-tombstone FSM so covered rows are shadowed;
                            // otherwise (physical read) only advance past it.
                            if let Some(sh) = shadow.as_mut() {
                                match self.parse_range_tombstone_marker_full(data, offset, schema) {
                                    Ok((
                                        bound_values,
                                        bound_kind,
                                        del_primary,
                                        del_secondary,
                                        next_offset,
                                    )) => {
                                        if let Err(e) = sh.feed_range_marker(
                                            bound_values,
                                            bound_kind,
                                            del_primary,
                                            del_secondary,
                                        ) {
                                            // Issue #3721: FRAMED marker (body
                                            // continues at `next_offset`) with an
                                            // unrepresentable bound kind; `break`
                                            // dropped every later row and said `Ok`.
                                            // #3928 (I2) a fortiori: a `return`
                                            // reaches no header arm — see that arm,
                                            // "Reconciling #3721 and #3928".
                                            return Err(range_marker_error::range_marker_refused(
                                                e,
                                                &partition_index,
                                                offset,
                                                next_offset,
                                            ));
                                        }
                                        offset = next_offset;
                                        continue; // Continue to next row/marker
                                    }
                                    // Issue #3721 (job 78): corruption, never a
                                    // terminator (`range_marker_error` docs). The
                                    // cause used to be dropped into `debug!`.
                                    //
                                    // Issue #3782 x #3721 (job 98): DELIBERATELY UNCONDITIONAL,
                                    // and the alternative was MEASURED to be worse.
                                    //
                                    // roborev job 98 is right that an index-NARROWED window can
                                    // legitimately end inside a valid marker, and that this
                                    // refusal is `Error::Corruption`, which
                                    // `indexed_walk_falls_back` does not recognise (it matches
                                    // `is_column_decode` only) — so such a read fails instead of
                                    // retracting. That is a real, conservative false refusal.
                                    //
                                    // But gating this on `extent.is_complete()` REINTRODUCED
                                    // #3721's defect on the SELECT path: the scan passes `Window`
                                    // even with the whole partition buffered, so all three
                                    // `d9_select_marker_parse_failure` cases went back to
                                    // returning `Ok` with the marker and every later row silently
                                    // dropped. A false refusal on a narrowed read is recoverable;
                                    // a silent short answer from `SELECT` is the defect itself.
                                    //
                                    // The correct fix is a RETRACTION SIGNAL the indexed walk can
                                    // recognise — not tolerance, which would merely make the
                                    // narrowed read silently short instead. That needs a TYPED
                                    // discriminator (matching the message text would violate the
                                    // no-heuristics rule, issue #28), so it is left as a named
                                    // residual rather than invented here.
                                    //
                                    // Issue #3928 (I2) a fortiori: a `return` ends the walk at
                                    // every extent, so the header arm is never re-entered with
                                    // the cursor ON the marker. That reconciliation, and the
                                    // SECOND residual it leaves, are recorded in
                                    // `partition_header_arm.rs`, "Reconciling #3721 and #3928".
                                    Err(cause) => {
                                        return Err(unparseable(cause, &partition_index, offset))
                                    }
                                }
                            }
                            match self.skip_range_tombstone_marker(data, offset, schema) {
                                Ok(next_offset) => {
                                    offset = next_offset;
                                    continue;
                                }
                                // Same decision, and the same two residuals, on the
                                // PHYSICAL path (no shadowing); #3928's I2 invariant
                                // holds here for the same reason (a `return` ends the
                                // walk at every extent).
                                Err(cause) => {
                                    return Err(unparseable(cause, &partition_index, offset))
                                }
                            }
                        }

                        match self.parse_row_data_with_offset(
                            data,
                            offset,
                            Some(schema),
                            reader,
                            false,
                            &resolution,
                            shadow.as_ref(),
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

                                tracing::debug!(
                                    "V5CompressedLegacy: Partition {} Row {} - Parsed {} cells, now at offset {} (is_static={})",
                                    partition_index,
                                    row_count,
                                    cells.len(),
                                    offset,
                                    is_static
                                );

                                if let Some(ref header) = row_header_opt {
                                    tracing::debug!(
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
                                    tracing::debug!(
                                        "V5CompressedLegacy: Partition {} - Storing {} static cells for merging into clustering rows",
                                        partition_index,
                                        cells.len()
                                    );
                                    // Issue #1741 (Finding 1): a static row can itself be
                                    // shadowed by the partition tombstone (static write ts
                                    // <= markedForDeleteAt) or expired by its own TTL. If
                                    // so its cells are STALE and must NOT be merged into a
                                    // surviving clustering row, or a SELECT would resurface
                                    // the deleted/expired static value. Static rows carry no
                                    // clustering key, so an open range tombstone never covers
                                    // them (empty clustering). No-op when shadowing is off.
                                    let static_hidden = shadow.as_ref().is_some_and(|sh| {
                                        row_header_opt
                                            .as_ref()
                                            .is_some_and(|h| sh.row_hidden(h, &[]))
                                    });
                                    static_cells = if static_hidden { Vec::new() } else { cells };
                                    // Do NOT push to results — static rows are not result rows
                                    // Continue to next row/marker in partition
                                } else {
                                    // Issue #1741: hide rows shadowed by a partition or
                                    // range tombstone, or expired by TTL, matching a
                                    // Cassandra SELECT. Active only for query reads
                                    // (shadow is `Some`). A row reduced to only its
                                    // primary key by per-cell filtering is hidden here
                                    // too: the dropped cells still fold into the row
                                    // aggregate, so `row_hidden` sees it shadowed/expired.
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

                                    // Issue #505/#932: row-tombstone display rule lives
                                    // in the shared `build_display_row` helper. Issue
                                    // #480/#1642: static cells are merged in
                                    // (positional, clustering-row-wins).
                                    //
                                    // Issue #3095: on a user-facing SELECT read the
                                    // tombstone decision is taken over the row's OWN
                                    // cells FIRST, so a static value cannot revive a
                                    // row-tombstoned row (see
                                    // `build_display_row_read_path`). Physical consumers
                                    // keep the historical inject-then-decide order, so
                                    // their byte-pinned output is unchanged.
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

                                    // Issue #954: count each clustering row actually
                                    // decoded out of Data.db so a slice query can be
                                    // proven to decode O(matched rows + index block),
                                    // not the whole partition. Counted at the row
                                    // grain (here), distinct from the per-partition
                                    // `partitions_decoded`. Gated `not(tombstones)`
                                    // because the mutator is compiled only there.
                                    #[cfg(not(feature = "tombstones"))]
                                    crate::storage::sstable::work_counters::add_rows_decoded(1);

                                    if !hidden {
                                        // Issue #3095 (roborev + rust-reviewer
                                        // BLOCKER): only a VISIBLE row counts as one
                                        // of Cassandra's `partition.hasNext()` rows —
                                        // a `ScanRow::Marker` (pure row tombstone) is
                                        // suppressed downstream by every user-facing
                                        // consumer, so counting it would hide the
                                        // static row of a partition whose clustering
                                        // rows are all deleted. See `row_is_visible`.
                                        emitted_clustering_row |= row_is_visible(&row_value);
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
                                    partition_complete = true;
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
                                    tracing::error!(
                                        "V5CompressedLegacy: Partition {} - Failed to parse first row at offset {}: {}",
                                        partition_index, offset, e
                                    );
                                }
                                // Issue #3782: on a PROVEN-complete buffer no further
                                // bytes can finish this row, so the failure is
                                // truncation/corruption — DATA LOSS — and is reported;
                                // swallowing it made a SELECT over a fixture with ONE
                                // corrupted clustering byte return 23 of 100 rows.
                                // Otherwise the tolerant break STAYS: a chunk-covering
                                // window can legitimately cut a row at its tail. See
                                // `BufferExtent`.
                                // Issue #3721 composes INSIDE that gate: on a complete
                                // buffer the failure is authoritative, but it is still
                                // two different facts — a per-column decode failure
                                // (propagate, naming the column) versus the ordinary
                                // end-of-partition-body signal (break). Only
                                // `column_decode_error` can tell them apart, so returning
                                // `Err(e)` unconditionally here would report a normal
                                // partition end as corruption.
                                // #3782 FIRST and unconditionally: on a proven-complete
                                // buffer ANY failure is data loss, whatever its kind.
                                if extent.is_complete() {
                                    return Err(e);
                                }
                                // #3721 for the incomplete case: a per-column decode
                                // failure must STILL reach the caller, because the caller
                                // owns the tolerance decision (the BTI point read remembers
                                // it and retries with more bytes; the driver uses
                                // `at_final_chunk`). Gating this on `is_complete()` put that
                                // decision back INSIDE the parse — contradicting #3782's own
                                // rule — and silently reintroduced job 80's phantom absence,
                                // caught by `issue_3721_bti_point_read_absence`.
                                column_decode_error::end_of_partition_or_bail(
                                    e,
                                    partition_index,
                                    row_count,
                                    offset,
                                )?;
                                // Issue #3928 round 5 (I2): END THE WALK. `Ok` from
                                // `end_of_partition_or_bail` establishes only "not
                                // `Error::ColumnDecode`" and consumed nothing, so the
                                // cursor still sits where the failed row parse STARTED —
                                // the only exit here leaving it UNCONFIRMED (both boundary
                                // exits set `partition_complete`), reachable only when
                                // `!extent.is_complete()`, i.e. MID-ROW under the straddle
                                // protocol. How this composes with #3721 above, and the
                                // measurement in BOTH directions:
                                // `partition_header_arm.rs`, "Reconciling #3721 and #3928".
                                break 'partitions; // End of valid data in partition
                            }
                        }
                    }

                    // Issue #3095: Cassandra's static-content-on-an-empty-partition
                    // rule. `SelectStatement.processPartition()` (cassandra-5.0.8,
                    // L1099-1120): with NO clustering rows and a non-empty
                    // out-of-band `partition.staticRow()`, the query returns EXACTLY
                    // ONE row — clustering + REGULAR columns null
                    // (`default: result.add((ByteBuffer) null)`), STATIC columns
                    // populated — and that branch `return`s, making it mutually
                    // exclusive with the per-row loop.
                    //
                    // Gated on `read_shadowing` (user-facing SELECT reads only), so a
                    // PHYSICAL consumer still sees exactly the on-disk unfiltereds and
                    // sstabledump/compaction parity is unchanged; on
                    // `partition_complete`, so a partially-present partition body is
                    // never mistaken for an empty one; and on a clustering-slice
                    // window being absent, since a slice read decodes only part of the
                    // partition (a static-bearing schema never takes the row-index
                    // fast-forward today, so this is a belt-and-braces invariant).
                    //
                    // RESIDUAL, stated rather than left implicit (issue #3095 review):
                    // `partition_complete` is established PER BLOCK here — it is set
                    // only by this block's `END_OF_PARTITION` byte or by the next
                    // partition's header appearing in THIS block. So a static-only
                    // partition whose `END_OF_PARTITION` byte lands in the NEXT
                    // decompressed block yields 0 rows where Cassandra returns 1. The
                    // direction is FAIL-CLOSED (a row withheld, never a phantom row or
                    // a wrong value), and the shape is narrow: a static-only partition
                    // is a partition header + one static row, so its body straddling a
                    // block boundary requires the boundary to fall inside those few
                    // bytes. Closing it needs an `at_final_block`-style signal threaded
                    // from every caller (`data_access::big_promoted`,
                    // `data_access::bti_point`, the Summary-guided walk), i.e. the
                    // `at_final_chunk` contract `drive_partition_sliding` already has —
                    // which is why the SLIDING path (the full-scan route, and the one
                    // the Flight fast arm drives) does NOT have this residual.
                    //
                    // `static_cells` is already empty when the static row was shadowed
                    // by the partition tombstone or expired by its own TTL (#1741
                    // Finding 1), so a stale static row cannot resurface. The
                    // clustering/regular-restriction half of
                    // `returnStaticContentOnPartitionWithNoRows()` is enforced
                    // downstream: this row's clustering AND regular columns are null,
                    // so any restriction on one of them (the only way
                    // `queriesFullPartitions()` becomes false) rejects it under the
                    // three-valued predicate rule that keeps a row only when the
                    // predicate is definitely True.
                    if self.read_shadowing
                        && partition_complete
                        && row_body_end.is_none()
                        && !emitted_clustering_row
                        && !static_cells.is_empty()
                    {
                        // Non-empty cells holding a static (non-key) column, so the
                        // shared display rule yields `ScanRow::Row`; no header is
                        // needed (a shadowed/tombstoned static row already emptied
                        // `static_cells`).
                        let row_value = build_display_row(static_cells, None, schema);
                        match emit((table_id.clone(), partition_key.clone(), row_value))? {
                            std::ops::ControlFlow::Continue(()) => emitted += 1,
                            std::ops::ControlFlow::Break(()) => return Ok(()),
                        }
                    }

                    partition_index += 1;
                    // Issue #3721: `column_decode_error`, "Where an
                    // INDEX-POSITIONED walk must stop".
                    if row_body_window.is_some() {
                        break;
                    }
                }
                HeaderStep::EndOfBlock => break,
                HeaderStep::Resync => {
                    skipped_partitions += 1;
                    offset += 1; // Minimal forward progress to avoid an infinite loop
                    continue; // Skip this partition, try the next offset
                }
            }
        }

        if skipped_partitions > 0 {
            tracing::warn!(
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

    /// Issue #1741 (Finding 1): replay the range-tombstone (and any other)
    /// unfiltereds between the partition body start (`start`) and the
    /// clustering-slice window start (`body_start`) into `shadow`, so an open
    /// range tombstone that begins BEFORE the slice correctly shadows covered
    /// rows inside it. Rows in this prefix are NOT emitted — only markers move
    /// the FSM. `body_start` aligns to the start of an unfiltered (a BTI row-index
    /// block boundary), so markers before it are fully contained in the prefix.
    ///
    /// Returns `true` when the prefix was replayed cleanly all the way to
    /// `body_start` (the caller may then fast-forward). Returns `false` when the
    /// state cannot be faithfully reconstructed (an unrepresentable marker, an
    /// undecodable row framing, an early END_OF_PARTITION, or a non-advancing
    /// parse); the caller then declines the fast-forward and decodes the full
    /// partition, which is still correct because the post-scan backstop trims rows
    /// before the slice.
    ///
    /// Issue #1741 (Finding 2): this is a MARKER-ONLY scan. Data rows in the prefix
    /// are skipped via their framing (`skip_row_framing`) WITHOUT decoding any cell
    /// values — only range-tombstone markers move the FSM. It therefore adds no
    /// per-cell decode / allocation over the pre-window prefix; only the (cheap)
    /// row framing (flags + clustering + row_size VInts) is parsed to advance. It
    /// runs solely on the slice-read fast-forward branch when shadowing is active
    /// AND the SSTable may contain range tombstones (see the call site gate).
    fn prime_shadow_before_window(
        &self,
        data: &[u8],
        start: usize,
        body_start: usize,
        schema: &TableSchema,
        shadow: &mut PartitionShadow,
    ) -> bool {
        let mut offset = start;
        while offset < body_start {
            if offset >= data.len() {
                return false;
            }
            let flags = data[offset];
            // An END_OF_PARTITION before the window means the window is not in this
            // partition — bail to the safe full-decode path.
            if Self::is_end_of_partition(flags) {
                return false;
            }
            if Self::is_range_tombstone_marker(flags) {
                match self.parse_range_tombstone_marker_full(data, offset, schema) {
                    Ok((bound_values, bound_kind, del_primary, del_secondary, next_offset)) => {
                        if shadow
                            .feed_range_marker(bound_values, bound_kind, del_primary, del_secondary)
                            .is_err()
                        {
                            return false;
                        }
                        if next_offset <= offset {
                            return false; // non-advancing parse guard
                        }
                        offset = next_offset;
                        continue;
                    }
                    Err(_) => return false,
                }
            }
            // A data row in the prefix: advance past it via framing ONLY (no cell
            // decode), so the fast-forward stays O(prefix framing), not O(prefix
            // cells). The prefix carries no static row (the caller only requests a
            // start fast-forward when the schema has no static columns).
            match self.skip_row_framing(data, offset, schema) {
                Ok(next_offset) => {
                    if next_offset <= offset {
                        return false; // non-advancing parse guard
                    }
                    offset = next_offset;
                }
                Err(_) => return false,
            }
        }
        true
    }

    /// Issue #1741 (Finding 2): compute the offset immediately after the row at
    /// `offset` by parsing ONLY its framing — flags, clustering prefix, and the
    /// `row_size` VInt — never decoding cell values. Mirrors the authoritative
    /// offset arithmetic in `parse_row_data_with_offset`
    /// (`next = row_metadata_offset + row_size_vint_len + row_size`) so a marker-only
    /// prefix scan can skip data rows cheaply. Returns an error on truncation or an
    /// out-of-range `row_size`, which the caller treats as "cannot prime".
    pub(super) fn skip_row_framing(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<usize> {
        let (row_flags, extended_flags, flags_size) = self.parse_row_flags(data, offset)?;
        let after_flags = offset + flags_size;
        let is_static = extended_flags
            .map(|ef| (ef & EXTENDED_IS_STATIC) != 0)
            .unwrap_or(false);
        // Static rows carry no clustering prefix; regular rows do.
        let row_metadata_offset = if is_static {
            after_flags
        } else {
            let (_clustering, after_clustering) =
                self.parse_clustering_prefix(data, after_flags, schema)?;
            after_clustering
        };
        let (row_header, row_size) =
            self.parse_row_metadata(data, row_metadata_offset, row_flags, extended_flags)?;
        let body_start = row_metadata_offset + row_header.row_size_vint_len;
        if row_size > data.len().saturating_sub(body_start) as u64 {
            return Err(Error::corruption(
                "prime_shadow: row framing extends past decompressed block".to_string(),
            ));
        }
        Ok(body_start + row_size as usize)
    }

    /// Parse all partitions in a decompressed block, returning per-row timestamps.
    ///
    /// This is the compaction-specific variant of [`parse_block`].  It returns
    /// `(TableId, RowKey, ScanRow, row_timestamp_micros)` so that the k-way merger
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
    ) -> Result<Vec<(TableId, RowKey, ScanRow, i64)>> {
        // Thin wrapper that collects the streaming emit variant into a Vec, so
        // every existing caller/test is byte-for-byte unchanged (issue #827).
        let mut results: Vec<(TableId, RowKey, ScanRow, i64)> = Vec::new();
        self.parse_block_with_timestamps_emit(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Streaming variant of [`parse_block_with_timestamps`]: invokes `emit` for
    /// each parsed `(TableId, RowKey, ScanRow, row_timestamp_micros)` entry rather
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
        F: FnMut((TableId, RowKey, ScanRow, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{}",
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
                //
                // Compaction NEVER shadows: this parser is built without
                // `read_shadowing`, so the merger sees the raw tombstones/expired
                // cells it needs to reconcile across generations (#1741).
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
            tracing::warn!(
                "V5CompressedLegacy: skipped {} malformed partitions",
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
    /// Read-side shadowing (issue #1741) is driven by `self.read_shadowing`: when the
    /// parser was built for a user-facing query read, rows shadowed by a partition/
    /// range tombstone or expired by TTL are dropped before they reach `emit`. The
    /// compaction read path builds the parser WITHOUT shadowing (`read_shadowing ==
    /// false`) — it needs the raw tombstones/expired cells preserved to reconcile
    /// across generations. Un-gated: read correctness does not depend on `write-support`.
    pub fn parse_one_partition_with_timestamps<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        at_final_chunk: bool,
        emit: &mut F,
    ) -> Result<ParseStep>
    where
        F: FnMut((TableId, RowKey, ScanRow, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        // Issue #1640 (K1): thin adapter over the single sliding-window partition
        // driver. `TimestampPolicy` supplies the three per-consumer hooks
        // (read-side shadow open, range-tombstone FSM feed, static-merge +
        // display-row build); the driver owns the framing skeleton + ParseStep +
        // pending buffering this function used to hand-roll.
        let mut policy = TimestampPolicy::new(self);
        self.drive_partition_sliding(data, schema, reader, at_final_chunk, &mut policy, |row| {
            emit(row)
        })
    }
}
