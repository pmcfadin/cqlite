//! Static-row emission: static row write path, static row body, static column bitmap and static cells.
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, flag constants, and crate imports
//! re-exported from `data_writer/mod.rs`. No emitted bytes change.

use super::*;

impl DataWriter {
    /// Write a static row for the current partition
    ///
    /// Static rows contain STATIC column values at partition level.
    /// They use extended flags and have NO clustering prefix.
    ///
    /// # Arguments
    /// * `mutation` - Mutation containing static column values
    /// * `schema` - Table schema for column metadata
    ///
    /// # Binary Format
    /// ```text
    /// [row_flags: u8]        ← 0x80 | other_flags (always HAS_EXTENDED_FLAGS)
    /// [extended_flags: u8]   ← 0x01 (EXTENDED_IS_STATIC)
    /// [row_size: VInt]       ← Size of body after this
    /// [prev_size: VInt]      ← 0 or previous row size
    /// [timestamp: VInt]      ← If HAS_TIMESTAMP (delta)
    /// [ttl: VInt]            ← If HAS_TTL (delta)
    /// [deletion: 2 VInts]    ← If HAS_DELETION
    /// [column_bitmap]        ← If NOT HAS_ALL_COLUMNS
    /// [cell_data...]         ← Static column cells only
    /// ```
    pub fn write_static_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        // Legacy/test entry point: derive per-op metadata from the single
        // mutation (each op inherits the mutation's timestamp + effective LDT).
        let static_ops: Vec<StaticMergedOp> = mutation
            .operations
            .iter()
            .map(|op| StaticMergedOp {
                // #921 finding 2: a `Delete` cell tombstone keeps its own surfaced
                // LDT; every other op falls back to the mutation's effective LDT.
                cell_local_deletion_time: op_cell_local_deletion_time(op, mutation),
                op: op.clone(),
                timestamp_micros: mutation.timestamp_micros,
            })
            .collect();
        self.write_static_row_with_prev_size(
            &static_ops,
            mutation.timestamp_micros,
            mutation.ttl_seconds,
            schema,
            0,
        )?;
        Ok(())
    }

    // (write_static_row_with_prev_size returns (bytes, cells); see Issue #851.)

    /// Write a static row from the merged static operations of a partition.
    ///
    /// Issue #764: each `StaticMergedOp` carries its own originating timestamp
    /// and local deletion time, so a surviving static-column delete from an
    /// older mutation keeps its own LDT instead of inheriting a single
    /// synthetic mutation-level value (which corrupted the unsigned-VInt delta
    /// when stats were seeded from that older delete's explicit lower LDT).
    ///
    /// Returns `(bytes_written, cells_written)` (Issue #851, review). The cell
    /// count is sourced from the physical static-cell write path so Statistics'
    /// column count matches Data.db (0 for a static row tombstone).
    pub(super) fn write_static_row_with_prev_size(
        &mut self,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        ttl_seconds: Option<u32>,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<(usize, u64)> {
        let start_len = self.buffer.len();

        // Build row header flags - always includes HAS_EXTENDED_FLAGS for static rows
        let mut flags = ROW_HAS_EXTENDED_FLAGS;

        // Check if this is a row tombstone (only reachable via the public
        // single-mutation entry point; `collect_static_operations` never emits
        // a DeleteRow into the merged set).
        let is_row_tombstone = static_ops.iter().any(|mop| {
            matches!(
                mop.op,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow
            )
        });

        if is_row_tombstone {
            flags |= ROW_HAS_DELETION;
        }

        // Issue #1196: a static row carries NO row-level primary-key liveness.
        // Cassandra's static block is the pseudo-row keyed on the empty
        // clustering; PK/row liveness lives on regular (clustering) rows, never
        // on the static row. Every committed Cassandra 5.0.2 golden confirms
        // this — no `static_block` ever has a `liveness_info`, so the static-row
        // flags byte never sets ROW_HAS_TIMESTAMP (e.g. a static-only UPDATE
        // emits `0xa0`, not `0xa4`). The writetime instead rides on each static
        // CELL (see `write_static_cells`, which writes explicit per-cell
        // timestamps below). We therefore never set ROW_HAS_TIMESTAMP here and
        // never write a row-level timestamp delta in the body.

        // TTL: a static row has no row-level liveness timestamp, so a row-level
        // TTL/expiring marker would be meaningless without it (Cassandra never
        // sets ROW_HAS_TTL on the static block). Per-cell TTL rides on the static
        // cell via `write_cell_with_ttl` (which is self-describing: explicit
        // timestamp + LDT + TTL deltas, no USE_ROW_* borrowing). We therefore
        // never set ROW_HAS_TTL here; `ttl_seconds` is threaded through to
        // `build_static_row_body` only so the (now-unreached) row-TTL body branch
        // stays consistent.

        // Check if all static columns are present
        if !is_row_tombstone {
            let all_writes = static_ops.iter().all(|mop| {
                matches!(
                    mop.op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = static_ops.iter().any(|mop| match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { value, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    value,
                    ..
                } => {
                    matches!(value, Value::Null)
                }
                _ => false,
            });
            // Count static columns only for static row
            let static_column_count = schema.columns.iter().filter(|c| c.is_static).count();

            if all_writes && !has_nulls && static_ops.len() == static_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Write row flags
        self.buffer.push(flags);

        // Write extended flags - always EXTENDED_IS_STATIC for static rows
        self.buffer.push(EXTENDED_IS_STATIC);

        // NO clustering prefix for static rows (key difference from write_row)

        // Build row body
        let (row_body, cells_written) =
            self.build_static_row_body(static_ops, liveness_ts, ttl_seconds, schema, flags)?;

        let prev_size_vint_len = unsigned_len(prev_size);

        // Write row_size (VInt) — includes prev_unfiltered_size VInt + rest of body
        let row_body_size = prev_size_vint_len as u64 + row_body.len() as u64;
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_unfiltered_size (VInt, inside the row body)
        encode_unsigned(prev_size, &mut self.buffer);

        // Write rest of row body
        self.buffer.extend_from_slice(&row_body);

        Ok((self.buffer.len() - start_len, cells_written))
    }

    /// Build static row body (everything after row_size VInt)
    ///
    /// Similar to build_row_body but only processes static columns.
    ///
    /// Returns the body bytes and the number of static cells (columns)
    /// physically written (Issue #851, review). A static row tombstone writes no
    /// cells (count 0); otherwise the count is sourced from `write_static_cells`.
    pub(super) fn build_static_row_body(
        &self,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        ttl_seconds: Option<u32>,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<(Vec<u8>, u64)> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        //
        // Fix #644 (S6): Cassandra writes UNSIGNED VInt for all temporal deltas.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let timestamp_delta = (liveness_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        //
        // Fix #644 (S6): Both TTL and LDT deltas are UNSIGNED VInt.
        // SerializationHeader.java:177: out.writeUnsignedVInt32(ttl - stats.minTTL)
        // SerializationHeader.java:172: out.writeUnsignedVInt32(ldt - stats.minLocalDeletionTime)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, &mut body);

                let local_deletion_time = self.expiring_local_deletion_time(ttl)?;
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
            // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
            // Fix #644 (S6): both are UNSIGNED VInt.
            //
            // The DeleteRow op carries the deletion timestamp + explicit LDT
            // (Issue #764). Reachable only via the single-mutation entry point.
            let delete_op = static_ops.iter().find(|mop| {
                matches!(
                    mop.op,
                    crate::storage::write_engine::mutation::CellOperation::DeleteRow
                )
            });
            let (deletion_ts, local_deletion_time) = delete_op
                .map(|mop| (mop.timestamp_micros, mop.cell_local_deletion_time))
                .unwrap_or((liveness_ts, (liveness_ts / 1_000_000) as i32));
            let ts_delta = (deletion_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, &mut body);

            // Issue #873: reject a row-tombstone LDT that is genuinely below the
            // baseline, in normal (non-negative i32) time space — a silent
            // `wrapping_sub` here would zero-extend the underflow into a huge u32,
            // emit a multi-byte VInt, and corrupt the row body / row-size. A
            // far-future LDT (negative as i32, value in [2^31, 2^32)) is a
            // legitimate value, not corruption, so the wrapping arithmetic is
            // intended there (matches the complex-deletion guard).
            if local_deletion_time >= 0
                && self.stats.min_local_deletion_time >= 0
                && local_deletion_time < self.stats.min_local_deletion_time
            {
                return Err(Error::InvalidInput(format!(
                    "Row tombstone: local deletion time {} is less than min_local_deletion_time {}",
                    local_deletion_time, self.stats.min_local_deletion_time
                )));
            }
            let ldt_delta =
                local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, &mut body);

            // Issue #717: the columns subset is NOT optional for tombstone rows.
            // Cassandra's UnfilteredSerializer always reads it after the deletion
            // times whenever HAS_ALL_COLUMNS is unset; omitting it makes the
            // reader consume the next row's bytes as a subset bitmask
            // ("Invalid Columns subset bytes; too many bits set").
            if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
                let static_columns = self.static_columns(schema);
                let empty_present: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                self.write_column_subset(&mut body, &static_columns, &empty_present)?;
            }

            // No cells written for row tombstones
            return Ok((body, 0));
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS)
        // For static rows, bitmap only covers static columns
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_static_column_bitmap(&mut body, static_ops, schema)?;
        }

        // Write cell data for static columns only
        let cells_written = self.write_static_cells(&mut body, static_ops, liveness_ts, schema)?;

        Ok((body, cells_written))
    }

    /// Write column bitmap for static columns only.
    ///
    /// Same Cassandra `Columns.Serializer.serializeSubset()` format as
    /// `write_column_bitmap()` but scoped to static columns.
    pub(super) fn write_static_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        static_ops: &[StaticMergedOp],
        schema: &TableSchema,
    ) -> Result<()> {
        // Collect names of columns that are present (non-NULL writes + deletes)
        let present_columns: std::collections::HashSet<&str> = static_ops
            .iter()
            .filter_map(|mop| match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ..
                } if !matches!(value, Value::Null) => Some(column.as_str()),
                crate::storage::write_engine::mutation::CellOperation::Delete {
                    column, ..
                } => Some(column.as_str()),
                _ => None,
            })
            .collect();

        let static_columns = self.static_columns(schema);
        self.write_column_subset(buf, &static_columns, &present_columns)
    }

    /// Write cells for static columns only.
    ///
    /// Issue #764: deletes use their ORIGINATING op's timestamp and local
    /// deletion time (carried in `StaticMergedOp`), not a single synthetic
    /// mutation-level value.
    ///
    /// Issue #851 (review): returns the number of static cells (columns)
    /// physically serialized — sourced from this loop, the only place that
    /// decides whether a static cell is emitted (null writes skipped; deletes
    /// and non-null writes written) — so Statistics cannot drift from Data.db.
    pub(super) fn write_static_cells(
        &self,
        buf: &mut Vec<u8>,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        schema: &TableSchema,
    ) -> Result<u64> {
        // Get set of static column names for validation
        let static_column_names: std::collections::HashSet<_> = schema
            .columns
            .iter()
            .filter(|c| c.is_static)
            .map(|c| &c.name)
            .collect();

        let mut cells_written: u64 = 0;
        for mop in self.sorted_static_ops(static_ops, schema) {
            match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        cells_written += 1;
                        // Issue #1196: a static row carries NO row-level liveness
                        // (HAS_TIMESTAMP is never set on the static block — see
                        // write_static_row_with_prev_size). There is therefore no
                        // row timestamp to borrow: every static cell must carry its
                        // OWN explicit timestamp delta (cell flags WITHOUT
                        // CELL_USE_ROW_TIMESTAMP), matching Cassandra 5.0.2 (a
                        // static-only UPDATE writes the static cell with flags 0x00
                        // + an explicit timestamp delta, not 0x08/USE_ROW_TIMESTAMP).
                        let _ = liveness_ts; // static cells never use row timestamp
                        self.write_cell_explicit_ts(buf, column, value, mop.timestamp_micros)?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ttl_seconds,
                } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        cells_written += 1;
                        self.write_cell_with_ttl(
                            buf,
                            column,
                            value,
                            mop.timestamp_micros,
                            *ttl_seconds,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::Delete {
                    column, ..
                } => {
                    // Only process if it's a static column
                    if static_column_names.contains(column) {
                        cells_written += 1;
                        // Issue #764: honor the originating op's explicit LDT.
                        self.write_tombstone_cell(
                            buf,
                            column,
                            mop.timestamp_micros,
                            mop.cell_local_deletion_time,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => {
                    // Row deletion handled at row level with HAS_DELETION flag
                }
                // Per-element complex ops (epic #899) are never collected into the
                // static-op set (collect_static_operations skips them); STATIC
                // complex columns are out of scope for the Phase B capability.
                crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                    ..
                } => {}
            }
        }

        Ok(cells_written)
    }

    /// Sort merged static ops into Cassandra static-column serialization order
    /// (simple columns before complex, then by name).
    pub(super) fn sorted_static_ops<'a, 'b>(
        &self,
        ops: &'b [StaticMergedOp],
        schema: &'a TableSchema,
    ) -> Vec<&'b StaticMergedOp> {
        let columns = self.static_columns(schema);
        let column_order: std::collections::HashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| (column.name.as_str(), idx))
            .collect();

        let mut sorted: Vec<&'b StaticMergedOp> = ops.iter().collect();
        sorted.sort_by_key(|mop| match &mop.op {
            crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
            | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                column, ..
            }
            | crate::storage::write_engine::mutation::CellOperation::Delete { column, .. }
            | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                column,
                ..
            }
            | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                column,
                ..
            } => column_order
                .get(column.as_str())
                .copied()
                .unwrap_or(usize::MAX - 1),
            crate::storage::write_engine::mutation::CellOperation::DeleteRow => usize::MAX,
        });
        sorted
    }
}
