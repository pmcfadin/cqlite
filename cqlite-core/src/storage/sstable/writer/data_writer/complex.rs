//! Complex (non-frozen collection / UDT) column emission and merged-cell dispatch.
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, flag constants, and crate imports
//! re-exported from `data_writer/mod.rs`. No emitted bytes change.

use super::*;

impl DataWriter {
    /// Write cells for this row
    ///
    /// Cells are written in alphabetical column name order to match Cassandra's
    /// `Columns` sorting (regular columns are sorted by name).
    /// Write the surviving cells of a merged row, in regular-column order.
    ///
    /// Cells whose timestamp matches the row liveness timestamp use
    /// USE_ROW_TIMESTAMP; cells merged in from other mutations (e.g. a later
    /// single-cell DELETE) carry an explicit timestamp delta.
    /// Write the surviving cells of a merged row and return the number of cells
    /// (columns) actually serialized.
    ///
    /// Issue #851 (review): Statistics' `totalColumnsSet` must equal the cells
    /// PHYSICALLY written to Data.db, not `row.ops.len()`. This loop is the sole
    /// place that decides whether a cell is emitted (null `Write`/`WriteWithTtl`
    /// ops are skipped; deletes and non-null writes are written), so we return
    /// the count from here — the caller threads it straight into the emit tally,
    /// making the column count impossible to drift from Data.db.
    /// `now_seconds`: the caller's single captured wall-clock reading for this
    /// row write (issue #2038 Scope B) — threaded to every expiring cell
    /// derived below instead of each one reading the clock independently.
    pub(super) fn write_merged_cells(
        &self,
        buf: &mut Vec<u8>,
        row: &RowWrite<'_>,
        schema: &TableSchema,
        now_seconds: i32,
    ) -> Result<u64> {
        use crate::storage::write_engine::mutation::CellOperation;

        // Index the reconciled WHOLE-COLUMN ops by column name. `merge_row_group`
        // keeps at most one surviving op per column (last-write-wins) and only for
        // regular columns (primary-key and static ops are filtered upstream), so
        // every key here is a regular column.
        let mut whole_by_col: std::collections::HashMap<&str, &MergedOp<'_>> =
            std::collections::HashMap::new();
        for mop in &row.ops {
            match mop.op {
                CellOperation::Write { column, .. }
                | CellOperation::WriteWithTtl { column, .. }
                | CellOperation::Delete { column, .. } => {
                    whole_by_col.insert(column.as_str(), mop);
                }
                // Row deletion is a row-level flag, not a cell; per-element complex
                // ops live in `row.complex_element_ops` and are grouped below.
                CellOperation::DeleteRow
                | CellOperation::WriteComplexElement { .. }
                | CellOperation::ComplexDeletion { .. } => {}
            }
        }

        // Group the surviving PER-ELEMENT complex ops by column (one emitted
        // complex column each).
        let mut per_element_by_col = self.group_complex_element_ops(row);

        // ONE schema-ordered emission pass (issue #930): for each regular column
        // emit its whole-column op and/or its per-element complex column, walking
        // `regular_columns` order — the exact order the header bitmap /
        // `write_column_subset` use. A previous two-pass design emitted every
        // whole-column op first and every per-element column second, which
        // inverted the wire order whenever a row mixed a whole-column write for
        // one column with per-element ops for another column that sorts earlier.
        let mut cells_written: u64 = 0;
        // Issue #1674 (R3): `is_complex` from the per-writer cache — no per-row sort.
        for (col, is_complex) in self.regular_columns_with_complex(schema) {
            let name = col.name.as_str();
            if let Some(mop) = whole_by_col.get(name) {
                cells_written +=
                    self.emit_whole_column_op(buf, row, col, mop, is_complex, now_seconds)?;
            }
            if let Some((complex_deletion, elements)) = per_element_by_col.remove(name) {
                self.write_complex_column_per_element(
                    buf,
                    col,
                    complex_deletion,
                    &elements,
                    row.liveness_ts.unwrap_or(0),
                )?;
                cells_written += 1;
            }
        }

        Ok(cells_written)
    }

    /// Emit a single reconciled WHOLE-COLUMN op (`Write` / `WriteWithTtl` /
    /// `Delete`) for `col`. Returns the number of cells serialized: 0 for a
    /// skipped NULL write (represented by absence in the bitmap), else 1.
    /// `is_complex` is supplied from the per-writer column cache (issue #1674, R3)
    /// so this hot path never re-lowercases `col`'s type.
    pub(super) fn emit_whole_column_op(
        &self,
        buf: &mut Vec<u8>,
        row: &RowWrite<'_>,
        col: &Column,
        mop: &MergedOp<'_>,
        is_complex: bool,
        now_seconds: i32,
    ) -> Result<u64> {
        use crate::storage::write_engine::mutation::CellOperation;

        match mop.op {
            CellOperation::Write { column, value } => {
                // Skip NULL values - they are represented by absence in the bitmap
                if matches!(value, Value::Null) {
                    return Ok(0);
                }
                if is_complex {
                    // Issue #2038 (Scope B): thread the mutation's row-level
                    // `USING TTL` (`mop.row_ttl_seconds`) instead of dropping it
                    // (`None`), mirroring the scalar arm below. Every element cell
                    // is stamped IS_EXPIRING with localDeletionTime = now + ttl, so
                    // a row-level `USING TTL` collection/UDT write round-trips.
                    let row_ttl = mop.row_ttl_seconds;
                    self.write_complex_column(
                        buf,
                        col,
                        value,
                        mop.timestamp_micros,
                        row_ttl,
                        now_seconds,
                    )?;
                } else {
                    // roborev #1020 Finding 1: a frozen-UDT (or UDT-bearing frozen
                    // collection/tuple) simple-cell value is canonicalized against
                    // the column's declared marshal BEFORE serialization, so its
                    // field bytes follow declared order / `-1` padding and match the
                    // advertised `FrozenType(UserType(...))` header. A non-UDT column
                    // is returned unchanged (byte-identical path).
                    let canon = canonicalize_udt_value(&col.data_type, value)?;
                    let value = canon.as_ref();
                    if let Some(ttl_seconds) = mop.row_ttl_seconds {
                        if row.ttl_seconds == Some(ttl_seconds)
                            && row.liveness_ts == Some(mop.timestamp_micros)
                        {
                            self.write_cell_with_row_ttl(
                                buf,
                                column,
                                value,
                                mop.timestamp_micros,
                                ttl_seconds,
                            )?;
                        } else {
                            // Row-level `USING TTL` write (no per-cell LDT source):
                            // derive `now_seconds + ttl` (historical behavior).
                            self.write_cell_with_ttl(
                                buf,
                                column,
                                value,
                                mop.timestamp_micros,
                                ttl_seconds,
                                None,
                                now_seconds,
                            )?;
                        }
                    } else if row.liveness_ts == Some(mop.timestamp_micros) {
                        self.write_cell(buf, column, value, mop.timestamp_micros)?;
                    } else {
                        self.write_cell_explicit_ts(buf, column, value, mop.timestamp_micros)?;
                    }
                }
                Ok(1)
            }
            CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time,
            } => {
                // Skip NULL values - they are represented by absence in the bitmap
                if matches!(value, Value::Null) {
                    return Ok(0);
                }
                if is_complex {
                    self.write_complex_column(
                        buf,
                        col,
                        value,
                        mop.timestamp_micros,
                        Some(*ttl_seconds),
                        now_seconds,
                    )?;
                } else {
                    // roborev #1020 Finding 1: schema-aware frozen-UDT value.
                    let canon = canonicalize_udt_value(&col.data_type, value)?;
                    // Issue #1538: stamp the authoritative per-cell LDT verbatim
                    // when present (a surviving expiring cell preserved through
                    // compaction), else derive `now_seconds + ttl`.
                    self.write_cell_with_ttl(
                        buf,
                        column,
                        canon.as_ref(),
                        mop.timestamp_micros,
                        *ttl_seconds,
                        *local_deletion_time,
                        now_seconds,
                    )?;
                }
                Ok(1)
            }
            CellOperation::Delete { column, .. } => {
                if is_complex {
                    // Complex column deletion: write empty complex column
                    // with active deletion time (not LIVE).
                    // Issue #764: honor the originating mutation's explicit
                    // local_deletion_time, not a timestamp-derived value.
                    self.write_complex_column_deletion(
                        buf,
                        mop.timestamp_micros,
                        mop.cell_local_deletion_time,
                    )?;
                } else {
                    // Issue #764: honor explicit local_deletion_time.
                    self.write_tombstone_cell(
                        buf,
                        column,
                        mop.timestamp_micros,
                        mop.cell_local_deletion_time,
                    )?;
                }
                Ok(1)
            }
            // Only whole-column ops are indexed into `whole_by_col`; these never
            // reach here.
            CellOperation::DeleteRow
            | CellOperation::WriteComplexElement { .. }
            | CellOperation::ComplexDeletion { .. } => Ok(0),
        }
    }

    /// Group `row.complex_element_ops` by column for the single emission pass.
    /// Each entry is `(strongest complex deletion marker, surviving element
    /// writes)`, keyed by column name. The emission order is decided by the
    /// caller walking `regular_columns(schema)`; the map only deduplicates the
    /// per-column deletion marker and accumulates element writes verbatim.
    pub(super) fn group_complex_element_ops<'a>(
        &self,
        row: &RowWrite<'a>,
    ) -> std::collections::BTreeMap<&'a str, ComplexColumnGroup> {
        use crate::storage::write_engine::mutation::CellOperation;

        let mut per_column: std::collections::BTreeMap<&'a str, ComplexColumnGroup> =
            std::collections::BTreeMap::new();

        for mop in &row.complex_element_ops {
            match mop.op {
                CellOperation::WriteComplexElement {
                    column,
                    cell_path,
                    value,
                    timestamp_micros,
                    ttl_seconds,
                    local_deletion_time,
                    is_deleted,
                } => {
                    let entry = per_column.entry(column.as_str()).or_default();
                    entry.1.push(ComplexElementWrite {
                        cell_path: cell_path.clone(),
                        value: value.clone(),
                        timestamp_micros: *timestamp_micros,
                        ttl_seconds: *ttl_seconds,
                        local_deletion_time: *local_deletion_time,
                        // No-heuristics (roborev #885, Finding 2): carry the
                        // reader's authoritative IS_DELETED flag verbatim. An
                        // expiring SET member (value None, ttl Some, ldt Some) is
                        // NOT a tombstone — re-deriving from value/ldt shape would
                        // misclassify it as IS_DELETED.
                        is_deleted: *is_deleted,
                    });
                }
                CellOperation::ComplexDeletion {
                    column,
                    marked_for_delete_at,
                    local_deletion_time,
                } => {
                    let entry = per_column.entry(column.as_str()).or_default();
                    // Keep the strongest (highest markedForDeleteAt) marker.
                    let candidate = (*marked_for_delete_at, *local_deletion_time);
                    entry.0 = Some(match entry.0 {
                        Some(existing) if existing.0 >= candidate.0 => existing,
                        _ => candidate,
                    });
                }
                _ => {}
            }
        }

        per_column
    }

    /// Write a complex column (non-frozen collection stored as multiple cells).
    ///
    /// Complex columns use the following wire format:
    /// ```text
    /// [complex_deletion: marked_for_delete_at (signed VInt) + local_deletion_time (unsigned VInt)]
    /// [cell_count: unsigned VInt]
    /// For each cell:
    ///   [flags: u8]
    ///   [cell_path_length: unsigned VInt]
    ///   [cell_path_bytes]
    ///   [value_length: unsigned VInt]  (if not HAS_EMPTY_VALUE)
    ///   [value_bytes]
    /// ```
    ///
    /// Per collection type:
    /// - SET<T>: cell_path = serialized element, value = empty (HAS_EMPTY_VALUE)
    /// - MAP<K,V>: cell_path = serialized key, value = serialized value
    /// - LIST<T>: cell_path = 16-byte TimeUUID, value = serialized element
    ///
    /// `now_seconds`: the caller's single captured wall-clock reading for this
    /// write (issue #2038 Scope B) — shared by every element cell so a
    /// multi-element collection under one uniform TTL gets an IDENTICAL
    /// `localDeletionTime` across all elements (see `capture_now_seconds`).
    pub(super) fn write_complex_column(
        &self,
        buf: &mut Vec<u8>,
        column: &Column,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        // Write complex deletion time: DeletionTime.LIVE
        // Cassandra canonical order: markedForDeleteAt first, then localDeletionTime
        // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
        // Fix #644 (S6): markedForDeleteAt delta is UNSIGNED VInt.
        // DeletionTime.LIVE.markedForDeleteAt = Long.MIN_VALUE; delta wraps to large positive u64.
        let ts_delta = i64::MIN.wrapping_sub(self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, buf);
        // localDeletionTime delta = Integer.MAX_VALUE - stats.min_local_deletion_time (unsigned VInt)
        let ldt_delta = i32::MAX.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(ldt_delta as u64, buf);

        let dt = column.data_type.to_lowercase();

        if dt.starts_with("set<") || dt.starts_with("org.apache.cassandra.db.marshal.settype(") {
            self.write_set_complex_cells(buf, value, timestamp_micros, ttl_seconds, now_seconds)?;
        } else if dt.starts_with("map<")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            self.write_map_complex_cells(
                buf,
                value,
                &column.data_type,
                timestamp_micros,
                ttl_seconds,
                now_seconds,
            )?;
        } else if dt.starts_with("list<")
            || dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        {
            self.write_list_complex_cells(buf, value, timestamp_micros, ttl_seconds, now_seconds)?;
        } else if is_udt_marshal(&dt) {
            // Issue #927: decompose a whole-`Value::Udt` literal into per-field
            // cells (cell_path = 2-byte signed-short DECLARED field index, value =
            // field datum). Field index comes from the column's declared order, NOT
            // the literal's position — a sparse / reordered literal lands each field
            // at its correct index.
            self.write_udt_complex_cells(
                buf,
                column,
                value,
                timestamp_micros,
                ttl_seconds,
                now_seconds,
            )?;
        } else {
            return Err(Error::InvalidInput(format!(
                "Column '{}' has type '{}' which is not a recognized complex column type",
                column.name, column.data_type
            )));
        }

        Ok(())
    }

    /// Write the per-field cells of a whole-`Value::Udt` write, following the
    /// column header already emitted by [`write_complex_column`] (issue #927).
    ///
    /// Each non-null field becomes one complex cell whose cell_path is the 2-byte
    /// big-endian DECLARED field index (resolved by NAME from the `UserType(...)`
    /// marshal string) and whose value is the serialized field datum. Null fields
    /// are absent (no cell), matching Cassandra. Cells are emitted in ascending
    /// field-index order. Row TTL, when present, propagates to every field cell as
    /// an expiring cell.
    pub(super) fn write_udt_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        column: &Column,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let udt = match value {
            Value::Udt(udt) => udt,
            Value::Frozen(inner) => {
                if let Value::Udt(udt) = inner.as_ref() {
                    udt
                } else {
                    return Err(Error::InvalidInput(format!(
                        "Column '{}' is a UDT complex column but value is {:?}",
                        column.name, value
                    )));
                }
            }
            other => {
                return Err(Error::InvalidInput(format!(
                    "Column '{}' is a UDT complex column but value is {:?}",
                    column.name, other
                )));
            }
        };

        let declared = udt_declared_field_names(&column.data_type)?;

        // Resolve each literal field to its DECLARED index by name; reject unknown
        // field names (no-heuristics). Build elements in declared-index order.
        let mut elements: Vec<ComplexElementWrite> = Vec::new();
        for field in &udt.fields {
            let Some(field_value) = &field.value else {
                // Null field: absent cell.
                continue;
            };
            let idx = declared
                .iter()
                .position(|n| n == &field.name)
                .ok_or_else(|| {
                    Error::InvalidInput(format!(
                        "UDT column '{}': literal field '{}' is not a declared field of {}",
                        column.name, field.name, column.data_type
                    ))
                })?;
            let cell_path = (idx as u16).to_be_bytes().to_vec();
            // Issue #2038 Scope B: derive from the SHARED `now_seconds` (not a
            // fresh clock read per field) so every field of a multi-field UDT
            // written under one uniform TTL gets an identical LDT.
            let local_deletion_time =
                ttl_seconds.map(|ttl| self.expiring_local_deletion_time(now_seconds, ttl));
            elements.push(ComplexElementWrite {
                cell_path,
                value: Some(field_value.clone()),
                timestamp_micros,
                ttl_seconds,
                local_deletion_time,
                is_deleted: false,
            });
        }

        // Ascending field-index (signed-short) order.
        elements.sort_by(|a, b| compare_cell_paths(&a.cell_path, &b.cell_path, true));

        encode_unsigned(elements.len() as u64, buf);
        let mut value_scratch = Vec::new();
        for elem in &elements {
            self.write_complex_element_cell(buf, elem, timestamp_micros, &mut value_scratch)?;
        }
        Ok(())
    }

    /// Write a complex column deletion (delete all elements of a collection).
    ///
    /// Wire format: active deletion time + zero cells.
    /// Per SerializationHeader.writeDeletionTime(): timestamp first, LDT second.
    /// ```text
    /// [marked_for_delete_at: unsigned VInt]  ← mutation timestamp (delta from min)
    /// [local_deletion_time: unsigned VInt]   ← seconds since epoch (delta from min)
    /// [cell_count: unsigned VInt]            ← 0 (no cells)
    /// ```
    pub(super) fn write_complex_column_deletion(
        &self,
        buf: &mut Vec<u8>,
        timestamp_micros: i64,
        local_deletion_time: i32,
    ) -> Result<()> {
        // Active deletion: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
        // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
        // Fix #644 (S6): marked_for_delete_at delta is UNSIGNED VInt.
        let ts_delta = (timestamp_micros - self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, buf);

        // Issue #853: encode the localDeletionTime delta with the SAME i32 cast +
        // wrapping behaviour that Cassandra's DeletionTime.serialize uses (and that
        // the row-deletion / range-bound paths already use), so the encoded SIZE of
        // this complex-deletion marker equals the bytes actually written for
        // far-future localDeletionTime in [2^31, 2^32) (~year 2038-2106).
        //
        // Cassandra (c81fbae1): localDeletionTime and minLocalDeletionTime are Java
        // `int`s; the wire delta is `writeUnsignedVInt32(localDeletionTime -
        // minLocalDeletionTime)`, a 32-bit subtraction whose result is zero-extended
        // into [0, 2^32). A value in [2^31, 2^32) is a negative i32 here; widening to
        // i64 first (the previous code) both rejected it and would have produced a
        // different byte count than the i32 form, corrupting the row-size vint.
        //
        // Issue #764: still reject a genuine below-baseline ordering violation, but
        // only in normal (non-negative i32) time space; a far-future LDT (negative
        // as i32) is a legitimate value, not corruption.
        if local_deletion_time >= 0
            && self.stats.min_local_deletion_time >= 0
            && local_deletion_time < self.stats.min_local_deletion_time
        {
            return Err(Error::InvalidInput(format!(
                "Complex deletion: local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        let deletion_time_delta =
            local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(deletion_time_delta as u64, buf);

        // Zero cells
        encode_unsigned(0u64, buf);

        Ok(())
    }

    /// Write per-cell TTL fields for a complex cell.
    ///
    /// When TTL is present, writes:
    /// - flags: CELL_IS_EXPIRING (0x02), NO USE_ROW_TIMESTAMP
    /// - timestamp delta (unsigned VInt; fix #644: all temporal deltas are unsigned)
    /// - local_deletion_time delta (unsigned VInt)
    /// - TTL delta (unsigned VInt)
    ///
    /// When TTL is absent, writes:
    /// - flags: base_flags | CELL_USE_ROW_TIMESTAMP (0x08)
    ///
    /// Returns the flags byte written (for caller to check HAS_EMPTY_VALUE etc.).
    ///
    /// `now_seconds` (issue #2038 Scope B, roborev): the caller's SHARED
    /// captured wall-clock reading for this write — NOT a fresh
    /// `SystemTime::now()` read per cell. This function used to read the
    /// clock independently on every call, so a multi-element collection
    /// written under one uniform TTL could get a different
    /// `local_deletion_time` per element if the clock ticked mid-write,
    /// defeating the read-side `ExpiryHomogeneity` check (which requires an
    /// EXACT match to surface `TTL(col)`).
    pub(super) fn write_complex_cell_header(
        &self,
        buf: &mut Vec<u8>,
        base_flags: u8,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        match ttl_seconds {
            Some(ttl) => {
                // Expiring cell: IS_EXPIRING flag, explicit timestamp + LDT + TTL
                let flags = base_flags | CELL_IS_EXPIRING;
                buf.push(flags);

                // Timestamp delta (UNSIGNED VInt, NOT USE_ROW_TIMESTAMP)
                // Fix #644 (S6): SerializationHeader.java:167 uses writeUnsignedVInt.
                let timestamp_delta = (timestamp_micros - self.stats.min_timestamp) as u64;
                encode_unsigned(timestamp_delta, buf);

                // local_deletion_time = now_seconds + ttl (shared `now_seconds`).
                let local_deletion_time = now_seconds.saturating_add(ttl as i32);
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Complex cell: local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, buf);

                // TTL delta
                let ttl_delta = (ttl as i64) - (self.stats.min_ttl as i64);
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Complex cell: TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, buf);
            }
            None => {
                // Non-expiring cell: use row timestamp
                buf.push(base_flags | CELL_USE_ROW_TIMESTAMP);
            }
        }
        Ok(())
    }

    /// Write SET complex cells.
    ///
    /// SET elements: cell_path = serialized element value, cell value = empty (HAS_EMPTY_VALUE).
    /// Elements are ordered by the element type's Cassandra `SetType` comparator (#1275).
    pub(super) fn write_set_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let elements = match value {
            Value::Set(elements) => elements,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Set value for complex SET column, got {:?}",
                    value
                )))
            }
        };

        // Order by the element type's Cassandra `SetType` comparator (#1275, see
        // collection_order: SIGNED numerics, unsigned-byte otherwise) decided from
        // the element `Value`s. serialize_collection_element rejects Value::Null.
        let mut ordered: Vec<&Value> = elements.iter().collect();
        ordered.sort_by(|a, b| compare_collection_elements(a, b));
        let serialized: Vec<Vec<u8>> = ordered
            .iter()
            .map(|e| serialize_collection_element(e, "SET"))
            .collect::<Result<Vec<_>>>()?;

        encode_unsigned(serialized.len() as u64, buf); // cell count
        for path_bytes in &serialized {
            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(
                buf,
                CELL_HAS_EMPTY_VALUE,
                timestamp_micros,
                ttl_seconds,
                now_seconds,
            )?;

            // Cell path: serialized element value
            encode_unsigned(path_bytes.len() as u64, buf);
            buf.extend_from_slice(path_bytes);
            // No value bytes (HAS_EMPTY_VALUE flag set)
        }

        Ok(())
    }

    /// Write MAP complex cells.
    ///
    /// MAP entries: cell_path = serialized key, cell value = serialized value.
    /// Entries are sorted by their serialized key byte representation for Cassandra compatibility.
    ///
    /// `map_data_type` is the COLUMN's DECLARED type (e.g. `map<int, int>`) and
    /// is threaded down for ONE reason: the cell path is the only write-path
    /// position where an empty-buffer sentinel is legal (issue #3805), and
    /// legality there depends on the declared KEY type, which a bare `Value`
    /// cannot supply (roborev job 449 finding D). See
    /// [`serialize_map_cell_path_key_into`]. The cell VALUE deliberately keeps
    /// the type-blind [`serialize_value_into`], which REFUSES a sentinel: a
    /// zero-byte map VALUE is not a sentinel, it is the empty value of the
    /// value type — or, with `HAS_EMPTY_VALUE`, a null.
    pub(super) fn write_map_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        map_data_type: &str,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let entries = match value {
            Value::Map(entries) => entries,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Map value for complex MAP column, got {:?}",
                    value
                )))
            }
        };

        // Order by the KEY type's Cassandra `MapType` comparator (#1275, see
        // collection_order: SIGNED numerics so negative keys sort -1 before 0/1,
        // unsigned-byte otherwise) from the key `Value`s. Null keys rejected inline.
        let mut ordered: Vec<&(Value, Value)> = entries.iter().collect();
        ordered.sort_by(|a, b| compare_collection_elements(&a.0, &b.0));

        // Reusable per-entry scratch (issue #1672): one alloc for the whole map,
        // not a Vec-of-Vecs holding every key/value.
        encode_unsigned(ordered.len() as u64, buf); // cell count
        let mut key_scratch = Vec::new();
        let mut val_scratch = Vec::new();
        for (key, val) in ordered {
            if matches!(key, Value::Null) {
                return Err(Error::InvalidInput(
                    "MAP keys cannot be null (CQL semantics)".to_string(),
                ));
            }

            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(buf, 0, timestamp_micros, ttl_seconds, now_seconds)?;

            // Cell path: serialized key. SCHEMA-AWARE, because this is the one
            // position an empty-buffer sentinel may occupy (issue #3805) and its
            // tag must be validated against the DECLARED key type.
            key_scratch.clear();
            serialize_map_cell_path_key_into(key, map_data_type, &mut key_scratch)?;
            encode_unsigned(key_scratch.len() as u64, buf);
            buf.extend_from_slice(&key_scratch);

            // Cell value: serialized value
            val_scratch.clear();
            serialize_value_into(val, &mut val_scratch)?;
            encode_unsigned(val_scratch.len() as u64, buf);
            buf.extend_from_slice(&val_scratch);
        }

        Ok(())
    }

    /// Write LIST complex cells.
    ///
    /// LIST elements: cell_path = 16-byte TimeUUID, cell value = serialized element.
    /// Lists preserve insertion order (no sorting) — TimeUUIDs provide ordering.
    pub(super) fn write_list_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let elements = match value {
            Value::List(elements) => elements,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected List value for complex LIST column, got {:?}",
                    value
                )))
            }
        };

        // Cell count
        encode_unsigned(elements.len() as u64, buf);

        // Reusable per-element scratch buffer (issue #1672).
        let mut value_scratch = Vec::new();
        for (i, elem) in elements.iter().enumerate() {
            // Reject null elements inline (CQL semantics)
            if matches!(elem, Value::Null) {
                return Err(Error::InvalidInput(
                    "LIST elements cannot be null (CQL semantics)".to_string(),
                ));
            }

            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(buf, 0, timestamp_micros, ttl_seconds, now_seconds)?;

            // Cell path: 16-byte TimeUUID
            let timeuuid = generate_list_cell_path_timeuuid(timestamp_micros, i as u64);
            encode_unsigned(16u64, buf);
            buf.extend_from_slice(&timeuuid);

            // Cell value: serialized element
            value_scratch.clear();
            serialize_value_into(elem, &mut value_scratch)?;
            encode_unsigned(value_scratch.len() as u64, buf);
            buf.extend_from_slice(&value_scratch);
        }

        Ok(())
    }

    /// Write a complex (non-frozen collection) column from per-element cells,
    /// each carrying its OWN timestamp/ttl/local-deletion-time and its PRESERVED
    /// source cell path (epic #899, Phase B — writer capability).
    ///
    /// This is the per-element counterpart of [`write_complex_column`] (which
    /// takes a whole-column `Value` at one row timestamp). It differs in two
    /// ways that are the whole point of epic #899:
    ///
    /// 1. **Real complex deletion** — when `complex_deletion` is `Some((mfda,
    ///    ldt))` the column header is the REAL deletion marker (unsigned VInt
    ///    deltas against the seeded baselines), not the hardcoded
    ///    `DeletionTime.LIVE` sentinel that [`write_complex_column`] always
    ///    writes. `None` writes the LIVE sentinel (byte-identical to the
    ///    whole-column path).
    /// 2. **Per-element metadata** — each element is stamped with its own
    ///    timestamp (kept as `USE_ROW_TIMESTAMP` only when equal to `row_ts`,
    ///    else an explicit unsigned delta), ttl, and local deletion time, and
    ///    its source `cell_path` is written verbatim (LIST 16-byte TimeUUID
    ///    round-trips, NOT regenerated).
    ///
    /// Element ORDER follows the on-disk invariant: SET/MAP are sorted by
    /// `cell_path` bytes (the serialized element / key); LIST preserves the
    /// caller-supplied (insertion) order — per-element timestamps must not
    /// reorder elements.
    ///
    /// PHASE B: exercised by unit tests only; `merge_entry_to_mutation` does NOT
    /// yet emit the ops that reach here (Phase C).
    pub(super) fn write_complex_column_per_element(
        &self,
        buf: &mut Vec<u8>,
        column: &Column,
        complex_deletion: Option<(i64, i32)>,
        elements: &[ComplexElementWrite],
        row_ts: i64,
    ) -> Result<()> {
        // ---- Column deletion header (markedForDeleteAt then localDeletionTime).
        match complex_deletion {
            None => {
                // DeletionTime.LIVE — byte-identical to write_complex_column.
                let ts_delta = i64::MIN.wrapping_sub(self.stats.min_timestamp) as u64;
                encode_unsigned(ts_delta, buf);
                let ldt_delta = i32::MAX.wrapping_sub(self.stats.min_local_deletion_time) as u32;
                encode_unsigned(ldt_delta as u64, buf);
            }
            Some((marked_for_delete_at, local_deletion_time)) => {
                // Real deletion marker (matches write_complex_column_deletion's
                // header encoding, but followed by surviving cells rather than 0).
                let ts_delta = (marked_for_delete_at - self.stats.min_timestamp) as u64;
                encode_unsigned(ts_delta, buf);

                // Issue #853 / epic #899 invariant: encode the LDT delta with the
                // same i32 wrapping cast Cassandra uses, so a far-future LDT in
                // [2^31, 2^32) keeps the correct byte count. Reject only a genuine
                // below-baseline ordering violation in normal (non-negative) space.
                if local_deletion_time >= 0
                    && self.stats.min_local_deletion_time >= 0
                    && local_deletion_time < self.stats.min_local_deletion_time
                {
                    return Err(Error::InvalidInput(format!(
                        "Complex deletion: local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                let ldt_delta =
                    local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
                encode_unsigned(ldt_delta as u64, buf);
            }
        }

        // ---- Element order: SET/MAP by cell_path bytes; UDT by SIGNED-short field
        // index (issue #927); LIST insertion order.
        let dt = column.data_type.to_lowercase();
        let is_list =
            dt.starts_with("list<") || dt.starts_with("org.apache.cassandra.db.marshal.listtype(");
        let is_udt = is_udt_marshal(&dt);
        let mut ordered: Vec<&ComplexElementWrite> = elements.iter().collect();
        if !is_list {
            ordered.sort_by(|a, b| compare_cell_paths(&a.cell_path, &b.cell_path, is_udt));
        }

        // ---- Cell count.
        encode_unsigned(ordered.len() as u64, buf);

        // ---- Per-element cells.
        let mut value_scratch = Vec::new();
        for elem in ordered {
            self.write_complex_element_cell(buf, elem, row_ts, &mut value_scratch)?;
        }

        Ok(())
    }

    /// Write one per-element complex cell (epic #899, Phase B).
    ///
    /// Wire format (matching the reader's `parse_complex_cell_value`):
    /// ```text
    /// [flags: u8]
    /// [timestamp_delta: unsigned VInt]   if NOT USE_ROW_TIMESTAMP
    /// [ldt_delta: unsigned VInt]         if (IS_DELETED || IS_EXPIRING) && !USE_ROW_TTL
    /// [ttl_delta: unsigned VInt]         if IS_EXPIRING && !USE_ROW_TTL
    /// [path_len: unsigned VInt][path_bytes]
    /// [value_len: unsigned VInt][value_bytes]   if NOT HAS_EMPTY_VALUE
    /// ```
    ///
    /// A tombstone (`is_deleted`) carries no value bytes, so it sets
    /// HAS_EMPTY_VALUE (0x04) alongside IS_DELETED (0x01) — final flags 0x05 —
    /// matching Cassandra's Cell.Serializer (`hasValue = !flag(HAS_EMPTY_VALUE)`).
    /// `value_scratch` (issue #1672): caller-owned buffer reused across the
    /// element loop; cleared before serializing each element's value.
    pub(super) fn write_complex_element_cell(
        &self,
        buf: &mut Vec<u8>,
        elem: &ComplexElementWrite,
        row_ts: i64,
        value_scratch: &mut Vec<u8>,
    ) -> Result<()> {
        // Determine flags.
        //
        // Cassandra's Cell.Serializer (BufferCell) derives value presence from the
        // HAS_EMPTY_VALUE (0x04) bit alone: `hasValue = !flag(HAS_EMPTY_VALUE_MASK)`.
        // A cell that carries NO value bytes on disk MUST set 0x04, otherwise a
        // strict reader will attempt to read a value-length VInt that is not present
        // and desynchronize. Two cases carry no value bytes here:
        //   1. A tombstone (IS_DELETED 0x01) — a deleted element holds no value.
        //   2. A live empty-value element (e.g. a SET member, whose datum lives in
        //      the cell_path rather than a value).
        // Both therefore set HAS_EMPTY_VALUE. A tombstone serializes with
        // IS_DELETED | HAS_EMPTY_VALUE = 0x05; a live SET member with 0x04. A live
        // MAP/LIST element that carries a value sets neither, so a value-length VInt
        // and value bytes follow.
        let mut flags = 0u8;
        if elem.is_deleted {
            // IS_DELETED implies no value bytes; pair it with HAS_EMPTY_VALUE so
            // Cassandra/strict readers do not look for a value length.
            flags |= CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE;
        } else if elem.value.is_none() {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        if elem.ttl_seconds.is_some() {
            flags |= CELL_IS_EXPIRING;
        }
        // Keep USE_ROW_TIMESTAMP only when the element's timestamp equals the row
        // timestamp; otherwise the element carries its own explicit delta.
        let use_row_ts = elem.timestamp_micros == row_ts;
        if use_row_ts {
            flags |= CELL_USE_ROW_TIMESTAMP;
        }

        buf.push(flags);

        // Timestamp delta (unsigned VInt) only when not borrowing the row ts.
        if !use_row_ts {
            let ts_delta = (elem.timestamp_micros - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, buf);
        }

        // Local deletion time delta — present for deleted or expiring cells.
        let is_expiring = elem.ttl_seconds.is_some();
        if elem.is_deleted || is_expiring {
            let ldt = match elem.local_deletion_time {
                Some(ldt) => ldt,
                None => {
                    return Err(Error::InvalidInput(format!(
                        "Complex element (deleted/expiring) requires a local_deletion_time \
                         (cell_path={:?})",
                        elem.cell_path
                    )));
                }
            };
            // Same i32 wrapping cast as the row/range/complex-deletion paths so a
            // far-future LDT in [2^31, 2^32) keeps the right byte count (epic #899).
            if ldt >= 0
                && self.stats.min_local_deletion_time >= 0
                && ldt < self.stats.min_local_deletion_time
            {
                return Err(Error::InvalidInput(format!(
                    "Complex element: local deletion time {} is less than min_local_deletion_time {}",
                    ldt, self.stats.min_local_deletion_time
                )));
            }
            let ldt_delta = ldt.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, buf);
        }

        // TTL delta — present for expiring cells.
        if is_expiring {
            let ttl = elem.ttl_seconds.unwrap_or(0);
            let ttl_delta = (ttl as i64) - (self.stats.min_ttl as i64);
            if ttl_delta < 0 {
                return Err(Error::InvalidInput(format!(
                    "Complex element: TTL {} is less than min_ttl {}",
                    ttl, self.stats.min_ttl
                )));
            }
            encode_unsigned(ttl_delta as u64, buf);
        }

        // Cell path — PRESERVED verbatim (LIST 16-byte TimeUUID round-trips).
        encode_unsigned(elem.cell_path.len() as u64, buf);
        buf.extend_from_slice(&elem.cell_path);

        // Value — written iff HAS_EMPTY_VALUE is clear (Cassandra:
        // `hasValue = !flag(HAS_EMPTY_VALUE_MASK)`). 0x04 is set above for both
        // tombstones and live empty-value elements, so this writes value bytes only
        // for a live MAP/LIST element that actually carries one. Keying off the flag
        // (not is_deleted directly) keeps the wire byte-for-byte consistent with the
        // header: a value length VInt is emitted only when a reader will read it.
        let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
        if !has_empty_value {
            if let Some(value) = &elem.value {
                value_scratch.clear();
                serialize_value_into(value, value_scratch)?;
                encode_unsigned(value_scratch.len() as u64, buf);
                buf.extend_from_slice(value_scratch);
            }
        }

        Ok(())
    }
}
