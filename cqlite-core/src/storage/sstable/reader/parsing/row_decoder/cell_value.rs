use super::*;

impl V5CompressedLegacyParser {
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
    ///
    /// The scalar value-decode arms live in [`cell_value_scalar`], the
    /// frozen/tuple/collection/marshal-UDT ladder in [`cell_value_complex`]
    /// (campsite split, issue #1795); this function owns the flag/conditional-field
    /// parsing and the tombstone/empty short-circuits.
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
        // J1 (issue #1635): the precomputed per-column dispatch tag (from
        // `ColumnToParse.kind`, resolved ONCE per block). `Some` on the hot per-cell
        // scan loop → dispatch on it with NO per-cell `to_lowercase`. `None` on the
        // rare recursive frozen-inner / in-crate test callers → resolve it locally
        // from `column.data_type` (bounded, off the per-cell scan hot path).
        kind: Option<&CellKind>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
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

        tracing::debug!(
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
            tracing::debug!(
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
            tracing::debug!(
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
            tracing::debug!(
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
            tracing::debug!(
                "V5CompressedLegacy: Cell '{}' is tombstone (deleted), returning Tombstone(deletion_time={})",
                column.name, deletion_time
            );
            return Ok((
                Value::Tombstone(Box::new(TombstoneInfo {
                    deletion_time,
                    tombstone_type: TombstoneType::CellTombstone,
                    // On-disk `localDeletionTime` (GC clock, seconds) for the cell
                    // tombstone; `0` when not surfaced (#873).
                    local_deletion_time: cell_local_deletion_time.unwrap_or(0),
                    ttl: None,
                    range_start: None,
                    range_end: None,
                })),
                cell_timestamp,
                cell_expiration,
                offset,
            ));
        }

        // J1 (issue #1635): dispatch on the PRECOMPUTED per-column tag. The hot
        // per-cell scan loop passes `Some(&ctp.kind)` (resolved once per block in
        // `RowColumnResolution::build`), so the per-cell `column.data_type
        // .to_lowercase()` + string-ladder walk is gone — no per-cell normalization,
        // no per-cell allocation. `None` (recursive frozen-inner / in-crate tests)
        // resolves the tag locally from the declared type; that path is bounded and
        // off the per-cell scan hot path, so it neither regresses throughput nor the
        // `TYPE_NORMALIZE_CALLS` gauge (which measures the per-cell decode path).
        let resolved_kind: CellKind;
        let kind: &CellKind = match kind {
            Some(k) => k,
            None => {
                resolved_kind = CellKind::from_type(&column.data_type);
                &resolved_kind
            }
        };

        // Handle empty cells (no value bytes to read)
        if !has_value {
            tracing::debug!(
                "V5CompressedLegacy: Cell '{}' has HAS_EMPTY_VALUE flag, returning empty value",
                column.name
            );
            // Issue #1077: the empty (zero-length) value MUST decode to the empty
            // value of the column's DECLARED type — never blindly `Text("")`.
            // An empty `blob` is `Blob([])` (sstabledump renders `"0x"`), an empty
            // text/ascii/varchar is `Text("")`. Mirrors the clustering-key EMPTY
            // handling above; fixed-width types should not normally carry an empty
            // value, so treat that as NULL with a warning. Dispatched on the
            // precomputed tag (Text ⇔ text/varchar/ascii, Blob ⇔ literal blob;
            // every other declared type → NULL), byte-identical to the pre-J1
            // `match normalized_type.as_str()` empty arm.
            let empty_value = match kind {
                CellKind::Text => Value::Text(String::new()),
                CellKind::Blob => Value::Blob(Vec::new()),
                // Issue #1885: an empty `varint` cell is `Varint([])`, matching the
                // block / `ComparatorType::Varint` path (empty slice → `Varint([])`).
                // In this split layout `varint` routes through `CellKind::Complex`
                // (the retained string ladder, decoded in `cell_value_complex`), so
                // match on the lowered declared-type string rather than a dedicated
                // scalar tag.
                CellKind::Complex(lowered) if lowered.as_ref() == "varint" => {
                    Value::Varint(Vec::new())
                }
                _ => {
                    tracing::warn!(
                        "V5CompressedLegacy: EMPTY value for cell '{}' (type {}), treating as NULL",
                        column.name,
                        column.data_type
                    );
                    Value::Null
                }
            };
            return Ok((empty_value, cell_timestamp, cell_expiration, offset));
        }

        // At this point, we have a live cell with value data. Dispatch the decode on
        // the precomputed `CellKind` (jump table): scalar arms in `cell_value_scalar`,
        // the frozen/tuple/collection/marshal-UDT/default ladder in
        // `cell_value_complex` (campsite split, issue #1795).
        let value = match kind {
            CellKind::Complex(lowered) => self.decode_complex_cell_value(
                data,
                &mut offset,
                lowered,
                column,
                header_type,
                reader,
            )?,
            scalar => Self::decode_scalar_cell_value(data, &mut offset, scalar, column)?,
        };

        Ok((value, cell_timestamp, cell_expiration, offset))
    }

    /// Read a Cassandra-VInt-length-prefixed byte run with **overflow-safe** bounds
    /// checks (issue #1795).
    ///
    /// The reported panic (`attempt to add with overflow`) came from decoding an
    /// UNCAPPED `parse_vuint` length into `usize` and then computing
    /// `offset + total_len > data.len()` — the ADD overflowed on an adversarial
    /// length prefix before the guard could reject it. This helper mirrors the
    /// blessed [`crate::parser::vint::parse_vint_length`] pattern: it rejects a
    /// length exceeding [`MAX_CELL_VALUE_LENGTH`] BEFORE the `as usize` cast, and
    /// compares against the remaining bytes with a saturating subtraction (never
    /// `offset + len`) so no attacker-controlled length can trigger an add-overflow
    /// panic. Returns the value bytes (borrowed from `data`) and advances `offset`
    /// past them. Shared by the scalar blob/text/decimal/duration arms and the
    /// complex blob/varint fall-throughs so the bounds logic lives in one place.
    pub(super) fn read_vint_length_prefixed_bytes<'d>(
        data: &'d [u8],
        offset: &mut usize,
        column: &crate::schema::Column,
        what: &str,
    ) -> Result<&'d [u8]> {
        if *offset >= data.len() {
            return Err(Error::corruption(format!(
                "Cell '{}': unexpected end at {} length",
                column.name, what
            )));
        }
        let (remaining, len_raw) = parse_vuint(&data[*offset..]).map_err(|e| {
            Error::corruption(format!(
                "Cell '{}': failed to parse {} length as VInt: {:?}",
                column.name, what, e
            ))
        })?;
        // Cap BEFORE the `as usize` cast (matching `parse_vint_length`): an
        // adversarial length must return `Err`, never overflow the add below.
        if len_raw > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Cell '{}': {} length {} exceeds maximum {}",
                column.name, what, len_raw, MAX_CELL_VALUE_LENGTH
            )));
        }
        let len = len_raw as usize;
        let bytes_consumed = data[*offset..].len() - remaining.len();
        *offset += bytes_consumed;

        // Overflow-safe bounds check: `len > remaining` rather than
        // `offset + len > data.len()` (the latter can overflow `usize`).
        if len > data.len().saturating_sub(*offset) {
            return Err(Error::corruption(format!(
                "Cell '{}': need {} bytes for {}, only {} available",
                column.name,
                len,
                what,
                data.len() - *offset
            )));
        }

        let bytes = &data[*offset..*offset + len];
        *offset += len;
        Ok(bytes)
    }
}
