use super::*;

use super::marshal_element::MarshalCollectionElements;

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
                CellKind::Varint => Value::Varint(Vec::new()),
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

        // VInt-length-prefixed raw-bytes decode. Reads a single unsigned-VInt length
        // prefix, bounds-checks it, and returns the exact payload bytes, advancing the
        // shared `offset` cursor. The on-disk framing is identical for `blob` and
        // `varint`; the caller wraps the bytes in the target `Value` variant. Extracted
        // to one closure so the dispatch sites do not duplicate the length parse,
        // bounds check, cursor advance, and allocation (issue #1885, roborev DRY).
        let read_vint_prefixed_bytes = |offset: &mut usize| -> Result<Vec<u8>> {
            if *offset >= data.len() {
                return Err(Error::corruption(format!(
                    "Cell '{}': unexpected end at blob length (type: {})",
                    column.name, column.data_type
                )));
            }
            // Parse the length prefix as an unsigned VInt (can be > 255 bytes).
            let (remaining, len) = parse_vuint(&data[*offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse blob length as VInt: {:?}",
                    column.name, e
                ))
            })?;
            let len = len as usize;
            let bytes_consumed = data[*offset..].len() - remaining.len();
            *offset += bytes_consumed;

            if *offset + len > data.len() {
                return Err(Error::corruption(format!(
                    "Cell '{}': need {} bytes for blob, only {} available (type: {})",
                    column.name,
                    len,
                    data.len() - *offset,
                    column.data_type
                )));
            }

            let bytes = data[*offset..*offset + len].to_vec();
            *offset += len;
            Ok(bytes)
        };

        // Thin wrapper: the literal `blob` type (`CellKind::Blob`) and the
        // `CellKind::Complex` default fall-through (unknown types) both wrap the shared
        // VInt-prefixed bytes in `Value::Blob` — identical pre-J1 decode (the single
        // `_ =>` blob arm). Advances the shared `offset` cursor.
        let decode_vint_blob = |offset: &mut usize| -> Result<Value> {
            Ok(Value::Blob(read_vint_prefixed_bytes(offset)?))
        };

        let value = match kind {
            CellKind::Blob => decode_vint_blob(&mut offset)?,
            // CQL `varint`: VInt-length-prefixed raw two's-complement big-endian
            // bytes → `Value::Varint`. Byte-for-byte the same on-disk framing as the
            // `blob` arm (shares `read_vint_prefixed_bytes`), but preserves the declared
            // `varint` type instead of blobbing it. Mirrors the block /
            // `ComparatorType::Varint` path (`Value::Varint(value_data.to_vec())`) —
            // issue #1885.
            CellKind::Varint => Value::Varint(read_vint_prefixed_bytes(&mut offset)?),
            CellKind::Boolean => {
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

            CellKind::Int => {
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

            CellKind::Text => {
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

            CellKind::Uuid => {
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

            CellKind::Decimal => {
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

            CellKind::BigInt => {
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

            CellKind::Counter => {
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

                tracing::debug!(
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
                        tracing::debug!(
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
                        tracing::debug!(
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

            CellKind::Double => {
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

            CellKind::Timestamp => {
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

            CellKind::Date => {
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

            CellKind::Duration => {
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

                // months/days are i32 in Cassandra's DurationType. Reject
                // (rather than silently truncate via `as i32`) any encoded value
                // outside the i32 range so a corrupt encoding errors instead of
                // wrapping (issue #1632, item b).
                let months = i32::try_from(months).map_err(|_| {
                    Error::corruption(format!(
                        "Cell '{}': duration months out of i32 range",
                        column.name
                    ))
                })?;
                let days = i32::try_from(days).map_err(|_| {
                    Error::corruption(format!(
                        "Cell '{}': duration days out of i32 range",
                        column.name
                    ))
                })?;

                offset += duration_len;
                Value::Duration {
                    months,
                    days,
                    nanos,
                }
            }

            CellKind::Float => {
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

            CellKind::SmallInt => {
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

            CellKind::TinyInt => {
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

            CellKind::Time => {
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

            CellKind::Inet => {
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

            // Complex types: frozen, tuple, non-frozen collection, marshal-UDT, and
            // any unnamed/default type. J1 (issue #1635): the DISPATCH
            // is per-column via this single `CellKind::Complex` arm; the retained
            // prefix ladder (Epic J2 collapses it) uses the tag's already-lowercased
            // declared type as `type_str` — exactly the pre-J1 `normalized_type`, so
            // there is NO per-cell `to_lowercase`. The ladder ordering is unchanged.
            CellKind::Complex(lowered) => {
                let type_str: &str = lowered;
                if type_str.starts_with("frozen<") {
                    // Frozen types: unwrap inner type and route to appropriate parser
                    let inner_type = self.extract_frozen_inner_type(type_str)?;

                    tracing::debug!(
                        "V5CompressedLegacy: Parsing frozen type '{}' -> inner type '{}'",
                        type_str,
                        inner_type
                    );

                    // Issue #1340: extract the AUTHORITATIVE marshal element type(s)
                    // from the on-disk SerializationHeader marshal type (`header_type`).
                    // When an element is a `frozen<UDT>`, threading the marshal type lets
                    // it decode to a typed `Value::Frozen(Value::Udt)` registry-free
                    // (precedence: header marshal → registry → Blob, no byte-pattern
                    // inference — no-heuristics #28). Extracted once per frozen cell
                    // (before the element loop); the result borrows from `header_type`,
                    // so the per-element loop is allocation-free.
                    let marshal_elems =
                        header_type.and_then(Self::extract_marshal_collection_elements);
                    // Shared for list & set (both are `Sequence`): the borrowed element
                    // marshal type, or `None` for a map / absent / mismatched marshal.
                    let sequence_marshal_elem = match &marshal_elems {
                        Some(MarshalCollectionElements::Sequence(m)) => Some(*m),
                        _ => None,
                    };

                    // Route to appropriate frozen collection parser
                    let (inner_value, new_offset) = if inner_type.starts_with("list<") {
                        let schema_elem =
                            self.extract_collection_element_type(&inner_type, "list")?;
                        let element_type =
                            Self::prefer_udt_marshal_element(sequence_marshal_elem, &schema_elem);
                        self.parse_frozen_list_value(data, offset, element_type, column, _reader)?
                    } else if inner_type.starts_with("set<") {
                        let schema_elem =
                            self.extract_collection_element_type(&inner_type, "set")?;
                        let element_type =
                            Self::prefer_udt_marshal_element(sequence_marshal_elem, &schema_elem);
                        self.parse_frozen_set_value(data, offset, element_type, column, _reader)?
                    } else if inner_type.starts_with("map<") {
                        let (schema_key, schema_val) = self.extract_map_types(&inner_type)?;
                        let (marshal_key, marshal_val) = match &marshal_elems {
                            Some(MarshalCollectionElements::Map(k, v)) => (Some(*k), Some(*v)),
                            _ => (None, None),
                        };
                        let key_type = Self::prefer_udt_marshal_element(marshal_key, &schema_key);
                        let value_type = Self::prefer_udt_marshal_element(marshal_val, &schema_val);
                        self.parse_frozen_map_value(
                            data, offset, key_type, value_type, column, _reader,
                        )?
                    } else if Self::is_udt_type(&column.data_type) {
                        // Frozen UDT - parse using UDT parser
                        // The column.data_type contains the full Cassandra type string including UserType
                        tracing::debug!(
                            "V5CompressedLegacy: Parsing frozen UDT column '{}' type='{}'",
                            column.name,
                            column.data_type
                        );

                        // Parse UDT definition from the type string
                        let udt_def = Self::parse_udt_type_definition(&column.data_type)?;

                        // First read the VInt-prefixed blob length
                        let (remaining, blob_len_raw) =
                            parse_vuint(&data[offset..]).map_err(|e| {
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
                        tracing::debug!(
                        "V5CompressedLegacy: Resolving frozen UDT '{}' via registry for column '{}'",
                        inner_type,
                        column.name,
                    );

                        // Read VUInt-prefixed blob length (same framing as tuple and
                        // marshal-format UDT cells).
                        let (remaining, blob_len_raw) =
                            parse_vuint(&data[offset..]).map_err(|e| {
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
                                // Frozen-inner recursion: resolve the tag locally from
                                // `inner_column.data_type` (bounded, off the per-cell scan
                                // hot path).
                                None,
                                _reader,
                            )?;
                        (inner_val, inner_off)
                    };

                    offset = new_offset;

                    // Wrap in Frozen
                    Value::Frozen(Box::new(inner_value))
                } else if type_str.starts_with("tuple<") {
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
                else if type_str.starts_with("list<")
                    || type_str.starts_with("set<")
                    || type_str.starts_with("map<")
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
                else if Self::marshal_is_top_level_frozen_udt(&column.data_type) {
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

                // Default: treat as VInt-length-prefixed blob (unknown type).
                // Shares the `decode_vint_blob` closure with the literal-`blob`
                // `CellKind::Blob` arm — identical decode pre-J1 (both hit `_ =>`).
                else {
                    decode_vint_blob(&mut offset)?
                }
            }
        };

        Ok((value, cell_timestamp, cell_expiration, offset))
    }
}
