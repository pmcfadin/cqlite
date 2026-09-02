//! Scalar value-decode arms for the V5CompressedLegacy per-cell decoder.
//!
//! Campsite split of `cell_value.rs` (issue #1795): the flag/conditional-field
//! parsing and dispatch stay in `cell_value.rs`; the fixed-width and
//! VInt-length-prefixed scalar arms live here, the frozen/tuple/collection/
//! marshal-UDT ladder in `cell_value_complex.rs`.
//!
//! All VInt-length-prefixed arms route their length prefix through
//! [`super::V5CompressedLegacyParser::read_vint_length_prefixed_bytes`], which caps
//! the length before the `as usize` cast and uses saturating bounds arithmetic — no
//! attacker-controlled length can trigger an add-overflow panic (issue #1795).

use super::*;

impl V5CompressedLegacyParser {
    /// Decode a live (non-tombstone, non-empty) scalar cell value.
    ///
    /// `kind` is one of the scalar [`CellKind`] variants; [`CellKind::Complex`] is
    /// routed to [`Self::decode_complex_cell_value`] by the caller and returns an
    /// internal error here (never reached in practice). Advances `offset` past the
    /// consumed value bytes on success.
    pub(super) fn decode_scalar_cell_value(
        data: &[u8],
        offset: &mut usize,
        kind: &CellKind,
        column: &crate::schema::Column,
    ) -> Result<Value> {
        let mut off = *offset;
        let value = match kind {
            CellKind::Blob => {
                let bytes = Self::read_vint_length_prefixed_bytes(data, &mut off, column, "blob")?;
                // Issue #1644 (K5 stage 2): borrow a zero-copy view of the active
                // scan window's Bytes when this slice lies within it (the common
                // streaming case); falls back to an owned copy outside a windowed
                // scan (get()/compaction) or across a chunk straddle.
                Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(bytes))
            }
            CellKind::Boolean => {
                // Boolean: [0x08][u8 value]
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at boolean value",
                        column.name
                    )));
                }
                let bool_byte = data[off];
                off += 1;
                Value::Boolean(bool_byte != 0)
            }

            CellKind::Int => {
                // Integer (i32): fixed-width 4 bytes (no length prefix in Cassandra 5.0)
                if off + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for int, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }
                let int_val =
                    i32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                off += 4;
                Value::Integer(int_val)
            }

            CellKind::Text => {
                // Text: [VInt len][text bytes]
                let bytes = Self::read_vint_length_prefixed_bytes(data, &mut off, column, "text")?;
                // Issue #1644 (K5 stage 2): validate UTF-8 IN PLACE on the borrowed
                // slice, then store the (possibly zero-copy) Bytes — no separate
                // owned-String detour.
                std::str::from_utf8(bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': invalid UTF-8 in text value: {}",
                        column.name, e
                    ))
                })?;
                Value::Text(crate::storage::sstable::reader::value_borrow::borrow_active(bytes))
            }

            CellKind::Uuid => {
                // UUID/TimeUUID: fixed-width 16 bytes (no length prefix in Cassandra 5.0 writer)
                if off + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 16 bytes for UUID, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }

                let uuid_bytes: [u8; 16] = data[off..off + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                off += 16;
                Value::Uuid(uuid_bytes)
            }

            CellKind::Decimal => {
                // Decimal: [VInt total_len][i32 scale][unscaled bytes]
                let bytes =
                    Self::read_vint_length_prefixed_bytes(data, &mut off, column, "decimal")?;

                // First 4 bytes: scale (i32 BE)
                if bytes.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': decimal length {} too small for scale",
                        column.name,
                        bytes.len()
                    )));
                }
                let scale = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                // Remaining bytes: unscaled value
                let unscaled = bytes[4..].to_vec();
                Value::Decimal { scale, unscaled }
            }

            CellKind::BigInt => {
                // BigInt: fixed-width 8 bytes (no length prefix in Cassandra 5.0)
                if off + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for bigint, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }
                let val = i64::from_be_bytes([
                    data[off],
                    data[off + 1],
                    data[off + 2],
                    data[off + 3],
                    data[off + 4],
                    data[off + 5],
                    data[off + 6],
                    data[off + 7],
                ]);
                off += 8;
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

                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at counter context length",
                        column.name
                    )));
                }
                // Read the VInt length prefix.
                let (remaining, context_len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse counter context length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                // Cap BEFORE the `as usize` cast so an adversarial length returns
                // `Err` rather than overflowing the bounds add below (issue #1795).
                if context_len > MAX_CELL_VALUE_LENGTH {
                    return Err(Error::corruption(format!(
                        "Cell '{}': counter context length {} exceeds maximum {}",
                        column.name, context_len, MAX_CELL_VALUE_LENGTH
                    )));
                }
                let context_len = context_len as usize;
                let len_bytes_consumed = data[off..].len() - remaining.len();
                off += len_bytes_consumed;

                tracing::debug!(
                    "V5CompressedLegacy: Counter '{}' context_len={} (len prefix: {} bytes)",
                    column.name,
                    context_len,
                    len_bytes_consumed
                );

                // Overflow-safe bounds check (never `off + context_len`).
                if context_len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for counter context, only {} available",
                        column.name,
                        context_len,
                        data.len() - off
                    )));
                }

                // Try the full CounterContext parse first.
                match Self::parse_counter_context(data, off, &column.name) {
                    Ok((total, consumed)) if consumed == context_len => {
                        // Successfully parsed a proper CounterContext.
                        off += consumed;
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
                            data[off],
                            data[off + 1],
                            data[off + 2],
                            data[off + 3],
                            data[off + 4],
                            data[off + 5],
                            data[off + 6],
                            data[off + 7],
                        ]);
                        off += 8;
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
                if off + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for double, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }
                let val = f64::from_be_bytes([
                    data[off],
                    data[off + 1],
                    data[off + 2],
                    data[off + 3],
                    data[off + 4],
                    data[off + 5],
                    data[off + 6],
                    data[off + 7],
                ]);
                off += 8;
                Value::Float(val)
            }

            CellKind::Timestamp => {
                // Timestamp: 8 bytes, i64 milliseconds big-endian (NO length prefix, per Cassandra spec)
                if off + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for timestamp, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[off],
                    data[off + 1],
                    data[off + 2],
                    data[off + 3],
                    data[off + 4],
                    data[off + 5],
                    data[off + 6],
                    data[off + 7],
                ]);
                off += 8;
                Value::Timestamp(millis)
            }

            CellKind::Date => {
                // Date: [VInt len=4][i32 BE days]
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at date length",
                        column.name
                    )));
                }

                let (remaining, date_len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse date length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;

                // #3848: compare the RAW `u64` against the required width, then
                // narrow. Casting first would let `(1 << 32) + N` pass the `== N`
                // test on a 32-bit target (truncation is chosen, not random).
                let date_len =
                    checked_vuint_exact_length(date_len, &[4], "Cell", &column.name, "date")?;

                if date_len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for date, only {} available",
                        column.name,
                        date_len,
                        data.len() - off
                    )));
                }

                let stored =
                    u32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                off += 4;
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Value::Date(days_since_epoch)
            }

            CellKind::Duration => {
                // Duration: [VInt len][months VInt][days VInt][nanos VInt]
                let duration_bytes =
                    Self::read_vint_length_prefixed_bytes(data, &mut off, column, "duration")?;

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

                Value::Duration {
                    months,
                    days,
                    nanos,
                }
            }

            CellKind::Float => {
                // Float: 4 bytes, f32 big-endian (NO length prefix, fixed size).
                // CQL `float` is IEEE single precision — decode to Value::Float32,
                // matching the block path (issue #1884). Widening to f64 was lossy.
                if off + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for float, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }

                let val =
                    f32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                off += 4;
                Value::Float32(val)
            }

            CellKind::SmallInt => {
                // SmallInt: [VInt len=2][i16 BE]
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at smallint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse smallint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;

                // #3848: compare the RAW `u64` against the required width, then
                // narrow. Casting first would let `(1 << 32) + N` pass the `== N`
                // test on a 32-bit target (truncation is chosen, not random).
                let len = checked_vuint_exact_length(len, &[2], "Cell", &column.name, "smallint")?;

                if len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for smallint, only {} available",
                        column.name,
                        len,
                        data.len() - off
                    )));
                }

                let val = i16::from_be_bytes([data[off], data[off + 1]]);
                off += 2;
                Value::SmallInt(val)
            }

            CellKind::TinyInt => {
                // TinyInt: [VInt len=1][i8]
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at tinyint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse tinyint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;

                // #3848: compare the RAW `u64` against the required width, then
                // narrow. Casting first would let `(1 << 32) + N` pass the `== N`
                // test on a 32-bit target (truncation is chosen, not random).
                let len = checked_vuint_exact_length(len, &[1], "Cell", &column.name, "tinyint")?;

                if len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 1 byte for tinyint, only {} available",
                        column.name,
                        data.len() - off
                    )));
                }

                let val = data[off] as i8;
                off += 1;
                Value::TinyInt(val)
            }

            CellKind::Time => {
                // Time: [VInt len=8][i64 BE nanoseconds since midnight]
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at time length",
                        column.name
                    )));
                }
                let (remaining, time_len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse time length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;
                // #3848: compare the RAW `u64` against the required width, then
                // narrow. Casting first would let `(1 << 32) + N` pass the `== N`
                // test on a 32-bit target (truncation is chosen, not random).
                let time_len =
                    checked_vuint_exact_length(time_len, &[8], "Cell", &column.name, "time")?;
                if time_len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for time value, only {} available",
                        column.name,
                        time_len,
                        data.len() - off
                    )));
                }
                let nanos = i64::from_be_bytes([
                    data[off],
                    data[off + 1],
                    data[off + 2],
                    data[off + 3],
                    data[off + 4],
                    data[off + 5],
                    data[off + 6],
                    data[off + 7],
                ]);
                off += 8;
                Value::Time(nanos)
            }

            CellKind::Inet => {
                // Inet: [VInt len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                if off >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at inet length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse inet length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;

                // #3848: compare the RAW `u64` against the required width, then
                // narrow. Casting first would let `(1 << 32) + N` pass the `== N`
                // test on a 32-bit target (truncation is chosen, not random).
                let len = checked_vuint_exact_length(len, &[4, 16], "Cell", &column.name, "inet")?;

                if len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for inet, only {} available",
                        column.name,
                        len,
                        data.len() - off
                    )));
                }

                // Issue #1644 (K5 stage 2): borrow, see the blob arm above.
                let bytes = crate::storage::sstable::reader::value_borrow::borrow_active(
                    &data[off..off + len],
                );
                off += len;
                Value::Inet(bytes)
            }

            // The complex ladder is dispatched by the caller
            // (`parse_cell_value_schema_order`) to `decode_complex_cell_value`;
            // reaching here means an internal dispatch bug, not adversarial input.
            CellKind::Complex(_) => {
                return Err(Error::corruption(format!(
                    "Cell '{}': complex type routed to the scalar decoder (internal error)",
                    column.name
                )));
            }
        };

        *offset = off;
        Ok(value)
    }
}
