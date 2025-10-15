//! Parser for Cassandra 5.0 V5CompressedLegacy decompressed blocks
//!
//! This format uses **u8 length prefixes** (NOT VInt) for partition keys and strings,
//! and simplified encoding optimized for compression. This differs from the newer
//! V5_0NewBig and V5_0Bti formats which use pure VInt encoding.
//!
//! Based on format research in docs/V5_COMPRESSED_LEGACY_FORMAT_SPEC.md
//!
//! ## Format Structure
//!
//! ```text
//! Decompressed Block:
//! ├─ [0x00] Partition flags (u8)
//! ├─ [0x01] Partition key length (u8, NOT VInt)
//! ├─ [0x02..] Raw partition key bytes
//! ├─ [+0] Partition deletion time (i32 big-endian)
//! ├─ [+4] Unknown 8-byte field
//! ├─ [+8] Row data begins
//! │  ├─ Row header (flags, timestamp)
//! │  └─ Cells:
//! │     ├─ Type tag or flags (u8)
//! │     ├─ Column name length (u8)
//! │     ├─ Column name bytes
//! │     ├─ Value length (varies)
//! │     └─ Value bytes
//! ```

use std::collections::HashMap;

use log::{debug, warn};

use crate::{schema::TableSchema, types::TableId, Error, Result, RowKey, Value};

/// Parser for V5CompressedLegacy format decompressed blocks
pub struct V5CompressedLegacyParser {
    keyspace: String,
    table_name: String,
}

impl V5CompressedLegacyParser {
    /// Create a new V5CompressedLegacy parser
    pub fn new(keyspace: String, table_name: String) -> Self {
        Self {
            keyspace,
            table_name,
        }
    }

    /// Parse decompressed block into (TableId, RowKey, Value) entries
    ///
    /// # Arguments
    /// * `data` - Decompressed block bytes
    /// * `schema` - Optional table schema for type-aware parsing
    /// * `reader` - Reference to SSTableReader for value parsing
    ///
    /// # Returns
    /// * `Ok(Vec<(TableId, RowKey, Value)>)` - Parsed entries
    /// * `Err(Error)` - Parse error with context
    pub fn parse_block(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // V5CompressedLegacy format stores cells WITHOUT column names,
        // relying on schema to interpret the binary data. Schema is REQUIRED.
        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        log::debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );
        log::debug!(
            "V5CompressedLegacy: Schema has {} columns",
            schema.columns.len()
        );
        for (i, col) in schema.columns.iter().enumerate() {
            log::debug!("  Column {}: {} ({})", i, col.name, col.data_type);
        }
        log::debug!(
            "V5CompressedLegacy: First 64 bytes of data: {}",
            hex::encode(&data[..std::cmp::min(64, data.len())])
        );
        debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );

        let mut results = Vec::new();
        let mut offset = 0;
        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        // Parse ALL partitions in block (Issue #2 fix: previously only parsed one partition)
        while offset < data.len() {
            // Try to parse partition header
            match self.parse_partition_header(data, offset) {
                Ok((partition_key, new_offset)) => {
                    offset = new_offset;

                    log::debug!(
                        "V5CompressedLegacy: Parsed partition key: {} bytes, now at offset {}",
                        partition_key.0.len(),
                        offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Row data starts at offset {}, remaining: {} bytes",
                        offset,
                        data.len() - offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Row data hex (first 128 bytes): {}",
                        hex::encode(&data[offset..std::cmp::min(offset + 128, data.len())])
                    );

                    debug!(
                        "V5CompressedLegacy: Parsed partition key: {} bytes, now at offset {}",
                        partition_key.0.len(),
                        offset
                    );

                    // Parse row data for this partition
                    // Note: V5CompressedLegacy format documentation incomplete - assuming single row for now
                    // TODO: Determine if/how multiple rows per partition are encoded (Issue #160)
                    match self.parse_row_data_with_offset(data, offset, Some(schema), reader) {
                        Ok((cells, final_offset)) => {
                            offset = final_offset;

                            log::debug!(
                                "V5CompressedLegacy: Parsed {} cells from row data",
                                cells.len()
                            );

                            debug!(
                                "V5CompressedLegacy: Parsed {} cells from row data",
                                cells.len()
                            );

                            // Convert cells HashMap to Value::Map (required by SelectExecutor)
                            // SelectExecutor expects Value::Map(Vec<(Value, Value)>) where each entry is
                            // (Value::Text(column_name), column_value)
                            let row_value = if cells.is_empty() {
                                warn!(
                                    "V5CompressedLegacy: No cells extracted for {}.{} (partition key: {} bytes)",
                                    self.keyspace,
                                    self.table_name,
                                    partition_key.0.len()
                                );
                                Value::Null
                            } else {
                                // Convert HashMap<String, Value> to Vec<(Value, Value)> for Value::Map
                                let map_entries: Vec<(Value, Value)> = cells
                                    .into_iter()
                                    .map(|(name, value)| (Value::Text(name), value))
                                    .collect();
                                Value::Map(map_entries)
                            };

                            results.push((table_id.clone(), partition_key, row_value));
                        }
                        Err(e) => {
                            debug!(
                                "V5CompressedLegacy: Failed to parse row data at offset {}: {} (end of valid data)",
                                offset, e
                            );
                            break; // End of valid data in block
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        "V5CompressedLegacy: Failed to parse partition header at offset {}: {} (end of partitions)",
                        offset, e
                    );
                    break; // No more partitions in block
                }
            }
        }

        debug!(
            "V5CompressedLegacy: Parsed {} total entries from block",
            results.len()
        );

        Ok(results)
    }

    /// Parse partition header (flags, key, deletion time)
    ///
    /// # Format
    /// ```text
    /// [flags: u8][key_len: u8][key_bytes: [u8; key_len]][del_time: i32][unknown: 8 bytes]
    /// ```
    fn parse_partition_header(&self, data: &[u8], mut offset: usize) -> Result<(RowKey, usize)> {
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition header offset {} out of bounds (data len: {})",
                offset,
                data.len()
            )));
        }

        // Byte 0: Flags (ignore for now - may indicate static rows, deletions, etc.)
        let _flags = data[offset];
        offset += 1;

        // Byte 1: Partition key length (u8, NOT VInt)
        if offset >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at partition key length",
            ));
        }
        let key_len = data[offset] as usize;
        offset += 1;

        debug!(
            "V5CompressedLegacy: Partition key length = {} bytes",
            key_len
        );

        // Next key_len bytes: Partition key data (raw bytes, no component structure)
        if offset + key_len > data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition key extends beyond data (offset: {}, key_len: {}, data_len: {})",
                offset, key_len, data.len()
            )));
        }
        let key_bytes = data[offset..offset + key_len].to_vec();
        offset += key_len;

        // Next 4 bytes: Partition deletion time (i32 big-endian)
        // 0x7fffffff = Integer.MAX_VALUE = no deletion
        if offset + 4 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at partition deletion time",
            ));
        }
        let _del_time = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Next 8 bytes: Unknown field (possibly base timestamp or flags)
        // Skip for now - format research incomplete on this field
        if offset + 8 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at unknown 8-byte field",
            ));
        }
        offset += 8;

        // Create RowKey from partition key bytes
        let row_key = RowKey(key_bytes);

        Ok((row_key, offset))
    }

    /// Parse row data (header + cells) and return cells with new offset
    ///
    /// V5CompressedLegacy format stores cells WITHOUT column names in schema column order.
    /// Schema is REQUIRED to determine which column each value belongs to.
    ///
    /// Returns: (cells, new_offset)
    fn parse_row_data_with_offset(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<(HashMap<String, Value>, usize)> {
        let mut cells = HashMap::new();

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

        // Row header has VARIABLE length (7-23+ bytes) depending on flags, timestamps, etc.
        // Instead of assuming fixed size, search for first 0x08 byte (cell marker).
        //
        // Cell format varies by type (all start with 0x08 marker):
        // - Boolean: [0x08][u8 value] (2 bytes total)
        // - Int: [0x08][i32 BE value] (5 bytes total, NO length field)
        // - Decimal: [0x08][u8 len][i32 scale][unscaled bytes]
        // - Text/Ascii: [0x08][u8 len][text bytes]

        // Find first 0x08 marker (start of first cell)
        let cell_start = data[offset..]
            .iter()
            .position(|&b| b == 0x08)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "V5CompressedLegacy: No cell marker (0x08) found after partition header at offset {} (searched {} bytes)",
                    offset,
                    data.len() - offset
                ))
            })?;

        let row_header_size = cell_start;
        offset += cell_start;

        debug!(
            "V5CompressedLegacy: Found cell marker at offset {} (row header was {} bytes)",
            offset, row_header_size
        );

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

        let columns_in_order = &schema.columns;

        log::debug!("V5CompressedLegacy: Parsing up to {} cells in SCHEMA DEFINITION ORDER starting at offset {} (row header was {} bytes)", columns_in_order.len(), offset, row_header_size);
        log::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        for (col_idx, column) in columns_in_order.iter().enumerate() {
            if offset >= data.len() {
                log::debug!(
                    "V5CompressedLegacy: Reached end of data at column {} ('{}'), parsed {}/{} cells (remaining columns are NULL)",
                    col_idx,
                    column.name,
                    cells.len(),
                    columns_in_order.len()
                );
                break;
            }

            log::debug!(
                "V5CompressedLegacy: Parsing column {} '{}' ({}) at offset {}",
                col_idx,
                column.name,
                column.data_type,
                offset
            );

            match self.parse_cell_value_schema_order(data, offset, column, reader) {
                Ok((value, new_offset)) => {
                    log::debug!(
                        "V5CompressedLegacy:   ✓ Parsed '{}' = {:?}, consumed {} bytes",
                        column.name,
                        value,
                        new_offset - offset
                    );
                    cells.insert(column.name.clone(), value);
                    offset = new_offset;
                }
                Err(e) => {
                    log::debug!(
                        "V5CompressedLegacy:   ✗ Failed to parse '{}' at column index {} (offset {}): {} - treating as NULL and stopping parse",
                        column.name, col_idx, offset, e
                    );
                    // Show hex for debugging
                    let dump_len = std::cmp::min(32, data.len() - offset);
                    log::debug!(
                        "V5CompressedLegacy:     Hex at failure: {}",
                        hex::encode(&data[offset..std::cmp::min(offset + dump_len, data.len())])
                    );
                    // CRITICAL FIX: Stop parsing remaining columns when we hit an error
                    // The offset doesn't advance here, but we exit the loop cleanly
                    // rather than continuing with invalid offset
                    break;
                }
            }
        }

        log::debug!(
            "V5CompressedLegacy: Parsed {}/{} columns (missing columns are NULL)",
            cells.len(),
            columns_in_order.len()
        );

        debug!("V5CompressedLegacy: Parsed total of {} cells", cells.len());

        Ok((cells, offset))
    }

    /// Parse a single cell value WITHOUT column name (schema-order format)
    ///
    /// All cells start with 0x08 marker, but format varies by type:
    /// - Boolean: [0x08][u8 value] (2 bytes total, 0x00=false, 0x01=true)
    /// - Int: [0x08][i32 BE value] (5 bytes total, NO length field)
    /// - Decimal: [0x08][u8 len][i32 BE scale][unscaled bytes]
    /// - Text/Ascii/Varchar: [0x08][u8 len][text bytes]
    /// - UUID: [0x08][u8 len=16][16 bytes]
    /// - Blob: [0x08][u8 len][bytes]
    ///
    /// Returns: (value, new_offset)
    fn parse_cell_value_schema_order(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        // All cells start with 0x08 marker
        // V5CompressedLegacy format: Simple type tag/marker byte (0x08), NOT Cassandra cell flags
        // NOTE: The full Cassandra 5.0 cell flags format (with bitset flags like 0x20=NULL,
        // 0x04=EMPTY, etc.) applies to NEWER formats (V5_0NewBig, V5_0Bti), not this legacy format.
        // V5CompressedLegacy uses a simplified marker byte where 0x08 indicates "cell data follows".
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Cell '{}': unexpected end at marker byte",
                column.name
            )));
        }
        let marker = data[offset];
        if marker != 0x08 {
            return Err(Error::corruption(format!(
                "Cell '{}': expected marker 0x08, got 0x{:02x}",
                column.name, marker
            )));
        }
        offset += 1;

        // Parse based on column type (data_type is a String with CQL type name)
        let value = match column.data_type.as_str() {
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
                // Integer (i32): [0x08][i32 BE value] (NO length field!)
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
                // Text: [0x08][u8 len][text bytes]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at text length",
                        column.name
                    )));
                }
                let text_len = data[offset] as usize;
                offset += 1;

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

            "uuid" => {
                // UUID: [0x08][u8 len=16][16 bytes]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at UUID length",
                        column.name
                    )));
                }
                let uuid_len = data[offset] as usize;
                offset += 1;

                if uuid_len != 16 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected UUID length 16, got {}",
                        column.name, uuid_len
                    )));
                }

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
                // Decimal: [u8 total_len][i32 scale][unscaled bytes]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at decimal length",
                        column.name
                    )));
                }
                let total_len = data[offset] as usize;
                offset += 1;

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

            "bigint" | "counter" => {
                // BigInt/Counter: 8 bytes, i64 big-endian (NO length prefix)
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
                if column.data_type == "counter" {
                    Value::Counter(val)
                } else {
                    Value::BigInt(val)
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
                // Timestamp: 8 bytes, i64 microseconds big-endian (NO length prefix)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for timestamp, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let micros = i64::from_be_bytes([
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
                Value::Timestamp(micros)
            }

            "date" => {
                // Date: 4 bytes, u32 days since epoch (unsigned, adjusted by Integer.MIN_VALUE)
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for date, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let days = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                // Cassandra stores as unsigned offset, adjust by Integer.MIN_VALUE
                let adjusted = days.wrapping_sub(i32::MIN as u32) as i32;
                Value::Date(adjusted)
            }

            "time" => {
                // Time: 8 bytes, i64 nanoseconds since midnight (NO length prefix)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for time, only {} available",
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
                // Inet: [u8 len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at inet length",
                        column.name
                    )));
                }
                let len = data[offset] as usize;
                offset += 1;

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

            "timeuuid" => {
                // TimeUUID: [u8 len=16][16 bytes] (same as UUID but time-based)
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at timeuuid length",
                        column.name
                    )));
                }
                let uuid_len = data[offset] as usize;
                offset += 1;

                if uuid_len != 16 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected timeuuid length 16, got {}",
                        column.name, uuid_len
                    )));
                }

                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 16 bytes for timeuuid, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("TimeUUID byte conversion failed"))?;

                offset += 16;
                Value::Uuid(uuid_bytes)
            }

            // Default: treat as length-prefixed blob
            _ => {
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at blob length",
                        column.name
                    )));
                }
                let blob_len = data[offset] as usize;
                offset += 1;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for blob, only {} available",
                        column.name,
                        blob_len,
                        data.len() - offset
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }
        };

        Ok((value, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_header_parsing() {
        // Hex from test data: 00 10 15291a77... 7fffffff 8000000000000000
        let hex_str = "001015291a77d7394e738397b787442f3a1f7fffffff8000000000000000";
        let data = hex::decode(hex_str).unwrap();

        let parser =
            V5CompressedLegacyParser::new("test_basic".to_string(), "simple_table".to_string());
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction
        assert_eq!(row_key.0.len(), 16); // UUID is 16 bytes

        // Verify offset consumed: 1 (flags) + 1 (len) + 16 (uuid) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(offset, 30);

        // Verify UUID bytes match
        let expected_uuid_bytes = hex::decode("15291a77d7394e738397b787442f3a1f").unwrap();
        assert_eq!(row_key.0, expected_uuid_bytes);
    }
}
