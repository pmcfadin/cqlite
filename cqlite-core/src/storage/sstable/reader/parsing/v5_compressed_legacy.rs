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

        let mut offset = 0;

        // Parse partition header
        let (partition_key, new_offset) = self.parse_partition_header(data, offset)?;
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

        // Parse row data
        let cells = self.parse_row_data(data, offset, Some(schema), reader)?;

        log::debug!(
            "V5CompressedLegacy: Parsed {} cells from row data",
            cells.len()
        );

        debug!(
            "V5CompressedLegacy: Parsed {} cells from row data",
            cells.len()
        );

        // Build result entries
        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        // Convert cells HashMap to Value::Row
        let row_value = if cells.is_empty() {
            warn!(
                "V5CompressedLegacy: No cells extracted for {}.{} (partition key: {} bytes)",
                self.keyspace,
                self.table_name,
                partition_key.0.len()
            );
            Value::Null
        } else {
            // Use UDT representation for rows (consistent with existing code)
            let fields = cells
                .into_iter()
                .map(|(name, value)| crate::types::UdtField {
                    name,
                    value: Some(value),
                })
                .collect();

            Value::Udt(crate::types::UdtValue {
                keyspace: self.keyspace.clone(),
                type_name: self.table_name.clone(),
                fields,
            })
        };

        Ok(vec![(table_id, partition_key, row_value)])
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

    /// Parse row data (header + cells)
    ///
    /// V5CompressedLegacy format stores cells WITHOUT column names in schema column order.
    /// Schema is REQUIRED to determine which column each value belongs to.
    fn parse_row_data(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<HashMap<String, Value>> {
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

        // Parse cells in schema column order (NO column names in data)
        // Cells are stored in alphabetical order by column name to match Cassandra schema ordering
        let mut sorted_columns = schema.columns.clone();
        sorted_columns.sort_by(|a, b| a.name.cmp(&b.name));

        log::debug!("V5CompressedLegacy: Parsing {} cells in schema order starting at offset {} (row header was {} bytes)", sorted_columns.len(), offset, row_header_size);
        log::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        for (col_idx, column) in sorted_columns.iter().enumerate() {
            if offset >= data.len() {
                warn!(
                    "V5CompressedLegacy: Ran out of data at column {} ('{}'), parsed {} cells",
                    col_idx,
                    column.name,
                    cells.len()
                );
                break;
            }

            log::debug!(
                "V5CompressedLegacy: Parsing column {} '{}' ({}) at offset {}",
                col_idx, column.name, column.data_type, offset
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
                        "V5CompressedLegacy:   ✗ Failed to parse '{}' at offset {}: {}",
                        column.name, offset, e
                    );
                    // Show hex for debugging
                    let dump_len = std::cmp::min(32, data.len() - offset);
                    log::debug!(
                        "V5CompressedLegacy:     Hex: {}",
                        hex::encode(&data[offset..offset + dump_len])
                    );
                    // Stop on first error
                    break;
                }
            }
        }

        debug!("V5CompressedLegacy: Parsed total of {} cells", cells.len());

        Ok(cells)
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
                // Decimal: [0x08][u8 total_len][i32 scale][unscaled bytes]
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
