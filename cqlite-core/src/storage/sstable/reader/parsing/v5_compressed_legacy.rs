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

use crate::{
    parser::vint::{parse_vint, parse_vuint},
    schema::TableSchema,
    types::TableId,
    Error, Result, RowKey, Value,
};

/// Row header data extracted from V5CompressedLegacy row
#[derive(Debug, Clone)]
struct RowHeader {
    /// Row-level timestamp (after delta decoding from min_timestamp)
    timestamp: Option<i64>,
    /// Row-level TTL (after delta decoding from min_ttl)
    ttl: Option<i32>,
    /// Row-level local deletion time (after delta decoding from min_local_deletion_time)
    local_deletion_time: Option<i32>,
    /// Number of bytes consumed by the header
    header_size: usize,
}

// Row header flag constants
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

/// Parser for V5CompressedLegacy format decompressed blocks
pub struct V5CompressedLegacyParser {
    keyspace: String,
    table_name: String,
    /// Minimum timestamp from Statistics.db for delta decoding
    min_timestamp: i64,
    /// Minimum local deletion time from Statistics.db for delta decoding
    min_local_deletion_time: i64,
    /// Minimum TTL from Statistics.db for delta decoding
    min_ttl: Option<i64>,
}

impl V5CompressedLegacyParser {
    /// Create a new V5CompressedLegacy parser
    ///
    /// # Arguments
    /// * `keyspace` - Keyspace name
    /// * `table_name` - Table name
    /// * `min_timestamp` - Minimum timestamp for delta decoding (from Statistics.db)
    /// * `min_local_deletion_time` - Minimum local deletion time for delta decoding (from Statistics.db)
    /// * `min_ttl` - Minimum TTL for delta decoding (from Statistics.db)
    pub fn new(
        keyspace: String,
        table_name: String,
        min_timestamp: i64,
        min_local_deletion_time: i64,
        min_ttl: Option<i64>,
    ) -> Self {
        Self {
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
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
                        Ok((cells, row_header, final_offset)) => {
                            offset = final_offset;

                            log::debug!(
                                "V5CompressedLegacy: Parsed {} cells from row data",
                                cells.len()
                            );

                            if let Some(ref header) = row_header {
                                log::debug!(
                                    "V5CompressedLegacy: Row metadata - timestamp={:?}, ttl={:?}, deletion={:?}",
                                    header.timestamp, header.ttl, header.local_deletion_time
                                );
                            }

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

    /// Parse row header with delta-decoded timestamps/TTL/deletion (Issue #162)
    ///
    /// # Format
    /// ```text
    /// [row_flags: u8]
    /// [extended_flags: u8 if 0x80 set]
    /// [row_size: VInt]
    /// [prev_size: VInt]
    /// [timestamp: VInt if 0x04 set] ← Delta from min_timestamp
    /// [ttl: VInt if 0x08 set] ← Delta from min_ttl
    /// [deletion: 2 VInts if 0x10 set] ← First is delta from min_local_deletion_time
    /// [column_bitmap: VInt + bytes if NOT 0x20]
    /// ```
    ///
    /// Returns RowHeader with decoded metadata and calculated header_size.
    fn parse_row_header(&self, data: &[u8], offset: usize) -> Result<RowHeader> {
        let mut pos = offset;

        // Read row flags
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading row flags",
            ));
        }
        let row_flags = data[pos];
        pos += 1;

        debug!(
            "V5CompressedLegacy: Row flags=0x{:02x} at offset {}",
            row_flags, offset
        );

        // Read extended flags if present
        if (row_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading extended flags",
                ));
            }
            let _extended_flags = data[pos];
            pos += 1;
        }

        // V5CompressedLegacy format header structure (Issue #162):
        // [row_flags: u8] [extended_flags: u8 if 0x80 set]
        // [row_size: VInt] [prev_size: VInt]
        // [timestamp: VInt if 0x04] [ttl: VInt if 0x08] [deletion: 2 VInts if 0x10]
        // [column_bitmap: VInt + bytes if NOT 0x20]
        //
        // Parse fields in order WITHOUT scanning for 0x08 marker.

        // Read row size (VInt)
        let (remaining, _row_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse row size at offset {}: {:?}",
                pos, e
            ))
        })?;
        pos = data.len() - remaining.len();

        // Read prev size (VInt)
        let (remaining, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev size at offset {}: {:?}",
                pos, e
            ))
        })?;
        pos = data.len() - remaining.len();

        // Read timestamp if HAS_TIMESTAMP flag is set
        let timestamp = if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
            let (remaining, delta) = parse_vint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse timestamp delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            pos = data.len() - remaining.len();

            // Apply delta decoding: absolute_timestamp = min_timestamp + delta
            let absolute_timestamp = self.min_timestamp.wrapping_add(delta);
            debug!(
                "V5CompressedLegacy: Row timestamp: delta={}, min={}, absolute={}",
                delta, self.min_timestamp, absolute_timestamp
            );
            Some(absolute_timestamp)
        } else {
            None
        };

        // Read TTL if HAS_TTL flag is set
        let ttl = if (row_flags & ROW_HAS_TTL) != 0 {
            let (remaining, delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse TTL delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            pos = data.len() - remaining.len();

            // Apply delta decoding: absolute_ttl = min_ttl + delta
            let absolute_ttl = if let Some(min_ttl) = self.min_ttl {
                min_ttl.wrapping_add(delta as i64) as i32
            } else {
                delta as i32
            };
            debug!(
                "V5CompressedLegacy: Row TTL: delta={}, min={:?}, absolute={}",
                delta, self.min_ttl, absolute_ttl
            );
            Some(absolute_ttl)
        } else {
            None
        };

        // Read deletion if HAS_DELETION flag is set
        let local_deletion_time = if (row_flags & ROW_HAS_DELETION) != 0 {
            // First VInt is local deletion time delta
            let (remaining, delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse deletion time delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            pos = data.len() - remaining.len();

            // Second VInt is deletion timestamp (we can skip for now)
            let (remaining, _deletion_timestamp) = parse_vint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse deletion timestamp at offset {}: {:?}",
                    pos, e
                ))
            })?;
            pos = data.len() - remaining.len();

            // Apply delta decoding: absolute_deletion_time = min_local_deletion_time + delta
            let absolute_deletion_time =
                self.min_local_deletion_time.wrapping_add(delta as i64) as i32;
            debug!(
                "V5CompressedLegacy: Row deletion time: delta={}, min={}, absolute={}",
                delta, self.min_local_deletion_time, absolute_deletion_time
            );
            Some(absolute_deletion_time)
        } else {
            None
        };

        // Parse and skip column bitmap if HAS_ALL_COLUMNS is NOT set
        if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
            // Column bitmap format: VInt column_count + (columns_in_row + 7) / 8 bytes of bitmap

            // Read column count (VInt)
            let (remaining, column_count) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse column count at offset {}: {:?}",
                    pos, e
                ))
            })?;
            pos = data.len() - remaining.len();

            // Calculate bitmap size in bytes: (column_count + 7) / 8
            let bitmap_bytes = column_count.div_ceil(8) as usize;

            if pos + bitmap_bytes > data.len() {
                return Err(Error::corruption(format!(
                    "V5CompressedLegacy: Not enough bytes for column bitmap at offset {} (need {} bytes, have {})",
                    pos, bitmap_bytes, data.len() - pos
                )));
            }

            // Skip the bitmap bytes
            pos += bitmap_bytes;

            debug!(
                "V5CompressedLegacy: Skipped column bitmap: {} columns, {} bitmap bytes",
                column_count, bitmap_bytes
            );
        }

        let header_size = pos - offset;
        debug!(
            "V5CompressedLegacy: Row header size={} bytes, timestamp={:?}, ttl={:?}, deletion={:?}",
            header_size, timestamp, ttl, local_deletion_time
        );

        Ok(RowHeader {
            timestamp,
            ttl,
            local_deletion_time,
            header_size,
        })
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
    /// Returns: (cells, row_header, new_offset)
    fn parse_row_data_with_offset(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<(HashMap<String, Value>, Option<RowHeader>, usize)> {
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

        // Parse row header with delta-decoded timestamps/TTL/deletion (Issue #162)
        let row_header = self.parse_row_header(data, offset)?;

        debug!(
            "V5CompressedLegacy: Parsed row header at offset {}: size={} bytes, timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, row_header.header_size, row_header.timestamp, row_header.ttl, row_header.local_deletion_time
        );

        // Advance offset to start of cell data
        offset += row_header.header_size;

        // Sanity check: verify we're at a cell marker (0x08) as expected
        if offset < data.len() && data[offset] == 0x08 {
            debug!(
                "V5CompressedLegacy: Verified cell marker (0x08) at offset {} after row header",
                offset
            );
        } else if offset < data.len() {
            warn!(
                "V5CompressedLegacy: Expected cell marker (0x08) at offset {}, found 0x{:02x}",
                offset, data[offset]
            );
        }

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

        log::debug!("V5CompressedLegacy: Parsing up to {} cells in SCHEMA DEFINITION ORDER starting at offset {} (row header was {} bytes)", columns_in_order.len(), offset, row_header.header_size);
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

        Ok((cells, Some(row_header), offset))
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

            // Complex types: frozen, tuple, UDT
            type_str if type_str.starts_with("frozen<") => {
                // Frozen types: unwrap inner type and parse
                let inner_type = self.extract_frozen_inner_type(type_str)?;

                // Create temporary column with unwrapped type
                let mut inner_column = column.clone();
                inner_column.data_type = inner_type.clone();

                // Recursively parse the inner type
                let (inner_value, new_offset) =
                    self.parse_cell_value_schema_order(data, offset, &inner_column, _reader)?;
                offset = new_offset;

                // Wrap in Frozen
                Value::Frozen(Box::new(inner_value))
            }

            type_str if type_str.starts_with("tuple<") => {
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
            type_str
                if type_str.starts_with("list<")
                    || type_str.starts_with("set<")
                    || type_str.starts_with("map<") =>
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

            // TODO(Issue #162): UDT parsing requires schema registry access
            // For now, UDTs fall through to blob. Future implementation will:
            // - Extract UDT name from type_str
            // - Look up UDT definition in schema registry
            // - Parse fields according to UDT schema
            // - Return Value::Udt(UdtValue)

            // Default: treat as length-prefixed blob
            _ => {
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at blob length (type: {})",
                        column.name, column.data_type
                    )));
                }
                let blob_len = data[offset] as usize;
                offset += 1;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for blob, only {} available (type: {})",
                        column.name,
                        blob_len,
                        data.len() - offset,
                        column.data_type
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }
        };

        Ok((value, offset))
    }

    /// Extract inner type from frozen<T> type string
    fn extract_frozen_inner_type(&self, type_str: &str) -> Result<String> {
        if !type_str.starts_with("frozen<") || !type_str.ends_with('>') {
            return Err(Error::schema(format!(
                "Invalid frozen type format: {}",
                type_str
            )));
        }

        let inner = &type_str[7..type_str.len() - 1];
        if inner.is_empty() {
            return Err(Error::schema(format!("Empty frozen type: {}", type_str)));
        }

        Ok(inner.to_string())
    }

    /// Parse tuple value from binary data
    /// Format: tuple elements are encoded sequentially according to their types
    fn parse_tuple_value(
        &self,
        data: &[u8],
        offset: &mut usize,
        type_str: &str,
        column: &crate::schema::Column,
        reader: &super::super::types::SSTableReader,
    ) -> Result<Value> {
        // Extract tuple element types from type string
        let element_types = self.extract_tuple_element_types(type_str)?;

        if element_types.is_empty() {
            return Err(Error::schema(format!("Empty tuple type: {}", type_str)));
        }

        let mut elements = Vec::new();

        // Parse each element according to its type
        for (idx, elem_type) in element_types.iter().enumerate() {
            // Create temporary column for this tuple element
            let mut elem_column = column.clone();
            elem_column.name = format!("{}[{}]", column.name, idx);
            elem_column.data_type = elem_type.clone();

            // Parse the element value recursively
            let (elem_value, new_offset) =
                self.parse_cell_value_schema_order(data, *offset, &elem_column, reader)?;

            *offset = new_offset;
            elements.push(elem_value);
        }

        Ok(Value::Tuple(elements))
    }

    /// Extract tuple element types from tuple<T1, T2, ...> string
    fn extract_tuple_element_types(&self, type_str: &str) -> Result<Vec<String>> {
        if !type_str.starts_with("tuple<") || !type_str.ends_with('>') {
            return Err(Error::schema(format!(
                "Invalid tuple type format: {}",
                type_str
            )));
        }

        let inner = &type_str[6..type_str.len() - 1];
        if inner.is_empty() {
            return Ok(Vec::new());
        }

        // Split by comma, handling nested angle brackets
        let mut types = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for ch in inner.chars() {
            match ch {
                '<' => {
                    depth += 1;
                    current.push(ch);
                }
                '>' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    types.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            types.push(current.trim().to_string());
        }

        Ok(types)
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

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,    // min_timestamp
            0,    // min_local_deletion_time
            None, // min_ttl
        );
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction
        assert_eq!(row_key.0.len(), 16); // UUID is 16 bytes

        // Verify offset consumed: 1 (flags) + 1 (len) + 16 (uuid) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(offset, 30);

        // Verify UUID bytes match
        let expected_uuid_bytes = hex::decode("15291a77d7394e738397b787442f3a1f").unwrap();
        assert_eq!(row_key.0, expected_uuid_bytes);
    }

    #[test]
    fn test_extract_frozen_inner_type() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test basic frozen type
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<list<int>>")
                .unwrap(),
            "list<int>"
        );

        // Test nested frozen
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<map<text,frozen<set<int>>>>")
                .unwrap(),
            "map<text,frozen<set<int>>>"
        );

        // Test error cases
        assert!(parser.extract_frozen_inner_type("frozen<>").is_err());
        assert!(parser.extract_frozen_inner_type("frozen").is_err());
        assert!(parser.extract_frozen_inner_type("list<int>").is_err());
    }

    #[test]
    fn test_extract_tuple_element_types() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test simple tuple
        let types = parser
            .extract_tuple_element_types("tuple<int,text,bigint>")
            .unwrap();
        assert_eq!(types, vec!["int", "text", "bigint"]);

        // Test tuple with nested collections
        let types = parser
            .extract_tuple_element_types("tuple<int,list<text>,map<text,int>>")
            .unwrap();
        assert_eq!(types, vec!["int", "list<text>", "map<text,int>"]);

        // Test tuple with frozen
        let types = parser
            .extract_tuple_element_types("tuple<int,frozen<list<int>>>")
            .unwrap();
        assert_eq!(types, vec!["int", "frozen<list<int>>"]);

        // Test empty tuple
        let types = parser.extract_tuple_element_types("tuple<>").unwrap();
        assert!(types.is_empty());

        // Test error cases
        assert!(parser.extract_tuple_element_types("tuple").is_err());
        assert!(parser.extract_tuple_element_types("int").is_err());
    }

    #[test]
    fn test_frozen_list_parsing() {
        // TODO(Issue #162): Add integration test with real SSTable data
        // For now, frozen types delegate to inner type parsing which is tested elsewhere
    }

    #[test]
    fn test_tuple_int_text_parsing() {
        // TODO(Issue #162): Add integration test with real SSTable data containing tuples
        // This would require:
        // 1. Real binary data with tuple encoding
        // 2. Schema definition with tuple column
        // 3. Expected parsed tuple values
    }

    #[test]
    fn test_non_zero_minima_delta_decoding() {
        // Test delta decoding with non-zero minima from ttl_test_table
        // Statistics.db shows:
        //   min_timestamp: 1759713125983682
        //   min_local_deletion_time: 1759799525
        //   min_ttl: 86400
        //
        // Row header format with HAS_TIMESTAMP (0x04) + HAS_TTL (0x08) + HAS_ALL_COLUMNS (0x20) = 0x2C
        // [row_flags: 0x2C] [row_size: VInt] [prev_size: VInt]
        // [timestamp_delta: VInt] [ttl_delta: VInt]
        // (NO column bitmap because HAS_ALL_COLUMNS is set)

        // Construct row header with flags 0x2C (HAS_TIMESTAMP | HAS_TTL | HAS_ALL_COLUMNS)
        // row_size=100 (encoded as 0x64), prev_size=0 (encoded as 0x00)
        // timestamp_delta=1000 (signed VInt: 0x87d0), ttl_delta=0 (unsigned VInt: 0x00)
        let row_header_hex = "2c640087d000"; // flags=0x2C, size=100, prev=0, ts_delta=1000, ttl_delta=0
        let data = hex::decode(row_header_hex).unwrap();

        let min_timestamp = 1759713125983682i64;
        let min_ttl = 86400i64;
        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "ttl_test_table".to_string(),
            min_timestamp,
            1759799525, // min_local_deletion_time
            Some(min_ttl),
        );

        let row_header = parser.parse_row_header(&data, 0).unwrap();

        // Verify delta decoding: absolute_timestamp = min_timestamp + delta
        assert_eq!(
            row_header.timestamp,
            Some(min_timestamp + 1000),
            "Timestamp should be decoded as min_timestamp + delta"
        );

        // Verify TTL delta decoding: absolute_ttl = min_ttl + delta
        assert_eq!(
            row_header.ttl,
            Some(min_ttl as i32),
            "TTL should be decoded as min_ttl + delta (delta=0)"
        );
    }

    #[test]
    fn test_row_header_with_deletion_time() {
        // Test delta decoding of local_deletion_time field
        // Row header with HAS_DELETION (0x10) + HAS_ALL_COLUMNS (0x20) = 0x30
        // [row_flags: 0x30] [row_size: VInt] [prev_size: VInt]
        // [local_deletion_time_delta: unsigned VInt] [deletion_time: signed VInt]

        let row_header_hex = "30640032645000"; // flags=0x30, size=100, prev=0, del_delta=50, del_time=80 (signed)
        let data = hex::decode(row_header_hex).unwrap();

        let min_local_deletion_time = 1759799525i64;
        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "test_table".to_string(),
            0,
            min_local_deletion_time,
            None,
        );

        let row_header = parser.parse_row_header(&data, 0).unwrap();

        // Verify delta decoding: absolute_deletion_time = min_local_deletion_time + delta
        assert_eq!(
            row_header.local_deletion_time,
            Some((min_local_deletion_time + 50) as i32),
            "Local deletion time should be decoded as min + delta"
        );
    }

    #[test]
    fn test_sparse_column_bitmap_parsing() {
        // Test column bitmap parsing when NOT HAS_ALL_COLUMNS
        // Row header WITHOUT HAS_ALL_COLUMNS flag (0x20)
        // Should parse column bitmap after metadata fields
        //
        // Row header format: [flags: 0x04] [row_size] [prev_size] [timestamp]
        // [column_bitmap_size: VInt] [column_bitmap_bytes]

        // Construct row with HAS_TIMESTAMP but NOT HAS_ALL_COLUMNS
        // bitmap_size=8 columns (0x08), bitmap=0b00000101 (columns 0 and 2 present)
        let row_header_hex = "046400000805"; // flags=0x04, size=100, prev=0, ts=0 (signed), col_count=8, bitmap=0x05
        let data = hex::decode(row_header_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "sparse_table".to_string(),
            0,
            0,
            None,
        );

        // This tests that parse_row_header handles column bitmap correctly
        // The bitmap parsing happens after the metadata fields
        let result = parser.parse_row_header(&data, 0);

        // Should succeed without panicking on bitmap parsing
        assert!(
            result.is_ok(),
            "Row header with column bitmap should parse successfully"
        );

        let row_header = result.unwrap();
        // Verify header was parsed (has timestamp)
        assert_eq!(row_header.timestamp, Some(0));

        // Verify header_size includes bitmap overhead
        // flags(1) + size(1) + prev(1) + timestamp(1) + column_count(1) + bitmap(1) = 6
        assert_eq!(
            row_header.header_size, 6,
            "Header size should include column bitmap"
        );
    }

    #[test]
    fn test_clustering_key_partition_header() {
        // Test partition header parsing for composite key table
        // composite_key_table has clustering columns: [ReversedType(TimestampType), UTF8Type]
        //
        // Partition header format:
        // [flags: u8] [key_len: u8] [partition_key_bytes] [deletion_time: i32] [unknown: i64]
        //
        // From composite_key_table JSONL:
        // partition key: "245dff69-026f-45c6-b68f-ba0c964df3c9"
        // clustering: ["2025-10-06 01:12:06.059Z","information"]
        //
        // Note: Clustering keys are part of row data, not partition header
        // This test verifies partition header parsing for composite key tables

        let partition_hex = "0010245dff69026f45c6b68fba0c964df3c97fffffff8000000000000000";
        let data = hex::decode(partition_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "composite_key_table".to_string(),
            1759713125977357, // min_timestamp from Statistics.db
            1442880000,       // min_local_deletion_time
            None,
        );

        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction (UUID is 16 bytes)
        assert_eq!(row_key.0.len(), 16);

        // Verify correct partition key bytes
        let expected_uuid_bytes = hex::decode("245dff69026f45c6b68fba0c964df3c9").unwrap();
        assert_eq!(row_key.0, expected_uuid_bytes);

        // Verify offset: flags(1) + len(1) + uuid(16) + del_time(4) + unknown(8) = 30
        assert_eq!(offset, 30);

        // Note: Clustering key parsing would happen during row data parsing,
        // which is tested separately in integration tests
    }
}
