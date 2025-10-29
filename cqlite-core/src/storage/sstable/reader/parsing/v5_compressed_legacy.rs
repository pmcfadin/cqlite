//! Parser for Cassandra 5.0 V5CompressedLegacy decompressed blocks
//!
//! This format uses **u8 length prefixes** (NOT VInt) for partition keys and strings,
//! and simplified encoding optimized for compression. This differs from the newer
//! V5_0NewBig and V5_0Bti formats which use pure VInt encoding.
//!
//! ## Partition Key Size Constraints
//!
//! **Apache Cassandra Specification**: Partition keys can be up to 64KB (65536 bytes).
//! **V5CompressedLegacy Format Limitation**: Uses u8 for key length field, limiting keys to 255 bytes max.
//!
//! This means V5CompressedLegacy format cannot represent partition keys larger than 255 bytes,
//! even though Cassandra allows keys up to 64KB. Tables with larger keys would use a different format.
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
//! │  ├─ Row header (flags, timestamp, row_size)
//! │  ├─ Cells:
//! │  │  ├─ Type tag or flags (u8)
//! │  │  ├─ Column name length (u8)
//! │  │  ├─ Column name bytes
//! │  │  ├─ Value length (varies)
//! │  │  └─ Value bytes
//! │  └─ Trailing 4-byte field (NOT included in row_size)
//! └─ [Next partition or end of block]
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

/// Size of the trailing field after each row's cell data in V5CompressedLegacy format.
/// This field is NOT included in the row_size value from the row header.
const ROW_TRAILING_FIELD_SIZE: usize = 4;

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

    /// Try to parse partition header at offset WITHOUT consuming it.
    ///
    /// This performs a full parse attempt to determine if the bytes at offset
    /// represent a valid partition header. This is the NO-HEURISTICS approach:
    /// we actually try to parse the structure instead of guessing based on byte patterns.
    ///
    /// # Arguments
    /// * `data` - Binary data buffer
    /// * `offset` - Offset to check
    ///
    /// # Returns
    /// * `true` if a valid partition header can be parsed at this offset
    /// * `false` if parsing fails (likely a row header or invalid data)
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition boundary detection
    #[doc(hidden)]
    pub fn peek_is_partition_header(&self, data: &[u8], offset: usize) -> bool {
        // Try to actually parse the partition header
        self.parse_partition_header(data, offset).is_ok()
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

        // Cassandra partition key size limits (used in header validation)
        // - CASSANDRA_MAX_KEY_SIZE: 64KB limit per Apache Cassandra specification
        // - FORMAT_MAX_KEY_SIZE: u8 max value - V5CompressedLegacy format limitation
        const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
        const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 max value - format limitation

        // Parse ALL partitions in block (Issue #2 fix: previously only parsed one partition)
        let mut partition_index = 0;
        let mut skipped_partitions = 0;
        while offset < data.len() {
            log::debug!(
                "V5CompressedLegacy: === PARTITION {} at offset {} (block size: {}) ===",
                partition_index,
                offset,
                data.len()
            );

            // CRITICAL FIX (Issue #164): Validate partition header format before attempting parse
            //
            // Most compressed blocks contain EXACTLY ONE partition. After parsing the first
            // partition's row data and trailing VInt, we should NOT assume there's another
            // partition just because offset < data.len().
            //
            // Partition header format validation:
            // - Byte 0: Flags (typically 0x00, sometimes has partition-level flags)
            // - Byte 1: Partition key length (u8, typically 16 for UUID)
            // - Bytes 2+: Partition key data
            //
            // If we don't see a valid partition header structure, we've reached the end
            // of partitions in this block (remaining bytes are likely padding or metadata).
            if offset >= data.len() {
                break; // End of block
            }

            // Check if this looks like a partition header (flags byte + reasonable key length)
            // Partition keys can be up to 64KB per Cassandra spec (composite keys, text, etc.)
            if offset + 2 > data.len() {
                log::debug!(
                    "V5CompressedLegacy: Not enough bytes for partition header at offset {} (need 2, have {}), stopping",
                    offset,
                    data.len() - offset
                );
                break;
            }

            let flags = data[offset];
            let key_len = data[offset + 1] as usize;

            // Validate partition header:
            // - Flags should be 0x00 or have partition-level flags (typically < 0x20)
            // - Key length must be non-zero and within format's limit (u8 max = 255 bytes)
            //   Note: Cassandra spec allows 64KB keys, but V5CompressedLegacy format uses u8 length
            // - Must have enough bytes for: flags(1) + len(1) + key(len) + del_time(4) + unknown(8)
            let header_min_size = 1 + 1 + key_len + 4 + 8;
            if flags > 0x20
                || key_len == 0
                || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE)
                || offset + header_min_size > data.len()
            {
                log::warn!(
                    "V5CompressedLegacy: Skipping malformed partition header at offset {} \
                     (flags=0x{:02x}, key_len={}, need {} bytes, have {}, partition={}): header validation failed",
                    offset,
                    flags,
                    key_len,
                    header_min_size,
                    data.len() - offset,
                    partition_index
                );
                // Try to skip to next potential partition boundary
                skipped_partitions += 1;
                offset += 1; // Minimal forward progress to avoid infinite loop
                continue; // Skip this partition, try next
            }

            // Try to parse partition header
            match self.parse_partition_header(data, offset) {
                Ok((partition_key, new_offset)) => {
                    let header_size = new_offset - offset;
                    offset = new_offset;

                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Parsed partition key: {} bytes (header consumed {} bytes, now at offset {})",
                        partition_index,
                        partition_key.0.len(),
                        header_size,
                        offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Row data starts at offset {}, remaining: {} bytes",
                        partition_index,
                        offset,
                        data.len() - offset
                    );
                    log::debug!(
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
                    // Each row starts with a row header (flags > 0x20), while partition headers have flags <= 0x20.
                    // We parse rows in a loop until we encounter:
                    // - End of block (offset >= data.len())
                    // - Next partition header (flags <= 0x20)
                    // - Parse error (invalid row data)
                    let mut row_count = 0;
                    loop {
                        match self.parse_row_data_with_offset(data, offset, Some(schema), reader) {
                            Ok((cells, row_header_opt, next_offset)) => {
                                // Update offset to point to the next row or partition
                                offset = next_offset;
                                row_count += 1;

                                log::debug!(
                                    "V5CompressedLegacy: Partition {} Row {} - Parsed {} cells, now at offset {}",
                                    partition_index,
                                    row_count,
                                    cells.len(),
                                    offset
                                );

                                if let Some(ref header) = row_header_opt {
                                    log::debug!(
                                        "V5CompressedLegacy: Row {} metadata - timestamp={:?}, ttl={:?}, deletion={:?}",
                                        row_count,
                                        header.timestamp, header.ttl, header.local_deletion_time
                                    );
                                }

                                debug!(
                                    "V5CompressedLegacy: Parsed {} cells from row {}",
                                    cells.len(),
                                    row_count
                                );

                                // Convert cells HashMap to Value::Map (required by SelectExecutor)
                                let row_value = if cells.is_empty() {
                                    warn!(
                                        "V5CompressedLegacy: No cells extracted for {}.{} partition {} row {} (partition key: {} bytes)",
                                        self.keyspace,
                                        self.table_name,
                                        partition_index,
                                        row_count,
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

                                results.push((table_id.clone(), partition_key.clone(), row_value));

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
                                    log::error!(
                                        "V5CompressedLegacy: Partition {} - Failed to parse first row at offset {}: {}",
                                        partition_index, offset, e
                                    );
                                }
                                break; // End of valid data in partition
                            }
                        }
                    }

                    partition_index += 1;
                }
                Err(e) => {
                    log::warn!(
                        "V5CompressedLegacy: Failed to parse partition header at offset {} \
                         (partition={}): {}. Attempting to continue to next partition.",
                        offset,
                        partition_index,
                        e
                    );
                    // Try to skip forward to find next partition
                    skipped_partitions += 1;
                    offset += 1;
                    continue; // Skip this partition, try next
                }
            }
        }

        if skipped_partitions > 0 {
            log::warn!(
                "V5CompressedLegacy: Successfully parsed {} entries, skipped {} malformed partitions",
                results.len(),
                skipped_partitions
            );
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
    /// [row_size: VInt] ← CRITICAL: Total bytes for this row (header + cells)
    /// [prev_size: VInt]
    /// [timestamp: VInt if 0x04 set] ← Delta from min_timestamp
    /// [ttl: VInt if 0x08 set] ← Delta from min_ttl
    /// [deletion: 2 VInts if 0x10 set] ← First is delta from min_local_deletion_time
    /// [column_bitmap: VInt + bytes if NOT 0x20]
    /// ```
    ///
    /// Returns RowHeader with decoded metadata, calculated header_size, and row_size.
    fn parse_row_header(&self, data: &[u8], offset: usize) -> Result<(RowHeader, u64)> {
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

        // Read row size (VInt) - CRITICAL for partition boundary detection!
        debug!(
            "V5CompressedLegacy: Parsing row_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, row_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse row size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let bytes_consumed = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: row_size={}, consumed {} bytes, pos before={}, pos after={}",
            row_size,
            bytes_consumed,
            pos,
            pos + bytes_consumed
        );
        pos += bytes_consumed;

        // Read prev size (VInt)
        debug!(
            "V5CompressedLegacy: Parsing prev_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let bytes_consumed = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: prev_size={}, consumed {} bytes, pos before={}, pos after={}",
            _prev_size,
            bytes_consumed,
            pos,
            pos + bytes_consumed
        );
        pos += bytes_consumed;

        // Read timestamp if HAS_TIMESTAMP flag is set
        let timestamp = if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
            let (remaining, delta) = parse_vint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse timestamp delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

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
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

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
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Second VInt is deletion timestamp (we can skip for now)
            let (remaining, _deletion_timestamp) = parse_vint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse deletion timestamp at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

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
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

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
            "V5CompressedLegacy: Row header parsing complete: offset_start={}, pos_end={}, header_size={} bytes, row_size={} bytes (total row including cells), timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, pos, header_size, row_size, timestamp, ttl, local_deletion_time
        );

        Ok((
            RowHeader {
                timestamp,
                ttl,
                local_deletion_time,
                header_size,
            },
            row_size,
        ))
    }

    /// Parse partition header (flags, key, deletion time)
    ///
    /// # Format
    /// ```text
    /// [flags: u8][key_len: u8][key_bytes: [u8; key_len]][del_time: i32][unknown: 8 bytes]
    /// ```
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition header parsing
    #[doc(hidden)]
    pub fn parse_partition_header(
        &self,
        data: &[u8],
        mut offset: usize,
    ) -> Result<(RowKey, usize)> {
        let start_offset = offset;

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

        debug!(
            "V5CompressedLegacy: Parsed partition header at offset {}, consumed {} bytes",
            start_offset,
            offset - start_offset
        );

        Ok((row_key, offset))
    }

    /// Parse clustering prefix section (between row header and cells)
    ///
    /// The clustering prefix encodes clustering key values using a compact VInt header
    /// with 2 bits per clustering column to indicate value state.
    ///
    /// # Format
    /// ```text
    /// [prefix_header: VInt] ← 2 bits per clustering column
    ///   - 00 = null
    ///   - 01 = empty
    ///   - 10/11 = has value
    /// [value_1: bytes if present]
    /// [value_2: bytes if present]
    /// [... more values ...]
    /// ```
    ///
    /// Returns: (clustering_values, new_offset)
    fn parse_clustering_prefix(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, usize)> {
        // If no clustering keys, skip this section
        if schema.clustering_keys.is_empty() {
            log::debug!(
                "V5CompressedLegacy: No clustering keys in schema, skipping clustering prefix"
            );
            return Ok((Vec::new(), offset));
        }

        log::debug!(
            "V5CompressedLegacy: Parsing clustering prefix at offset {} for {} clustering keys",
            offset,
            schema.clustering_keys.len()
        );

        // Read header VInt (2 bits per clustering column)
        let (remaining, header_vint) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse clustering prefix header VInt at offset {}: {:?}",
                offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        log::debug!(
            "V5CompressedLegacy: Clustering prefix header = 0x{:x}, consumed {} bytes",
            header_vint,
            bytes_consumed
        );

        // Decode each clustering value based on 2-bit state
        let mut clustering_values = Vec::new();
        for (i, col) in schema.clustering_keys.iter().enumerate() {
            let state = (header_vint >> (i * 2)) & 0x03;
            log::debug!(
                "V5CompressedLegacy: Clustering key {} '{}' state = {} (from bits {}..{})",
                i,
                col.name,
                state,
                i * 2,
                i * 2 + 1
            );

            match state {
                0 => {
                    // NULL
                    clustering_values.push(Value::Null);
                    log::debug!("V5CompressedLegacy:   -> NULL");
                }
                1 => {
                    // EMPTY
                    clustering_values.push(Value::Text(String::new()));
                    log::debug!("V5CompressedLegacy:   -> EMPTY");
                }
                2 | 3 => {
                    // PRESENT - parse value based on type
                    let (value, new_off) = self.parse_clustering_value(data, offset, col)?;
                    log::debug!(
                        "V5CompressedLegacy:   -> {:?} (consumed {} bytes)",
                        value,
                        new_off - offset
                    );
                    clustering_values.push(value);
                    offset = new_off;
                }
                _ => unreachable!(),
            }
        }

        log::debug!(
            "V5CompressedLegacy: Parsed {} clustering values, new offset = {}",
            clustering_values.len(),
            offset
        );

        Ok((clustering_values, offset))
    }

    /// Parse individual clustering value (type-specific)
    ///
    /// Clustering values are encoded based on their CQL type. This handles the most
    /// common clustering key types: timestamp, text, int, uuid.
    ///
    /// Returns: (value, new_offset)
    fn parse_clustering_value(
        &self,
        data: &[u8],
        offset: usize,
        col: &crate::schema::ClusteringColumn,
    ) -> Result<(Value, usize)> {
        let normalized = col.data_type.to_lowercase();
        log::debug!(
            "V5CompressedLegacy: Parsing clustering value '{}' type '{}' at offset {}",
            col.name,
            normalized,
            offset
        );

        match normalized.as_str() {
            "timestamp" | "reversedtype(timestamptype)" => {
                // Fixed 8-byte timestamp (big-endian i64)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 8 bytes for timestamp, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }
                let ts = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                Ok((Value::Timestamp(ts), offset + 8))
            }

            "text" | "utf8type" | "varchar" => {
                // VInt length + UTF-8 bytes
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse text length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len_offset + len as usize > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need {} bytes for text, only {} available",
                        col.name,
                        len,
                        data.len() - len_offset
                    )));
                }

                let text = String::from_utf8(data[len_offset..len_offset + len as usize].to_vec())
                    .map_err(|e| {
                        Error::corruption(format!(
                            "V5CompressedLegacy: Clustering '{}': invalid UTF-8: {:?}",
                            col.name, e
                        ))
                    })?;
                Ok((Value::Text(text), len_offset + len as usize))
            }

            "int" => {
                // VInt length + i32 big-endian
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse int length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len != 4 {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': expected int length 4, got {}",
                        col.name, len
                    )));
                }

                if len_offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 4 bytes for int, only {} available",
                        col.name,
                        data.len() - len_offset
                    )));
                }

                let val = i32::from_be_bytes([
                    data[len_offset],
                    data[len_offset + 1],
                    data[len_offset + 2],
                    data[len_offset + 3],
                ]);
                Ok((Value::Integer(val), len_offset + 4))
            }

            "uuid" | "timeuuid" => {
                // VInt length + 16 UUID bytes
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse UUID length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len != 16 {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': expected UUID length 16, got {}",
                        col.name, len
                    )));
                }

                if len_offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 16 bytes for UUID, only {} available",
                        col.name,
                        data.len() - len_offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[len_offset..len_offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                Ok((Value::Uuid(uuid_bytes), len_offset + 16))
            }

            _ => {
                // For other types, read VInt length + skip that many bytes
                // Return as blob for now
                warn!(
                    "V5CompressedLegacy: Clustering '{}' has unsupported type '{}', treating as blob",
                    col.name, col.data_type
                );
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse blob length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len_offset + len as usize > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need {} bytes, only {} available",
                        col.name,
                        len,
                        data.len() - len_offset
                    )));
                }

                Ok((
                    Value::Blob(data[len_offset..len_offset + len as usize].to_vec()),
                    len_offset + len as usize,
                ))
            }
        }
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
        let input_offset = offset;
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
        let (row_header, row_size) = self.parse_row_header(data, offset)?;

        // CRITICAL VALIDATION: row_size must be reasonable
        //
        // In V5CompressedLegacy format, row_size should never exceed the block size (typically 16KB).
        // If row_size is unreasonably large, it indicates either:
        // 1. Partition tombstone or deletion marker (no actual row data)
        // 2. Format parsing error (landed at wrong offset)
        // 3. Corrupted data
        //
        // In all cases, we should skip this partition rather than panic.
        const MAX_REASONABLE_ROW_SIZE: u64 = 1_000_000; // 1MB max (very generous)
        if row_size > MAX_REASONABLE_ROW_SIZE {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Unreasonably large row_size={} at offset {} (max: {}). Likely partition tombstone or format error.",
                row_size,
                offset,
                MAX_REASONABLE_ROW_SIZE
            )));
        }

        // Row payloads can span multiple compressed chunks in V5CompressedLegacy format.
        // The reader has already stitched all chunks together (see get_all_entries()),
        // so row_size is valid across chunk boundaries. We MUST NOT validate against
        // individual chunk sizes as rows naturally span chunks in Cassandra's format.
        // This is NOT corruption - it's the intended file layout.

        log::debug!(
            "V5CompressedLegacy: Parsed row header at offset {}: header_size={} bytes, row_size={} bytes, timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, row_header.header_size, row_size, row_header.timestamp, row_header.ttl, row_header.local_deletion_time
        );

        // CRITICAL FIX (Issue #191, Phase 2): Row tombstone detection
        // If the row has deletion metadata (local_deletion_time is set), the entire row is deleted.
        // In this case, there are NO cell values to parse - the row_size includes ONLY the header.
        // Attempting to parse cells from a tombstoned row will read garbage data and fail.
        //
        // According to Cassandra 5.0 format:
        // - Deleted rows have ROW_HAS_DELETION flag (0x10) set
        // - Row header contains deletion time and deletion timestamp
        // - row_size = header_size (no cell data follows)
        // - Cell parsing must be skipped entirely
        if row_header.local_deletion_time.is_some() {
            log::debug!(
                "V5CompressedLegacy: Row is tombstoned (deletion_time={:?}), skipping cell parsing",
                row_header.local_deletion_time
            );

            // Calculate offset after row data (based on row_size from header)
            let after_row_offset = input_offset + row_size as usize;

            // Skip the trailing field to reach the next partition
            if after_row_offset + ROW_TRAILING_FIELD_SIZE > data.len() {
                let remaining = data.len().saturating_sub(after_row_offset);
                return Err(Error::corruption(format!(
                    "V5CompressedLegacy: Not enough bytes for {}-byte trailing field at offset {} (need {}, have {})",
                    ROW_TRAILING_FIELD_SIZE,
                    after_row_offset,
                    ROW_TRAILING_FIELD_SIZE,
                    remaining
                )));
            }

            let next_offset = after_row_offset + ROW_TRAILING_FIELD_SIZE;
            log::debug!(
                "V5CompressedLegacy: Skipped tombstoned row, next offset = {}",
                next_offset
            );

            // Return empty cells for tombstoned row
            return Ok((cells, Some(row_header), next_offset));
        }

        // Advance offset to start of clustering prefix / cell data
        debug!(
            "V5CompressedLegacy: BEFORE advancing offset: offset={}, row_header.header_size={}",
            offset, row_header.header_size
        );
        offset += row_header.header_size;
        debug!("V5CompressedLegacy: AFTER advancing offset: offset={}, data[offset]={:02x}, data[offset+1]={:02x}", offset, data[offset], data[offset+1]);

        // CRITICAL FIX: Parse clustering prefix BEFORE cell data
        // Clustering prefix encodes clustering key values between row header and cells
        let (clustering_values, new_offset) = self.parse_clustering_prefix(data, offset, schema)?;
        offset = new_offset;

        log::debug!(
            "V5CompressedLegacy: Parsed {} clustering values, cell data starts at offset {}",
            clustering_values.len(),
            offset
        );

        log::debug!(
            "V5CompressedLegacy: Cell data starts at offset {} (after {} byte header + clustering prefix), first 32 bytes: {}",
            offset,
            row_header.header_size,
            hex::encode(&data[offset..std::cmp::min(offset + 32, data.len())])
        );

        // Cell flags validation: First byte should be valid cell flags (0x00-0x1F)
        // Common flags: 0x00 (basic cell), 0x08 (USE_ROW_TIMESTAMP), 0x04 (HAS_EMPTY_VALUE)
        // Deleted cells have 0x01 (IS_DELETED), expiring cells have 0x02 (IS_EXPIRING)
        if offset < data.len() {
            let first_byte = data[offset];
            if first_byte <= 0x1F {
                debug!(
                    "V5CompressedLegacy: Valid cell flags 0x{:02x} at offset {} after row header",
                    first_byte, offset
                );
            } else {
                warn!(
                    "V5CompressedLegacy: Invalid cell flags 0x{:02x} at offset {} (expected 0x00-0x1F)",
                    first_byte, offset
                );
            }
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

        // CRITICAL FIX (Issue #164): Filter out partition keys and clustering keys!
        // The schema.columns list contains ALL columns (including keys), but cells
        // are only stored for REGULAR columns. Partition/clustering keys are part
        // of the row key and do NOT have cell data.
        let partition_key_names: std::collections::HashSet<_> = schema
            .partition_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        let clustering_key_names: std::collections::HashSet<_> = schema
            .clustering_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();

        // CRITICAL FIX (Issue #191): Use serialization header column order, not schema order
        // Cassandra 5.0 V5CompressedLegacy stores cells in the order defined by Statistics.db
        // serialization header (alphabetical by ColumnIdentifier/comparator), NOT CQL schema order.
        // We must iterate reader.header.columns directly to align binary layout with logical columns.
        let columns_in_order: Vec<_> = if !reader.header.columns.is_empty() {
            // Build lookup map from schema for column details
            let schema_map: HashMap<String, &crate::schema::Column> = schema
                .columns
                .iter()
                .map(|col| (col.name.clone(), col))
                .collect();

            // Iterate serialization header columns in exact order (skipping keys)
            reader
                .header
                .columns
                .iter()
                .filter(|col_info| !col_info.is_primary_key && !col_info.is_clustering)
                .filter_map(|col_info| schema_map.get(&col_info.name).copied())
                .collect()
        } else {
            // Fallback to schema order when header is empty (shouldn't happen for real SSTables)
            log::warn!("V5CompressedLegacy: reader.header.columns is empty, falling back to schema order (may cause column misalignment)");
            schema
                .columns
                .iter()
                .filter(|col| {
                    !partition_key_names.contains(col.name.as_str())
                        && !clustering_key_names.contains(col.name.as_str())
                })
                .collect()
        };

        log::debug!("V5CompressedLegacy: Parsing {} cells in SERIALIZATION HEADER ORDER starting at offset {} (row header was {} bytes)", columns_in_order.len(), offset, row_header.header_size);
        log::debug!(
            "V5CompressedLegacy: Column order: {:?}",
            columns_in_order.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
        log::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        for (col_idx, &column) in columns_in_order.iter().enumerate() {
            if offset >= data.len() {
                log::debug!(
                    "V5CompressedLegacy: Reached end of data at column {} ('{}'), parsed {}/{} cells",
                    col_idx,
                    column.name,
                    cells.len(),
                    columns_in_order.len()
                );
                break;
            }

            match self.parse_cell_value_schema_order(data, offset, column, reader) {
                Ok((value, new_offset)) => {
                    log::debug!(
                        "V5CompressedLegacy:   ✓ Column {} '{}' ({}) = {:?}, consumed {} bytes",
                        col_idx,
                        column.name,
                        column.data_type,
                        value,
                        new_offset - offset
                    );
                    cells.insert(column.name.clone(), value);
                    offset = new_offset;
                }
                Err(e) => {
                    log::debug!(
                        "V5CompressedLegacy:   ✗ Column {} '{}' ({}) at offset {} FAILED: {}",
                        col_idx,
                        column.name,
                        column.data_type,
                        offset,
                        e
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
        log::debug!(
            "V5CompressedLegacy: Cells HashMap keys: {:?}",
            cells.keys().collect::<Vec<_>>()
        );

        debug!("V5CompressedLegacy: Parsed total of {} cells", cells.len());

        // CRITICAL FINDING (Issue #164): V5CompressedLegacy format partition boundary calculation
        //
        // Analysis from JSONL reference data revealed that partitions are NOT contiguous based
        // solely on row_size. There is a 4-byte trailing field after each row that is NOT
        // included in the row_size value.
        //
        // Format structure (validated against real Cassandra 5.0 SSTables):
        //   [Partition Header: 30 bytes]
        //   [Row Header: variable, reported in header_size]
        //   [Row Cells: variable, row_size includes header + cells]
        //   [Trailing 4-byte field: NOT included in row_size]
        //   [Next Partition starts here]
        //
        // Example from simple_table:
        //   - Partition 1 at offset 30
        //   - Row size: 603 bytes
        //   - Trailing field: 4 bytes (offsets 633-637)
        //   - Partition 2 at offset 637 (not 633!)
        //
        // The 4-byte trailing field appears to be a partition/row boundary marker or metadata.
        // Its exact semantics are unclear from Cassandra source, but it's consistently present
        // and must be skipped to find the next partition.

        // Calculate offset after cell data (based on row_size from header)
        let after_cells_offset = input_offset + row_size as usize;

        // Skip the trailing field to reach the next partition
        if after_cells_offset + ROW_TRAILING_FIELD_SIZE > data.len() {
            let remaining = data.len().saturating_sub(after_cells_offset);
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Not enough bytes for {}-byte trailing field at offset {} (need {}, have {})",
                ROW_TRAILING_FIELD_SIZE,
                after_cells_offset,
                ROW_TRAILING_FIELD_SIZE,
                remaining
            )));
        }

        // Read the trailing field for debugging/validation purposes
        let trailing_bytes =
            &data[after_cells_offset..after_cells_offset + ROW_TRAILING_FIELD_SIZE];

        debug!(
            "V5CompressedLegacy: Row complete - row_size={} bytes, trailing field at offset {} = {:02x?}",
            row_size, after_cells_offset, trailing_bytes
        );

        // Calculate next offset AFTER the trailing field
        let next_offset = after_cells_offset + ROW_TRAILING_FIELD_SIZE;

        debug!(
            "V5CompressedLegacy: Calculated next partition offset: {} (row at {}, row_size={}, +4 trailing)",
            next_offset, input_offset, row_size
        );

        Ok((cells, Some(row_header), next_offset))
    }

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
    /// Returns: (value, new_offset)
    fn parse_cell_value_schema_order(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
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

        log::debug!(
            "V5CompressedLegacy: Cell '{}' flags=0x{:02x} (deleted={}, expiring={}, empty={}, use_row_ts={}, use_row_ttl={})",
            column.name, flags, is_deleted, is_expiring, has_empty_value, use_row_timestamp, use_row_ttl
        );

        // === PHASE 2: Parse conditional fields between flags and value ===
        // Based on Cassandra 5.0 Cell.Serializer format specification

        // Step 1: Read timestamp (if not using row timestamp)
        if !use_row_timestamp {
            let (remaining, timestamp_delta) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse timestamp delta as VInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            log::debug!(
                "V5CompressedLegacy: Cell '{}' timestamp_delta={} (min_timestamp={})",
                column.name,
                timestamp_delta,
                self.min_timestamp
            );
            // Note: actual timestamp = min_timestamp + timestamp_delta (from Statistics.db)
        }

        // Step 2: Read localDeletionTime (if deleted or expiring, and not using row TTL)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, deletion_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse localDeletionTime delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            log::debug!(
                "V5CompressedLegacy: Cell '{}' deletion_delta={} (min_local_deletion_time={})",
                column.name,
                deletion_delta,
                self.min_local_deletion_time
            );
            // Note: actual localDeletionTime = min_local_deletion_time + deletion_delta
        }

        // Step 3: Read TTL (if expiring and not using row TTL)
        if !use_row_ttl && is_expiring {
            let (remaining, ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse TTL delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            log::debug!(
                "V5CompressedLegacy: Cell '{}' ttl_delta={} (min_ttl={:?})",
                column.name,
                ttl_delta,
                self.min_ttl
            );
            // Note: actual TTL = min_ttl + ttl_delta (if min_ttl exists)
        }

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
        if is_deleted {
            log::debug!(
                "V5CompressedLegacy: Cell '{}' is tombstone (deleted), returning Null",
                column.name
            );
            // TODO(Issue #191, Phase 2): Parse deletion metadata (timestamp, localDeletionTime)
            // For now, skip to next cell by returning offset without advancing further
            return Ok((Value::Null, offset));
        }

        // Handle empty cells (no value bytes to read)
        if !has_value {
            log::debug!(
                "V5CompressedLegacy: Cell '{}' has HAS_EMPTY_VALUE flag, returning empty value",
                column.name
            );
            // Return appropriate empty value for type
            // For most types, empty = empty string or empty collection
            return Ok((Value::Text(String::new()), offset));
        }

        // At this point, we have a live cell with value data
        // The value parsing logic below is unchanged from the original implementation

        // Parse based on column type (data_type is a String with CQL type name)
        // CRITICAL: Normalize type name to lowercase for case-insensitive matching
        // Schema may provide "TEXT", "INT", etc. (uppercase) while match arms use lowercase
        let normalized_type = column.data_type.to_lowercase();
        let value = match normalized_type.as_str() {
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

            "text" | "varchar" | "ascii" => {
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

            "uuid" | "timeuuid" => {
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

            "decimal" => {
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

            "bigint" | "counter" => {
                // BigInt/Counter: fixed-width 8 bytes (no length prefix in Cassandra 5.0)
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
                if normalized_type == "counter" {
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

            "date" => {
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

            "duration" => {
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

                offset += duration_len;
                Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                }
            }

            "float" => {
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

            "smallint" | "short" => {
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

            "tinyint" | "byte" => {
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

            "time" => {
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

            "inet" => {
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

            // Default: treat as VInt-length-prefixed blob
            // CRITICAL: V5CompressedLegacy format uses VInt encoding for blob/bytes lengths,
            // NOT simple u8 length prefix. This allows blobs > 255 bytes.
            _ => {
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at blob length (type: {})",
                        column.name, column.data_type
                    )));
                }

                // Parse blob length as unsigned VInt (can be > 255 bytes)
                let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse blob length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let blob_len = blob_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

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

        let (row_header, row_size) = parser.parse_row_header(&data, 0).unwrap();

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

        // Verify row_size was parsed
        assert!(row_size > 0, "Row size should be positive");
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

        let (row_header, _row_size) = parser.parse_row_header(&data, 0).unwrap();

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

        let (row_header, _row_size) = result.unwrap();
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
