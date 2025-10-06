//! Block entry parsing for SSTable data
//!
//! This module handles parsing of block entries from decompressed block data,
//! including modern Cassandra 5+ format parsing with state machine integration
//! and legacy format support.

use crate::{parser::vint::parse_vint_length, types::TableId, Error, Result, RowKey, Value};

use super::super::{
    super::{
        compression::Compression,
        row_cell_state_machine::{ParsedRow, RowCellStateMachine},
    },
    types::SSTableReader,
};

impl SSTableReader {
    /// Parse block entries from decompressed block data
    pub(in crate::storage::sstable::reader) fn parse_block_entries(
        &self,
        block_data: &[u8],
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(block_data) {
                Ok(decompressed) => {
                    println!(
                        "✅ Block decompressed {} bytes to {} bytes",
                        block_data.len(),
                        decompressed.len()
                    );
                    decompressed
                }
                Err(e) => {
                    println!(
                        "⚠️  Block decompression failed ({}), parsing raw data instead",
                        e
                    );
                    println!(
                        "First 32 bytes of block data: {:02x?}",
                        &block_data[..std::cmp::min(32, block_data.len())]
                    );
                    // Fall back to raw data
                    block_data.to_vec()
                }
            }
        } else {
            block_data.to_vec()
        };

        // Use the new state machine for Cassandra 5+ 'oa' format parsing
        if self.header.cassandra_version != crate::parser::header::CassandraVersion::Legacy {
            return self.parse_block_entries_with_state_machine(&data);
        }

        // Enhanced partition data parsing for legacy formats
        let mut offset = 0;
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse table ID with enhanced validation for binary IDs
            let table_id_bytes = &data[offset..offset + table_id_len];
            let table_id = match String::from_utf8(table_id_bytes.to_vec()) {
                Ok(s) => TableId::new(s),
                Err(_) => {
                    // Handle binary table IDs in Cassandra 5.0
                    let hex_id = hex::encode(table_id_bytes);
                    TableId::new(format!("binary_{}", hex_id))
                }
            };
            offset += table_id_len;

            // Enhanced row key parsing with Cassandra 5.0 format support
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse compound/composite keys properly
            let key_data = &data[offset..offset + key_len];
            let key = if key_len > 0 {
                self.parse_composite_key(key_data)?
            } else {
                RowKey::new(Vec::new()) // Empty key
            };
            offset += key_len;

            // Enhanced column data extraction with proper type handling
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings in Cassandra 5.0
            let value = if value_len == 0 {
                // Empty value
                Value::Null
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
                )));
            } else {
                let value_data = &data[offset..offset + value_len];
                self.parse_column_value_enhanced(value_data, &table_id, &key)?
            };
            offset += value_len;

            entries.push((table_id, key, value));
        }

        Ok(entries)
    }

    /// Parse block entries using the Cassandra 5 'oa' format state machine
    pub(in crate::storage::sstable::reader) fn parse_block_entries_with_state_machine(
        &self,
        data: &[u8],
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        println!("🔄 Using state machine for Cassandra 5+ 'oa' format parsing");

        // Process multiple rows in the block
        while offset < data.len() {
            // Create state machine with schema information if available
            let state_machine_result = if let Some(_schema) = self.get_table_schema() {
                // Modern formats should use SchemaAwareReader with proper comparators
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        Err(Error::Schema(format!(
                            "Modern format {:?} requires SchemaAwareReader with proper comparators - \
                             basic reader with blob fallback is not allowed.",
                            self.header.cassandra_version
                        )))
                    }
                    _ => {
                        // Legacy formats can use basic state machine as last resort
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            Err(Error::Schema(
                                "Basic state machine parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            } else {
                // No schema available - check format restrictions
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        Err(Error::Schema(format!(
                            "Schema is required for modern format {:?} - cannot use basic reader.",
                            self.header.cassandra_version
                        )))
                    }
                    _ => {
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            Err(Error::Schema(
                                "Schema-less parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            };

            let mut _state_machine: RowCellStateMachine = state_machine_result?;

            // Process data starting from current offset
            let remaining_data = &data[offset..];
            match _state_machine.process(remaining_data) {
                Ok(consumed) => {
                    if consumed == 0 {
                        // No progress made, avoid infinite loop
                        println!(
                            "⚠️  State machine made no progress at offset {}, stopping",
                            offset
                        );
                        break;
                    }

                    if _state_machine.is_complete() {
                        if let Some(parsed_row) = _state_machine.take_parsed_row() {
                            // Convert parsed row to entries
                            let converted_entries =
                                self.convert_parsed_row_to_entries(&parsed_row)?;
                            entries.extend(converted_entries);
                            println!(
                                "✅ Successfully parsed row with {} clustering rows",
                                parsed_row.clustering_rows.len()
                            );
                        }
                    } else if _state_machine.has_error() {
                        println!(
                            "❌ State machine error: {}",
                            _state_machine.error_message().unwrap_or("Unknown error")
                        );
                        // Try to continue with legacy parsing for this portion
                        break;
                    }

                    offset += consumed;
                }
                Err(e) => {
                    println!("❌ State machine processing error: {}", e);
                    // Fall back to legacy parsing
                    break;
                }
            }
        }

        // If state machine didn't handle all data, fall back to legacy parsing for remainder
        if offset < data.len() {
            println!(
                "🔄 Falling back to legacy parsing for remaining {} bytes",
                data.len() - offset
            );
            let legacy_entries = self.parse_block_entries_legacy(&data[offset..])?;
            entries.extend(legacy_entries);
        }

        Ok(entries)
    }

    /// Convert a parsed row from the state machine to entries
    pub(in crate::storage::sstable::reader) fn convert_parsed_row_to_entries(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();

        // Create table ID from keyspace and table name (would be better to get from header)
        let table_id = TableId::new(format!(
            "{}_{}",
            self.header.keyspace, self.header.table_name
        ));

        // Create partition key
        let _partition_key = RowKey::new(parsed_row.partition_key.key_bytes.clone());

        // Add static row if present
        if let Some(ref static_row) = parsed_row.static_row {
            for (column_name, value) in &static_row.columns {
                // Create a compound key for static columns
                let mut static_key_bytes = parsed_row.partition_key.key_bytes.clone();
                static_key_bytes.extend_from_slice(b"#static#");
                static_key_bytes.extend_from_slice(column_name.as_bytes());

                let static_key = RowKey::new(static_key_bytes);
                entries.push((table_id.clone(), static_key, value.clone()));
            }
        }

        // Add clustering rows
        for clustering_row in &parsed_row.clustering_rows {
            for (column_name, value) in &clustering_row.columns {
                // Create compound key: partition_key + clustering_key + column_name
                let mut compound_key_bytes = parsed_row.partition_key.key_bytes.clone();
                compound_key_bytes.extend_from_slice(&clustering_row.clustering_key);
                compound_key_bytes.extend_from_slice(column_name.as_bytes());

                let compound_key = RowKey::new(compound_key_bytes);
                entries.push((table_id.clone(), compound_key, value.clone()));
            }
        }

        Ok(entries)
    }

    /// Legacy parsing method for backward compatibility
    pub(in crate::storage::sstable::reader) fn parse_block_entries_legacy(
        &self,
        data: &[u8],
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Enhanced partition data parsing for legacy formats
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse table ID with enhanced validation for binary IDs
            let table_id_bytes = &data[offset..offset + table_id_len];
            let table_id = match String::from_utf8(table_id_bytes.to_vec()) {
                Ok(s) => TableId::new(s),
                Err(_) => {
                    // Handle binary table IDs in Cassandra 5.0
                    let hex_id = hex::encode(table_id_bytes);
                    TableId::new(format!("binary_{}", hex_id))
                }
            };
            offset += table_id_len;

            // Enhanced row key parsing with Cassandra 5.0 format support
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse compound/composite keys properly
            let key_data = &data[offset..offset + key_len];
            let key = if key_len > 0 {
                self.parse_composite_key(key_data)?
            } else {
                RowKey::new(Vec::new()) // Empty key
            };
            offset += key_len;

            // Enhanced column data extraction with proper type handling
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings
            let value = if value_len == 0 {
                // Empty value
                Value::Null
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
                )));
            } else {
                let value_data = &data[offset..offset + value_len];
                self.parse_column_value_enhanced(value_data, &table_id, &key)?
            };
            offset += value_len;

            entries.push((table_id, key, value));
        }

        Ok(entries)
    }
}
