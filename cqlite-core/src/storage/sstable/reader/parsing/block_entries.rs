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
    /// Parse block entries with optional schema parameter
    ///
    /// This method provides schema-aware parsing when a schema is provided via the parameter.
    /// It delegates to the four-tier schema lookup strategy in `get_table_schema()`.
    ///
    /// # Arguments
    /// * `block_data` - Raw block data to parse
    /// * `schema` - Optional schema from query executor (highest priority in lookup chain)
    ///
    /// # Schema Resolution Strategy
    /// The schema parameter is passed to `get_table_schema()` which implements:
    /// 0. Provided schema (from this parameter) - highest priority
    /// 1. SSTable header schema (V5.0+ formats)
    /// 2. Schema registry lookup (external schema files)
    /// 3. Header-constructed fallback schema
    pub(in crate::storage::sstable::reader) fn parse_block_entries_with_schema(
        &self,
        block_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        // Pass the provided schema through to the parsing logic
        // This schema parameter flows through the call chain to get_table_schema()
        self.parse_block_entries(block_data, schema)
    }

    /// Parse block entries from decompressed block data
    pub(in crate::storage::sstable::reader) fn parse_block_entries(
        &self,
        block_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        log::debug!(
            "parse_block_entries: Starting parse (data size: {} bytes, version: {:?})",
            block_data.len(),
            self.header.cassandra_version
        );

        let mut entries = Vec::new();

        // Decompress block data if compression is enabled
        let data = if let Some(compression_reader) = &self.compression_reader {
            log::debug!(
                "parse_block_entries: Attempting block decompression with algorithm: {:?}",
                compression_reader.algorithm()
            );
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(block_data) {
                Ok(decompressed) => {
                    log::debug!(
                        "parse_block_entries: Block decompressed {} bytes to {} bytes",
                        block_data.len(),
                        decompressed.len()
                    );
                    decompressed
                }
                Err(e) => {
                    log::debug!("parse_block_entries: Block decompression failed ({}), parsing raw data instead. First 32 bytes: {:02x?}",
                        e, &block_data[..std::cmp::min(32, block_data.len())]);
                    // Fall back to raw data
                    block_data.to_vec()
                }
            }
        } else {
            log::debug!("parse_block_entries: No compression, using raw block data");
            block_data.to_vec()
        };

        // Determine parsing strategy based on data format classification
        // V5_0DataFormat and related formats use compressed 'nb' with legacy serialization (u8 lengths)
        // V5_0NewBig/Bti use true 'oa' format with VInt encoding
        let data_format = self.header.cassandra_version.data_format();

        log::debug!(
            "parse_block_entries: Format: {:?}, DataFormat: {:?}",
            self.header.cassandra_version,
            data_format
        );

        // Use state machine ONLY for true V5 uncompressed OA format (VInt encoding)
        let use_state_machine = matches!(
            data_format,
            crate::parser::header::DataFormat::V5UncompressedOA
        );

        log::debug!(
            "parse_block_entries: use_state_machine: {}",
            use_state_machine
        );

        if use_state_machine {
            log::debug!("parse_block_entries: Using state machine for true V5.0 'oa' format (VInt encoding)");

            // Log schema availability - NB format files may not have embedded schema
            if self.schema.is_some() {
                log::debug!(
                    "parse_block_entries: Schema available: {}.{}",
                    self.schema.as_ref().unwrap().keyspace,
                    self.schema.as_ref().unwrap().table
                );
            } else {
                log::debug!(
                    "[DEBUG SSTableReader::parse_block_entries] No schema in header for {:?}, will use basic state machine",
                    self.header.cassandra_version
                );
            }

            let result = self.parse_block_entries_with_state_machine(&data, schema);
            match &result {
                Ok(entries) => {
                    log::debug!(
                        "parse_block_entries: State machine returned {} entries",
                        entries.len()
                    );
                }
                Err(e) => {
                    log::debug!(
                        "[DEBUG SSTableReader::parse_block_entries] State machine failed: {}",
                        e
                    );
                }
            }
            return result;
        }

        // V5CompressedLegacy formats use dedicated parser (u8 length prefixes, not VInt)
        if matches!(
            data_format,
            crate::parser::header::DataFormat::V5CompressedLegacy
        ) {
            // Extract keyspace/table from path (most reliable for V5CompressedLegacy)
            // SSTable path format: {keyspace}/{table_name}-{uuid}/nb-1-big-Data.db
            let (keyspace, table_name) = super::extract_keyspace_table_from_path(&self.file_path)
                .unwrap_or_else(|_| {
                    // Fallback to header values if path extraction fails
                    (self.header.keyspace.clone(), self.header.table_name.clone())
                });

            log::debug!(
                "V5CompressedLegacy format detected, using dedicated parser for {}.{} (from path)",
                keyspace,
                table_name
            );

            // Validate metadata
            if keyspace.is_empty() || table_name.is_empty() {
                log::warn!(
                    "V5CompressedLegacy: keyspace/table extraction failed, falling back to legacy parser"
                );
            } else {
                // Use dedicated V5CompressedLegacy parser with EncodingStats from Statistics.db
                let table_id = TableId::from(format!("{}.{}", keyspace, table_name));

                // Extract EncodingStats from statistics_reader (if available)
                let (min_timestamp, min_local_deletion_time, min_ttl) = if let Some(stats_reader) =
                    &self.statistics_reader
                {
                    let ts_stats = &stats_reader.statistics().timestamp_stats;
                    (
                        ts_stats.min_timestamp,
                        ts_stats.min_deletion_time,
                        ts_stats.min_ttl,
                    )
                } else {
                    // No statistics reader - use zeros (may cause incorrect absolute values for delta-coded fields)
                    log::warn!("V5CompressedLegacy: No statistics_reader available, delta-coded timestamps/TTLs will use zero baseline");
                    (0, 0, None)
                };

                // Extract keyspace and table_name from table_id (format: "keyspace.table_name")
                let table_id_str = table_id.name();
                let (keyspace, table_name) =
                    table_id_str.split_once('.').unwrap_or(("", table_id_str));

                let parser = super::V5CompressedLegacyParser::new(
                    keyspace.to_string(),
                    table_name.to_string(),
                    min_timestamp,
                    min_local_deletion_time,
                    min_ttl,
                );

                // Get schema using four-tier lookup (provided -> header -> registry -> fallback)
                let table_schema = self.get_table_schema(schema);

                return parser.parse_block(&data, table_schema.as_ref(), self);
            }
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
                self.parse_composite_key(key_data, schema)?
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
                self.parse_column_value_enhanced(value_data, &table_id, &key, schema)?
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
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Starting");
        log::debug!(
            "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Data size: {} bytes",
            data.len()
        );

        let mut entries = Vec::new();
        let mut offset = 0;

        // Process multiple rows in the block
        while offset < data.len() {
            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Processing at offset {}/{}", offset, data.len());

            // Create state machine with schema information if available
            let has_schema = self.get_table_schema(schema).is_some();
            log::debug!(
                "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Has schema: {}",
                has_schema
            );

            let state_machine_result: Result<RowCellStateMachine> = if let Some(_schema) =
                self.get_table_schema(schema)
            {
                log::debug!(
                    "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Schema found"
                );
                // Modern formats should use SchemaAwareReader with proper comparators
                // NOTE: Only V5_0NewBig and V5_0Bti use true 'oa' format with VInt encoding
                // V5_0DataFormat uses compressed legacy format and should NOT reach this code path
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] True V5.0 'oa' format with VInt encoding");
                        // V5.0 true 'oa' formats: Use schema-aware state machine with partition key comparators
                        // These use VInt-encoded partition key component counts and lengths

                        // Use schema-aware state machine for V5.0 formats
                        match _schema.get_partition_key_comparators() {
                            Ok(comparators) if !comparators.is_empty() => {
                                log::debug!("Creating schema-aware state machine with {} partition key comparators", comparators.len());
                                // Use first comparator for now (composite keys handled internally)
                                Ok(RowCellStateMachine::with_schema(
                                    _schema.clone(),
                                    comparators[0].clone(),
                                ))
                            }
                            Ok(_) => {
                                log::debug!("Schema has no partition key comparators, using basic state machine");
                                Ok(RowCellStateMachine::new())
                            }
                            Err(e) => {
                                log::debug!("Failed to get partition key comparators: {}, using basic state machine", e);
                                Ok(RowCellStateMachine::new())
                            }
                        }
                    }
                    _ => {
                        // Legacy formats can use basic state machine as last resort
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format, using basic state machine");
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format but legacy-heuristics not enabled");
                            Err(Error::Schema(
                                "Basic state machine parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            } else {
                log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] No schema available from header");
                // No schema available from header - check format restrictions
                // NOTE: Only V5_0NewBig and V5_0Bti use true 'oa' format with VInt encoding
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] True V5.0 'oa' format without header schema");
                        // V5.0 true 'oa' format without header schema - use basic state machine
                        // Schema may be provided later by Database layer
                        // Note: These formats use VInt encoding and don't require legacy-heuristics
                        log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Using basic state machine for V5.0 'oa' format (no schema available)");
                        Ok(RowCellStateMachine::new())
                    }
                    _ => {
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format without schema, using basic state machine");
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] No schema and legacy-heuristics not enabled");
                            Err(Error::Schema(
                                "Schema-less parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            };

            log::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] State machine creation result: {}",
                      if state_machine_result.is_ok() { "OK" } else { "ERROR" });

            let mut _state_machine: RowCellStateMachine = state_machine_result?;

            // Process data starting from current offset
            let remaining_data = &data[offset..];
            match _state_machine.process(remaining_data) {
                Ok(consumed) => {
                    if consumed == 0 {
                        // No progress made, avoid infinite loop
                        log::warn!(
                            "State machine made no progress at offset {}, stopping",
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
                            log::debug!(
                                "Successfully parsed row with {} clustering rows",
                                parsed_row.clustering_rows.len()
                            );
                        }
                    } else if _state_machine.has_error() {
                        log::warn!(
                            "State machine error: {}",
                            _state_machine.error_message().unwrap_or("Unknown error")
                        );
                        // Try to continue with legacy parsing for this portion
                        break;
                    }

                    offset += consumed;
                }
                Err(e) => {
                    log::warn!("State machine processing error: {}", e);
                    // Fall back to legacy parsing
                    break;
                }
            }
        }

        // If state machine didn't handle all data, fall back to legacy parsing for remainder
        if offset < data.len() {
            log::debug!(
                "Falling back to legacy parsing for remaining {} bytes",
                data.len() - offset
            );
            let legacy_entries = self.parse_block_entries_legacy(&data[offset..], schema)?;
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
        schema: Option<&crate::schema::TableSchema>,
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
                self.parse_composite_key(key_data, schema)?
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
                self.parse_column_value_enhanced(value_data, &table_id, &key, schema)?
            };
            offset += value_len;

            entries.push((table_id, key, value));
        }

        Ok(entries)
    }
}
