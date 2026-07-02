//! Parsing logic for SSTable block entries and partition data
//!
//! This module contains all parsing functions for converting raw SSTable data
//! into typed entries. It handles:
//! - Block entry parsing (modern and legacy formats)
//! - Composite key parsing with schema awareness
//! - Value parsing using schema types and comparators
//! - Collection types (list, set, map, tuple, UDT)

// Sub-modules
mod block_entries;
pub(crate) mod byte_comparable; // Needs to be accessible from row_cell_state_machine
pub(crate) mod comparator_value_parsing; // Standalone comparator-based parsing for state machine
mod key_parsing;
mod v5_compressed_legacy;
mod value_parsing;

// Re-export all parsing methods (they're implemented on SSTableReader)
// No explicit re-exports needed since they're all impl blocks on SSTableReader

// Re-export V5CompressedLegacy parser for internal use
pub(in crate::storage::sstable::reader) use v5_compressed_legacy::V5CompressedLegacyParser;
// ComplexColumnMeta is used internally within v5_compressed_legacy.rs;
// delta_scan.rs accesses it via the parse_block_emit_delta closure type
// without needing an explicit re-export (Issue #700, DS4).

// Re-export the sliding-window parse outcome enum (issue #827) so the
// compaction-read streaming driver in data_access.rs can match on it.
pub(in crate::storage::sstable::reader) use v5_compressed_legacy::ParseStep;

// Re-export publicly for integration tests (Issue #166 regression tests)
// Using doc(hidden) to keep it out of public documentation but available for testing
#[doc(hidden)]
pub use v5_compressed_legacy::V5CompressedLegacyParser as PublicV5CompressedLegacyParser;

use std::collections::HashMap;
use std::path::Path;

use log::{debug, error, warn};

use crate::{
    schema::{ClusteringColumn, Column, KeyColumn, TableSchema},
    Error, Result, RowCells, RowKey, ScanRow, Value,
};

use super::{super::row_cell_state_machine::ParsedRow, types::SSTableReader};

/// Extract keyspace and table name from SSTable directory path
///
/// SSTable paths follow Cassandra convention:
/// `/path/to/sstables/{keyspace}/{table_name}-{uuid}/nb-1-big-Data.db`
///
/// # Arguments
/// * `path` - Path to the SSTable Data.db file
///
/// # Returns
/// * `Ok((keyspace, table_name))` - Extracted names
/// * `Err(Error::Schema)` - If path doesn't match expected format
///
/// # Examples
/// ```ignore
/// let path = Path::new("/data/test_basic/simple_table-abc/nb-1-big-Data.db");
/// let (keyspace, table) = extract_keyspace_table_from_path(path)?;
/// assert_eq!(keyspace, "test_basic");
/// assert_eq!(table, "simple_table");
/// ```
fn extract_keyspace_table_from_path(path: &Path) -> Result<(String, String)> {
    // Get parent directory containing table_name-uuid
    let table_dir = path
        .parent()
        .ok_or_else(|| Error::schema("SSTable path has no parent directory"))?;

    // Extract table directory name
    let table_dir_name = table_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::schema("Invalid table directory name"))?;

    // Split on last hyphen to handle table names containing hyphens
    // Format: "table_name-uuid" or "user-profiles-abc123"
    let table_name = table_dir_name
        .rsplit_once('-')
        .ok_or_else(|| {
            Error::schema(format!(
                "Table directory '{}' does not match 'tablename-uuid' format",
                table_dir_name
            ))
        })?
        .0
        .to_string();

    // Get keyspace directory (parent of table directory)
    let keyspace_dir = table_dir
        .parent()
        .ok_or_else(|| Error::schema("Table directory has no parent (keyspace) directory"))?;

    // Extract keyspace name
    let keyspace = keyspace_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::schema("Invalid keyspace directory name"))?
        .to_string();

    Ok((keyspace, table_name))
}

impl SSTableReader {
    /// Get table schema using four-tier lookup strategy
    ///
    /// This method implements a fallback chain for resolving table schemas:
    /// 0. **Provided Schema**: Use schema passed from query executor (highest priority)
    /// 1. **SSTable Header**: Check `self.schema` (extracted during SSTable opening from V5.0+ headers)
    /// 2. **Schema Registry**: Look up schema from external registry (loaded via --schema flag)
    /// 3. **Header Construction**: Build basic schema from header column metadata (fallback)
    pub(in crate::storage::sstable::reader) fn get_table_schema(
        &self,
        provided_schema: Option<&TableSchema>,
    ) -> Option<TableSchema> {
        // Strategy 0: Use provided schema from query executor (highest priority)
        if let Some(schema) = provided_schema {
            debug!(
                "get_table_schema: Using provided schema for {}.{}",
                schema.keyspace, schema.table
            );
            return Some(schema.clone());
        }

        // Strategy 1: Use schema extracted from SSTable header (if available)
        if let Some(schema) = self.schema.as_deref() {
            debug!(
                "get_table_schema: Using schema from SSTable header for {}.{}",
                self.header.keyspace, self.header.table_name
            );
            return Some(schema.clone());
        }

        // Strategy 2: Look up schema from schema registry (if available)
        #[cfg(feature = "state_machine")]
        {
            if let Some(registry_rwlock) = self.schema_registry.as_ref() {
                // We need to call async methods from a sync context.
                // Use futures::executor::block_on() which is safe here since this is
                // called from parsing contexts that are already in async contexts.
                if tokio::runtime::Handle::try_current().is_ok() {
                    // We're in a tokio context, use block_on
                    use futures::executor::block_on;

                    let registry = block_on(registry_rwlock.read());

                    // Extract keyspace/table from SSTable path (authoritative source)
                    // Directory structure: {keyspace}/{table_name}-{uuid}/Data.db
                    let (keyspace, table_name) = match extract_keyspace_table_from_path(
                        &self.file_path,
                    ) {
                        Ok(names) => names,
                        Err(e) => {
                            debug!(
                                "get_table_schema: Failed to extract names from path {}: {}. Falling back to header names.",
                                self.file_path.display(), e
                            );
                            // Fallback to header names if path parsing fails
                            (self.header.keyspace.clone(), self.header.table_name.clone())
                        }
                    };

                    match block_on(registry.get_schema(&keyspace, &table_name)) {
                        Ok(schema) => {
                            debug!(
                                "get_table_schema: Using schema from registry for {}.{}",
                                keyspace, table_name
                            );
                            return Some(schema);
                        }
                        Err(e) => {
                            debug!(
                                "get_table_schema: Schema not found in registry for {}.{}: {}",
                                keyspace, table_name, e
                            );
                        }
                    }
                } else {
                    debug!("get_table_schema: Not in tokio context, skipping registry lookup");
                }
            }
        }

        // For non-state_machine builds, schema_registry is Arc<SchemaRegistry> (not async)
        #[cfg(not(feature = "state_machine"))]
        {
            if let Some(registry) = self.schema_registry.as_ref() {
                // Extract keyspace/table from SSTable path (authoritative source)
                // Directory structure: {keyspace}/{table_name}-{uuid}/Data.db
                let (keyspace, table_name) = match extract_keyspace_table_from_path(&self.file_path)
                {
                    Ok(names) => names,
                    Err(e) => {
                        debug!(
                            "get_table_schema: Failed to extract names from path {}: {}. Falling back to header names.",
                            self.file_path.display(), e
                        );
                        // Fallback to header names if path parsing fails
                        (self.header.keyspace.clone(), self.header.table_name.clone())
                    }
                };

                // Non-state_machine SchemaRegistry doesn't have async get_schema method
                // This path is currently not implemented for non-async registries
                debug!("get_table_schema: Schema registry lookup not available in non-state_machine builds");
                let _ = (registry, &keyspace, &table_name); // Avoid unused variable warnings
            }
        }

        // Strategy 3: Construct basic schema from header columns (existing logic)
        debug!(
            "get_table_schema: Falling back to header column construction for {}.{}",
            self.header.keyspace, self.header.table_name
        );

        // Try to construct a basic schema from header information
        if self.header.columns.is_empty() {
            return None;
        }

        let mut columns = Vec::new();
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();

        // Convert header columns to schema columns
        for col_info in self.header.columns.iter() {
            let column = Column {
                name: col_info.name.clone(),
                data_type: col_info.column_type.clone(), // Use column_type field
                nullable: true,
                default: None,
                // Static-column classification comes from the Statistics.db
                // SerializationHeader (authoritative metadata) carried on
                // ColumnInfo.is_static. Issue #758 / Epic #756.
                is_static: col_info.is_static,
            };

            // Check if this is a key column based on primary key and clustering status
            if col_info.is_primary_key && !col_info.is_clustering {
                // This is a partition key
                partition_keys.push(KeyColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: partition_keys.len(),
                });
            } else if col_info.is_clustering {
                clustering_keys.push(ClusteringColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: clustering_keys.len(),
                    order: crate::schema::ClusteringOrder::Asc,
                });
            }

            columns.push(column);
        }

        Some(TableSchema {
            keyspace: self.header.keyspace.clone(),
            table: self.header.table_name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        })
    }

    /// Parse partition data from raw bytes using schema-driven approach
    ///
    /// This method uses the row cell state machine to parse partition data into
    /// individual row entries. It handles the SSTable row format including:
    /// - Row headers and metadata
    /// - Clustering key parsing
    /// - Cell value extraction
    /// - Tombstone handling
    pub(in crate::storage::sstable::reader) fn parse_partition_data(
        &self,
        data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        if data.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Create schema-aware state machine if schema is available
        let mut state_machine = if let Some(schema) = schema {
            // Get partition key comparators from schema
            match schema.get_partition_key_comparators() {
                Ok(comparators) if !comparators.is_empty() => {
                    debug!("parse_partition_data: Creating schema-aware state machine with {} partition key comparators", comparators.len());
                    super::super::row_cell_state_machine::RowCellStateMachine::with_schema_and_version(
                        schema.clone(),
                        comparators[0].clone(),
                        self.header.cassandra_version
                    )
                }
                Ok(_empty) => {
                    warn!(
                        "parse_partition_data: Schema for {}.{} has {} partition keys but comparator parsing returned empty - falling back to schemaless parsing",
                        schema.keyspace, schema.table, schema.partition_keys.len()
                    );
                    super::super::row_cell_state_machine::RowCellStateMachine::new()
                }
                Err(e) => {
                    warn!(
                        "parse_partition_data: Failed to get partition key comparators for {}.{}: {} - falling back to schemaless parsing",
                        schema.keyspace, schema.table, e
                    );
                    super::super::row_cell_state_machine::RowCellStateMachine::new()
                }
            }
        } else {
            debug!("parse_partition_data: No schema provided, using basic state machine");
            super::super::row_cell_state_machine::RowCellStateMachine::new()
        };
        let mut results = Vec::new();

        // Parse the partition data using the row cell state machine
        match state_machine.parse_partition_data(data) {
            Ok(parsed_rows) => {
                for parsed_row in parsed_rows {
                    // Extract row key and value from parsed row
                    let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;

                    // Use schema-aware extraction if schema is available
                    let value = if let Some(s) = schema {
                        // Use schema-aware extraction for proper typing
                        self.extract_value_from_parsed_row_with_schema(&parsed_row, s)?
                    } else {
                        // Fallback to basic extraction when no schema
                        self.extract_value_from_parsed_row_fallback(&parsed_row)?
                    };

                    results.push((row_key, value));
                }
                Ok(Some(results))
            }
            Err(e) => {
                let context = if let Some(s) = schema {
                    format!("table {}.{} with schema", s.keyspace, s.table)
                } else {
                    "unknown table without schema".to_string()
                };

                error!(
                    "parse_partition_data: Failed to parse partition data for {} (format {:?}): {}",
                    context, self.header.cassandra_version, e
                );

                // For Issue #35 compliance, we must not return synthetic data
                // If parsing fails, we return None to indicate parsing failure
                // This ensures zero-tolerance validation and forces proper implementation
                Err(Error::corruption(format!(
                    "Partition data parsing failed for {} (format {:?}): {}. \
                     Ensure schema is provided for Cassandra 5.0+ formats (Issue #35 compliance).",
                    context, self.header.cassandra_version, e
                )))
            }
        }
    }

    /// Extract row key from parsed row data
    pub(in crate::storage::sstable::reader) fn extract_row_key_from_parsed_row(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<RowKey> {
        // Extract the clustering key components from the parsed row
        // Combine partition key + clustering key to form the full row key

        // Use the row's clustering key if available
        if let Some(clustering_key) = &parsed_row.clustering_key {
            Ok(RowKey::from(clustering_key.clone()))
        } else if !parsed_row.clustering_rows.is_empty() {
            // Extract from first clustering row
            let first_clustering_row = &parsed_row.clustering_rows[0];
            let clustering_key_str = String::from_utf8_lossy(&first_clustering_row.clustering_key);
            Ok(RowKey::from(clustering_key_str.to_string()))
        } else {
            // Fallback: create synthetic key based on partition key
            let partition_key_str = String::from_utf8_lossy(&parsed_row.partition_key.key_bytes);
            Ok(RowKey::from(format!("partition_{}", partition_key_str)))
        }
    }

    /// Extract value from parsed row data (fallback for schemaless parsing)
    ///
    /// This method is used when no schema is available. It returns the first
    /// non-null cell value, which may be a blob.
    pub(in crate::storage::sstable::reader) fn extract_value_from_parsed_row_fallback(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<ScanRow> {
        use std::sync::Arc;

        // Issue #1334: assemble the single `ScanRow` row carrier the read path
        // consumes from whatever named cells the schema-less parse recovered. This
        // replaces the prior "return the first cell value" stand-in (which forced a
        // degenerate one-column row downstream); every recovered `(name, value)`
        // is carried instead.
        let mut row_cells: RowCells = Vec::new();
        for cell in &parsed_row.cells {
            if let Some(ref value) = cell.value {
                row_cells.push((Arc::from(cell.column_name.as_str()), value.clone()));
            }
        }
        for clustering_row in &parsed_row.clustering_rows {
            for (name, value) in &clustering_row.columns {
                row_cells.push((Arc::from(name.as_str()), value.clone()));
            }
        }
        if let Some(ref static_row) = parsed_row.static_row {
            for (name, value) in &static_row.columns {
                row_cells.push((Arc::from(name.as_str()), value.clone()));
            }
        }

        if row_cells.is_empty() {
            // Nothing decodable without a schema — a suppressed marker so no
            // synthetic row surfaces (the prior code emitted a placeholder text).
            return Ok(ScanRow::Marker(Value::Null));
        }

        row_cells.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        Ok(ScanRow::Row(row_cells))
    }

    /// Extract typed value from parsed row using schema information
    ///
    /// This method builds a complete row map with all columns properly typed
    /// according to the schema, instead of returning just the first cell as a blob.
    pub(in crate::storage::sstable::reader) fn extract_value_from_parsed_row_with_schema(
        &self,
        parsed_row: &ParsedRow,
        schema: &crate::schema::TableSchema,
    ) -> Result<ScanRow> {
        use std::collections::HashMap;

        let mut columns: HashMap<String, Value> = HashMap::new();

        debug!(
            "extract_value_with_schema: Processing row with {} cells",
            parsed_row.cells.len()
        );
        debug!(
            "extract_value_with_schema: Schema has {} partition keys, {} clustering keys, {} columns",
            schema.partition_keys.len(),
            schema.clustering_keys.len(),
            schema.columns.len()
        );

        // Process partition key components
        for (idx, component) in parsed_row.partition_key.components.iter().enumerate() {
            if let Some(pk_col) = schema.partition_keys.get(idx) {
                debug!(
                    "extract_value_with_schema: Processing partition key column: {}",
                    pk_col.name
                );

                // Parse the component bytes with schema type
                let typed_value =
                    self.parse_value_with_schema_type(component, &pk_col.data_type)
                        .unwrap_or_else(|e| {
                            warn!(
                                "extract_value_with_schema: Failed to parse partition key {}: {}, using blob fallback",
                                pk_col.name, e
                            );
                            Value::Blob(component.clone())
                        });

                columns.insert(pk_col.name.clone(), typed_value);
            }
        }

        // Process clustering key if present
        if let Some(ref clustering_key) = parsed_row.clustering_key {
            // The clustering_key is a String representation, we need to use the raw bytes
            // from partition_key components if they represent clustering
            // For now, we'll check if clustering_rows have data
            if !parsed_row.clustering_rows.is_empty() {
                // Extract clustering key from first clustering row
                let first_clustering_row = &parsed_row.clustering_rows[0];
                // clustering_key is Vec<u8>, parse it as composite if needed
                let ck_bytes = &first_clustering_row.clustering_key;

                // For single clustering key
                if schema.clustering_keys.len() == 1 {
                    let ck_col = &schema.clustering_keys[0];
                    debug!(
                        "extract_value_with_schema: Processing clustering key column: {}",
                        ck_col.name
                    );

                    let typed_value =
                        self.parse_value_with_schema_type(ck_bytes, &ck_col.data_type)
                            .unwrap_or_else(|e| {
                                warn!(
                                    "extract_value_with_schema: Failed to parse clustering key {}: {}, using blob fallback",
                                    ck_col.name, e
                                );
                                Value::Blob(ck_bytes.clone())
                            });

                    columns.insert(ck_col.name.clone(), typed_value);
                } else if schema.clustering_keys.len() > 1 {
                    // For composite clustering keys, we need to parse the composite structure
                    // This is a TODO for now - use blob fallback
                    warn!(
                        "extract_value_with_schema: Composite clustering keys not yet implemented for {}.{} ({} keys) - using string representation fallback",
                        schema.keyspace, schema.table, schema.clustering_keys.len()
                    );
                    for ck_col in &schema.clustering_keys {
                        columns.insert(ck_col.name.clone(), Value::Text(clustering_key.clone()));
                    }
                }
            } else {
                // No clustering rows, just use the string representation
                for ck_col in &schema.clustering_keys {
                    columns.insert(ck_col.name.clone(), Value::Text(clustering_key.clone()));
                }
            }
        }

        // Process regular columns from cells
        for cell in &parsed_row.cells {
            if let Some(col) = schema.columns.iter().find(|c| c.name == cell.column_name) {
                debug!(
                    "extract_value_with_schema: Processing regular column: {}",
                    cell.column_name
                );

                // Get value from cell - it's already parsed, but might be a blob
                if let Some(ref cell_value) = cell.value {
                    match cell_value {
                        Value::Blob(bytes) if !bytes.is_empty() => {
                            // Try to parse blob with schema type for better typing
                            let typed_value =
                                self.parse_value_with_schema_type(bytes, &col.data_type)
                                    .unwrap_or_else(|e| {
                                        debug!(
                                            "extract_value_with_schema: Failed to parse column {}: {}, keeping blob",
                                            cell.column_name, e
                                        );
                                        Value::Blob(bytes.clone())
                                    });
                            columns.insert(cell.column_name.clone(), typed_value);
                        }
                        _ => {
                            // Use the already-typed value from cell
                            columns.insert(cell.column_name.clone(), cell_value.clone());
                        }
                    }
                } else {
                    // Null value
                    columns.insert(cell.column_name.clone(), Value::Null);
                }
            }
        }

        // Also process clustering row columns if present
        for clustering_row in &parsed_row.clustering_rows {
            for (col_name, col_value) in &clustering_row.columns {
                if let Some(col) = schema.columns.iter().find(|c| c.name == *col_name) {
                    debug!(
                        "extract_value_with_schema: Processing clustering row column: {}",
                        col_name
                    );

                    match col_value {
                        Value::Blob(bytes) if !bytes.is_empty() => {
                            // Try to parse blob with schema type
                            let typed_value =
                                self.parse_value_with_schema_type(bytes, &col.data_type)
                                    .unwrap_or_else(|e| {
                                        debug!(
                                            "extract_value_with_schema: Failed to parse clustering row column {}: {}, keeping blob",
                                            col_name, e
                                        );
                                        Value::Blob(bytes.clone())
                                    });
                            columns.insert(col_name.clone(), typed_value);
                        }
                        _ => {
                            // Use the already-typed value
                            columns.insert(col_name.clone(), col_value.clone());
                        }
                    }
                }
            }
        }

        // Process static row columns if present
        if let Some(ref static_row) = parsed_row.static_row {
            for (col_name, col_value) in &static_row.columns {
                if let Some(col) = schema.columns.iter().find(|c| c.name == *col_name) {
                    debug!(
                        "extract_value_with_schema: Processing static row column: {}",
                        col_name
                    );

                    match col_value {
                        Value::Blob(bytes) if !bytes.is_empty() => {
                            // Try to parse blob with schema type
                            let typed_value =
                                self.parse_value_with_schema_type(bytes, &col.data_type)
                                    .unwrap_or_else(|e| {
                                        debug!(
                                            "extract_value_with_schema: Failed to parse static row column {}: {}, keeping blob",
                                            col_name, e
                                        );
                                        Value::Blob(bytes.clone())
                                    });
                            columns.insert(col_name.clone(), typed_value);
                        }
                        _ => {
                            // Use the already-typed value
                            columns.insert(col_name.clone(), col_value.clone());
                        }
                    }
                }
            }
        }

        // Validate we got at least some columns
        if columns.is_empty() {
            return Err(Error::Schema(format!(
                "No columns matched schema - parsed {} cells but none matched {} schema columns",
                parsed_row.cells.len(),
                schema.columns.len()
            )));
        }

        debug!(
            "extract_value_with_schema: Extracted {} columns into row map",
            columns.len()
        );

        // Issue #1334: return the single `ScanRow` row carrier the read path
        // consumes (previously this path returned a `Value::Udt` stand-in because
        // no row carrier existed). Names become interned `Arc<str>` handles; the
        // emit-time alphabetical ordering matches the other scan producers.
        let mut row_cells: RowCells = columns
            .into_iter()
            .map(|(name, value)| (std::sync::Arc::from(name.as_str()), value))
            .collect();
        row_cells.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        Ok(ScanRow::Row(row_cells))
    }

    /// Parse partition data at a specific offset in the Data.db file
    ///
    /// This method reads and parses partition data from a specific offset,
    /// returning the parsed row key-value pairs.
    pub(in crate::storage::sstable::reader) async fn parse_partition_at_offset(
        &self,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        // Read data from file at specified offset
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer).await?;
        drop(file); // Release lock early

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression =
                super::super::compression::Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(&buffer) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    log::warn!("Decompression failed at offset {}: {}", offset, e);
                    buffer // Fallback to raw data
                }
            }
        } else {
            buffer
        };

        // Parse the partition data with schema using four-tier lookup
        let table_schema = self.get_table_schema(None);
        self.parse_partition_data(&data, table_schema.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_extract_keyspace_table_standard_format() {
        // Standard Cassandra format
        let path = Path::new("/data/sstables/test_basic/simple_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }

    #[test]
    fn test_extract_keyspace_table_with_hyphens() {
        // Table name contains hyphens
        let path = Path::new("/data/sstables/my_keyspace/user-profiles-xyz789/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "my_keyspace");
        assert_eq!(table, "user-profiles");
    }

    #[test]
    fn test_extract_keyspace_table_real_test_data() {
        // Real path from test-data
        let path = Path::new("/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }

    #[test]
    fn test_extract_keyspace_table_collections() {
        // Collections table from test-data
        let path = Path::new("test-data/datasets/sstables/test_collections/collection_table-6b8c8fb0a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_collections");
        assert_eq!(table, "collection_table");
    }

    #[test]
    fn test_extract_keyspace_table_invalid_no_parent() {
        // Invalid path - no parent directory
        let path = Path::new("/Data.db");
        let result = extract_keyspace_table_from_path(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_keyspace_table_invalid_format() {
        // Invalid format - no hyphen in table directory
        let path = Path::new("/data/keyspace/tablename/Data.db");
        let result = extract_keyspace_table_from_path(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_keyspace_table_relative_path() {
        // Relative path (should work)
        let path = Path::new("test_basic/simple_table-abc123/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }
}
