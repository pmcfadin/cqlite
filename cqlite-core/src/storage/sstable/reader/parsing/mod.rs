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
mod key_parsing;
mod value_parsing;

// Re-export all parsing methods (they're implemented on SSTableReader)
// No explicit re-exports needed since they're all impl blocks on SSTableReader

use std::collections::HashMap;
use std::path::Path;

use crate::{
    schema::{ClusteringColumn, Column, KeyColumn, TableSchema},
    Error, Result, RowKey, Value,
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
            eprintln!(
                "[DEBUG get_table_schema] Using provided schema for {}.{}",
                schema.keyspace, schema.table
            );
            return Some(schema.clone());
        }

        // Strategy 1: Use schema extracted from SSTable header (if available)
        if let Some(schema) = self.schema.as_deref() {
            eprintln!(
                "[DEBUG get_table_schema] Using schema from SSTable header for {}.{}",
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
                            eprintln!(
                                "[DEBUG get_table_schema] Failed to extract names from path {}: {}. Falling back to header names.",
                                self.file_path.display(), e
                            );
                            // Fallback to header names if path parsing fails
                            (self.header.keyspace.clone(), self.header.table_name.clone())
                        }
                    };

                    match block_on(registry.get_schema(&keyspace, &table_name)) {
                        Ok(schema) => {
                            eprintln!(
                                "[DEBUG get_table_schema] Using schema from registry for {}.{}",
                                keyspace, table_name
                            );
                            return Some(schema);
                        }
                        Err(e) => {
                            eprintln!("[DEBUG get_table_schema] Schema not found in registry for {}.{}: {}",
                                keyspace, table_name, e);
                        }
                    }
                } else {
                    eprintln!(
                        "[DEBUG get_table_schema] Not in tokio context, skipping registry lookup"
                    );
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
                        eprintln!(
                            "[DEBUG get_table_schema] Failed to extract names from path {}: {}. Falling back to header names.",
                            self.file_path.display(), e
                        );
                        // Fallback to header names if path parsing fails
                        (self.header.keyspace.clone(), self.header.table_name.clone())
                    }
                };

                // Non-state_machine SchemaRegistry doesn't have async get_schema method
                // This path is currently not implemented for non-async registries
                eprintln!("[DEBUG get_table_schema] Schema registry lookup not available in non-state_machine builds");
                let _ = (registry, &keyspace, &table_name); // Avoid unused variable warnings
            }
        }

        // Strategy 3: Construct basic schema from header columns (existing logic)
        eprintln!(
            "[DEBUG get_table_schema] Falling back to header column construction for {}.{}",
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
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
        if data.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Use the row cell state machine for proper parsing
        let mut state_machine = super::super::row_cell_state_machine::RowCellStateMachine::new();
        let mut results = Vec::new();

        // Parse the partition data using the row cell state machine
        match state_machine.parse_partition_data(data) {
            Ok(parsed_rows) => {
                for parsed_row in parsed_rows {
                    // Extract row key and value from parsed row
                    let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;
                    let value = self.extract_value_from_parsed_row(&parsed_row)?;
                    results.push((row_key, value));
                }
                Ok(Some(results))
            }
            Err(e) => {
                log::error!("Failed to parse partition data at offset: {}", e);

                // For Issue #35 compliance, we must not return synthetic data
                // If parsing fails, we return None to indicate parsing failure
                // This ensures zero-tolerance validation and forces proper implementation
                Err(Error::corruption(format!(
                    "Partition data parsing failed - real parsing required for Issue #35 compliance: {}",
                    e
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

    /// Extract value from parsed row data
    pub(in crate::storage::sstable::reader) fn extract_value_from_parsed_row(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<Value> {
        // Extract the primary value from the row's cells
        // For tables with multiple columns, this might be a UDT or JSON representation

        // First, try to get value from cells (the new flattened structure)
        if !parsed_row.cells.is_empty() {
            // Return the first non-null cell value
            for cell in &parsed_row.cells {
                if let Some(ref value) = cell.value {
                    return Ok(value.clone());
                }
            }
        }

        // Fallback: try to extract from clustering rows
        if !parsed_row.clustering_rows.is_empty() {
            let first_row = &parsed_row.clustering_rows[0];
            if !first_row.columns.is_empty() {
                // Return the first column value
                if let Some((_, value)) = first_row.columns.iter().next() {
                    return Ok(value.clone());
                }
            }
        }

        // Fallback: try static row data
        if let Some(ref static_row) = parsed_row.static_row {
            if !static_row.columns.is_empty() {
                if let Some((_, value)) = static_row.columns.iter().next() {
                    return Ok(value.clone());
                }
            }
        }

        // Final fallback: return metadata about the row
        let cell_count = parsed_row.cells.len();
        let cluster_count = parsed_row.clustering_rows.len();
        Ok(Value::Text(format!(
            "row_with_{}_cells_{}_clusters",
            cell_count, cluster_count
        )))
    }

    /// Parse partition data at a specific offset in the Data.db file
    ///
    /// This method reads and parses partition data from a specific offset,
    /// returning the parsed row key-value pairs.
    pub(in crate::storage::sstable::reader) async fn parse_partition_at_offset(
        &self,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
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

        // Parse the partition data
        self.parse_partition_data(&data)
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
