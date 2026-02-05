//! SSTable export functionality for M5.2 (Issue #388)
//!
//! Provides the `export_sstable()` API for consolidating internal SSTables
//! into a single, Cassandra-compatible SSTable suitable for distribution.
//!
//! ## Export Flow
//!
//! 1. Flush memtable (if not empty)
//! 2. Full compaction (merge all L0 files)
//! 3. Copy to output with Cassandra naming
//! 4. Validate exported SSTable
//!
//! ## Naming Convention
//!
//! Exported files follow Cassandra's naming convention:
//! `nb-{gen}-big-{Component}.db`
//!
//! Files are organized in a directory structure: `{output_dir}/{keyspace}/{table}/`
//!
//! Example: `output/test_ks/users/nb-1-big-Data.db`

#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use crate::storage::sstable::directory::types::SSTableComponent;
#[cfg(feature = "write-support")]
use std::path::{Path, PathBuf};

/// Options for exporting an SSTable
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Keyspace name (for filename generation)
    pub keyspace: String,
    /// Table name (for filename generation)
    pub table: String,
    /// Generation number (for filename generation)
    pub generation: u64,
    /// Whether to perform compaction before export (default: true)
    pub compact_before_export: bool,
    /// Whether to validate the exported SSTable (default: true)
    pub validate_after_export: bool,
}

#[cfg(feature = "write-support")]
impl ExportOptions {
    /// Create new export options
    ///
    /// # Arguments
    ///
    /// * `keyspace` - Keyspace name
    /// * `table` - Table name
    /// * `generation` - Generation number
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let options = ExportOptions::new("test_ks", "users", 1);
    /// ```
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>, generation: u64) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
            generation,
            compact_before_export: true,
            validate_after_export: true,
        }
    }

    /// Disable compaction before export
    ///
    /// Use this if you know there's only one SSTable or want to export
    /// the raw internal files without merging.
    pub fn skip_compaction(mut self) -> Self {
        self.compact_before_export = false;
        self
    }

    /// Disable validation after export
    ///
    /// Use this to skip post-export validation checks. Not recommended
    /// for production use.
    pub fn skip_validation(mut self) -> Self {
        self.validate_after_export = false;
        self
    }
}

/// Report of an export operation
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct ExportReport {
    /// Output directory containing exported files
    pub output_path: PathBuf,
    /// Size of Data.db file in bytes
    pub data_file_size: u64,
    /// Size of Index.db file in bytes
    pub index_file_size: u64,
    /// Number of rows exported
    pub row_count: u64,
    /// Number of partitions exported
    pub partition_count: u64,
    /// List of all exported component files
    pub components: Vec<PathBuf>,
}

#[cfg(feature = "write-support")]
impl ExportReport {
    /// Total size of all exported files
    pub fn total_size(&self) -> u64 {
        self.components
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum()
    }

    /// Check if all expected components exist
    pub fn validate_components(&self) -> Result<()> {
        let required_components = [
            "Data.db",
            "Index.db",
            "Statistics.db",
            "Filter.db",
            "Summary.db",
            "Digest.crc32",
            "TOC.txt",
        ];

        for component in &required_components {
            let exists = self.components.iter().any(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.ends_with(component))
                    .unwrap_or(false)
            });

            if !exists {
                return Err(Error::Storage(format!(
                    "Missing required component: {}",
                    component
                )));
            }
        }

        Ok(())
    }
}

/// Build the Cassandra-style filename for a component
///
/// Format: `nb-{generation}-big-{component}`
#[cfg(feature = "write-support")]
fn build_cassandra_filename(generation: u64, component: &str) -> String {
    format!("nb-{}-big-{}", generation, component)
}

/// Export implementation methods (added to WriteEngine)
#[cfg(feature = "write-support")]
impl crate::storage::write_engine::WriteEngine {
    /// Export an SSTable suitable for distribution
    ///
    /// This method performs the following steps:
    /// 1. Flushes the memtable if not empty
    /// 2. Performs full compaction (if enabled) to merge all L0 files
    /// 3. Copies the resulting SSTable to the output directory with Cassandra naming
    /// 4. Validates the exported SSTable (if enabled)
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Directory where exported files will be written
    /// * `options` - Export configuration
    ///
    /// # Returns
    ///
    /// An `ExportReport` containing metadata about the export operation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - Flush fails
    /// - Compaction fails
    /// - File copy fails
    /// - Validation fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let options = ExportOptions::new("test_ks", "users", 1);
    /// let report = engine.export_sstable(Path::new("/export"), options).await?;
    /// println!("Exported {} partitions ({} bytes)", report.partition_count, report.total_size());
    /// ```
    pub async fn export_sstable(
        &mut self,
        output_dir: &Path,
        options: ExportOptions,
    ) -> Result<ExportReport> {
        use std::sync::atomic::Ordering;

        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        log::info!(
            "Starting SSTable export to {} with keyspace={}, table={}, generation={}",
            output_dir.display(),
            options.keyspace,
            options.table,
            options.generation
        );

        // Create keyspace/table directory structure
        let export_path = output_dir.join(&options.keyspace).join(&options.table);
        tokio::fs::create_dir_all(&export_path).await.map_err(|e| {
            Error::Storage(format!(
                "Failed to create export directory {:?}: {}",
                export_path, e
            ))
        })?;

        // Step 1: Flush memtable if not empty
        if !self.memtable.is_empty() {
            log::info!(
                "Flushing memtable before export ({} rows, {} bytes)",
                self.memtable_row_count(),
                self.memtable_size()
            );
            self.flush_internal_async().await?;
        } else {
            log::info!("Memtable is empty, skipping flush");
        }

        // Step 2: Full compaction (if enabled)
        let source_sstable = if options.compact_before_export {
            return Err(Error::InvalidInput(
                "compact_before_export is not yet implemented (requires M5.3 SSTable reader integration). \
                 Set compact_before_export=false or use maintenance_step() to compact first.".to_string()
            ));
        } else {
            log::info!("Skipping compaction, using most recent SSTable");
            self.find_most_recent_sstable().await?
        };

        // Step 3: Copy to output with Cassandra naming
        let mut exported_components = Vec::new();
        let mut data_file_size = 0u64;
        let mut index_file_size = 0u64;

        // Get the source generation number from the SSTable info
        let (source_generation, source_dir) = source_sstable;

        // List of components to copy
        let components_to_copy = [
            ("Data.db", SSTableComponent::Data),
            ("Index.db", SSTableComponent::Index),
            ("Statistics.db", SSTableComponent::Statistics),
            ("Filter.db", SSTableComponent::Filter),
            ("Summary.db", SSTableComponent::Summary),
            ("Digest.crc32", SSTableComponent::Digest),
            ("TOC.txt", SSTableComponent::TOC),
        ];

        for (component_name, _component_type) in &components_to_copy {
            // Build source filename with generation: nb-{gen}-big-{Component}.db
            let source_filename = format!("nb-{}-big-{}", source_generation, component_name);
            let source_path = source_dir.join(&source_filename);

            if !source_path.exists() {
                log::warn!(
                    "Component {} not found at {}, skipping",
                    component_name,
                    source_path.display()
                );
                continue;
            }

            let dest_filename = build_cassandra_filename(options.generation, component_name);
            let dest_path = export_path.join(&dest_filename);

            // Copy file
            tokio::fs::copy(&source_path, &dest_path)
                .await
                .map_err(|e| {
                    Error::Storage(format!(
                        "Failed to copy {} to {}: {}",
                        source_path.display(),
                        dest_path.display(),
                        e
                    ))
                })?;

            log::debug!(
                "Copied {} to {}",
                source_path.display(),
                dest_path.display()
            );

            // Track sizes
            if *component_name == "Data.db" {
                data_file_size = tokio::fs::metadata(&dest_path).await?.len();
            } else if *component_name == "Index.db" {
                index_file_size = tokio::fs::metadata(&dest_path).await?.len();
            }

            exported_components.push(dest_path);
        }

        // Step 4: Collect statistics from exported SSTable
        let (partition_count, row_count) =
            self.read_statistics_from_export(&exported_components)?;

        let report = ExportReport {
            output_path: export_path,
            data_file_size,
            index_file_size,
            row_count,
            partition_count,
            components: exported_components,
        };

        // Step 5: Validate exported SSTable (if enabled)
        if options.validate_after_export {
            log::info!("Validating exported SSTable");
            report.validate_components()?;
            log::info!("Validation passed");
        }

        log::info!(
            "Export complete: {} partitions, {} rows, {} total bytes",
            report.partition_count,
            report.row_count,
            report.total_size()
        );

        Ok(report)
    }

    /// Find the most recent SSTable in the data directory
    ///
    /// Returns a tuple of (generation number, directory path) for the most recent SSTable.
    async fn find_most_recent_sstable(&self) -> Result<(u64, PathBuf)> {
        // Find the highest generation number in data directory
        let mut max_generation = 0u64;
        let mut found = false;

        if !self.config.data_dir.exists() {
            return Err(Error::Storage(
                "Data directory does not exist (no SSTables to export)".to_string(),
            ));
        }

        let mut entries = tokio::fs::read_dir(&self.config.data_dir)
            .await
            .map_err(|e| Error::Storage(format!("Failed to read data directory: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read directory entry: {}", e)))?
        {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Parse generation from filename: nb-{generation}-big-{Component}.db
            if filename_str.starts_with("nb-") && filename_str.contains("-big-") {
                if let Some(gen_str) = filename_str
                    .strip_prefix("nb-")
                    .and_then(|s| s.split('-').next())
                {
                    if let Ok(gen) = gen_str.parse::<u64>() {
                        if gen > max_generation {
                            max_generation = gen;
                            found = true;
                        }
                    }
                }
            }
        }

        if !found {
            return Err(Error::Storage(
                "No SSTables found in data directory (nothing to export)".to_string(),
            ));
        }

        // Return the generation and data directory
        Ok((max_generation, self.config.data_dir.clone()))
    }

    /// Read partition and row counts from Statistics.db
    ///
    /// This is a simple extraction from the exported SSTable's Statistics.db file.
    fn read_statistics_from_export(&self, components: &[PathBuf]) -> Result<(u64, u64)> {
        // Find Statistics.db in exported components
        let stats_path = components
            .iter()
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.ends_with("Statistics.db"))
                    .unwrap_or(false)
            })
            .ok_or_else(|| Error::Storage("Statistics.db not found in export".to_string()))?;

        // Statistics.db parsing not yet implemented
        // Return placeholder values with warning
        log::warn!(
            "Statistics.db parsing not implemented; partition/row counts unavailable for {}",
            stats_path.display()
        );
        Ok((0, 0))
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use crate::types::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn create_test_mutation(id: i32, name: &str, timestamp: i64) -> Mutation {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }];

        Mutation::new(table_id, pk, None, ops, timestamp, None)
    }

    #[test]
    fn test_export_options_defaults() {
        let options = ExportOptions::new("test_ks", "users", 1);

        assert_eq!(options.keyspace, "test_ks");
        assert_eq!(options.table, "users");
        assert_eq!(options.generation, 1);
        assert!(options.compact_before_export);
        assert!(options.validate_after_export);
    }

    #[test]
    fn test_export_options_skip_compaction() {
        let options = ExportOptions::new("test_ks", "users", 1).skip_compaction();

        assert!(!options.compact_before_export);
        assert!(options.validate_after_export);
    }

    #[test]
    fn test_export_options_skip_validation() {
        let options = ExportOptions::new("test_ks", "users", 1).skip_validation();

        assert!(options.compact_before_export);
        assert!(!options.validate_after_export);
    }

    #[test]
    fn test_build_cassandra_filename() {
        let filename = build_cassandra_filename(1, "Data.db");
        assert_eq!(filename, "nb-1-big-Data.db");

        let filename = build_cassandra_filename(42, "Index.db");
        assert_eq!(filename, "nb-42-big-Index.db");
    }

    #[tokio::test]
    async fn test_export_empty_engine() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Try to export without any data (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 1).skip_compaction();
        let result = engine.export_sstable(export_dir.path(), options).await;

        // Should fail because there are no SSTables to export
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No SSTables found"));
    }

    #[tokio::test]
    async fn test_export_single_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write some data and flush
        for i in 0..5 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        engine.flush().await.unwrap();

        // Export (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine.export_sstable(export_dir.path(), options).await;

        // Should succeed
        assert!(report.is_ok());
        let report = report.unwrap();

        // Verify report
        assert!(report.data_file_size > 0);
        assert!(!report.components.is_empty());

        // Verify files exist with correct naming and directory structure
        let data_file = export_dir
            .path()
            .join("test_ks")
            .join("test_table")
            .join("nb-1-big-Data.db");
        assert!(data_file.exists());

        let index_file = export_dir
            .path()
            .join("test_ks")
            .join("test_table")
            .join("nb-1-big-Index.db");
        assert!(index_file.exists());
    }

    #[tokio::test]
    async fn test_export_with_memtable_flush() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write data but DON'T flush manually
        for i in 0..3 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        // Verify memtable is not empty
        assert!(engine.memtable_row_count() > 0);

        // Export should automatically flush (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine.export_sstable(export_dir.path(), options).await;

        assert!(report.is_ok());

        // Memtable should be empty after export (flushed)
        assert_eq!(engine.memtable_row_count(), 0);
    }

    #[tokio::test]
    async fn test_export_report_total_size() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write and flush
        for i in 0..5 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }
        engine.flush().await.unwrap();

        // Export (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine
            .export_sstable(export_dir.path(), options)
            .await
            .unwrap();

        // Total size should be sum of all components
        let total_size = report.total_size();
        assert!(total_size > 0);
        assert!(total_size >= report.data_file_size + report.index_file_size);
    }

    #[tokio::test]
    async fn test_export_report_validate_components() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write and flush
        let mutation = create_test_mutation(1, "Alice", 1000000);
        engine.write(mutation).unwrap();
        engine.flush().await.unwrap();

        // Export without validation (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine
            .export_sstable(export_dir.path(), options)
            .await
            .unwrap();

        // Manual validation should pass
        let validation_result = report.validate_components();
        assert!(validation_result.is_ok());
    }

    #[tokio::test]
    async fn test_export_after_close_fails() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write and flush
        let mutation = create_test_mutation(1, "Alice", 1000000);
        engine.write(mutation).unwrap();
        engine.flush().await.unwrap();

        // Close engine
        engine.close().await.unwrap();

        // Try to export - should fail
        let options = ExportOptions::new("test_ks", "test_table", 1);
        let result = engine.export_sstable(export_dir.path(), options).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("closed"));
    }

    #[tokio::test]
    async fn test_export_multiple_generations() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write, flush, write again, flush again (creates 2 generations)
        let mutation1 = create_test_mutation(1, "Alice", 1000000);
        engine.write(mutation1).unwrap();
        engine.flush().await.unwrap();

        let mutation2 = create_test_mutation(2, "Bob", 2000000);
        engine.write(mutation2).unwrap();
        engine.flush().await.unwrap();

        // Export should use the most recent generation (skip compaction since it's not implemented yet)
        let options = ExportOptions::new("test_ks", "test_table", 100)
            .skip_validation()
            .skip_compaction();
        let report = engine.export_sstable(export_dir.path(), options).await;

        assert!(report.is_ok());

        // Verify exported files use generation 100 (from options, not internal generation)
        let data_file = export_dir
            .path()
            .join("test_ks")
            .join("test_table")
            .join("nb-100-big-Data.db");
        assert!(data_file.exists());
    }
}
