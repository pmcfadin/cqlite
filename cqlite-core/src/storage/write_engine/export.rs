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
    /// Whether to perform compaction before export (default: false)
    ///
    /// **Deprecated**: Setting this to `true` emits a warning but has no effect.
    /// Use `WriteEngine::maintenance_step()` before calling `export_sstable()` instead.
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
            compact_before_export: false,
            validate_after_export: true,
        }
    }

    /// Disable compaction before export (no-op — compaction is off by default)
    ///
    /// This method is a no-op since `compact_before_export` defaults to `false`.
    /// Use `WriteEngine::maintenance_step()` before export for compaction.
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

    /// Check that all expected components exist.
    ///
    /// Two layers of validation:
    /// 1. The always-required components must have been exported.
    /// 2. Every component listed in the exported `TOC.txt` must have a matching
    ///    file in `self.components`. This catches optional components — such as
    ///    `CRC.db` for uncompressed BIG SSTables (Issue #1197) — that the
    ///    TOC references but that may have been missed during the copy step.
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
            if !self.has_component(component) {
                return Err(Error::Storage(format!(
                    "Missing required component: {}",
                    component
                )));
            }
        }

        self.validate_toc_components()?;

        Ok(())
    }

    /// Returns true when an exported component path ends with `suffix`.
    fn has_component(&self, suffix: &str) -> bool {
        self.components.iter().any(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.ends_with(suffix))
                .unwrap_or(false)
        })
    }

    /// Validate that every component named in the exported `TOC.txt` was
    /// actually copied. The TOC is the authoritative component manifest, so
    /// any optional component it lists (e.g. `CRC.db`) must exist on disk.
    fn validate_toc_components(&self) -> Result<()> {
        let toc_path = self
            .components
            .iter()
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.ends_with("TOC.txt"))
                    .unwrap_or(false)
            })
            .ok_or_else(|| Error::Storage("Missing required component: TOC.txt".to_string()))?;

        let toc = std::fs::read_to_string(toc_path).map_err(|e| {
            Error::Storage(format!(
                "Failed to read exported TOC.txt at {:?}: {}",
                toc_path, e
            ))
        })?;

        for line in toc.lines() {
            let component = line.trim();
            if component.is_empty() {
                continue;
            }
            if !self.has_component(component) {
                return Err(Error::Storage(format!(
                    "TOC.txt lists component {} but it was not exported",
                    component
                )));
            }
        }

        Ok(())
    }
}

/// Validate that a name is safe for use in filesystem paths.
///
/// Rejects empty strings, path traversal sequences (`..`), path separators
/// (`/`, `\`), and null bytes.
#[cfg(feature = "write-support")]
fn validate_export_name(name: &str, field: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidPath(format!(
            "Export {} must not be empty",
            field
        )));
    }
    if name.contains("..") {
        return Err(Error::InvalidPath(format!(
            "Export {} contains path traversal sequence '..': {:?}",
            field, name
        )));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(Error::InvalidPath(format!(
            "Export {} contains path separator: {:?}",
            field, name
        )));
    }
    if name.contains('\0') {
        return Err(Error::InvalidPath(format!(
            "Export {} contains null byte: {:?}",
            field, name
        )));
    }
    Ok(())
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

        // Validate keyspace/table names to prevent path traversal
        validate_export_name(&options.keyspace, "keyspace")?;
        validate_export_name(&options.table, "table")?;

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

        // Step 2: Compaction (deprecated on ExportOptions — use maintenance_step() instead)
        if options.compact_before_export {
            log::warn!(
                "compact_before_export on ExportOptions is deprecated. \
                 Use WriteEngine::maintenance_step() before export instead."
            );
        }
        let source_sstable = self.find_most_recent_sstable().await?;

        // Step 3: Copy to output with Cassandra naming
        let mut exported_components = Vec::new();
        let mut data_file_size = 0u64;
        let mut index_file_size = 0u64;

        // Get the source generation number from the SSTable info
        let (source_generation, source_dir) = source_sstable;

        // List of components to copy.
        // CompressionInfo.db is omitted for uncompressed data (Issue #429).
        // CRC.db is an optional component emitted only for uncompressed BIG
        // SSTables (Issue #1197) and absent for compressed/BTI tables — the
        // loop below skips any component whose source file does not exist, so
        // it is only copied when the source actually has it.
        let components_to_copy = [
            ("Data.db", SSTableComponent::Data),
            ("Index.db", SSTableComponent::Index),
            ("Statistics.db", SSTableComponent::Statistics),
            ("Filter.db", SSTableComponent::Filter),
            ("Summary.db", SSTableComponent::Summary),
            ("Digest.crc32", SSTableComponent::Digest),
            ("CRC.db", SSTableComponent::Crc),
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
        let (partition_count, row_count) = read_statistics_from_export(&exported_components)?;

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
        if !self.config.data_dir.exists() {
            return Err(Error::Storage(
                "Data directory does not exist (no SSTables to export)".to_string(),
            ));
        }

        Self::find_max_generation(
            &self.config.data_dir,
            crate::storage::sstable::MAX_SSTABLE_SCAN_DEPTH,
        )
        .await?
        .ok_or_else(|| {
            Error::Storage("No SSTables found in data directory (nothing to export)".to_string())
        })
    }

    /// Recursively find the highest SSTable generation and its directory.
    ///
    /// Returns `Some((generation, dir))` if any SSTable files were found.
    #[allow(clippy::type_complexity)]
    fn find_max_generation<'a>(
        dir: &'a Path,
        depth: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<(u64, PathBuf)>>> + Send + 'a>,
    > {
        let dir = dir.to_path_buf();
        Box::pin(async move {
            let mut best: Option<(u64, PathBuf)> = None;

            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| Error::Storage(format!("Failed to read directory: {}", e)))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| Error::Storage(format!("Failed to read directory entry: {}", e)))?
            {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                // Parse generation from filename: nb-{generation}-big-{Component}.db
                if filename_str.starts_with("nb-") && filename_str.contains("-big-") {
                    if let Some(gen) = filename_str
                        .strip_prefix("nb-")
                        .and_then(|s| s.split('-').next())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        if best.as_ref().is_none_or(|(cur, _)| gen > *cur) {
                            best = Some((gen, dir.clone()));
                        }
                    }
                } else if depth > 0 {
                    let path = entry.path();
                    if entry
                        .file_type()
                        .await
                        .map(|ft| ft.is_dir())
                        .unwrap_or(false)
                    {
                        if let Some((gen, sub_dir)) =
                            Self::find_max_generation(&path, depth - 1).await?
                        {
                            if best.as_ref().is_none_or(|(cur, _)| gen > *cur) {
                                best = Some((gen, sub_dir));
                            }
                        }
                    }
                }
            }
            Ok(best)
        })
    }
}

/// Read partition and row counts from the exported Statistics.db.
///
/// Both counts come from the single authoritative gated STATS reader
/// [`crate::parser::repair_metadata::read_table_counts`] (issue #944):
/// - `partition_count` is the Σ of the self-describing `estimatedPartitionSize`
///   histogram bucket counts. It needs no version gates and is format-agnostic,
///   so it is correct for `nb` BIG *and* `da` BTI exports (issue #1622).
/// - `totalRows` is decoded from the version-gated STATS body, which dynamically
///   skips the two leading `EstimatedHistogram`s AND the tombstone histogram
///   before reading the count (issue #1327).
///
/// There is intentionally NO hand-rolled Index.db walk here: BTI SSTables have
/// no Index.db (they use Partitions.db/Rows.db), so a BIG-only walk silently
/// reported 0 partitions for BTI exports (issue #1622).
#[cfg(feature = "write-support")]
fn read_statistics_from_export(components: &[PathBuf]) -> Result<(u64, u64)> {
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

    let file_data = std::fs::read(stats_path)
        .map_err(|e| Error::Storage(format!("Failed to read Statistics.db: {}", e)))?;

    // Issue #1327: reuse the AUTHORITATIVE gated STATS reader (`read_table_counts`,
    // issue #944) instead of a parallel hand-rolled layout walk. That reader
    // dynamically skips BOTH leading `EstimatedHistogram`s AND the tombstone
    // histogram (`estimatedTombstoneDropTime`), then decodes `totalRows` from the
    // version-gated body. The old hand-rolled walk hardcoded an EMPTY tombstone
    // histogram in its fixed post-histogram prefix, so a writer-produced
    // Statistics.db carrying a non-empty tombstone histogram read `totalRows` from
    // the wrong offset (finding 2). Authoritative gates come from the exported
    // Statistics.db filename (`nb-{gen}-big-Statistics.db`).
    let gates = crate::storage::sstable::version_gate::VersionGates::from_path(stats_path).ok();
    // Issue #1622: derive BOTH counts from the ONE authoritative gated STATS
    // reader. `partition_count` is the Σ of the self-describing
    // `estimatedPartitionSize` histogram bucket counts (needs no version gates
    // and is format-agnostic), so it is correct for `nb` BIG *and* `da` BTI
    // exports. The previous code hand-rolled a BIG-only `Index.db` walk
    // (`count_index_entries`); a BTI SSTable has no `Index.db` (it uses
    // `Partitions.db`/`Rows.db`), so that walk failed and the swallowed error
    // left `partition_count == 0` even for tables with many partitions.
    let counts = match crate::parser::repair_metadata::read_table_counts(&file_data, gates.as_ref())
    {
        Ok(counts) => counts,
        Err(e) => {
            log::warn!("Failed to read counts from Statistics.db: {e}; defaulting to 0");
            crate::parser::repair_metadata::TableCounts {
                partition_count: 0,
                total_rows: None,
            }
        }
    };
    let partition_count = counts.partition_count;
    let row_count = counts.total_rows.unwrap_or_else(|| {
        // `total_rows` is None only when the version-gated walk could not reach
        // field 12 (e.g. unmodeled improved-min-max bounds). `nb` exports use
        // the legacy min/max branch, which is always traversable, so this is a
        // fail-safe rather than an expected path.
        log::warn!("Statistics.db STATS walk could not reach totalRows; defaulting to 0");
        0
    });

    log::info!(
        "Read from export: row_count={}, partition_count={}",
        row_count,
        partition_count
    );

    Ok((partition_count, row_count))
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
            dropped_columns: HashMap::new(),
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
    fn test_export_path_traversal_rejected() {
        let bad_names = vec!["../etc", "foo/bar", "foo\\bar", "", "test\0ks", ".."];
        for name in &bad_names {
            let result = validate_export_name(name, "keyspace");
            assert!(result.is_err(), "Should reject {:?} as export name", name);
        }
    }

    #[test]
    fn test_export_valid_names_accepted() {
        let good_names = vec!["test_ks", "my-table", "T1", "keyspace123", "a.b"];
        for name in &good_names {
            let result = validate_export_name(name, "keyspace");
            assert!(
                result.is_ok(),
                "Should accept {:?} as export name: {:?}",
                name,
                result
            );
        }
    }

    #[test]
    fn test_export_options_defaults() {
        let options = ExportOptions::new("test_ks", "users", 1);

        assert_eq!(options.keyspace, "test_ks");
        assert_eq!(options.table, "users");
        assert_eq!(options.generation, 1);
        assert!(!options.compact_before_export);
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

        assert!(!options.compact_before_export);
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
        assert_eq!(report.row_count, 5, "Expected 5 rows");
        // Partition count should be non-zero (actual count depends on data writer implementation)
        assert!(
            report.partition_count > 0,
            "Expected non-zero partition count, got {}",
            report.partition_count
        );

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

        // Verify CompressionInfo.db is NOT included for uncompressed data (Issue #429)
        let compression_info_file = export_dir
            .path()
            .join("test_ks")
            .join("test_table")
            .join("nb-1-big-CompressionInfo.db");
        assert!(
            !compression_info_file.exists(),
            "CompressionInfo.db must NOT be included for uncompressed data"
        );
    }

    /// Regression for issue #1327 finding 2: the exported-Statistics.db
    /// `totalRows` reader must skip the tombstone histogram DYNAMICALLY.
    ///
    /// When a write-produced Statistics.db carries a NON-EMPTY
    /// `estimatedTombstoneDropTime` histogram, the old hand-rolled walk (which
    /// hardcoded an 8-byte EMPTY tombstone histogram in its fixed post-histogram
    /// prefix) read `totalRows` from the wrong offset. This test writes rows AND
    /// cell tombstones so the tombstone histogram is non-empty, exports, and
    /// asserts the read-back row count matches the report — and that the OLD
    /// hardcoded-prefix walk would have produced a DIFFERENT (wrong) value.
    #[tokio::test]
    async fn test_export_row_count_with_nonempty_tombstone_histogram() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // INDEPENDENTLY-KNOWN row count: the test controls exactly how many rows
        // it writes, so we anchor the read-back assertion to that literal — NOT to
        // `report.row_count`, which `export_sstable` itself derives from the same
        // shared `read_statistics_from_export` reader (issue #1327 finding 2: a
        // broken reader would satisfy a `row_count == report.row_count` check
        // circularly). 4 live rows + 4 cell-tombstone rows (each tombstone lands
        // on a distinct new partition, creating one row) == 8 totalRows.
        const LIVE_ROWS: i32 = 4;
        const TOMBSTONE_ROWS: i32 = 4;
        const EXPECTED_TOTAL_ROWS: u64 = (LIVE_ROWS + TOMBSTONE_ROWS) as u64;

        // Live rows.
        for i in 0..LIVE_ROWS {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
            engine.write(mutation).unwrap();
        }
        // Cell tombstones with explicit local-deletion-times → non-empty
        // `estimatedTombstoneDropTime` histogram in the flushed Statistics.db.
        for i in 100..(100 + TOMBSTONE_ROWS) {
            let table_id = TableId::new("test_ks", "test_table");
            let pk = PartitionKey::single("id", Value::Integer(i));
            let ops = vec![CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: Some(1_600_000_000 + i),
            }];
            let mutation = Mutation::new(table_id, pk, None, ops, 2_000_000 + i as i64, None);
            engine.write(mutation).unwrap();
        }

        engine.flush().await.unwrap();

        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine
            .export_sstable(export_dir.path(), options)
            .await
            .unwrap();

        // Read the exported Statistics.db back through the production read path.
        let (_partitions, row_count) = read_statistics_from_export(&report.components).unwrap();

        // Assert against the INDEPENDENTLY-KNOWN written count, so a broken reader
        // is actually caught. Cross-checking `report.row_count` here would be
        // circular (it comes from the same reader), so we additionally assert the
        // report agrees with the same independent anchor.
        assert_eq!(
            row_count, EXPECTED_TOTAL_ROWS,
            "read-back totalRows must equal the number of rows the test WROTE \
             ({EXPECTED_TOTAL_ROWS}) when the tombstone histogram is non-empty; a \
             broken dynamic tombstone-histogram skip would read a different value"
        );
        assert_eq!(
            report.row_count, EXPECTED_TOTAL_ROWS,
            "export report row_count must also equal the independently-known \
             written count ({EXPECTED_TOTAL_ROWS})"
        );

        // Prove the tombstone histogram is actually non-empty AND that the OLD
        // hardcoded-89-byte-prefix walk would have read a DIFFERENT value from the
        // wrong offset (the bug this fix addresses).
        let stats_path = report
            .components
            .iter()
            .find(|p| p.to_string_lossy().ends_with("Statistics.db"))
            .unwrap();
        let data = std::fs::read(stats_path).unwrap();
        let (buggy_row_count, tombstone_bucket_count) =
            legacy_hardcoded_prefix_row_count(&data).expect("STATS component present");
        assert!(
            tombstone_bucket_count > 0,
            "test precondition: tombstone histogram must be non-empty (got {} buckets)",
            tombstone_bucket_count
        );
        assert_ne!(
            buggy_row_count, row_count,
            "the old hardcoded-empty-tombstone walk must read the WRONG totalRows \
             when the tombstone histogram is non-empty (regression pin)"
        );
    }

    /// Reproduces the PRE-#1327-fix read: skip the two leading histograms
    /// dynamically but then apply the OLD fixed 89-byte post-histogram prefix
    /// (which assumed an EMPTY 8-byte tombstone histogram) before reading
    /// `totalRows`. Returns `(buggy_row_count, tombstone_bucket_count)`. Used only
    /// to prove the regression; the production reader no longer does this.
    fn legacy_hardcoded_prefix_row_count(data: &[u8]) -> Option<(u64, i32)> {
        // Locate STATS component (type == 2) via the TOC.
        let num = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut stats_offset = None;
        for i in 0..num {
            let base = 8 + i * 8;
            if base + 8 > data.len() {
                return None;
            }
            let ty =
                u32::from_be_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]]);
            if ty == 2 {
                stats_offset = Some(u32::from_be_bytes([
                    data[base + 4],
                    data[base + 5],
                    data[base + 6],
                    data[base + 7],
                ]) as usize);
                break;
            }
        }
        let mut cur = stats_offset?;
        let hist_len = |off: usize| -> Option<usize> {
            let bc = i32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
            if bc < 0 {
                return None;
            }
            Some(4 + (bc as usize) * 16)
        };
        // The tombstone histogram's bucket count (after the two leading histograms)
        // — proves the test precondition (non-empty when > 0).
        let after_two = cur + hist_len(cur)? + hist_len(cur + hist_len(cur)?)?;
        cur = after_two;
        // Old fixed prefix assumed an EMPTY (8-byte) tombstone histogram; the
        // tombstone bucket count sits after the fixed non-histogram fields:
        //   commitLog(12)+minTs(8)+maxTs(8)+min/maxLDT(8)+min/maxTTL(8)+ratio(8) = 52.
        let tomb_bc_off = after_two + 52;
        let tombstone_bucket_count = i32::from_be_bytes([
            data[tomb_bc_off],
            data[tomb_bc_off + 1],
            data[tomb_bc_off + 2],
            data[tomb_bc_off + 3],
        ]);
        const OLD_FIXED_PREFIX: usize = 89;
        let abs = cur + OLD_FIXED_PREFIX;
        if abs + 8 > data.len() {
            return None;
        }
        let rc = u64::from_be_bytes([
            data[abs],
            data[abs + 1],
            data[abs + 2],
            data[abs + 3],
            data[abs + 4],
            data[abs + 5],
            data[abs + 6],
            data[abs + 7],
        ]);
        Some((rc, tombstone_bucket_count))
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

    /// Issue #1197: uncompressed BIG SSTables now emit a `CRC.db` component
    /// listed in TOC.txt. The export-copy path must copy it (when the source
    /// has it) and validation — which now cross-checks every TOC-listed
    /// component — must pass. This pins both halves of the roborev follow-up
    /// against an in-process flushed SSTable (no external fixture required).
    #[tokio::test]
    async fn test_export_copies_crc_db_when_present() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        for i in 0..3 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
            engine.write(mutation).unwrap();
        }
        engine.flush().await.unwrap();

        // Confirm the flushed (uncompressed BIG) source actually has a CRC.db;
        // if a future writer change drops it, this guard makes the intent clear
        // rather than silently passing.
        let source = engine.find_most_recent_sstable().await.unwrap();
        let (source_gen, source_dir) = source;
        let source_crc = source_dir.join(format!("nb-{}-big-CRC.db", source_gen));
        assert!(
            source_crc.exists(),
            "uncompressed BIG flush should emit CRC.db at {:?}",
            source_crc
        );

        // Export WITH validation enabled (default) — exercises both the copy
        // path and the TOC-aware validate_components().
        let options = ExportOptions::new("test_ks", "test_table", 1).skip_compaction();
        let report = engine
            .export_sstable(export_dir.path(), options)
            .await
            .unwrap();

        let crc_file = export_dir
            .path()
            .join("test_ks")
            .join("test_table")
            .join("nb-1-big-CRC.db");
        assert!(
            crc_file.exists(),
            "export must copy CRC.db when the source SSTable has it"
        );

        // The copied CRC.db must be among the tracked components.
        assert!(
            report.has_component("CRC.db"),
            "exported component list must include CRC.db"
        );

        // Validation passed implicitly via validate_after_export default; assert
        // explicitly too for clarity.
        report.validate_components().unwrap();
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

    #[tokio::test]
    async fn test_export_default_options_does_not_fail() {
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
        for i in 0..3 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }
        engine.flush().await.unwrap();

        // Export with default options (compact_before_export=false) should succeed
        let options = ExportOptions::new("test_ks", "test_table", 1).skip_validation();
        let result = engine.export_sstable(export_dir.path(), options).await;
        assert!(
            result.is_ok(),
            "Export with default options should not fail: {:?}",
            result
        );
    }

    /// Locate the shared test datasets root (`CQLITE_DATASETS_ROOT`). Returns
    /// `None` (test skips cleanly) when unset or not a directory.
    fn datasets_root() -> Option<std::path::PathBuf> {
        let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
        let p = std::path::PathBuf::from(root);
        if p.is_dir() {
            Some(p)
        } else {
            None
        }
    }

    /// Regression test for issue #1622: a BTI (`da`) export must report a
    /// non-zero `partition_count`. Before the fix, `partition_count` came from a
    /// hand-rolled BIG-only `Index.db` walk (`count_index_entries`); a BTI
    /// SSTable has NO `Index.db` (it uses `Partitions.db`/`Rows.db`), so that
    /// walk returned `Err("Index.db not found")`, the error was swallowed, and
    /// the report claimed 0 partitions even for a table with many. The fix reads
    /// the authoritative, format-agnostic partition count from the Statistics.db
    /// `estimatedPartitionSize` histogram via `read_table_counts` — the same
    /// gated reader already used for `row_count` (issue #1327).
    ///
    /// `test_da` is a local-only fixture, so this test SKIPS cleanly when the
    /// binaries are absent; but a PRESENT fixture that reports 0 partitions is a
    /// hard failure (matches the dataset-test posture in MEMORY).
    ///
    /// CI COVERAGE NOTE (issue #1622, roborev follow-up): this fixture-based test
    /// SKIPS in default CI (the repo ships only `test_da` metadata sidecars, not
    /// the binary Statistics.db). That is acceptable because the CORE fix under
    /// test — `read_statistics_from_export` -> `read_table_counts` ->
    /// `partition_count` (Σ of the `estimatedPartitionSize` histogram buckets) —
    /// is FORMAT-AGNOSTIC: it has no BTI/`da`-vs-BIG/`nb` branch and no version
    /// gate on the partition-count path (see `read_statistics_from_export`
    /// above). `WriteEngine::flush` cannot emit `da`/BTI (it is hardwired to
    /// `SSTableWriter::with_expected_partitions_and_registry`, i.e.
    /// `SSTableFormat::Big`), so a self-contained WriteEngine-`da`-flush variant
    /// is not constructible. The identical code path is therefore exercised in
    /// CI by the `nb` test `test_export_partition_count_nonzero` below, which
    /// flushes a real `nb` SSTable via `WriteEngine` and asserts the same
    /// `partition_count > 0 && partition_count <= row_count` invariant through
    /// the same `read_statistics_from_export` reader. This fixture test remains
    /// as local BTI assurance (it proves the reader consumes a real `da`
    /// Statistics.db) but is not required for CI coverage of the fix.
    #[test]
    fn test_read_statistics_from_export_bti_partition_count() {
        let Some(root) = datasets_root() else {
            eprintln!("CQLITE_DATASETS_ROOT unset; skipping BTI partition-count test");
            return;
        };
        let dir = root.join("sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11");
        if !dir.join("da-2-bti-Statistics.db").is_file() {
            eprintln!("BTI da fixture absent at {dir:?}; skipping");
            return;
        }

        // Feed the reader the exact BTI component set on disk (no Index.db).
        let components: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e == "db")
                    .unwrap_or(false)
            })
            .collect();

        let (partition_count, row_count) = read_statistics_from_export(&components).unwrap();

        assert!(
            partition_count > 0,
            "BTI (da) export partition_count should be > 0, got {partition_count}"
        );
        assert!(
            partition_count <= row_count,
            "partition_count ({partition_count}) should be <= row_count ({row_count})"
        );
    }

    #[tokio::test]
    async fn test_export_partition_count_nonzero() {
        let temp_dir = TempDir::new().unwrap();
        let export_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write data and flush
        for i in 0..10 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }
        engine.flush().await.unwrap();

        // Export and verify partition_count is non-zero
        let options = ExportOptions::new("test_ks", "test_table", 1)
            .skip_validation()
            .skip_compaction();
        let report = engine
            .export_sstable(export_dir.path(), options)
            .await
            .unwrap();

        // Main assertion: partition_count should now be non-zero (was always 0 before fix)
        assert!(
            report.partition_count > 0,
            "partition_count should be non-zero, got {}",
            report.partition_count
        );

        // Verify row count is correct
        assert_eq!(report.row_count, 10, "Expected 10 rows");

        // Partition count should be <= row count
        assert!(
            report.partition_count <= report.row_count,
            "partition_count ({}) should be <= row_count ({})",
            report.partition_count,
            report.row_count
        );
    }
}
