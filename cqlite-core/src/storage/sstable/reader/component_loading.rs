//! Component loading methods for SSTableReader
//!
//! This module contains methods for loading SSTable component files
//! (Index.db, Filter.db, Summary.db, Statistics.db) and related operations.

use super::{compression::extract_sstable_base_name, SSTableReader};
use crate::platform::Platform;
use crate::storage::sstable::{
    bloom::BloomFilter, index::SSTableIndex, index_reader::IndexReader,
    statistics_reader::StatisticsReader, summary_reader::SummaryReader,
};
use crate::{Error, Result, RowKey};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, BufReader};

use super::source::BlockSource;

/// Outcome of loading a sibling `Index.db` (issue #2302): distinguishes a
/// genuinely ABSENT file (quiet, expected) from a PRESENT-but-unloadable one
/// (open/parse failure — a silent-degradation signal to surface loud, not fold
/// into a bare `None`). Boxed reader keeps the enum small (large-variant lint).
pub(super) enum IndexLoadOutcome {
    /// Index.db opened and parsed.
    Loaded(Box<IndexReader>),
    /// No Index.db on disk (or the path/base-name could not be derived).
    Absent,
    /// Index.db exists on disk but `open` returned a non-`NotFound` error.
    PresentButUnloadable,
}

impl SSTableReader {
    /// Load index from integrated or component-based format
    pub(super) async fn load_index(
        file: &Arc<tokio::sync::Mutex<BlockSource>>,
        header: &crate::parser::SSTableHeader,
        platform: &Arc<Platform>,
        data_file_path: &Path,
        cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Option<SSTableIndex>> {
        // Strategy 1: Check if index information is available in header (for integrated formats)
        if let Some(index_offset) = header.properties.get("index_offset") {
            let offset: u64 = index_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid index offset in header"))?;

            // Load index from file
            {
                let mut file_guard = file.lock().await;
                file_guard.seek(std::io::SeekFrom::Start(offset)).await?;
                let index = SSTableIndex::load(&mut *file_guard).await?;
                tracing::debug!("Loaded integrated index from Data.db at offset {}", offset);
                return Ok(Some(index));
            }
        }

        // Strategy 2: Check for separate Index.db component file (Cassandra 5+ standard)
        if let Some(base_name) = extract_sstable_base_name(data_file_path) {
            let index_path = data_file_path
                .parent()
                .ok_or_else(|| {
                    Error::invalid_operation("Cannot determine parent directory for Index.db")
                })?
                .join(format!("{}-Index.db", base_name));

            if tokio::fs::metadata(&index_path).await.is_ok() {
                match IndexReader::open_with_summary_cancellable(
                    &index_path,
                    platform.clone(),
                    None,
                    cancel,
                )
                .await
                {
                    Ok(index_reader) => {
                        tracing::debug!(
                            "Found separate Index.db component at {}",
                            index_path.display()
                        );

                        // Convert IndexReader to SSTableIndex by extracting partition entries
                        match Self::convert_index_reader_to_sstable_index(
                            index_reader,
                            data_file_path,
                        )
                        .await
                        {
                            Ok(sstable_index) => {
                                tracing::debug!(
                                    "Successfully converted Index.db component to SSTableIndex"
                                );
                                return Ok(Some(sstable_index));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to convert Index.db component to SSTableIndex: {}. This may indicate an incompatible Index.db format or corruption.",
                                    e
                                );
                                // Continue to fallback strategies
                            }
                        }
                    }
                    // A mid-parse cancellation (issue #2383) must ABORT, never fall
                    // through to a fallback strategy (that would ignore the cancel
                    // and keep the worker spinning). Surfaced by variant.
                    Err(e @ Error::Cancelled) => return Err(e),
                    Err(e) => {
                        tracing::debug!(
                            "Failed to load Index.db component: {}. This may indicate file corruption, permission issues, or format incompatibility.",
                            e
                        );
                        // Continue to fallback strategies
                    }
                }
            } else {
                tracing::debug!(
                    "No Index.db component file found at {}",
                    index_path.display()
                );
            }
        }

        tracing::debug!("No index source available (neither header offset nor Index.db component)");
        Ok(None)
    }

    /// Load bloom filter from integrated or component-based format
    pub(super) async fn load_bloom_filter(
        file: &Arc<tokio::sync::Mutex<BlockSource>>,
        header: &crate::parser::SSTableHeader,
        _platform: &Arc<Platform>,
        data_file_path: &Path,
    ) -> Result<Option<BloomFilter>> {
        // Strategy 1: Check if bloom filter information is available in header
        if let Some(bloom_offset) = header.properties.get("bloom_filter_offset") {
            let offset: u64 = bloom_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid bloom filter offset in header"))?;

            // Load bloom filter from file
            {
                let mut file_guard = file.lock().await;
                file_guard.seek(std::io::SeekFrom::Start(offset)).await?;
                let bloom_filter = BloomFilter::load(&mut *file_guard).await?;
                tracing::debug!(
                    "Loaded integrated bloom filter from Data.db at offset {}",
                    offset
                );
                return Ok(Some(bloom_filter));
            }
        }

        // Strategy 2: Check for separate Filter.db component file
        if let Some(base_name) = extract_sstable_base_name(data_file_path) {
            let filter_path = data_file_path
                .parent()
                .ok_or_else(|| {
                    Error::invalid_operation("Cannot determine parent directory for Filter.db")
                })?
                .join(format!("{}-Filter.db", base_name));

            if tokio::fs::metadata(&filter_path).await.is_ok() {
                match tokio::fs::File::open(&filter_path).await {
                    Ok(filter_file) => {
                        let mut reader = BufReader::new(filter_file);
                        match BloomFilter::load(&mut reader).await {
                            Ok(bloom_filter) => {
                                tracing::debug!(
                                    "Loaded separate Filter.db component from {}",
                                    filter_path.display()
                                );
                                return Ok(Some(bloom_filter));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to parse Filter.db component: {}. Bloom filter functionality will be unavailable.",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Failed to open Filter.db component: {}. Bloom filter functionality will be unavailable.",
                            e
                        );
                    }
                }
            } else {
                tracing::debug!(
                    "No Filter.db component file found at {}",
                    filter_path.display()
                );
            }
        }

        tracing::debug!(
            "No bloom filter source available (neither header offset nor Filter.db component)"
        );
        Ok(None)
    }

    /// Load Index.db reader for partition lookup
    pub(super) async fn load_index_reader(
        path: &Path,
        platform: &Arc<Platform>,
        cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<IndexLoadOutcome> {
        let Some(base_name) = extract_sstable_base_name(path) else {
            return Ok(IndexLoadOutcome::Absent);
        };
        let Some(parent) = path.parent() else {
            return Ok(IndexLoadOutcome::Absent);
        };
        let index_path = parent.join(format!("{}-Index.db", base_name));

        match IndexReader::open_with_summary_cancellable(
            &index_path,
            platform.clone(),
            None,
            cancel,
        )
        .await
        {
            Ok(reader) => {
                tracing::debug!("Loaded Index.db reader for {}", index_path.display());
                Ok(IndexLoadOutcome::Loaded(Box::new(reader)))
            }
            // A genuinely absent Index.db (some shapes legitimately ship without one)
            // is quiet & expected. A PRESENT-but-unloadable Index.db (open/parse
            // errored) is the silent-degradation class issue #2302 exists to kill:
            // surface it so `iterate_all_partitions` can WARN loud rather than
            // silently full-scan. `IndexReader::open` returns `NotFound` iff the file
            // is absent, so the error kind is the authoritative discriminator.
            Err(Error::NotFound(_)) => {
                tracing::debug!("No Index.db present at {}", index_path.display());
                Ok(IndexLoadOutcome::Absent)
            }
            // A mid-parse cancellation (issue #2383) aborts the open, never masked
            // as a present-but-unloadable degradation.
            Err(e @ Error::Cancelled) => Err(e),
            Err(e) => {
                tracing::debug!(
                    "Index.db present at {} but failed to load: {}",
                    index_path.display(),
                    e
                );
                Ok(IndexLoadOutcome::PresentButUnloadable)
            }
        }
    }

    /// Load Summary.db reader for token-range iteration
    pub(super) async fn load_summary_reader(
        path: &Path,
        platform: &Arc<Platform>,
    ) -> Option<SummaryReader> {
        let base_name = extract_sstable_base_name(path)?;
        let summary_path = path.parent()?.join(format!("{}-Summary.db", base_name));

        match SummaryReader::open(&summary_path, platform.clone()).await {
            Ok(reader) => {
                tracing::debug!("Loaded Summary.db reader for {}", summary_path.display());
                Some(reader)
            }
            Err(e) => {
                tracing::debug!("Failed to load Summary.db reader: {}", e);
                None
            }
        }
    }

    /// Load Statistics.db reader for min/max timestamps and metadata.
    ///
    /// # Errors
    ///
    /// A *present but unparseable* Statistics.db is a HARD FAILURE (issue #1626):
    /// proceeding with zero EncodingStats baselines and no SerializationHeader
    /// columns would make every WRITETIME()/TTL/deletion-time from this SSTable
    /// silently wrong (the "default-on-parse-failure" anti-pattern the
    /// no-heuristics mandate forbids, issue #28). Corruption/UnsupportedVersion/IO
    /// errors are propagated with the component file path named.
    ///
    /// Out of scope (returns `Ok(None)`, preserving prior behavior):
    /// - a genuinely *missing* Statistics.db (`Error::NotFound`);
    /// - a path from which the SSTable base name / parent dir cannot be derived.
    pub(super) async fn load_statistics_reader(
        path: &Path,
        platform: &Arc<Platform>,
    ) -> Result<Option<StatisticsReader>> {
        let Some(base_name) = extract_sstable_base_name(path) else {
            return Ok(None);
        };
        let Some(parent) = path.parent() else {
            return Ok(None);
        };
        let statistics_path = parent.join(format!("{}-Statistics.db", base_name));

        match StatisticsReader::open(&statistics_path, platform.clone()).await {
            Ok(reader) => {
                tracing::debug!(
                    "Loaded Statistics.db reader for {}",
                    statistics_path.display()
                );
                Ok(Some(reader))
            }
            // A missing Statistics.db keeps prior behavior: proceed without it.
            Err(Error::NotFound(_)) => Ok(None),
            // A genuine PARSE failure is data corruption: keep the `Corruption`
            // kind but add the component path + underlying error for diagnosis.
            Err(e @ Error::Corruption(_)) => Err(Error::corruption(format!(
                "Failed to load Statistics.db from {}: {}",
                statistics_path.display(),
                e
            ))),
            // Any other failure of a PRESENT Statistics.db (IO read error, a
            // below-floor `UnsupportedVersion`, ...) must still abort open()
            // (issue #1626), but propagate the ORIGINAL error unchanged so its
            // category/source is preserved rather than mislabeled as data
            // corruption. `UnsupportedVersion` already names version + floor; an
            // IO error keeps its `System` category.
            Err(e) => Err(e),
        }
    }

    /// Extract keyspace and table name from SSTable file path.
    ///
    /// Expected Cassandra directory structure:
    /// `<data_dir>/<keyspace_name>/<table_name>-<uuid>/<sstable_file>`
    ///
    /// For example:
    /// `/var/lib/cassandra/data/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db`
    /// → keyspace: "test_basic", table: "simple_table"
    ///
    /// # Errors
    /// Returns error if path doesn't match expected structure.
    fn extract_keyspace_and_table(sstable_path: &Path) -> Result<(String, String)> {
        // Extract table name (already handles UUID stripping)
        let table_name =
            crate::storage::sstable::extract_table_name(sstable_path).ok_or_else(|| {
                Error::invalid_path(format!(
                    "Cannot extract table name from SSTable path: {}",
                    sstable_path.display()
                ))
            })?;

        // Extract keyspace from grandparent directory
        // Path structure: .../keyspace_name/table_name-uuid/sstable_file.db
        //                       ↑ keyspace    ↑ table dir    ↑ file
        let keyspace = sstable_path
            .parent() // Step 1: .../keyspace_name/table_name-uuid
            .and_then(|p| p.parent()) // Step 2: .../keyspace_name
            .and_then(|p| p.file_name()) // Step 3: Get directory name
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::invalid_path(format!(
                    "Cannot extract keyspace from SSTable path: {}. \
                     Expected Cassandra directory structure: <data_dir>/<keyspace>/<table-uuid>/file",
                    sstable_path.display()
                ))
            })?;

        tracing::debug!(
            "Extracted keyspace='{}', table='{}' from path: {}",
            keyspace,
            table_name,
            sstable_path.display()
        );

        Ok((keyspace, table_name))
    }

    /// Convert IndexReader to SSTableIndex for backward compatibility
    pub(super) async fn convert_index_reader_to_sstable_index(
        index_reader: IndexReader,
        data_file_path: &Path,
    ) -> Result<SSTableIndex> {
        use crate::storage::sstable::index::{Index, IndexEntry};

        // Extract keyspace and table name from SSTable directory path
        // Issue #188: Must use fully-qualified table ID (keyspace.table) to match
        // query executor expectations, not just table name alone
        let (keyspace, table_name) = Self::extract_keyspace_and_table(data_file_path)?;

        // Create fully-qualified table ID: "keyspace.table"
        let table_id = crate::types::TableId::new(format!("{}.{}", keyspace, table_name));

        let mut index = Index::new();

        // Extract partition entries from IndexReader and convert to IndexEntry format
        let partition_entries = index_reader.get_partition_entries();

        for partition_entry in partition_entries {
            // Convert partition entry to our internal IndexEntry format
            let index_entry = IndexEntry {
                table_id: table_id.clone(),
                key: RowKey::new(partition_entry.key_digest.to_vec()),
                offset: partition_entry.data_offset,
                size: partition_entry.data_size,
                compressed: false,
            };

            // Add to index using extracted table ID
            index.add_entry(index_entry);
        }

        tracing::debug!(
            "Converted {} partition entries from IndexReader to SSTableIndex for table '{}' (keyspace: {}, table: {})",
            partition_entries.len(),
            table_id.name(),
            keyspace,
            table_name
        );

        Ok(index)
    }

    /// Detect and construct paths for SSTable component files
    pub(super) async fn detect_component_files(
        data_path: &Path,
    ) -> Result<HashMap<String, PathBuf>> {
        let mut components = HashMap::new();

        let base_name = match extract_sstable_base_name(data_path) {
            Some(name) => name,
            None => {
                tracing::warn!(
                    "Could not extract base name from path: {}. Component file discovery requires standard SSTable naming convention.",
                    data_path.display()
                );
                return Ok(components);
            }
        };

        let parent_dir = data_path.parent().ok_or_else(|| {
            Error::invalid_operation("Cannot determine parent directory for component files")
        })?;

        // Standard Cassandra 5+ component file types with criticality flags
        let component_types = [
            ("Index", true),            // Critical for lookups
            ("Filter", false),          // Optional bloom filter
            ("Summary", false),         // Optional summary
            ("Statistics", false),      // Optional statistics
            ("CompressionInfo", false), // Optional compression metadata
            ("TOC", false),             // Optional table of contents
            ("Digest", false),          // Optional digest/checksum
        ];

        let mut critical_missing = Vec::new();

        for (component_type, is_critical) in &component_types {
            let component_path = parent_dir.join(format!("{}-{}.db", base_name, component_type));

            match tokio::fs::metadata(&component_path).await {
                Ok(metadata) => {
                    if metadata.len() == 0 {
                        tracing::warn!("Component file is empty: {}", component_path.display());
                        if *is_critical {
                            critical_missing.push(component_type.to_string());
                        }
                    } else {
                        tracing::debug!(
                            "Found component file: {} (size: {} bytes)",
                            component_path.display(),
                            metadata.len()
                        );
                        components.insert(component_type.to_string(), component_path);
                    }
                }
                Err(_) => {
                    tracing::debug!("Component file not found: {}", component_path.display());
                    if *is_critical {
                        critical_missing.push(component_type.to_string());
                    }
                }
            }
        }

        // Log component architecture analysis
        if components.is_empty() {
            tracing::debug!(
                "No component files found for base name: {}. This SSTable likely uses integrated format (all data in Data.db).",
                base_name
            );
        } else {
            tracing::debug!(
                "Detected {} component files for {} (component-based architecture)",
                components.len(),
                base_name
            );

            if !critical_missing.is_empty() {
                tracing::warn!(
                    "Missing critical component files: {:?}. Index-based lookups may be unavailable.",
                    critical_missing
                );
            }
        }

        Ok(components)
    }

    /// Validate component file integrity and consistency
    pub(super) async fn validate_component_integrity(
        data_path: &Path,
        components: &HashMap<String, PathBuf>,
    ) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Validate that Data.db file exists and is accessible
        match tokio::fs::metadata(data_path).await {
            Ok(data_metadata) => {
                if data_metadata.len() == 0 {
                    issues.push("Data.db file is empty".to_string());
                }
            }
            Err(e) => {
                issues.push(format!("Cannot access Data.db file: {}", e));
                return Ok(issues); // Can't validate further without Data.db
            }
        }

        // Check for suspicious file sizes (basic sanity check)
        for (component_type, component_path) in components {
            match tokio::fs::metadata(component_path).await {
                Ok(metadata) => {
                    let size = metadata.len();
                    match component_type.as_str() {
                        "Index" if size < 8 => {
                            issues
                                .push(format!("Index.db file suspiciously small: {} bytes", size));
                        }
                        "Filter" if size < 8 => {
                            issues
                                .push(format!("Filter.db file suspiciously small: {} bytes", size));
                        }
                        _ => {} // Other components can vary widely in size
                    }
                }
                Err(e) => {
                    issues.push(format!(
                        "Cannot access component file {}: {}",
                        component_path.display(),
                        e
                    ));
                }
            }
        }

        if issues.is_empty() {
            tracing::debug!(
                "Component integrity validation passed for {}",
                data_path.display()
            );
        } else {
            tracing::warn!(
                "Component integrity issues detected for {}: {:?}",
                data_path.display(),
                issues
            );
        }

        Ok(issues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // =========================================================================
    // extract_keyspace_and_table tests (via SSTableReader)
    // =========================================================================

    #[test]
    fn test_extract_keyspace_and_table_standard_path() {
        // Standard Cassandra directory structure
        let path = PathBuf::from(
            "/var/lib/cassandra/data/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        );
        let result = SSTableReader::extract_keyspace_and_table(&path);
        assert!(result.is_ok(), "Should extract from standard path");

        let (keyspace, table) = result.unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }

    #[test]
    fn test_extract_keyspace_and_table_different_keyspace() {
        // UUID must be exactly 32 hex characters for proper extraction
        let path =
            PathBuf::from("/data/system/local-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let result = SSTableReader::extract_keyspace_and_table(&path);
        assert!(result.is_ok(), "Should extract from system keyspace path");

        let (keyspace, table) = result.unwrap();
        assert_eq!(keyspace, "system");
        assert_eq!(table, "local");
    }

    #[test]
    fn test_extract_keyspace_and_table_complex_table_name() {
        // UUID must be exactly 32 hex characters for proper extraction
        let path = PathBuf::from(
            "/data/my_keyspace/complex_table_name-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        );
        let result = SSTableReader::extract_keyspace_and_table(&path);
        assert!(result.is_ok(), "Should handle complex table names");

        let (keyspace, table) = result.unwrap();
        assert_eq!(keyspace, "my_keyspace");
        assert_eq!(table, "complex_table_name");
    }

    #[test]
    fn test_extract_keyspace_and_table_too_shallow_path() {
        // Path too shallow - missing keyspace directory level
        let path = PathBuf::from("nb-1-big-Data.db");
        let result = SSTableReader::extract_keyspace_and_table(&path);
        assert!(result.is_err(), "Should fail for too shallow path");
    }

    // =========================================================================
    // Component path construction tests
    // =========================================================================

    #[test]
    fn test_component_path_construction() {
        let data_path = PathBuf::from("/test/keyspace/table-uuid/nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&data_path).unwrap();
        let parent = data_path.parent().unwrap();

        // Verify component paths are constructed correctly
        let index_path = parent.join(format!("{}-Index.db", base_name));
        assert_eq!(
            index_path.file_name().unwrap().to_str().unwrap(),
            "nb-1-big-Index.db"
        );

        let filter_path = parent.join(format!("{}-Filter.db", base_name));
        assert_eq!(
            filter_path.file_name().unwrap().to_str().unwrap(),
            "nb-1-big-Filter.db"
        );

        let summary_path = parent.join(format!("{}-Summary.db", base_name));
        assert_eq!(
            summary_path.file_name().unwrap().to_str().unwrap(),
            "nb-1-big-Summary.db"
        );

        let statistics_path = parent.join(format!("{}-Statistics.db", base_name));
        assert_eq!(
            statistics_path.file_name().unwrap().to_str().unwrap(),
            "nb-1-big-Statistics.db"
        );
    }

    #[test]
    fn test_component_path_with_different_generation() {
        let data_path = PathBuf::from("/test/keyspace/table-uuid/nb-45-big-Data.db");
        let base_name = extract_sstable_base_name(&data_path).unwrap();
        let parent = data_path.parent().unwrap();

        let compression_info_path = parent.join(format!("{}-CompressionInfo.db", base_name));
        assert_eq!(
            compression_info_path.file_name().unwrap().to_str().unwrap(),
            "nb-45-big-CompressionInfo.db"
        );
    }

    // =========================================================================
    // Async integration tests (use #[tokio::test])
    // =========================================================================

    #[tokio::test]
    async fn test_detect_component_files_with_real_data() {
        // This test requires CQLITE_DATASETS_ROOT to be set
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic directory not found, skipping test");
            return;
        }

        // Find simple_table directory
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .expect("Should read directory")
            .filter_map(|e| e.ok())
            .find(|e| {
                e.path().is_dir()
                    && e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
            });

        let Some(table_entry) = table_dir else {
            eprintln!("simple_table not found, skipping test");
            return;
        };

        // Find Data.db file
        let data_file = std::fs::read_dir(table_entry.path())
            .expect("Should read table dir")
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            });

        let Some(data_entry) = data_file else {
            eprintln!("Data.db not found, skipping test");
            return;
        };

        let data_path = data_entry.path();

        // Test detect_component_files
        let components = SSTableReader::detect_component_files(&data_path)
            .await
            .expect("Should detect component files");

        eprintln!("Detected {} component files:", components.len());
        for (component_type, path) in &components {
            eprintln!("  {}: {}", component_type, path.display());
        }

        // simple_table should have standard components
        assert!(
            components.contains_key("Index") || components.contains_key("Statistics"),
            "Should detect at least Index or Statistics component"
        );
    }

    #[tokio::test]
    async fn test_validate_component_integrity_with_real_data() {
        // This test requires CQLITE_DATASETS_ROOT to be set
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic directory not found, skipping test");
            return;
        }

        // Find simple_table directory
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .expect("Should read directory")
            .filter_map(|e| e.ok())
            .find(|e| {
                e.path().is_dir()
                    && e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
            });

        let Some(table_entry) = table_dir else {
            eprintln!("simple_table not found, skipping test");
            return;
        };

        // Find Data.db file
        let data_file = std::fs::read_dir(table_entry.path())
            .expect("Should read table dir")
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            });

        let Some(data_entry) = data_file else {
            eprintln!("Data.db not found, skipping test");
            return;
        };

        let data_path = data_entry.path();

        // First detect components
        let components = SSTableReader::detect_component_files(&data_path)
            .await
            .expect("Should detect component files");

        // Then validate integrity
        let issues = SSTableReader::validate_component_integrity(&data_path, &components)
            .await
            .expect("Should validate integrity");

        eprintln!("Validation issues: {:?}", issues);

        // Real test data should be valid
        assert!(
            issues.is_empty(),
            "Real test data should have no integrity issues: {:?}",
            issues
        );
    }

    #[tokio::test]
    async fn test_detect_component_files_nonexistent_path() {
        let nonexistent_path = PathBuf::from("/nonexistent/path/nb-1-big-Data.db");

        // This should not panic - should return empty or handle gracefully
        let result = SSTableReader::detect_component_files(&nonexistent_path).await;

        // Result depends on implementation - either Ok with empty map or error
        match result {
            Ok(components) => {
                assert!(
                    components.is_empty(),
                    "Should return empty components for nonexistent path"
                );
            }
            Err(_) => {
                // Also acceptable - error for invalid path
            }
        }
    }
}
