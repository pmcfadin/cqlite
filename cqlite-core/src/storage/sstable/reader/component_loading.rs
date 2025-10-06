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
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, BufReader};

impl SSTableReader {
    /// Load index from integrated or component-based format
    pub(super) async fn load_index(
        file: &Arc<tokio::sync::Mutex<BufReader<File>>>,
        header: &crate::parser::SSTableHeader,
        platform: &Arc<Platform>,
        data_file_path: &Path,
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
                log::debug!("Loaded integrated index from Data.db at offset {}", offset);
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
                match IndexReader::open(&index_path, platform.clone()).await {
                    Ok(index_reader) => {
                        log::debug!(
                            "Found separate Index.db component at {}",
                            index_path.display()
                        );

                        // Convert IndexReader to SSTableIndex by extracting partition entries
                        match Self::convert_index_reader_to_sstable_index(index_reader).await {
                            Ok(sstable_index) => {
                                log::debug!(
                                    "Successfully converted Index.db component to SSTableIndex"
                                );
                                return Ok(Some(sstable_index));
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to convert Index.db component to SSTableIndex: {}. This may indicate an incompatible Index.db format or corruption.",
                                    e
                                );
                                // Continue to fallback strategies
                            }
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "Failed to load Index.db component: {}. This may indicate file corruption, permission issues, or format incompatibility.",
                            e
                        );
                        // Continue to fallback strategies
                    }
                }
            } else {
                log::debug!(
                    "No Index.db component file found at {}",
                    index_path.display()
                );
            }
        }

        log::debug!("No index source available (neither header offset nor Index.db component)");
        Ok(None)
    }

    /// Load bloom filter from integrated or component-based format
    pub(super) async fn load_bloom_filter(
        file: &Arc<tokio::sync::Mutex<BufReader<File>>>,
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
                log::debug!(
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
                                log::debug!(
                                    "Loaded separate Filter.db component from {}",
                                    filter_path.display()
                                );
                                return Ok(Some(bloom_filter));
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to parse Filter.db component: {}. Bloom filter functionality will be unavailable.",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "Failed to open Filter.db component: {}. Bloom filter functionality will be unavailable.",
                            e
                        );
                    }
                }
            } else {
                log::debug!(
                    "No Filter.db component file found at {}",
                    filter_path.display()
                );
            }
        }

        log::debug!(
            "No bloom filter source available (neither header offset nor Filter.db component)"
        );
        Ok(None)
    }

    /// Load Index.db reader for partition lookup
    pub(super) async fn load_index_reader(
        path: &Path,
        platform: &Arc<Platform>,
    ) -> Option<IndexReader> {
        let base_name = extract_sstable_base_name(path)?;
        let index_path = path.parent()?.join(format!("{}-Index.db", base_name));

        match IndexReader::open(&index_path, platform.clone()).await {
            Ok(reader) => {
                log::debug!("Loaded Index.db reader for {}", index_path.display());
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Index.db reader: {}", e);
                None
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
                log::debug!("Loaded Summary.db reader for {}", summary_path.display());
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Summary.db reader: {}", e);
                None
            }
        }
    }

    /// Load Statistics.db reader for min/max timestamps and metadata
    pub(super) async fn load_statistics_reader(
        path: &Path,
        platform: &Arc<Platform>,
    ) -> Option<StatisticsReader> {
        let base_name = extract_sstable_base_name(path)?;
        let statistics_path = path.parent()?.join(format!("{}-Statistics.db", base_name));

        match StatisticsReader::open(&statistics_path, platform.clone()).await {
            Ok(reader) => {
                log::debug!(
                    "Loaded Statistics.db reader for {}",
                    statistics_path.display()
                );
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Statistics.db reader: {}", e);
                None
            }
        }
    }

    /// Convert IndexReader to SSTableIndex for backward compatibility
    pub(super) async fn convert_index_reader_to_sstable_index(
        index_reader: IndexReader,
    ) -> Result<SSTableIndex> {
        use crate::storage::sstable::index::{Index, IndexEntry};

        let mut index = Index::new();

        // Extract partition entries from IndexReader and convert to IndexEntry format
        let partition_entries = index_reader.get_partition_entries();

        for partition_entry in partition_entries {
            // Convert partition entry to our internal IndexEntry format
            let index_entry = IndexEntry {
                table_id: crate::types::TableId::new("default"),
                key: RowKey::new(partition_entry.key_digest.to_vec()),
                offset: partition_entry.data_offset,
                size: partition_entry.data_size,
                compressed: false,
            };

            // Add to index using default table ID
            index.add_entry(index_entry);
        }

        log::debug!(
            "Converted {} partition entries from IndexReader to SSTableIndex",
            partition_entries.len()
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
                log::warn!(
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
                        log::warn!("Component file is empty: {}", component_path.display());
                        if *is_critical {
                            critical_missing.push(component_type.to_string());
                        }
                    } else {
                        log::debug!(
                            "Found component file: {} (size: {} bytes)",
                            component_path.display(),
                            metadata.len()
                        );
                        components.insert(component_type.to_string(), component_path);
                    }
                }
                Err(_) => {
                    log::debug!("Component file not found: {}", component_path.display());
                    if *is_critical {
                        critical_missing.push(component_type.to_string());
                    }
                }
            }
        }

        // Log component architecture analysis
        if components.is_empty() {
            log::debug!(
                "No component files found for base name: {}. This SSTable likely uses integrated format (all data in Data.db).",
                base_name
            );
        } else {
            log::debug!(
                "Detected {} component files for {} (component-based architecture)",
                components.len(),
                base_name
            );

            if !critical_missing.is_empty() {
                log::warn!(
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
            log::debug!(
                "Component integrity validation passed for {}",
                data_path.display()
            );
        } else {
            log::warn!(
                "Component integrity issues detected for {}: {:?}",
                data_path.display(),
                issues
            );
        }

        Ok(issues)
    }
}
