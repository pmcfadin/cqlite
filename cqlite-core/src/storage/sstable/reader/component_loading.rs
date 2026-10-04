//! Component loading methods for SSTableReader
//!
//! This module contains methods for loading SSTable component files
//! (Index.db, Filter.db, Summary.db, Statistics.db) and related operations.

use super::{compression::extract_sstable_base_name, SSTableReader};
use crate::platform::Platform;
use crate::storage::sstable::{
    bloom::BloomFilter,
    index::SSTableIndex,
    index_reader::IndexReader,
    manager_open::{is_fd_exhaustion, is_io_fd_exhaustion},
    statistics_reader::StatisticsReader,
    summary_reader::SummaryReader,
};
use crate::{Error, Result};
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
    /// Load the integrated in-Data.db index, if the header advertises one.
    ///
    /// Issue #2385 / #2395: the separate `Index.db` component is NO LONGER read
    /// here. It is parsed exactly once by [`Self::load_index_reader`] into the
    /// raw-key `IndexReader` that actually serves point lookups (`big_get`) and
    /// full-index scans. The legacy digest/raw-key `SSTableIndex` this used to
    /// build (via `convert_index_reader_to_sstable_index`) was a REDUNDANT second
    /// parse of the same file (#2395's double parse) whose per-entry
    /// `binary_search` + `Vec::insert` build was the O(N²) cold-open cost (#2385) —
    /// and whose only consumers already fall through to the same authoritative
    /// fallbacks when it is `None`:
    /// - `big_get` reaches its `self.index` branch only after the `index_reader`
    ///   fast path (present) soft-misses or is absent; both cases already end at
    ///   the whole-file `scan_for_key` oracle (`data_access/big_point.rs`).
    /// - the range scan's `self.index` path builds entries with `size == 0` (the
    ///   BIG `Index.db` parser stores no partition size), so its `has_zero_size`
    ///   guard ALWAYS degrades it to `sequential_scan` — identical to the `None`
    ///   fallback (`data_access/sequential.rs`).
    ///
    /// The integrated-format Strategy 1 below is retained unchanged (real
    /// Cassandra 5.0 SSTables never set `index_offset`, so it is inert for them).
    pub(super) async fn load_index(
        file: &Arc<tokio::sync::Mutex<BlockSource>>,
        header: &crate::parser::SSTableHeader,
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

        // Strategy 2 (separate Index.db component) retired — parsed once by
        // load_index_reader (issue #2385 / #2395). See the doc comment above.
        tracing::debug!(
            "No integrated index in header; separate Index.db is served by index_reader"
        );
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
                            Err(e) if is_fd_exhaustion(&e) => return Err(e),
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to parse Filter.db component: {}. Bloom filter functionality will be unavailable.",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) if is_io_fd_exhaustion(&e) => return Err(e.into()),
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

    /// Load Index.db reader for partition lookup.
    ///
    /// Issue #2412 (design §A): when `summary_usable` (a `Summary.db` loaded with at
    /// least one sample entry — the authority a bounded lazy walk needs) is `true`,
    /// this defers the whole-file parse via [`IndexReader::open_lazy`] — BIG open
    /// then costs O(summary), not O(partitions). When no usable `Summary.db` exists,
    /// this falls back to today's EAGER parse (§A1's counted FellBack): there is no
    /// summary to bound a lazy walk against, so the full parse happens immediately,
    /// surfaced via this debug trace (never silent) and still counted on the
    /// unchanged `index_parses_total` counter (`parse.rs`'s single full-parse site).
    pub(super) async fn load_index_reader(
        path: &Path,
        platform: &Arc<Platform>,
        cancel: &crate::storage::scan_cancel::ScanCancel,
        summary_usable: bool,
    ) -> Result<IndexLoadOutcome> {
        let Some(base_name) = extract_sstable_base_name(path) else {
            return Ok(IndexLoadOutcome::Absent);
        };
        let Some(parent) = path.parent() else {
            return Ok(IndexLoadOutcome::Absent);
        };
        let index_path = parent.join(format!("{}-Index.db", base_name));

        let open_result = if summary_usable {
            IndexReader::open_lazy(&index_path, platform.clone()).await
        } else {
            tracing::debug!(
                "Index.db FellBack to an eager full parse for {} (issue #2412 §A1): no usable \
                 Summary.db to bound a lazy Summary-guided walk",
                index_path.display()
            );
            IndexReader::open_with_summary_cancellable(&index_path, platform.clone(), None, cancel)
                .await
        };

        match open_result {
            Ok(reader) => {
                tracing::debug!(
                    "Loaded Index.db reader for {} ({})",
                    index_path.display(),
                    if summary_usable { "lazy" } else { "eager" }
                );
                Ok(IndexLoadOutcome::Loaded(Box::new(reader)))
            }
            // A genuinely absent Index.db (some shapes legitimately ship without one)
            // is quiet & expected. A PRESENT-but-unloadable Index.db (open/parse
            // errored) is the silent-degradation class issue #2302 exists to kill:
            // surface it so `iterate_all_partitions` can WARN loud rather than
            // silently full-scan. `IndexReader::open`/`open_lazy` both return
            // `NotFound` iff the file is absent, so the error kind is the
            // authoritative discriminator.
            Err(Error::NotFound(_)) => {
                tracing::debug!("No Index.db present at {}", index_path.display());
                Ok(IndexLoadOutcome::Absent)
            }
            // A mid-parse cancellation (issue #2383) aborts the open, never masked
            // as a present-but-unloadable degradation.
            Err(e @ Error::Cancelled) => Err(e),
            Err(e) if is_fd_exhaustion(&e) => Err(e),
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
    ) -> Result<Option<SummaryReader>> {
        let Some(base_name) = extract_sstable_base_name(path) else {
            return Ok(None);
        };
        let Some(parent) = path.parent() else {
            return Ok(None);
        };
        let summary_path = parent.join(format!("{}-Summary.db", base_name));

        match SummaryReader::open(&summary_path, platform.clone()).await {
            Ok(reader) => {
                tracing::debug!("Loaded Summary.db reader for {}", summary_path.display());
                Ok(Some(reader))
            }
            Err(e) if is_fd_exhaustion(&e) => Err(e),
            Err(e) => {
                tracing::debug!("Failed to load Summary.db reader: {}", e);
                Ok(None)
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
    /// Issue #2385: the production caller (the retired `SSTableIndex` conversion)
    /// is gone; the equivalent public helper is
    /// `crate::storage::sstable::extract_keyspace_and_table_name`. Retained under
    /// `cfg(test)` for the path-parsing unit tests below.
    ///
    /// # Errors
    /// Returns error if path doesn't match expected structure.
    #[cfg(test)]
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
#[path = "component_loading_tests.rs"]
mod tests;
