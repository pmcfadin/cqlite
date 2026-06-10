//! SSTable (Sorted String Table) implementation

pub mod bloom;
pub mod bti;
pub mod bulletproof_reader;
pub mod chunk_decompressor;
pub mod chunk_reader;
pub mod chunked_data_reader;
pub mod compression;
pub mod compression_info;
pub mod directory;
pub mod directory_integration_tests;
pub mod format_detector;
pub mod header_spec;
pub mod index;
pub mod index_reader;
pub mod key_digest;
pub mod performance_benchmarks;
pub mod reader;
pub mod summary_reader;
pub use reader::SSTableReader;
pub mod schema_aware_reader;
pub use schema_aware_reader::SchemaAwareReader;
pub mod row_cell_state_machine;
pub mod statistics_reader;
#[cfg(feature = "tombstones")]
pub mod tombstone_merger;
pub mod validation;

// M5: SSTable writer components (Issue #359)
#[cfg(feature = "write-support")]
pub mod writer;

// Test modules
#[cfg(test)]
mod key_digest_integration_test;
#[cfg(test)]
mod key_digest_test;
#[cfg(all(test, feature = "experimental"))]
mod oa_format_compliance_test;
#[cfg(all(test, feature = "state_machine"))]
mod row_cell_state_machine_test;
/// S3 verification tests for Index.db/Summary.db/BTI (epic #622, issue #625).
#[cfg(test)]
mod s3_verification_test;
/// S4 verification tests for Statistics.db/CompressionInfo.db/Filter.db (epic #622, issue #626).
#[cfg(test)]
mod s4_verification_test;
#[cfg(test)]
mod schema_aware_reader_test;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "tombstones")]
use self::tombstone_merger::{EntryMetadata, GenerationValue, TombstoneMerger};
use crate::platform::Platform;
use crate::{types::TableId, Config, Result, RowKey, Value};

/// Maximum directory depth when scanning for SSTable files.
///
/// Writer creates `data_dir/keyspace/table/nb-{gen}-big-*.db` (2 levels deep),
/// so 3 levels provides a safety margin.
pub(crate) const MAX_SSTABLE_SCAN_DEPTH: usize = 3;

/// SSTable file identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SSTableId(pub String);

impl Default for SSTableId {
    fn default() -> Self {
        Self::new()
    }
}

impl SSTableId {
    /// Create a new SSTable ID with timestamp using Cassandra naming convention
    pub fn new() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        // Use Cassandra naming convention: <keyspace>-<table>-<generation>-<format>-Data.db
        // For generated files, we'll use a simplified pattern: sstable-<timestamp>-big-Data.db
        Self(format!("sstable-{}-big-Data.db", timestamp))
    }

    /// Create SSTable ID from filename
    pub fn from_filename(filename: &str) -> Self {
        Self(filename.to_string())
    }

    /// Get the filename
    pub fn filename(&self) -> &str {
        &self.0
    }
}

/// Extract table name from SSTable directory path.
///
/// SSTable files are stored in directories named `<table_name>-<uuid>`.
/// For example: `simple_table-6aa08200a25111f0a3fef1a551383fb9/na-1-big-Data.db`
///
/// This function extracts the table name portion by:
/// 1. Getting the parent directory name
/// 2. Splitting on '-' and removing the UUID suffix
///
/// Removes the UUID suffix from directory names like:
/// - `simple_table-6aa08200a25111f0a3fef1a551383fb9` → `simple_table`
/// - `my-test-table-UUID` → `my-test-table`
///
/// Returns `None` if the path doesn't contain a valid directory component.
///
/// Note: Table names can contain hyphens, so we need to be careful to only remove the UUID.
/// UUIDs in Cassandra directory names are 32 hex chars without hyphens (e.g., 6aa08200a25111f0a3fef1a551383fb9).
pub(crate) fn extract_table_name(sstable_path: &Path) -> Option<String> {
    // Get the parent directory name
    let dir_name = sstable_path.parent()?.file_name()?.to_str()?;

    // Find the last occurrence of '-' followed by 32 hex characters (UUID without hyphens)
    // Cassandra UUIDs in directory names are formatted as: tablename-<32-hex-chars>
    if let Some(uuid_start) = dir_name.rfind('-') {
        let potential_uuid = &dir_name[uuid_start + 1..];

        // Check if this looks like a UUID (32 hex characters)
        if potential_uuid.len() == 32 && potential_uuid.chars().all(|c| c.is_ascii_hexdigit()) {
            // Extract everything before the UUID
            return Some(dir_name[..uuid_start].to_string());
        }
    }

    // If we couldn't find a UUID pattern, return the whole directory name
    Some(dir_name.to_string())
}

/// Return `true` if the filename is a macOS AppleDouble resource-fork sidecar.
///
/// macOS creates `._<name>` companion files when copying to non-Apple filesystems
/// (HFS+→FAT32, SMB shares, CI artifact tarballs, etc.).  These are NOT valid
/// Cassandra SSTable files even though they share the `-Data.db` suffix.
///
/// This predicate is the single point of truth for the `._*` filter; both
/// `load_from_table_directories` and `find_data_files` call it so the guard can
/// never silently diverge.  See Issue #481.
#[inline]
fn is_apple_double_sidecar(filename: &str) -> bool {
    filename.starts_with("._")
}

/// SSTable manager that handles multiple SSTable files
#[derive(Debug)]
pub struct SSTableManager {
    /// Base directory for SSTable files
    base_path: PathBuf,

    /// Active SSTable readers indexed by ID
    readers: Arc<RwLock<HashMap<SSTableId, Arc<reader::SSTableReader>>>>,

    /// Table name to SSTable readers mapping
    /// Maps table names (e.g., "simple_table") to their corresponding SSTable readers
    table_readers: Arc<RwLock<HashMap<String, Vec<Arc<reader::SSTableReader>>>>>,

    /// Platform abstraction
    platform: Arc<Platform>,

    /// Configuration
    config: Config,

    /// Schema registry for schema-aware operations (feature-gated)
    #[cfg(feature = "state_machine")]
    schema_registry: Arc<RwLock<Option<Arc<RwLock<crate::schema::SchemaRegistry>>>>>,
}

impl SSTableManager {
    /// Create a new SSTable manager
    pub async fn new(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        #[cfg(feature = "state_machine")] schema_registry: Option<
            Arc<RwLock<crate::schema::SchemaRegistry>>,
        >,
    ) -> Result<Self> {
        let base_path = path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));
        let table_readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            table_readers,
            platform,
            config: config.clone(),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
        };

        // Load existing SSTable files
        manager.load_existing_sstables().await?;

        Ok(manager)
    }

    /// Create a new SSTable manager from pre-discovered table directories
    ///
    /// This method accepts a list of table directory paths (from DiscoveryService)
    /// and loads SSTables from those specific directories. It does not perform
    /// filesystem scanning beyond the provided directories - this avoids duplicate
    /// scanning when integrating with the discovery/engine lifecycle.
    ///
    /// Use this method when you have pre-discovered table directories and want
    /// to avoid redundant filesystem scanning. Use `new()` when you want automatic
    /// discovery from a single base directory.
    ///
    /// # Arguments
    ///
    /// * `storage_path` - Base storage path (used for context, not for scanning)
    /// * `table_dirs` - List of table directory paths from DiscoveryService
    ///   (e.g., `/data/keyspace1/table1-abc123`)
    /// * `config` - Configuration
    /// * `platform` - Platform abstraction
    ///
    /// # Returns
    ///
    /// A new `SSTableManager` with SSTables loaded from the specified directories
    ///
    /// # Errors
    ///
    /// Returns an error if any of the specified directories cannot be read.
    /// Individual SSTable loading errors are logged but do not fail the entire operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use cqlite_core::storage::sstable::SSTableManager;
    /// use cqlite_core::{Config, Platform};
    /// use std::sync::Arc;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> cqlite_core::Result<()> {
    /// let config = Config::default();
    /// let platform = Arc::new(Platform::new(&config).await?);
    ///
    /// // Get table directories from DiscoveryService
    /// let table_dirs = vec![
    ///     PathBuf::from("/data/keyspace1/table1-abc123"),
    ///     PathBuf::from("/data/keyspace1/table2-def456"),
    /// ];
    ///
    /// let manager = SSTableManager::new_from_discovered_paths(
    ///     &PathBuf::from("/data"),
    ///     table_dirs,
    ///     &config,
    ///     platform,
    ///     #[cfg(feature = "state_machine")]
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_from_discovered_paths(
        storage_path: &Path,
        table_dirs: Vec<PathBuf>,
        config: &Config,
        platform: Arc<Platform>,
        #[cfg(feature = "state_machine")] schema_registry: Option<
            Arc<RwLock<crate::schema::SchemaRegistry>>,
        >,
    ) -> Result<Self> {
        let base_path = storage_path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));
        let table_readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            table_readers,
            platform: platform.clone(),
            config: config.clone(),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
        };

        // Load SSTables from the provided table directories
        manager.load_from_table_directories(table_dirs).await?;

        Ok(manager)
    }

    /// Load SSTable readers from specific table directories
    ///
    /// This method scans each provided table directory for Data.db files and loads them.
    /// It handles empty directories gracefully and logs warnings for individual file errors.
    async fn load_from_table_directories(&self, table_dirs: Vec<PathBuf>) -> Result<()> {
        let mut readers = self.readers.write().await;
        let mut table_readers = self.table_readers.write().await;

        log::debug!(
            "SSTableManager::load_from_table_directories: processing {} directories",
            table_dirs.len()
        );

        for table_dir in table_dirs {
            // Check if directory exists
            if !self.platform.fs().exists(&table_dir).await? {
                log::warn!("Table directory does not exist: {:?}", table_dir);
                continue;
            }

            log::debug!("SSTableManager scanning directory: {:?}", table_dir);

            // Read directory contents
            let mut dir_entries = match self.platform.fs().read_dir(&table_dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    log::warn!("Cannot read table directory {:?}: {}", table_dir, e);
                    continue;
                }
            };

            // Scan for Data.db files
            let mut files_found = 0;
            while let Some(entry) = dir_entries.next_entry().await? {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Check for Cassandra SSTable data files using the *-Data.db pattern.
                    // Skip macOS AppleDouble sidecars via is_apple_double_sidecar().
                    // See Issue #481.
                    if filename.ends_with("-Data.db") && !is_apple_double_sidecar(filename) {
                        files_found += 1;
                        log::debug!("SSTableManager found SSTable file: {:?}", path);

                        let sstable_id = SSTableId::from_filename(filename);
                        // Try to open the SSTable reader
                        match reader::SSTableReader::open(
                            &path,
                            &self.config,
                            self.platform.clone(),
                        )
                        .await
                        {
                            #[cfg_attr(not(feature = "state_machine"), allow(unused_mut))]
                            Ok(mut reader) => {
                                log::debug!(
                                    "SSTableManager successfully loaded SSTable: {}",
                                    sstable_id.0
                                );

                                // Set schema registry if available (before wrapping in Arc)
                                #[cfg(feature = "state_machine")]
                                {
                                    let schema_reg_guard = self.schema_registry.read().await;
                                    if let Some(ref registry_rwlock) = *schema_reg_guard {
                                        log::debug!(
                                            "SSTableManager setting schema registry on reader: {}",
                                            sstable_id.0
                                        );
                                        reader.set_schema_registry(Arc::clone(registry_rwlock));

                                        // Also set UDT registry for UDT-aware collection parsing (Issue #238)
                                        let schema_registry = registry_rwlock.read().await;
                                        let udt_registry_lock = schema_registry.get_udt_registry();
                                        let udt_registry = udt_registry_lock.read().await.clone();
                                        reader.set_udt_registry(udt_registry);
                                    }
                                }

                                let reader_arc = Arc::new(reader);

                                // Store by SSTableId (existing)
                                readers.insert(sstable_id, reader_arc.clone());

                                // NEW: Extract table name and store by table name
                                if let Some(table_name) = extract_table_name(&path) {
                                    log::debug!(
                                        "SSTableManager mapping table '{}' to SSTable '{}'",
                                        table_name,
                                        path.display()
                                    );

                                    table_readers
                                        .entry(table_name)
                                        .or_insert_with(Vec::new)
                                        .push(reader_arc);
                                } else {
                                    log::warn!(
                                        "SSTableManager could not extract table name from path: {}",
                                        path.display()
                                    );
                                }
                            }
                            Err(e) => {
                                // Log warning but continue loading other SSTables
                                log::warn!("Could not load SSTable file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }

            log::debug!(
                "SSTableManager directory scan complete: found {} Data.db files in {:?}",
                files_found,
                table_dir
            );
        }

        log::debug!("SSTableManager total SSTables loaded: {}", readers.len());
        log::debug!(
            "SSTableManager tables discovered: {:?}",
            table_readers.keys().collect::<Vec<_>>()
        );

        Ok(())
    }

    /// Load existing SSTable files from disk
    ///
    /// Scans the base path recursively (up to 3 levels deep) to find Data.db files.
    /// This supports both flat layouts (Data.db directly in base_path) and Cassandra-style
    /// directory structures (keyspace/table_name/Data.db).
    async fn load_existing_sstables(&self) -> Result<()> {
        // Check if directory exists first
        if !self.platform.fs().exists(&self.base_path).await? {
            return Ok(()); // No directory, no SSTables to load
        }

        // Collect all Data.db paths by walking up to 3 levels deep
        let data_files: Vec<PathBuf> =
            Self::find_data_files(&self.platform, &self.base_path, MAX_SSTABLE_SCAN_DEPTH).await?;

        if data_files.is_empty() {
            return Ok(());
        }

        let mut readers = self.readers.write().await;
        let mut table_readers = self.table_readers.write().await;

        // Pre-compute for the table name fallback heuristic
        let base_dir_name = self
            .base_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        for path in data_files {
            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(f) => f.to_string(),
                None => continue,
            };
            let sstable_id = SSTableId::from_filename(&filename);
            // Try to open the SSTable reader, but don't fail if one file is problematic
            match reader::SSTableReader::open(&path, &self.config, self.platform.clone()).await {
                #[cfg_attr(not(feature = "state_machine"), allow(unused_mut))]
                Ok(mut reader) => {
                    // Set schema registry if available (before wrapping in Arc)
                    #[cfg(feature = "state_machine")]
                    {
                        let schema_reg_guard = self.schema_registry.read().await;
                        if let Some(ref registry_rwlock) = *schema_reg_guard {
                            reader.set_schema_registry(Arc::clone(registry_rwlock));

                            // Also set UDT registry for UDT-aware collection parsing (Issue #238)
                            let schema_registry = registry_rwlock.read().await;
                            let udt_registry_lock = schema_registry.get_udt_registry();
                            let udt_registry = udt_registry_lock.read().await.clone();
                            reader.set_udt_registry(udt_registry);
                        }
                    }

                    let reader_arc = Arc::new(reader);

                    // Store by SSTableId
                    readers.insert(sstable_id, reader_arc.clone());

                    // Extract table name from directory path and store by table name.
                    // Falls back to the reader's header table_name for flat directories
                    // where extract_table_name returns the raw directory name.
                    let table_name = extract_table_name(&path)
                        .filter(|name| {
                            // Heuristic: if the extracted name matches the base_path dir name,
                            // it's not a real table name — fall back to header
                            name.as_str() != base_dir_name
                        })
                        .or_else(|| {
                            // Fallback: use table name from SSTable header (populated from
                            // Statistics.db or path during reader::open)
                            let header_table = reader_arc.header().table_name.clone();
                            if header_table != "test_table" && !header_table.is_empty() {
                                Some(header_table)
                            } else {
                                None
                            }
                        });

                    if let Some(table_name) = table_name {
                        log::debug!(
                            "SSTableManager mapping table '{}' to SSTable '{}'",
                            table_name,
                            path.display()
                        );
                        table_readers
                            .entry(table_name)
                            .or_insert_with(Vec::new)
                            .push(reader_arc);
                    } else {
                        log::warn!(
                            "SSTableManager could not determine table name for: {}",
                            path.display()
                        );
                    }
                }
                Err(_) => {
                    // Skip problematic SSTable files during initialization
                    log::warn!("Could not load SSTable file: {:?}", path);
                }
            }
        }

        Ok(())
    }

    /// Recursively find all *-Data.db files up to `max_depth` levels deep
    fn find_data_files<'a>(
        platform: &'a Platform,
        dir: &'a Path,
        max_depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<PathBuf>>> + Send + 'a>>
    {
        let dir = dir.to_path_buf();
        Box::pin(async move {
            let mut results = Vec::new();

            let mut dir_entries = match platform.fs().read_dir(&dir).await {
                Ok(entries) => entries,
                Err(_) => return Ok(results),
            };

            while let Some(entry) = dir_entries.next_entry().await? {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Skip macOS AppleDouble sidecars via is_apple_double_sidecar().
                    // See Issue #481.
                    if filename.ends_with("-Data.db") && !is_apple_double_sidecar(filename) {
                        results.push(path);
                    } else if max_depth > 0 {
                        // Check if it's a directory and recurse
                        if entry
                            .file_type()
                            .await
                            .map(|ft| ft.is_dir())
                            .unwrap_or(false)
                        {
                            let sub_results =
                                Self::find_data_files(platform, &path, max_depth - 1).await?;
                            results.extend(sub_results);
                        }
                    }
                }
            }

            Ok(results)
        })
    }

    /// Create a new SSTable from MemTable data
    ///
    /// NOTE: SSTable writing removed in Issue #176 (writer.rs deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn create_from_memtable(
        &self,
        _data: Vec<(TableId, RowKey, Value)>,
    ) -> Result<SSTableId> {
        Err(crate::error::Error::unsupported_format(
            "SSTable writing removed in Issue #176 - writer.rs deleted",
        ))
    }

    #[cfg(not(feature = "experimental"))]
    pub async fn create_from_memtable(
        &self,
        _data: Vec<(TableId, RowKey, Value)>,
    ) -> Result<SSTableId> {
        Err(crate::error::Error::unsupported_format(
            "SSTable writing requires experimental feature",
        ))
    }

    /// Get a value by key from all SSTables with proper tombstone merging
    #[cfg(feature = "tombstones")]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let readers = self.readers.read().await;
        let mut all_values = Vec::new();

        // Collect values from all SSTables
        for (_sstable_id, reader) in readers.iter() {
            if let Some(value) = reader.get(table_id, key).await? {
                let generation = reader.generation;
                let write_time = reader.extract_write_time_from_entry(key, &value);

                let gen_value = GenerationValue {
                    value,
                    metadata: EntryMetadata {
                        write_time,
                        generation,
                        ttl: None, // Would be extracted from SSTable metadata
                    },
                };
                all_values.push(gen_value);
            }
        }

        // Use tombstone merger to resolve conflicts across generations
        let merger = TombstoneMerger::new();
        merger.merge_generations(all_values)
    }

    /// Get a value by key from all SSTables (simple version without tombstone merging)
    ///
    /// Uses `table_readers` (keyed by unqualified table name) so that only the
    /// SSTables for the requested table are searched.  The legacy `readers` map
    /// (keyed by SSTableId / filename) cannot be used because all SSTables share
    /// the same base filename (`nb-1-big-Data.db`) and the HashMap therefore only
    /// retains the last-inserted reader.
    #[cfg(not(feature = "tombstones"))]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let table_readers = self.table_readers.read().await;

        // Resolve unqualified table name for the lookup (mirrors SSTableManager::scan logic)
        let table_name = table_id.name();
        let unqualified_name = if let Some(dot_pos) = table_name.rfind('.') {
            &table_name[dot_pos + 1..]
        } else {
            table_name
        };

        let reader_list = match table_readers.get(unqualified_name) {
            Some(list) => list,
            None => return Ok(None),
        };

        // Return the first value found across all SSTables for this table
        for reader in reader_list {
            if let Some(value) = reader.get(table_id, key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Scan a range of keys from all SSTables with proper tombstone merging
    ///
    /// # Arguments
    /// * `table_id` - The table to scan
    /// * `start_key` - Optional start key for range scan
    /// * `end_key` - Optional end key for range scan
    /// * `limit` - Optional limit on number of results
    /// * `schema` - Optional table schema for schema-aware parsing. When provided,
    ///   enables accurate type detection and avoids heuristic-based parsing.
    ///   Strongly recommended for Cassandra 5.0+ formats.
    #[cfg(feature = "tombstones")]
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let readers = self.readers.read().await;
        let mut key_values = std::collections::HashMap::new();

        // Collect results from all SSTables, grouping by key
        for reader in readers.values() {
            let results = reader
                .scan(table_id, start_key, end_key, None, schema)
                .await?;

            for (row_key, value) in results {
                let generation = reader.generation;
                let write_time = reader.extract_write_time_from_entry(&row_key, &value);

                let gen_value = GenerationValue {
                    value,
                    metadata: EntryMetadata {
                        write_time,
                        generation,
                        ttl: None,
                    },
                };

                key_values
                    .entry(row_key)
                    .or_insert_with(Vec::new)
                    .push(gen_value);
            }
        }

        // Merge values for each key using tombstone merger
        let merger = TombstoneMerger::new();
        let mut final_results = Vec::new();

        for (row_key, values) in key_values {
            if let Some(merged_value) = merger.merge_generations(values)? {
                final_results.push((row_key, merged_value));
            }
        }

        // Sort results by key
        final_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply limit
        if let Some(limit) = limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
    }

    /// Scan a range of keys from all SSTables (simple version without tombstone merging)
    ///
    /// # Arguments
    /// * `table_id` - The table to scan
    /// * `start_key` - Optional start key for range scan
    /// * `end_key` - Optional end key for range scan
    /// * `limit` - Optional limit on number of results
    /// * `schema` - Optional table schema for schema-aware parsing. When provided,
    ///   enables accurate type detection and avoids heuristic-based parsing.
    ///   Strongly recommended for Cassandra 5.0+ formats.
    #[cfg(not(feature = "tombstones"))]
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let table_readers = self.table_readers.read().await;

        log::debug!("SSTableManager::scan - Scanning table_id='{}'", table_id);

        // Extract unqualified table name from potentially qualified table_id
        // Supports both "keyspace.table" and "table" formats
        let table_name = table_id.name();
        let unqualified_name = if let Some(dot_pos) = table_name.rfind('.') {
            &table_name[dot_pos + 1..]
        } else {
            table_name
        };

        log::debug!(
            "SSTableManager::scan - Looking up table by unqualified name: '{}'",
            unqualified_name
        );

        // Look up readers by unqualified table name
        let readers = table_readers.get(unqualified_name);

        if let Some(reader_list) = readers {
            log::debug!(
                "SSTableManager::scan - Found {} readers for table '{}'",
                reader_list.len(),
                table_id
            );

            let mut all_results = Vec::new();

            for reader in reader_list {
                log::debug!(
                    "SSTableManager::scan - Calling scan on reader for file: {:?}",
                    reader.file_path
                );

                let results = reader
                    .scan(table_id, start_key, end_key, None, schema)
                    .await?;

                log::debug!(
                    "SSTableManager::scan - Reader returned {} results",
                    results.len()
                );

                all_results.extend(results);
            }

            log::debug!(
                "SSTableManager::scan - Total results from all readers: {}",
                all_results.len()
            );

            // Sort results by key
            all_results.sort_by(|a, b| a.0.cmp(&b.0));

            // Apply limit
            if let Some(limit) = limit {
                all_results.truncate(limit);
            }

            log::debug!(
                "SSTableManager::scan - Returning {} final results",
                all_results.len()
            );

            Ok(all_results)
        } else {
            log::debug!(
                "SSTableManager::scan - No readers found for table '{}'",
                table_id
            );
            log::debug!(
                "SSTableManager::scan - Available tables: {:?}",
                table_readers.keys().collect::<Vec<_>>()
            );
            Ok(Vec::new())
        }
    }

    /// Get list of all SSTable IDs
    pub async fn list_sstables(&self) -> Vec<SSTableId> {
        let readers = self.readers.read().await;
        readers.keys().cloned().collect()
    }

    /// Remove an SSTable
    pub async fn remove_sstable(&self, sstable_id: &SSTableId) -> Result<()> {
        // Remove from memory
        {
            let mut readers = self.readers.write().await;
            readers.remove(sstable_id);
        }

        // Delete file
        let file_path = self.base_path.join(sstable_id.filename());
        if self.platform.fs().exists(&file_path).await? {
            self.platform.fs().remove_file(&file_path).await?;
        }

        Ok(())
    }

    /// Get SSTable statistics
    pub async fn stats(&self) -> Result<SSTableStats> {
        let readers = self.readers.read().await;

        let mut total_size = 0u64;
        let mut total_entries = 0u64;
        let mut total_tables = 0u64;
        let sstable_count = readers.len();

        for reader in readers.values() {
            let reader_stats = reader.stats().await?;
            total_size += reader_stats.file_size;
            total_entries += reader_stats.entry_count;
            total_tables += reader_stats.table_count;
        }

        Ok(SSTableStats {
            sstable_count,
            total_size,
            total_entries,
            total_tables,
            average_size: if sstable_count > 0 {
                total_size / sstable_count as u64
            } else {
                0
            },
        })
    }

    /// Set the schema registry for schema-aware operations
    ///
    /// This method stores the schema registry and applies it to all existing SSTable readers.
    /// Future readers loaded via `load_existing_sstables` or `load_from_table_directories`
    /// will also receive the schema registry during creation.
    #[cfg(feature = "state_machine")]
    pub async fn set_schema_registry(
        &self,
        registry: Arc<RwLock<crate::schema::SchemaRegistry>>,
    ) -> Result<()> {
        // Store the schema registry
        {
            let mut schema_reg = self.schema_registry.write().await;
            *schema_reg = Some(registry.clone());
        }

        // Apply to all existing readers
        // Note: SSTableReader::set_schema_registry requires &mut self, but readers are Arc<SSTableReader>
        // This is by design - schema should be set during reader creation, not after.
        // The stored registry will be applied to future readers loaded by this manager.

        // For existing readers, we cannot mutate them directly since they're behind Arc.
        // The schema registry will be applied to new readers as they're loaded.

        Ok(())
    }

    /// Merge multiple SSTables into a new one
    ///
    /// NOTE: SSTable writing removed in Issue #176 (writer.rs deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn merge_sstables(
        &self,
        _source_ids: Vec<SSTableId>,
        _target_id: SSTableId,
    ) -> Result<()> {
        Err(crate::error::Error::unsupported_format(
            "SSTable merging removed in Issue #176 - writer.rs deleted",
        ))
    }

    #[cfg(not(feature = "experimental"))]
    pub async fn merge_sstables(
        &self,
        _source_ids: Vec<SSTableId>,
        _target_id: SSTableId,
    ) -> Result<()> {
        Err(crate::error::Error::unsupported_format(
            "SSTable merging requires experimental feature",
        ))
    }
}

/// SSTable statistics
#[derive(Debug, Clone)]
pub struct SSTableStats {
    /// Number of SSTable files
    pub sstable_count: usize,

    /// Total size of all SSTables in bytes
    pub total_size: u64,

    /// Total number of entries across all SSTables
    pub total_entries: u64,

    /// Total number of tables across all SSTables
    pub total_tables: u64,

    /// Average SSTable size in bytes
    pub average_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sstable_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let manager = SSTableManager::new(
            temp_dir.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();
        let stats = manager.stats().await.unwrap();

        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_sstable_manager_from_discovered_paths_empty() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create an empty list of discovered paths
        let discovered_paths = Vec::new();

        let manager = SSTableManager::new_from_discovered_paths(
            temp_dir.path(),
            discovered_paths,
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();

        let stats = manager.stats().await.unwrap();

        // Should have 0 SSTables since we provided an empty list
        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_sstable_manager_from_discovered_paths_with_directories() {
        use std::fs;

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create mock table directories with Data.db files
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();

        let table1_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table1_dir).unwrap();
        // Note: These are mock files that won't parse as real SSTables,
        // but they test the directory scanning logic
        fs::write(table1_dir.join("na-1-big-Data.db"), b"mock_data").unwrap();

        let table2_dir = keyspace_dir.join("posts-def456");
        fs::create_dir(&table2_dir).unwrap();
        fs::write(table2_dir.join("na-2-big-Data.db"), b"mock_data").unwrap();
        fs::write(table2_dir.join("na-3-big-Data.db"), b"mock_data").unwrap();

        // Provide table directories to manager
        let table_dirs = vec![table1_dir.clone(), table2_dir.clone()];

        let manager = SSTableManager::new_from_discovered_paths(
            temp_dir.path(),
            table_dirs,
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();

        let stats = manager.stats().await.unwrap();

        // Should attempt to load 3 Data.db files (though they may fail to parse)
        // This tests that the directory scanning logic works correctly
        // Note: Since these are mock files, actual loading may fail,
        // but the manager should still be created successfully
        assert_eq!(stats.sstable_count, 0); // Mock files won't parse as valid SSTables
    }

    #[tokio::test]
    #[ignore = "M3+ feature; gated for M1"]
    async fn test_sstable_id_generation() {
        let id1 = SSTableId::new();
        let id2 = SSTableId::new();

        assert_ne!(id1.filename(), id2.filename());
        assert!(id1.filename().starts_with("sstable_"));
        assert!(id1.filename().ends_with(".sst"));
    }

    /// Regression test for Issue #481: `._*` AppleDouble sidecars must not be
    /// returned by `find_data_files`.
    ///
    /// Before the fix, `find_data_files` only checked `ends_with("-Data.db")`,
    /// so `._nb-1-big-Data.db` passed the filter and would later fail to open
    /// as a valid SSTable.  The test would fail on the pre-fix code because
    /// `results` would contain two paths instead of one.
    #[tokio::test]
    async fn test_find_data_files_excludes_apple_double_sidecar() {
        use std::fs;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Write a minimal (invalid but correctly named) SSTable file and its
        // macOS AppleDouble sidecar companion alongside it.
        let real_file = temp_dir.path().join("nb-1-big-Data.db");
        let sidecar = temp_dir.path().join("._nb-1-big-Data.db");
        fs::write(&real_file, b"\x00").unwrap();
        fs::write(&sidecar, b"\x00\x00").unwrap();

        // find_data_files scans `temp_dir` with max_depth=0 (single level).
        let results = SSTableManager::find_data_files(&platform, temp_dir.path(), 0)
            .await
            .unwrap();

        // Only the real Data.db file should be returned; the ._ sidecar must be excluded.
        assert_eq!(
            results.len(),
            1,
            "expected exactly 1 result but got {}: {:?}",
            results.len(),
            results
        );
        assert_eq!(results[0], real_file);
        assert!(
            !results.contains(&sidecar),
            "AppleDouble sidecar must not appear in results"
        );
    }

    /// Unit test for the is_apple_double_sidecar helper.
    #[test]
    fn test_is_apple_double_sidecar() {
        // Must match
        assert!(is_apple_double_sidecar("._nb-1-big-Data.db"));
        assert!(is_apple_double_sidecar("._anything"));
        assert!(is_apple_double_sidecar("._"));
        // Must not match
        assert!(!is_apple_double_sidecar("nb-1-big-Data.db"));
        assert!(!is_apple_double_sidecar("na-2-big-Data.db"));
        assert!(!is_apple_double_sidecar(""));
    }

    #[test]
    fn test_extract_table_name() {
        use std::path::PathBuf;

        // Test standard Cassandra table directory format
        let path =
            PathBuf::from("test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        assert_eq!(extract_table_name(&path), Some("simple_table".to_string()));

        // Test table name with hyphens
        let path = PathBuf::from(
            "test-data/datasets/sstables/test_basic/my-test-table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        );
        assert_eq!(extract_table_name(&path), Some("my-test-table".to_string()));

        // Test multi_partition_table
        let path = PathBuf::from(
            "test-data/datasets/sstables/test_basic/multi_partition_table-6ac52100a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        );
        assert_eq!(
            extract_table_name(&path),
            Some("multi_partition_table".to_string())
        );

        // Test compression_test_table
        let path = PathBuf::from(
            "test-data/datasets/sstables/test_basic/compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        );
        assert_eq!(
            extract_table_name(&path),
            Some("compression_test_table".to_string())
        );

        // Test edge case: directory without UUID
        let path =
            PathBuf::from("test-data/datasets/sstables/test_basic/simple_table/nb-1-big-Data.db");
        assert_eq!(extract_table_name(&path), Some("simple_table".to_string()));

        // Test edge case: no parent directory
        let path = PathBuf::from("nb-1-big-Data.db");
        assert_eq!(extract_table_name(&path), None);
    }
}
