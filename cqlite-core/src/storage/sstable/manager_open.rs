//! `SSTableManager` construction and initial discovery, split out of
//! `sstable/mod.rs` per the campsite rule (epic #1116).
//!
//! Holds the two PUBLIC constructors — [`SSTableManager::new`] (scan a base path)
//! and [`SSTableManager::new_from_discovered_paths`] (pre-discovered table dirs) —
//! together with the best-effort load routines they drive. Grouping them here puts
//! the boundary checks and the load behaviour those checks exist to protect against
//! (a per-file reader error is LOGGED AND SKIPPED, so nothing else would report a
//! systematically failing open) in one readable place.

use super::{
    build_chunk_cache, is_apple_double_sidecar, refresh, SSTableId, SSTableManager,
    MAX_SSTABLE_SCAN_DEPTH,
};
use crate::platform::Platform;
use crate::{Config, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

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
        // Reject an out-of-range `direct_io_memory_fraction` before any
        // filesystem work (#1696 roborev r3 F2). This constructor is public, so
        // it is a boundary in its own right — and `load_existing_sstables`
        // treats a per-file reader-open error as best-effort (log and skip), so
        // without this an invalid fraction would build a manager holding ZERO
        // readers and report success. One rule, one definition:
        // `validated_direct_io_memory_fraction`.
        config.storage.validated_direct_io_memory_fraction()?;

        let base_path = path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));
        let table_readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            table_readers,
            platform,
            config: config.clone(),
            discovery_source: refresh::DiscoverySource::BasePath,
            refresh_lock: Arc::new(Mutex::new(())),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
            chunk_cache: build_chunk_cache(config),
            #[cfg(test)]
            scan_gate: std::sync::Mutex::new(None),
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
    /// Returns an error if the configuration is invalid (an out-of-range
    /// `storage.direct_io_memory_fraction`, checked before any filesystem work),
    /// or if any of the specified directories cannot be read.
    /// Individual SSTable loading errors are logged but do not fail the entire operation —
    /// which is exactly why a config defect must be rejected here rather than left
    /// to the reader opens it would silently swallow (#1696).
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
        // Same fraction check, same reason, as `new` above (#1696 roborev r3 F2).
        config.storage.validated_direct_io_memory_fraction()?;

        let base_path = storage_path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));
        let table_readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            table_readers,
            platform: platform.clone(),
            config: config.clone(),
            discovery_source: refresh::DiscoverySource::TableDirs(table_dirs.clone()),
            refresh_lock: Arc::new(Mutex::new(())),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
            chunk_cache: build_chunk_cache(config),
            #[cfg(test)]
            scan_gate: std::sync::Mutex::new(None),
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

        tracing::debug!(
            "SSTableManager::load_from_table_directories: processing {} directories",
            table_dirs.len()
        );

        for table_dir in table_dirs {
            // Check if directory exists
            if !self.platform.fs().exists(&table_dir).await? {
                tracing::warn!("Table directory does not exist: {:?}", table_dir);
                continue;
            }

            tracing::debug!("SSTableManager scanning directory: {:?}", table_dir);

            // Read directory contents
            let mut dir_entries = match self.platform.fs().read_dir(&table_dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!("Cannot read table directory {:?}: {}", table_dir, e);
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
                        tracing::debug!("SSTableManager found SSTable file: {:?}", path);

                        let sstable_id = SSTableId::from_filename(filename);
                        // Open + wire registries via the shared helper so refresh
                        // opens readers identically (issue #1749). A per-file open
                        // error is logged and skipped here (best-effort load).
                        match self.open_reader_with_schema(&path).await {
                            Ok(reader_arc) => {
                                tracing::debug!(
                                    "SSTableManager successfully loaded SSTable: {}",
                                    sstable_id.0
                                );

                                // Store by SSTableId (existing)
                                readers.insert(sstable_id, reader_arc.clone());

                                // Fully-qualified "keyspace.table" key (or unqualified
                                // fallback) via the shared keying helper (Issue #680).
                                if let Some(key) = refresh::table_dir_table_key(&path) {
                                    tracing::debug!(
                                        "SSTableManager mapping table '{}' to SSTable '{}'",
                                        key,
                                        path.display()
                                    );
                                    table_readers
                                        .entry(key)
                                        .or_insert_with(Vec::new)
                                        .push(reader_arc);
                                } else {
                                    tracing::warn!(
                                        "SSTableManager could not extract table name from path: {}",
                                        path.display()
                                    );
                                }
                            }
                            Err(e) => {
                                // Log warning but continue loading other SSTables
                                tracing::warn!("Could not load SSTable file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }

            tracing::debug!(
                "SSTableManager directory scan complete: found {} Data.db files in {:?}",
                files_found,
                table_dir
            );
        }

        tracing::debug!("SSTableManager total SSTables loaded: {}", readers.len());
        tracing::debug!(
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
            // Open + wire registries via the shared helper so refresh opens
            // readers identically (issue #1749). Don't fail the whole load if one
            // file is problematic — skip it (best-effort initial load).
            match self.open_reader_with_schema(&path).await {
                Ok(reader_arc) => {
                    // Store by SSTableId
                    readers.insert(sstable_id, reader_arc.clone());

                    // Fully-qualified "keyspace.table" key (base-dir-excluded, with
                    // header fallback) via the shared keying helper (Issue #680).
                    let table_key = refresh::base_path_table_key(
                        &path,
                        &base_dir_name,
                        &reader_arc.header().table_name,
                    );

                    if let Some(key) = table_key {
                        tracing::debug!(
                            "SSTableManager mapping table '{}' to SSTable '{}'",
                            key,
                            path.display()
                        );
                        table_readers
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(reader_arc);
                    } else {
                        tracing::warn!(
                            "SSTableManager could not determine table name for: {}",
                            path.display()
                        );
                    }
                }
                Err(_) => {
                    // Skip problematic SSTable files during initialization
                    tracing::warn!("Could not load SSTable file: {:?}", path);
                }
            }
        }

        Ok(())
    }
}
