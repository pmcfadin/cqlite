//! Ingestion Module for M2-CLI One-Shot Execution
//!
//! This module orchestrates schema loading and SSTable discovery to build
//! a fully-configured Database instance for one-shot query execution.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::discovery::DiscoveryService;
use crate::error::{Error, Result};
use crate::schema::{
    aggregator::{AggregatorConfig, LoadResult, SchemaAggregator},
    registry::{SchemaRegistry, SchemaRegistryConfig},
};
use crate::Config;
use crate::Database;
use crate::Platform;

/// Configuration for one-shot ingestion
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Schema file paths (.cql or .json) to load
    pub schema_paths: Vec<PathBuf>,

    /// Root data directory containing SSTables (e.g., /var/lib/cassandra/data)
    pub data_dir: PathBuf,

    /// Optional Cassandra version hint (e.g., "5.0")
    pub version_hint: Option<String>,

    /// Core database configuration
    pub core_config: Config,

    /// Optional filter for table directories: a **SUBSTRING** match over the whole
    /// directory path (e.g. `/test_basic/`). Only table directories whose path
    /// CONTAINS this string are loaded.
    ///
    /// The substring semantics are stated here because they are load-bearing and
    /// surprising: a filter of `/<ks>/<table>-<uuid>` also matches a SIBLING whose
    /// name EXTENDS it (`<table>-<uuid>-backup`, `<table>-backup`), which silently
    /// adds generations to the ingest. A caller that needs EXACTLY one directory must
    /// not express it as a filter — use [`TableDirSelection::Exact`] via
    /// [`ingest_with_selection`], which compares complete path components (issue
    /// #3234).
    pub table_directory_filter: Option<String>,
}

/// Which discovered table directories the `Database` is built from.
///
/// Two genuinely different semantics, kept apart so neither can be mistaken for the
/// other (issue #3234 roborev F1):
///
/// - [`TableDirSelection::Filter`] — the historical
///   [`IngestionConfig::table_directory_filter`] SUBSTRING match. Loose by design
///   (`/test_basic/` selects a whole keyspace) and **cannot express "exactly this
///   directory"**: any sibling whose full name extends the filter also matches.
/// - [`TableDirSelection::Exact`] — exactly the named directories, compared as
///   complete path components after canonicalization. No substring, prefix or glob
///   semantics anywhere in the path, so a `<table>-<uuid>-backup` sibling contributes
///   nothing. This matters beyond tidiness: an extra directory changes the GENERATION
///   COUNT, and the generation count selects the scan route.
#[derive(Debug, Clone, Copy)]
pub enum TableDirSelection<'a> {
    /// Substring match over the whole path, from `table_directory_filter`.
    Filter,
    /// Exactly these directories, by complete-path-component identity.
    Exact(&'a [PathBuf]),
}

/// Complete-path-component identity: canonicalize both sides (absolute,
/// symlink-free, `.`/`..` resolved) and compare. `Path`'s `Eq` is component-wise, so
/// this is an exact component comparison and NEVER a substring or prefix one — a
/// canonicalized `<table>-<uuid>` can no longer match `<table>-<uuid>-backup`.
///
/// A path that cannot be canonicalized (it does not exist, or is unreadable) matches
/// nothing: fail-closed, so an unresolvable request can never widen the selection.
fn is_same_dir(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// Apply a [`TableDirSelection`] to discovery's table directories.
fn select_table_dirs(
    discovered: &[PathBuf],
    filter: Option<&str>,
    selection: TableDirSelection<'_>,
) -> Vec<PathBuf> {
    match selection {
        TableDirSelection::Filter => match filter {
            Some(pattern) => discovered
                .iter()
                .filter(|path| path.to_string_lossy().contains(pattern))
                .cloned()
                .collect(),
            None => discovered.to_vec(),
        },
        TableDirSelection::Exact(wanted) => discovered
            .iter()
            .filter(|path| wanted.iter().any(|w| is_same_dir(path, w)))
            .cloned()
            .collect(),
    }
}

/// Result of ingestion operation
#[derive(Debug)]
pub struct IngestionResult {
    /// The initialized Database instance
    pub database: Database,

    /// Schema loading summary
    pub schema_load_result: LoadResult,

    /// SSTable discovery summary
    pub discovery_summary: DiscoverySummary,

    /// Schema registry for coverage reporting
    pub schema_registry: Arc<RwLock<SchemaRegistry>>,
}

/// SSTable discovery summary
#[derive(Debug, Clone)]
pub struct DiscoverySummary {
    /// Total number of SSTables discovered
    pub sstables_found: usize,
    /// Keyspaces discovered
    pub keyspaces: Vec<String>,
    /// Tables discovered per keyspace
    pub tables: Vec<String>,
    /// Full directory paths for each discovered table
    pub table_directories: Vec<PathBuf>,
    /// Resolved Cassandra version (from precedence)
    pub resolved_version: Option<String>,
}

/// Main ingestion function for one-shot execution
///
/// This function orchestrates:
/// 1. Schema loading from provided paths (CQL/JSON files)
/// 2. SSTable discovery from data directory
/// 3. Database construction with QueryEngine
///
/// # Errors
///
/// Returns Error::Schema for schema loading failures (exit code 3)
/// Returns Error::Io for discovery/data-dir failures (exit code 4)
/// Returns Error::QueryExecution for query engine setup failures (exit code 5)
pub async fn ingest(config: IngestionConfig) -> Result<IngestionResult> {
    ingest_with_selection(config, TableDirSelection::Filter).await
}

/// [`ingest`], with the table-directory selection stated explicitly.
///
/// `TableDirSelection::Filter` is exactly what [`ingest`] does. `Exact` is for a
/// caller whose measurement or correctness claim depends on ingesting EXACTLY one
/// directory and no sibling that happens to share a prefix (issue #3234 roborev F1).
pub async fn ingest_with_selection(
    config: IngestionConfig,
    selection: TableDirSelection<'_>,
) -> Result<IngestionResult> {
    // Step 1: Validate data directory exists
    if !config.data_dir.exists() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Data directory does not exist: {}",
                config.data_dir.display()
            ),
        )));
    }

    if !config.data_dir.is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Data directory path is not a directory: {}",
                config.data_dir.display()
            ),
        )));
    }

    // Step 2: Initialize Platform
    let platform = Arc::new(Platform::new(&config.core_config).await?);

    // Step 3: Load schemas using SchemaAggregator
    let registry_config = SchemaRegistryConfig::default();
    let schema_registry = Arc::new(RwLock::new(
        SchemaRegistry::new(
            registry_config,
            platform.clone(),
            config.core_config.clone(),
        )
        .await
        .map_err(|e| Error::Schema(format!("Failed to create schema registry: {}", e)))?,
    ));

    // Use the SchemaRegistry's internal UDT registry so UDTs are available for query execution
    // This ensures UDTs loaded from CQL files are available when parsing SSTable data (Issue #238)
    let udt_registry = schema_registry.read().await.get_udt_registry();

    let aggregator_config = AggregatorConfig {
        graceful_degradation: false, // Fail fast for one-shot execution
        validate_udt_dependencies: true,
    };

    let mut aggregator = SchemaAggregator::new(
        schema_registry.clone(),
        udt_registry.clone(),
        aggregator_config,
    );

    let schema_load_result = if !config.schema_paths.is_empty() {
        aggregator
            .load_from_paths(&config.schema_paths)
            .await
            .map_err(|e| Error::Schema(format!("Schema loading failed: {}", e)))?
    } else {
        // No schema paths provided - return empty result
        LoadResult {
            schemas_loaded: 0,
            udts_loaded: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    };

    // Check for schema loading errors (fail fast)
    if !schema_load_result.errors.is_empty() {
        let error_messages: Vec<String> = schema_load_result
            .errors
            .iter()
            .map(|e| format!("{:?}: {}", e.error_type, e.message))
            .collect();
        return Err(Error::Schema(format!(
            "Schema loading failed with {} error(s): {}",
            schema_load_result.errors.len(),
            error_messages.join("; ")
        )));
    }

    // Step 4: Discover SSTables using DiscoveryService
    let discovery_service = DiscoveryService::with_schema_registry(
        config.data_dir.clone(),
        config.version_hint.clone(),
        schema_registry.clone(),
    );

    let service_summary = discovery_service.scan().await.map_err(|e| {
        // Map discovery errors to appropriate error types
        match e {
            Error::Io(_) => e,
            _ => Error::Io(std::io::Error::other(format!(
                "SSTable discovery failed: {}",
                e
            ))),
        }
    })?;

    // Step 5: Select the table directories this Database is built from.
    let filtered_table_dirs = select_table_dirs(
        &service_summary.table_directories,
        config.table_directory_filter.as_deref(),
        selection,
    );

    // Step 6: Build Database with discovered (and optionally filtered) SSTables
    // Pass the loaded schema_registry to the Database so schemas are available to the query engine
    // Storage path is data_dir (for runtime storage), discovered directories from DiscoveryService
    let database = Database::open_with_discovered_sstables_and_registry(
        &config.data_dir,
        filtered_table_dirs.clone(),
        config.core_config.clone(),
        Some(schema_registry.clone()),
    )
    .await
    .map_err(|e| {
        // Map database creation errors appropriately
        match e {
            Error::Schema(_) => e,
            Error::Io(_) => e,
            #[cfg(feature = "state_machine")]
            Error::QueryExecution(_) => e,
            _ => Error::QueryExecution(format!("Database initialization failed: {}", e)),
        }
    })?;

    // Convert from discovery module's DiscoverySummary to ingestion's DiscoverySummary
    // Use the filtered table directories in the summary
    let discovery_summary = DiscoverySummary {
        sstables_found: service_summary.sstables_found,
        keyspaces: service_summary.keyspaces,
        tables: service_summary.tables,
        table_directories: filtered_table_dirs,
        resolved_version: service_summary.resolved_version,
    };

    Ok(IngestionResult {
        database,
        schema_load_result,
        discovery_summary,
        schema_registry: schema_registry.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Note: Tests for discover_sstables() and resolve_cassandra_version()
    // have been removed as these functions are now in the discovery module
    // and are tested there.

    #[tokio::test]
    async fn test_ingest_invalid_data_dir() {
        let config = IngestionConfig {
            schema_paths: vec![],
            data_dir: PathBuf::from("/nonexistent/path"),
            version_hint: None,
            core_config: Config::default(),
            table_directory_filter: None,
        };

        let result = ingest(config).await;
        assert!(result.is_err());

        if let Err(Error::Io(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);
        } else {
            panic!("Expected Io error for nonexistent directory");
        }
    }

    #[tokio::test]
    async fn test_ingest_with_empty_schema_paths() {
        let temp_dir = TempDir::new().unwrap();

        let config = IngestionConfig {
            schema_paths: vec![],
            data_dir: temp_dir.path().to_path_buf(),
            version_hint: Some("5.0".to_string()),
            core_config: Config::default(),
            table_directory_filter: None,
        };

        let result = ingest(config).await;
        assert!(result.is_ok());

        let ingestion_result = result.unwrap();
        assert_eq!(ingestion_result.schema_load_result.schemas_loaded, 0);
        assert_eq!(ingestion_result.schema_load_result.udts_loaded, 0);
    }

    /// A keyspace tree holding the wanted `<table>-<uuid>` directory plus two
    /// siblings whose full names EXTEND it. Returns `(root, wanted, siblings)`.
    fn tree_with_name_extending_siblings() -> (TempDir, PathBuf, Vec<PathBuf>) {
        let root = TempDir::new().unwrap();
        let ks = root.path().join("perf_bti");
        let uuid = "a1b2c3d40000000000000000000000ff";
        let wanted = ks.join(format!("wide_multiclustering-{uuid}"));
        // Both of these are what a substring/prefix matcher cannot distinguish from
        // `wanted`: the first EXTENDS its complete name, the second extends the bare
        // table name.
        let siblings = vec![
            ks.join(format!("wide_multiclustering-{uuid}-backup")),
            ks.join("wide_multiclustering-backup"),
        ];
        for dir in std::iter::once(&wanted).chain(siblings.iter()) {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join("da-1-bti-Data.db"), b"x").unwrap();
        }
        (root, wanted, siblings)
    }

    /// Issue #3234 roborev F1: `Exact` selects the named directory and NOTHING whose
    /// name merely extends it.
    #[test]
    fn exact_selection_excludes_name_extending_siblings() {
        let (_root, wanted, siblings) = tree_with_name_extending_siblings();
        let mut discovered = vec![wanted.clone()];
        discovered.extend(siblings.iter().cloned());

        let selected = select_table_dirs(
            &discovered,
            // A filter is present AND would over-match; `Exact` must ignore it
            // entirely rather than intersect with it.
            Some("/perf_bti/wide_multiclustering"),
            TableDirSelection::Exact(std::slice::from_ref(&wanted)),
        );
        assert_eq!(
            selected,
            vec![wanted],
            "Exact must select exactly the named directory"
        );
    }

    /// The control for the test above: the SUBSTRING filter really does sweep in the
    /// name-extending siblings, which is why `Exact` exists and why a filter of
    /// `/<ks>/<table>-<uuid>` was never an exact scope (issue #3234 roborev F1).
    #[test]
    fn substring_filter_matches_name_extending_siblings() {
        let (_root, wanted, siblings) = tree_with_name_extending_siblings();
        let mut discovered = vec![wanted.clone()];
        discovered.extend(siblings.iter().cloned());

        let wanted_name = wanted.file_name().unwrap().to_string_lossy().to_string();
        let selected = select_table_dirs(
            &discovered,
            Some(&format!("/perf_bti/{wanted_name}")),
            TableDirSelection::Filter,
        );
        assert_eq!(
            selected.len(),
            2,
            "the substring filter matches the wanted dir AND the sibling extending its \
             full name: {selected:?}"
        );
        assert!(selected.contains(&siblings[0]));
    }

    /// `Exact` is fail-closed on a path that cannot be canonicalized: an unresolvable
    /// request selects nothing rather than widening the selection.
    #[test]
    fn exact_selection_of_a_nonexistent_dir_selects_nothing() {
        let (root, wanted, _siblings) = tree_with_name_extending_siblings();
        let absent = root.path().join("perf_bti/wide_multiclustering-00000000");
        let selected = select_table_dirs(
            &[wanted],
            None,
            TableDirSelection::Exact(std::slice::from_ref(&absent)),
        );
        assert!(selected.is_empty(), "got {selected:?}");
    }

    /// Canonicalization means an equivalent spelling of the SAME directory still
    /// matches — exactness is about identity, not about string form.
    #[test]
    fn exact_selection_matches_an_equivalent_spelling() {
        let (_root, wanted, _siblings) = tree_with_name_extending_siblings();
        let indirect = wanted.join("..").join(wanted.file_name().unwrap());
        let selected = select_table_dirs(
            std::slice::from_ref(&wanted),
            None,
            TableDirSelection::Exact(std::slice::from_ref(&indirect)),
        );
        assert_eq!(selected, vec![wanted]);
    }
}
