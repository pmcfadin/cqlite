//! REPL Data Access API
//!
//! This module provides a high-level API for accessing SSTable data from the REPL.
//! It integrates with the data manager to provide efficient, cached access to real
//! Cassandra data with support for interactive queries and exploration.
//!
//! This module requires the `state_machine` feature for query functionality.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    platform::Platform,
    query::result::{QueryResult, QueryRow},
    schema::{SchemaManager, TableSchema},
    storage::sstable_data_manager::{
        CacheStatistics, DataRow, SSTableDataManager, SSTableDataManagerConfig, TableDiscovery,
        TableInfo,
    },
    Config, Error, Result, Value,
};

/// REPL data access configuration
#[derive(Debug, Clone)]
pub struct ReplDataConfig {
    /// Data manager configuration
    pub data_manager_config: SSTableDataManagerConfig,
    /// Default query timeout in seconds
    pub default_timeout_seconds: u64,
    /// Enable automatic schema detection
    pub auto_detect_schema: bool,
    /// Maximum rows per query (safety limit)
    pub max_rows_per_query: usize,
    /// Enable query result caching
    pub enable_query_cache: bool,
    /// Query cache TTL in seconds
    pub query_cache_ttl_seconds: u64,
}

impl Default for ReplDataConfig {
    fn default() -> Self {
        Self {
            data_manager_config: SSTableDataManagerConfig::default(),
            default_timeout_seconds: 30,
            auto_detect_schema: true,
            max_rows_per_query: 10000,
            enable_query_cache: true,
            query_cache_ttl_seconds: 300,
        }
    }
}

/// Query execution context for REPL
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Current keyspace
    pub keyspace: Option<String>,
    /// Query timeout
    pub timeout: Duration,
    /// Maximum rows to return
    pub limit: Option<usize>,
    /// Enable timing information
    pub timing_enabled: bool,
    /// Page size for pagination
    pub page_size: Option<usize>,
    /// Current page offset
    pub page_offset: usize,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self {
            keyspace: None,
            timeout: Duration::from_secs(30),
            limit: Some(100),
            timing_enabled: false,
            page_size: Some(50),
            page_offset: 0,
        }
    }
}

/// Query execution result with metadata
#[derive(Debug, Clone)]
pub struct ReplQueryResult {
    /// Query result data
    pub result: QueryResult,
    /// Execution metadata
    pub metadata: QueryMetadata,
    /// Schema information (if available)
    pub schema: Option<TableSchema>,
}

/// Query execution metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    /// Total execution time
    pub execution_time: Duration,
    /// Number of rows returned
    pub rows_returned: usize,
    /// Total rows available (before limit)
    pub total_rows_available: Option<usize>,
    /// Whether result was cached
    pub from_cache: bool,
    /// Source SSTable files accessed
    pub source_files: Vec<PathBuf>,
    /// Data size read in bytes
    pub bytes_read: u64,
    /// Cache hit ratio for this query
    pub cache_hit_ratio: f64,
}

/// Table listing result
#[derive(Debug, Clone)]
pub struct TableListing {
    /// Keyspace name
    pub keyspace: String,
    /// Table information
    pub tables: Vec<TableSummary>,
    /// Discovery timestamp
    pub discovered_at: Instant,
}

/// Summary information for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    /// Table name
    pub name: String,
    /// Estimated row count
    pub estimated_rows: usize,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Number of SSTable files
    pub sstable_count: usize,
    /// Schema availability
    pub has_schema: bool,
    /// Last modified time
    pub last_modified: Option<std::time::SystemTime>,
    /// Health status
    pub health_status: String,
}

/// REPL data access API
pub struct ReplDataApi {
    /// Configuration
    config: ReplDataConfig,
    /// Data manager for SSTable access
    data_manager: Arc<SSTableDataManager>,
    /// Current query context
    query_context: Arc<RwLock<QueryContext>>,
    /// Query cache (if enabled)
    query_cache: Arc<RwLock<HashMap<String, (ReplQueryResult, Instant)>>>,
    /// Discovery cache
    discovery_cache: Arc<RwLock<Option<(TableDiscovery, Instant)>>>,
}

impl ReplDataApi {
    /// Create a new REPL data API
    pub async fn new(
        config: ReplDataConfig,
        platform: Arc<Platform>,
        core_config: Config,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        let data_manager = Arc::new(
            SSTableDataManager::new(
                config.data_manager_config.clone(),
                platform,
                core_config,
                schema_manager,
            )
            .await?,
        );

        Ok(Self {
            config,
            data_manager,
            query_context: Arc::new(RwLock::new(QueryContext::default())),
            query_cache: Arc::new(RwLock::new(HashMap::new())),
            discovery_cache: Arc::new(RwLock::new(None)),
        })
    }

    /// Initialize the API with a data directory
    pub async fn initialize(&self, data_dir: &Path) -> Result<TableDiscovery> {
        let discovery = self.data_manager.discover_tables(data_dir).await?;

        // Cache the discovery results
        {
            let mut cache = self.discovery_cache.write().await;
            *cache = Some((discovery.clone(), Instant::now()));
        }

        Ok(discovery)
    }

    /// Set the current keyspace
    pub async fn use_keyspace(&self, keyspace: &str) -> Result<()> {
        // Validate keyspace exists
        let keyspaces = self.list_keyspaces().await?;
        if !keyspaces.contains(&keyspace.to_string()) {
            return Err(Error::SqlParse(format!(
                "Keyspace '{}' does not exist",
                keyspace
            )));
        }

        let mut context = self.query_context.write().await;
        context.keyspace = Some(keyspace.to_string());
        Ok(())
    }

    /// Get the current keyspace
    pub async fn current_keyspace(&self) -> Option<String> {
        let context = self.query_context.read().await;
        context.keyspace.clone()
    }

    /// Execute a SELECT query
    pub async fn select(
        &self,
        table: &str,
        columns: Option<Vec<String>>,
        where_clause: Option<String>,
        limit: Option<usize>,
    ) -> Result<ReplQueryResult> {
        let start_time = Instant::now();
        let context = self.query_context.read().await;

        // Ensure we have a keyspace
        let keyspace = context.keyspace.as_ref().ok_or_else(|| {
            Error::InvalidState("No keyspace selected. Use 'USE keyspace;' first.".to_string())
        })?;

        // Apply context limits
        let effective_limit = limit
            .or(context.limit)
            .map(|l| l.min(self.config.max_rows_per_query))
            .unwrap_or(self.config.max_rows_per_query);

        // Check query cache if enabled
        if self.config.enable_query_cache {
            let cache_key = format!(
                "{}:{}:{}:{:?}:{:?}",
                keyspace,
                table,
                columns.as_ref().map(|c| c.join(",")).unwrap_or_default(),
                where_clause,
                effective_limit
            );

            if let Some((cached_result, cached_at)) = self.get_cached_query(&cache_key).await {
                let cache_ttl = Duration::from_secs(self.config.query_cache_ttl_seconds);
                if cached_at.elapsed() < cache_ttl {
                    let mut result = cached_result;
                    result.metadata.from_cache = true;
                    result.metadata.execution_time = start_time.elapsed();
                    return Ok(result);
                }
            }
        }

        // Execute the query
        let rows = self
            .data_manager
            .query_data(
                keyspace,
                table,
                where_clause.as_deref(),
                Some(effective_limit),
            )
            .await?;

        // Get schema information
        let schema = self.data_manager.get_table_schema(keyspace, table).await?;

        // Convert to QueryResult
        let query_result = self.convert_to_query_result(rows.clone(), &columns, &schema)?;

        // Create metadata
        let metadata = QueryMetadata {
            execution_time: start_time.elapsed(),
            rows_returned: rows.len(),
            total_rows_available: None, // Would require separate count query
            from_cache: false,
            source_files: rows
                .iter()
                .map(|r| r.metadata.source_file.clone())
                .collect(),
            bytes_read: self.estimate_bytes_read(&rows),
            cache_hit_ratio: self.calculate_cache_hit_ratio().await,
        };

        let result = ReplQueryResult {
            result: query_result,
            metadata,
            schema,
        };

        // Cache the result if enabled
        if self.config.enable_query_cache {
            let cache_key = format!(
                "{}:{}:{}:{:?}:{:?}",
                keyspace,
                table,
                columns.as_ref().map(|c| c.join(",")).unwrap_or_default(),
                where_clause,
                effective_limit
            );
            self.cache_query_result(cache_key, result.clone()).await;
        }

        Ok(result)
    }

    /// List all available keyspaces
    pub async fn list_keyspaces(&self) -> Result<Vec<String>> {
        self.data_manager.list_keyspaces().await
    }

    /// List tables in the current or specified keyspace
    pub async fn list_tables(&self, keyspace: Option<&str>) -> Result<TableListing> {
        let target_keyspace = if let Some(ks) = keyspace {
            ks.to_string()
        } else {
            let context = self.query_context.read().await;
            context
                .keyspace
                .as_ref()
                .ok_or_else(|| Error::InvalidState("No keyspace selected".to_string()))?
                .clone()
        };

        let table_names = self.data_manager.list_tables(&target_keyspace).await?;
        let mut tables = Vec::new();

        // Get detailed information for each table
        for table_name in table_names {
            if let Ok(Some(_schema)) = self
                .data_manager
                .get_table_schema(&target_keyspace, &table_name)
                .await
            {
                // Get table info from discovery cache
                let discovery = self.get_discovery_cache().await;
                if let Some((ref discovery_data, _)) = discovery {
                    for keyspace_info in &discovery_data.keyspaces {
                        if keyspace_info.name == target_keyspace {
                            for table_info in &keyspace_info.tables {
                                if table_info.name == table_name {
                                    let summary = TableSummary {
                                        name: table_name.clone(),
                                        estimated_rows: table_info.estimated_rows,
                                        size_bytes: table_info.total_size_bytes,
                                        sstable_count: table_info.sstable_files.len(),
                                        has_schema: table_info.schema.is_some(),
                                        last_modified: table_info.last_modified,
                                        health_status: self.assess_table_health(table_info),
                                    };
                                    tables.push(summary);
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(TableListing {
            keyspace: target_keyspace,
            tables,
            discovered_at: Instant::now(),
        })
    }

    /// Describe a table schema
    pub async fn describe_table(&self, table: &str, keyspace: Option<&str>) -> Result<TableSchema> {
        let target_keyspace = if let Some(ks) = keyspace {
            ks.to_string()
        } else {
            let context = self.query_context.read().await;
            context
                .keyspace
                .as_ref()
                .ok_or_else(|| Error::InvalidState("No keyspace selected".to_string()))?
                .clone()
        };

        self.data_manager
            .get_table_schema(&target_keyspace, table)
            .await?
            .ok_or_else(|| {
                Error::Table(format!(
                    "Table {}.{} not found or no schema available",
                    target_keyspace, table
                ))
            })
    }

    /// Get system information and statistics
    pub async fn get_system_info(&self) -> Result<SystemInfo> {
        let cache_stats = self.data_manager.get_cache_stats();
        let (discovery_in_progress, last_discovery) = self.data_manager.get_discovery_status();

        let discovery_info = self.get_discovery_cache().await;
        let (total_keyspaces, total_tables, total_sstables) =
            if let Some((ref discovery, _)) = discovery_info {
                (
                    discovery.keyspaces.len(),
                    discovery.keyspaces.iter().map(|ks| ks.tables.len()).sum(),
                    discovery.total_sstables,
                )
            } else {
                (0, 0, 0)
            };

        let memory_usage_mb = cache_stats.current_cache_size_bytes / (1024 * 1024);

        Ok(SystemInfo {
            total_keyspaces,
            total_tables,
            total_sstables,
            cache_stats,
            discovery_in_progress,
            last_discovery_time: last_discovery,
            memory_usage_mb,
            active_connections: 1, // REPL is single-connection
        })
    }

    /// Update query context settings
    pub async fn update_context(&self, updates: QueryContextUpdate) -> Result<()> {
        let mut context = self.query_context.write().await;

        if let Some(timeout) = updates.timeout_seconds {
            context.timeout = Duration::from_secs(timeout);
        }

        if let Some(limit) = updates.limit {
            context.limit = Some(limit.min(self.config.max_rows_per_query));
        }

        if let Some(timing) = updates.timing_enabled {
            context.timing_enabled = timing;
        }

        if let Some(page_size) = updates.page_size {
            context.page_size = Some(page_size);
        }

        Ok(())
    }

    /// Get current query context
    pub async fn get_context(&self) -> QueryContext {
        let context = self.query_context.read().await;
        context.clone()
    }

    /// Clear all caches
    pub async fn clear_caches(&self) -> Result<()> {
        {
            let mut query_cache = self.query_cache.write().await;
            query_cache.clear();
        }

        {
            let mut discovery_cache = self.discovery_cache.write().await;
            *discovery_cache = None;
        }

        Ok(())
    }

    // Helper methods

    async fn get_cached_query(&self, cache_key: &str) -> Option<(ReplQueryResult, Instant)> {
        let cache = self.query_cache.read().await;
        cache.get(cache_key).cloned()
    }

    async fn cache_query_result(&self, cache_key: String, result: ReplQueryResult) {
        let mut cache = self.query_cache.write().await;
        cache.insert(cache_key, (result, Instant::now()));

        // Simple cache eviction (keep last 100 queries)
        if cache.len() > 100 {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, (_, time))| time)
                .map(|(key, _)| key.clone());

            if let Some(key) = oldest_key {
                cache.remove(&key);
            }
        }
    }

    async fn get_discovery_cache(&self) -> Option<(TableDiscovery, Instant)> {
        let cache = self.discovery_cache.read().await;
        cache.clone()
    }

    fn convert_to_query_result(
        &self,
        rows: Vec<DataRow>,
        requested_columns: &Option<Vec<String>>,
        schema: &Option<TableSchema>,
    ) -> Result<QueryResult> {
        let mut query_rows = Vec::new();

        for data_row in rows {
            let mut row_values = Vec::new();

            // Determine column order
            let columns = if let Some(cols) = requested_columns {
                cols.clone()
            } else if let Some(schema) = schema {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                data_row.columns.keys().cloned().collect()
            };

            // Extract values in order
            for column_name in &columns {
                let value = data_row
                    .columns
                    .get(column_name)
                    .cloned()
                    .unwrap_or(Value::Null);
                row_values.push(value);
            }

            let query_row = QueryRow {
                values: row_values
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| (format!("col_{}", i), value))
                    .collect(),
                key: data_row.key.clone(),
                metadata: crate::query::result::RowMetadata {
                    version: Some(data_row.metadata.generation),
                    ttl: data_row.metadata.ttl.map(|duration| duration.as_secs()),
                    tags: std::collections::HashMap::new(),
                },
            };
            query_rows.push(query_row);
        }

        Ok(QueryResult {
            rows: query_rows,
            rows_affected: 0,
            execution_time_ms: 0,
            metadata: crate::query::result::QueryMetadata::default(),
        })
    }

    fn estimate_bytes_read(&self, rows: &[DataRow]) -> u64 {
        // Rough estimation
        (rows.len() * 256) as u64
    }

    async fn calculate_cache_hit_ratio(&self) -> f64 {
        let stats = self.data_manager.get_cache_stats();
        let total = stats.cache_hits + stats.cache_misses;
        if total > 0 {
            stats.cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    fn assess_table_health(&self, table_info: &TableInfo) -> String {
        let healthy_files = table_info
            .sstable_files
            .iter()
            .filter(|f| {
                f.health_status == crate::storage::sstable_data_manager::FileHealthStatus::Healthy
            })
            .count();

        let total_files = table_info.sstable_files.len();

        if healthy_files == total_files {
            "Healthy".to_string()
        } else if healthy_files > total_files / 2 {
            "Degraded".to_string()
        } else {
            "Corrupted".to_string()
        }
    }
}

/// System information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Total number of keyspaces
    pub total_keyspaces: usize,
    /// Total number of tables
    pub total_tables: usize,
    /// Total number of SSTable files
    pub total_sstables: usize,
    /// Cache statistics
    pub cache_stats: CacheStatistics,
    /// Whether discovery is in progress
    pub discovery_in_progress: bool,
    /// Time since last discovery
    pub last_discovery_time: Option<Duration>,
    /// Memory usage in MB
    pub memory_usage_mb: usize,
    /// Active connections (always 1 for REPL)
    pub active_connections: usize,
}

/// Query context update structure
#[derive(Debug, Clone, Default)]
pub struct QueryContextUpdate {
    /// New timeout in seconds
    pub timeout_seconds: Option<u64>,
    /// New default limit
    pub limit: Option<usize>,
    /// Enable/disable timing
    pub timing_enabled: Option<bool>,
    /// New page size
    pub page_size: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_repl_api_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = ReplDataConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());
        let schema_manager = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        let api = ReplDataApi::new(config, platform, core_config, schema_manager)
            .await
            .unwrap();

        let context = api.get_context().await;
        assert!(context.keyspace.is_none());
        assert_eq!(context.limit, Some(100));
    }

    #[tokio::test]
    async fn test_query_context_updates() {
        let temp_dir = TempDir::new().unwrap();
        let config = ReplDataConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());
        let schema_manager = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        let api = ReplDataApi::new(config, platform, core_config, schema_manager)
            .await
            .unwrap();

        let updates = QueryContextUpdate {
            timeout_seconds: Some(60),
            limit: Some(200),
            timing_enabled: Some(true),
            page_size: Some(25),
        };

        api.update_context(updates).await.unwrap();

        let context = api.get_context().await;
        assert_eq!(context.timeout, Duration::from_secs(60));
        assert_eq!(context.limit, Some(200));
        assert!(context.timing_enabled);
        assert_eq!(context.page_size, Some(25));
    }
}
