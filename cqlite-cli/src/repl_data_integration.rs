//! REPL Data Integration
//!
//! This module integrates the SSTable data loading and caching system with the
//! existing REPL infrastructure, providing seamless access to real Cassandra data
//! for interactive queries and exploration.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use anyhow::{Context, Result};
use colored::Colorize;
use prettytable::{Cell, Row, Table};

use cqlite_core::{
    Config as CoreConfig,
    platform::Platform,
    schema::SchemaManager,
    storage::{
        repl_data_api::{ReplDataApi, ReplDataConfig, QueryContext, ReplQueryResult},
        sstable_data_manager::{SSTableDataManagerConfig, TableDiscovery, CacheStatistics},
        schema_discovery::{SchemaDiscovery, SchemaDiscoveryConfig},
    },
    query::result::QueryResult,
};

use crate::{
    config::Config,
    formatter::CqlshTableFormatter,
};

/// REPL data integration configuration
#[derive(Debug, Clone)]
pub struct ReplIntegrationConfig {
    /// Data API configuration
    pub data_api_config: ReplDataConfig,
    /// Schema discovery configuration
    pub schema_discovery_config: SchemaDiscoveryConfig,
    /// Auto-discovery interval in seconds
    pub auto_discovery_interval: Option<u64>,
    /// Enable background preloading
    pub enable_background_preloading: bool,
    /// Show performance metrics in results
    pub show_performance_metrics: bool,
    /// Enable query result caching
    pub enable_result_caching: bool,
}

impl Default for ReplIntegrationConfig {
    fn default() -> Self {
        Self {
            data_api_config: ReplDataConfig::default(),
            schema_discovery_config: SchemaDiscoveryConfig::default(),
            auto_discovery_interval: Some(300), // 5 minutes
            enable_background_preloading: true,
            show_performance_metrics: true,
            enable_result_caching: true,
        }
    }
}

/// Integrated REPL data manager
pub struct ReplDataIntegration {
    /// Configuration
    config: ReplIntegrationConfig,
    /// Data access API
    data_api: Arc<ReplDataApi>,
    /// Schema discovery engine
    schema_discovery: Arc<SchemaDiscovery>,
    /// Data directory path
    data_dir: Option<PathBuf>,
    /// Last discovery results
    last_discovery: Option<(TableDiscovery, Instant)>,
    /// Background task handles
    background_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ReplDataIntegration {
    /// Create a new REPL data integration
    pub async fn new(
        config: ReplIntegrationConfig,
        cli_config: &Config,
    ) -> Result<Self> {
        // Create core configuration
        let core_config = CoreConfig::default();
        
        // Create platform abstraction
        let platform = Arc::new(
            Platform::new(&core_config).await
                .context("Failed to create platform abstraction")?
        );

        // Create schema manager (temporary directory for now)
        let temp_schema_dir = std::env::temp_dir().join("cqlite_schemas");
        std::fs::create_dir_all(&temp_schema_dir)?;
        let schema_manager = Arc::new(
            SchemaManager::new(&temp_schema_dir).await
                .context("Failed to create schema manager")?
        );

        // Create data API
        let data_api = Arc::new(
            ReplDataApi::new(
                config.data_api_config.clone(),
                platform.clone(),
                core_config.clone(),
                schema_manager,
            ).await.context("Failed to create data API")?
        );

        // Create schema discovery
        let schema_discovery = Arc::new(
            SchemaDiscovery::new(
                config.schema_discovery_config.clone(),
                platform,
                core_config,
            ).await.context("Failed to create schema discovery")?
        );

        Ok(Self {
            config,
            data_api,
            schema_discovery,
            data_dir: None,
            last_discovery: None,
            background_tasks: Vec::new(),
        })
    }

    /// Initialize with a data directory
    pub async fn initialize_with_data_dir(&mut self, data_dir: &Path) -> Result<TableDiscovery> {
        self.data_dir = Some(data_dir.to_path_buf());
        
        println!("{}", "🔍 Discovering tables and schemas...".cyan().bold());
        let start_time = Instant::now();
        
        // Perform initial discovery
        let discovery = self.data_api.initialize(data_dir).await
            .context("Failed to initialize data API")?;
        
        self.last_discovery = Some((discovery.clone(), Instant::now()));
        
        // Start background tasks if enabled
        if self.config.enable_background_preloading {
            self.start_background_tasks().await;
        }

        println!("✅ Discovery completed in {:.2}s", start_time.elapsed().as_secs_f64());
        self.print_discovery_summary(&discovery);

        Ok(discovery)
    }

    /// Execute a SELECT query with full integration
    pub async fn execute_select(
        &self,
        table: &str,
        columns: Option<Vec<String>>,
        where_clause: Option<String>,
        limit: Option<usize>,
        show_timing: bool,
    ) -> Result<ReplQueryResult> {
        let start_time = Instant::now();
        
        // Execute the query
        let result = self.data_api.select(table, columns, where_clause, limit).await
            .context("Query execution failed")?;

        // Display results with formatting
        self.display_query_result(&result, show_timing).await?;

        Ok(result)
    }

    /// List all keyspaces
    pub async fn list_keyspaces(&self) -> Result<Vec<String>> {
        let keyspaces = self.data_api.list_keyspaces().await
            .context("Failed to list keyspaces")?;
        
        self.display_keyspaces(&keyspaces);
        Ok(keyspaces)
    }

    /// List tables in current or specified keyspace
    pub async fn list_tables(&self, keyspace: Option<&str>) -> Result<()> {
        let table_listing = self.data_api.list_tables(keyspace).await
            .context("Failed to list tables")?;
        
        self.display_tables(&table_listing);
        Ok(())
    }

    /// Describe a table
    pub async fn describe_table(&self, table: &str, keyspace: Option<&str>) -> Result<()> {
        let schema = self.data_api.describe_table(table, keyspace).await
            .context("Failed to describe table")?;
        
        self.display_table_schema(&schema);
        Ok(())
    }

    /// Use a keyspace
    pub async fn use_keyspace(&self, keyspace: &str) -> Result<()> {
        self.data_api.use_keyspace(keyspace).await
            .context("Failed to set keyspace")?;
        
        println!("✅ Using keyspace: {}", keyspace.green().bold());
        Ok(())
    }

    /// Get current keyspace
    pub async fn current_keyspace(&self) -> Option<String> {
        self.data_api.current_keyspace().await
    }

    /// Show system information
    pub async fn show_system_info(&self) -> Result<()> {
        let system_info = self.data_api.get_system_info().await
            .context("Failed to get system info")?;
        
        self.display_system_info(&system_info);
        Ok(())
    }

    /// Show cache statistics
    pub async fn show_cache_stats(&self) -> Result<()> {
        let stats = self.data_api.get_system_info().await?.cache_stats;
        self.display_cache_statistics(&stats);
        Ok(())
    }

    /// Clear all caches
    pub async fn clear_caches(&self) -> Result<()> {
        self.data_api.clear_caches().await
            .context("Failed to clear caches")?;
        
        println!("✅ All caches cleared");
        Ok(())
    }

    /// Refresh discovery (manual)
    pub async fn refresh_discovery(&mut self) -> Result<TableDiscovery> {
        if let Some(ref data_dir) = self.data_dir {
            println!("{}", "🔄 Refreshing table discovery...".cyan());
            let discovery = self.data_api.initialize(data_dir).await
                .context("Failed to refresh discovery")?;
            
            self.last_discovery = Some((discovery.clone(), Instant::now()));
            self.print_discovery_summary(&discovery);
            Ok(discovery)
        } else {
            Err(anyhow::anyhow!("No data directory configured"))
        }
    }

    /// Update query settings
    pub async fn update_settings(
        &self,
        timeout_seconds: Option<u64>,
        limit: Option<usize>,
        timing_enabled: Option<bool>,
        page_size: Option<usize>,
    ) -> Result<()> {
        use cqlite_core::storage::repl_data_api::QueryContextUpdate;
        
        let updates = QueryContextUpdate {
            timeout_seconds,
            limit,
            timing_enabled,
            page_size,
        };
        
        self.data_api.update_context(updates).await
            .context("Failed to update settings")?;
        
        println!("✅ Settings updated");
        Ok(())
    }

    // Display methods

    async fn display_query_result(&self, result: &ReplQueryResult, show_timing: bool) -> Result<()> {
        let formatter = CqlshTableFormatter::new();
        
        // Display the data table
        if result.result.rows.is_empty() {
            println!("{}", "No rows returned".yellow());
        } else {
            // Get column names
            let columns = if let Some(ref cols) = result.result.columns {
                cols.clone()
            } else if let Some(ref schema) = result.schema {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                (0..result.result.rows[0].values.len())
                    .map(|i| format!("column_{}", i))
                    .collect()
            };

            // Convert to display format
            let mut display_rows = Vec::new();
            for row in &result.result.rows {
                let row_strings: Vec<String> = row.values.iter()
                    .map(|v| format!("{}", v))
                    .collect();
                display_rows.push(row_strings);
            }

            formatter.format_table_data(&columns, display_rows)?;
        }

        // Display metadata if enabled
        if show_timing || self.config.show_performance_metrics {
            self.display_query_metadata(&result.metadata);
        }

        Ok(())
    }

    fn display_keyspaces(&self, keyspaces: &[String]) {
        println!("\n{}", "Available Keyspaces:".blue().bold());
        let mut table = Table::new();
        table.set_titles(Row::new(vec![Cell::new("Keyspace")]));
        
        for keyspace in keyspaces {
            table.add_row(Row::new(vec![Cell::new(keyspace)]));
        }
        
        table.printstd();
    }

    fn display_tables(&self, table_listing: &cqlite_core::storage::repl_data_api::TableListing) {
        println!("\n{} {}", "Tables in keyspace:".blue().bold(), table_listing.keyspace.green());
        
        if table_listing.tables.is_empty() {
            println!("{}", "No tables found".yellow());
            return;
        }

        let mut table = Table::new();
        table.set_titles(Row::new(vec![
            Cell::new("Table"),
            Cell::new("Estimated Rows"),
            Cell::new("Size (bytes)"),
            Cell::new("SSTable Files"),
            Cell::new("Status"),
        ]));
        
        for table_info in &table_listing.tables {
            table.add_row(Row::new(vec![
                Cell::new(&table_info.name),
                Cell::new(&table_info.estimated_rows.to_string()),
                Cell::new(&format_bytes(table_info.size_bytes)),
                Cell::new(&table_info.sstable_count.to_string()),
                Cell::new(&table_info.health_status),
            ]));
        }
        
        table.printstd();
    }

    fn display_table_schema(&self, schema: &cqlite_core::schema::TableSchema) {
        println!("\n{} {}.{}", "Schema for table:".blue().bold(), 
                schema.keyspace.green(), schema.table.green());
        
        let mut table = Table::new();
        table.set_titles(Row::new(vec![
            Cell::new("Column"),
            Cell::new("Type"),
            Cell::new("Key Type"),
        ]));
        
        for column in &schema.columns {
            let key_type = if column.is_primary_key {
                "PRIMARY"
            } else if column.is_clustering_key {
                "CLUSTERING"
            } else {
                "REGULAR"
            };
            
            table.add_row(Row::new(vec![
                Cell::new(&column.name),
                Cell::new(&column.data_type),
                Cell::new(key_type),
            ]));
        }
        
        table.printstd();
    }

    fn display_system_info(&self, info: &cqlite_core::storage::repl_data_api::SystemInfo) {
        println!("\n{}", "System Information:".blue().bold());
        
        let mut table = Table::new();
        table.set_titles(Row::new(vec![Cell::new("Property"), Cell::new("Value")]));
        
        table.add_row(Row::new(vec![
            Cell::new("Total Keyspaces"), 
            Cell::new(&info.total_keyspaces.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Total Tables"), 
            Cell::new(&info.total_tables.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Total SSTable Files"), 
            Cell::new(&info.total_sstables.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Memory Usage"), 
            Cell::new(&format!("{} MB", info.memory_usage_mb))
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Cache Hit Rate"), 
            Cell::new(&format!("{:.1}%", 
                info.cache_stats.cache_hits as f64 / 
                (info.cache_stats.cache_hits + info.cache_stats.cache_misses).max(1) as f64 * 100.0))
        ]));
        
        table.printstd();
    }

    fn display_cache_statistics(&self, stats: &CacheStatistics) {
        println!("\n{}", "Cache Statistics:".blue().bold());
        
        let mut table = Table::new();
        table.set_titles(Row::new(vec![Cell::new("Metric"), Cell::new("Value")]));
        
        table.add_row(Row::new(vec![
            Cell::new("Cache Hits"), 
            Cell::new(&stats.cache_hits.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Cache Misses"), 
            Cell::new(&stats.cache_misses.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Cache Entries"), 
            Cell::new(&stats.cache_entries.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Current Size"), 
            Cell::new(&format_bytes(stats.current_cache_size_bytes as u64))
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Evictions"), 
            Cell::new(&stats.evictions.to_string())
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Avg Access Time"), 
            Cell::new(&format!("{}μs", stats.avg_access_time_micros))
        ]));
        
        table.printstd();
    }

    fn display_query_metadata(&self, metadata: &cqlite_core::storage::repl_data_api::QueryMetadata) {
        println!("\n{}", "Query Metadata:".dim());
        println!("  Execution time: {:.3}s", metadata.execution_time.as_secs_f64());
        println!("  Rows returned: {}", metadata.rows_returned);
        println!("  From cache: {}", if metadata.from_cache { "Yes" } else { "No" });
        println!("  Source files: {}", metadata.source_files.len());
        println!("  Bytes read: {}", format_bytes(metadata.bytes_read));
        println!("  Cache hit ratio: {:.1}%", metadata.cache_hit_ratio * 100.0);
    }

    fn print_discovery_summary(&self, discovery: &TableDiscovery) {
        println!("\n{}", "Discovery Summary:".blue().bold());
        println!("  📁 Keyspaces found: {}", discovery.keyspaces.len());
        
        let total_tables: usize = discovery.keyspaces.iter()
            .map(|ks| ks.tables.len())
            .sum();
        println!("  📊 Tables found: {}", total_tables);
        println!("  📄 SSTable files: {}", discovery.total_sstables);
        
        for keyspace in &discovery.keyspaces {
            if !keyspace.tables.is_empty() {
                println!("    {} {}: {} tables", 
                    "•".cyan(), 
                    keyspace.name.green(), 
                    keyspace.tables.len()
                );
            }
        }
    }

    // Background task management

    async fn start_background_tasks(&mut self) {
        if let Some(interval_seconds) = self.config.auto_discovery_interval {
            let data_api = Arc::clone(&self.data_api);
            let data_dir = self.data_dir.clone();
            
            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
                loop {
                    interval.tick().await;
                    if let Some(ref dir) = data_dir {
                        if let Err(e) = data_api.initialize(dir).await {
                            eprintln!("Background discovery failed: {}", e);
                        }
                    }
                }
            });
            
            self.background_tasks.push(task);
        }
    }

    /// Shutdown background tasks
    pub async fn shutdown(&mut self) {
        for task in self.background_tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for ReplDataIntegration {
    fn drop(&mut self) {
        // Abort background tasks on drop
        for task in &self.background_tasks {
            task.abort();
        }
    }
}

// Helper functions

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_repl_integration_creation() {
        let config = ReplIntegrationConfig::default();
        let cli_config = Config::default();
        
        let integration = ReplDataIntegration::new(config, &cli_config).await.unwrap();
        
        // Test that it was created successfully
        assert!(integration.data_dir.is_none());
        assert!(integration.last_discovery.is_none());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1000), "1000 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
    }
}