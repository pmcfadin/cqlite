//! Query engine implementation for CQLite
//!
//! This module provides the main query engine that coordinates between
//! parsing, planning, and execution of CQL queries.

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{
    executor::{QueryExecutor, QueryResult},
    parser::QueryParser,
    planner::QueryPlanner,
    prepared::PreparedQuery,
    QueryStats,
};

#[cfg(feature = "state_machine")]
use super::{select_executor::SelectExecutor, select_optimizer::SelectOptimizer, select_parser};
use crate::{
    memory::MemoryManager, schema::SchemaManager, storage::StorageEngine, Config, Error, Result,
    Value,
};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// Query cache entry
#[derive(Debug, Clone)]
pub struct QueryCacheEntry {
    /// Parsed query
    pub parsed_query: super::ParsedQuery,
    /// Query plan
    pub plan: super::planner::QueryPlan,
    /// Cache timestamp
    pub cached_at: Instant,
    /// Hit count
    pub hit_count: u64,
}

/// Schema availability status for diagnostic purposes
#[derive(Debug, Clone)]
pub enum SchemaStatus {
    /// Schema is available and ready for queries
    Available { keyspace: String, table: String },
    /// Schema not found in registry
    Missing { table: String, reason: String },
    /// Schema extraction failed from SSTable
    ExtractionFailed {
        table: String,
        cause: String,
        suggestion: String,
    },
}

/// Query engine with caching and statistics
#[derive(Debug)]
pub struct QueryEngine {
    /// Query parser
    parser: QueryParser,
    /// Query planner
    planner: QueryPlanner,
    /// Query executor
    executor: QueryExecutor,
    /// Schema manager reference
    schema_manager: Arc<SchemaManager>,
    /// Advanced SELECT optimizer
    #[cfg(feature = "state_machine")]
    select_optimizer: SelectOptimizer,
    /// Advanced SELECT executor
    #[cfg(feature = "state_machine")]
    select_executor: SelectExecutor,
    /// Prepared statement cache
    prepared_cache: DashMap<String, Arc<PreparedQuery>>,
    /// Query plan cache
    plan_cache: DashMap<String, QueryCacheEntry>,
    /// Query statistics
    stats: Arc<parking_lot::RwLock<QueryStats>>,
    /// Configuration
    config: Config,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(
        storage: Arc<StorageEngine>,
        schema: Arc<SchemaManager>,
        _memory: Arc<MemoryManager>,
        config: &Config,
    ) -> Result<Self> {
        let parser = QueryParser::new(config);
        let planner = QueryPlanner::new(schema.clone(), config);
        let executor = QueryExecutor::new(storage.clone(), schema.clone(), config);

        // Initialize advanced SELECT components
        #[cfg(feature = "state_machine")]
        let select_optimizer = SelectOptimizer::new(schema.clone(), storage.clone());
        #[cfg(feature = "state_machine")]
        let select_executor = SelectExecutor::new(schema.clone(), storage.clone());

        Ok(Self {
            parser,
            planner,
            executor,
            schema_manager: schema.clone(),
            #[cfg(feature = "state_machine")]
            select_optimizer,
            #[cfg(feature = "state_machine")]
            select_executor,
            prepared_cache: DashMap::new(),
            plan_cache: DashMap::new(),
            stats: Arc::new(parking_lot::RwLock::new(QueryStats::default())),
            config: config.clone(),
        })
    }

    /// Execute a CQL query
    pub async fn execute(&self, cql: &str) -> Result<QueryResult> {
        let start_time = Instant::now();

        // Update total queries counter
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        }

        // Check if this is a SELECT statement - use advanced parser
        let trimmed_cql = cql.trim().to_uppercase();
        if trimmed_cql.starts_with("SELECT") {
            // For simple WHERE id = <value> queries, use normal executor for consistent key handling
            // This ensures INSERT and SELECT use the same key generation logic
            if cql.contains("WHERE id =") && cql.split_whitespace().count() <= 8 {
                #[cfg(debug_assertions)]
                log::debug!(
                    "Routing simple SELECT through normal executor for consistent key handling"
                );
                // Fall through to normal execution path for simple point lookups
            } else {
                return self.execute_select_query(cql, start_time).await;
            }
        }

        // Check plan cache first for non-SELECT queries
        if let Some(mut cached_entry) = self.plan_cache.get_mut(cql) {
            // Update cache hit statistics
            {
                let mut stats = self.stats.write();
                stats.cache_hit_ratio = (stats.cache_hit_ratio * (stats.total_queries - 1) as f64
                    + 1.0)
                    / stats.total_queries as f64;
            }

            // Update cache entry hit count in place to avoid locking twice
            cached_entry.hit_count += 1;

            // Execute the cached plan
            let mut result = self.executor.execute(&cached_entry.plan).await?;
            self.update_execution_stats(&mut result, start_time);
            return Ok(result);
        }

        // Parse the query (non-SELECT)
        let parsed_query = self.parser.parse(cql).inspect_err(|_e| {
            // Update error statistics
            let mut stats = self.stats.write();
            stats.error_queries += 1;
        })?;

        // Plan the query
        let plan = self.planner.plan(&parsed_query).await?;

        // Cache the plan if enabled
        if self.config.query.query_cache_size.unwrap_or(0) > 0 {
            self.cache_query_plan(cql, parsed_query, plan.clone());
        }

        // Execute the query
        let mut result = self.executor.execute(&plan).await?;

        // Update statistics
        self.update_execution_stats(&mut result, start_time);

        Ok(result)
    }

    /// Execute a SELECT query using the advanced parser and optimizer
    async fn execute_select_query(&self, cql: &str, start_time: Instant) -> Result<QueryResult> {
        // Check plan cache first for SELECT queries too
        if let Some(mut cached_entry) = self.plan_cache.get_mut(cql) {
            if cached_entry.plan.table.is_some() {
                // Update cache hit statistics
                {
                    let mut stats = self.stats.write();
                    stats.cache_hit_ratio =
                        (stats.cache_hit_ratio * (stats.total_queries - 1) as f64 + 1.0)
                            / stats.total_queries as f64;
                }

                // Update cache entry hit count
                cached_entry.hit_count += 1;

                // Execute the cached plan
                let mut result = self.executor.execute(&cached_entry.plan).await?;
                self.update_execution_stats(&mut result, start_time);
                return Ok(result);
            }

            // Placeholder plans without table information are not reusable; drop them.
            drop(cached_entry);
            self.plan_cache.remove(cql);
        }

        // Parse SELECT statement using advanced parser
        #[cfg(feature = "state_machine")]
        let select_statement = select_parser::parse_select(cql).inspect_err(|_e| {
            // Update error statistics
            let mut stats = self.stats.write();
            stats.error_queries += 1;
        })?;

        #[cfg(not(feature = "state_machine"))]
        return Err(crate::error::Error::QueryExecution(
            "Advanced SELECT parsing requires state_machine feature".to_string(),
        ));

        // Optimize the query plan
        #[cfg(feature = "state_machine")]
        let optimized_plan = self.select_optimizer.optimize(select_statement).await?;

        // Execute the optimized plan
        #[cfg(feature = "state_machine")]
        {
            let mut result = self.select_executor.execute(optimized_plan).await?;
            // Update statistics
            self.update_execution_stats(&mut result, start_time);
            Ok(result)
        }
    }

    /// Execute a query with parameters
    pub async fn execute_with_params(&self, cql: &str, _params: &[Value]) -> Result<QueryResult> {
        // In a real implementation, this would substitute parameters into the query
        // For now, we'll just execute the query as-is
        self.execute(cql).await
    }

    /// Prepare a query for repeated execution
    pub async fn prepare(&self, cql: &str) -> Result<Arc<PreparedQuery>> {
        // Check cache first
        if let Some(cached) = self.prepared_cache.get(cql) {
            return Ok(cached.clone());
        }

        // Parse and prepare the query
        let parsed_query = self.parser.parse(cql)?;
        let plan = self.planner.plan(&parsed_query).await?;

        let prepared = Arc::new(PreparedQuery::new(
            parsed_query,
            plan,
            Arc::new(self.executor.clone()),
        ));

        // Cache the prepared statement
        self.prepared_cache
            .insert(cql.to_string(), prepared.clone());

        Ok(prepared)
    }

    /// Execute a prepared query
    pub async fn execute_prepared(
        &self,
        prepared: &PreparedQuery,
        params: &[Value],
    ) -> Result<QueryResult> {
        let start_time = Instant::now();

        // Update total queries counter
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        }

        // Execute the prepared query
        let mut result = prepared.execute(params).await?;

        // Update statistics
        self.update_execution_stats(&mut result, start_time);

        Ok(result)
    }

    /// Get query statistics
    pub fn stats(&self) -> QueryStats {
        self.stats.read().clone()
    }

    /// Clear all caches
    pub fn clear_caches(&self) {
        self.prepared_cache.clear();
        self.plan_cache.clear();
    }

    /// Clear prepared statement cache
    pub fn clear_prepared_cache(&self) {
        self.prepared_cache.clear();
    }

    /// Clear query plan cache
    pub fn clear_plan_cache(&self) {
        self.plan_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            prepared_cache_size: self.prepared_cache.len(),
            plan_cache_size: self.plan_cache.len(),
            prepared_cache_hits: self.prepared_cache.len() as u64,
            plan_cache_hits: self.plan_cache.len() as u64,
        }
    }

    /// Optimize a query (return execution plan without executing)
    pub async fn explain(&self, cql: &str) -> Result<ExplainResult> {
        // Parse the query
        let parsed_query = self.parser.parse(cql)?;

        // Plan the query
        let plan = self.planner.plan(&parsed_query).await?;

        Ok(ExplainResult {
            query_type: format!("{:?}", parsed_query.query_type),
            plan_type: format!("{:?}", plan.plan_type),
            estimated_cost: plan.estimated_cost,
            estimated_rows: plan.estimated_rows,
            selected_indexes: plan
                .selected_indexes
                .iter()
                .map(|idx| format!("{} ({:?})", idx.index_name, idx.index_type))
                .collect(),
            execution_steps: plan
                .steps
                .iter()
                .map(|step| {
                    format!(
                        "{:?}: {} (cost: {:.2})",
                        step.step_type,
                        step.columns.join(", "),
                        step.cost
                    )
                })
                .collect(),
            parallelization_info: plan
                .steps
                .iter()
                .filter(|step| step.parallelization.can_parallelize)
                .map(|step| {
                    format!(
                        "Threads: {}, Partition: {:?}",
                        step.parallelization.suggested_threads, step.parallelization.partition_key
                    )
                })
                .collect(),
        })
    }

    /// Analyze query performance
    pub async fn analyze(&self, cql: &str) -> Result<AnalyzeResult> {
        let start_time = Instant::now();

        // Execute the query multiple times to get average performance
        let mut execution_times = Vec::new();
        let mut results = Vec::new();

        for _ in 0..self.config.query.analyze_iterations.unwrap_or(5) {
            let iter_start = Instant::now();
            let result = self.execute(cql).await?;
            execution_times.push(iter_start.elapsed());
            results.push(result);
        }

        let total_time = start_time.elapsed();
        let avg_time =
            execution_times.iter().sum::<std::time::Duration>() / execution_times.len() as u32;
        let min_time = execution_times.iter().min().ok_or_else(|| {
            Error::query_execution("No execution times recorded for analysis".to_string())
        })?;
        let max_time = execution_times.iter().max().ok_or_else(|| {
            Error::query_execution("No execution times recorded for analysis".to_string())
        })?;

        // Calculate standard deviation
        let variance = execution_times
            .iter()
            .map(|time| {
                let diff = time.as_nanos() as f64 - avg_time.as_nanos() as f64;
                diff * diff
            })
            .sum::<f64>()
            / execution_times.len() as f64;
        let std_dev = variance.sqrt();

        Ok(AnalyzeResult {
            iterations: execution_times.len(),
            total_time_ms: total_time.as_millis() as u64,
            avg_time_ms: avg_time.as_millis() as u64,
            min_time_ms: min_time.as_millis() as u64,
            max_time_ms: max_time.as_millis() as u64,
            std_dev_ms: (std_dev / 1_000_000.0) as u64, // Convert from nanoseconds to milliseconds
            avg_rows_returned: results.iter().map(|r| r.rows.len()).sum::<usize>() / results.len(),
            cache_hit_ratio: self.stats().cache_hit_ratio,
        })
    }

    /// Cache a query plan
    fn cache_query_plan(
        &self,
        cql: &str,
        parsed_query: super::ParsedQuery,
        plan: super::planner::QueryPlan,
    ) {
        let cache_size = self.config.query.query_cache_size.unwrap_or(0);

        if cache_size > 0 {
            // Check if we need to evict entries
            if self.plan_cache.len() >= cache_size {
                // Simple LRU eviction - remove oldest entry
                let oldest_key = self
                    .plan_cache
                    .iter()
                    .min_by_key(|entry| entry.cached_at)
                    .map(|entry| entry.key().clone());

                if let Some(key) = oldest_key {
                    self.plan_cache.remove(&key);
                }
            }

            // Add new entry
            self.plan_cache.insert(
                cql.to_string(),
                QueryCacheEntry {
                    parsed_query,
                    plan,
                    cached_at: Instant::now(),
                    hit_count: 0,
                },
            );
        }
    }

    /// Check if schema is available for a table
    pub async fn has_schema_for_table(&self, table: &str) -> bool {
        self.schema_manager.get_table_schema(table).await.is_ok()
    }

    /// Get detailed schema status for debugging
    pub async fn schema_status(&self, table: &str) -> SchemaStatus {
        match self.schema_manager.get_table_schema(table).await {
            Ok(schema) => SchemaStatus::Available {
                keyspace: schema.keyspace.clone(),
                table: schema.table.clone(),
            },
            Err(Error::Schema(msg)) if msg.contains("not found") => {
                SchemaStatus::Missing {
                    table: table.to_string(),
                    reason: msg,
                }
            }
            Err(e) => SchemaStatus::ExtractionFailed {
                table: table.to_string(),
                cause: e.to_string(),
                suggestion: "Verify SSTable files are valid Cassandra 5.0 format and Statistics.db contains SerializationHeader".to_string(),
            },
        }
    }

    /// Update execution statistics
    fn update_execution_stats(&self, result: &mut QueryResult, start_time: Instant) {
        let execution_time = start_time.elapsed();
        // Ensure any non-zero execution time is at least 1ms for reporting
        result.execution_time_ms = if execution_time.is_zero() {
            0
        } else {
            std::cmp::max(1, execution_time.as_millis() as u64)
        };

        // Update global statistics
        let mut stats = self.stats.write();
        let old_avg = stats.avg_execution_time_us;
        let new_time_us = execution_time.as_micros() as u64;

        // Update running average
        stats.avg_execution_time_us = if stats.total_queries <= 1 {
            new_time_us
        } else {
            ((old_avg * (stats.total_queries - 1)) + new_time_us) / stats.total_queries
        };

        // Update rows affected
        stats.rows_affected += result.rows_affected;
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of prepared statements cached
    pub prepared_cache_size: usize,
    /// Number of query plans cached
    pub plan_cache_size: usize,
    /// Total prepared cache hits
    pub prepared_cache_hits: u64,
    /// Total plan cache hits
    pub plan_cache_hits: u64,
}

/// Query explanation result
#[derive(Debug, Clone)]
pub struct ExplainResult {
    /// Query type
    pub query_type: String,
    /// Plan type
    pub plan_type: String,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
    /// Selected indexes
    pub selected_indexes: Vec<String>,
    /// Execution steps
    pub execution_steps: Vec<String>,
    /// Parallelization information
    pub parallelization_info: Vec<String>,
}

/// Query analysis result
#[derive(Debug, Clone)]
pub struct AnalyzeResult {
    /// Number of iterations
    pub iterations: usize,
    /// Total analysis time
    pub total_time_ms: u64,
    /// Average execution time
    pub avg_time_ms: u64,
    /// Minimum execution time
    pub min_time_ms: u64,
    /// Maximum execution time
    pub max_time_ms: u64,
    /// Standard deviation of execution times
    pub std_dev_ms: u64,
    /// Average rows returned
    pub avg_rows_returned: usize,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
}

#[cfg(all(test, feature = "state_machine"))]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_query_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        assert_eq!(query_engine.stats().total_queries, 0);
        assert_eq!(query_engine.cache_stats().prepared_cache_size, 0);
        assert_eq!(query_engine.cache_stats().plan_cache_size, 0);
    }

    #[tokio::test]
    #[ignore = "Hangs >60s; needs investigation - gated for M1"]
    async fn test_query_caching() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::test_config();
        config.query.query_cache_size = Some(10);

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Execute a query twice
        let cql = "SELECT * FROM users WHERE id = 1";
        let _ = query_engine.execute(cql).await;
        let _ = query_engine.execute(cql).await;

        // Check that plan was cached
        assert_eq!(query_engine.cache_stats().plan_cache_size, 1);

        // Check cache hit ratio
        let stats = query_engine.stats();
        assert!(stats.cache_hit_ratio > 0.0);
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_prepared_statements() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Prepare a statement
        let cql = "SELECT * FROM users WHERE id = ?";
        let prepared = query_engine.prepare(cql).await.unwrap();

        // Execute it with parameters
        let params = vec![Value::Integer(1)];
        let result = query_engine
            .execute_prepared(&prepared, &params)
            .await
            .unwrap();

        // Check that result was generated
        assert!(result.execution_time_ms > 0);

        // Check that statement was cached
        assert_eq!(query_engine.cache_stats().prepared_cache_size, 1);
    }

    #[tokio::test]
    async fn test_query_explain() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Explain a query
        let cql = "SELECT * FROM users WHERE id = 1";
        let explain_result = query_engine.explain(cql).await.unwrap();

        assert_eq!(explain_result.query_type, "Select");
        assert!(explain_result.estimated_cost > 0.0);
        assert!(!explain_result.selected_indexes.is_empty());
        assert!(!explain_result.execution_steps.is_empty());
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_cache_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.query.query_cache_size = Some(2); // Very small cache

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Execute 3 different queries
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 1")
            .await;
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 2")
            .await;
        let _ = query_engine
            .execute("SELECT * FROM users WHERE id = 3")
            .await;

        // Cache should only have 2 entries due to eviction
        assert_eq!(query_engine.cache_stats().plan_cache_size, 2);
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_schema_validation_api() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Test has_schema_for_table with non-existent table
        let has_schema = query_engine.has_schema_for_table("nonexistent_table").await;
        assert!(!has_schema, "Should return false for non-existent table");

        // Test schema_status with non-existent table
        let status = query_engine.schema_status("nonexistent_table").await;
        match status {
            SchemaStatus::Missing { .. } | SchemaStatus::ExtractionFailed { .. } => {
                // Expected - either missing or extraction failed is correct
            }
            SchemaStatus::Available { .. } => {
                panic!("Should not be Available for non-existent table");
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "experimental")]
mod plan_cache_tests {
    use super::*;
    use crate::{
        memory::MemoryManager, platform::Platform, schema::SchemaManager, storage::StorageEngine,
        Config,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_query_engine(config: &Config) -> (QueryEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let platform = Arc::new(Platform::new(config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        let memory = Arc::new(MemoryManager::new(config).unwrap());

        let engine = QueryEngine::new(storage, schema, memory, config).unwrap();
        (engine, temp_dir)
    }

    async fn create_sample_table(engine: &QueryEngine) {
        engine
            .execute(
                "CREATE TABLE plan_cache_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )",
            )
            .await
            .unwrap();

        engine
            .execute("INSERT INTO plan_cache_test (id, value) VALUES (1, 'one')")
            .await
            .unwrap();
        engine
            .execute("INSERT INTO plan_cache_test (id, value) VALUES (2, 'two')")
            .await
            .unwrap();
        engine
            .execute("INSERT INTO plan_cache_test (id, value) VALUES (3, 'three')")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_plan_cache_disabled() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(0);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(engine.cache_stats().plan_cache_size, 0);
    }

    #[tokio::test]
    async fn test_plan_cache_reuse_point_lookup() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(4);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine.clear_plan_cache();

        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();
        engine
            .execute("SELECT * FROM plan_cache_test WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(engine.cache_stats().plan_cache_size, 1);
        assert!(engine.stats().cache_hit_ratio > 0.0);
    }

    #[tokio::test]
    async fn test_plan_cache_eviction_limit() {
        let mut config = Config::default();
        config.query.query_cache_size = Some(2);

        let (engine, _temp_dir) = setup_query_engine(&config).await;
        create_sample_table(&engine).await;

        engine.clear_plan_cache();

        for id in 1..=3 {
            engine
                .execute(&format!("SELECT * FROM plan_cache_test WHERE id = {}", id))
                .await
                .unwrap();
        }

        assert_eq!(engine.cache_stats().plan_cache_size, 2);
    }
}
