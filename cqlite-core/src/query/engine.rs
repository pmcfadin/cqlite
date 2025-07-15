//! Query engine implementation for CQLite
//!
//! This module provides the main query engine that coordinates between
//! parsing, planning, and execution of CQL queries.

use super::{
    executor::{QueryExecutor, QueryResult},
    parser::QueryParser,
    planner::QueryPlanner,
    prepared::PreparedQuery,
    QueryStats,
};
use crate::{
    memory::MemoryManager, schema::SchemaManager, storage::StorageEngine, Config, Result, Value,
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

/// Query engine with caching and statistics
#[derive(Debug)]
pub struct QueryEngine {
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Schema manager reference
    schema: Arc<SchemaManager>,
    /// Memory manager reference
    memory: Arc<MemoryManager>,
    /// Query parser
    parser: QueryParser,
    /// Query planner
    planner: QueryPlanner,
    /// Query executor
    executor: QueryExecutor,
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
        memory: Arc<MemoryManager>,
        config: &Config,
    ) -> Result<Self> {
        let parser = QueryParser::new(config);
        let planner = QueryPlanner::new(schema.clone(), config);
        let executor = QueryExecutor::new(storage.clone(), schema.clone(), config);

        Ok(Self {
            storage,
            schema,
            memory,
            parser,
            planner,
            executor,
            prepared_cache: DashMap::new(),
            plan_cache: DashMap::new(),
            stats: Arc::new(parking_lot::RwLock::new(QueryStats::default())),
            config: config.clone(),
        })
    }

    /// Execute a CQL query
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let start_time = Instant::now();

        // Update total queries counter
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        }

        // Check plan cache first
        if let Some(cached_entry) = self.plan_cache.get(sql) {
            // Update cache hit statistics
            {
                let mut stats = self.stats.write();
                stats.cache_hit_ratio = (stats.cache_hit_ratio * (stats.total_queries - 1) as f64
                    + 1.0)
                    / stats.total_queries as f64;
            }

            // Update cache entry hit count
            let mut entry = cached_entry.clone();
            entry.hit_count += 1;
            self.plan_cache.insert(sql.to_string(), entry.clone());

            // Execute the cached plan
            let mut result = self.executor.execute(&entry.plan).await?;
            self.update_execution_stats(&mut result, start_time);
            return Ok(result);
        }

        // Parse the query
        let parsed_query = self.parser.parse(sql).map_err(|e| {
            // Update error statistics
            let mut stats = self.stats.write();
            stats.error_queries += 1;
            e
        })?;

        // Plan the query
        let plan = self.planner.plan(&parsed_query).await?;

        // Cache the plan if enabled
        if self.config.query.query_cache_size.unwrap_or(0) > 0 {
            self.cache_query_plan(sql, parsed_query, plan.clone());
        }

        // Execute the query
        let mut result = self.executor.execute(&plan).await?;

        // Update statistics
        self.update_execution_stats(&mut result, start_time);

        Ok(result)
    }

    /// Execute a query with parameters
    pub async fn execute_with_params(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        // In a real implementation, this would substitute parameters into the query
        // For now, we'll just execute the query as-is
        self.execute(sql).await
    }

    /// Prepare a query for repeated execution
    pub async fn prepare(&self, sql: &str) -> Result<Arc<PreparedQuery>> {
        // Check cache first
        if let Some(cached) = self.prepared_cache.get(sql) {
            return Ok(cached.clone());
        }

        // Parse and prepare the query
        let parsed_query = self.parser.parse(sql)?;
        let plan = self.planner.plan(&parsed_query).await?;

        let prepared = Arc::new(PreparedQuery::new(
            parsed_query,
            plan,
            Arc::new(self.executor.clone()),
        ));

        // Cache the prepared statement
        self.prepared_cache
            .insert(sql.to_string(), prepared.clone());

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
            prepared_cache_hits: self
                .prepared_cache
                .iter()
                .map(|entry| entry.hit_count)
                .sum(),
            plan_cache_hits: self.plan_cache.iter().map(|entry| entry.hit_count).sum(),
        }
    }

    /// Optimize a query (return execution plan without executing)
    pub async fn explain(&self, sql: &str) -> Result<ExplainResult> {
        // Parse the query
        let parsed_query = self.parser.parse(sql)?;

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
                .map(|idx| format!("{} ({})", idx.index_name, format!("{:?}", idx.index_type)))
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
    pub async fn analyze(&self, sql: &str) -> Result<AnalyzeResult> {
        let start_time = Instant::now();

        // Execute the query multiple times to get average performance
        let mut execution_times = Vec::new();
        let mut results = Vec::new();

        for _ in 0..self.config.query.analyze_iterations.unwrap_or(5) {
            let iter_start = Instant::now();
            let result = self.execute(sql).await?;
            execution_times.push(iter_start.elapsed());
            results.push(result);
        }

        let total_time = start_time.elapsed();
        let avg_time =
            execution_times.iter().sum::<std::time::Duration>() / execution_times.len() as u32;
        let min_time = execution_times.iter().min().unwrap();
        let max_time = execution_times.iter().max().unwrap();

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
        sql: &str,
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
                sql.to_string(),
                QueryCacheEntry {
                    parsed_query,
                    plan,
                    cached_at: Instant::now(),
                    hit_count: 0,
                },
            );
        }
    }

    /// Update execution statistics
    fn update_execution_stats(&self, result: &mut QueryResult, start_time: Instant) {
        let execution_time = start_time.elapsed();
        result.execution_time_ms = execution_time.as_millis() as u64;

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

#[cfg(test)]
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
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage.clone(), &config)
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
    async fn test_query_caching() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.query.query_cache_size = Some(10);

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage.clone(), &config)
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Execute a query twice
        let sql = "SELECT * FROM users WHERE id = 1";
        let _ = query_engine.execute(sql).await;
        let _ = query_engine.execute(sql).await;

        // Check that plan was cached
        assert_eq!(query_engine.cache_stats().plan_cache_size, 1);

        // Check cache hit ratio
        let stats = query_engine.stats();
        assert!(stats.cache_hit_ratio > 0.0);
    }

    #[tokio::test]
    async fn test_prepared_statements() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage.clone(), &config)
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Prepare a statement
        let sql = "SELECT * FROM users WHERE id = ?";
        let prepared = query_engine.prepare(sql).await.unwrap();

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
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage.clone(), &config)
                .await
                .unwrap(),
        );
        let memory = Arc::new(crate::memory::MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        // Explain a query
        let sql = "SELECT * FROM users WHERE id = 1";
        let explain_result = query_engine.explain(sql).await.unwrap();

        assert_eq!(explain_result.query_type, "Select");
        assert!(explain_result.estimated_cost > 0.0);
        assert!(!explain_result.selected_indexes.is_empty());
        assert!(!explain_result.execution_steps.is_empty());
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.query.query_cache_size = Some(2); // Very small cache

        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage.clone(), &config)
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
}
