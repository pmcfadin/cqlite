//! Query execution engine for CQLite
//!
//! This module provides the core query execution engine that bridges between
//! CQL parsing and the storage engine. It includes:
//!
//! - CQL query parsing and validation
//! - Query planning and optimization
//! - Query execution with storage integration
//! - Prepared statement support
//! - Result set management

pub mod engine;
pub mod executor;
pub mod parser;
pub mod planner;
pub mod prepared;
pub mod result;

pub use engine::{
    AnalyzeResult, CacheStats, ExplainResult, QueryCacheEntry, QueryEngine as AdvancedQueryEngine,
};
pub use executor::{
    QueryExecutor, QueryResult as ExecutorQueryResult, QueryRow as ExecutorQueryRow,
};
pub use parser::QueryParser;
pub use planner::{ExecutionStep, IndexSelection, PlanType, QueryHints, QueryPlan, QueryPlanner};
pub use prepared::{
    ExecutionHints, ParameterMetadata, PreparedQuery, PreparedQueryBuilder, PreparedQueryStats,
};
pub use result::{
    ColumnInfo, PerformanceMetrics, QueryMetadata, QueryResult, QueryRow, RowMetadata,
};

use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    memory::MemoryManager, schema::SchemaManager, storage::StorageEngine, Config, Result, TableId,
    Value,
};

/// Query execution statistics
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    /// Total queries executed
    pub total_queries: u64,
    /// Queries that resulted in errors
    pub error_queries: u64,
    /// Average execution time in microseconds
    pub avg_execution_time_us: u64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Total rows affected by write operations
    pub rows_affected: u64,
}

/// Legacy query engine wrapper for backward compatibility
pub struct QueryEngine {
    /// Advanced query engine
    advanced_engine: AdvancedQueryEngine,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(
        storage: Arc<StorageEngine>,
        schema: Arc<SchemaManager>,
        memory: Arc<MemoryManager>,
        config: &Config,
    ) -> Result<Self> {
        let advanced_engine = AdvancedQueryEngine::new(storage, schema, memory, config)?;

        Ok(Self { advanced_engine })
    }

    /// Execute a CQL query
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.advanced_engine.execute(sql).await
    }

    /// Prepare a query for repeated execution
    pub async fn prepare(&self, sql: &str) -> Result<Arc<PreparedQuery>> {
        self.advanced_engine.prepare(sql).await
    }

    /// Get query statistics
    pub fn stats(&self) -> QueryStats {
        self.advanced_engine.stats()
    }

    /// Clear prepared statement cache
    pub fn clear_cache(&self) {
        self.advanced_engine.clear_prepared_cache();
    }

    /// Explain a query
    pub async fn explain(&self, sql: &str) -> Result<ExplainResult> {
        self.advanced_engine.explain(sql).await
    }

    /// Analyze query performance
    pub async fn analyze(&self, sql: &str) -> Result<AnalyzeResult> {
        self.advanced_engine.analyze(sql).await
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.advanced_engine.cache_stats()
    }
}

/// CQL query types
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// SELECT statement
    Select,
    /// INSERT statement
    Insert,
    /// UPDATE statement
    Update,
    /// DELETE statement
    Delete,
    /// CREATE TABLE statement
    CreateTable,
    /// DROP TABLE statement
    DropTable,
    /// CREATE INDEX statement
    CreateIndex,
    /// DROP INDEX statement
    DropIndex,
    /// DESCRIBE statement
    Describe,
    /// USE statement (for keyspace)
    Use,
}

/// Parsed CQL query representation
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Query type
    pub query_type: QueryType,
    /// Target table (if applicable)
    pub table: Option<TableId>,
    /// Column selections (for SELECT)
    pub columns: Vec<String>,
    /// WHERE clause conditions
    pub where_clause: Option<WhereClause>,
    /// VALUES for INSERT
    pub values: Vec<Value>,
    /// SET clause for UPDATE
    pub set_clause: HashMap<String, Value>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByClause>,
    /// LIMIT clause
    pub limit: Option<usize>,
    /// Original SQL text
    pub sql: String,
}

/// WHERE clause representation
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// Conditions in the WHERE clause
    pub conditions: Vec<Condition>,
}

/// Individual condition in WHERE clause
#[derive(Debug, Clone)]
pub struct Condition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub operator: ComparisonOperator,
    /// Value to compare against
    pub value: Value,
}

/// Comparison operators
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    /// Equal (=)
    Equal,
    /// Not equal (<>)
    NotEqual,
    /// Less than (<)
    LessThan,
    /// Less than or equal (<=)
    LessThanOrEqual,
    /// Greater than (>)
    GreaterThan,
    /// Greater than or equal (>=)
    GreaterThanOrEqual,
    /// IN operator
    In,
    /// NOT IN operator
    NotIn,
    /// LIKE operator
    Like,
    /// NOT LIKE operator
    NotLike,
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderByClause {
    /// Column name
    pub column: String,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    /// Ascending
    Asc,
    /// Descending
    Desc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_query_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(storage.clone(), &config).await.unwrap());
        let memory = Arc::new(MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        assert_eq!(query_engine.stats().total_queries, 0);
    }
}
