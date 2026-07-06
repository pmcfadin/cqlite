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
//! - REVOLUTIONARY SELECT parser for direct SSTable access

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

pub mod access_path;
pub mod engine;
pub(crate) mod engine_stats;
pub mod executor;
pub mod m2_select_validator;
pub mod parser;
pub mod planner;
pub mod prepared;
pub mod result;
pub(crate) mod result_budget;
pub mod writetime_ttl_validator;

// Advanced SELECT query components
#[cfg(feature = "state_machine")]
pub mod select_ast;
#[cfg(feature = "state_machine")]
pub mod select_executor;
#[cfg(feature = "state_machine")]
pub mod select_integration_tests;
#[cfg(feature = "state_machine")]
pub mod select_naming;
#[cfg(feature = "state_machine")]
pub mod select_optimizer;
#[cfg(feature = "state_machine")]
pub mod select_parser;

pub use access_path::{AccessPath, FallbackReason};
pub use engine::{
    AnalyzeResult, CacheStats, ExplainResult, QueryEngine as AdvancedQueryEngine, SchemaStatus,
};
pub use executor::{
    QueryExecutor, QueryResult as ExecutorQueryResult, QueryRow as ExecutorQueryRow,
};
pub use m2_select_validator::{M2SelectValidator, SelectValidationResult, UnsupportedFeature};
pub use parser::QueryParser;
pub use planner::{ExecutionStep, IndexSelection, PlanType, QueryHints, QueryPlan, QueryPlanner};
pub use prepared::{
    ExecutionHints, ParameterMetadata, PreparedContext, PreparedQuery, PreparedQueryBuilder,
    PreparedQueryStats,
};
pub use result::{
    cql_type_to_data_type, ColumnInfo, PerformanceMetrics, QueryMetadata, QueryResult,
    QueryResultIterator, QueryRow, RowMetadata, StreamingConfig,
};
pub use writetime_ttl_validator::{
    descriptors_from_table_schema, validate_all_writetime_ttl_calls, validate_writetime_ttl_call,
    ColumnDescriptor as WriteTimeTtlColumnDescriptor, WriteTimeTtlKind,
};

// Re-export advanced SELECT components.
// Only the symbols with known callers are re-exported here. `select_ast` is
// intentionally not glob-re-exported: a `pub use select_ast::*` would shadow the
// inline `ComparisonOperator`, `OrderByClause`, and `SortDirection` types below.
// Other `select_ast` items remain reachable via `cqlite_core::query::select_ast::*`.
#[cfg(feature = "state_machine")]
pub use select_ast::SelectStatement;
#[cfg(feature = "state_machine")]
pub use select_executor::{
    build_row_from_scan, evaluate_leaf, evaluate_predicates, LeafOutcome, SelectExecutor,
};
#[cfg(feature = "state_machine")]
pub use select_optimizer::{
    OptimizedQueryPlan, SSTableFilterOp, SSTablePredicate, SelectOptimizer,
};
#[cfg(feature = "state_machine")]
pub use select_parser::{parse_select, SelectParser};

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

/// Legacy query engine wrapper for backward compatibility.
///
/// Delegates to [`AdvancedQueryEngine`] (re-exported from [`engine`]); kept as a
/// stable public surface used by [`crate::Database`] and the CLI's REPL.
#[derive(Debug)]
pub struct QueryEngine {
    advanced_engine: AdvancedQueryEngine,
}

impl QueryEngine {
    /// Create a new query engine.
    pub fn new(
        storage: Arc<StorageEngine>,
        schema: Arc<SchemaManager>,
        memory: Arc<MemoryManager>,
        config: &Config,
    ) -> Result<Self> {
        Ok(Self {
            advanced_engine: AdvancedQueryEngine::new(storage, schema, memory, config)?,
        })
    }

    /// Execute a CQL query
    pub async fn execute(&self, cql: &str) -> Result<QueryResult> {
        self.advanced_engine.execute(cql).await
    }

    /// Execute a SELECT with positional `?` parameters (Issue #961).
    pub async fn execute_with_params(&self, cql: &str, params: &[Value]) -> Result<QueryResult> {
        self.advanced_engine.execute_with_params(cql, params).await
    }

    /// Prepare a query for repeated execution
    pub async fn prepare(&self, cql: &str) -> Result<Arc<PreparedQuery>> {
        self.advanced_engine.prepare(cql).await
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
    pub async fn explain(&self, cql: &str) -> Result<ExplainResult> {
        self.advanced_engine.explain(cql).await
    }

    /// Analyze query performance
    pub async fn analyze(&self, cql: &str) -> Result<AnalyzeResult> {
        self.advanced_engine.analyze(cql).await
    }

    /// Execute a CQL query with streaming results (Issue #280)
    ///
    /// Returns a `QueryResultIterator` that yields rows incrementally via a bounded
    /// channel, enabling memory-efficient processing of large result sets.
    #[cfg(feature = "state_machine")]
    pub async fn execute_streaming(
        &self,
        cql: &str,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        self.advanced_engine.execute_streaming(cql, config).await
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.advanced_engine.cache_stats()
    }

    /// Check if schema is available for a table
    pub async fn has_schema_for_table(&self, table: &str) -> bool {
        self.advanced_engine.has_schema_for_table(table).await
    }

    /// Get detailed schema status for debugging
    pub async fn schema_status(&self, table: &str) -> SchemaStatus {
        self.advanced_engine.schema_status(table).await
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
    /// Original CQL text
    pub cql: String,
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

#[cfg(all(test, feature = "state_machine"))]
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
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        let memory = Arc::new(MemoryManager::new(&config).unwrap());

        let query_engine = QueryEngine::new(storage, schema, memory, &config).unwrap();

        assert_eq!(query_engine.stats().total_queries, 0);
    }
}
