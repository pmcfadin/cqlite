//! Prepared statements for CQLite
//!
//! This module provides prepared statement support for CQL queries.
//! Prepared statements offer several benefits:
//!
//! - Performance: Query parsing and planning is done once
//! - Security: Parameters are safely bound preventing query injection
//! - Reusability: Same query can be executed with different parameters

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{
    executor::{QueryExecutor, QueryResult},
    planner::{PlanType, QueryPlan},
    ParsedQuery,
};
use crate::types::DataType;
use crate::{Error, Result, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// SELECT-statement execution pipeline for prepared statements (Issue #961).
///
/// When a prepared statement is a SELECT, the engine attaches this pipeline so
/// that `?` placeholders are bound into the parsed `SelectStatement` and the
/// bound query runs through the *same* optimizer + `SelectExecutor` path as a
/// literal `execute()`. This is what lets a prepared `WHERE pk = ?` engage the
/// partition-targeted fast path (#949/#956) instead of the legacy
/// `QueryExecutor` plan, which never bound parameters.
#[cfg(feature = "state_machine")]
#[derive(Debug)]
struct PreparedSelect {
    /// The parsed SELECT statement with unbound `?` markers. Cloned per execution
    /// and bound positionally before optimization.
    statement: super::select_ast::SelectStatement,
    /// Shared optimizer (same instance the engine uses for literal SELECTs).
    optimizer: Arc<super::select_optimizer::SelectOptimizer>,
    /// Shared SELECT executor (same instance the engine uses for literal SELECTs).
    executor: Arc<super::select_executor::SelectExecutor>,
}

/// Prepared query statement
#[derive(Debug)]
pub struct PreparedQuery {
    /// Original CQL text
    pub cql: String,
    /// Parsed query
    pub parsed_query: ParsedQuery,
    /// Query execution plan
    pub plan: QueryPlan,
    /// Parameter metadata
    pub parameters: Vec<ParameterMetadata>,
    /// Query executor
    executor: Arc<QueryExecutor>,
    /// SELECT pipeline used to bind parameters and reach the partition-targeted
    /// fast path. `None` for non-SELECT prepared statements, which retain the
    /// legacy `QueryExecutor` path (Issue #961).
    #[cfg(feature = "state_machine")]
    select_pipeline: Option<PreparedSelect>,
}

/// Parameter metadata for prepared statements
#[derive(Debug, Clone)]
pub struct ParameterMetadata {
    /// Parameter name (if named)
    pub name: Option<String>,
    /// Parameter position (0-based)
    pub position: usize,
    /// Expected parameter type
    pub expected_type: Option<DataType>,
    /// Whether parameter is optional
    pub optional: bool,
}

/// Prepared statement execution context
#[derive(Debug)]
pub struct PreparedContext {
    /// Bound parameters
    pub parameters: HashMap<String, Value>,
    /// Positional parameters
    pub positional_params: Vec<Value>,
    /// Execution hints
    pub hints: ExecutionHints,
}

/// Execution hints for prepared statements
#[derive(Debug, Clone, Default)]
pub struct ExecutionHints {
    /// Force specific index usage
    pub force_index: Option<String>,
    /// Query timeout in milliseconds
    pub timeout_ms: Option<u64>,
    /// Parallelization preference
    pub parallelism: Option<usize>,
    /// Cache results
    pub cache_results: bool,
}

impl PreparedQuery {
    /// Create a new prepared query (legacy / non-SELECT path).
    pub fn new(parsed_query: ParsedQuery, plan: QueryPlan, executor: Arc<QueryExecutor>) -> Self {
        let cql = parsed_query.cql.clone();
        let parameters = Self::extract_parameters(&parsed_query);

        Self {
            cql,
            parsed_query,
            plan,
            parameters,
            executor,
            #[cfg(feature = "state_machine")]
            select_pipeline: None,
        }
    }

    /// Create a prepared SELECT that binds positional `?` parameters and routes
    /// through the SELECT optimizer + executor (Issue #961).
    ///
    /// `statement` is the parsed SELECT with unbound markers; `select_marker_count`
    /// is its `?` count (used to derive strict parameter metadata). The legacy
    /// `parsed_query`/`plan`/`executor` are still stored so the accessor methods
    /// (`plan()`, `is_cache_friendly()`, etc.) keep working, but execution goes
    /// through the SELECT pipeline.
    #[cfg(feature = "state_machine")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_select(
        parsed_query: ParsedQuery,
        plan: QueryPlan,
        executor: Arc<QueryExecutor>,
        statement: super::select_ast::SelectStatement,
        select_marker_count: usize,
        optimizer: Arc<super::select_optimizer::SelectOptimizer>,
        select_executor: Arc<super::select_executor::SelectExecutor>,
    ) -> Self {
        let cql = parsed_query.cql.clone();
        // Strict, positional metadata: one untyped slot per `?` marker. The bound
        // value's type is enforced downstream by the partition-key codec /
        // coercion, so we do not over-constrain here (a UUID `?` is valid).
        let parameters = (0..select_marker_count)
            .map(|position| ParameterMetadata {
                name: None,
                position,
                expected_type: None,
                optional: false,
            })
            .collect();

        Self {
            cql,
            parsed_query,
            plan,
            parameters,
            executor,
            select_pipeline: Some(PreparedSelect {
                statement,
                optimizer,
                executor: select_executor,
            }),
        }
    }

    /// Execute the prepared query with positional parameters.
    ///
    /// Issue #961: for prepared SELECTs the parameters are bound into the parsed
    /// statement and the bound query runs through the SELECT optimizer + executor,
    /// so a prepared `WHERE pk = ?` engages the partition-targeted fast path.
    /// Non-SELECT prepared statements retain the legacy executor path.
    pub async fn execute(&self, params: &[Value]) -> Result<QueryResult> {
        self.validate_params(params)?;

        #[cfg(feature = "state_machine")]
        if let Some(pipeline) = &self.select_pipeline {
            let mut statement = pipeline.statement.clone();
            statement.bind_parameters(params)?;
            let optimized = pipeline.optimizer.optimize(statement).await?;
            return pipeline.executor.execute(optimized).await;
        }

        // Default execution path: no hints, so skip the plan clone in
        // execute_with_context and call the executor directly.
        self.executor.execute(&self.plan).await
    }

    /// Execute with named parameters
    pub async fn execute_named(&self, params: &HashMap<String, Value>) -> Result<QueryResult> {
        // Convert named parameters to positional, in declaration order.
        let mut positional_params = Vec::with_capacity(self.parameters.len());
        for metadata in &self.parameters {
            let Some(name) = &metadata.name else {
                continue;
            };
            match params.get(name) {
                Some(value) => positional_params.push(value.clone()),
                None if metadata.optional => positional_params.push(Value::Null),
                None => {
                    return Err(Error::query_execution(format!(
                        "Missing required parameter: {}",
                        name
                    )));
                }
            }
        }

        self.execute(&positional_params).await
    }

    /// Execute with execution context.
    ///
    /// Issue #961 (Finding 2): when this prepared statement is a SELECT it owns a
    /// [`PreparedSelect`] pipeline, and the context's `positional_params` must be
    /// bound and run through the same SELECT optimizer + executor as
    /// [`Self::execute`]. Previously this method ignored the pipeline and ran the
    /// legacy `QueryExecutor` plan with the parameters silently dropped, so a
    /// prepared `WHERE pk = ?` executed through the context API never bound its
    /// params and never engaged the partition-targeted fast path — inconsistent
    /// with `execute`.
    ///
    /// The legacy `ExecutionHints` (`force_index`, `timeout_ms`, `parallelism`)
    /// are properties of the legacy `QueryPlan` and have no representation in the
    /// SELECT pipeline. Rather than silently ignore a caller-supplied hint, a
    /// SELECT prepared statement that is handed any hint returns a clear error.
    /// SELECTs with no hints bind their params and run through the pipeline,
    /// matching `execute(context.positional_params)`.
    pub async fn execute_with_context(&self, context: &PreparedContext) -> Result<QueryResult> {
        let hints = &context.hints;

        #[cfg(feature = "state_machine")]
        if let Some(pipeline) = &self.select_pipeline {
            if hints.force_index.is_some()
                || hints.timeout_ms.is_some()
                || hints.parallelism.is_some()
            {
                return Err(Error::query_execution(
                    "Execution hints (force_index/timeout_ms/parallelism) are not supported for \
                     prepared SELECT statements; they apply only to the legacy plan path. Re-run \
                     without hints to bind parameters and use the partition-targeted SELECT path.",
                ));
            }
            // Bind the context's positional params and run the same SELECT
            // optimizer + executor as `execute`, so the fast path engages.
            self.validate_params(&context.positional_params)?;
            let mut statement = pipeline.statement.clone();
            statement.bind_parameters(&context.positional_params)?;
            let optimized = pipeline.optimizer.optimize(statement).await?;
            return pipeline.executor.execute(optimized).await;
        }

        // Avoid cloning the plan if no hints would override it.
        if hints.force_index.is_none() && hints.timeout_ms.is_none() && hints.parallelism.is_none()
        {
            return self.executor.execute(&self.plan).await;
        }

        let mut modified_plan = self.plan.clone();
        if let Some(force_index) = &hints.force_index {
            modified_plan.hints.force_index = Some(force_index.clone());
        }
        if let Some(timeout) = hints.timeout_ms {
            modified_plan.hints.timeout_ms = Some(timeout);
        }
        if let Some(parallelism) = hints.parallelism {
            modified_plan.hints.preferred_parallelization = Some(parallelism);
        }
        self.executor.execute(&modified_plan).await
    }

    /// Get parameter metadata
    pub fn parameters(&self) -> &[ParameterMetadata] {
        &self.parameters
    }

    /// Get CQL text
    pub fn cql(&self) -> &str {
        &self.cql
    }

    /// Get query plan
    pub fn plan(&self) -> &QueryPlan {
        &self.plan
    }

    /// Get query statistics
    pub fn stats(&self) -> PreparedQueryStats {
        PreparedQueryStats {
            parameter_count: self.parameters.len(),
            plan_type: format!("{:?}", self.plan.plan_type),
            estimated_cost: self.plan.estimated_cost,
            estimated_rows: self.plan.estimated_rows,
            cache_friendly: self.is_cache_friendly(),
        }
    }

    /// Check if query is cache-friendly
    pub fn is_cache_friendly(&self) -> bool {
        // Cache-friendly plans have predictable execution patterns and no complex
        // aggregations. The simplified implementation also treats TableScan as
        // cache-friendly.
        matches!(
            self.plan.plan_type,
            PlanType::PointLookup | PlanType::IndexScan | PlanType::TableScan
        )
    }

    /// Validate parameter count and types against this query's metadata.
    fn validate_params(&self, params: &[Value]) -> Result<()> {
        if params.len() != self.parameters.len() {
            return Err(Error::query_execution(format!(
                "Parameter count mismatch: expected {}, got {}",
                self.parameters.len(),
                params.len()
            )));
        }

        for (i, (param, metadata)) in params.iter().zip(&self.parameters).enumerate() {
            if let Some(expected_type) = &metadata.expected_type {
                if !type_matches(param, expected_type) {
                    return Err(Error::query_execution(format!(
                        "Parameter {} type mismatch: expected {:?}, got {:?}",
                        i, expected_type, param
                    )));
                }
            }
        }

        Ok(())
    }

    /// Extract parameter placeholders from parsed query.
    ///
    /// Simplified implementation: a single positional Integer parameter is
    /// emitted whenever the query has a WHERE clause. A real implementation
    /// would scan the CQL text for `?` and `:name` placeholders.
    fn extract_parameters(parsed_query: &ParsedQuery) -> Vec<ParameterMetadata> {
        if parsed_query.where_clause.is_none() {
            return Vec::new();
        }
        vec![ParameterMetadata {
            name: None,
            position: 0,
            expected_type: Some(DataType::Integer),
            optional: false,
        }]
    }
}

/// Check if `value` matches `expected_type`. `Null` is compatible with any type.
fn type_matches(value: &Value, expected_type: &DataType) -> bool {
    matches!(
        (value, expected_type),
        (Value::Integer(_), DataType::Integer)
            | (Value::Float(_), DataType::Float)
            | (Value::Text(_), DataType::Text)
            | (Value::Boolean(_), DataType::Boolean)
            | (Value::Null, _)
    )
}

/// Statistics for prepared queries
#[derive(Debug, Clone)]
pub struct PreparedQueryStats {
    /// Number of parameters
    pub parameter_count: usize,
    /// Plan type
    pub plan_type: String,
    /// Estimated execution cost
    pub estimated_cost: f64,
    /// Estimated rows returned
    pub estimated_rows: u64,
    /// Whether query is cache-friendly
    pub cache_friendly: bool,
}

/// Prepared statement builder
#[derive(Default)]
pub struct PreparedQueryBuilder {
    /// CQL text
    cql: String,
    /// Parameter metadata
    parameters: Vec<ParameterMetadata>,
    /// Execution hints
    hints: ExecutionHints,
}

impl PreparedQueryBuilder {
    /// Create a new builder
    pub fn new(cql: &str) -> Self {
        Self {
            cql: cql.to_string(),
            ..Self::default()
        }
    }

    /// Add a parameter
    pub fn parameter(mut self, name: Option<String>, data_type: DataType, optional: bool) -> Self {
        self.push_parameter(name, data_type, optional);
        self
    }

    /// Add a positional parameter
    pub fn positional_parameter(mut self, data_type: DataType) -> Self {
        self.push_parameter(None, data_type, false);
        self
    }

    /// Add a named parameter
    pub fn named_parameter(mut self, name: &str, data_type: DataType, optional: bool) -> Self {
        self.push_parameter(Some(name.to_string()), data_type, optional);
        self
    }

    /// Set execution hints
    pub fn hints(mut self, hints: ExecutionHints) -> Self {
        self.hints = hints;
        self
    }

    /// Force index usage
    pub fn force_index(mut self, index_name: &str) -> Self {
        self.hints.force_index = Some(index_name.to_string());
        self
    }

    /// Set query timeout
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.hints.timeout_ms = Some(timeout_ms);
        self
    }

    /// Set parallelism preference
    pub fn parallelism(mut self, threads: usize) -> Self {
        self.hints.parallelism = Some(threads);
        self
    }

    /// Enable result caching
    pub fn cache_results(mut self) -> Self {
        self.hints.cache_results = true;
        self
    }

    /// Build the prepared query (this would typically be called by the query engine)
    pub fn build(
        self,
        parsed_query: ParsedQuery,
        plan: QueryPlan,
        executor: Arc<QueryExecutor>,
    ) -> PreparedQuery {
        PreparedQuery {
            cql: self.cql,
            parsed_query,
            plan,
            parameters: self.parameters,
            executor,
            #[cfg(feature = "state_machine")]
            select_pipeline: None,
        }
    }

    fn push_parameter(&mut self, name: Option<String>, data_type: DataType, optional: bool) {
        self.parameters.push(ParameterMetadata {
            name,
            position: self.parameters.len(),
            expected_type: Some(data_type),
            optional,
        });
    }
}

#[cfg(all(test, feature = "state_machine"))]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_prepared_query_creation() {
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
        let executor = Arc::new(crate::query::executor::QueryExecutor::new(
            storage, schema, &config,
        ));

        let parsed_query = ParsedQuery {
            query_type: crate::query::QueryType::Select,
            table: Some(crate::TableId::new("users")),
            columns: vec!["*".to_string()],
            where_clause: None,
            values: vec![],
            set_clause: std::collections::HashMap::new(),
            order_by: vec![],
            limit: None,
            cql: "SELECT * FROM users".to_string(),
        };

        let plan = crate::query::planner::QueryPlan {
            plan_type: crate::query::planner::PlanType::TableScan,
            table: Some(crate::TableId::new("users")),
            estimated_cost: 100.0,
            estimated_rows: 1000,
            selected_indexes: vec![],
            steps: vec![],
            hints: crate::query::planner::QueryHints::default(),
        };

        let prepared = PreparedQuery::new(parsed_query, plan, executor);

        assert_eq!(prepared.cql(), "SELECT * FROM users");
        assert_eq!(prepared.parameters().len(), 0);
        // TableScan is treated as cache-friendly by the simplified implementation.
        assert!(prepared.is_cache_friendly());
    }

    #[test]
    fn test_prepared_query_builder() {
        let builder = PreparedQueryBuilder::new("SELECT * FROM users WHERE id = ? AND name = ?")
            .positional_parameter(DataType::Integer)
            .positional_parameter(DataType::Text)
            .timeout(5000)
            .parallelism(4);

        assert_eq!(builder.cql, "SELECT * FROM users WHERE id = ? AND name = ?");
        assert_eq!(builder.parameters.len(), 2);
        assert_eq!(builder.hints.timeout_ms, Some(5000));
        assert_eq!(builder.hints.parallelism, Some(4));
    }

    #[test]
    fn test_parameter_metadata() {
        let metadata = ParameterMetadata {
            name: Some("user_id".to_string()),
            position: 0,
            expected_type: Some(DataType::Integer),
            optional: false,
        };

        assert_eq!(metadata.name, Some("user_id".to_string()));
        assert_eq!(metadata.position, 0);
        assert!(!metadata.optional);
    }

    #[test]
    fn test_execution_hints() {
        let hints = ExecutionHints {
            force_index: Some("idx_user_id".to_string()),
            timeout_ms: Some(10000),
            parallelism: Some(8),
            cache_results: true,
        };

        assert_eq!(hints.force_index, Some("idx_user_id".to_string()));
        assert_eq!(hints.timeout_ms, Some(10000));
        assert_eq!(hints.parallelism, Some(8));
        assert!(hints.cache_results);
    }

    #[test]
    fn test_type_matching() {
        assert!(type_matches(&Value::Integer(42), &DataType::Integer));
        assert!(type_matches(
            &Value::Text("test".to_string()),
            &DataType::Text
        ));
        // Null matches any expected type.
        assert!(type_matches(&Value::Null, &DataType::Integer));
        assert!(!type_matches(&Value::Integer(42), &DataType::Text));
    }
}
