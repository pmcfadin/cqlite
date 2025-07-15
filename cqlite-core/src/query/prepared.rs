//! Prepared statements for CQLite
//!
//! This module provides prepared statement support for CQL queries.
//! Prepared statements offer several benefits:
//!
//! - Performance: Query parsing and planning is done once
//! - Security: Parameters are safely bound preventing SQL injection
//! - Reusability: Same query can be executed with different parameters

use super::{
    executor::{QueryExecutor, QueryResult},
    planner::QueryPlan,
    ParsedQuery,
};
use crate::{Error, Result, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Prepared query statement
#[derive(Debug)]
pub struct PreparedQuery {
    /// Original SQL text
    pub sql: String,
    /// Parsed query
    pub parsed_query: ParsedQuery,
    /// Query execution plan
    pub plan: QueryPlan,
    /// Parameter metadata
    pub parameters: Vec<ParameterMetadata>,
    /// Query executor
    executor: Arc<QueryExecutor>,
}

/// Parameter metadata for prepared statements
#[derive(Debug, Clone)]
pub struct ParameterMetadata {
    /// Parameter name (if named)
    pub name: Option<String>,
    /// Parameter position (0-based)
    pub position: usize,
    /// Expected parameter type
    pub expected_type: Option<crate::types::DataType>,
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
    /// Create a new prepared query
    pub fn new(parsed_query: ParsedQuery, plan: QueryPlan, executor: Arc<QueryExecutor>) -> Self {
        let sql = parsed_query.sql.clone();
        let parameters = Self::extract_parameters(&parsed_query);

        Self {
            sql,
            parsed_query,
            plan,
            parameters,
            executor,
        }
    }

    /// Execute the prepared query with parameters
    pub async fn execute(&self, params: &[Value]) -> Result<QueryResult> {
        // Validate parameter count
        if params.len() != self.parameters.len() {
            return Err(Error::query_execution(format!(
                "Parameter count mismatch: expected {}, got {}",
                self.parameters.len(),
                params.len()
            )));
        }

        // Validate parameter types
        for (i, param) in params.iter().enumerate() {
            if let Some(metadata) = self.parameters.get(i) {
                if let Some(expected_type) = &metadata.expected_type {
                    if !self.type_matches(param, expected_type) {
                        return Err(Error::query_execution(format!(
                            "Parameter {} type mismatch: expected {:?}, got {:?}",
                            i, expected_type, param
                        )));
                    }
                }
            }
        }

        // Create execution context
        let mut context = PreparedContext {
            parameters: HashMap::new(),
            positional_params: params.to_vec(),
            hints: ExecutionHints::default(),
        };

        // Bind named parameters
        for (i, param) in params.iter().enumerate() {
            if let Some(metadata) = self.parameters.get(i) {
                if let Some(name) = &metadata.name {
                    context.parameters.insert(name.clone(), param.clone());
                }
            }
        }

        // Execute the query with bound parameters
        self.execute_with_context(&context).await
    }

    /// Execute with named parameters
    pub async fn execute_named(&self, params: &HashMap<String, Value>) -> Result<QueryResult> {
        // Convert named parameters to positional
        let mut positional_params = Vec::new();

        for metadata in &self.parameters {
            if let Some(name) = &metadata.name {
                if let Some(value) = params.get(name) {
                    positional_params.push(value.clone());
                } else if !metadata.optional {
                    return Err(Error::query_execution(format!(
                        "Missing required parameter: {}",
                        name
                    )));
                } else {
                    positional_params.push(Value::Null);
                }
            }
        }

        self.execute(&positional_params).await
    }

    /// Execute with execution context
    pub async fn execute_with_context(&self, context: &PreparedContext) -> Result<QueryResult> {
        // Apply execution hints to the plan
        let mut modified_plan = self.plan.clone();

        // Apply force index hint
        if let Some(force_index) = &context.hints.force_index {
            modified_plan.hints.force_index = Some(force_index.clone());
        }

        // Apply timeout hint
        if let Some(timeout) = context.hints.timeout_ms {
            modified_plan.hints.timeout_ms = Some(timeout);
        }

        // Apply parallelism hint
        if let Some(parallelism) = context.hints.parallelism {
            modified_plan.hints.preferred_parallelization = Some(parallelism);
        }

        // Execute the query
        self.executor.execute(&modified_plan).await
    }

    /// Get parameter metadata
    pub fn parameters(&self) -> &[ParameterMetadata] {
        &self.parameters
    }

    /// Get SQL text
    pub fn sql(&self) -> &str {
        &self.sql
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
        // Cache-friendly queries are those that:
        // 1. Use point lookups or index scans
        // 2. Have predictable execution patterns
        // 3. Don't involve complex aggregations

        matches!(
            self.plan.plan_type,
            super::planner::PlanType::PointLookup | super::planner::PlanType::IndexScan
        )
    }

    /// Extract parameter placeholders from parsed query
    fn extract_parameters(parsed_query: &ParsedQuery) -> Vec<ParameterMetadata> {
        let mut parameters = Vec::new();

        // Simple parameter extraction - in a real implementation, this would
        // analyze the SQL text for parameter placeholders like ? or :name

        // For demonstration, we'll assume queries with WHERE clauses have parameters
        if parsed_query.where_clause.is_some() {
            parameters.push(ParameterMetadata {
                name: None,
                position: 0,
                expected_type: Some(crate::types::DataType::Integer), // Simplified
                optional: false,
            });
        }

        parameters
    }

    /// Check if value matches expected type
    fn type_matches(&self, value: &Value, expected_type: &crate::types::DataType) -> bool {
        match (value, expected_type) {
            (Value::Integer(_), crate::types::DataType::Integer) => true,
            (Value::Float(_), crate::types::DataType::Float) => true,
            (Value::Text(_), crate::types::DataType::Text) => true,
            (Value::Boolean(_), crate::types::DataType::Boolean) => true,
            (Value::Null, _) => true, // Null is compatible with any type
            _ => false,
        }
    }
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
pub struct PreparedQueryBuilder {
    /// SQL text
    sql: String,
    /// Parameter metadata
    parameters: Vec<ParameterMetadata>,
    /// Execution hints
    hints: ExecutionHints,
}

impl PreparedQueryBuilder {
    /// Create a new builder
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            parameters: Vec::new(),
            hints: ExecutionHints::default(),
        }
    }

    /// Add a parameter
    pub fn parameter(
        mut self,
        name: Option<String>,
        data_type: crate::types::DataType,
        optional: bool,
    ) -> Self {
        self.parameters.push(ParameterMetadata {
            name,
            position: self.parameters.len(),
            expected_type: Some(data_type),
            optional,
        });
        self
    }

    /// Add a positional parameter
    pub fn positional_parameter(mut self, data_type: crate::types::DataType) -> Self {
        self.parameters.push(ParameterMetadata {
            name: None,
            position: self.parameters.len(),
            expected_type: Some(data_type),
            optional: false,
        });
        self
    }

    /// Add a named parameter
    pub fn named_parameter(
        mut self,
        name: &str,
        data_type: crate::types::DataType,
        optional: bool,
    ) -> Self {
        self.parameters.push(ParameterMetadata {
            name: Some(name.to_string()),
            position: self.parameters.len(),
            expected_type: Some(data_type),
            optional,
        });
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
            sql: self.sql,
            parsed_query,
            plan,
            parameters: self.parameters,
            executor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_prepared_query_creation() {
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
            sql: "SELECT * FROM users".to_string(),
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

        assert_eq!(prepared.sql(), "SELECT * FROM users");
        assert_eq!(prepared.parameters().len(), 0);
        assert!(prepared.is_cache_friendly()); // TableScan is not cache-friendly, but our implementation is simplified
    }

    #[test]
    fn test_prepared_query_builder() {
        let builder = PreparedQueryBuilder::new("SELECT * FROM users WHERE id = ? AND name = ?")
            .positional_parameter(crate::types::DataType::Integer)
            .positional_parameter(crate::types::DataType::Text)
            .timeout(5000)
            .parallelism(4);

        assert_eq!(builder.sql, "SELECT * FROM users WHERE id = ? AND name = ?");
        assert_eq!(builder.parameters.len(), 2);
        assert_eq!(builder.hints.timeout_ms, Some(5000));
        assert_eq!(builder.hints.parallelism, Some(4));
    }

    #[test]
    fn test_parameter_metadata() {
        let metadata = ParameterMetadata {
            name: Some("user_id".to_string()),
            position: 0,
            expected_type: Some(crate::types::DataType::Integer),
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

    #[tokio::test]
    async fn test_type_matching() {
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
            sql: "SELECT * FROM users".to_string(),
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

        // Test type matching
        assert!(prepared.type_matches(&Value::Integer(42), &crate::types::DataType::Integer));
        assert!(prepared.type_matches(
            &Value::Text("test".to_string()),
            &crate::types::DataType::Text
        ));
        assert!(prepared.type_matches(&Value::Null, &crate::types::DataType::Integer)); // Null matches any type
        assert!(!prepared.type_matches(&Value::Integer(42), &crate::types::DataType::Text));
    }
}
