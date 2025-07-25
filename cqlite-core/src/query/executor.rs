//! Query executor for CQLite
//!
//! This module provides query execution capabilities for CQL queries.
//! It includes:
//!
//! - Query plan execution
//! - Parallel query processing
//! - Result set construction
//! - Index utilization

use super::{
    planner::{ExecutionStep, ParallelizationInfo, QueryPlan, StepType},
    ComparisonOperator, Condition,
};
use crate::{
    schema::SchemaManager, storage::StorageEngine, Config, Error, Result, RowKey, TableId, Value,
};
use crossbeam::channel;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

// Use QueryResult and QueryRow from result module
pub use super::result::{QueryResult, QueryRow};

/// Query executor
#[derive(Debug, Clone)]
pub struct QueryExecutor {
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Schema manager reference
    schema: Arc<SchemaManager>,
    /// Configuration
    config: Config,
    /// Thread pool for parallel execution
    thread_pool: tokio::runtime::Handle,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<StorageEngine>, schema: Arc<SchemaManager>, config: &Config) -> Self {
        Self {
            storage,
            schema,
            config: config.clone(),
            thread_pool: tokio::runtime::Handle::current(),
        }
    }

    /// Execute a query plan
    pub async fn execute(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let start_time = Instant::now();

        let result = match plan.plan_type {
            super::planner::PlanType::PointLookup => self.execute_point_lookup(plan).await,
            super::planner::PlanType::IndexScan => self.execute_index_scan(plan).await,
            super::planner::PlanType::RangeScan => self.execute_range_scan(plan).await,
            super::planner::PlanType::TableScan => self.execute_table_scan(plan).await,
            super::planner::PlanType::Join => self.execute_join(plan).await,
            super::planner::PlanType::Aggregation => self.execute_aggregation(plan).await,
            super::planner::PlanType::Subquery => self.execute_subquery(plan).await,
        };

        let execution_time = start_time.elapsed();

        match result {
            Ok(mut query_result) => {
                query_result.execution_time_ms = execution_time.as_millis() as u64;
                query_result.metadata.plan_info = Some(super::result::PlanInfo {
                    plan_type: format!("{:?}", plan.plan_type),
                    estimated_cost: plan.estimated_cost,
                    actual_cost: execution_time.as_millis() as f64,
                    indexes_used: Vec::new(), // TODO: populate with actual indexes used
                    steps: plan
                        .steps
                        .iter()
                        .map(|s| format!("{:?}", s.step_type))
                        .collect(),
                    parallelization: if plan.steps.iter().any(|s| s.parallelization.can_parallelize)
                    {
                        Some(super::result::ParallelizationInfo {
                            threads_used: plan
                                .steps
                                .iter()
                                .find(|s| s.parallelization.can_parallelize)
                                .map(|s| s.parallelization.suggested_threads)
                                .unwrap_or(1),
                            effective: true,
                            partitions: Vec::new(),
                        })
                    } else {
                        None
                    },
                });
                Ok(query_result)
            }
            Err(e) => Err(e),
        }
    }

    /// Execute point lookup plan
    async fn execute_point_lookup(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = plan
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in plan".to_string()))?;

        // Find the lookup condition
        let lookup_condition = plan
            .steps
            .iter()
            .find_map(|step| step.conditions.first())
            .ok_or_else(|| Error::query_execution("No lookup condition found".to_string()))?;

        // Convert condition value to row key
        let row_key = self.value_to_row_key(&lookup_condition.value)?;

        // Perform the lookup
        let mut rows = Vec::new();
        if let Some(row_data) = self.storage.get(table, &row_key).await? {
            // Convert storage data to query row
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        let mut result = QueryResult::with_rows(rows);
        result.execution_time_ms = 0; // Will be set by caller
        Ok(result)
    }

    /// Execute index scan plan
    async fn execute_index_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = plan
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in plan".to_string()))?;

        // Get the first index selection
        let index_selection = plan
            .selected_indexes
            .first()
            .ok_or_else(|| Error::query_execution("No index selected".to_string()))?;

        // Execute based on index type
        let mut rows = match index_selection.index_type {
            super::planner::IndexType::Secondary => {
                self.execute_secondary_index_scan(table, index_selection, &plan.steps)
                    .await?
            }
            super::planner::IndexType::BloomFilter => {
                self.execute_bloom_filter_scan(table, index_selection, &plan.steps)
                    .await?
            }
            super::planner::IndexType::Primary => {
                self.execute_primary_index_scan(table, index_selection, &plan.steps)
                    .await?
            }
            super::planner::IndexType::Composite => {
                self.execute_composite_index_scan(table, index_selection, &plan.steps)
                    .await?
            }
        };

        // Apply additional processing steps
        rows = self.apply_execution_steps(rows, &plan.steps).await?;

        let mut result = QueryResult::with_rows(rows);
        result.execution_time_ms = 0;
        Ok(result)
    }

    /// Execute range scan plan
    async fn execute_range_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = plan
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in plan".to_string()))?;

        // Find range conditions
        let _range_conditions = self.extract_range_conditions(&plan.steps);

        // Execute range scan
        let mut rows = Vec::new();

        // Use storage engine's scan capability (simplified range scan)
        let range_results = self.storage.scan(table, None, None, None).await?;

        // Process results
        for (row_key, row_data) in range_results {
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        // Apply additional processing steps
        rows = self.apply_execution_steps(rows, &plan.steps).await?;

        let mut result = QueryResult::with_rows(rows);
        result.execution_time_ms = 0;
        Ok(result)
    }

    /// Execute table scan plan
    async fn execute_table_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = plan
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in plan".to_string()))?;

        // Check if we can parallelize the scan
        let can_parallelize = plan
            .steps
            .iter()
            .any(|step| step.parallelization.can_parallelize);

        let mut rows = if can_parallelize {
            self.execute_parallel_table_scan(table, &plan.steps).await?
        } else {
            self.execute_sequential_table_scan(table, &plan.steps)
                .await?
        };

        // Apply additional processing steps
        rows = self.apply_execution_steps(rows, &plan.steps).await?;

        let mut result = QueryResult::with_rows(rows);
        result.execution_time_ms = 0;
        Ok(result)
    }

    /// Execute join plan
    async fn execute_join(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        // Simplified join implementation
        // In a real implementation, this would handle complex join operations
        Ok(QueryResult::new())
    }

    /// Execute aggregation plan
    async fn execute_aggregation(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        // Simplified aggregation implementation
        // In a real implementation, this would handle GROUP BY, COUNT, SUM, etc.
        Ok(QueryResult::new())
    }

    /// Execute subquery plan
    async fn execute_subquery(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        // Simplified subquery implementation
        // In a real implementation, this would execute nested queries
        Ok(QueryResult::new())
    }

    /// Execute secondary index scan
    async fn execute_secondary_index_scan(
        &self,
        table: &TableId,
        index_selection: &super::planner::IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Find the condition for this index
        let _condition = steps
            .iter()
            .find_map(|step| {
                step.conditions
                    .iter()
                    .find(|c| c.column == index_selection.columns[0])
            })
            .ok_or_else(|| Error::query_execution("No condition found for index".to_string()))?;

        // Simplified secondary index scan using basic scan
        let scan_results = self.storage.scan(table, None, None, None).await?;

        // Process results and filter by condition
        for (row_key, row_data) in scan_results {
            // TODO: Implement proper secondary index lookup
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        Ok(rows)
    }

    /// Execute bloom filter scan
    async fn execute_bloom_filter_scan(
        &self,
        table: &TableId,
        index_selection: &super::planner::IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Find the condition for this bloom filter
        let condition = steps
            .iter()
            .find_map(|step| {
                step.conditions
                    .iter()
                    .find(|c| c.column == index_selection.columns[0])
            })
            .ok_or_else(|| {
                Error::query_execution("No condition found for bloom filter".to_string())
            })?;

        // Simplified bloom filter scan - just do direct lookup
        let row_key = self.value_to_row_key(&condition.value)?;

        // Direct lookup instead of bloom filter check
        if let Some(row_data) = self.storage.get(table, &row_key).await? {
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        Ok(rows)
    }

    /// Execute primary index scan
    async fn execute_primary_index_scan(
        &self,
        table: &TableId,
        index_selection: &super::planner::IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Find the condition for primary key
        let condition = steps
            .iter()
            .find_map(|step| {
                step.conditions
                    .iter()
                    .find(|c| c.column == index_selection.columns[0])
            })
            .ok_or_else(|| {
                Error::query_execution("No condition found for primary key".to_string())
            })?;

        // Direct primary key lookup
        let row_key = self.value_to_row_key(&condition.value)?;

        if let Some(row_data) = self.storage.get(table, &row_key).await? {
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        Ok(rows)
    }

    /// Execute composite index scan
    async fn execute_composite_index_scan(
        &self,
        table: &TableId,
        index_selection: &super::planner::IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Find conditions for all columns in the composite index
        let mut conditions = Vec::new();
        for column in &index_selection.columns {
            if let Some(condition) = steps
                .iter()
                .find_map(|step| step.conditions.iter().find(|c| c.column == *column))
            {
                conditions.push(condition.clone());
            }
        }

        // Simplified composite index scan using basic scan
        let scan_results = self.storage.scan(table, None, None, None).await?;

        // Process results
        for (row_key, row_data) in scan_results {
            // TODO: Implement proper composite index lookup
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        Ok(rows)
    }

    /// Execute parallel table scan
    async fn execute_parallel_table_scan(
        &self,
        table: &TableId,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Determine parallelization strategy
        let parallelization = steps
            .iter()
            .find(|step| step.parallelization.can_parallelize)
            .map(|step| &step.parallelization)
            .unwrap_or(&ParallelizationInfo {
                can_parallelize: true,
                suggested_threads: 4,
                partition_key: None,
            });

        let thread_count = parallelization.suggested_threads;

        // Create channels for worker communication
        let (tx, rx) = channel::unbounded();

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..thread_count {
            let storage = self.storage.clone();
            let table = table.clone();
            let tx = tx.clone();

            let handle = tokio::spawn(async move {
                // Worker scans a portion of the table (simplified)
                let partition_result = storage.scan(&table, None, None, None).await;

                match partition_result {
                    Ok(results) => {
                        for (row_key, row_data) in results {
                            let _ = tx.send((row_key, row_data));
                        }
                    }
                    Err(e) => {
                        eprintln!("Worker {} error: {:?}", worker_id, e);
                    }
                }
            });

            handles.push(handle);
        }

        // Close the sender
        drop(tx);

        // Collect results
        while let Ok((row_key, row_data)) = rx.recv() {
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        // Wait for all workers to complete
        for handle in handles {
            let _ = handle.await;
        }

        Ok(rows)
    }

    /// Execute sequential table scan
    async fn execute_sequential_table_scan(
        &self,
        table: &TableId,
        _steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::new();

        // Use storage engine's sequential scan
        let scan_results = self.storage.scan(table, None, None, None).await?;

        // Process results
        for (row_key, row_data) in scan_results {
            let query_row = self.storage_data_to_query_row(row_data, &row_key)?;
            rows.push(query_row);
        }

        Ok(rows)
    }

    /// Apply execution steps to result rows
    async fn apply_execution_steps(
        &self,
        mut rows: Vec<QueryRow>,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        for step in steps {
            match step.step_type {
                StepType::Filter => {
                    rows = self.apply_filter_step(rows, step).await?;
                }
                StepType::Sort => {
                    rows = self.apply_sort_step(rows, step).await?;
                }
                StepType::Limit => {
                    rows = self.apply_limit_step(rows, step).await?;
                }
                StepType::Project => {
                    rows = self.apply_project_step(rows, step).await?;
                }
                StepType::Aggregate => {
                    rows = self.apply_aggregate_step(rows, step).await?;
                }
                StepType::Join => {
                    rows = self.apply_join_step(rows, step).await?;
                }
                StepType::Scan => {
                    // Scan step is handled by the scan methods
                }
            }
        }

        Ok(rows)
    }

    /// Apply filter step
    async fn apply_filter_step(
        &self,
        rows: Vec<QueryRow>,
        step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        let mut filtered_rows = Vec::new();

        for row in rows {
            let mut matches = true;

            for condition in &step.conditions {
                if !self.evaluate_condition(&row, condition)? {
                    matches = false;
                    break;
                }
            }

            if matches {
                filtered_rows.push(row);
            }
        }

        Ok(filtered_rows)
    }

    /// Apply sort step
    async fn apply_sort_step(
        &self,
        mut rows: Vec<QueryRow>,
        step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        if step.columns.is_empty() {
            return Ok(rows);
        }

        let sort_column = &step.columns[0];

        rows.sort_by(|a, b| {
            let a_val = a.values.get(sort_column).unwrap_or(&Value::Null);
            let b_val = b.values.get(sort_column).unwrap_or(&Value::Null);

            self.compare_values(a_val, b_val)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(rows)
    }

    /// Apply limit step
    async fn apply_limit_step(
        &self,
        rows: Vec<QueryRow>,
        _step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        // Limit is typically handled at the query level
        // For now, we'll just return the rows as-is
        Ok(rows)
    }

    /// Apply project step
    async fn apply_project_step(
        &self,
        rows: Vec<QueryRow>,
        step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        let mut projected_rows = Vec::new();

        for row in rows {
            let mut projected_values = HashMap::new();

            for column in &step.columns {
                if let Some(value) = row.values.get(column) {
                    projected_values.insert(column.clone(), value.clone());
                }
            }

            projected_rows.push(QueryRow::with_values(row.key.clone(), projected_values));
        }

        Ok(projected_rows)
    }

    /// Apply aggregate step
    async fn apply_aggregate_step(
        &self,
        rows: Vec<QueryRow>,
        _step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        // Simplified aggregation - in a real implementation, this would handle
        // GROUP BY, COUNT, SUM, AVG, etc.
        Ok(rows)
    }

    /// Apply join step
    async fn apply_join_step(
        &self,
        rows: Vec<QueryRow>,
        _step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        // Simplified join - in a real implementation, this would handle
        // various join types (INNER, LEFT, RIGHT, FULL)
        Ok(rows)
    }

    /// Extract range conditions from execution steps
    fn extract_range_conditions(&self, steps: &[ExecutionStep]) -> Vec<Condition> {
        let mut range_conditions = Vec::new();

        for step in steps {
            for condition in &step.conditions {
                if matches!(
                    condition.operator,
                    ComparisonOperator::LessThan
                        | ComparisonOperator::LessThanOrEqual
                        | ComparisonOperator::GreaterThan
                        | ComparisonOperator::GreaterThanOrEqual
                ) {
                    range_conditions.push(condition.clone());
                }
            }
        }

        range_conditions
    }

    /// Evaluate a condition against a row
    fn evaluate_condition(&self, row: &QueryRow, condition: &Condition) -> Result<bool> {
        let row_value = row.values.get(&condition.column).unwrap_or(&Value::Null);

        match condition.operator {
            ComparisonOperator::Equal => Ok(row_value == &condition.value),
            ComparisonOperator::NotEqual => Ok(row_value != &condition.value),
            ComparisonOperator::LessThan => {
                match self.compare_values(row_value, &condition.value)? {
                    std::cmp::Ordering::Less => Ok(true),
                    _ => Ok(false),
                }
            }
            ComparisonOperator::LessThanOrEqual => {
                match self.compare_values(row_value, &condition.value)? {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(true),
                    _ => Ok(false),
                }
            }
            ComparisonOperator::GreaterThan => {
                match self.compare_values(row_value, &condition.value)? {
                    std::cmp::Ordering::Greater => Ok(true),
                    _ => Ok(false),
                }
            }
            ComparisonOperator::GreaterThanOrEqual => {
                match self.compare_values(row_value, &condition.value)? {
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => Ok(true),
                    _ => Ok(false),
                }
            }
            ComparisonOperator::In => {
                // Simplified IN operator
                Ok(row_value == &condition.value)
            }
            ComparisonOperator::NotIn => {
                // Simplified NOT IN operator
                Ok(row_value != &condition.value)
            }
            ComparisonOperator::Like => {
                // Simplified LIKE operator
                match (row_value, &condition.value) {
                    (Value::Text(row_text), Value::Text(pattern)) => Ok(row_text.contains(pattern)),
                    _ => Ok(false),
                }
            }
            ComparisonOperator::NotLike => {
                // Simplified NOT LIKE operator
                match (row_value, &condition.value) {
                    (Value::Text(row_text), Value::Text(pattern)) => {
                        Ok(!row_text.contains(pattern))
                    }
                    _ => Ok(true),
                }
            }
        }
    }

    /// Compare two values
    fn compare_values(&self, a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            }
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            (Value::Null, _) => Ok(std::cmp::Ordering::Less),
            (_, Value::Null) => Ok(std::cmp::Ordering::Greater),
            _ => Err(Error::query_execution(
                "Cannot compare values of different types".to_string(),
            )),
        }
    }

    /// Convert Value to RowKey
    fn value_to_row_key(&self, value: &Value) -> Result<RowKey> {
        match value {
            Value::Integer(i) => Ok(RowKey::new(i.to_be_bytes().to_vec())),
            Value::Text(s) => Ok(RowKey::new(s.as_bytes().to_vec())),
            Value::Float(f) => Ok(RowKey::new(f.to_be_bytes().to_vec())),
            Value::Boolean(b) => Ok(RowKey::new(vec![if *b { 1 } else { 0 }])),
            Value::Null => Ok(RowKey::new(vec![0])),
            _ => Err(Error::query_execution(
                "Cannot convert value to row key".to_string(),
            )),
        }
    }

    /// Convert storage data to query row
    fn storage_data_to_query_row(&self, data: Value, key: &RowKey) -> Result<QueryRow> {
        // In a real implementation, this would deserialize the storage data
        // For now, we'll create a simplified row
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Text(format!("{:?}", key)));
        values.insert("data".to_string(), data);

        Ok(QueryRow::with_values(key.clone(), values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_query_executor_creation() {
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

        let executor = QueryExecutor::new(storage, schema, &config);
        assert_eq!(
            executor.config.query.query_parallelism,
            config.query.query_parallelism
        );
    }

    #[tokio::test]
    async fn test_value_comparison() {
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

        let executor = QueryExecutor::new(storage, schema, &config);

        let a = Value::Integer(10);
        let b = Value::Integer(20);
        let result = executor.compare_values(&a, &b).unwrap();
        assert_eq!(result, std::cmp::Ordering::Less);

        let a = Value::Text("apple".to_string());
        let b = Value::Text("banana".to_string());
        let result = executor.compare_values(&a, &b).unwrap();
        assert_eq!(result, std::cmp::Ordering::Less);
    }

    #[tokio::test]
    async fn test_condition_evaluation() {
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

        let executor = QueryExecutor::new(storage, schema, &config);

        let mut row_values = HashMap::new();
        row_values.insert("id".to_string(), Value::Integer(1));
        row_values.insert("name".to_string(), Value::Text("test".to_string()));

        let row = QueryRow::with_values(RowKey::new(vec![1]), row_values);

        let condition = Condition {
            column: "id".to_string(),
            operator: ComparisonOperator::Equal,
            value: Value::Integer(1),
        };

        let result = executor.evaluate_condition(&row, &condition).unwrap();
        assert!(result);

        let condition = Condition {
            column: "name".to_string(),
            operator: ComparisonOperator::Like,
            value: Value::Text("test".to_string()),
        };

        let result = executor.evaluate_condition(&row, &condition).unwrap();
        assert!(result);
    }
}
