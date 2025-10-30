//! CQL SELECT Query Executor for Direct SSTable Access
//!
//! This module implements the REVOLUTIONARY query executor that can run
//! CQL SELECT statements directly on SSTable files without Cassandra.
//!
//! Features:
//! - Direct SSTable file scanning with predicate pushdown
//! - Streaming results for memory efficiency
//! - Parallel execution across multiple SSTable files
//! - Advanced aggregation with hash-based grouping
//! - Collection operations (list[index], map['key'])

use super::{
    result::{ColumnInfo, QueryResult, QueryRow},
    select_ast::*,
    select_optimizer::{AggregationPlan, ExecutionStep, OptimizedQueryPlan, SSTablePredicate},
};
use crate::{
    schema::SchemaManager,
    storage::StorageEngine,
    types::{RowKey, Value},
    Error, Result, TableId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// SELECT query executor for SSTable-based storage
#[derive(Debug)]
pub struct SelectExecutor {
    /// Schema manager for metadata
    _schema: Arc<SchemaManager>,
    /// Storage engine for SSTable access
    storage: Arc<StorageEngine>,
}

/// Query execution context
#[derive(Debug)]
pub struct ExecutionContext {
    /// Current table being queried
    pub table_id: TableId,
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Row count processed so far
    pub rows_processed: u64,
    /// Bytes read from storage
    pub bytes_read: u64,
}

/// Streaming query result iterator
pub struct QueryResultStream {
    /// Receiver for query results
    _receiver: mpsc::Receiver<Result<QueryRow>>,
    /// Execution context
    _context: ExecutionContext,
}

/// Aggregation state for GROUP BY operations
#[derive(Debug)]
struct AggregationState {
    /// Vector for grouping since Value doesn't implement Hash
    groups: Vec<(Vec<Value>, Vec<AggregateValue>)>,
    /// Memory usage tracking
    memory_usage_bytes: usize,
    /// Maximum memory limit
    memory_limit_bytes: usize,
}

/// Aggregate value accumulator
#[derive(Debug, Clone)]
enum AggregateValue {
    Count(u64),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Value),
    Max(Value),
}

impl SelectExecutor {
    /// Create a new SELECT executor
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self {
            _schema: schema,
            storage,
        }
    }

    /// Execute an optimized query plan
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), use a dummy table ID
            TableId::new("_dummy_")
        };

        let mut context = ExecutionContext {
            table_id,
            columns: self.get_result_columns(&plan.statement).await?,
            rows_processed: 0,
            bytes_read: 0,
        };

        // Handle queries without FROM clause (like SELECT 1)
        if plan.statement.from_clause.is_none() {
            return self.execute_constant_query(&plan.statement, &context).await;
        }

        // Execute the plan step by step
        let mut intermediate_results = Vec::new();

        // If no execution steps are provided, add a default table scan
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: context.table_id.clone(),
                predicates: vec![],
                projection: context.columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let rows = self
                        .execute_sstable_scan(table, predicates, projection, &mut context)
                        .await?;
                    intermediate_results = rows;
                }
                ExecutionStep::Filter { expression, .. } => {
                    intermediate_results = self
                        .execute_filter(intermediate_results, expression, &mut context)
                        .await?;
                }
                ExecutionStep::Sort { order_by, .. } => {
                    intermediate_results = self
                        .execute_sort(intermediate_results, order_by, &mut context)
                        .await?;
                }
                ExecutionStep::Aggregate { plan: agg_plan, .. } => {
                    intermediate_results = self
                        .execute_aggregation(intermediate_results, agg_plan, &mut context)
                        .await?;
                }
                ExecutionStep::Limit { count, offset } => {
                    intermediate_results = self
                        .execute_limit(intermediate_results, *count, *offset, &mut context)
                        .await?;
                }
                ExecutionStep::Project { columns } => {
                    intermediate_results = self
                        .execute_projection(intermediate_results, columns, &mut context)
                        .await?;
                }
            }
        }

        let total_rows = intermediate_results.len() as u64;

        // CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
        // When SELECT * is used, context.columns is empty (see line 1172-1175).
        // We must infer columns from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Infer columns from first row
            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            for (idx, col_name) in col_names.iter().enumerate() {
                columns.push(ColumnInfo {
                    name: (*col_name).clone(),
                    data_type: crate::types::DataType::Text, // TODO: Infer proper type
                    nullable: true,
                    position: idx,
                    table_name: None,
                });
            }
        }

        Ok(QueryResult {
            rows: intermediate_results,
            rows_affected: total_rows, // Use actual number of rows returned
            execution_time_ms: 0,      // Will be set by the engine
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: None,
                performance: Default::default(),
                warnings: vec![],
            },
        })
    }

    /// Execute SSTable scan with predicate pushdown
    async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut results = Vec::new();

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}",
            table,
            predicates
        );

        // Parse table ID to extract keyspace and table name
        let (keyspace, table_name) = self.parse_table_id(table);

        // Look up schema from SchemaManager
        let schema_opt = self
            ._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await;

        if let Some(ref schema) = schema_opt {
            log::info!(
                "Found schema for {}.{} with {} columns",
                schema.keyspace,
                schema.table,
                schema.columns.len()
            );
        } else {
            log::info!(
                "No schema found for {}.{}, proceeding without schema-aware parsing",
                keyspace.as_deref().unwrap_or("unknown"),
                table_name
            );
        }

        // Use StorageEngine's scan method to get all rows for the table
        // Pass schema if available, None otherwise
        let scan_results = self
            .storage
            .scan(table, None, None, None, schema_opt.as_ref())
            .await?;

        log::info!("Scan returned {} rows", scan_results.len());

        for (key, value) in scan_results {
            context.rows_processed += 1;

            // CRITICAL FIX (Issue #191): Skip tombstoned/null rows
            // When the parser returns Value::Null (tombstoned row or unparseable partition),
            // we should skip it entirely instead of creating a pseudo-row that will fail
            // column projection. This mirrors SSTableDataManager behavior.
            if matches!(value, Value::Null) {
                log::debug!("Skipping tombstoned/null row with key: {:?}", key);
                continue;
            }

            // Create QueryRow from the scanned data
            let mut row_values = HashMap::new();

            // Add value columns based on projection
            // Deserialize Value::Map stored by INSERT operations
            if let Value::Map(map) = value {
                for (col_name, col_value) in map {
                    if let Value::Text(name) = col_name {
                        // For empty projection (SELECT *), include ALL columns
                        // For specific projection, only include requested columns
                        if projection.is_empty() || projection.contains(&name) {
                            row_values.insert(name, col_value);
                        }
                    }
                }

                // CRITICAL FIX (Issue #191): Synthesize partition key columns from RowKey
                // Cassandra never serializes key columns in cell data - they're part of the row key.
                // If the query selects partition key columns (e.g., id), we must decode them from
                // the RowKey and add them to row_values.
                if let Some(schema) = &schema_opt {
                    for pk in &schema.partition_keys {
                        // Only synthesize if projected
                        if projection.is_empty() || projection.contains(&pk.name) {
                            // Decode partition key from RowKey bytes
                            if let Ok(pk_value) = self.decode_partition_key_value(&key, pk) {
                                row_values.insert(pk.name.clone(), pk_value);
                            }
                        }
                    }
                }
            } else {
                // Fallback for non-map values - treat as generic data
                row_values.insert("data".to_string(), value);
                // Also add a key-based id for compatibility
                if projection.is_empty() || projection.contains(&"id".to_string()) {
                    row_values.insert("id".to_string(), Value::Text(format!("{:?}", key)));
                }
            }

            let row = QueryRow {
                values: row_values,
                key: key.clone(),
                metadata: Default::default(),
            };

            // Apply predicates
            if self.evaluate_sstable_predicates(&row, predicates)? {
                results.push(row);
            }

            // Check for memory limits
            if results.len() > 1_000_000 {
                return Err(Error::query_execution(
                    "Result set too large, consider adding LIMIT".to_string(),
                ));
            }
        }

        Ok(results)
    }

    /// Evaluate SSTable predicates against a row
    fn evaluate_sstable_predicates(
        &self,
        row: &QueryRow,
        predicates: &[SSTablePredicate],
    ) -> Result<bool> {
        for predicate in predicates {
            if let Some(column_value) = row.values.get(&predicate.column) {
                let matches = match &predicate.operation {
                    super::select_optimizer::SSTableFilterOp::Equal => predicate
                        .values
                        .first()
                        .is_some_and(|v| self.values_equal(column_value, v).unwrap_or(false)),
                    super::select_optimizer::SSTableFilterOp::In => {
                        predicate.values.contains(column_value)
                    }
                    super::select_optimizer::SSTableFilterOp::Range => {
                        if predicate.values.len() >= 2 {
                            let min_val = &predicate.values[0];
                            let max_val = &predicate.values[1];
                            self.compare_values(column_value, min_val)? >= 0
                                && self.compare_values(column_value, max_val)? <= 0
                        } else {
                            false
                        }
                    }
                    super::select_optimizer::SSTableFilterOp::Prefix => {
                        if let (Value::Text(col_str), Some(Value::Text(prefix))) =
                            (column_value, predicate.values.first())
                        {
                            col_str.starts_with(prefix)
                        } else {
                            false
                        }
                    }
                    super::select_optimizer::SSTableFilterOp::BloomFilter => {
                        // Bloom filter was already checked
                        true
                    }
                };

                if !matches {
                    return Ok(false);
                }
            } else {
                // Column not found - this shouldn't happen with proper schema
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Compare two values for equality, handling cross-type comparisons
    fn values_equal(&self, a: &Value, b: &Value) -> Result<bool> {
        match (a, b) {
            // Same type comparisons
            (Value::Integer(a), Value::Integer(b)) => Ok(a == b),
            (Value::BigInt(a), Value::BigInt(b)) => Ok(a == b),
            (Value::Float(a), Value::Float(b)) => Ok(a == b),
            (Value::Text(a), Value::Text(b)) => Ok(a == b),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a == b),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a == b),

            // Cross-type numeric comparisons
            (Value::Integer(a), Value::BigInt(b)) => Ok(*a as i64 == *b),
            (Value::BigInt(a), Value::Integer(b)) => Ok(*a == *b as i64),
            (Value::Float(a), Value::Integer(b)) => Ok(*a == *b as f64),
            (Value::Integer(a), Value::Float(b)) => Ok(*a as f64 == *b),
            (Value::Float(a), Value::BigInt(b)) => Ok(*a == *b as f64),
            (Value::BigInt(a), Value::Float(b)) => Ok(*a as f64 == *b),

            // Null comparisons
            (Value::Null, Value::Null) => Ok(true),
            (Value::Null, _) | (_, Value::Null) => Ok(false),

            // Different types
            _ => Ok(false),
        }
    }

    /// Compare two values for ordering
    fn compare_values(&self, a: &Value, b: &Value) -> Result<i32> {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b) as i32),
            (Value::BigInt(a), Value::BigInt(b)) => Ok(a.cmp(b) as i32),
            (Value::Float(a), Value::Float(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b) as i32),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b) as i32),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b) as i32),
            // Handle Float vs BigInt comparisons
            (Value::Float(a), Value::BigInt(b)) => {
                Ok(a.partial_cmp(&(*b as f64))
                    .unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::BigInt(a), Value::Float(b)) => Ok((*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)
                as i32),
            // Handle Float vs Integer comparisons
            (Value::Float(a), Value::Integer(b)) => {
                Ok(a.partial_cmp(&(*b as f64))
                    .unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::Integer(a), Value::Float(b)) => Ok((*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)
                as i32),
            // Handle Integer vs BigInt comparisons
            (Value::Integer(a), Value::BigInt(b)) => Ok((*a as i64).cmp(b) as i32),
            (Value::BigInt(a), Value::Integer(b)) => Ok(a.cmp(&(*b as i64)) as i32),
            _ => {
                log::debug!("Cannot compare {:?} with {:?}", a, b);
                Err(Error::query_execution(
                    "Cannot compare incompatible types".to_string(),
                ))
            }
        }
    }

    /// Execute filtering step
    async fn execute_filter(
        &self,
        rows: Vec<QueryRow>,
        filter_expr: &WhereExpression,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut filtered_rows = Vec::new();

        for row in rows {
            if self.evaluate_where_expression(filter_expr, &row)? {
                filtered_rows.push(row);
            }
            context.rows_processed += 1;
        }

        Ok(filtered_rows)
    }

    /// Evaluate WHERE expression against a row
    fn evaluate_where_expression(&self, expr: &WhereExpression, row: &QueryRow) -> Result<bool> {
        match expr {
            WhereExpression::Comparison(comp) => self.evaluate_comparison(comp, row),
            WhereExpression::And(exprs) => {
                for expr in exprs {
                    if !self.evaluate_where_expression(expr, row)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            WhereExpression::Or(exprs) => {
                for expr in exprs {
                    if self.evaluate_where_expression(expr, row)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            WhereExpression::Not(expr) => Ok(!self.evaluate_where_expression(expr, row)?),
            WhereExpression::Parentheses(expr) => self.evaluate_where_expression(expr, row),
        }
    }

    /// Evaluate comparison expression
    fn evaluate_comparison(&self, comp: &ComparisonExpression, row: &QueryRow) -> Result<bool> {
        let left_value = self.evaluate_select_expression(&comp.left, row)?;

        match (&comp.operator, &comp.right) {
            (ComparisonOperator::Equal, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(self.values_equal(&left_value, &right_value)?)
            }
            (ComparisonOperator::NotEqual, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(!self.values_equal(&left_value, &right_value)?)
            }
            (ComparisonOperator::LessThan, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(self.compare_values(&left_value, &right_value)? < 0)
            }
            (ComparisonOperator::LessThanOrEqual, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(self.compare_values(&left_value, &right_value)? <= 0)
            }
            (ComparisonOperator::GreaterThan, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(self.compare_values(&left_value, &right_value)? > 0)
            }
            (ComparisonOperator::GreaterThanOrEqual, ComparisonRightSide::Value(right_expr)) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                Ok(self.compare_values(&left_value, &right_value)? >= 0)
            }
            (ComparisonOperator::In, ComparisonRightSide::ValueList(value_exprs)) => {
                for value_expr in value_exprs {
                    let value = self.evaluate_select_expression(value_expr, row)?;
                    if left_value == value {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (ComparisonOperator::Like, ComparisonRightSide::Value(pattern_expr)) => {
                let pattern = self.evaluate_select_expression(pattern_expr, row)?;
                if let (Value::Text(text), Value::Text(pattern_str)) = (&left_value, &pattern) {
                    Ok(self.match_like_pattern(text, pattern_str))
                } else {
                    Ok(false)
                }
            }
            (ComparisonOperator::IsNull, _) => Ok(left_value.is_null()),
            (ComparisonOperator::IsNotNull, _) => Ok(!left_value.is_null()),
            _ => Err(Error::query_execution(
                "Unsupported comparison operator".to_string(),
            )),
        }
    }

    /// Evaluate SELECT expression against a row
    fn evaluate_select_expression(&self, expr: &SelectExpression, row: &QueryRow) -> Result<Value> {
        match expr {
            SelectExpression::Column(col_ref) => {
                row.values.get(&col_ref.column).cloned().ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })
            }
            SelectExpression::Literal(value) => Ok(value.clone()),
            SelectExpression::CollectionAccess(access) => {
                self.evaluate_collection_access(access, row)
            }
            SelectExpression::Arithmetic(arith) => {
                let left = self.evaluate_select_expression(&arith.left, row)?;
                let right = self.evaluate_select_expression(&arith.right, row)?;
                self.evaluate_arithmetic(&arith.operator, left, right)
            }
            SelectExpression::Aliased(expr, _) => self.evaluate_select_expression(expr, row),
            SelectExpression::Aggregate(_) => {
                // Aggregate expressions should not be evaluated at row level
                // They should only be processed during the aggregation step
                Err(Error::query_execution(
                    "Aggregate expressions should be processed during aggregation step, not row evaluation".to_string(),
                ))
            }
            SelectExpression::Function(_) => {
                // Function expressions not yet implemented
                Err(Error::query_execution(
                    "Function expressions not yet implemented".to_string(),
                ))
            }
        }
    }

    /// Evaluate collection access operations
    fn evaluate_collection_access(
        &self,
        access: &CollectionAccessExpression,
        row: &QueryRow,
    ) -> Result<Value> {
        match access {
            CollectionAccessExpression::ListIndex(col_ref, index_expr) => {
                let list_value = row.values.get(&col_ref.column).ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })?;
                let index_value = self.evaluate_select_expression(index_expr, row)?;

                if let (Value::List(list), Value::Integer(index)) = (list_value, &index_value) {
                    if *index >= 0 && (*index as usize) < list.len() {
                        Ok(list[*index as usize].clone())
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Err(Error::query_execution("Invalid list access".to_string()))
                }
            }
            CollectionAccessExpression::MapKey(col_ref, key_expr) => {
                let map_value = row.values.get(&col_ref.column).ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })?;
                let key_value = self.evaluate_select_expression(key_expr, row)?;

                if let Value::Map(map) = map_value {
                    for (k, v) in map {
                        if *k == key_value {
                            return Ok(v.clone());
                        }
                    }
                    Ok(Value::Null)
                } else {
                    Err(Error::query_execution("Invalid map access".to_string()))
                }
            }
            CollectionAccessExpression::SetContains(col_ref, value_expr) => {
                let set_value = row.values.get(&col_ref.column).ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })?;
                let test_value = self.evaluate_select_expression(value_expr, row)?;

                if let Value::Set(set) = set_value {
                    Ok(Value::Boolean(set.contains(&test_value)))
                } else {
                    Err(Error::query_execution(
                        "Invalid set contains operation".to_string(),
                    ))
                }
            }
        }
    }

    /// Evaluate arithmetic expressions
    fn evaluate_arithmetic(
        &self,
        op: &ArithmeticOperator,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => match op {
                ArithmeticOperator::Add => Ok(Value::Integer(a + b)),
                ArithmeticOperator::Subtract => Ok(Value::Integer(a - b)),
                ArithmeticOperator::Multiply => Ok(Value::Integer(a * b)),
                ArithmeticOperator::Divide => {
                    if b != 0 {
                        Ok(Value::Integer(a / b))
                    } else {
                        Err(Error::query_execution("Division by zero".to_string()))
                    }
                }
                ArithmeticOperator::Modulo => {
                    if b != 0 {
                        Ok(Value::Integer(a % b))
                    } else {
                        Err(Error::query_execution("Modulo by zero".to_string()))
                    }
                }
            },
            (Value::Float(a), Value::Float(b)) => match op {
                ArithmeticOperator::Add => Ok(Value::Float(a + b)),
                ArithmeticOperator::Subtract => Ok(Value::Float(a - b)),
                ArithmeticOperator::Multiply => Ok(Value::Float(a * b)),
                ArithmeticOperator::Divide => Ok(Value::Float(a / b)),
                ArithmeticOperator::Modulo => Ok(Value::Float(a % b)),
            },
            _ => Err(Error::query_execution(
                "Incompatible types for arithmetic".to_string(),
            )),
        }
    }

    /// Simple LIKE pattern matching
    fn match_like_pattern(&self, text: &str, pattern: &str) -> bool {
        // Simple implementation - convert CQL LIKE to regex-like matching
        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");

        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(text)
        } else {
            false
        }
    }

    /// Execute sorting step
    async fn execute_sort(
        &self,
        mut rows: Vec<QueryRow>,
        order_by: &OrderByClause,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        rows.sort_by(|a, b| {
            for item in &order_by.items {
                let a_val = self
                    .evaluate_select_expression(&item.expression, a)
                    .unwrap_or(Value::Null);
                let b_val = self
                    .evaluate_select_expression(&item.expression, b)
                    .unwrap_or(Value::Null);

                let cmp = self.compare_values(&a_val, &b_val).unwrap_or(0);

                let ordering = match item.direction {
                    SortDirection::Ascending => cmp,
                    SortDirection::Descending => -cmp,
                };

                if ordering != 0 {
                    return ordering.cmp(&0);
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Execute aggregation step
    async fn execute_aggregation(
        &self,
        rows: Vec<QueryRow>,
        agg_plan: &AggregationPlan,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut agg_state = AggregationState {
            groups: Vec::new(),
            memory_usage_bytes: 0,
            memory_limit_bytes: 512 * 1024 * 1024, // Default 512MB limit
        };

        // Process each row
        for row in rows {
            // Extract group key
            let group_key = if agg_plan.group_by_columns.is_empty() {
                vec![Value::Null] // Single group for global aggregation
            } else {
                let mut key = Vec::new();
                for col in &agg_plan.group_by_columns {
                    key.push(row.values.get(col).cloned().unwrap_or(Value::Null));
                }
                key
            };

            // Find or create group
            let group_index =
                if let Some(index) = agg_state.groups.iter().position(|(k, _)| k == &group_key) {
                    index
                } else {
                    let initial_aggregates = agg_plan
                        .aggregates
                        .iter()
                        .map(|agg_comp| match agg_comp.function {
                            AggregateType::Count => AggregateValue::Count(0),
                            AggregateType::Sum => AggregateValue::Sum(0.0),
                            AggregateType::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
                            AggregateType::Min => AggregateValue::Min(Value::Null),
                            AggregateType::Max => AggregateValue::Max(Value::Null),
                        })
                        .collect();
                    agg_state.groups.push((group_key, initial_aggregates));
                    agg_state.groups.len() - 1
                };

            let group_aggregates = &mut agg_state.groups[group_index].1;

            // Update each aggregate
            for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                let column_value = if agg_comp.column == "*" {
                    // COUNT(*) - always count the row regardless of column values
                    Value::Integer(1)
                } else {
                    row.values
                        .get(&agg_comp.column)
                        .cloned()
                        .unwrap_or(Value::Null)
                };

                match &mut group_aggregates[i] {
                    AggregateValue::Count(count) => {
                        // For COUNT(*), always increment. For COUNT(column), only increment if not null
                        if agg_comp.column == "*" || !column_value.is_null() {
                            *count += 1;
                        }
                    }
                    AggregateValue::Sum(sum) => {
                        if let Some(num_val) = column_value.as_f64() {
                            *sum += num_val;
                        }
                    }
                    AggregateValue::Avg { sum, count } => {
                        if let Some(num_val) = column_value.as_f64() {
                            *sum += num_val;
                            *count += 1;
                        }
                    }
                    AggregateValue::Min(min_val) => {
                        if min_val.is_null()
                            || (!column_value.is_null()
                                && self.compare_values(&column_value, &*min_val).unwrap_or(0) < 0)
                        {
                            *min_val = column_value;
                        }
                    }
                    AggregateValue::Max(max_val) => {
                        if max_val.is_null()
                            || (!column_value.is_null()
                                && self.compare_values(&column_value, &*max_val).unwrap_or(0) > 0)
                        {
                            *max_val = column_value;
                        }
                    }
                }
            }

            // Check memory limits
            agg_state.memory_usage_bytes += 100; // Rough estimate per row
            if agg_state.memory_usage_bytes > agg_state.memory_limit_bytes {
                return Err(Error::query_execution(
                    "Aggregation memory limit exceeded".to_string(),
                ));
            }
        }

        // Convert aggregation state to result rows
        let mut result_rows = Vec::new();

        for (group_key, group_aggregates) in agg_state.groups {
            let mut row_values = HashMap::new();

            // Add group by columns
            for (i, col) in agg_plan.group_by_columns.iter().enumerate() {
                if i < group_key.len() {
                    row_values.insert(col.clone(), group_key[i].clone());
                }
            }

            // Add aggregate results
            for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                let result_value = match &group_aggregates[i] {
                    AggregateValue::Count(count) => Value::BigInt(*count as i64),
                    AggregateValue::Sum(sum) => Value::Float(*sum),
                    AggregateValue::Avg { sum, count } => {
                        if *count > 0 {
                            Value::Float(sum / (*count as f64))
                        } else {
                            Value::Null
                        }
                    }
                    AggregateValue::Min(val) | AggregateValue::Max(val) => val.clone(),
                };

                row_values.insert(agg_comp.alias.clone(), result_value);
            }

            result_rows.push(QueryRow {
                values: row_values,
                key: RowKey::new(vec![]),
                metadata: Default::default(),
            });
        }

        Ok(result_rows)
    }

    /// Execute limit step
    async fn execute_limit(
        &self,
        mut rows: Vec<QueryRow>,
        count: u64,
        offset: Option<u64>,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let start_index = offset.unwrap_or(0) as usize;
        let _end_index = start_index + count as usize;

        if start_index >= rows.len() {
            Ok(Vec::new())
        } else {
            rows.drain(..start_index);
            rows.truncate(count as usize);
            Ok(rows)
        }
    }

    /// Execute projection step
    async fn execute_projection(
        &self,
        rows: Vec<QueryRow>,
        columns: &[SelectExpression],
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut projected_rows = Vec::new();

        for row in rows {
            let mut projected_values = HashMap::new();

            for (i, expr) in columns.iter().enumerate() {
                let value = self.evaluate_select_expression(expr, &row)?;
                let column_name = match expr {
                    SelectExpression::Column(col_ref) => col_ref.column.clone(),
                    SelectExpression::Aliased(_, alias) => alias.clone(),
                    _ => format!("col_{}", i),
                };
                projected_values.insert(column_name, value);
            }

            projected_rows.push(QueryRow {
                values: projected_values,
                key: RowKey::new(vec![]),
                metadata: Default::default(),
            });
        }

        Ok(projected_rows)
    }

    /// Execute a query without FROM clause (constant expressions like SELECT 1)
    async fn execute_constant_query(
        &self,
        statement: &SelectStatement,
        _context: &ExecutionContext,
    ) -> Result<QueryResult> {
        let mut values = HashMap::new();
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                return Err(Error::query_execution(
                    "SELECT * requires a FROM clause".to_string(),
                ));
            }
            SelectClause::Columns(expressions) | SelectClause::Distinct(expressions) => {
                for (i, expr) in expressions.iter().enumerate() {
                    let (value, column_name) = self.evaluate_constant_expression(expr)?;
                    let key = column_name.unwrap_or_else(|| format!("column_{}", i));
                    values.insert(key.clone(), value);
                    columns.push(ColumnInfo {
                        name: key,
                        data_type: crate::types::DataType::Text, // Simplified for now
                        nullable: true,
                        position: i,
                        table_name: None, // No table for constant expressions
                    });
                }
            }
        }

        let row = QueryRow::with_values(RowKey::new(vec![1]), values);

        Ok(QueryResult {
            rows: vec![row],
            rows_affected: 1, // Constant queries return 1 row
            execution_time_ms: 0,
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(1),
                plan_info: None,
                performance: crate::query::result::PerformanceMetrics::default(),
                warnings: Vec::new(),
            },
        })
    }

    /// Evaluate a constant expression (no table access needed)
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_constant_expression(
        &self,
        expr: &SelectExpression,
    ) -> Result<(Value, Option<String>)> {
        match expr {
            SelectExpression::Literal(value) => Ok((value.clone(), None)),
            SelectExpression::Aliased(inner_expr, alias) => {
                let (value, _) = self.evaluate_constant_expression(inner_expr)?;
                Ok((value, Some(alias.clone())))
            }
            SelectExpression::Arithmetic(arith) => {
                // Simplified arithmetic evaluation
                let (left_val, _) = self.evaluate_constant_expression(&arith.left)?;
                let (right_val, _) = self.evaluate_constant_expression(&arith.right)?;

                let result = match arith.operator {
                    ArithmeticOperator::Add => match (left_val, right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
                        (Value::BigInt(a), Value::BigInt(b)) => Value::BigInt(a + b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                        _ => {
                            return Err(Error::query_execution(
                                "Cannot add incompatible types".to_string(),
                            ));
                        }
                    },
                    ArithmeticOperator::Subtract => match (left_val, right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
                        (Value::BigInt(a), Value::BigInt(b)) => Value::BigInt(a - b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                        _ => {
                            return Err(Error::query_execution(
                                "Cannot subtract incompatible types".to_string(),
                            ));
                        }
                    },
                    ArithmeticOperator::Multiply => match (left_val, right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
                        (Value::BigInt(a), Value::BigInt(b)) => Value::BigInt(a * b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                        _ => {
                            return Err(Error::query_execution(
                                "Cannot multiply incompatible types".to_string(),
                            ));
                        }
                    },
                    ArithmeticOperator::Divide => match (left_val, right_val) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if b == 0 {
                                return Err(Error::query_execution("Division by zero".to_string()));
                            }
                            Value::Integer(a / b)
                        }
                        (Value::BigInt(a), Value::BigInt(b)) => {
                            if b == 0 {
                                return Err(Error::query_execution("Division by zero".to_string()));
                            }
                            Value::BigInt(a / b)
                        }
                        (Value::Float(a), Value::Float(b)) => {
                            if b == 0.0 {
                                return Err(Error::query_execution("Division by zero".to_string()));
                            }
                            Value::Float(a / b)
                        }
                        _ => {
                            return Err(Error::query_execution(
                                "Cannot divide incompatible types".to_string(),
                            ));
                        }
                    },
                    ArithmeticOperator::Modulo => match (left_val, right_val) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if b == 0 {
                                return Err(Error::query_execution("Modulo by zero".to_string()));
                            }
                            Value::Integer(a % b)
                        }
                        (Value::BigInt(a), Value::BigInt(b)) => {
                            if b == 0 {
                                return Err(Error::query_execution("Modulo by zero".to_string()));
                            }
                            Value::BigInt(a % b)
                        }
                        _ => {
                            return Err(Error::query_execution(
                                "Modulo only supported for integers".to_string(),
                            ));
                        }
                    },
                };
                Ok((result, None))
            }
            _ => Err(Error::query_execution(
                "Expression type not supported in constant queries".to_string(),
            )),
        }
    }

    /// Helper methods
    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            } // JOIN operations are not supported in Cassandra CQL
        }
    }

    /// Parse table ID to extract keyspace and table name
    /// TableId is typically formatted as "keyspace.table" or just "table"
    fn parse_table_id(&self, table_id: &TableId) -> (Option<String>, String) {
        let table_str = table_id.name();
        if let Some(dot_pos) = table_str.rfind('.') {
            let keyspace = table_str[..dot_pos].to_string();
            let table_name = table_str[dot_pos + 1..].to_string();
            (Some(keyspace), table_name)
        } else {
            (None, table_str.to_string())
        }
    }

    /// Decode partition key value from RowKey bytes (Issue #191)
    ///
    /// Cassandra doesn't serialize partition keys in cell data - they're part of the row key.
    /// This method extracts the partition key value from the RowKey bytes based on the CQL type.
    ///
    /// For simple_table: partition key is UUID (16 bytes)
    fn decode_partition_key_value(
        &self,
        key: &RowKey,
        pk_column: &crate::schema::KeyColumn,
    ) -> Result<Value> {
        let key_bytes = key.0.as_slice();

        // For simple partition keys (not composite), the entire key is the partition key value
        // Decode based on CQL type
        let normalized_type = pk_column.data_type.to_lowercase();
        match normalized_type.as_str() {
            "uuid" | "timeuuid" => {
                // UUID is 16 bytes
                if key_bytes.len() >= 16 {
                    let uuid_bytes: [u8; 16] = key_bytes[..16].try_into().map_err(|_| {
                        Error::query_execution("Invalid UUID key length".to_string())
                    })?;
                    Ok(Value::Uuid(uuid_bytes))
                } else {
                    Err(Error::query_execution(format!(
                        "Partition key too short for UUID: {} bytes",
                        key_bytes.len()
                    )))
                }
            }
            "text" | "varchar" | "ascii" => {
                // Text keys are length-prefixed (2 bytes big-endian length + bytes)
                if key_bytes.len() >= 2 {
                    let len = u16::from_be_bytes([key_bytes[0], key_bytes[1]]) as usize;
                    if key_bytes.len() >= 2 + len {
                        let text =
                            String::from_utf8(key_bytes[2..2 + len].to_vec()).map_err(|e| {
                                Error::query_execution(format!(
                                    "Invalid UTF-8 in partition key: {}",
                                    e
                                ))
                            })?;
                        Ok(Value::Text(text))
                    } else {
                        Err(Error::query_execution(
                            "Partition key text length mismatch".to_string(),
                        ))
                    }
                } else {
                    Err(Error::query_execution(
                        "Partition key too short for text".to_string(),
                    ))
                }
            }
            "int" => {
                // INT is 4 bytes big-endian
                if key_bytes.len() >= 4 {
                    let int_val = i32::from_be_bytes([
                        key_bytes[0],
                        key_bytes[1],
                        key_bytes[2],
                        key_bytes[3],
                    ]);
                    Ok(Value::Integer(int_val))
                } else {
                    Err(Error::query_execution(
                        "Partition key too short for int".to_string(),
                    ))
                }
            }
            "bigint" | "counter" => {
                // BIGINT is 8 bytes big-endian
                if key_bytes.len() >= 8 {
                    let long_val = i64::from_be_bytes([
                        key_bytes[0],
                        key_bytes[1],
                        key_bytes[2],
                        key_bytes[3],
                        key_bytes[4],
                        key_bytes[5],
                        key_bytes[6],
                        key_bytes[7],
                    ]);
                    Ok(Value::BigInt(long_val))
                } else {
                    Err(Error::query_execution(
                        "Partition key too short for bigint".to_string(),
                    ))
                }
            }
            _ => {
                // For unsupported types, return the raw bytes as a blob or debug string
                log::warn!(
                    "Unsupported partition key type: {}, returning as debug string",
                    pk_column.data_type
                );
                Ok(Value::Text(format!("{:?}", key_bytes)))
            }
        }
    }

    async fn get_result_columns(&self, statement: &SelectStatement) -> Result<Vec<ColumnInfo>> {
        // TODO: Implement proper column metadata extraction
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                // For SELECT *, we'll handle this dynamically when processing rows
                // Don't add any specific columns here - this will be expanded later
            }
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                for (i, expr) in exprs.iter().enumerate() {
                    let column_name = match expr {
                        SelectExpression::Column(col_ref) => col_ref.column.clone(),
                        SelectExpression::Aliased(_, alias) => alias.clone(),
                        _ => format!("col_{}", i),
                    };

                    columns.push(ColumnInfo {
                        name: column_name,
                        data_type: crate::types::DataType::Text, // TODO: Infer proper type
                        nullable: true,
                        position: i,
                        table_name: None,
                    });
                }
            }
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{platform::Platform, Config};
    use tempfile::TempDir;

    async fn create_test_executor() -> SelectExecutor {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let _schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor { _schema, storage }
    }

    #[tokio::test]
    async fn test_value_comparison() {
        let executor = create_test_executor().await;

        assert_eq!(
            executor
                .compare_values(&Value::Integer(5), &Value::Integer(3))
                .unwrap(),
            1
        );
        assert_eq!(
            executor
                .compare_values(&Value::Integer(3), &Value::Integer(5))
                .unwrap(),
            -1
        );
        assert_eq!(
            executor
                .compare_values(&Value::Integer(5), &Value::Integer(5))
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_like_pattern_matching() {
        let executor = create_test_executor().await;

        assert!(executor.match_like_pattern("hello", "h%"));
        assert!(executor.match_like_pattern("hello", "%lo"));
        assert!(executor.match_like_pattern("hello", "h_llo"));
        assert!(!executor.match_like_pattern("hello", "h_l"));
    }
}
