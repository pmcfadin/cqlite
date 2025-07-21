//! Query Optimizer for CQL SELECT Statements
//!
//! This module implements advanced query optimization techniques specifically
//! designed for SSTable-based storage. It provides:
//! - Predicate pushdown to SSTable level for maximum efficiency
//! - Index selection and utilization
//! - Query plan generation with cost estimation
//! - Parallel execution planning

use super::select_ast::*;
use crate::{schema::SchemaManager, storage::StorageEngine, Error, Result, TableId, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Query optimizer for SELECT statements
#[derive(Debug)]
pub struct SelectOptimizer {
    /// Schema manager for metadata
    schema: Arc<SchemaManager>,
    /// Storage engine for statistics
    storage: Arc<StorageEngine>,
}

/// Optimized query execution plan
#[derive(Debug, Clone)]
pub struct OptimizedQueryPlan {
    /// Original SELECT statement
    pub statement: SelectStatement,
    /// Execution steps in order
    pub execution_steps: Vec<ExecutionStep>,
    /// Estimated cost (lower is better)
    pub estimated_cost: f64,
    /// Estimated number of rows to be processed
    pub estimated_rows: u64,
    /// Whether the plan can use indexes
    pub uses_indexes: bool,
    /// SSTable-level predicates that can be pushed down
    pub sstable_predicates: Vec<SSTablePredicate>,
    /// Aggregation plan (if needed)
    pub aggregation_plan: Option<AggregationPlan>,
    /// Parallelization strategy
    pub parallelization: ParallelizationPlan,
}

/// Individual execution step
#[derive(Debug, Clone)]
pub enum ExecutionStep {
    /// Scan SSTable files with optional predicates
    SSTableScan {
        table: TableId,
        predicates: Vec<SSTablePredicate>,
        projection: Vec<String>,
        estimated_cost: f64,
    },
    /// Filter rows based on complex WHERE clauses
    Filter {
        expression: WhereExpression,
        estimated_selectivity: f64,
    },
    /// Sort results
    Sort {
        order_by: OrderByClause,
        estimated_cost: f64,
    },
    /// Apply aggregation
    Aggregate {
        plan: AggregationPlan,
        estimated_cost: f64,
    },
    /// Limit results
    Limit { count: u64, offset: Option<u64> },
    /// Project final columns
    Project { columns: Vec<SelectExpression> },
}

/// SSTable-level predicate that can be pushed down
#[derive(Debug, Clone)]
pub struct SSTablePredicate {
    /// Column to filter on
    pub column: String,
    /// Filter operation
    pub operation: SSTableFilterOp,
    /// Value(s) to compare against
    pub values: Vec<Value>,
    /// Estimated selectivity (0.0 to 1.0)
    pub selectivity: f64,
}

/// SSTable filter operations
#[derive(Debug, Clone)]
pub enum SSTableFilterOp {
    /// Exact equality
    Equal,
    /// Range filter (min, max)
    Range,
    /// Set membership
    In,
    /// Prefix match (for string columns)
    Prefix,
    /// Bloom filter test (for existence)
    BloomFilter,
}

/// Aggregation execution plan
#[derive(Debug, Clone)]
pub struct AggregationPlan {
    /// Group by columns
    pub group_by_columns: Vec<String>,
    /// Aggregate functions to compute
    pub aggregates: Vec<AggregateComputation>,
    /// Whether to use hash-based grouping
    pub use_hash_grouping: bool,
    /// Memory limit for grouping
    pub memory_limit_mb: u64,
}

/// Individual aggregate computation
#[derive(Debug, Clone)]
pub struct AggregateComputation {
    /// Function type
    pub function: AggregateType,
    /// Column to aggregate
    pub column: String,
    /// Output alias
    pub alias: String,
    /// Whether DISTINCT is required
    pub distinct: bool,
}

/// Parallelization plan
#[derive(Debug, Clone)]
pub struct ParallelizationPlan {
    /// Whether the query can be parallelized
    pub can_parallelize: bool,
    /// Suggested number of threads
    pub suggested_threads: usize,
    /// Parallelization strategy
    pub strategy: ParallelStrategy,
    /// Partition boundaries (for range-based parallelism)
    pub partitions: Vec<PartitionBounds>,
}

/// Parallelization strategies
#[derive(Debug, Clone)]
pub enum ParallelStrategy {
    /// No parallelization
    None,
    /// Parallel SSTable scanning
    SSTableLevel,
    /// Parallel row processing
    RowLevel,
    /// Parallel aggregation with merge
    AggregationWithMerge,
}

/// Partition boundaries for parallel execution
#[derive(Debug, Clone)]
pub struct PartitionBounds {
    /// Start key (inclusive)
    pub start_key: Option<Value>,
    /// End key (exclusive)
    pub end_key: Option<Value>,
    /// Expected number of rows in this partition
    pub estimated_rows: u64,
}

impl SelectOptimizer {
    /// Create a new query optimizer
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self { schema, storage }
    }

    /// Optimize a SELECT statement
    pub async fn optimize(&self, statement: SelectStatement) -> Result<OptimizedQueryPlan> {
        let mut plan = OptimizedQueryPlan {
            statement: statement.clone(),
            execution_steps: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: 0,
            uses_indexes: false,
            sstable_predicates: Vec::new(),
            aggregation_plan: None,
            parallelization: ParallelizationPlan {
                can_parallelize: false,
                suggested_threads: 1,
                strategy: ParallelStrategy::None,
                partitions: Vec::new(),
            },
        };

        // Step 1: Analyze table metadata
        let table_id = self.extract_table_id(&statement.from_clause)?;
        let table_stats = self.get_table_statistics(&table_id).await?;

        // Step 2: Analyze WHERE clause for predicate pushdown
        if let Some(ref where_clause) = statement.where_clause {
            plan.sstable_predicates = self
                .extract_sstable_predicates(where_clause, &table_id)
                .await?;
        }

        // Step 3: Estimate row count after filtering
        plan.estimated_rows = self.estimate_filtered_rows(&table_stats, &plan.sstable_predicates);

        // Step 4: Plan SSTable scan
        let projection = self.extract_projection_columns(&statement.select_clause);
        let scan_step = ExecutionStep::SSTableScan {
            table: table_id.clone(),
            predicates: plan.sstable_predicates.clone(),
            projection: projection.clone(),
            estimated_cost: self.estimate_scan_cost(&table_stats, &plan.sstable_predicates),
        };
        plan.execution_steps.push(scan_step);
        plan.estimated_cost += plan.execution_steps.last().unwrap().cost();

        // Step 5: Plan additional filtering (for predicates that can't be pushed down)
        if let Some(ref where_clause) = statement.where_clause {
            if let Some(remaining_filter) =
                self.extract_remaining_filters(where_clause, &plan.sstable_predicates)
            {
                let filter_step = ExecutionStep::Filter {
                    expression: remaining_filter,
                    estimated_selectivity: 0.1, // Conservative estimate
                };
                plan.execution_steps.push(filter_step);
                plan.estimated_rows = (plan.estimated_rows as f64 * 0.1) as u64;
                plan.estimated_cost += plan.estimated_rows as f64 * 0.001; // Cost per row filtering
            }
        }

        // Step 6: Plan aggregation
        if statement.requires_aggregation() {
            let agg_plan = self.plan_aggregation(&statement).await?;
            let agg_step = ExecutionStep::Aggregate {
                plan: agg_plan.clone(),
                estimated_cost: self.estimate_aggregation_cost(&agg_plan, plan.estimated_rows),
            };
            plan.aggregation_plan = Some(agg_plan);
            plan.execution_steps.push(agg_step);
            plan.estimated_cost += plan.execution_steps.last().unwrap().cost();

            // Aggregation typically reduces row count significantly
            plan.estimated_rows = if plan
                .aggregation_plan
                .as_ref()
                .unwrap()
                .group_by_columns
                .is_empty()
            {
                1 // Single aggregate result
            } else {
                (plan.estimated_rows as f64 * 0.01).max(1.0) as u64 // Assume 1% unique groups
            };
        }

        // Step 7: Plan sorting
        if let Some(ref order_by) = statement.order_by {
            let sort_step = ExecutionStep::Sort {
                order_by: order_by.clone(),
                estimated_cost: self.estimate_sort_cost(plan.estimated_rows),
            };
            plan.execution_steps.push(sort_step);
            plan.estimated_cost += plan.execution_steps.last().unwrap().cost();
        }

        // Step 8: Plan limit
        if let Some(ref limit) = statement.limit {
            let limit_step = ExecutionStep::Limit {
                count: limit.count,
                offset: statement.offset,
            };
            plan.execution_steps.push(limit_step);
            plan.estimated_rows = plan.estimated_rows.min(limit.count);
        }

        // Step 9: Plan final projection
        let final_columns = self.extract_final_projection(&statement.select_clause);
        let project_step = ExecutionStep::Project {
            columns: final_columns,
        };
        plan.execution_steps.push(project_step);

        // Step 10: Plan parallelization
        plan.parallelization = self.plan_parallelization(&plan, &table_stats).await?;

        Ok(plan)
    }

    /// Extract table ID from FROM clause
    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            }
            FromClause::Join(_) => Err(Error::query_execution(
                "JOINs not yet supported".to_string(),
            )),
        }
    }

    /// Get table statistics for cost estimation
    async fn get_table_statistics(&self, table_id: &TableId) -> Result<TableStatistics> {
        // TODO: Implement real statistics gathering from storage engine
        Ok(TableStatistics {
            row_count: 1_000_000, // Default estimate
            size_bytes: 100_000_000,
            average_row_size: 100,
            column_statistics: HashMap::new(),
        })
    }

    /// Extract predicates that can be pushed down to SSTable level
    async fn extract_sstable_predicates(
        &self,
        where_clause: &WhereExpression,
        _table_id: &TableId,
    ) -> Result<Vec<SSTablePredicate>> {
        let mut predicates = Vec::new();
        self.extract_predicates_recursive(where_clause, &mut predicates);
        Ok(predicates)
    }

    /// Recursively extract predicates from WHERE expression
    fn extract_predicates_recursive(
        &self,
        expr: &WhereExpression,
        predicates: &mut Vec<SSTablePredicate>,
    ) {
        match expr {
            WhereExpression::Comparison(comp) => {
                if let Some(predicate) = self.comparison_to_sstable_predicate(comp) {
                    predicates.push(predicate);
                }
            }
            WhereExpression::And(exprs) => {
                for expr in exprs {
                    self.extract_predicates_recursive(expr, predicates);
                }
            }
            WhereExpression::Or(_) => {
                // OR predicates are harder to optimize at SSTable level
            }
            WhereExpression::Not(_) => {
                // NOT predicates require full scan
            }
            WhereExpression::Parentheses(expr) => {
                self.extract_predicates_recursive(expr, predicates);
            }
        }
    }

    /// Convert comparison expression to SSTable predicate
    fn comparison_to_sstable_predicate(
        &self,
        comp: &ComparisonExpression,
    ) -> Option<SSTablePredicate> {
        if let SelectExpression::Column(col_ref) = &comp.left {
            let column = col_ref.column.clone();

            match (&comp.operator, &comp.right) {
                (ComparisonOperator::Equal, ComparisonRightSide::Value(value_expr)) => {
                    if let Some(value) = self.extract_literal_value(value_expr) {
                        return Some(SSTablePredicate {
                            column,
                            operation: SSTableFilterOp::Equal,
                            values: vec![value],
                            selectivity: 0.01, // Assume 1% selectivity for equality
                        });
                    }
                }
                (ComparisonOperator::In, ComparisonRightSide::ValueList(value_exprs)) => {
                    let values: Vec<Value> = value_exprs
                        .iter()
                        .filter_map(|expr| self.extract_literal_value(expr))
                        .collect();
                    if !values.is_empty() {
                        let selectivity = (values.len() as f64 * 0.01).min(1.0);
                        return Some(SSTablePredicate {
                            column,
                            operation: SSTableFilterOp::In,
                            values,
                            selectivity,
                        });
                    }
                }
                (ComparisonOperator::Between, ComparisonRightSide::Range(start_expr, end_expr)) => {
                    if let (Some(start), Some(end)) = (
                        self.extract_literal_value(start_expr),
                        self.extract_literal_value(end_expr),
                    ) {
                        return Some(SSTablePredicate {
                            column,
                            operation: SSTableFilterOp::Range,
                            values: vec![start, end],
                            selectivity: 0.1, // Assume 10% selectivity for ranges
                        });
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Extract literal value from expression
    fn extract_literal_value(&self, expr: &SelectExpression) -> Option<Value> {
        match expr {
            SelectExpression::Literal(value) => Some(value.clone()),
            _ => None,
        }
    }

    /// Estimate filtered row count
    fn estimate_filtered_rows(
        &self,
        stats: &TableStatistics,
        predicates: &[SSTablePredicate],
    ) -> u64 {
        let mut selectivity = 1.0;
        for predicate in predicates {
            selectivity *= predicate.selectivity;
        }
        (stats.row_count as f64 * selectivity) as u64
    }

    /// Extract projection columns
    fn extract_projection_columns(&self, select_clause: &SelectClause) -> Vec<String> {
        match select_clause {
            SelectClause::All => vec!["*".to_string()],
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => exprs
                .iter()
                .filter_map(|expr| self.extract_column_name(expr))
                .collect(),
        }
    }

    /// Extract column name from expression
    fn extract_column_name(&self, expr: &SelectExpression) -> Option<String> {
        match expr {
            SelectExpression::Column(col_ref) => Some(col_ref.column.clone()),
            SelectExpression::Aliased(_, alias) => Some(alias.clone()),
            _ => None,
        }
    }

    /// Estimate SSTable scan cost
    fn estimate_scan_cost(&self, stats: &TableStatistics, predicates: &[SSTablePredicate]) -> f64 {
        let base_cost = stats.size_bytes as f64 / 1_000_000.0; // Cost per MB

        // Reduce cost based on predicate selectivity
        let selectivity = predicates
            .iter()
            .map(|p| p.selectivity)
            .fold(1.0, |acc, s| acc * s);

        base_cost * selectivity
    }

    /// Extract filters that can't be pushed down
    fn extract_remaining_filters(
        &self,
        where_clause: &WhereExpression,
        sstable_predicates: &[SSTablePredicate],
    ) -> Option<WhereExpression> {
        // For now, assume all complex expressions need post-processing
        // TODO: Implement sophisticated filter separation
        if sstable_predicates.is_empty() {
            Some(where_clause.clone())
        } else {
            None
        }
    }

    /// Plan aggregation execution
    async fn plan_aggregation(&self, statement: &SelectStatement) -> Result<AggregationPlan> {
        let group_by_columns = if let Some(ref group_by) = statement.group_by {
            group_by
                .columns
                .iter()
                .map(|col| col.column.clone())
                .collect()
        } else {
            Vec::new()
        };

        let mut aggregates = Vec::new();
        if let SelectClause::Columns(exprs) = &statement.select_clause {
            for expr in exprs {
                if let SelectExpression::Aggregate(agg) = expr {
                    if let Some(col_name) = agg
                        .args
                        .first()
                        .and_then(|arg| self.extract_column_name(arg))
                    {
                        aggregates.push(AggregateComputation {
                            function: agg.function.clone(),
                            column: col_name.clone(),
                            alias: format!("{:?}_{}", agg.function, col_name),
                            distinct: agg.distinct,
                        });
                    }
                }
            }
        }

        Ok(AggregationPlan {
            group_by_columns,
            aggregates,
            use_hash_grouping: true,
            memory_limit_mb: 512, // Default 512MB limit
        })
    }

    /// Estimate aggregation cost
    fn estimate_aggregation_cost(&self, plan: &AggregationPlan, input_rows: u64) -> f64 {
        let base_cost = input_rows as f64 * 0.01; // Base cost per row

        // Add cost for grouping
        let grouping_cost = if plan.group_by_columns.is_empty() {
            0.0
        } else {
            input_rows as f64 * 0.005 // Cost for hash table operations
        };

        base_cost + grouping_cost
    }

    /// Estimate sort cost
    fn estimate_sort_cost(&self, rows: u64) -> f64 {
        if rows == 0 {
            0.0
        } else {
            // O(n log n) cost for sorting
            rows as f64 * (rows as f64).log2() * 0.001
        }
    }

    /// Extract final projection columns
    fn extract_final_projection(&self, select_clause: &SelectClause) -> Vec<SelectExpression> {
        match select_clause {
            SelectClause::All => vec![SelectExpression::Column(ColumnRef::new("*"))],
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => exprs.clone(),
        }
    }

    /// Plan parallelization strategy
    async fn plan_parallelization(
        &self,
        plan: &OptimizedQueryPlan,
        stats: &TableStatistics,
    ) -> Result<ParallelizationPlan> {
        let can_parallelize = stats.row_count > 100_000; // Only parallelize large datasets

        if !can_parallelize {
            return Ok(ParallelizationPlan {
                can_parallelize: false,
                suggested_threads: 1,
                strategy: ParallelStrategy::None,
                partitions: Vec::new(),
            });
        }

        let suggested_threads = if plan.aggregation_plan.is_some() {
            4 // Conservative for aggregation
        } else {
            8 // More aggressive for simple scans
        };

        let strategy = if plan.aggregation_plan.is_some() {
            ParallelStrategy::AggregationWithMerge
        } else {
            ParallelStrategy::SSTableLevel
        };

        // Create partition boundaries
        let partitions = self.create_partitions(stats, suggested_threads).await?;

        Ok(ParallelizationPlan {
            can_parallelize: true,
            suggested_threads,
            strategy,
            partitions,
        })
    }

    /// Create partition boundaries for parallel execution
    async fn create_partitions(
        &self,
        stats: &TableStatistics,
        num_partitions: usize,
    ) -> Result<Vec<PartitionBounds>> {
        let rows_per_partition = stats.row_count / num_partitions as u64;

        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            partitions.push(PartitionBounds {
                start_key: None, // TODO: Implement real key ranges
                end_key: None,
                estimated_rows: rows_per_partition,
            });
        }

        Ok(partitions)
    }
}

/// Table statistics for cost estimation
#[derive(Debug, Clone)]
struct TableStatistics {
    row_count: u64,
    size_bytes: u64,
    average_row_size: u64,
    column_statistics: HashMap<String, ColumnStatistics>,
}

/// Column-level statistics
#[derive(Debug, Clone)]
struct ColumnStatistics {
    distinct_values: u64,
    null_count: u64,
    min_value: Option<Value>,
    max_value: Option<Value>,
}

impl ExecutionStep {
    /// Get the estimated cost of this execution step
    fn cost(&self) -> f64 {
        match self {
            ExecutionStep::SSTableScan { estimated_cost, .. } => *estimated_cost,
            ExecutionStep::Filter { .. } => 0.1, // Minimal cost for in-memory filtering
            ExecutionStep::Sort { estimated_cost, .. } => *estimated_cost,
            ExecutionStep::Aggregate { estimated_cost, .. } => *estimated_cost,
            ExecutionStep::Limit { .. } => 0.01, // Very cheap operation
            ExecutionStep::Project { .. } => 0.01, // Very cheap operation
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::{Config, platform::Platform, schema::SchemaManager, storage::StorageEngine};

    async fn create_test_optimizer() -> SelectOptimizer {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone()).await.unwrap());
        let schema = Arc::new(SchemaManager::new(storage.clone(), &config).await.unwrap());
        
        SelectOptimizer {
            schema,
            storage,
        }
    }

    #[tokio::test]
    async fn test_predicate_extraction() {
        let _optimizer = create_test_optimizer().await;
        // This would test the predicate extraction logic
        // Implementation depends on having mock schema/storage
    }

    #[tokio::test]
    async fn test_cost_estimation() {
        let stats = TableStatistics {
            row_count: 1_000_000,
            size_bytes: 100_000_000,
            average_row_size: 100,
            column_statistics: HashMap::new(),
        };

        let optimizer = create_test_optimizer().await;

        let cost = optimizer.estimate_scan_cost(&stats, &[]);
        assert!(cost > 0.0);
    }
}
