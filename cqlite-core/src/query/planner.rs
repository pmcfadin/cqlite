//! Query planner for CQLite
//!
//! This module provides query planning and optimization capabilities for CQL queries.
//! It includes:
//!
//! - Query plan generation and optimization
//! - Index selection and utilization
//! - Cost-based optimization
//! - Execution plan representation

use super::{ComparisonOperator, Condition, ParsedQuery, QueryType, WhereClause};
use crate::{schema::SchemaManager, Config, Error, Result, TableId};
use std::sync::Arc;

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Plan type
    pub plan_type: PlanType,
    /// Target table
    pub table: Option<TableId>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
    /// Selected indexes
    pub selected_indexes: Vec<IndexSelection>,
    /// Execution steps
    pub steps: Vec<ExecutionStep>,
    /// Query hints
    pub hints: QueryHints,
}

/// Plan type enum
#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    /// Table scan
    TableScan,
    /// Index scan
    IndexScan,
    /// Point lookup
    PointLookup,
    /// Range scan
    RangeScan,
    /// Multi-table join
    Join,
    /// Aggregation
    Aggregation,
    /// Subquery
    Subquery,
}

/// Index selection information
#[derive(Debug, Clone)]
pub struct IndexSelection {
    /// Index name
    pub index_name: String,
    /// Columns covered by index
    pub columns: Vec<String>,
    /// Selectivity estimate
    pub selectivity: f64,
    /// Index type
    pub index_type: IndexType,
}

/// Index type
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// Primary key index
    Primary,
    /// Secondary index
    Secondary,
    /// Bloom filter
    BloomFilter,
    /// Composite index
    Composite,
}

/// Execution step
#[derive(Debug, Clone)]
pub struct ExecutionStep {
    /// Step type
    pub step_type: StepType,
    /// Columns involved
    pub columns: Vec<String>,
    /// Conditions to apply
    pub conditions: Vec<Condition>,
    /// Estimated cost
    pub cost: f64,
    /// Parallelization info
    pub parallelization: ParallelizationInfo,
}

/// Execution step type
#[derive(Debug, Clone, PartialEq)]
pub enum StepType {
    /// Scan table or index
    Scan,
    /// Filter rows
    Filter,
    /// Sort results
    Sort,
    /// Limit results
    Limit,
    /// Project columns
    Project,
    /// Join tables
    Join,
    /// Aggregate rows
    Aggregate,
}

/// Parallelization information
#[derive(Debug, Clone)]
pub struct ParallelizationInfo {
    /// Can be parallelized
    pub can_parallelize: bool,
    /// Suggested thread count
    pub suggested_threads: usize,
    /// Partition key for parallel execution
    pub partition_key: Option<String>,
}

/// Query hints and optimization settings
#[derive(Debug, Clone, Default)]
pub struct QueryHints {
    /// Force index usage
    pub force_index: Option<String>,
    /// Disable bloom filter
    pub disable_bloom_filter: bool,
    /// Preferred parallelization
    pub preferred_parallelization: Option<usize>,
    /// Query timeout
    pub timeout_ms: Option<u64>,
}

/// Query planner
#[derive(Debug)]
pub struct QueryPlanner {
    /// Schema manager reference
    schema: Arc<SchemaManager>,
    /// Configuration
    config: Config,
    /// Cost model
    cost_model: CostModel,
}

/// Cost model for query optimization
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Cost per row scan
    pub row_scan_cost: f64,
    /// Cost per index lookup
    pub index_lookup_cost: f64,
    /// Cost per sort operation
    pub sort_cost_per_row: f64,
    /// Cost per join operation
    pub join_cost_per_row: f64,
    /// Memory cost factor
    pub memory_cost_factor: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            row_scan_cost: 1.0,
            index_lookup_cost: 0.1,
            sort_cost_per_row: 0.01,
            join_cost_per_row: 0.05,
            memory_cost_factor: 0.001,
        }
    }
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new(schema: Arc<SchemaManager>, config: &Config) -> Self {
        Self {
            schema,
            config: config.clone(),
            cost_model: CostModel::default(),
        }
    }

    /// Plan a query
    pub async fn plan(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        match query.query_type {
            QueryType::Select => self.plan_select(query).await,
            QueryType::Insert => self.plan_insert(query).await,
            QueryType::Update => self.plan_update(query).await,
            QueryType::Delete => self.plan_delete(query).await,
            QueryType::CreateTable => self.plan_create_table(query).await,
            QueryType::DropTable => self.plan_drop_table(query).await,
            QueryType::CreateIndex => self.plan_create_index(query).await,
            QueryType::DropIndex => self.plan_drop_index(query).await,
            QueryType::Describe => self.plan_describe(query).await,
            QueryType::Use => self.plan_use(query).await,
        }
    }

    /// Plan SELECT query
    async fn plan_select(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let table = query
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in SELECT".to_string()))?;

        // Get table statistics
        let table_stats = self.get_table_statistics(table).await?;

        // Analyze WHERE clause for index selection
        let index_selection = self.select_indexes(table, &query.where_clause).await?;

        // Determine plan type based on conditions
        let plan_type = self.determine_plan_type(&index_selection, &query.where_clause);

        // Build execution steps
        let mut steps = Vec::new();

        // Step 1: Scan/Lookup
        steps.push(ExecutionStep {
            step_type: StepType::Scan,
            columns: query.columns.clone(),
            conditions: query
                .where_clause
                .as_ref()
                .map(|w| w.conditions.clone())
                .unwrap_or_default(),
            cost: self.calculate_scan_cost(&index_selection, &table_stats),
            parallelization: self.determine_parallelization(&index_selection, &table_stats),
        });

        // Step 2: Filter (if needed)
        if query.where_clause.is_some() && plan_type != PlanType::PointLookup {
            steps.push(ExecutionStep {
                step_type: StepType::Filter,
                columns: vec![],
                conditions: query.where_clause.as_ref().unwrap().conditions.clone(),
                cost: table_stats.row_count as f64 * self.cost_model.row_scan_cost * 0.1,
                parallelization: ParallelizationInfo {
                    can_parallelize: true,
                    suggested_threads: self.config.query.query_parallelism.unwrap_or(4),
                    partition_key: None,
                },
            });
        }

        // Step 3: Sort (if needed)
        if !query.order_by.is_empty() {
            steps.push(ExecutionStep {
                step_type: StepType::Sort,
                columns: query.order_by.iter().map(|o| o.column.clone()).collect(),
                conditions: vec![],
                cost: table_stats.row_count as f64 * self.cost_model.sort_cost_per_row,
                parallelization: ParallelizationInfo {
                    can_parallelize: true,
                    suggested_threads: self.config.query.query_parallelism.unwrap_or(4),
                    partition_key: None,
                },
            });
        }

        // Step 4: Limit (if needed)
        if query.limit.is_some() {
            steps.push(ExecutionStep {
                step_type: StepType::Limit,
                columns: vec![],
                conditions: vec![],
                cost: 0.0, // Limit is virtually free
                parallelization: ParallelizationInfo {
                    can_parallelize: false,
                    suggested_threads: 1,
                    partition_key: None,
                },
            });
        }

        // Step 5: Project (if needed)
        if !query.columns.is_empty() && query.columns != vec!["*"] {
            steps.push(ExecutionStep {
                step_type: StepType::Project,
                columns: query.columns.clone(),
                conditions: vec![],
                cost: table_stats.row_count as f64 * 0.001, // Very cheap
                parallelization: ParallelizationInfo {
                    can_parallelize: true,
                    suggested_threads: self.config.query.query_parallelism.unwrap_or(4),
                    partition_key: None,
                },
            });
        }

        let total_cost = steps.iter().map(|s| s.cost).sum();
        let estimated_rows = self.estimate_result_rows(&table_stats, &query.where_clause);

        Ok(QueryPlan {
            plan_type,
            table: Some(table.clone()),
            estimated_cost: total_cost,
            estimated_rows,
            selected_indexes: index_selection,
            steps,
            hints: QueryHints::default(),
        })
    }

    /// Plan INSERT query
    async fn plan_insert(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let table = query
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in INSERT".to_string()))?;

        let table_stats = self.get_table_statistics(table).await?;

        let steps = vec![ExecutionStep {
            step_type: StepType::Scan, // Insert operation
            columns: query.columns.clone(),
            conditions: vec![],
            cost: self.cost_model.row_scan_cost,
            parallelization: ParallelizationInfo {
                can_parallelize: false,
                suggested_threads: 1,
                partition_key: None,
            },
        }];

        Ok(QueryPlan {
            plan_type: PlanType::PointLookup,
            table: Some(table.clone()),
            estimated_cost: self.cost_model.row_scan_cost,
            estimated_rows: 1,
            selected_indexes: vec![],
            steps,
            hints: QueryHints::default(),
        })
    }

    /// Plan UPDATE query
    async fn plan_update(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let table = query
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in UPDATE".to_string()))?;

        let table_stats = self.get_table_statistics(table).await?;
        let index_selection = self.select_indexes(table, &query.where_clause).await?;

        let mut steps = vec![ExecutionStep {
            step_type: StepType::Scan,
            columns: vec![],
            conditions: query
                .where_clause
                .as_ref()
                .map(|w| w.conditions.clone())
                .unwrap_or_default(),
            cost: self.calculate_scan_cost(&index_selection, &table_stats),
            parallelization: self.determine_parallelization(&index_selection, &table_stats),
        }];

        // Add update step
        steps.push(ExecutionStep {
            step_type: StepType::Filter, // Update operation
            columns: query.set_clause.keys().cloned().collect(),
            conditions: vec![],
            cost: table_stats.row_count as f64 * self.cost_model.row_scan_cost * 0.5,
            parallelization: ParallelizationInfo {
                can_parallelize: true,
                suggested_threads: self.config.query.query_parallelism.unwrap_or(4),
                partition_key: None,
            },
        });

        let total_cost = steps.iter().map(|s| s.cost).sum();
        let estimated_rows = self.estimate_result_rows(&table_stats, &query.where_clause);

        Ok(QueryPlan {
            plan_type: PlanType::TableScan,
            table: Some(table.clone()),
            estimated_cost: total_cost,
            estimated_rows,
            selected_indexes: index_selection,
            steps,
            hints: QueryHints::default(),
        })
    }

    /// Plan DELETE query
    async fn plan_delete(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let table = query
            .table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in DELETE".to_string()))?;

        let table_stats = self.get_table_statistics(table).await?;
        let index_selection = self.select_indexes(table, &query.where_clause).await?;

        let steps = vec![ExecutionStep {
            step_type: StepType::Scan,
            columns: vec![],
            conditions: query
                .where_clause
                .as_ref()
                .map(|w| w.conditions.clone())
                .unwrap_or_default(),
            cost: self.calculate_scan_cost(&index_selection, &table_stats),
            parallelization: self.determine_parallelization(&index_selection, &table_stats),
        }];

        let total_cost = steps.iter().map(|s| s.cost).sum();
        let estimated_rows = self.estimate_result_rows(&table_stats, &query.where_clause);

        Ok(QueryPlan {
            plan_type: PlanType::TableScan,
            table: Some(table.clone()),
            estimated_cost: total_cost,
            estimated_rows,
            selected_indexes: index_selection,
            steps,
            hints: QueryHints::default(),
        })
    }

    /// Plan DDL operations
    async fn plan_create_table(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::TableScan,
            table: query.table.clone(),
            estimated_cost: 1.0,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    async fn plan_drop_table(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::TableScan,
            table: query.table.clone(),
            estimated_cost: 1.0,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    async fn plan_create_index(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::IndexScan,
            table: query.table.clone(),
            estimated_cost: 1.0,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    async fn plan_drop_index(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::IndexScan,
            table: query.table.clone(),
            estimated_cost: 1.0,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    async fn plan_describe(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::PointLookup,
            table: query.table.clone(),
            estimated_cost: 0.1,
            estimated_rows: 1,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    async fn plan_use(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        Ok(QueryPlan {
            plan_type: PlanType::PointLookup,
            table: query.table.clone(),
            estimated_cost: 0.1,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        })
    }

    /// Select optimal indexes for the query
    async fn select_indexes(
        &self,
        table: &TableId,
        where_clause: &Option<WhereClause>,
    ) -> Result<Vec<IndexSelection>> {
        let mut selections = Vec::new();

        // Always consider primary key
        selections.push(IndexSelection {
            index_name: "PRIMARY".to_string(),
            columns: vec!["id".to_string()], // Simplified
            selectivity: 1.0,
            index_type: IndexType::Primary,
        });

        // Check for applicable secondary indexes
        if let Some(where_clause) = where_clause {
            for condition in &where_clause.conditions {
                // Simulate index selection logic
                selections.push(IndexSelection {
                    index_name: format!("idx_{}", condition.column),
                    columns: vec![condition.column.clone()],
                    selectivity: self.estimate_selectivity(condition),
                    index_type: IndexType::Secondary,
                });
            }
        }

        // Consider bloom filter for equality checks
        if let Some(where_clause) = where_clause {
            for condition in &where_clause.conditions {
                if condition.operator == ComparisonOperator::Equal {
                    selections.push(IndexSelection {
                        index_name: format!("bloom_{}", condition.column),
                        columns: vec![condition.column.clone()],
                        selectivity: 0.1, // Bloom filters are highly selective
                        index_type: IndexType::BloomFilter,
                    });
                }
            }
        }

        Ok(selections)
    }

    /// Determine plan type based on index selection
    fn determine_plan_type(
        &self,
        index_selection: &[IndexSelection],
        where_clause: &Option<WhereClause>,
    ) -> PlanType {
        if let Some(where_clause) = where_clause {
            // Check for point lookup (primary key equality)
            for condition in &where_clause.conditions {
                if condition.operator == ComparisonOperator::Equal {
                    for index in index_selection {
                        if index.index_type == IndexType::Primary
                            && index.columns.contains(&condition.column)
                        {
                            return PlanType::PointLookup;
                        }
                    }
                }
            }

            // Check for range scan
            for condition in &where_clause.conditions {
                if matches!(
                    condition.operator,
                    ComparisonOperator::LessThan
                        | ComparisonOperator::LessThanOrEqual
                        | ComparisonOperator::GreaterThan
                        | ComparisonOperator::GreaterThanOrEqual
                ) {
                    return PlanType::RangeScan;
                }
            }

            // Check for index scan
            for index in index_selection {
                if index.index_type == IndexType::Secondary {
                    return PlanType::IndexScan;
                }
            }
        }

        PlanType::TableScan
    }

    /// Calculate scan cost based on index selection
    fn calculate_scan_cost(
        &self,
        index_selection: &[IndexSelection],
        table_stats: &TableStatistics,
    ) -> f64 {
        let mut min_cost = table_stats.row_count as f64 * self.cost_model.row_scan_cost;

        for index in index_selection {
            let index_cost = match index.index_type {
                IndexType::Primary => {
                    table_stats.row_count as f64 * self.cost_model.index_lookup_cost * 0.1
                }
                IndexType::Secondary => {
                    table_stats.row_count as f64
                        * self.cost_model.index_lookup_cost
                        * index.selectivity
                }
                IndexType::BloomFilter => {
                    table_stats.row_count as f64 * self.cost_model.index_lookup_cost * 0.01
                }
                IndexType::Composite => {
                    table_stats.row_count as f64
                        * self.cost_model.index_lookup_cost
                        * index.selectivity
                        * 0.5
                }
            };

            min_cost = min_cost.min(index_cost);
        }

        min_cost
    }

    /// Determine parallelization strategy
    fn determine_parallelization(
        &self,
        index_selection: &[IndexSelection],
        table_stats: &TableStatistics,
    ) -> ParallelizationInfo {
        // Large tables benefit from parallelization
        let can_parallelize = table_stats.row_count > 10000;

        let suggested_threads = if can_parallelize {
            self.config.query.query_parallelism.unwrap_or(4)
        } else {
            1
        };

        // Look for partition key in indexes
        let partition_key = index_selection
            .iter()
            .find(|idx| idx.index_type == IndexType::Primary)
            .and_then(|idx| idx.columns.first())
            .cloned();

        ParallelizationInfo {
            can_parallelize,
            suggested_threads,
            partition_key,
        }
    }

    /// Estimate selectivity of a condition
    fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        match condition.operator {
            ComparisonOperator::Equal => 0.1,
            ComparisonOperator::NotEqual => 0.9,
            ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual => 0.3,
            ComparisonOperator::In => 0.2,
            ComparisonOperator::NotIn => 0.8,
            ComparisonOperator::Like => 0.5,
            ComparisonOperator::NotLike => 0.5,
        }
    }

    /// Estimate result rows
    fn estimate_result_rows(
        &self,
        table_stats: &TableStatistics,
        where_clause: &Option<WhereClause>,
    ) -> u64 {
        let mut selectivity = 1.0;

        if let Some(where_clause) = where_clause {
            for condition in &where_clause.conditions {
                selectivity *= self.estimate_selectivity(condition);
            }
        }

        (table_stats.row_count as f64 * selectivity) as u64
    }

    /// Get table statistics
    async fn get_table_statistics(&self, table: &TableId) -> Result<TableStatistics> {
        // In a real implementation, this would query actual table statistics
        Ok(TableStatistics {
            row_count: 100_000, // Simulated
            avg_row_size: 256,
            table_size: 25_600_000,
            index_count: 3,
        })
    }
}

/// Table statistics for cost estimation
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Number of rows in table
    pub row_count: u64,
    /// Average row size in bytes
    pub avg_row_size: u32,
    /// Total table size in bytes
    pub table_size: u64,
    /// Number of indexes
    pub index_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_query_planner_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage, &config)
                .await
                .unwrap(),
        );

        let planner = QueryPlanner::new(schema, &config);
        assert_eq!(planner.cost_model.row_scan_cost, 1.0);
    }

    #[tokio::test]
    async fn test_plan_type_determination() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            crate::storage::StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(
            crate::schema::SchemaManager::new(storage, &config)
                .await
                .unwrap(),
        );

        let planner = QueryPlanner::new(schema, &config);

        // Test point lookup
        let index_selection = vec![IndexSelection {
            index_name: "PRIMARY".to_string(),
            columns: vec!["id".to_string()],
            selectivity: 1.0,
            index_type: IndexType::Primary,
        }];

        let where_clause = Some(WhereClause {
            conditions: vec![Condition {
                column: "id".to_string(),
                operator: ComparisonOperator::Equal,
                value: Value::Integer(1),
            }],
        });

        let plan_type = planner.determine_plan_type(&index_selection, &where_clause);
        assert_eq!(plan_type, PlanType::PointLookup);
    }

    #[test]
    fn test_selectivity_estimation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let planner = QueryPlanner::new(
            Arc::new(
                crate::schema::SchemaManager::new(
                    Arc::new(
                        crate::storage::StorageEngine::open(
                            temp_dir.path(),
                            &config,
                            Arc::new(crate::platform::Platform::new(&config).await.unwrap()),
                        )
                        .await
                        .unwrap(),
                    ),
                    &config,
                )
                .await
                .unwrap(),
            ),
            &config,
        );

        let condition = Condition {
            column: "name".to_string(),
            operator: ComparisonOperator::Equal,
            value: Value::Text("test".to_string()),
        };

        let selectivity = planner.estimate_selectivity(&condition);
        assert_eq!(selectivity, 0.1);
    }
}
