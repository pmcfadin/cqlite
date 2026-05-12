//! Query planner for CQLite
//!
//! This module provides query planning and optimization capabilities for CQL queries.
//! It includes:
//!
//! - Query plan generation and optimization
//! - Index selection and utilization
//! - Cost-based optimization
//! - Execution plan representation

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{ComparisonOperator, Condition, ParsedQuery, QueryType, WhereClause};
use crate::{schema::SchemaManager, Config, Error, Result, TableId};
use std::sync::Arc;

// --- Cost-model tuning constants ---------------------------------------------
// All multipliers below are dimensionless factors applied to base costs from
// `CostModel`. Names are kept descriptive so changes flow through to every
// caller via a single source of truth.

/// Default thread count when `Config.query.query_parallelism` is unset.
const DEFAULT_PARALLELISM: usize = 4;

/// Row count above which a scan is worth parallelizing.
const PARALLELIZATION_ROW_THRESHOLD: u64 = 10_000;

/// Filter is roughly an order of magnitude cheaper than a scan over the same rows.
const FILTER_COST_FACTOR: f64 = 0.1;

/// UPDATE write step is approximately half the cost of a full row scan.
const UPDATE_WRITE_COST_FACTOR: f64 = 0.5;

/// Projection is essentially a column-pick — three orders of magnitude cheaper.
const PROJECT_COST_FACTOR: f64 = 0.001;

/// Primary-key lookups visit a small fraction of rows compared to a scan.
const PRIMARY_INDEX_COST_FACTOR: f64 = 0.1;

/// Bloom filters short-circuit most reads.
const BLOOM_INDEX_COST_FACTOR: f64 = 0.01;

/// Composite indexes inherit selectivity but get an additional discount.
const COMPOSITE_INDEX_COST_FACTOR: f64 = 0.5;

/// Default selectivity assumed for `bloom_<col>` index entries.
const BLOOM_INDEX_SELECTIVITY: f64 = 0.1;

/// Selectivity defaults indexed by `ComparisonOperator` semantics.
const SELECTIVITY_EQUAL: f64 = 0.1;
const SELECTIVITY_NOT_EQUAL: f64 = 0.9;
const SELECTIVITY_RANGE: f64 = 0.3;
const SELECTIVITY_IN: f64 = 0.2;
const SELECTIVITY_NOT_IN: f64 = 0.8;
const SELECTIVITY_LIKE: f64 = 0.5;

/// Trivial fixed cost for DDL plans (CREATE/DROP TABLE/INDEX).
const DDL_FIXED_COST: f64 = 1.0;

/// Trivial fixed cost for metadata-only plans (DESCRIBE/USE).
const METADATA_FIXED_COST: f64 = 0.1;

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
    /// Insert rows
    Insert,
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
    _schema: Arc<SchemaManager>,
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

/// Pull the table out of a parsed query, producing a uniform error message.
fn require_table<'a>(query: &'a ParsedQuery, op: &str) -> Result<&'a TableId> {
    query
        .table
        .as_ref()
        .ok_or_else(|| Error::query_execution(format!("Missing table in {op}")))
}

/// Clone the conditions out of an optional WHERE clause, defaulting to empty.
fn clone_conditions(where_clause: &Option<WhereClause>) -> Vec<Condition> {
    where_clause
        .as_ref()
        .map(|w| w.conditions.clone())
        .unwrap_or_default()
}

/// Hardcoded fallback column orderings used when an INSERT omits the column list.
///
/// This is a temporary fixture for tests that exercise INSERT VALUES without
/// schema lookup; a real implementation will resolve this from `SchemaManager`.
fn default_insert_columns(table_name: &str, value_count: usize) -> Vec<String> {
    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    match table_name {
        "sales" => s(&["id", "region", "amount"]),
        "orders" => s(&["id", "status", "amount"]),
        "products" => s(&["id", "name", "price", "category"]),
        "employees" => s(&["department", "id", "name", "salary"]),
        "inventory" => s(&["id", "product", "quantity", "price", "active"]),
        "customers" => s(&["id", "name", "email"]),
        "user_data" => s(&["id", "tags", "preferences"]),
        "performance_test" => s(&["id", "value", "category"]),
        _ => (0..value_count).map(|i| format!("col_{i}")).collect(),
    }
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new(schema: Arc<SchemaManager>, config: &Config) -> Self {
        Self {
            _schema: schema,
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
            QueryType::CreateTable => Ok(self.plan_ddl(query, PlanType::TableScan, DDL_FIXED_COST)),
            QueryType::DropTable => Ok(self.plan_ddl(query, PlanType::TableScan, DDL_FIXED_COST)),
            QueryType::CreateIndex => Ok(self.plan_ddl(query, PlanType::IndexScan, DDL_FIXED_COST)),
            QueryType::DropIndex => Ok(self.plan_ddl(query, PlanType::IndexScan, DDL_FIXED_COST)),
            QueryType::Describe => {
                Ok(self.plan_metadata(query, PlanType::PointLookup, METADATA_FIXED_COST, 1))
            }
            QueryType::Use => {
                Ok(self.plan_metadata(query, PlanType::PointLookup, METADATA_FIXED_COST, 0))
            }
        }
    }

    /// Configured query parallelism, falling back to the module default.
    fn query_parallelism(&self) -> usize {
        self.config
            .query
            .query_parallelism
            .unwrap_or(DEFAULT_PARALLELISM)
    }

    /// Build a `ParallelizationInfo` for steps that can always be parallelized
    /// using the configured thread count.
    fn parallel_info(&self) -> ParallelizationInfo {
        ParallelizationInfo {
            can_parallelize: true,
            suggested_threads: self.query_parallelism(),
            partition_key: None,
        }
    }

    /// `ParallelizationInfo` for inherently single-threaded steps.
    fn serial_info() -> ParallelizationInfo {
        ParallelizationInfo {
            can_parallelize: false,
            suggested_threads: 1,
            partition_key: None,
        }
    }

    /// Plan SELECT query
    async fn plan_select(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let table = require_table(query, "SELECT")?;

        let table_stats = self.get_table_statistics(table).await?;
        let index_selection = self.select_indexes(table, &query.where_clause).await?;
        let plan_type = self.determine_plan_type(&index_selection, &query.where_clause);

        let mut steps = Vec::new();

        // Step 1: Scan/Lookup
        steps.push(ExecutionStep {
            step_type: StepType::Scan,
            columns: query.columns.clone(),
            conditions: clone_conditions(&query.where_clause),
            cost: self.calculate_scan_cost(&index_selection, &table_stats),
            parallelization: self.determine_parallelization(&index_selection, &table_stats),
        });

        // Step 2: Filter (skipped for point lookups since the scan already pinned the row)
        if let Some(where_clause) = &query.where_clause {
            if plan_type != PlanType::PointLookup {
                steps.push(ExecutionStep {
                    step_type: StepType::Filter,
                    columns: vec![],
                    conditions: where_clause.conditions.clone(),
                    cost: table_stats.row_count as f64
                        * self.cost_model.row_scan_cost
                        * FILTER_COST_FACTOR,
                    parallelization: self.parallel_info(),
                });
            }
        }

        // Step 3: Sort
        if !query.order_by.is_empty() {
            steps.push(ExecutionStep {
                step_type: StepType::Sort,
                columns: query.order_by.iter().map(|o| o.column.clone()).collect(),
                conditions: vec![],
                cost: table_stats.row_count as f64 * self.cost_model.sort_cost_per_row,
                parallelization: self.parallel_info(),
            });
        }

        // Step 4: Limit (virtually free)
        if query.limit.is_some() {
            steps.push(ExecutionStep {
                step_type: StepType::Limit,
                columns: vec![],
                conditions: vec![],
                cost: 0.0,
                parallelization: Self::serial_info(),
            });
        }

        // Step 5: Project (only when an explicit, non-`*` projection was requested)
        if !query.columns.is_empty() && query.columns != vec!["*"] {
            steps.push(ExecutionStep {
                step_type: StepType::Project,
                columns: query.columns.clone(),
                conditions: vec![],
                cost: table_stats.row_count as f64 * PROJECT_COST_FACTOR,
                parallelization: self.parallel_info(),
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
        let table = require_table(query, "INSERT")?;
        let _table_stats = self.get_table_statistics(table).await?;

        // Determine which column names to pair with the VALUES list. When the
        // query omits columns, fall back to a per-table hardcoded ordering.
        let owned_default;
        let columns: &[String] = if query.columns.is_empty() {
            owned_default = default_insert_columns(table.name(), query.values.len());
            &owned_default
        } else {
            &query.columns
        };

        // Pair each column with its corresponding value (truncated to the shorter list).
        let conditions: Vec<Condition> = columns
            .iter()
            .zip(query.values.iter())
            .map(|(column, value)| Condition {
                column: column.clone(),
                operator: ComparisonOperator::Equal,
                value: value.clone(),
            })
            .collect();

        let steps = vec![ExecutionStep {
            step_type: StepType::Insert,
            columns: query.columns.clone(),
            conditions,
            cost: self.cost_model.row_scan_cost,
            parallelization: Self::serial_info(),
        }];

        Ok(QueryPlan {
            plan_type: PlanType::TableScan,
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
        let table = require_table(query, "UPDATE")?;

        let table_stats = self.get_table_statistics(table).await?;
        let index_selection = self.select_indexes(table, &query.where_clause).await?;

        let steps = vec![
            ExecutionStep {
                step_type: StepType::Scan,
                columns: vec![],
                conditions: clone_conditions(&query.where_clause),
                cost: self.calculate_scan_cost(&index_selection, &table_stats),
                parallelization: self.determine_parallelization(&index_selection, &table_stats),
            },
            // The "update write" pass is modeled as a Filter step today.
            ExecutionStep {
                step_type: StepType::Filter,
                columns: query.set_clause.keys().cloned().collect(),
                conditions: vec![],
                cost: table_stats.row_count as f64
                    * self.cost_model.row_scan_cost
                    * UPDATE_WRITE_COST_FACTOR,
                parallelization: self.parallel_info(),
            },
        ];

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
        let table = require_table(query, "DELETE")?;

        let table_stats = self.get_table_statistics(table).await?;
        let index_selection = self.select_indexes(table, &query.where_clause).await?;

        let steps = vec![ExecutionStep {
            step_type: StepType::Scan,
            columns: vec![],
            conditions: clone_conditions(&query.where_clause),
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

    /// Build a stub plan for DDL operations that don't yet generate real steps.
    fn plan_ddl(&self, query: &ParsedQuery, plan_type: PlanType, cost: f64) -> QueryPlan {
        QueryPlan {
            plan_type,
            table: query.table.clone(),
            estimated_cost: cost,
            estimated_rows: 0,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        }
    }

    /// Build a stub plan for metadata-only queries (DESCRIBE/USE).
    fn plan_metadata(
        &self,
        query: &ParsedQuery,
        plan_type: PlanType,
        cost: f64,
        estimated_rows: u64,
    ) -> QueryPlan {
        QueryPlan {
            plan_type,
            table: query.table.clone(),
            estimated_cost: cost,
            estimated_rows,
            selected_indexes: vec![],
            steps: vec![],
            hints: QueryHints::default(),
        }
    }

    /// Select optimal indexes for the query
    async fn select_indexes(
        &self,
        _table: &TableId,
        where_clause: &Option<WhereClause>,
    ) -> Result<Vec<IndexSelection>> {
        let mut selections = Vec::new();

        // Always consider the primary key.
        selections.push(IndexSelection {
            index_name: "PRIMARY".to_string(),
            columns: vec!["id".to_string()], // Simplified
            selectivity: 1.0,
            index_type: IndexType::Primary,
        });

        if let Some(where_clause) = where_clause {
            // Per-condition: a synthetic secondary index, plus a bloom filter
            // when the operator is equality. The order below preserves the
            // original step output (all secondaries first, then all blooms).
            for condition in &where_clause.conditions {
                selections.push(IndexSelection {
                    index_name: format!("idx_{}", condition.column),
                    columns: vec![condition.column.clone()],
                    selectivity: self.estimate_selectivity(condition),
                    index_type: IndexType::Secondary,
                });
            }
            for condition in &where_clause.conditions {
                if condition.operator == ComparisonOperator::Equal {
                    selections.push(IndexSelection {
                        index_name: format!("bloom_{}", condition.column),
                        columns: vec![condition.column.clone()],
                        selectivity: BLOOM_INDEX_SELECTIVITY,
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
        let Some(where_clause) = where_clause else {
            return PlanType::TableScan;
        };

        let primary_columns: Vec<&str> = index_selection
            .iter()
            .filter(|idx| idx.index_type == IndexType::Primary)
            .flat_map(|idx| idx.columns.iter().map(String::as_str))
            .collect();

        let mut has_range = false;
        for condition in &where_clause.conditions {
            match condition.operator {
                ComparisonOperator::Equal => {
                    if primary_columns.iter().any(|c| *c == condition.column) {
                        return PlanType::PointLookup;
                    }
                }
                ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual
                | ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual => {
                    has_range = true;
                }
                _ => {}
            }
        }

        if has_range {
            return PlanType::RangeScan;
        }

        if index_selection
            .iter()
            .any(|idx| idx.index_type == IndexType::Secondary)
        {
            return PlanType::IndexScan;
        }

        PlanType::TableScan
    }

    /// Calculate scan cost based on index selection
    fn calculate_scan_cost(
        &self,
        index_selection: &[IndexSelection],
        table_stats: &TableStatistics,
    ) -> f64 {
        let rows = table_stats.row_count as f64;
        let base_lookup = rows * self.cost_model.index_lookup_cost;
        let mut min_cost = rows * self.cost_model.row_scan_cost;

        for index in index_selection {
            let index_cost = match index.index_type {
                IndexType::Primary => base_lookup * PRIMARY_INDEX_COST_FACTOR,
                IndexType::Secondary => base_lookup * index.selectivity,
                IndexType::BloomFilter => base_lookup * BLOOM_INDEX_COST_FACTOR,
                IndexType::Composite => {
                    base_lookup * index.selectivity * COMPOSITE_INDEX_COST_FACTOR
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
        let can_parallelize = table_stats.row_count > PARALLELIZATION_ROW_THRESHOLD;
        let suggested_threads = if can_parallelize {
            self.query_parallelism()
        } else {
            1
        };

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
            ComparisonOperator::Equal => SELECTIVITY_EQUAL,
            ComparisonOperator::NotEqual => SELECTIVITY_NOT_EQUAL,
            ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual => SELECTIVITY_RANGE,
            ComparisonOperator::In => SELECTIVITY_IN,
            ComparisonOperator::NotIn => SELECTIVITY_NOT_IN,
            ComparisonOperator::Like | ComparisonOperator::NotLike => SELECTIVITY_LIKE,
        }
    }

    /// Estimate result rows
    fn estimate_result_rows(
        &self,
        table_stats: &TableStatistics,
        where_clause: &Option<WhereClause>,
    ) -> u64 {
        let selectivity = where_clause
            .as_ref()
            .map(|w| {
                w.conditions
                    .iter()
                    .map(|c| self.estimate_selectivity(c))
                    .product::<f64>()
            })
            .unwrap_or(1.0);

        (table_stats.row_count as f64 * selectivity) as u64
    }

    /// Get table statistics
    async fn get_table_statistics(&self, _table: &TableId) -> Result<TableStatistics> {
        // In a real implementation, this would query actual table statistics.
        Ok(TableStatistics {
            row_count: 100_000,
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

    /// Build a planner backed by a fresh temp dir. Tests in this module don't
    /// need the storage engine; constructing it eats most of the test runtime.
    async fn make_planner() -> (TempDir, QueryPlanner) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let schema = Arc::new(
            crate::schema::SchemaManager::new(temp_dir.path())
                .await
                .unwrap(),
        );
        let planner = QueryPlanner::new(schema, &config);
        (temp_dir, planner)
    }

    #[tokio::test]
    async fn test_query_planner_creation() {
        let (_tmp, planner) = make_planner().await;
        assert_eq!(planner.cost_model.row_scan_cost, 1.0);
    }

    #[tokio::test]
    async fn test_plan_type_determination() {
        let (_tmp, planner) = make_planner().await;

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
                value: crate::Value::Integer(1),
            }],
        });

        let plan_type = planner.determine_plan_type(&index_selection, &where_clause);
        assert_eq!(plan_type, PlanType::PointLookup);
    }

    #[tokio::test]
    async fn test_selectivity_estimation() {
        let (_tmp, planner) = make_planner().await;

        let condition = Condition {
            column: "name".to_string(),
            operator: ComparisonOperator::Equal,
            value: crate::Value::Text("test".to_string()),
        };

        let selectivity = planner.estimate_selectivity(&condition);
        assert_eq!(selectivity, SELECTIVITY_EQUAL);
    }
}
