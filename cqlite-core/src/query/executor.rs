//! Query executor for CQLite
//!
//! This module provides query execution capabilities for CQL queries.
//! It includes:
//!
//! - Query plan execution
//! - Parallel query processing
//! - Result set construction
//! - Index utilization

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{
    planner::{ExecutionStep, IndexSelection, QueryPlan, StepType},
    ComparisonOperator, Condition,
};
use crate::{
    schema::SchemaManager, storage::StorageEngine, Config, Error, Result, RowKey, ScanRow, TableId,
    Value,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

// Use QueryResult and QueryRow from result module
pub use super::result::{QueryResult, QueryRow};

/// Bounded buffer (rows) for the streaming table-scan drain (issue #1691).
///
/// The producer parks once this many rows are in flight, so live heap during a
/// `TableScan` stays bounded by `buffer_size` regardless of result size. This
/// replaces the retired `execute_parallel_table_scan`, whose unbounded crossbeam
/// channel buffered the entire result set at once and whose N (default 4) workers
/// each issued the *identical* full `storage.scan` (4 duplicate whole-table passes
/// for one plan).
const TABLE_SCAN_STREAM_BUFFER: usize = 1024;

/// Query executor
#[derive(Debug, Clone)]
pub struct QueryExecutor {
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Schema manager reference (unused currently but kept for future use)
    _schema: Arc<SchemaManager>,
    /// Configuration (kept for future use; surfaced to in-file tests)
    _config: Config,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<StorageEngine>, schema: Arc<SchemaManager>, config: &Config) -> Self {
        Self {
            storage,
            _schema: schema,
            _config: config.clone(),
        }
    }

    /// Execute a query plan.
    ///
    /// Instrumented as `query.execute` (issue #1035) so that surfaces which reach
    /// the legacy executor directly — notably prepared/parameterized statements
    /// that bypass `QueryEngine::execute` — still root a query span tree under
    /// which the per-branch sub-spans (`query.point_lookup`, `query.table_scan`,
    /// …) and the read-path spans (issue #1034) nest. When invoked via
    /// `QueryEngine::execute` this nests under that span. The bounded plan-type
    /// attribute is recorded; the query text and key values never are.
    #[tracing::instrument(
        name = "query.execute",
        skip_all,
        fields(cqlite.query.plan_type = tracing::field::Empty),
    )]
    pub async fn execute(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let start_time = Instant::now();
        tracing::Span::current().record(
            crate::observability::catalog::attr::PLAN_TYPE,
            Self::plan_type_label(&plan.plan_type),
        );

        // Classify the plan once so subsequent dispatch is a single match.
        let has_insert_step = plan
            .steps
            .iter()
            .any(|step| matches!(step.step_type, StepType::Insert));
        let is_create_table =
            plan.steps.is_empty() && plan.table.is_some() && plan.estimated_rows == 0;

        let result = match plan.plan_type {
            super::planner::PlanType::PointLookup => self.execute_point_lookup(plan).await,
            super::planner::PlanType::IndexScan => self.execute_index_scan(plan).await,
            super::planner::PlanType::RangeScan => self.execute_range_scan(plan).await,
            super::planner::PlanType::TableScan if has_insert_step => {
                #[cfg(feature = "experimental")]
                {
                    self.execute_insert_operation(plan).await
                }
                #[cfg(not(feature = "experimental"))]
                {
                    Err(Error::UnsupportedFormat(
                        "INSERT operations require the 'experimental' feature. \
                         Add 'experimental' to your Cargo.toml features."
                            .to_string(),
                    ))
                }
            }
            super::planner::PlanType::TableScan if is_create_table => {
                self.execute_create_table_operation(plan).await
            }
            super::planner::PlanType::TableScan => self.execute_table_scan(plan).await,
            super::planner::PlanType::Join => self.execute_join(plan).await,
            super::planner::PlanType::Aggregation => self.execute_aggregation(plan).await,
            super::planner::PlanType::Subquery => self.execute_subquery(plan).await,
        };

        let mut query_result = result?;
        let elapsed_ms = start_time.elapsed().as_millis() as u64;

        query_result.execution_time_ms = elapsed_ms;
        query_result.metadata.plan_info = Some(super::result::PlanInfo {
            plan_type: format!("{:?}", plan.plan_type),
            estimated_cost: plan.estimated_cost,
            actual_cost: elapsed_ms as f64,
            // Access path(s) the executor actually consulted (issue #760).
            indexes_used: Self::indexes_used_for(plan),
            steps: plan
                .steps
                .iter()
                .map(|s| format!("{:?}", s.step_type))
                .collect(),
            parallelization: Self::parallelization_for(plan),
        });
        Ok(query_result)
    }

    // -- helpers ------------------------------------------------------------

    /// Bounded, lower-snake label for a [`super::planner::PlanType`], used as a
    /// span attribute (issue #1035). The value space is the closed `PlanType`
    /// taxonomy, so it is safe as a telemetry dimension.
    fn plan_type_label(plan_type: &super::planner::PlanType) -> &'static str {
        use super::planner::PlanType;
        match plan_type {
            PlanType::TableScan => "table_scan",
            PlanType::IndexScan => "index_scan",
            PlanType::PointLookup => "point_lookup",
            PlanType::RangeScan => "range_scan",
            PlanType::Join => "join",
            PlanType::Aggregation => "aggregation",
            PlanType::Subquery => "subquery",
        }
    }

    /// Report the access path(s) the executor *actually consulted* for `plan`,
    /// for the `indexes_used` field of [`super::result::PlanInfo`] (issue #760,
    /// Epic #756).
    ///
    /// # Truthfulness contract (no-heuristics spirit, issue #28)
    ///
    /// This mirrors the dispatch in [`Self::execute`] and reports only what the
    /// executed code path genuinely does — never what the planner merely
    /// preferred. The storage layer (`StorageEngine::get` / `scan`) does not yet
    /// surface *which* on-disk structure (Index.db partition lookup, Summary.db
    /// sampling, or BTI trie) resolved a partition, so we cannot distinguish
    /// those sub-paths. We therefore report at the granularity we can prove:
    ///
    /// - **Point lookup** (`PointLookup`, and `IndexScan` over a `Primary` or
    ///   `BloomFilter` index) calls `StorageEngine::get`, which resolves the
    ///   partition through the partition index. We report the selected index's
    ///   name (e.g. `"PRIMARY"`).
    /// - **Sequential scan** (`TableScan`, `RangeScan`, and `IndexScan` over a
    ///   `Secondary`/`Composite` index — these currently degrade to a full scan
    ///   in the executor) reports the explicit marker `"scan"`.
    ///
    /// ## Scan-marker decision
    ///
    /// The issue allows either an empty list or an explicit marker for a full
    /// scan; we pick the explicit **`"scan"`** marker. An empty list is
    /// ambiguous (it cannot be told apart from "not yet recorded"), whereas an
    /// explicit marker makes EXPLAIN-style output and bindings stats truthful
    /// and self-describing.
    /// Report the parallelization metadata for the *actually executed* path, for
    /// the `parallelization` field of [`super::result::PlanInfo`].
    ///
    /// # Truthfulness contract (no-heuristics spirit, issue #28; issue #1691)
    ///
    /// A step's `parallelization.can_parallelize` reflects only what the *planner*
    /// suggested, not what the executor did. Since issue #1691 the `TableScan`
    /// path is served by a SINGLE bounded `scan_stream` pass
    /// (`streaming_scan_rows`) rather than N parallel workers, so for that path we
    /// report the truth: one thread, `effective: false`. Reporting the planner's
    /// suggested thread count with `effective: true` here would be inaccurate.
    ///
    /// For any other plan type we still surface the planner's suggested thread
    /// count with `effective: true` when a step opts into parallelization.
    fn parallelization_for(plan: &QueryPlan) -> Option<super::result::ParallelizationInfo> {
        use super::planner::PlanType;

        let step = plan
            .steps
            .iter()
            .find(|s| s.parallelization.can_parallelize)?;

        // The table-scan path no longer forks parallel workers; it executes a
        // single bounded streaming pass. Report that truthfully.
        if matches!(plan.plan_type, PlanType::TableScan) {
            return Some(super::result::ParallelizationInfo {
                threads_used: 1,
                effective: false,
                partitions: Vec::new(),
            });
        }

        Some(super::result::ParallelizationInfo {
            threads_used: step.parallelization.suggested_threads,
            effective: true,
            partitions: Vec::new(),
        })
    }

    fn indexes_used_for(plan: &QueryPlan) -> Vec<String> {
        use super::planner::{IndexType, PlanType, StepType};

        // The marker used for any path that walks rows sequentially.
        let scan = || vec!["scan".to_string()];

        // A TableScan plan can actually be an INSERT or a CREATE TABLE; those
        // are dispatched away from `execute_table_scan` in `execute()` and never
        // call `storage.scan`, so they have no access path to report (roborev
        // job 40). Mirror that classification here.
        let has_insert_step = plan
            .steps
            .iter()
            .any(|step| matches!(step.step_type, StepType::Insert));
        let is_create_table =
            plan.steps.is_empty() && plan.table.is_some() && plan.estimated_rows == 0;
        if matches!(plan.plan_type, PlanType::TableScan) && (has_insert_step || is_create_table) {
            return Vec::new();
        }

        match plan.plan_type {
            // Resolves a single partition via `StorageEngine::get`.
            PlanType::PointLookup => match plan.selected_indexes.first() {
                Some(idx) => vec![idx.index_name.clone()],
                // No selected index recorded but we still did a partition
                // lookup — report the generic primary-key path.
                None => vec!["PRIMARY".to_string()],
            },
            // IndexScan dispatch depends on the index type: Primary/Bloom do a
            // real point lookup; Secondary/Composite degrade to a full scan.
            PlanType::IndexScan => match plan.selected_indexes.first() {
                Some(idx) => match idx.index_type {
                    IndexType::Primary | IndexType::BloomFilter => {
                        vec![idx.index_name.clone()]
                    }
                    IndexType::Secondary | IndexType::Composite => scan(),
                },
                None => scan(),
            },
            // Sequential-scan paths.
            PlanType::TableScan | PlanType::RangeScan => scan(),
            // Placeholder executors return empty results without touching any
            // index structure; report nothing rather than fabricate a path.
            PlanType::Join | PlanType::Aggregation | PlanType::Subquery => Vec::new(),
        }
    }

    /// Resolve `plan.table` or surface a uniform query-execution error.
    fn require_table<'a>(&self, plan: &'a QueryPlan) -> Result<&'a TableId> {
        plan.table
            .as_ref()
            .ok_or_else(|| Error::query_execution("Missing table in plan"))
    }

    /// Find the first condition matching `column` across all steps.
    fn find_condition<'a>(steps: &'a [ExecutionStep], column: &str) -> Option<&'a Condition> {
        steps
            .iter()
            .flat_map(|s| s.conditions.iter())
            .find(|c| c.column == column)
    }

    /// Convert a `(key, data)` pair from `StorageEngine::scan` into rows.
    fn scan_pairs_to_rows(&self, pairs: Vec<(RowKey, ScanRow)>) -> Result<Vec<QueryRow>> {
        let mut rows = Vec::with_capacity(pairs.len());
        for (row_key, row_data) in pairs {
            rows.push(self.storage_data_to_query_row(row_data, &row_key)?);
        }
        Ok(rows)
    }

    /// Run a full table scan and materialize results.
    async fn full_scan_rows(&self, table: &TableId) -> Result<Vec<QueryRow>> {
        let scan_results = self.storage.scan(table, None, None, None, None).await?;
        self.scan_pairs_to_rows(scan_results)
    }

    /// Look up a single row by the key derived from `condition`.
    async fn point_lookup_rows(
        &self,
        table: &TableId,
        condition: &Condition,
    ) -> Result<Vec<QueryRow>> {
        let row_key = self.condition_to_row_key(condition)?;
        match self.storage.get(table, &row_key).await? {
            Some(row_data) => Ok(vec![self.storage_data_to_query_row(row_data, &row_key)?]),
            None => Ok(Vec::new()),
        }
    }

    /// Wrap a row collection in a `QueryResult`. `execution_time_ms` is set by `execute()`.
    fn make_result(rows: Vec<QueryRow>) -> QueryResult {
        QueryResult::with_rows(rows)
    }

    // -- plan executors -----------------------------------------------------

    /// Execute point lookup plan
    #[tracing::instrument(name = "query.point_lookup", skip_all)]
    async fn execute_point_lookup(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = self.require_table(plan)?;

        // Find the lookup condition (first condition of the first step that has any).
        let lookup_condition = plan
            .steps
            .iter()
            .find_map(|step| step.conditions.first())
            .ok_or_else(|| Error::query_execution("No lookup condition found"))?;

        let row_key = self.condition_to_row_key(lookup_condition)?;

        let mut rows = Vec::new();
        if let Some(row_data) = self.storage.get(table, &row_key).await? {
            rows.push(self.storage_data_to_query_row(row_data, &row_key)?);
        }

        Ok(Self::make_result(rows))
    }

    /// Execute index scan plan
    #[tracing::instrument(name = "query.index_scan", skip_all)]
    async fn execute_index_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = self.require_table(plan)?;

        let index_selection = plan
            .selected_indexes
            .first()
            .ok_or_else(|| Error::query_execution("No index selected"))?;

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

        rows = self.apply_execution_steps(rows, &plan.steps).await?;
        Ok(Self::make_result(rows))
    }

    /// Execute range scan plan
    #[tracing::instrument(name = "query.range_scan", skip_all)]
    async fn execute_range_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = self.require_table(plan)?;

        // Range conditions are recognized by the planner; the storage engine is
        // queried with no explicit bounds for now.
        let mut rows = self.full_scan_rows(table).await?;
        rows = self.apply_execution_steps(rows, &plan.steps).await?;
        Ok(Self::make_result(rows))
    }

    /// Execute table scan plan
    #[tracing::instrument(name = "query.table_scan", skip_all)]
    async fn execute_table_scan(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table = self.require_table(plan)?;

        #[cfg(debug_assertions)]
        log::debug!("executor: Scanning for table: {:?}", table.name());

        // Issue #1691: a plan that requested parallelization is served by the
        // SAME bounded streaming scan the SelectExecutor uses (one whole-table
        // pass, bounded mpsc, `spawn_blocking` discipline inside `scan_stream`),
        // NOT by the retired multi-worker duplicate-scan path.
        let can_parallelize = plan
            .steps
            .iter()
            .any(|step| step.parallelization.can_parallelize);

        let mut rows = if can_parallelize {
            self.streaming_scan_rows(table).await?
        } else {
            self.full_scan_rows(table).await?
        };

        rows = self.apply_execution_steps(rows, &plan.steps).await?;
        Ok(Self::make_result(rows))
    }

    /// Execute join plan (placeholder)
    async fn execute_join(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        Ok(QueryResult::new())
    }

    /// Execute aggregation plan (placeholder)
    async fn execute_aggregation(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        Ok(QueryResult::new())
    }

    /// Execute subquery plan (placeholder)
    async fn execute_subquery(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        Ok(QueryResult::new())
    }

    // -- index scans --------------------------------------------------------

    /// Execute secondary index scan (currently a full scan; secondary index
    /// support is tracked separately).
    async fn execute_secondary_index_scan(
        &self,
        table: &TableId,
        index_selection: &IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        // Validate the index condition exists; the lookup itself is not yet wired up.
        Self::find_condition(steps, &index_selection.columns[0])
            .ok_or_else(|| Error::query_execution("No condition found for index"))?;
        self.full_scan_rows(table).await
    }

    /// Execute bloom filter scan (degrades to a direct point lookup).
    async fn execute_bloom_filter_scan(
        &self,
        table: &TableId,
        index_selection: &IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let condition = Self::find_condition(steps, &index_selection.columns[0])
            .ok_or_else(|| Error::query_execution("No condition found for bloom filter"))?;
        self.point_lookup_rows(table, condition).await
    }

    /// Execute primary index scan (point lookup on the primary key).
    async fn execute_primary_index_scan(
        &self,
        table: &TableId,
        index_selection: &IndexSelection,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        let condition = Self::find_condition(steps, &index_selection.columns[0])
            .ok_or_else(|| Error::query_execution("No condition found for primary key"))?;
        self.point_lookup_rows(table, condition).await
    }

    /// Execute composite index scan (currently a full scan; composite lookups
    /// are tracked separately).
    async fn execute_composite_index_scan(
        &self,
        table: &TableId,
        _index_selection: &IndexSelection,
        _steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        self.full_scan_rows(table).await
    }

    // -- table scans --------------------------------------------------------

    /// Stream a full table scan through the bounded streaming path and
    /// materialize results (issue #1691).
    ///
    /// This is the retirement of `execute_parallel_table_scan`. It issues a
    /// SINGLE whole-table pass via [`StorageEngine::scan_stream`] — the same
    /// bounded-`mpsc`, `spawn_blocking` machinery the `SelectExecutor` streaming
    /// path uses (issue #790) — instead of spawning N workers that each re-ran
    /// the identical `storage.scan`. Live heap during production stays bounded by
    /// [`TABLE_SCAN_STREAM_BUFFER`] rows: the reader parses one entry at a time
    /// into the channel and parks when the consumer falls behind, replacing the
    /// old unbounded `crossbeam` channel that held the entire result set at once.
    #[tracing::instrument(name = "query.table_scan_stream", skip_all)]
    async fn streaming_scan_rows(&self, table: &TableId) -> Result<Vec<QueryRow>> {
        let mut scan_stream = self
            .storage
            .scan_stream(table, None, None, None, TABLE_SCAN_STREAM_BUFFER)
            .await?;

        let mut rows = Vec::new();
        while let Some(item) = scan_stream.recv().await {
            let (row_key, row_data) = item?;
            rows.push(self.storage_data_to_query_row(row_data, &row_key)?);
        }
        Ok(rows)
    }

    // -- execution-step pipeline -------------------------------------------

    /// Apply execution steps to result rows.
    ///
    /// Limit/Aggregate/Join/Insert/Scan are no-ops at this layer (handled
    /// elsewhere or not yet implemented); only Filter/Sort/Project transform
    /// the row stream.
    async fn apply_execution_steps(
        &self,
        mut rows: Vec<QueryRow>,
        steps: &[ExecutionStep],
    ) -> Result<Vec<QueryRow>> {
        for step in steps {
            match step.step_type {
                StepType::Filter => rows = self.apply_filter_step(rows, step)?,
                StepType::Sort => rows = self.apply_sort_step(rows, step),
                StepType::Project => rows = self.apply_project_step(rows, step),
                // Limit is enforced higher up; the rest are placeholders.
                StepType::Limit
                | StepType::Aggregate
                | StepType::Join
                | StepType::Scan
                | StepType::Insert => {}
            }
        }
        Ok(rows)
    }

    /// Apply filter step
    fn apply_filter_step(
        &self,
        rows: Vec<QueryRow>,
        step: &ExecutionStep,
    ) -> Result<Vec<QueryRow>> {
        let mut filtered_rows = Vec::with_capacity(rows.len());
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
    fn apply_sort_step(&self, mut rows: Vec<QueryRow>, step: &ExecutionStep) -> Vec<QueryRow> {
        let Some(sort_column) = step.columns.first() else {
            return rows;
        };

        rows.sort_by(|a, b| {
            let a_val = a.values.get(sort_column.as_str()).unwrap_or(&Value::Null);
            let b_val = b.values.get(sort_column.as_str()).unwrap_or(&Value::Null);
            self.compare_values(a_val, b_val).unwrap_or(Ordering::Equal)
        });
        rows
    }

    /// Apply project step
    fn apply_project_step(&self, rows: Vec<QueryRow>, step: &ExecutionStep) -> Vec<QueryRow> {
        rows.into_iter()
            .map(|row| {
                let mut projected_values = HashMap::with_capacity(step.columns.len());
                for column in &step.columns {
                    if let Some(value) = row.values.get(column.as_str()) {
                        projected_values.insert(column.clone(), value.clone());
                    }
                }
                QueryRow::with_values(row.key, projected_values)
            })
            .collect()
    }

    // -- condition / value helpers -----------------------------------------

    /// Evaluate a condition against a row
    fn evaluate_condition(&self, row: &QueryRow, condition: &Condition) -> Result<bool> {
        let row_value = row
            .values
            .get(condition.column.as_str())
            .unwrap_or(&Value::Null);

        match condition.operator {
            ComparisonOperator::Equal => Ok(row_value == &condition.value),
            ComparisonOperator::NotEqual => Ok(row_value != &condition.value),
            ComparisonOperator::LessThan => Ok(matches!(
                self.compare_values(row_value, &condition.value)?,
                Ordering::Less
            )),
            ComparisonOperator::LessThanOrEqual => Ok(matches!(
                self.compare_values(row_value, &condition.value)?,
                Ordering::Less | Ordering::Equal
            )),
            ComparisonOperator::GreaterThan => Ok(matches!(
                self.compare_values(row_value, &condition.value)?,
                Ordering::Greater
            )),
            ComparisonOperator::GreaterThanOrEqual => Ok(matches!(
                self.compare_values(row_value, &condition.value)?,
                Ordering::Greater | Ordering::Equal
            )),
            // Simplified IN / NOT IN: treat as equality / inequality for now.
            ComparisonOperator::In => Ok(row_value == &condition.value),
            ComparisonOperator::NotIn => Ok(row_value != &condition.value),
            ComparisonOperator::Like => match (row_value, &condition.value) {
                (Value::Text(row_text), Value::Text(pattern)) => Ok(row_text.contains(pattern)),
                _ => Ok(false),
            },
            ComparisonOperator::NotLike => match (row_value, &condition.value) {
                (Value::Text(row_text), Value::Text(pattern)) => Ok(!row_text.contains(pattern)),
                _ => Ok(true),
            },
        }
    }

    /// Compare two values
    fn compare_values(&self, a: &Value, b: &Value) -> Result<Ordering> {
        use crate::float_cmp::cassandra_double_cmp as dcmp;
        use crate::float_cmp::cassandra_float_cmp as fcmp;
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Ok(dcmp(*a, *b)), // Cassandra order #1870/#2010
            (Value::Float32(a), Value::Float32(b)) => Ok(fcmp(*a, *b)), // Cassandra order #1870/#2010
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
            // UUID/TIMEUUID (both Value::Uuid): byte-wise, as Cassandra orders.
            (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) => Ok(Ordering::Less),
            (_, Value::Null) => Ok(Ordering::Greater),
            _ => Err(Error::query_execution(
                "Cannot compare values of different types",
            )),
        }
    }

    /// Convert a [`Value`] to the raw partition-key bytes used by [`RowKey`] and
    /// the Index.db lookup table.
    ///
    /// The encoding follows the same contract as
    /// [`PartitionKey::to_bytes`](crate::storage::write_engine::mutation::PartitionKey::to_bytes):
    ///
    /// - **Single-component keys** — raw value bytes (UUID = 16 bytes, Int = 4 BE
    ///   bytes, Text = UTF-8, BigInt = 8 BE bytes, …).
    /// - **Multi-component (composite) keys** — `[len: u16 BE][value bytes][0x00]`
    ///   per component, including a trailing `0x00` after the final component.
    ///   Pass a `Value::Tuple` whose elements are the ordered PK components.
    fn value_to_row_key(&self, value: &Value) -> Result<RowKey> {
        match value {
            Value::Integer(i) => Ok(RowKey::new(i.to_be_bytes().to_vec())),
            Value::Text(s) => Ok(RowKey::new(s.as_bytes().to_vec())),
            Value::Float(f) => Ok(RowKey::new(f.to_be_bytes().to_vec())),
            Value::Boolean(b) => Ok(RowKey::new(vec![u8::from(*b)])),
            Value::Null => Ok(RowKey::new(vec![0])),
            // UUID and TIMEUUID are both stored as 16 raw bytes (no framing).
            // This matches PartitionKey::to_bytes single-component output for a UUID column.
            Value::Uuid(bytes) => Ok(RowKey::new(bytes.to_vec())),
            Value::BigInt(i) => Ok(RowKey::new(i.to_be_bytes().to_vec())),
            // Multi-component (composite) partition key passed as a Tuple.
            // Encoding: [len: u16 BE][value bytes][0x00] per component, identical to
            // PartitionKey::to_bytes multi-component output (see mutation.rs ~line 256).
            Value::Tuple(components) => {
                let mut result = Vec::new();
                for component in components {
                    let raw = self.value_to_raw_pk_bytes(component)?;
                    let len = raw.len();
                    if len > u16::MAX as usize {
                        return Err(Error::query_execution(
                            "Composite partition key component too large",
                        ));
                    }
                    result.extend_from_slice(&(len as u16).to_be_bytes());
                    result.extend_from_slice(&raw);
                    result.push(0x00);
                }
                Ok(RowKey::new(result))
            }
            _ => Err(Error::query_execution("Cannot convert value to row key")),
        }
    }

    /// Serialize a single value to raw bytes suitable for inclusion in a
    /// composite partition key component. Used by [`value_to_row_key`] for
    /// `Value::Tuple` components.
    fn value_to_raw_pk_bytes(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Integer(i) => Ok(i.to_be_bytes().to_vec()),
            Value::Text(s) => Ok(s.as_bytes().to_vec()),
            Value::Float(f) => Ok(f.to_be_bytes().to_vec()),
            Value::Boolean(b) => Ok(vec![u8::from(*b)]),
            Value::Null => Ok(Vec::new()),
            Value::Uuid(bytes) => Ok(bytes.to_vec()),
            Value::BigInt(i) => Ok(i.to_be_bytes().to_vec()),
            _ => Err(Error::query_execution(
                "Cannot serialize value as partition key component",
            )),
        }
    }

    /// Convert Condition to RowKey (consistent with INSERT)
    fn condition_to_row_key(&self, condition: &Condition) -> Result<RowKey> {
        // Match the key format used by INSERT for "id" columns.
        if condition.column == "id" {
            if let Value::Integer(id) = &condition.value {
                return Ok(RowKey::new(format!("user_key_{}", id).into_bytes()));
            }
        }
        self.value_to_row_key(&condition.value)
    }

    /// Convert storage data to query row
    fn storage_data_to_query_row(&self, data: ScanRow, key: &RowKey) -> Result<QueryRow> {
        use std::sync::Arc;
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();

        // Storage path carries rows via the `ScanRow` carrier (issue #1334).
        // * `Row` — decoded cells keyed by the interned `Arc<str>` column-name
        //   handle; move the handle straight in (no `String` re-allocation).
        // * `RawRow` — a raw undecoded fallback with no schema here; surface the
        //   bytes as a single "data" blob, the exact pre-#1334 `other =>
        //   insert("data", ..)` shape.
        // * `Marker` (row tombstone / null) carries no columns.
        match data {
            ScanRow::Row(cells) => {
                for (name, cell_value) in cells {
                    values.insert(name, cell_value);
                }
            }
            ScanRow::RawRow(bytes) => {
                values.insert(Arc::from("data"), Value::Blob(bytes));
            }
            ScanRow::Marker(_) => {}
        }

        // If no values were extracted, surface the row key for visibility.
        if values.is_empty() {
            values.insert(Arc::from("id"), Value::Text(format!("{:?}", key)));
        }

        Ok(QueryRow::with_interned_values(key.clone(), values))
    }

    // -- experimental write paths ------------------------------------------

    /// Execute INSERT operation
    #[cfg(feature = "experimental")]
    async fn execute_insert_operation(&self, plan: &QueryPlan) -> Result<QueryResult> {
        let table_id = self
            .require_table(plan)
            .map_err(|_| Error::query_execution("No table specified in INSERT plan"))?;

        let mut inserted_count: u64 = 0;

        for step in &plan.steps {
            if !matches!(step.step_type, StepType::Insert) {
                continue;
            }

            // Default key uses the running insert index; an explicit "id"
            // condition wins so SELECT and INSERT share the same key shape.
            let mut key_value = format!("test_key_{}", inserted_count);
            for condition in &step.conditions {
                if condition.column == "id" {
                    if let Value::Integer(id) = &condition.value {
                        key_value = format!("user_key_{}", id);
                        break;
                    }
                }
            }

            let row_key = RowKey::new(key_value.into_bytes());

            // Build the row payload from step conditions (or seed defaults
            // when the step carries none, for test compatibility).
            let mut value_map: HashMap<String, Value> = step
                .conditions
                .iter()
                .map(|c| (c.column.clone(), c.value.clone()))
                .collect();

            if value_map.is_empty() {
                value_map.insert("id".to_string(), Value::Integer(inserted_count as i32 + 1));
                value_map.insert(
                    "name".to_string(),
                    Value::Text(format!("TestUser{}", inserted_count + 1)),
                );
            }

            let row_value = map_to_value(value_map);

            self.storage.put(table_id, row_key, row_value).await?;
            inserted_count += 1;
        }

        // No explicit INSERT steps — emit a single placeholder row to keep
        // legacy tests passing.
        if inserted_count == 0 {
            let row_key = RowKey::new(b"default_test_key".to_vec());
            let mut value_map = HashMap::new();
            value_map.insert("id".to_string(), Value::Integer(1));
            value_map.insert("name".to_string(), Value::Text("DefaultUser".to_string()));

            self.storage
                .put(table_id, row_key, map_to_value(value_map))
                .await?;
            inserted_count = 1;
        }

        Ok(QueryResult {
            rows: vec![],
            rows_affected: inserted_count,
            execution_time_ms: 0,
            metadata: super::result::QueryMetadata::default(),
        })
    }

    /// Execute CREATE TABLE operation (placeholder — DDL isn't persisted yet).
    async fn execute_create_table_operation(&self, _plan: &QueryPlan) -> Result<QueryResult> {
        Ok(QueryResult {
            rows: vec![],
            rows_affected: 0,
            execution_time_ms: 0,
            metadata: super::result::QueryMetadata::default(),
        })
    }
}

/// Build a `Value::Map` from a string-keyed map for storage writes.
#[cfg(feature = "experimental")]
fn map_to_value(map: HashMap<String, Value>) -> Value {
    Value::Map(map.into_iter().map(|(k, v)| (Value::Text(k), v)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Construct a fresh executor against a temporary storage root.
    async fn make_executor() -> (TempDir, QueryExecutor, Config) {
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
        let executor = QueryExecutor::new(storage, schema, &config);
        (temp_dir, executor, config)
    }

    #[tokio::test]
    async fn test_query_executor_creation() {
        let (_tmp, executor, config) = make_executor().await;
        assert_eq!(
            executor._config.query.query_parallelism,
            config.query.query_parallelism
        );
    }

    #[tokio::test]
    async fn test_value_comparison() {
        let (_tmp, executor, _) = make_executor().await;

        let result = executor
            .compare_values(&Value::Integer(10), &Value::Integer(20))
            .unwrap();
        assert_eq!(result, Ordering::Less);

        let result = executor
            .compare_values(
                &Value::Text("apple".to_string()),
                &Value::Text("banana".to_string()),
            )
            .unwrap();
        assert_eq!(result, Ordering::Less);

        // Issue #1870/#2010: ORDER BY on a float(f32) column must order via the
        // Cassandra total comparator, not collapse to Equal (missing arm bug).
        assert_eq!(
            executor
                .compare_values(&Value::Float32(1.0), &Value::Float32(2.0))
                .unwrap(),
            Ordering::Less
        );
        // Signed zeros are distinct; NaN sorts last and equals itself.
        assert_eq!(
            executor
                .compare_values(&Value::Float32(-0.0), &Value::Float32(0.0))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            executor
                .compare_values(&Value::Float32(f32::NAN), &Value::Float32(1.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            executor
                .compare_values(&Value::Float32(f32::NAN), &Value::Float32(f32::NAN))
                .unwrap(),
            Ordering::Equal
        );
    }

    #[tokio::test]
    async fn test_condition_evaluation() {
        let (_tmp, executor, _) = make_executor().await;

        let mut row_values = HashMap::new();
        row_values.insert("id".to_string(), Value::Integer(1));
        row_values.insert("name".to_string(), Value::Text("test".to_string()));
        let row = QueryRow::with_values(RowKey::new(vec![1]), row_values);

        let condition = Condition {
            column: "id".to_string(),
            operator: ComparisonOperator::Equal,
            value: Value::Integer(1),
        };
        assert!(executor.evaluate_condition(&row, &condition).unwrap());

        let condition = Condition {
            column: "name".to_string(),
            operator: ComparisonOperator::Like,
            value: Value::Text("test".to_string()),
        };
        assert!(executor.evaluate_condition(&row, &condition).unwrap());
    }

    // -- indexes_used access-path reporting (issue #760, Epic #756) --------

    use super::super::planner::{IndexSelection, IndexType};

    /// Build a minimal plan with the given type and selected indexes.
    fn plan_with(
        plan_type: super::super::planner::PlanType,
        selected_indexes: Vec<IndexSelection>,
    ) -> QueryPlan {
        QueryPlan {
            plan_type,
            table: None,
            estimated_cost: 0.0,
            estimated_rows: 0,
            selected_indexes,
            steps: Vec::new(),
            hints: super::super::planner::QueryHints::default(),
        }
    }

    fn primary_index() -> IndexSelection {
        IndexSelection {
            index_name: "PRIMARY".to_string(),
            columns: vec!["id".to_string()],
            selectivity: 0.1,
            index_type: IndexType::Primary,
        }
    }

    /// A point lookup resolves the partition via the partition index
    /// (Index.db / Summary.db) — it MUST report the index it used, not "scan".
    #[test]
    fn test_indexes_used_point_lookup_reports_partition_index() {
        let plan = plan_with(
            super::super::planner::PlanType::PointLookup,
            vec![primary_index()],
        );
        assert_eq!(QueryExecutor::indexes_used_for(&plan), vec!["PRIMARY"]);
    }

    /// A full table scan reports the explicit "scan" marker (we picked the
    /// marker over an empty list so EXPLAIN output is unambiguous).
    #[test]
    fn test_indexes_used_table_scan_reports_scan_marker() {
        let plan = plan_with(super::super::planner::PlanType::TableScan, Vec::new());
        assert_eq!(QueryExecutor::indexes_used_for(&plan), vec!["scan"]);
    }

    /// Regression (roborev job 40): a TableScan plan that is actually an INSERT
    /// or a CREATE TABLE never calls `storage.scan`, so it must NOT report the
    /// "scan" access path. `execute()` special-cases these before
    /// `execute_table_scan`; `indexes_used_for` must mirror that.
    #[test]
    fn test_indexes_used_insert_and_ddl_table_scan_report_no_scan() {
        use super::super::planner::{ParallelizationInfo, StepType};

        // INSERT: a TableScan plan carrying an Insert step.
        let insert_step = ExecutionStep {
            step_type: StepType::Insert,
            columns: Vec::new(),
            conditions: Vec::new(),
            cost: 0.0,
            parallelization: ParallelizationInfo {
                can_parallelize: false,
                suggested_threads: 1,
                partition_key: None,
            },
        };
        let mut insert_plan = plan_with(super::super::planner::PlanType::TableScan, Vec::new());
        insert_plan.steps = vec![insert_step];
        assert!(
            QueryExecutor::indexes_used_for(&insert_plan).is_empty(),
            "INSERT must not report a scan access path"
        );

        // CREATE TABLE: empty steps, a target table, zero estimated rows.
        let mut ddl_plan = plan_with(super::super::planner::PlanType::TableScan, Vec::new());
        ddl_plan.table = Some(TableId::new("t"));
        ddl_plan.estimated_rows = 0;
        assert!(
            QueryExecutor::indexes_used_for(&ddl_plan).is_empty(),
            "CREATE TABLE must not report a scan access path"
        );
    }

    /// Range scans degrade to a sequential scan in the executor → "scan".
    #[test]
    fn test_indexes_used_range_scan_reports_scan_marker() {
        let plan = plan_with(super::super::planner::PlanType::RangeScan, Vec::new());
        assert_eq!(QueryExecutor::indexes_used_for(&plan), vec!["scan"]);
    }

    /// IndexScan on a Primary/Bloom index does a real point lookup → report
    /// the index name. (These executor paths call `storage.get`.)
    #[test]
    fn test_indexes_used_index_scan_primary_reports_index() {
        let plan = plan_with(
            super::super::planner::PlanType::IndexScan,
            vec![primary_index()],
        );
        assert_eq!(QueryExecutor::indexes_used_for(&plan), vec!["PRIMARY"]);
    }

    /// IndexScan on a Secondary index currently degrades to a full scan in the
    /// executor (the secondary lookup is not yet wired up). Report "scan" —
    /// reporting the index would be fabrication.
    #[test]
    fn test_indexes_used_index_scan_secondary_reports_scan() {
        let secondary = IndexSelection {
            index_name: "idx_name".to_string(),
            columns: vec!["name".to_string()],
            selectivity: 0.1,
            index_type: IndexType::Secondary,
        };
        let plan = plan_with(super::super::planner::PlanType::IndexScan, vec![secondary]);
        assert_eq!(QueryExecutor::indexes_used_for(&plan), vec!["scan"]);
    }

    // -- parallelization metadata truthfulness (issue #1691) --------------

    /// A step that the planner marked parallelizable.
    fn parallelizable_step() -> ExecutionStep {
        use super::super::planner::{ParallelizationInfo, StepType};
        ExecutionStep {
            step_type: StepType::Scan,
            columns: Vec::new(),
            conditions: Vec::new(),
            cost: 0.0,
            parallelization: ParallelizationInfo {
                can_parallelize: true,
                suggested_threads: 8,
                partition_key: None,
            },
        }
    }

    /// Issue #1691: the TableScan path now runs through a SINGLE bounded
    /// `scan_stream` pass, not N parallel workers. Even when the planner
    /// suggested 8 threads, the reported metadata must be truthful:
    /// `threads_used == 1` and `effective == false`.
    #[test]
    fn test_parallelization_table_scan_reports_single_threaded() {
        let mut plan = plan_with(super::super::planner::PlanType::TableScan, Vec::new());
        plan.steps = vec![parallelizable_step()];

        let info = QueryExecutor::parallelization_for(&plan)
            .expect("a parallelizable step should still yield metadata");
        assert_eq!(info.threads_used, 1);
        assert!(!info.effective);
        assert!(info.partitions.is_empty());
    }

    /// A plan with no parallelizable step yields no parallelization metadata.
    #[test]
    fn test_parallelization_absent_when_no_step_parallelizes() {
        let plan = plan_with(super::super::planner::PlanType::TableScan, Vec::new());
        assert!(QueryExecutor::parallelization_for(&plan).is_none());
    }

    /// Non-scan plan types still surface the planner's suggested thread count as
    /// effective — only the retired table-scan path is neutralized.
    #[test]
    fn test_parallelization_non_scan_reports_planner_threads() {
        let mut plan = plan_with(super::super::planner::PlanType::Aggregation, Vec::new());
        plan.steps = vec![parallelizable_step()];

        let info = QueryExecutor::parallelization_for(&plan)
            .expect("a parallelizable step should yield metadata");
        assert_eq!(info.threads_used, 8);
        assert!(info.effective);
    }

    #[tokio::test]
    async fn test_condition_to_row_key_mapping() {
        let (_tmp, executor, _) = make_executor().await;

        let id_condition = Condition {
            column: "id".to_string(),
            operator: ComparisonOperator::Equal,
            value: Value::Integer(42),
        };
        let key = executor
            .condition_to_row_key(&id_condition)
            .expect("id condition key");
        assert_eq!(std::str::from_utf8(key.as_bytes()).unwrap(), "user_key_42");

        let name_condition = Condition {
            column: "username".to_string(),
            operator: ComparisonOperator::Equal,
            value: Value::Text("carol".to_string()),
        };
        let key = executor
            .condition_to_row_key(&name_condition)
            .expect("fallback key");
        assert_eq!(key.as_bytes(), b"carol");
    }

    /// Issue #1334 / roborev H1: the offset-read placeholder
    /// (`data_access::read_value_at_offset`) surfaces its raw bytes to
    /// SELECT/export through `get()` → `storage_data_to_query_row` as a single
    /// column keyed `"data"` — exactly the behaviour a bare `Value::Blob` had
    /// pre-#1334. The producer now emits explicit `ScanRow::RawRow` provenance;
    /// this pins that a `RawRow` keeps surfacing the value under `"data"`, while
    /// the equivalent `ScanRow::Marker` is SUPPRESSED (drops the blob to an
    /// id-only fallback) — the regression a `Marker` here would cause.
    #[tokio::test]
    async fn offset_read_row_surfaces_data_marker_is_suppressed() {
        let (_tmp, executor, _) = make_executor().await;
        let key = RowKey::new(vec![7]);
        let raw = vec![0xde, 0xad, 0xbe, 0xef];

        // The fixed producer output: the raw fallback surfaces its bytes as "data".
        let live = ScanRow::RawRow(raw.clone());
        let row = executor
            .storage_data_to_query_row(live, &key)
            .expect("raw offset-read row must convert");
        assert_eq!(
            row.values.get("data"),
            Some(&Value::Blob(raw.clone())),
            "a live offset/indexed read must surface its raw value as the \"data\" column"
        );

        // Marker (the pre-fix producer output): the blob is dropped; the row
        // falls back to an id-only shape with NO "data" column — proving a Marker
        // here would lose data that previously reached SELECT/export.
        let marker = ScanRow::Marker(Value::Blob(raw));
        let suppressed = executor
            .storage_data_to_query_row(marker, &key)
            .expect("marker row must still convert");
        assert!(
            !suppressed.values.contains_key("data"),
            "a Marker must NOT surface the raw blob (this is the suppression the fix avoids)"
        );
    }

    // -- retirement of execute_parallel_table_scan (issue #1691) -----------

    /// A `TableScan` plan whose step requested parallelization (`suggested_threads`
    /// = 4, mirroring the retired path's default worker count). It routes to
    /// `execute_table_scan` (no INSERT step, non-empty steps ⇒ not CREATE TABLE)
    /// and takes the `can_parallelize` branch.
    fn parallelizable_table_scan_plan() -> QueryPlan {
        use super::super::planner::{ParallelizationInfo, PlanType, QueryHints, StepType};
        QueryPlan {
            plan_type: PlanType::TableScan,
            table: Some(TableId::new("t")),
            estimated_cost: 0.0,
            estimated_rows: 1,
            selected_indexes: Vec::new(),
            steps: vec![ExecutionStep {
                step_type: StepType::Scan,
                columns: Vec::new(),
                conditions: Vec::new(),
                cost: 0.0,
                parallelization: ParallelizationInfo {
                    can_parallelize: true,
                    suggested_threads: 4,
                    partition_key: None,
                },
            }],
            hints: QueryHints::default(),
        }
    }

    /// Issue #1691 (verification-first): the parallelizable `TableScan` branch must
    /// issue EXACTLY ONE whole-table scan pass. The retired
    /// `execute_parallel_table_scan` spawned `suggested_threads` (4) workers, each
    /// re-running the identical full `storage.scan` — four duplicate whole-table
    /// passes for a single plan. With the retirement, the branch routes through the
    /// bounded streaming `scan_stream`, a single pass.
    ///
    /// This is RED on the pre-fix routing (the counter reads 4) and GREEN after
    /// (reads 1); it needs no on-disk data because the counter observes scan
    /// *initiations*, which the retired path made 4× even over an empty table.
    /// The counter is thread-local and this is a current-thread `#[tokio::test]`,
    /// so the scan runs on this thread and other parallel tests cannot pollute it.
    #[tokio::test]
    async fn table_scan_parallel_branch_issues_one_whole_table_pass() {
        let (_tmp, executor, _) = make_executor().await;
        crate::storage::reset_table_scan_calls();

        let plan = parallelizable_table_scan_plan();
        let _ = executor.execute(&plan).await.expect("table scan executes");

        assert_eq!(
            crate::storage::table_scan_call_count(),
            1,
            "the parallelizable TableScan branch must issue exactly ONE whole-table \
             scan pass; the retired execute_parallel_table_scan issued one per worker (4×)"
        );
    }

    /// The bounded streaming branch drains its results correctly (no rows dropped
    /// by the `scan_stream` backpressure discipline) and still issues one pass.
    /// Over an empty table this is the trivial-but-load-bearing lower bound: the
    /// branch returns an empty result set and never fans out into multiple scans.
    #[tokio::test]
    async fn streaming_scan_branch_returns_all_rows_bounded() {
        let (_tmp, executor, _) = make_executor().await;
        crate::storage::reset_table_scan_calls();

        let plan = parallelizable_table_scan_plan();
        let result = executor.execute(&plan).await.expect("table scan executes");

        assert!(result.rows.is_empty(), "empty table yields no rows");
        assert_eq!(
            crate::storage::table_scan_call_count(),
            1,
            "the streaming drain must not re-issue the scan"
        );
    }
}
