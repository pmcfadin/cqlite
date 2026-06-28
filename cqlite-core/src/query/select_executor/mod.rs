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
//!
//! ## Module layout
//!
//! The executor is split by responsibility (epic #1116):
//! - [`value_ops`] — value comparison + arithmetic primitives,
//! - [`predicate`] — SSTable leaf-predicate evaluation (public `evaluate_*`),
//! - [`lookup`] — partition/clustering lookup classification,
//! - [`aggregation`] — GROUP BY accumulation,
//! - [`row_build`] — scan-row assembly (public `build_row_from_scan`),
//! - [`writetime_ttl`] — WRITETIME/TTL projection + injectable clock,
//! - this `mod.rs` — the [`SelectExecutor`] orchestration and entry points.

mod aggregation;
mod lookup;
mod predicate;
mod row_build;
mod value_ops;
mod writetime_ttl;

#[cfg(test)]
pub(crate) mod test_support;

use super::{
    access_path::{AccessPath, FallbackReason},
    result::{
        cql_type_to_data_type, ColumnInfo, ProjectionFlags, QueryMetadata, QueryResult,
        QueryResultIterator, QueryRow, StreamingConfig,
    },
    select_ast::*,
    select_optimizer::{AggregationPlan, ExecutionStep, OptimizedQueryPlan, SSTablePredicate},
};
use crate::{
    schema::{CqlType, SchemaManager},
    storage::StorageEngine,
    types::{RowKey, Value},
    Error, Result, TableId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use aggregation::{
    build_group_key, finalize_group, find_or_init_group, update_aggregate, AggregationState,
};
use lookup::{
    classify_partition_lookup, honest_targeted_path, sort_rows_by_token, PartitionLookupOutcome,
};
use row_build::{column_info_from_type_str, parse_cql_type_str, parse_table_id};
use value_ops::{
    compare_values_ordering, const_arithmetic, eval_arithmetic, try_compare_values, values_equal,
};
use writetime_ttl::{
    evaluate_writetime_ttl, like_pattern_to_regex, select_has_writetime_ttl,
    writetime_ttl_column_name, SystemClock,
};

#[cfg(not(feature = "tombstones"))]
use lookup::classify_clustering_slice;

// Public surface re-exports (kept identical to the pre-split module so
// `query::mod`'s `pub use select_executor::{...}` resolves unchanged).
pub use predicate::{evaluate_leaf, evaluate_predicates, LeafOutcome};
pub use row_build::build_row_from_scan;
pub use writetime_ttl::{FixedClock, NowSeconds};

// `validate_token_predicates` is used by the executor's scan paths.
use predicate::validate_token_predicates;

/// SELECT query executor for SSTable-based storage
pub struct SelectExecutor {
    /// Schema manager for metadata
    _schema: Arc<SchemaManager>,
    /// Storage engine for SSTable access
    storage: Arc<StorageEngine>,
    /// Clock used for TTL "remaining seconds" computation (injectable for tests).
    clock: Arc<dyn NowSeconds>,
}

impl std::fmt::Debug for SelectExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectExecutor")
            .field("_schema", &self._schema)
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

/// Query execution context
///
/// Pure bookkeeping for an in-flight query. Only used internally; the public
/// API surface is `SelectExecutor` itself.
#[derive(Debug)]
struct ExecutionContext {
    /// Current table being queried
    pub table_id: TableId,
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Row count processed so far
    pub rows_processed: u64,
    /// Rows examined by the SSTable-scan step ONLY (issue #1035). Distinct from
    /// `rows_processed`, which is also bumped by the residual `Filter` step, so
    /// this is the correct, non-double-counted source for the
    /// `cqlite.query.rows_scanned` metric and span field.
    pub scan_rows: u64,
    /// Projection flags controlling opt-in metadata collection (Issue #692).
    ///
    /// Set to `include_cell_metadata = true` when any `WRITETIME` or `TTL`
    /// select item is detected during planning so the reader can thread
    /// per-cell write metadata.
    pub projection_flags: ProjectionFlags,
    /// Access path chosen by the SSTable-scan step for THIS query (Issue #960).
    ///
    /// Per-query state, set where the scan step decides its path. The
    /// result-attached `QueryMetadata.access_path` is read from here, NOT from
    /// the process-global probe, so concurrent SELECTs cannot overwrite each
    /// other's reported path between `record()` and the result build. The global
    /// probe (`access_path::record/last`) remains for test assertions only.
    pub access_path: Option<AccessPath>,
}

impl SelectExecutor {
    /// Create a new SELECT executor with a system (wall-clock) now source.
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self {
            _schema: schema,
            storage,
            clock: Arc::new(SystemClock),
        }
    }

    /// Create a SELECT executor with a custom clock (for deterministic tests).
    #[cfg(test)]
    pub fn with_clock(
        schema: Arc<SchemaManager>,
        storage: Arc<StorageEngine>,
        clock: Arc<dyn NowSeconds>,
    ) -> Self {
        Self {
            _schema: schema,
            storage,
            clock,
        }
    }

    /// Derive a bounded plan-family label for a SELECT, from the plan it executed
    /// and the honest access path the SSTable-scan step actually took (issue #1035).
    ///
    /// The string is one of the `PlanType` `Debug` forms (`"TableScan"`,
    /// `"PointLookup"`, `"RangeScan"`, `"Aggregation"`) so that
    /// `QueryEngine::plan_type_label` maps it onto the same bounded metric/span
    /// taxonomy the legacy executor already uses — keeping cardinality bounded and
    /// the dimension consistent across surfaces. The access path is the same
    /// per-query signal surfaced on `QueryMetadata.access_path` (epic #951/#960);
    /// this never inspects query text or key values.
    fn select_plan_family(plan: &OptimizedQueryPlan, access_path: Option<&AccessPath>) -> String {
        // Aggregation dominates: a query that aggregates is reported as such
        // regardless of how its underlying scan was served.
        if plan.aggregation_plan.is_some() {
            return "Aggregation".to_string();
        }
        match access_path {
            Some(
                AccessPath::PartitionLookup
                | AccessPath::MultiPartitionLookup
                | AccessPath::MetadataPartitionLookup
                | AccessPath::StreamingPartitionLookup,
            ) => "PointLookup".to_string(),
            Some(AccessPath::ClusteringSlice) => "RangeScan".to_string(),
            // Full scan, any documented fallback, or no recorded path (e.g. a plan
            // with no scan step) is honestly a table scan.
            Some(AccessPath::FullScan | AccessPath::FallbackFullScan { .. }) | None => {
                "TableScan".to_string()
            }
        }
    }

    /// Build the bounded `PlanInfo` carried on a SELECT result so the engine's
    /// single observability chokepoint can dimension `QUERY_DURATION`/`QUERY_ROWS`
    /// and the parent span by a real plan family instead of `"unknown"`
    /// (issue #1035). Only the bounded plan-family string and the chosen access
    /// path (as the single "index used" entry) are recorded — never query text.
    fn select_plan_info(
        plan: &OptimizedQueryPlan,
        access_path: Option<&AccessPath>,
    ) -> crate::query::result::PlanInfo {
        crate::query::result::PlanInfo {
            plan_type: Self::select_plan_family(plan, access_path),
            estimated_cost: 0.0,
            actual_cost: 0.0,
            indexes_used: access_path
                .map(|p| vec![p.label().to_string()])
                .unwrap_or_default(),
            steps: vec![],
            parallelization: None,
        }
    }

    /// Execute an optimized query plan.
    ///
    /// Instrumented as `query.select.plan` (issue #1035): this span covers the
    /// modern SELECT pipeline — SSTable scan, filtering, projection, aggregation,
    /// and WRITETIME/TTL metadata extraction — and is the parent under which the
    /// read-path spans (issue #1034) nest. On completion it emits
    /// [`catalog::QUERY_ROWS_SCANNED`] (rows the scan step examined) dimensioned by
    /// the honest access path, so the rows-scanned vs rows-returned gap is
    /// observable. The bounded access-path attribute is recorded on the span; the
    /// query text and key values never are.
    #[tracing::instrument(
        name = "query.select.plan",
        skip_all,
        fields(
            cqlite.query.access_path = tracing::field::Empty,
            cqlite.query.rows_scanned = tracing::field::Empty,
            cqlite.query.rows = tracing::field::Empty,
        )
    )]
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        // Issue #960: clear the global access-path probe so a stale value from a
        // previous query cannot satisfy a test assertion against this one.
        crate::query::access_path::reset();

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), use a dummy table ID
            TableId::new("_dummy_")
        };

        // Issue #692: detect whether any WRITETIME/TTL select items are present
        // during planning and set the opt-in flag so the reader threads per-cell
        // metadata. This is the "planning" half of the executor wiring; the
        // "evaluation" half lives in `evaluate_select_expression`.
        let projection_flags = ProjectionFlags {
            include_cell_metadata: select_has_writetime_ttl(&plan.statement),
        };
        log::debug!(
            "Query plan: include_cell_metadata={}",
            projection_flags.include_cell_metadata
        );

        let mut context = ExecutionContext {
            table_id,
            columns: self.get_result_columns(&plan.statement).await?,
            rows_processed: 0,
            scan_rows: 0,
            projection_flags,
            access_path: None,
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
                ExecutionStep::PerPartitionLimit { count } => {
                    intermediate_results =
                        Self::execute_per_partition_limit(intermediate_results, *count);
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
        // When SELECT * is used and no schema was found, context.columns is empty.
        // Fall back to inferring column names from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Try to resolve schema to get proper CQL types (Issue #674).
            let schema_opt = if let Some(ref from_clause) = plan.statement.from_clause {
                if let Ok(table_id) = self.extract_table_id(from_clause) {
                    let (keyspace, table_name) = parse_table_id(&table_id);
                    self._schema
                        .find_schema_by_table(&keyspace, &table_name)
                        .await
                } else {
                    None
                }
            } else {
                None
            };

            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            let table_name_for_meta = schema_opt
                .as_ref()
                .map(|s| format!("{}.{}", s.keyspace, s.table));

            for (idx, col_name) in col_names.iter().enumerate() {
                // Look up CQL type from schema; derive flat DataType from it (Issue #674).
                let col_info = match schema_opt.as_ref().and_then(|schema| {
                    schema
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == col_name.as_str())
                }) {
                    Some(schema_col) => column_info_from_type_str(
                        (*col_name).clone(),
                        &schema_col.data_type,
                        idx,
                        table_name_for_meta.clone(),
                    ),
                    None => ColumnInfo {
                        name: (*col_name).clone(),
                        data_type: crate::types::DataType::Text,
                        nullable: true,
                        position: idx,
                        table_name: table_name_for_meta.clone(),
                        cql_type: None,
                    },
                };
                columns.push(col_info);
            }
        }

        // Observability (issue #1035): the `query.select.plan` span declared
        // `access_path`/`rows_scanned`/`rows` but never recorded them, and
        // `QUERY_ROWS_SCANNED` was never emitted. Do both here, sourced from the
        // honest per-query signal (`context.access_path`, set by the SSTable-scan
        // step) and the rows the scan examined (`context.rows_processed`). Bounded
        // attributes only — never the query text or key values.
        {
            use crate::observability::{self as obs, catalog, AttrValue};

            let access_path_label: &'static str = context
                .access_path
                .as_ref()
                .map(|p| p.label())
                .unwrap_or("unknown");

            obs::add_counter(
                catalog::QUERY_ROWS_SCANNED,
                context.scan_rows,
                &[(
                    catalog::attr::ACCESS_PATH,
                    AttrValue::StaticStr(access_path_label),
                )],
            );

            let span = tracing::Span::current();
            span.record(catalog::attr::ACCESS_PATH, access_path_label);
            span.record("cqlite.query.rows_scanned", context.scan_rows);
            span.record("cqlite.query.rows", total_rows);
        }

        // Issue #1035: carry a bounded plan family on the result so the engine's
        // single observability chokepoint reports a real plan type for SELECTs
        // (the modern executor previously always returned `plan_info: None`,
        // forcing plan_type to "unknown").
        let plan_info = Self::select_plan_info(&plan, context.access_path.as_ref());

        Ok(QueryResult {
            rows: intermediate_results,
            rows_affected: total_rows, // Use actual number of rows returned
            execution_time_ms: 0,      // Will be set by the engine
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: Some(plan_info),
                performance: Default::default(),
                warnings: vec![],
                // Issue #960: surface the access path the SSTable-scan step chose
                // on the result from PER-QUERY state (not the global probe), so a
                // concurrent SELECT cannot overwrite it between record() and here.
                access_path: context.access_path.clone(),
            },
        })
    }

    /// Execute an optimized query plan with streaming results (Issue #280)
    ///
    /// Instead of materializing all rows in memory, this method returns a
    /// `QueryResultIterator` that yields rows incrementally via a bounded channel.
    /// This enables memory-efficient processing of large result sets.
    ///
    /// # Memory Budget
    ///
    /// With default `StreamingConfig::buffer_size` of 1024 rows and ~1KB avg row size:
    /// - Channel buffer: ~1MB in flight
    /// - Background task: minimal overhead
    /// - Total streaming overhead: ~1-2MB (well within 128MB target)
    ///
    /// # Limitations
    ///
    /// Currently supports:
    /// - SSTableScan with predicates (streaming)
    /// - Filter/Limit/Project (applied during scan)
    ///
    /// `LIMIT` (and `OFFSET`, when present in the plan) is enforced by the
    /// streaming producer (`execute_streaming_background`): it skips `OFFSET`
    /// matches and stops scanning once `count` rows have been sent, so a
    /// `LIMIT N` query yields exactly `N` rows without materializing the rest
    /// (Issue #581).
    ///
    /// For ORDER BY/GROUP BY/DISTINCT, falls back to full execution then streams results.
    pub async fn execute_streaming(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Issue #960: clear the global access-path probe so a stale value from a
        // previous query cannot satisfy a test assertion against this one.
        crate::query::access_path::reset();

        // Check if query requires full materialization (ORDER BY, GROUP BY, aggregates)
        if self.requires_materialization(&plan) {
            log::info!("Query requires materialization (ORDER BY/GROUP BY/aggregates), using execute-then-stream");
            return self.execute_and_stream(plan, config).await;
        }

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), fall back to execute
            return self.execute_and_stream(plan, config).await;
        };

        let columns = self.get_result_columns(&plan.statement).await?;

        // Create bounded channel for backpressure
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Determine execution steps
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: table_id.clone(),
                predicates: vec![],
                projection: columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        // FINDING 1 (roborev, Issue #955 follow-up): synchronous preconditions
        // that should FAIL the query must be checked BEFORE spawning the
        // streaming task. Errors raised inside `execute_streaming_background`
        // are only logged by the spawn closure (the channel then closes), so the
        // caller would receive an apparently-successful iterator that yields zero
        // rows — silently hiding an invalid `token(...)` query. Validating here
        // surfaces the error synchronously from `execute_streaming`, matching the
        // materializing `execute()` path. The schema must be resolved before the
        // spawn for this, so we resolve it per scan step here.
        for step in &execution_steps {
            if let ExecutionStep::SSTableScan {
                table, predicates, ..
            } = step
            {
                let (keyspace, table_name) = parse_table_id(table);
                let schema_opt = self
                    ._schema
                    .find_schema_by_table(&keyspace, &table_name)
                    .await;
                validate_token_predicates(predicates, schema_opt.as_ref())?;
            }
        }

        // Clone what we need for the background task
        let storage = Arc::clone(&self.storage);
        let schema_manager = Arc::clone(&self._schema);
        let buffer_size = config.buffer_size;

        // Spawn background task to stream rows
        tokio::spawn(async move {
            if let Err(e) = Self::execute_streaming_background(
                storage,
                schema_manager,
                table_id,
                execution_steps,
                tx,
                buffer_size,
            )
            .await
            {
                log::error!("Streaming execution error: {}", e);
                // Error is logged; channel will close and consumer will see None
            }
        });

        // Create metadata for the iterator
        let metadata = QueryMetadata {
            columns,
            total_rows: None, // Unknown for streaming
            plan_info: None,
            performance: Default::default(),
            warnings: vec![],
            // Issue #960: the streaming scan runs in the spawned task above, so the
            // access path is not yet recorded when this iterator is constructed.
            // Streaming surfaces report the path via the global probe
            // (`crate::query::access_path::last()`) after at least one row is
            // pulled, not on the iterator metadata.
            access_path: None,
        };

        Ok(QueryResultIterator::new(rx, metadata))
    }

    /// Check if query plan requires full materialization before streaming
    fn requires_materialization(&self, plan: &OptimizedQueryPlan) -> bool {
        for step in &plan.execution_steps {
            match step {
                ExecutionStep::Sort { .. } => return true,
                ExecutionStep::Aggregate { .. } => return true,
                _ => {}
            }
        }

        // Check for DISTINCT
        if matches!(plan.statement.select_clause, SelectClause::Distinct(_)) {
            return true;
        }

        // Issue #693: WRITETIME()/TTL() expressions require full materialisation
        // because the streaming background task only emits raw scan rows without
        // applying the WRITETIME/TTL projection (cell metadata extraction and
        // value computation).  Falling back to execute_and_stream ensures the
        // complete execute() path runs, which correctly populates writetime(col)/
        // ttl(col) keys in each row's values map.
        select_has_writetime_ttl(&plan.statement)
    }

    /// Fallback: Execute query fully, then stream the results
    async fn execute_and_stream(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Execute full query
        let result = self.execute(plan).await?;

        // Create channel to stream results
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Spawn task to send rows through channel
        tokio::spawn(async move {
            for row in result.rows {
                if tx.send(Ok(row)).await.is_err() {
                    break; // Consumer dropped
                }
            }
            // Channel closes automatically when tx drops
        });

        Ok(QueryResultIterator::new(rx, result.metadata))
    }

    /// Background task: Execute streaming scan and send rows through channel
    async fn execute_streaming_background(
        storage: Arc<StorageEngine>,
        schema_manager: Arc<SchemaManager>,
        _table_id: TableId,
        execution_steps: Vec<ExecutionStep>,
        tx: mpsc::Sender<Result<QueryRow>>,
        buffer_size: usize,
    ) -> Result<()> {
        // Issue #581: LIMIT/OFFSET must be enforced by the producer in the
        // streaming path. The `ExecutionStep::Limit` arm previously only logged a
        // message and relied on a consumer that never applied it, so
        // `execute_streaming` yielded the full result set regardless of LIMIT.
        // Extract the bound up front (steps are ordered with Limit after the scan)
        // and stop sending once it is satisfied — mirroring `execute_limit`
        // (drain OFFSET, then truncate to `count`) row-by-row so the producer
        // stops scanning early.
        let limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::Limit { count, offset } => Some((*count, offset.unwrap_or(0))),
            _ => None,
        });
        let (limit_count, mut offset_remaining) = match limit {
            Some((count, offset)) => (Some(count), offset),
            None => (None, 0),
        };

        // A `LIMIT 0` means no rows can ever be sent; return before scanning.
        if limit_count == Some(0) {
            return Ok(());
        }

        // Issue #757: PER PARTITION LIMIT caps rows per partition before the
        // query-wide LIMIT/OFFSET. The scan yields rows grouped by partition
        // key, so we track the current partition (by its raw key bytes) and
        // reset the counter at each boundary.
        let per_partition_limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::PerPartitionLimit { count } => Some(*count),
            _ => None,
        });
        let mut current_partition: Option<Vec<u8>> = None;
        let mut partition_count: u64 = 0;

        let mut sent: u64 = 0;

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let (keyspace, table_name) = parse_table_id(table);
                    let schema_opt = schema_manager
                        .find_schema_by_table(&keyspace, &table_name)
                        .await;

                    // FINDING 2 (Issue #955 follow-up): reject a `token(...)` whose
                    // columns are not the full partition key in declared order
                    // before scanning (same rule as the materializing path).
                    validate_token_predicates(predicates, schema_opt.as_ref())?;

                    // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
                    // partition-targeted lookup that prunes SSTables via bloom/BTI,
                    // instead of streaming a scan over every SSTable. The resulting
                    // rows are sent through the same per-row pipeline below
                    // (predicates, PER PARTITION LIMIT, OFFSET, LIMIT). Note
                    // `scan_partition` reconciles across SSTable generations like the
                    // materializing `scan()` (last-write-wins + tombstone shadowing),
                    // which is the authoritative read semantics; it does not merely
                    // mirror `scan_stream`'s per-key merge.
                    let lookup = classify_partition_lookup(predicates, schema_opt.as_ref());
                    if let PartitionLookupOutcome::Targeted(ref pk_bytes) = lookup {
                        // Issue #960: the streaming analogue of the materializing
                        // partition-targeted lookup. Epic #951 (honest paths): the
                        // `tombstones` build's `scan_partition` is a full-scan +
                        // retain with NO prune, reported via `engaged == false`; only
                        // claim `StreamingPartitionLookup` when it really pruned.
                        let (rows, engaged) = storage
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
                            .await?;
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::StreamingPartitionLookup,
                            engaged,
                        ));
                        for (key, value) in rows {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookup.
                        continue;
                    }

                    // Issue #955: `WHERE pk IN (...)` over the complete key is the
                    // union of N partition-targeted lookups. Gather them, sort by
                    // token to match full-scan order, then drive the same per-row
                    // pipeline (predicates, PER PARTITION LIMIT, OFFSET, LIMIT).
                    if let PartitionLookupOutcome::MultiTargeted(ref pk_keys) = lookup {
                        // Epic #951 (honest paths): each lookup reports whether it
                        // pruned. On the `tombstones` build every call full-scans
                        // (`engaged == false`); claim `MultiPartitionLookup` only when
                        // the lookups actually pruned, else report the honest fallback.
                        let mut combined = Vec::new();
                        let mut all_engaged = true;
                        for pk_bytes in pk_keys {
                            let (rows, engaged) = storage
                                .scan_partition(table, pk_bytes, schema_opt.as_ref())
                                .await?;
                            all_engaged &= engaged;
                            combined.extend(rows);
                        }
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::MultiPartitionLookup,
                            all_engaged,
                        ));
                        sort_rows_by_token(&mut combined);
                        for (key, value) in combined {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookups.
                        continue;
                    }

                    // Issue #960: the streaming path did not take a targeted
                    // lookup; report the honest fallback reason. `lookup` is the
                    // `Fallback` arm here (the `Targeted`/`MultiTargeted` arms
                    // returned above via `continue`).
                    if let PartitionLookupOutcome::Fallback(reason) = lookup {
                        crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    }

                    // Issue #790: pull rows lazily from a bounded streaming scan
                    // instead of materializing the full result `Vec`. The reader
                    // parses one entry at a time into this channel, so live heap
                    // stays bounded by `buffer_size` rather than O(result rows).
                    let mut scan_stream = storage
                        .scan_stream(table, None, None, schema_opt.as_ref(), buffer_size)
                        .await?;

                    while let Some(item) = scan_stream.recv().await {
                        let (key, value) = item?;
                        // Capture the partition key bytes before `key` is moved
                        // into row construction (only when needed).
                        let part_sig = per_partition_limit.map(|_| key.0.clone());
                        let Some(row) =
                            build_row_from_scan(key, value, projection, schema_opt.as_ref())
                        else {
                            continue;
                        };

                        if !evaluate_predicates(&row, predicates)? {
                            continue;
                        }

                        // Apply PER PARTITION LIMIT: cap matching rows per
                        // partition, before OFFSET/LIMIT (Cassandra semantics).
                        if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                            if current_partition.as_deref() != Some(sig.as_slice()) {
                                current_partition = Some(sig);
                                partition_count = 0;
                            }
                            if partition_count >= cap {
                                continue;
                            }
                            partition_count += 1;
                        }

                        // Apply OFFSET: skip the first `offset_remaining` matches.
                        if offset_remaining > 0 {
                            offset_remaining -= 1;
                            continue;
                        }

                        // Send row through channel (with backpressure). Consumer drop ends the scan.
                        if tx.send(Ok(row)).await.is_err() {
                            return Ok(());
                        }
                        sent += 1;

                        // Apply LIMIT: stop scanning once `count` rows have been
                        // sent. Dropping `scan_stream` here signals the producer
                        // (via a closed channel) to stop parsing early.
                        if let Some(count) = limit_count {
                            if sent >= count {
                                return Ok(());
                            }
                        }
                    }
                }
                ExecutionStep::Limit { .. } | ExecutionStep::PerPartitionLimit { .. } => {
                    // Enforced inline during the scan above (see the bounds
                    // extracted before the loop).
                }
                // Projection and predicate filtering are pushed into SSTableScan above.
                ExecutionStep::Project { .. } | ExecutionStep::Filter { .. } => {}
                _ => {
                    log::warn!("Streaming execution: skipping unsupported step {:?}", step);
                }
            }
        }

        Ok(())
    }

    /// Execute SSTable scan with predicate pushdown.
    ///
    /// Per-row work (build row, decode partition key, evaluate predicates) is
    /// handled by the free helpers `build_row_from_scan` and
    /// `evaluate_predicates`, which are shared with the streaming background
    /// task to keep the two execution paths in lockstep.
    async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const MAX_RESULTS: usize = 1_000_000;

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}, include_cell_metadata={}",
            table,
            predicates,
            context.projection_flags.include_cell_metadata,
        );

        let (keyspace, table_name) = parse_table_id(table);
        let schema_opt = self
            ._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await;

        match schema_opt.as_ref() {
            Some(schema) => log::info!(
                "Found schema for {}.{} with {} columns",
                schema.keyspace,
                schema.table,
                schema.columns.len()
            ),
            None => log::info!(
                "No schema found for {}.{}, proceeding without schema-aware parsing",
                keyspace.as_deref().unwrap_or("unknown"),
                table_name
            ),
        }

        // FINDING 2 (Issue #955 follow-up): a `token(...)` predicate is evaluated
        // by hashing the row's raw partition key, so its argument columns MUST be
        // the full partition key in declared order or the result is silently
        // wrong. Reject (Cassandra-style) before scanning/evaluating.
        validate_token_predicates(predicates, schema_opt.as_ref())?;

        // Issue #693: When WRITETIME(col) or TTL(col) is in the SELECT, use the
        // metadata-carrying scan so per-cell timestamps reach the QueryRow.
        let mut results = Vec::new();
        if context.projection_flags.include_cell_metadata {
            // Issue #962: route a fully-constrained `WHERE pk = ?` WRITETIME/TTL
            // projection through a partition-targeted metadata lookup that prunes
            // SSTables (bloom/BTI) before decoding, instead of full-scanning every
            // SSTable for the table. Reuses the SAME `classify_partition_lookup`
            // decision the non-metadata path uses (the shared resolved
            // partition-lookup representation). The per-row predicate evaluation
            // below is unchanged, so the pk equality itself is still applied as a
            // correctness backstop and any bloom/BTI over-inclusion is filtered out.
            let scan_results = match classify_partition_lookup(predicates, schema_opt.as_ref()) {
                PartitionLookupOutcome::Targeted(pk_bytes) => {
                    log::info!(
                        "SSTableScan(metadata): partition-key point lookup (key len={}) for \"{}\"",
                        pk_bytes.len(),
                        table
                    );
                    // Epic #951 (honest paths): the `tombstones` build's metadata
                    // lookup is a full metadata scan + retain with NO prune,
                    // reported via `engaged == false`; claim
                    // `MetadataPartitionLookup` only when it really pruned, else
                    // report the honest `TombstonesBuildNoPrune` fallback (the
                    // rows are byte-identical either way).
                    let (rows, engaged) = self
                        .storage
                        .scan_partition_with_cell_metadata(table, &pk_bytes, schema_opt.as_ref())
                        .await?;
                    let path = honest_targeted_path(AccessPath::MetadataPartitionLookup, engaged);
                    context.access_path = Some(path.clone());
                    crate::query::access_path::record(path);
                    rows
                }
                // Issue #962: `WHERE pk IN (...)` on the metadata path is NOT yet
                // fanned out to N targeted metadata lookups; it still full-scans.
                // Report that honestly (MetadataScanPath) rather than faking a
                // targeted path — the IN-metadata fan-out is a documented follow-up.
                PartitionLookupOutcome::MultiTargeted(_) | PartitionLookupOutcome::Fallback(_) => {
                    let metadata_path = AccessPath::FallbackFullScan {
                        reason: FallbackReason::MetadataScanPath,
                    };
                    context.access_path = Some(metadata_path.clone());
                    crate::query::access_path::record(metadata_path);
                    self.storage
                        .scan_with_cell_metadata(table, None, None, None, schema_opt.as_ref())
                        .await?
                }
            };

            log::info!("Scan (with metadata) returned {} rows", scan_results.len());

            for (key, value, cell_meta) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                let Some(mut row) =
                    build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                // Attach per-cell metadata so evaluate_writetime_ttl can read it.
                if !cell_meta.is_empty() {
                    row.set_cell_metadata(cell_meta);
                }

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
                }
            }
        } else {
            // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
            // partition-targeted lookup that prunes SSTables via bloom/BTI and only
            // parses the candidates, instead of scanning every SSTable for the
            // table. Falls back to a full scan when the partition key isn't fully
            // pinned or can't be encoded. The per-row predicate evaluation below is
            // unchanged, so clustering predicates and the pk equality itself are
            // still applied (and any over-inclusion is filtered out).
            let scan_results = match classify_partition_lookup(predicates, schema_opt.as_ref()) {
                PartitionLookupOutcome::Targeted(pk_bytes) => {
                    log::info!(
                        "SSTableScan: partition-key point lookup (key len={}) for \"{}\"",
                        pk_bytes.len(),
                        table
                    );
                    // Issue #954: when a single-column clustering restriction is
                    // present, push it down to a within-partition seek so a wide
                    // partition's slice decodes O(matched rows + index), not the
                    // whole partition. The seek reports whether the clustering
                    // narrowing actually engaged; the per-row backstop below applies
                    // the exact bound so output is byte-identical either way.
                    //
                    // Issue #960: report the HONEST access path — `ClusteringSlice`
                    // only when the seek engaged, else `PartitionLookup`. The
                    // clustering seek exists only on the default build; the
                    // `tombstones` build uses the plain partition lookup.
                    #[cfg(not(feature = "tombstones"))]
                    {
                        let clustering = classify_clustering_slice(predicates, schema_opt.as_ref());
                        let (rows, engaged) = self
                            .storage
                            .scan_partition_clustering(
                                table,
                                &pk_bytes,
                                clustering.as_ref(),
                                schema_opt.as_ref(),
                            )
                            .await?;
                        let path = if engaged {
                            AccessPath::ClusteringSlice
                        } else {
                            AccessPath::PartitionLookup
                        };
                        context.access_path = Some(path.clone());
                        crate::query::access_path::record(path);
                        rows
                    }
                    #[cfg(feature = "tombstones")]
                    {
                        // Epic #951 (honest paths): the `tombstones` build's
                        // `scan_partition` is a full scan + retain with NO prune,
                        // reported via `engaged == false`. Report the honest
                        // fallback rather than a fake `PartitionLookup`; the rows
                        // are byte-identical to the pruned build.
                        let (rows, engaged) = self
                            .storage
                            .scan_partition(table, &pk_bytes, schema_opt.as_ref())
                            .await?;
                        let path = honest_targeted_path(AccessPath::PartitionLookup, engaged);
                        context.access_path = Some(path.clone());
                        crate::query::access_path::record(path);
                        rows
                    }
                }
                PartitionLookupOutcome::MultiTargeted(pk_keys) => {
                    log::info!(
                        "SSTableScan: multi-partition lookup ({} keys) for \"{}\"",
                        pk_keys.len(),
                        table
                    );
                    // Issue #955/#960: `WHERE pk IN (...)` over the complete key
                    // is the union of N independent partition-targeted lookups,
                    // each of which prunes SSTables. Epic #951 (honest paths): on
                    // the `tombstones` build each lookup full-scans + retains with
                    // NO prune (`engaged == false`); report `MultiPartitionLookup`
                    // only when the lookups actually pruned, else the honest
                    // `TombstonesBuildNoPrune` fallback. Rows are unchanged.
                    let mut combined = Vec::new();
                    let mut all_engaged = true;
                    for pk_bytes in &pk_keys {
                        let (rows, engaged) = self
                            .storage
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
                            .await?;
                        all_engaged &= engaged;
                        combined.extend(rows);
                    }
                    let path = honest_targeted_path(AccessPath::MultiPartitionLookup, all_engaged);
                    context.access_path = Some(path.clone());
                    crate::query::access_path::record(path);
                    // Order the union to equal a full scan filtered to these keys:
                    // partitions are stored token-ordered, so sort the combined
                    // rows by (partition token, raw key bytes). A *stable* sort
                    // keeps each partition's clustering order (rows for one key
                    // arrive contiguously from one `scan_partition`) intact.
                    sort_rows_by_token(&mut combined);
                    combined
                }
                PartitionLookupOutcome::Fallback(reason) => {
                    // Issue #960: report the honest reason a full scan was chosen.
                    context.access_path = Some(AccessPath::FallbackFullScan { reason });
                    crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    self.storage
                        .scan(table, None, None, None, schema_opt.as_ref())
                        .await?
                }
            };

            log::info!("Scan returned {} rows", scan_results.len());

            for (key, value) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                // build_row_from_scan returns None for tombstoned/null rows (Issue #191).
                let Some(row) = build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
                }
            }
        }

        Ok(results)
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

    /// Evaluate comparison expression. Operators that need a single right
    /// operand share one `evaluate` call; IN/LIKE/IS NULL fall through to
    /// their custom branches.
    fn evaluate_comparison(&self, comp: &ComparisonExpression, row: &QueryRow) -> Result<bool> {
        use ComparisonOperator::*;

        let left_value = self.evaluate_select_expression(&comp.left, row)?;

        // Fast path for null tests, which ignore the right side.
        match comp.operator {
            IsNull => return Ok(left_value.is_null()),
            IsNotNull => return Ok(!left_value.is_null()),
            _ => {}
        }

        match (&comp.operator, &comp.right) {
            (
                op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan
                | GreaterThanOrEqual),
                ComparisonRightSide::Value(right_expr),
            ) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                let result = match op {
                    Equal => values_equal(&left_value, &right_value),
                    NotEqual => !values_equal(&left_value, &right_value),
                    LessThan => try_compare_values(&left_value, &right_value)?.is_lt(),
                    LessThanOrEqual => try_compare_values(&left_value, &right_value)?.is_le(),
                    GreaterThan => try_compare_values(&left_value, &right_value)?.is_gt(),
                    GreaterThanOrEqual => try_compare_values(&left_value, &right_value)?.is_ge(),
                    _ => unreachable!("guarded by outer match"),
                };
                Ok(result)
            }
            (In, ComparisonRightSide::ValueList(value_exprs)) => {
                for value_expr in value_exprs {
                    let value = self.evaluate_select_expression(value_expr, row)?;
                    if left_value == value {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (Like, ComparisonRightSide::Value(pattern_expr)) => {
                let pattern = self.evaluate_select_expression(pattern_expr, row)?;
                if let (Value::Text(text), Value::Text(pattern_str)) = (&left_value, &pattern) {
                    Ok(self.match_like_pattern(text, pattern_str))
                } else {
                    Ok(false)
                }
            }
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
            // Issue #961: a `?` placeholder must be bound to a concrete value
            // before execution. Reaching here means binding was skipped, which is
            // an internal logic error rather than user input — report it instead
            // of panicking.
            SelectExpression::BindMarker(idx) => Err(Error::query_execution(format!(
                "Unbound parameter placeholder ?{idx} reached execution; \
                 parameters must be bound before the query runs"
            ))),
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
            // Issue #692: evaluate WRITETIME(col) / TTL(col) against the per-cell
            // metadata carrier threaded by the reader when `ProjectionFlags::include_cell_metadata`
            // is set. Returns `Value::Null` when metadata is absent (e.g. no schema-aware
            // read path or the column was a partition-key column with no cell header).
            SelectExpression::WriteTimeTtl(call) => {
                let now_secs = self.clock.now_seconds();
                Ok(evaluate_writetime_ttl(call, row, now_secs))
            }
        }
    }

    /// Evaluate collection access operations (`list[idx]`, `map['key']`,
    /// `value IN set_column`).
    fn evaluate_collection_access(
        &self,
        access: &CollectionAccessExpression,
        row: &QueryRow,
    ) -> Result<Value> {
        let lookup_column = |col: &ColumnRef| -> Result<&Value> {
            row.values
                .get(&col.column)
                .ok_or_else(|| Error::query_execution(format!("Column not found: {}", col.column)))
        };

        match access {
            CollectionAccessExpression::ListIndex(col_ref, index_expr) => {
                let list_value = lookup_column(col_ref)?;
                let index_value = self.evaluate_select_expression(index_expr, row)?;

                let (Value::List(list), Value::Integer(index)) = (list_value, &index_value) else {
                    return Err(Error::query_execution("Invalid list access".to_string()));
                };
                if *index >= 0 && (*index as usize) < list.len() {
                    Ok(list[*index as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            CollectionAccessExpression::MapKey(col_ref, key_expr) => {
                let map_value = lookup_column(col_ref)?;
                let key_value = self.evaluate_select_expression(key_expr, row)?;

                let Value::Map(map) = map_value else {
                    return Err(Error::query_execution("Invalid map access".to_string()));
                };
                Ok(map
                    .iter()
                    .find(|(k, _)| *k == key_value)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null))
            }
            CollectionAccessExpression::SetContains(col_ref, value_expr) => {
                let set_value = lookup_column(col_ref)?;
                let test_value = self.evaluate_select_expression(value_expr, row)?;

                let Value::Set(set) = set_value else {
                    return Err(Error::query_execution(
                        "Invalid set contains operation".to_string(),
                    ));
                };
                Ok(Value::Boolean(set.contains(&test_value)))
            }
        }
    }

    /// Evaluate arithmetic expressions on a (left, op, right) triple.
    ///
    /// Runtime arithmetic supports same-type Integer or Float operands. Mixed
    /// types or non-numeric operands return an error. (Constant-folding
    /// arithmetic additionally accepts BigInt — see
    /// `evaluate_constant_expression`.)
    fn evaluate_arithmetic(
        &self,
        op: &ArithmeticOperator,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        match (&left, &right) {
            (Value::Integer(_), Value::Integer(_)) | (Value::Float(_), Value::Float(_)) => {
                eval_arithmetic(op, left, right)
            }
            _ => Err(Error::query_execution(
                "Incompatible types for arithmetic".to_string(),
            )),
        }
    }

    /// Simple LIKE pattern matching. The CQL pattern syntax (`%`, `_`) is
    /// translated by `like_pattern_to_regex` before compilation.
    fn match_like_pattern(&self, text: &str, pattern: &str) -> bool {
        regex::Regex::new(&like_pattern_to_regex(pattern))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
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

                let ordering = match item.direction {
                    SortDirection::Ascending => compare_values_ordering(&a_val, &b_val),
                    SortDirection::Descending => compare_values_ordering(&b_val, &a_val),
                };
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Execute the aggregation step. Splits naturally into three phases:
    /// build group key, accumulate per-aggregate state, then finalize each
    /// group into a result row.
    async fn execute_aggregation(
        &self,
        rows: Vec<QueryRow>,
        agg_plan: &AggregationPlan,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const PER_ROW_MEMORY_ESTIMATE_BYTES: usize = 100;
        const DEFAULT_AGGREGATION_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

        let mut agg_state = AggregationState {
            groups: Vec::new(),
            memory_usage_bytes: 0,
            memory_limit_bytes: DEFAULT_AGGREGATION_MEMORY_LIMIT,
        };

        for row in rows {
            let group_key = build_group_key(&row, &agg_plan.group_by_columns);
            let group_index =
                find_or_init_group(&mut agg_state.groups, group_key, &agg_plan.aggregates);
            let group_aggregates = &mut agg_state.groups[group_index].1;

            for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                update_aggregate(&mut group_aggregates[i], agg_comp, &row);
            }

            agg_state.memory_usage_bytes += PER_ROW_MEMORY_ESTIMATE_BYTES;
            if agg_state.memory_usage_bytes > agg_state.memory_limit_bytes {
                return Err(Error::query_execution(
                    "Aggregation memory limit exceeded".to_string(),
                ));
            }
        }

        let result_rows = agg_state
            .groups
            .into_iter()
            .map(|(group_key, group_aggregates)| {
                finalize_group(group_key, group_aggregates, agg_plan)
            })
            .collect();

        Ok(result_rows)
    }

    /// Execute PER PARTITION LIMIT: keep at most `count` rows per partition,
    /// preserving order (Issue #757). Counts are keyed on the partition (raw key
    /// bytes) rather than tracking only the most recent partition, so the cap
    /// holds even when a partition's rows are not contiguous — e.g. when an
    /// upstream `ORDER BY` interleaves rows from different partitions (roborev
    /// job 38).
    fn execute_per_partition_limit(rows: Vec<QueryRow>, count: u64) -> Vec<QueryRow> {
        let mut out = Vec::with_capacity(rows.len());
        let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
        for row in rows {
            let seen = counts.entry(row.key.0.clone()).or_insert(0);
            if *seen < count {
                *seen += 1;
                out.push(row);
            }
        }
        out
    }

    /// Execute limit step (apply OFFSET then truncate to LIMIT).
    async fn execute_limit(
        &self,
        mut rows: Vec<QueryRow>,
        count: u64,
        offset: Option<u64>,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let start_index = offset.unwrap_or(0) as usize;
        if start_index >= rows.len() {
            return Ok(Vec::new());
        }
        rows.drain(..start_index);
        rows.truncate(count as usize);
        Ok(rows)
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
                // Issue #692: WriteTimeTtl expressions use Cassandra-convention column names.
                let column_name = match expr {
                    SelectExpression::Column(col_ref) => col_ref.column.clone(),
                    SelectExpression::Aliased(_, alias) => alias.clone(),
                    SelectExpression::WriteTimeTtl(call) => writetime_ttl_column_name(call),
                    _ => format!("col_{i}"),
                };
                projected_values.insert(column_name, value);
            }

            projected_rows.push(QueryRow {
                values: projected_values,
                key: RowKey::new(vec![]),
                metadata: Default::default(),
                cell_metadata: None,
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
                        data_type: crate::types::DataType::Text, // Constant expressions have no schema type
                        nullable: true,
                        position: i,
                        table_name: None, // No table for constant expressions
                        cql_type: None,
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
                // Constant queries (e.g. `SELECT 1`) touch no SSTable.
                access_path: None,
            },
        })
    }

    /// Evaluate a constant expression (no table access needed).
    ///
    /// Accepts literals, aliases, and arithmetic over same-typed Integer,
    /// BigInt, or Float operands. Modulo is restricted to integers (matching
    /// the original behaviour). Error messages are kept verbatim from the
    /// legacy implementation so any callers asserting on them still pass.
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
                let (left_val, _) = self.evaluate_constant_expression(&arith.left)?;
                let (right_val, _) = self.evaluate_constant_expression(&arith.right)?;
                let result = const_arithmetic(&arith.operator, left_val, right_val)?;
                Ok((result, None))
            }
            _ => Err(Error::query_execution(
                "Expression type not supported in constant queries".to_string(),
            )),
        }
    }

    /// Extract a `TableId` from a FROM clause. Cassandra CQL has no JOINs, so
    /// either form (bare table or aliased table) yields the same result.
    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            }
        }
    }

    async fn get_result_columns(&self, statement: &SelectStatement) -> Result<Vec<ColumnInfo>> {
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                // For SELECT *, look up the schema to get column names and CQL types.
                // This is needed for streaming mode where we can't wait for the first row.
                if let Some(ref from_clause) = statement.from_clause {
                    let table_id = self.extract_table_id(from_clause)?;
                    let (keyspace_opt, table_name) = parse_table_id(&table_id);

                    // Look up schema from SchemaManager
                    if let Some(schema) = self
                        ._schema
                        .find_schema_by_table(&keyspace_opt, &table_name)
                        .await
                    {
                        // Collect all schema columns (sorted alphabetically for determinism)
                        let mut schema_cols: Vec<&crate::schema::Column> =
                            schema.columns.iter().collect();
                        schema_cols.sort_by_key(|c| c.name.as_str());

                        let keyspace_str = keyspace_opt.as_deref().unwrap_or("");
                        let table_name_str = format!("{}.{}", keyspace_str, table_name);

                        for (idx, schema_col) in schema_cols.iter().enumerate() {
                            columns.push(column_info_from_type_str(
                                schema_col.name.clone(),
                                &schema_col.data_type,
                                idx,
                                Some(table_name_str.clone()),
                            ));
                        }

                        log::debug!(
                            "SELECT * resolved {} columns from schema for {:?}.{}",
                            columns.len(),
                            keyspace_opt,
                            table_name
                        );
                    }
                    // If schema not found, columns stay empty - will be populated from first row at runtime
                }
            }
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                // Try to resolve a schema for the FROM table (if present) so we can
                // attach authoritative CQL types to explicitly projected columns (Issue #674).
                let schema_opt = if let Some(ref from_clause) = statement.from_clause {
                    if let Ok(table_id) = self.extract_table_id(from_clause) {
                        let (keyspace_opt, table_name) = parse_table_id(&table_id);
                        self._schema
                            .find_schema_by_table(&keyspace_opt, &table_name)
                            .await
                    } else {
                        None
                    }
                } else {
                    None
                };

                for (i, expr) in exprs.iter().enumerate() {
                    // Issue #692: WriteTimeTtl expressions produce fixed-schema output
                    // columns with Cassandra-convention names, independent of the table schema.
                    if let SelectExpression::WriteTimeTtl(call) = expr {
                        let col_name = writetime_ttl_column_name(call);
                        let (data_type, cql_type) = match call.function {
                            // WRITETIME returns bigint (µs since epoch)
                            WriteTimeTtlFunction::WriteTime => {
                                (crate::types::DataType::BigInt, Some(CqlType::BigInt))
                            }
                            // TTL returns int (remaining seconds)
                            WriteTimeTtlFunction::Ttl => {
                                (crate::types::DataType::Integer, Some(CqlType::Int))
                            }
                        };
                        let mut col_info = ColumnInfo {
                            name: col_name,
                            data_type,
                            nullable: true, // always nullable — absent cell → NULL
                            position: i,
                            table_name: None,
                            cql_type: None,
                        };
                        if let Some(ct) = cql_type {
                            col_info = col_info.with_cql_type(ct);
                        }
                        columns.push(col_info);
                        continue;
                    }

                    let column_name = match expr {
                        SelectExpression::Column(col_ref) => col_ref.column.clone(),
                        SelectExpression::Aliased(_, alias) => alias.clone(),
                        _ => format!("col_{i}"),
                    };

                    // Look up CQL type for this column in the schema (Issue #674).
                    let cql_type_opt = schema_opt.as_ref().and_then(|schema| {
                        schema
                            .columns
                            .iter()
                            .find(|c| c.name == column_name)
                            .and_then(|c| parse_cql_type_str(&c.data_type))
                    });
                    let data_type = cql_type_opt
                        .as_ref()
                        .map(cql_type_to_data_type)
                        .unwrap_or(crate::types::DataType::Text);

                    let mut col_info = ColumnInfo {
                        name: column_name,
                        data_type,
                        nullable: true,
                        position: i,
                        table_name: None,
                        cql_type: None,
                    };
                    if let Some(cql_type) = cql_type_opt {
                        col_info = col_info.with_cql_type(cql_type);
                    }
                    columns.push(col_info);
                }
            }
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::row_with_key;
    use super::*;
    use crate::query::result::{CellExpiration, CellWriteMetadata};
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
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor::new(schema, storage)
    }

    /// Create an executor with a fixed clock (deterministic TTL tests).
    async fn create_test_executor_with_clock(now_secs: i64) -> SelectExecutor {
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
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor::with_clock(schema, storage, Arc::new(FixedClock(now_secs)))
    }

    /// Helper: build a QueryRow with a given column value and optional cell metadata.
    fn row_with_cell_meta(column: &str, value: Value, meta: Option<CellWriteMetadata>) -> QueryRow {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set(column.to_string(), value);
        if let Some(m) = meta {
            row.insert_cell_metadata(column.to_string(), m);
        }
        row
    }

    #[tokio::test]
    async fn test_like_pattern_matching() {
        let executor = create_test_executor().await;

        assert!(executor.match_like_pattern("hello", "h%"));
        assert!(executor.match_like_pattern("hello", "%lo"));
        assert!(executor.match_like_pattern("hello", "h_llo"));
        assert!(!executor.match_like_pattern("hello", "h_l"));
    }

    /// Regression (roborev job 38): in the batch path PER PARTITION LIMIT must
    /// cap per partition even when a partition's rows are NOT contiguous (e.g.
    /// after ORDER BY interleaves them). Counting must key on the partition, not
    /// just track the most recent one.
    #[test]
    fn per_partition_limit_caps_interleaved_partitions() {
        let a = b"A".as_slice();
        let b = b"B".as_slice();
        // Partition A appears 3 times but is split by a B row in the middle.
        let rows = vec![
            row_with_key(a),
            row_with_key(b),
            row_with_key(a),
            row_with_key(a),
            row_with_key(b),
        ];
        let out = SelectExecutor::execute_per_partition_limit(rows, 2);
        let count = |p: &[u8]| out.iter().filter(|r| r.key.0 == p).count();
        assert_eq!(
            count(a),
            2,
            "partition A must be capped at 2 despite interleaving"
        );
        assert_eq!(count(b), 2, "partition B has 2 rows, all kept");
        assert_eq!(out.len(), 4);
    }

    /// The executor's `evaluate_select_expression` returns the correct value for
    /// a WRITETIME call when cell metadata is pre-attached to the row.
    #[tokio::test]
    async fn test_executor_evaluate_writetime_reads_cell_metadata() {
        let executor = create_test_executor_with_clock(0).await;

        let write_ts = 1_700_000_000_000_000_i64;
        let row = row_with_cell_meta(
            "name",
            Value::Text("Carol".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: write_ts,
                expiration: None,
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::BigInt(write_ts));
    }

    /// The executor's `evaluate_select_expression` returns NULL for WRITETIME
    /// when cell metadata is absent (the common case before the storage reader
    /// is updated to thread metadata).
    #[tokio::test]
    async fn test_executor_evaluate_writetime_null_when_no_metadata() {
        let executor = create_test_executor_with_clock(0).await;

        // Row has the column value but no attached cell metadata.
        let row = row_with_cell_meta("name", Value::Text("Dave".to_string()), None);

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::Null);
    }

    /// The executor returns correct TTL using the injected fixed clock.
    #[tokio::test]
    async fn test_executor_evaluate_ttl_with_injected_clock() {
        // now = epoch 1000; cell expires at epoch 5000 → remaining = 4000s
        let now_secs: i64 = 1000;
        let executor = create_test_executor_with_clock(now_secs).await;

        let row = row_with_cell_meta(
            "session",
            Value::Text("tok".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 5000,
                    expires_at_seconds: 5000,
                }),
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "session".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(
            result,
            Value::Integer(4000),
            "TTL must use the injected clock, not the wall clock"
        );
    }

    /// Expired cell: executor returns NULL via injected clock.
    #[tokio::test]
    async fn test_executor_evaluate_ttl_expired_cell_returns_null() {
        // now = epoch 9999; cell expired at epoch 100 → NULL
        let executor = create_test_executor_with_clock(9999).await;

        let row = row_with_cell_meta(
            "cache",
            Value::Text("val".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 100,
                    expires_at_seconds: 100,
                }),
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "cache".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::Null, "Expired TTL cell must produce NULL");
    }

    /// Column info for WRITETIME uses BigInt data type and bigint cql_type.
    #[tokio::test]
    async fn test_get_result_columns_writetime_has_bigint_type() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: None,
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "writetime(name)");
        assert_eq!(cols[0].data_type, crate::types::DataType::BigInt);
        assert!(cols[0].nullable, "WRITETIME column must be nullable");
        assert_eq!(cols[0].cql_type, Some(CqlType::BigInt));
    }

    /// Column info for TTL uses Integer data type and int cql_type.
    #[tokio::test]
    async fn test_get_result_columns_ttl_has_int_type() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::Ttl,
                    column: "score".to_string(),
                    alias: None,
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "ttl(score)");
        assert_eq!(cols[0].data_type, crate::types::DataType::Integer);
        assert!(cols[0].nullable, "TTL column must be nullable");
        assert_eq!(cols[0].cql_type, Some(CqlType::Int));
    }

    /// Column name uses alias when provided, overriding convention.
    #[tokio::test]
    async fn test_get_result_columns_writetime_with_alias() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: Some("wt".to_string()),
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].name, "wt",
            "Alias must override Cassandra convention"
        );
    }
}
