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
//! - [`execute`] — the three large `async` pipeline methods (`execute`,
//!   `execute_streaming_background`, `execute_sstable_scan`) as an `impl`
//!   continuation (issue #1174),
//! - this `mod.rs` — the [`SelectExecutor`] orchestration and remaining
//!   per-step helpers.
//!
//! ## Lint policy (issue #1590, E8)
//!
//! The read-path pipeline step helpers below are pure/synchronous — none awaits.
//! Deny `clippy::unused_async` for this module (and its submodules) so a future
//! edit cannot silently reintroduce the future/state-machine overhead of an
//! `async fn` that never awaits (which also infects every caller with an
//! `.await`). A crate-level deny is deliberately NOT used: `cqlite-core` still
//! has many legitimately-flagged `async fn`s on its public surface (out of E8's
//! scope), so the guard is scoped to the pipeline it protects.
#![deny(clippy::unused_async)]

mod aggregation;
mod execute;
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
    schema::{CqlType, SchemaManager, TableSchema},
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

// Public surface re-exports (kept identical to the pre-split module so
// `query::mod`'s `pub use select_executor::{...}` resolves unchanged).
pub use predicate::{evaluate_leaf, evaluate_predicates, LeafOutcome};
pub use row_build::build_row_from_scan;
pub use writetime_ttl::{FixedClock, NowSeconds};

// `validate_token_predicates` is used by the executor's scan paths.
use predicate::validate_token_predicates;

// Test-only counter for the number of times a projected column NAME is derived
// (issue #1584). A thread-local `Cell` (not a global atomic) so parallel test
// binaries cannot pollute one another; `#[tokio::test]` uses the current-thread
// runtime, so the future under test runs on the same thread that reads it.
#[cfg(test)]
thread_local! {
    pub(crate) static PROJECTION_NAME_DERIVATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

// Test-only counter for the number of times an ORDER BY sort-key expression is
// evaluated (issue #1587, E5). With decorate-sort-undecorate this is exactly
// `rows × order_by.items` (evaluated ONCE per row up front), never the
// `O(n log n)` per-comparison evaluation the old comparator incurred. Same
// thread-local rationale as `PROJECTION_NAME_DERIVATIONS`.
#[cfg(test)]
thread_local! {
    pub(crate) static SORT_KEY_EVALUATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// 128-bit digest of a partition key's raw bytes for PER PARTITION LIMIT
/// bookkeeping (issue #1590, E8).
///
/// PER PARTITION LIMIT keys its per-partition counters on the partition. Keying
/// a map (or an adjacent-boundary compare) on the raw `Vec<u8>` clones the key
/// bytes for every row; using this fixed-size `u128` digest as the FAST outer
/// lookup key makes the per-row work a heap-free hash of the bytes we already
/// hold. The digest is Cassandra's own `murmur3_x64_128` (already used for token
/// routing), so it is stable and well-distributed.
///
/// The digest is only a pre-check: both the batch counter map and the streaming
/// boundary confirm an EXACT match on the raw partition-key bytes (stored ONCE
/// per distinct partition, never cloned per row) before sharing a counter. So
/// correctness does NOT depend on collision absence — two distinct partitions
/// whose digests collide get separate counters and no valid row is dropped.
pub(super) fn partition_key_digest(key_bytes: &[u8]) -> u128 {
    let (h1, h2) = crate::util::cassandra_murmur3::cassandra_murmur3_x64_128(key_bytes);
    ((h1 as u64 as u128) << 64) | (h2 as u64 as u128)
}

/// PER PARTITION LIMIT counter map for the batch path (issue #1590, E8).
///
/// The outer key is the fast 128-bit [`partition_key_digest`]; each bucket is a
/// chain of `(raw_key_bytes, count)` entries disambiguated by EXACT byte
/// equality. The chain is near-always length 1 — it grows only on a genuine
/// digest collision between two DISTINCT partition keys — so the fast path is an
/// `O(1)` digest hash plus a single byte compare, yet correctness never depends
/// on collision absence.
type PartitionCounts = HashMap<u128, Vec<(Vec<u8>, u64)>>;

/// Admit one row under PER PARTITION LIMIT `count`, returning `true` when the
/// row is within its partition's cap (and incrementing that partition's count).
///
/// `digest` is the fast outer lookup key; the raw `key_bytes` confirm the exact
/// partition so a (vanishingly rare) digest collision between two distinct keys
/// never shares a counter. The key's bytes are cloned at most ONCE — on the row
/// that first opens the partition's counter — never per row.
fn admit_partition_row(
    counts: &mut PartitionCounts,
    digest: u128,
    key_bytes: &[u8],
    count: u64,
) -> bool {
    let chain = counts.entry(digest).or_default();
    let slot = match chain
        .iter()
        .position(|(bytes, _)| bytes.as_slice() == key_bytes)
    {
        Some(i) => &mut chain[i],
        None => {
            // The ONLY key-byte clone: once per distinct partition, not per row.
            chain.push((key_bytes.to_vec(), 0));
            let last = chain.len() - 1;
            &mut chain[last]
        }
    };
    if slot.1 < count {
        slot.1 += 1;
        true
    } else {
        false
    }
}

/// Derive the output column name for a projected SELECT expression (issue #1584).
///
/// Hoisted out of the per-row loop and called ONCE per column per query, so name
/// derivation is `O(columns)` rather than `O(rows × columns)`. The produced
/// `Arc<str>` is `clone`d (a ref-count bump) into every row, preserving the exact
/// output name — the materialized rows are byte-identical to the prior per-row
/// `String` derivation.
fn projected_column_name(expr: &SelectExpression, index: usize) -> std::sync::Arc<str> {
    #[cfg(test)]
    PROJECTION_NAME_DERIVATIONS.with(|c| c.set(c.get() + 1));
    match expr {
        // Issue #692: WriteTimeTtl expressions use Cassandra-convention names.
        SelectExpression::Column(col_ref) => col_ref.column.as_str().into(),
        SelectExpression::Aliased(_, alias) => alias.as_str().into(),
        SelectExpression::WriteTimeTtl(call) => writetime_ttl_column_name(call).into(),
        _ => format!("col_{index}").into(),
    }
}

/// True when a `Project` expression RENAMES or EVALUATES its input rather than
/// passing a stored column through unchanged (issue #1763).
///
/// A plain `Column` is emitted by the SSTable scan under the same name a
/// `Project` would re-key it to, so streaming it directly matches the query
/// metadata. Anything else — an alias (`category AS cat`), an aggregate, an
/// arithmetic/function expression, or a literal — reshapes or renames the row,
/// so the streaming path must materialize (fall back to execute-then-stream) to
/// apply the `Project` and keep row value keys equal to the metadata names.
fn project_expr_reshapes_row(expr: &SelectExpression) -> bool {
    !matches!(expr, SelectExpression::Column(_))
}

/// True when a plain-column `Project`'s OUTPUT column SET differs from the
/// SSTable scan projection's column SET — i.e. the `Project` must TRIM helper
/// columns the broadened scan added (issue #1952 streaming follow-up).
///
/// #1952 widened the scan projection to the UNION of the selected columns and
/// the WHERE / ORDER BY / GROUP BY / aggregate-argument helper columns, relying
/// on the `Project` step to trim the non-selected helpers back out. The
/// materialized path runs `Project`; the streaming producer
/// (`execute_streaming_background`) IGNORES a plain-column `Project`, so without
/// this check it would emit rows still carrying the helper columns
/// (e.g. `SELECT a WHERE b = 1` scans `["a","b"]` and would leak `b`).
///
/// Column ORDER is intentionally irrelevant: streamed rows are keyed by name in
/// a `HashMap`, and output ordering is carried by the query metadata, so only
/// the SET matters — a `Project` that merely reorders (same set) can still
/// stream directly with no perf regression. When there is no scan step
/// (e.g. a constant query) there is nothing to trim.
fn project_trims_scan_columns(
    columns: &[SelectExpression],
    scan_projection: Option<&[String]>,
) -> bool {
    let Some(scan) = scan_projection else {
        return false;
    };
    let output: std::collections::HashSet<&str> = columns
        .iter()
        .filter_map(|e| match e {
            SelectExpression::Column(c) => Some(c.column.as_str()),
            _ => None,
        })
        .collect();
    let scan_set: std::collections::HashSet<&str> = scan.iter().map(|s| s.as_str()).collect();
    output != scan_set
}

/// Default row-count safety valve for the bare `SelectExecutor` constructors
/// (issue #1582). Matches [`crate::config::QueryConfig::max_result_rows`]'s
/// default; the query engine overrides it via
/// [`SelectExecutor::with_max_result_rows`].
const DEFAULT_MAX_RESULT_ROWS: usize = 1_000_000;

/// SELECT query executor for SSTable-based storage
pub struct SelectExecutor {
    /// Schema manager for metadata
    _schema: Arc<SchemaManager>,
    /// Storage engine for SSTable access
    storage: Arc<StorageEngine>,
    /// Clock used for TTL "remaining seconds" computation (injectable for tests).
    clock: Arc<dyn NowSeconds>,
    /// Byte ceiling on a materialized result set (issue #1582 / D6).
    ///
    /// The materializing scan path accumulates a running estimate of the result
    /// bytes and fails with [`Error::ResultTooLarge`] once this is exceeded.
    /// Wired from [`crate::config::QueryConfig::max_result_bytes`] by the query
    /// engine; defaults to [`crate::config::DEFAULT_MAX_RESULT_BYTES`] for the
    /// bare constructors. Streaming queries are bounded by their channel buffer
    /// and do not consult this budget.
    max_result_bytes: usize,
    /// Row-count safety valve on a materialized result set (issue #1582).
    ///
    /// Secondary to `max_result_bytes` (bytes are the correct memory unit), but
    /// still load-bearing: the materializing scan fails once the collected row
    /// count exceeds this valve. Wired from
    /// [`crate::config::QueryConfig::max_result_rows`] by the query engine;
    /// defaults to 1,000,000 for the bare constructors.
    max_result_rows: usize,
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
    /// Per-query state, set where the scan step decides its path. The
    /// result-attached `QueryMetadata.access_path` is read from here, NOT from
    /// the process-global probe, so concurrent SELECTs cannot overwrite each
    /// other's reported path between `record()` and the result build. The global
    /// probe (`access_path::record/last`) remains for test assertions only.
    pub access_path: Option<AccessPath>,
    pub reverse_served: bool, // #1184: BIG reverse iterator produced DESC order; skip Sort.
}

impl SelectExecutor {
    /// Create a new SELECT executor with a system (wall-clock) now source.
    ///
    /// Uses the default materialized-result byte budget
    /// ([`crate::config::DEFAULT_MAX_RESULT_BYTES`]); call
    /// [`SelectExecutor::with_max_result_bytes`] to override it (the query
    /// engine wires it from [`crate::config::QueryConfig::max_result_bytes`]).
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self {
            _schema: schema,
            storage,
            clock: Arc::new(SystemClock),
            max_result_bytes: usize::try_from(crate::config::DEFAULT_MAX_RESULT_BYTES)
                .unwrap_or(usize::MAX),
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        }
    }

    /// Override the byte ceiling on a materialized result set (issue #1582).
    ///
    /// Builder-style: the query engine calls this with
    /// [`crate::config::QueryConfig::max_result_bytes`] so the config knob is
    /// load-bearing on the read path.
    pub fn with_max_result_bytes(mut self, max_result_bytes: usize) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    /// Override the row-count safety valve on a materialized result set (issue
    /// #1582).
    ///
    /// Builder-style: the query engine calls this with
    /// [`crate::config::QueryConfig::max_result_rows`] so the config knob is
    /// load-bearing on the read path (not a hardcoded constant).
    pub fn with_max_result_rows(mut self, max_result_rows: usize) -> Self {
        self.max_result_rows = max_result_rows;
        self
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
            max_result_bytes: usize::try_from(crate::config::DEFAULT_MAX_RESULT_BYTES)
                .unwrap_or(usize::MAX),
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
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

        // Issue #1587 (E5): resolve the schema ONCE for the whole streaming query
        // and share it (by reference here, and moved into the spawned task below)
        // so column-metadata building, the pre-spawn token validation, and the
        // background scan all reuse one `Arc<TableSchema>` instead of each
        // re-locking the registry and deep-cloning a fresh schema (2–4× per query).
        let query_schema: Option<Arc<TableSchema>> = self.resolve_table_schema(&table_id).await;

        let columns = self.get_result_columns(&plan.statement, query_schema.as_deref())?;

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
        // materializing `execute()` path. Uses the already-resolved schema.
        for step in &execution_steps {
            if let ExecutionStep::SSTableScan { predicates, .. } = step {
                validate_token_predicates(predicates, query_schema.as_deref())?;
            }
        }

        // Clone what we need for the background task.
        let storage = Arc::clone(&self.storage);
        let buffer_size = config.buffer_size;

        // Spawn background task to stream rows
        tokio::spawn(async move {
            if let Err(e) = Self::execute_streaming_background(
                storage,
                query_schema,
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
        // The SSTable scan projection = the source columns the scan keeps in each
        // streamed row. A `Project` step whose output column SET differs from this
        // scan set must TRIM helper columns (issue #1952), which the streaming
        // producer cannot do — so such a plan must materialize.
        let scan_projection: Option<&[String]> =
            plan.execution_steps.iter().find_map(|step| match step {
                ExecutionStep::SSTableScan { projection, .. } => Some(projection.as_slice()),
                _ => None,
            });

        for step in &plan.execution_steps {
            match step {
                ExecutionStep::Sort { .. } => return true,
                ExecutionStep::Aggregate { .. } => return true,
                // The streaming producer (`execute_streaming_background`) IGNORES
                // `Project`, so a `Project` step that changes the row's column SET
                // or SHAPE must fall back to the materialized execute()-then-stream
                // path (which applies the `Project`). Two disjoint cases:
                //
                //   * Issue #1763 — a RENAMING (`category AS cat`) or EVALUATING
                //     (aggregate / arithmetic / literal / function) item reshapes
                //     the row: streaming would key it by the SOURCE stored column
                //     name, disagreeing with the SELECT-output query metadata.
                //   * Issue #1952 (this follow-up) — a plain-column `Project` whose
                //     OUTPUT set ≠ the scan projection set TRIMS the WHERE / ORDER
                //     BY / GROUP BY / aggregate-arg helper columns the broadened
                //     scan added; streaming would LEAK those helper columns.
                //
                // A `Project` of exactly the scan's plain columns (no reshape, no
                // trim) keys each cell by the same name and can stream directly.
                ExecutionStep::Project { columns }
                    if columns.iter().any(project_expr_reshapes_row)
                        || project_trims_scan_columns(columns, scan_projection) =>
                {
                    return true
                }
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

    // `execute_streaming_background` and `execute_sstable_scan` live in the
    // `execute` submodule alongside `execute` (issue #1174).

    /// Execute filtering step
    fn execute_filter(
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
            SelectExpression::Column(col_ref) => row
                .values
                .get(col_ref.column.as_str())
                .cloned()
                .ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                }),
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
                .get(col.column.as_str())
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
    fn execute_sort(
        &self,
        mut rows: Vec<QueryRow>,
        order_by: &OrderByClause,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // Issue #1587 (E5): decorate-sort-undecorate. Evaluate each row's ORDER
        // BY key expressions ONCE (O(rows × items) total), then sort by the
        // precomputed keys. The comparator itself performs NO
        // `evaluate_select_expression` and NO `Value` clones — it only compares
        // already-materialized keys. This drops the O(n log n) per-comparison
        // expression evaluation + `Value::clone` the previous comparator
        // incurred. The ordering is byte-identical: the same
        // `compare_values_ordering` runs on the same per-row values, in the same
        // ascending/descending sense, item by item, and `sort_by` remains a
        // stable sort (ties keep input order).
        let mut decorated: Vec<(Vec<Value>, QueryRow)> = Vec::with_capacity(rows.len());
        for row in rows.drain(..) {
            let mut keys = Vec::with_capacity(order_by.items.len());
            for item in &order_by.items {
                #[cfg(test)]
                SORT_KEY_EVALUATIONS.with(|c| c.set(c.get() + 1));
                keys.push(
                    self.evaluate_select_expression(&item.expression, &row)
                        .unwrap_or(Value::Null),
                );
            }
            decorated.push((keys, row));
        }

        decorated.sort_by(|(a_keys, _), (b_keys, _)| {
            for (idx, item) in order_by.items.iter().enumerate() {
                let ordering = match item.direction {
                    SortDirection::Ascending => compare_values_ordering(&a_keys[idx], &b_keys[idx]),
                    SortDirection::Descending => {
                        compare_values_ordering(&b_keys[idx], &a_keys[idx])
                    }
                };
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(decorated.into_iter().map(|(_, row)| row).collect())
    }

    /// Execute the aggregation step. Splits naturally into three phases:
    /// build group key, accumulate per-aggregate state, then finalize each
    /// group into a result row.
    fn execute_aggregation(
        &self,
        rows: Vec<QueryRow>,
        agg_plan: &AggregationPlan,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const PER_ROW_MEMORY_ESTIMATE_BYTES: usize = 100;
        const DEFAULT_AGGREGATION_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

        // Issue #1578 (D2): record how many rows this buffered aggregate input
        // carried, so the O(1) memory guard can prove a GROUP-BY-free aggregate
        // served by the streaming fold buffers ZERO rows here (the fold never
        // calls this method). Zero-overhead in release (probe body is cfg-gated).
        crate::query::agg_stream_probe::record_buffered_rows(rows.len() as u64);

        let mut agg_state = AggregationState {
            groups: Vec::new(),
            group_index: rustc_hash::FxHashMap::default(),
            memory_usage_bytes: 0,
            memory_limit_bytes: DEFAULT_AGGREGATION_MEMORY_LIMIT,
        };

        for row in rows {
            let group_key = build_group_key(&row, &agg_plan.group_by_columns);
            let group_index = find_or_init_group(&mut agg_state, group_key, &agg_plan.aggregates);
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
    /// preserving order (Issue #757). Counts are keyed on the partition rather
    /// than tracking only the most recent partition, so the cap holds even when a
    /// partition's rows are not contiguous — e.g. when an upstream `ORDER BY`
    /// interleaves rows from different partitions (roborev job 38).
    ///
    /// Issue #1590 (E8): the counter map uses the partition-key 128-bit
    /// [`partition_key_digest`] as a FAST outer lookup key, then confirms an
    /// EXACT match on the raw key bytes (via [`admit_partition_row`]). No key
    /// bytes are cloned per row — a partition's bytes are stored at most ONCE,
    /// on the row that first opens its counter — and correctness stays
    /// independent of digest collisions.
    fn execute_per_partition_limit(rows: Vec<QueryRow>, count: u64) -> Vec<QueryRow> {
        let mut out = Vec::with_capacity(rows.len());
        let mut counts: PartitionCounts = HashMap::new();
        for row in rows {
            let digest = partition_key_digest(&row.key.0);
            if admit_partition_row(&mut counts, digest, &row.key.0, count) {
                out.push(row);
            }
        }
        out
    }

    /// Execute limit step (apply OFFSET then truncate to LIMIT).
    fn execute_limit(
        &self,
        mut rows: Vec<QueryRow>,
        count: u64,
        offset: Option<u64>,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let start_index = offset.unwrap_or(0) as usize;
        let limit = count as usize;
        // Issue #1590 (E8): with no OFFSET, truncate in place — no allocation and
        // no element shift. With an OFFSET, apply it via `skip`/`take` instead of
        // `drain(..offset)`: `drain` memmoves every surviving element left, while
        // `skip` drops the prefix and yields the survivors without shifting. The
        // resulting rows are identical (same slice, same order); `skip` past the
        // end yields nothing, matching the old `start_index >= len` early return.
        if start_index == 0 {
            rows.truncate(limit);
            return Ok(rows);
        }
        Ok(rows.into_iter().skip(start_index).take(limit).collect())
    }

    /// Execute projection step
    fn execute_projection(
        &self,
        rows: Vec<QueryRow>,
        columns: &[SelectExpression],
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // Issue #1584: derive the projected output column names ONCE per query
        // (`O(columns)`), then only `Arc`-clone them into each row's value map
        // (a ref-count bump). Previously each name was re-derived — `String`
        // clone / `format!` — for every row × column (`O(rows × columns)`).
        // Cloning the hoisted `Arc<str>` preserves the exact output name, so the
        // materialized rows are byte-identical to the prior per-row derivation.
        let column_names: Vec<std::sync::Arc<str>> = columns
            .iter()
            .enumerate()
            .map(|(i, expr)| projected_column_name(expr, i))
            .collect();

        let mut projected_rows = Vec::with_capacity(rows.len());

        for row in rows {
            // Issue #1584: one sized allocation per row — no rehash growth.
            let mut projected_values: HashMap<std::sync::Arc<str>, Value> =
                HashMap::with_capacity(columns.len());

            for (i, expr) in columns.iter().enumerate() {
                let value = self.evaluate_select_expression(expr, &row)?;
                projected_values.insert(column_names[i].clone(), value);
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

    /// Trim a plain-column `Project` down to its selected columns WITHOUT
    /// reshaping the row (issue #1952 round-6 fix).
    ///
    /// The Project step's ONLY job for a plain-column SELECT (every expr is a
    /// bare `Column`) is to drop the helper columns the #1952 scan-widening added
    /// (WHERE / ORDER BY / GROUP BY / aggregate-argument columns that are not in
    /// the SELECT clause). It is NOT a reshape: a plain column is identity-named,
    /// so there are no new names to derive (#1763's alias/reshape naming does not
    /// apply) and no computed values to build.
    ///
    /// Unlike [`Self::execute_projection`] — which is correct for reshaping /
    /// computed rows but rebuilds each row with an EMPTY `RowKey` and errors on a
    /// selected cell that is absent from a sparse row — this preserves each row's
    /// real `key` (partition/clustering key), `metadata`, and `cell_metadata`
    /// unchanged, and simply omits any selected cell that a sparse row lacks
    /// (never an error). Preserving the key is the #1587-class contract downstream
    /// consumers (per-partition-limit boundary detection, dedup, ordering, callers
    /// inspecting `row.key`) rely on; before #1952 these bare-column selects
    /// skipped the Project entirely and streamed the RAW keyed row.
    ///
    /// Rows are name-keyed maps and output column ORDER is carried by the query
    /// metadata, so trimming = retaining the correct SET of named cells in place.
    fn trim_projection(
        &self,
        mut rows: Vec<QueryRow>,
        columns: &[SelectExpression],
    ) -> Vec<QueryRow> {
        // The selected plain-column source names. This method is only reached
        // when every projection expr is a plain `Column` (the caller gates on
        // `!columns.iter().any(project_expr_reshapes_row)`), so every expr
        // contributes its source name and none reshapes.
        let selected: std::collections::HashSet<&str> = columns
            .iter()
            .filter_map(|e| match e {
                SelectExpression::Column(c) => Some(c.column.as_str()),
                _ => None,
            })
            .collect();

        for row in &mut rows {
            // Retain only the selected cells by source-name identity; key,
            // metadata, and cell_metadata are left untouched. A selected cell
            // absent from a sparse row is simply not present after the retain —
            // never an error.
            row.values
                .retain(|name, _| selected.contains(name.as_ref()));
        }
        rows
    }

    /// Execute a query without FROM clause (constant expressions like SELECT 1)
    fn execute_constant_query(
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

    /// Resolve a table's schema ONCE per query into a shared `Arc<TableSchema>`
    /// (issue #1587, E5). Every downstream planning/execution step then borrows
    /// the SAME schema (`Arc::clone` is a ref-count bump) instead of re-taking the
    /// registry lock and deep-cloning a fresh `TableSchema` per step (which was
    /// 2–4 deep clones per query). The registry itself is untouched (AJ1/AJ2 own
    /// its shape) — this is purely query-side de-duplication.
    async fn resolve_table_schema(&self, table: &TableId) -> Option<Arc<TableSchema>> {
        let (keyspace, table_name) = parse_table_id(table);
        // The registry owns freshness (issue #1708): an expired entry that cannot
        // be refreshed surfaces as `Err`; schema resolution here is best-effort
        // (missing/unresolvable schema falls back to row-derived columns), so a
        // refresh error folds to `None` rather than aborting the query.
        self._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await
            .ok()
            .flatten()
            .map(Arc::new)
    }

    /// Build result-column metadata from an ALREADY-RESOLVED schema (issue #1587,
    /// E5). The caller resolves the schema once per query and passes it here, so
    /// this never re-clones it out of the registry.
    fn get_result_columns(
        &self,
        statement: &SelectStatement,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<ColumnInfo>> {
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                // For SELECT *, use the schema to get column names and CQL types.
                // This is needed for streaming mode where we can't wait for the first row.
                if let Some(ref from_clause) = statement.from_clause {
                    let table_id = self.extract_table_id(from_clause)?;
                    let (keyspace_opt, table_name) = parse_table_id(&table_id);

                    if let Some(schema) = schema {
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
                // Issue #674: attach authoritative CQL types to explicitly projected
                // columns from the pre-resolved schema.
                let schema_opt = schema;

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

                    // Issue #1763: name aggregate result columns via the SAME
                    // single source (`result_column_name` → `aggregate_output_name`)
                    // that keys the emitted row values in `finalize_group`, so
                    // metadata and row keys can never diverge (never `col_N`).
                    let column_name = crate::query::select_naming::result_column_name(expr, i);

                    // Look up CQL type for this column in the schema (Issue #674).
                    let cql_type_opt = schema_opt.and_then(|schema| {
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

    /// Issue #1587 (E5): a query resolves its table schema ONCE and shares it by
    /// `Arc`, so a single `execute()` deep-clones the `TableSchema` out of the
    /// registry exactly once — NOT once per planning/execution step (column
    /// metadata + scan + fallback each re-resolved before, giving 2–4 clones).
    /// Runs over empty storage so the count is deterministic and data-free: the
    /// SELECT-* column metadata AND the scan step both consume the pre-resolved
    /// schema.
    #[tokio::test]
    async fn execute_resolves_schema_once_per_query() {
        use crate::schema::TABLE_SCHEMA_CLONES;

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
        schema
            .parse_and_register_cql_schema(
                "CREATE TABLE ks.t (id int PRIMARY KEY, name text, age int)",
            )
            .await
            .expect("schema registers");

        let executor = SelectExecutor::new(schema.clone(), storage.clone());
        let optimizer =
            crate::query::select_optimizer::SelectOptimizer::new(schema.clone(), storage.clone());

        let statement = crate::query::select_parser::parse_select("SELECT * FROM ks.t").unwrap();
        // Optimization performs no schema resolution; reset the counter AFTER it.
        let plan = optimizer.optimize(statement).await.unwrap();

        TABLE_SCHEMA_CLONES.with(|c| c.set(0));
        let _ = executor.execute(plan).await.expect("query executes");
        let clones = TABLE_SCHEMA_CLONES.with(|c| c.get());

        assert_eq!(
            clones, 1,
            "issue #1587: a query must deep-clone its schema out of the registry once \
             (was 2–4: column-metadata + scan + fallback each re-resolved), got {clones}"
        );
    }

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

    /// Issue #1584: projected column NAMES are derived once per query
    /// (`O(columns)`), never per row (`O(rows × columns)`). Cloning the hoisted
    /// `Arc<str>` per row is permitted; re-deriving / formatting a name per row is
    /// the regression this pins. `#[tokio::test]` runs on the current-thread
    /// runtime and the derivation counter is thread-local, so this is immune to
    /// pollution from other tests running in parallel.
    #[tokio::test]
    async fn execute_projection_derives_names_once_per_query() {
        let executor = create_test_executor().await;

        let columns = vec![
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "c".to_string(),
            }),
        ];
        let num_cols = columns.len();
        let num_rows = 100usize;

        let rows: Vec<QueryRow> = (0..num_rows)
            .map(|r| {
                let mut row = QueryRow::new(RowKey::new(vec![r as u8]));
                row.set("a", Value::Integer(r as i32));
                row.set("b", Value::Integer(r as i32));
                row.set("c", Value::Integer(r as i32));
                row
            })
            .collect();

        let mut ctx = ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: ProjectionFlags::default(),
            access_path: None,
            reverse_served: false,
        };

        PROJECTION_NAME_DERIVATIONS.with(|c| c.set(0));
        let projected = executor
            .execute_projection(rows, &columns, &mut ctx)
            .expect("projection must succeed");
        let derivations = PROJECTION_NAME_DERIVATIONS.with(|c| c.get());

        assert_eq!(projected.len(), num_rows, "one projected row per input row");
        assert_eq!(
            derivations,
            num_cols,
            "issue #1584: column names must be derived once per query \
             (O(cols) = {num_cols}), not per row (would be {})",
            num_rows * num_cols
        );
        // Output preserved byte-identically: names + values intact.
        assert_eq!(projected[0].values.get("a"), Some(&Value::Integer(0)));
        assert_eq!(projected[0].values.get("b"), Some(&Value::Integer(0)));
        assert_eq!(projected[0].values.get("c"), Some(&Value::Integer(0)));
        assert_eq!(projected[99].values.get("a"), Some(&Value::Integer(99)));
    }

    /// Issue #1952 (round-6 HIGH): a plain-column `Project` that only TRIMS the
    /// #1952-widened helper columns must PRESERVE each row's real `RowKey`,
    /// `metadata`, and `cell_metadata`, and drop only the unselected helper
    /// columns. Pre-fix these bare-column selects routed through
    /// `execute_projection`, which rebuilt every row with an EMPTY `RowKey`
    /// (`vec![]`) — a #1587-class regression. This pins the key-preserving trim.
    #[tokio::test]
    async fn trim_projection_preserves_key_and_trims_helpers() {
        let executor = create_test_executor().await;

        // SELECT a, b  (helper column `helper` was added to the scan by #1952).
        let columns = vec![
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
        ];

        let mut row = QueryRow::new(RowKey::new(vec![7, 8, 9]));
        row.set("a", Value::Integer(1));
        row.set("b", Value::Integer(2));
        row.set("helper", Value::Integer(3));
        row.set_metadata(crate::query::result::RowMetadata {
            version: Some(42),
            ttl: None,
            tags: Default::default(),
        });

        let out = executor.trim_projection(vec![row], &columns);
        assert_eq!(out.len(), 1);
        let r = &out[0];

        // Key PRESERVED (the core regression): not the empty `vec![]`.
        assert_eq!(
            r.key.0,
            vec![7, 8, 9],
            "trim must preserve the real RowKey, not destroy it to vec![]"
        );
        // Row metadata preserved.
        assert_eq!(
            r.metadata.version,
            Some(42),
            "row metadata must be preserved"
        );
        // Selected columns kept, helper trimmed.
        assert_eq!(r.values.get("a"), Some(&Value::Integer(1)));
        assert_eq!(r.values.get("b"), Some(&Value::Integer(2)));
        assert!(
            !r.values.contains_key("helper"),
            "the unselected helper column must be trimmed"
        );
        let mut keys: Vec<&str> = r.values.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["a", "b"], "only the selected columns remain");
    }

    /// Issue #1952 (round-6 HIGH, second defect): a selected column ABSENT from a
    /// sparse row must be OMITTED by the trim, never an error. Pre-fix
    /// `execute_projection` called `evaluate_select_expression(Column)`, which
    /// returns `Err("Column not found: <col>")` for an absent cell — so a sparse
    /// row aborted the whole query. This pins the tolerant trim AND documents the
    /// contrasting pre-fix defect (`execute_projection` still errors on the same
    /// input), proving the regression is real.
    #[tokio::test]
    async fn trim_projection_tolerates_absent_selected_cell() {
        let executor = create_test_executor().await;

        // SELECT a, b — but this row is sparse: it has `a` (and a helper) but no `b`.
        let columns = vec![
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            SelectExpression::Column(ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
        ];

        let build_row = || {
            let mut row = QueryRow::new(RowKey::new(vec![1]));
            row.set("a", Value::Integer(10));
            row.set("helper", Value::Integer(99));
            row
        };

        // NEW trim path: no error; `b` simply omitted, `a` kept, helper trimmed,
        // key preserved.
        let out = executor.trim_projection(vec![build_row()], &columns);
        assert_eq!(out.len(), 1);
        let r = &out[0];
        assert_eq!(r.key.0, vec![1], "sparse row keeps its real key");
        assert_eq!(r.values.get("a"), Some(&Value::Integer(10)));
        assert!(
            !r.values.contains_key("b"),
            "absent selected cell `b` is omitted, not defaulted"
        );
        assert!(!r.values.contains_key("helper"), "helper trimmed");

        // CONTRAST: the pre-fix path (`execute_projection`) ERRORS on the same
        // sparse input — this is exactly the defect the trim fixes.
        let mut ctx = ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: ProjectionFlags::default(),
            access_path: None,
            reverse_served: false,
        };
        let err = executor
            .execute_projection(vec![build_row()], &columns, &mut ctx)
            .expect_err("pre-fix execute_projection must error on the absent cell");
        assert!(
            err.to_string().contains("Column not found"),
            "the pre-fix defect is a 'Column not found' error on a sparse row; got: {err}"
        );
    }

    /// Issue #1590 (E8): `execute_limit` now applies OFFSET via `skip`/`take`
    /// (was `drain(..offset)` + `truncate`). This pins that the new path yields
    /// the SAME rows as the old drain/truncate reference across a matrix of
    /// (len, offset, limit) — including offset past the end, zero offset, and
    /// limit exceeding the remainder. No behavior change.
    #[tokio::test]
    async fn execute_limit_offset_matches_drain_truncate_reference() {
        let executor = create_test_executor().await;
        let mut ctx = ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: ProjectionFlags::default(),
            access_path: None,
            reverse_served: false,
        };

        let make = |len: usize| -> Vec<QueryRow> {
            (0..len)
                .map(|r| {
                    let mut row = QueryRow::new(RowKey::new(vec![r as u8]));
                    row.set("a", Value::Integer(r as i32));
                    row
                })
                .collect()
        };
        // The old in-place algorithm, kept verbatim as the parity oracle.
        let reference = |mut rows: Vec<QueryRow>, count: u64, offset: Option<u64>| {
            let start_index = offset.unwrap_or(0) as usize;
            if start_index >= rows.len() {
                return Vec::new();
            }
            rows.drain(..start_index);
            rows.truncate(count as usize);
            rows
        };
        let tags = |rows: &[QueryRow]| -> Vec<i32> {
            rows.iter()
                .map(|r| match r.values.get("a") {
                    Some(Value::Integer(v)) => *v,
                    _ => -1,
                })
                .collect()
        };

        for len in [0usize, 1, 5, 10] {
            for offset in [
                None,
                Some(0u64),
                Some(1),
                Some(3),
                Some(9),
                Some(10),
                Some(50),
            ] {
                for count in [0u64, 1, 3, 10, 1000] {
                    let got = executor
                        .execute_limit(make(len), count, offset, &mut ctx)
                        .expect("limit must succeed");
                    let want = reference(make(len), count, offset);
                    assert_eq!(
                        tags(&got),
                        tags(&want),
                        "len={len} offset={offset:?} count={count}: skip/take must \
                         equal drain/truncate"
                    );
                }
            }
        }
    }

    /// Issue #1590 (E8): PER PARTITION LIMIT now keys its per-partition counters
    /// on the partition-key 128-bit digest instead of a cloned `Vec<u8>` of the
    /// raw key bytes. Pin that the digest-keyed batch path yields the SAME rows
    /// as the raw-bytes reference — including NON-contiguous (interleaved)
    /// partitions, where the cap must still be enforced per distinct partition.
    #[test]
    fn per_partition_limit_digest_matches_raw_bytes_reference() {
        // Partition key bytes → row tag. Interleave partitions so the counter
        // cannot rely on contiguity (roborev job 38 invariant).
        let spec: [(&[u8], i32); 9] = [
            (b"pk-a", 0),
            (b"pk-b", 1),
            (b"pk-a", 2),
            (b"pk-c", 3),
            (b"pk-b", 4),
            (b"pk-a", 5),
            (b"pk-a", 6),
            (b"pk-c", 7),
            (b"pk-b", 8),
        ];
        let make = || -> Vec<QueryRow> {
            spec.iter()
                .map(|(pk, tag)| {
                    let mut row = QueryRow::new(RowKey::new(pk.to_vec()));
                    row.set("a", Value::Integer(*tag));
                    row
                })
                .collect()
        };
        // Old raw-`Vec<u8>`-keyed algorithm, kept verbatim as the parity oracle.
        let reference = |rows: Vec<QueryRow>, count: u64| -> Vec<QueryRow> {
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
        };
        let tags = |rows: &[QueryRow]| -> Vec<i32> {
            rows.iter()
                .map(|r| match r.values.get("a") {
                    Some(Value::Integer(v)) => *v,
                    _ => -1,
                })
                .collect()
        };

        for count in [0u64, 1, 2, 3, 100] {
            let got = SelectExecutor::execute_per_partition_limit(make(), count);
            let want = reference(make(), count);
            assert_eq!(
                tags(&got),
                tags(&want),
                "count={count}: digest-keyed per-partition-limit must equal raw-bytes reference"
            );
        }
    }

    /// Issue #1590 (E8, roborev fix #4 follow-up): PER PARTITION LIMIT uses the
    /// 128-bit digest only as a FAST outer key; it confirms EXACT partition-key
    /// bytes before sharing a counter. Correctness must NOT depend on
    /// collision absence. Real 128-bit digest collisions can't be produced, so
    /// drive [`admit_partition_row`] with a CONSTANT (forced-colliding) digest
    /// for two DISTINCT keys and assert each key keeps its OWN counter — i.e. no
    /// valid row is dropped when digests collide.
    #[test]
    fn per_partition_limit_exact_confirm_survives_digest_collision() {
        let mut counts: PartitionCounts = HashMap::new();
        // A single digest value shared by two genuinely distinct partitions.
        const COLLIDING: u128 = 0xDEAD_BEEF;
        let a = b"partition-a".as_slice();
        let b = b"partition-b".as_slice();

        // cap = 1 per partition. A's first row opens A's counter.
        assert!(
            admit_partition_row(&mut counts, COLLIDING, a, 1),
            "A's first row is admitted"
        );
        // B collides on the digest but is a DISTINCT key: exact-byte confirm
        // gives it its OWN counter, so its first row is admitted (NOT dropped by
        // A's now-exhausted counter). This is the exact bug the finding flags.
        assert!(
            admit_partition_row(&mut counts, COLLIDING, b, 1),
            "B's first row is admitted despite the colliding digest (separate counter)"
        );
        // Each partition's cap of 1 is now independently exhausted.
        assert!(
            !admit_partition_row(&mut counts, COLLIDING, a, 1),
            "A's second row is capped"
        );
        assert!(
            !admit_partition_row(&mut counts, COLLIDING, b, 1),
            "B's second row is capped"
        );
        // Both distinct keys are chained under the one colliding digest bucket,
        // proving the exact-confirm path (not the digest) disambiguates them.
        assert_eq!(
            counts.get(&COLLIDING).map(Vec::len),
            Some(2),
            "both distinct keys are chained under the colliding digest"
        );
    }

    /// Issue #1587 (E5): ORDER BY uses decorate-sort-undecorate, so each row's
    /// sort key is evaluated EXACTLY once (`O(rows × items)`), never inside the
    /// comparator (`O(n log n)` evaluations + a `Value::clone` per comparison).
    /// The counter pins the linear evaluation budget; the value assertions pin
    /// that the resulting order is byte-identical to the comparator sort.
    #[tokio::test]
    async fn execute_sort_evaluates_keys_once_per_row() {
        let executor = create_test_executor().await;

        let num_rows = 64usize;
        // Reverse-sorted input so the sort must actually reorder every row (a
        // pre-sorted input could let some sorts short-circuit comparisons).
        let rows: Vec<QueryRow> = (0..num_rows)
            .rev()
            .map(|r| {
                let mut row = QueryRow::new(RowKey::new(vec![r as u8]));
                row.set("k", Value::Integer(r as i32));
                row
            })
            .collect();

        let order_by = OrderByClause {
            items: vec![OrderByItem {
                expression: SelectExpression::Column(ColumnRef {
                    table: None,
                    column: "k".to_string(),
                }),
                direction: SortDirection::Ascending,
            }],
        };

        let mut ctx = ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: ProjectionFlags::default(),
            access_path: None,
            reverse_served: false,
        };

        SORT_KEY_EVALUATIONS.with(|c| c.set(0));
        let sorted = executor
            .execute_sort(rows, &order_by, &mut ctx)
            .expect("sort must succeed");
        let evaluations = SORT_KEY_EVALUATIONS.with(|c| c.get());

        // Exactly one evaluation per (row × order-by item). A comparator-based
        // sort would evaluate ~2 per pairwise comparison — strictly more than
        // `num_rows` for any non-trivial input (e.g. ~2·n·log2(n) ≫ n here).
        assert_eq!(
            evaluations, num_rows,
            "issue #1587: sort keys must be evaluated once per row (n = {num_rows}), \
             not O(n log n) times inside the comparator"
        );

        // Ordering preserved byte-identically: ascending 0..num_rows.
        assert_eq!(sorted.len(), num_rows);
        for (i, row) in sorted.iter().enumerate() {
            assert_eq!(
                row.values.get("k"),
                Some(&Value::Integer(i as i32)),
                "row {i} must sort into ascending position"
            );
        }
    }

    /// Issue #1587 (E5): the decorate-sort-undecorate refactor must PRESERVE the
    /// ORDER BY ordering for float keys — including NaN and signed zero — exactly
    /// as the pre-refactor per-comparison sort produced it. It reuses the shared
    /// `compare_values_ordering` comparator with a stable `sort_by`, so this test
    /// drives the decorate-sort path (via `execute_sort`) and asserts:
    ///
    ///   * with NaN present, the decorated output is order-IDENTICAL to a direct
    ///     reference sort over the same keys and comparator (ASC and DESC) — the
    ///     core preservation guarantee; and
    ///   * over a NaN-free float input (where the comparator IS a total order),
    ///     finite keys are correctly ordered and signed zeros (-0.0 vs +0.0, which
    ///     compare Equal) keep input order (stable).
    ///
    /// NOTE (pre-existing gaps, out of scope for #1587): `compare_values_ordering`
    /// compares floats via `f64::partial_cmp` (NaN → Equal, -0.0 == +0.0). So it
    /// does NOT reproduce Cassandra/Java float ordering (NaN sorted LAST,
    /// -0.0 < +0.0), and — because a NaN key makes the comparator a NON-total
    /// order — a NaN in the input leaves even the finite keys in an unspecified
    /// order. This test therefore pins the ordering the decorate-sort ACTUALLY
    /// preserves, not the (currently absent) Java semantics. Making ORDER BY match
    /// Cassandra float ordering is a separate behavior change; reported separately.
    #[tokio::test]
    async fn execute_sort_preserves_float_nan_signed_zero_ordering() {
        let executor = create_test_executor().await;

        let make_rows = |inputs: &[(u8, f64)]| -> Vec<QueryRow> {
            inputs
                .iter()
                .map(|(tag, f)| {
                    let mut row = QueryRow::new(RowKey::new(vec![*tag]));
                    row.set("f", Value::Float(*f));
                    row
                })
                .collect()
        };
        let order_by = |dir: &SortDirection| OrderByClause {
            items: vec![OrderByItem {
                expression: SelectExpression::Column(ColumnRef {
                    table: None,
                    column: "f".to_string(),
                }),
                direction: dir.clone(),
            }],
        };
        let mut ctx = ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: ProjectionFlags::default(),
            access_path: None,
            reverse_served: false,
        };
        let tags = |rows: &[QueryRow]| -> Vec<u8> { rows.iter().map(|r| r.key.0[0]).collect() };

        // --- Part A: NaN present → decorate-sort is order-identical to reference.
        // Tags 2 (-0.0) and 4 (+0.0) compare Equal; NaN appears twice (tags 1, 6).
        let with_nan: [(u8, f64); 7] = [
            (0, 2.0),
            (1, f64::NAN),
            (2, -0.0),
            (3, 1.0),
            (4, 0.0),
            (5, -1.0),
            (6, f64::NAN),
        ];
        for dir in [SortDirection::Ascending, SortDirection::Descending] {
            let sorted = executor
                .execute_sort(make_rows(&with_nan), &order_by(&dir), &mut ctx)
                .expect("sort must succeed");

            let mut reference = make_rows(&with_nan);
            reference.sort_by(|a, b| {
                let (ka, kb) = (
                    a.values.get("f").expect("key"),
                    b.values.get("f").expect("key"),
                );
                match dir {
                    SortDirection::Ascending => compare_values_ordering(ka, kb),
                    SortDirection::Descending => compare_values_ordering(kb, ka),
                }
            });
            assert_eq!(
                tags(&sorted),
                tags(&reference),
                "issue #1587: decorate-sort must be order-identical to the reference \
                 comparator sort for float/NaN keys ({dir:?})"
            );
        }

        // --- Part B: NaN-free input → the comparator is a total order, so the
        // decorate-sort must produce correct finite ordering and stable signed
        // zeros. Tags 2 (-0.0) and 4 (+0.0) compare Equal (must keep input order).
        let no_nan: [(u8, f64); 5] = [(0, 2.0), (2, -0.0), (3, 1.0), (4, 0.0), (5, -1.0)];
        // Ascending: -1.0, then -0.0 (tag 2) then +0.0 (tag 4) [stable tie], 1.0, 2.0.
        let asc = executor
            .execute_sort(
                make_rows(&no_nan),
                &order_by(&SortDirection::Ascending),
                &mut ctx,
            )
            .expect("sort must succeed");
        assert_eq!(
            tags(&asc),
            vec![5, 2, 4, 3, 0],
            "ascending float order with stable signed zeros"
        );
        // Descending: 2.0, 1.0, then -0.0/+0.0 (stable tie: 2 before 4), -1.0.
        let desc = executor
            .execute_sort(
                make_rows(&no_nan),
                &order_by(&SortDirection::Descending),
                &mut ctx,
            )
            .expect("sort must succeed");
        assert_eq!(
            tags(&desc),
            vec![0, 3, 2, 4, 5],
            "descending float order with stable signed zeros"
        );
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

        let cols = executor.get_result_columns(&stmt, None).unwrap();
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

        let cols = executor.get_result_columns(&stmt, None).unwrap();
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

        let cols = executor.get_result_columns(&stmt, None).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].name, "wt",
            "Alias must override Cassandra convention"
        );
    }

    /// Build an optimized plan for `sql` against a fixed 4-column table, so
    /// `requires_materialization` can be asserted directly (issue #1952 streaming
    /// follow-up). The scan projection is the #1952 broadened union (selected +
    /// WHERE/ORDER BY/GROUP BY/agg-arg helper columns).
    async fn plan_for(sql: &str) -> OptimizedQueryPlan {
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
        schema
            .parse_and_register_cql_schema(
                "CREATE TABLE ks.t (id int PRIMARY KEY, a int, b int, c int)",
            )
            .await
            .expect("schema registers");
        let optimizer =
            crate::query::select_optimizer::SelectOptimizer::new(schema.clone(), storage.clone());
        let statement = crate::query::select_parser::parse_select(sql).unwrap();
        optimizer.optimize(statement).await.unwrap()
    }

    /// #1952 streaming follow-up: a plain-column `SELECT` filtered by an
    /// UNSELECTED WHERE column produces a scan projection (`a`,`b`) wider than the
    /// SELECT output (`a`). The `Project` step must TRIM `b`; the streaming
    /// producer ignores `Project`, so the plan MUST be routed through the
    /// materialized path. Pre-fix this returned `false` (streaming leaked `b`).
    #[tokio::test]
    async fn requires_materialization_for_where_helper_trim() {
        let plan = plan_for("SELECT a FROM ks.t WHERE b = 1").await;
        let executor = create_test_executor().await;
        assert!(
            executor.requires_materialization(&plan),
            "SELECT a WHERE b=1 scans [a,b] but outputs [a]; the Project TRIM of \
             the helper column `b` must force materialization (streaming ignores \
             Project and would leak `b`)"
        );
    }

    /// #1952 streaming follow-up: an ORDER BY + WHERE query over UNSELECTED helper
    /// columns must materialize (the trim path is additionally guaranteed by the
    /// Sort step, but the projection-set check alone already forces it).
    #[tokio::test]
    async fn requires_materialization_for_order_by_helper_trim() {
        let plan = plan_for("SELECT a FROM ks.t WHERE b = 1 ORDER BY c").await;
        let executor = create_test_executor().await;
        assert!(
            executor.requires_materialization(&plan),
            "SELECT a WHERE b=1 ORDER BY c scans [a,b,c] but outputs [a]; must \
             materialize to trim helper columns b,c"
        );
    }

    /// Regression: when the selected columns EXACTLY equal the scan projection
    /// (no helper columns, no reshape) the optimizer emits NO redundant `Project`
    /// and the plan streams directly — this must NOT be forced into
    /// materialization (no perf regression for the common case).
    #[tokio::test]
    async fn no_materialization_when_output_equals_scan() {
        // Both selected columns are exactly the scan set (WHERE column `a` is
        // already selected), so there is no helper column to trim.
        let plan = plan_for("SELECT a, b FROM ks.t WHERE a = 1").await;
        let executor = create_test_executor().await;
        assert!(
            !executor.requires_materialization(&plan),
            "SELECT a,b WHERE a=1 scans exactly [a,b] and outputs [a,b]; it must \
             stream directly without materialization"
        );
    }

    /// Regression: a reordered SELECT whose WHERE column is already selected
    /// (`SELECT b, a WHERE a = 1`) has output set {a,b} == scan set {a,b}, so it
    /// streams directly — `project_trims_scan_columns` compares SETS (not order),
    /// so no false materialization is triggered.
    #[tokio::test]
    async fn no_materialization_for_reordered_select_no_helpers() {
        let plan = plan_for("SELECT b, a FROM ks.t WHERE a = 1").await;
        let executor = create_test_executor().await;
        assert!(
            !executor.requires_materialization(&plan),
            "a reordered select with no helper columns (same column set) must \
             stream directly"
        );
    }
}
