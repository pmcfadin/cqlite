//! Aggregation execution for the SELECT executor (issue #1116 split; issue #1578).
//!
//! Two paths live here:
//! - [`SelectExecutor::execute_aggregation`] — the buffered GROUP BY / global
//!   aggregate accumulation over an already-materialized `Vec<QueryRow>`.
//! - [`SelectExecutor::try_execute_global_aggregate`] — the issue #1578 (D2)
//!   O(1) streaming fold: a GROUP-BY-free aggregate over a full table scan folds
//!   each scanned row into a single running accumulator and drops the row, so
//!   `COUNT(*)`/`MIN`/`MAX`/`SUM`/`AVG` never buffer the whole table.

use super::aggregation::{
    build_group_key, finalize_empty_global_aggregate, finalize_group, find_or_init_group,
    init_aggregate_accumulators, update_aggregate, AggregationState,
};
use super::{
    build_row_from_scan_cached, classify_partition_lookup, evaluate_predicates,
    validate_token_predicates, AccessPath, ExecutionContext, ExecutionStep, PartitionKeyCache,
    PartitionLookupOutcome, SelectExecutor,
};
use crate::query::result::QueryRow;
use crate::query::select_ast::WhereExpression;
use crate::query::select_optimizer::{AggregationPlan, SSTablePredicate};
use crate::schema::TableSchema;
use crate::{Error, Result, TableId, Value};

/// Read-ahead window for the D2 aggregate fold's scan stream. The fold retains
/// only the running accumulator, so this bounds the in-flight scan rows (not the
/// result) — the same role `StreamingConfig::buffer_size` plays for the streaming
/// producer.
const AGG_FOLD_SCAN_BUFFER: usize = 1024;

/// The plan shape eligible for the O(1) global-aggregate fold: one full-scan step,
/// one GROUP-BY-free aggregate, and zero or more post-scan `Filter`s.
struct GlobalAggregatePlan<'a> {
    table: &'a TableId,
    predicates: &'a [SSTablePredicate],
    projection: &'a [String],
    filters: Vec<&'a WhereExpression>,
    agg_plan: &'a AggregationPlan,
}

impl SelectExecutor {
    /// Execute the aggregation step over already-materialized rows. Splits
    /// naturally into three phases: build group key, accumulate per-aggregate
    /// state, then finalize each group into a result row.
    pub(super) fn execute_aggregation(
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

        // Issue #2069: a GLOBAL aggregate (no GROUP BY) always emits exactly one
        // row, even when the input is empty. With rows present a single `[Null]`
        // group is created above (see `build_group_key`); with zero rows no group
        // exists yet, so synthesize the empty-input row (COUNT = 0, other
        // aggregates NULL). A GROUP BY query is UNAFFECTED — it correctly returns
        // zero rows when there are no groups.
        if agg_state.groups.is_empty() && agg_plan.group_by_columns.is_empty() {
            return Ok(vec![finalize_empty_global_aggregate(agg_plan)]);
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

    /// Issue #1578 (D2): if `steps` is a GROUP-BY-free aggregate over a FULL table
    /// scan, fold the scan stream into an O(1) accumulator and return the single
    /// finalized result row — instead of buffering the whole table into a
    /// `Vec<QueryRow>` and calling [`Self::execute_aggregation`].
    ///
    /// Returns `Ok(None)` when the plan is NOT eligible — a GROUP BY (E5 territory),
    /// a partition-targeted / multi-partition lookup (single/few small partitions
    /// the buffered path handles fine), a WRITETIME/TTL cell-metadata projection,
    /// or any `Sort`/`Project`/`Limit`/`PerPartitionLimit` step — and the caller
    /// must use the buffered path. On the eligible path the per-row update and
    /// finalize reuse the exact buffered-path helpers, so the answer is identical.
    pub(super) async fn try_execute_global_aggregate(
        &self,
        steps: &[ExecutionStep],
        schema_opt: Option<&TableSchema>,
        context: &mut ExecutionContext,
    ) -> Result<Option<Vec<QueryRow>>> {
        // A WRITETIME/TTL projection needs the per-cell metadata scan, not this fold.
        if context.projection_flags.include_cell_metadata {
            return Ok(None);
        }

        let Some(eligible) = classify_global_aggregate(steps) else {
            return Ok(None);
        };
        let GlobalAggregatePlan {
            table,
            predicates,
            projection,
            filters,
            agg_plan,
        } = eligible;

        // A `token(...)` predicate must cover the full partition key (same rule as
        // every scan path) before we scan/evaluate it.
        validate_token_predicates(predicates, schema_opt)?;

        // Only a full scan is folded. A targeted / multi-targeted lookup reads one
        // or a few small partitions, so the buffered path's memory is already
        // O(partition) — fall back and let it run unchanged.
        let reason = match classify_partition_lookup(predicates, schema_opt) {
            PartitionLookupOutcome::Fallback(reason) => reason,
            PartitionLookupOutcome::Targeted(_) | PartitionLookupOutcome::MultiTargeted(_) => {
                return Ok(None);
            }
        };

        // Record the honest full-scan access path, exactly as the buffered scan
        // step does, so observability/plan-family assertions are unchanged.
        context.access_path = Some(AccessPath::FallbackFullScan { reason });
        crate::query::access_path::record(AccessPath::FallbackFullScan { reason });

        let mut accumulators = init_aggregate_accumulators(&agg_plan.aggregates);
        let mut folded_any = false;

        // Issue #1592: fold over the BATCHED streaming surface — one async wake per
        // batch, not per row. Flattening each batch yields the same rows in the
        // same order as `scan_stream`, so the aggregate result is unchanged.
        let mut scan_stream = self
            .storage
            .scan_stream_batched(table, None, None, schema_opt, AGG_FOLD_SCAN_BUFFER)
            .await?;
        // Issue #1817: hoist the partition-key decode across a partition's rows;
        // the cache persists across batches (a partition may straddle a batch).
        let mut pk_cache = PartitionKeyCache::default();
        while let Some(batch) = scan_stream.recv().await {
            for (key, value) in batch? {
                context.rows_processed += 1;
                context.scan_rows += 1;

                let Some(row) =
                    build_row_from_scan_cached(key, value, projection, schema_opt, &mut pk_cache)
                else {
                    continue;
                };
                if !evaluate_predicates(&row, predicates)? {
                    continue;
                }
                // Apply any residual (non-pushed) WHERE as a per-row filter.
                let mut keep = true;
                for filter in &filters {
                    if !self.evaluate_where_expression(filter, &row)? {
                        keep = false;
                        break;
                    }
                }
                if !keep {
                    continue;
                }

                folded_any = true;
                for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                    update_aggregate(&mut accumulators[i], agg_comp, &row);
                }
            }
        }

        // Issue #2069: a GLOBAL aggregate (no GROUP BY) ALWAYS produces exactly one
        // result row, even over zero input rows. Cassandra returns COUNT = 0 and
        // NULL for every other aggregate (SUM/AVG/MIN/MAX) of empty input, so when
        // nothing folded we synthesize that single row rather than returning zero
        // rows. This fold only handles the GROUP-BY-free case (see
        // `classify_global_aggregate`), so it never affects a GROUP BY query, which
        // correctly yields zero rows when there are no groups.
        if !folded_any {
            let row = finalize_empty_global_aggregate(agg_plan);
            return Ok(Some(vec![row]));
        }

        let row = finalize_group(vec![Value::Null], accumulators, agg_plan);
        Ok(Some(vec![row]))
    }
}

/// Classify `steps` as an eligible GROUP-BY-free full-scan aggregate, returning
/// the borrowed scan + aggregate + residual-filter references, or `None` when any
/// step disqualifies it (see [`SelectExecutor::try_execute_global_aggregate`]).
fn classify_global_aggregate(steps: &[ExecutionStep]) -> Option<GlobalAggregatePlan<'_>> {
    let mut scan: Option<(&TableId, &[SSTablePredicate], &[String])> = None;
    let mut agg_plan: Option<&AggregationPlan> = None;
    let mut filters: Vec<&WhereExpression> = Vec::new();

    for step in steps {
        match step {
            ExecutionStep::SSTableScan {
                table,
                predicates,
                projection,
                ..
            } => {
                if scan.is_some() {
                    return None;
                }
                scan = Some((table, predicates.as_slice(), projection.as_slice()));
            }
            ExecutionStep::Aggregate { plan, .. } => {
                // Only a GLOBAL aggregate (no GROUP BY), and only one.
                if !plan.group_by_columns.is_empty() || agg_plan.is_some() {
                    return None;
                }
                agg_plan = Some(plan);
            }
            ExecutionStep::Filter { expression, .. } => filters.push(expression),
            // Sort / Project / Limit / PerPartitionLimit reshape or bound the result
            // in ways the single-row fold does not model — use the buffered path.
            _ => return None,
        }
    }

    let (table, predicates, projection) = scan?;
    let agg_plan = agg_plan?;
    Some(GlobalAggregatePlan {
        table,
        predicates,
        projection,
        filters,
        agg_plan,
    })
}
