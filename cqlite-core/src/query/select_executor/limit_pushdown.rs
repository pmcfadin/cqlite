//! Issue #1577 (Epic D / D1): LIMIT/OFFSET pushdown into the materializing scan.
//!
//! The materializing executor ([`SelectExecutor::execute`]) historically
//! decoded EVERY row of a table into a per-row `HashMap`, then threw all but the
//! first `LIMIT` away in the post-hoc `Limit` step. This module supplies the two
//! pieces that let it stop early WITHOUT changing a single result:
//!
//! - [`scan_pushdown_cap`] decides — from the plan alone — whether the scan may
//!   be bounded, and to how many ACCEPTED rows.
//! - [`SelectExecutor::capped_fallback_scan`] performs the bounded full-scan
//!   fallback by consuming the lazy `scan_stream` (definitionally in lockstep
//!   with `scan`) and dropping it once the cap is reached, which closes the
//!   channel and stops the producer decoding the tail. A stream that ends short
//!   of the cap is reconciled against the authoritative materializing `scan`, so
//!   a `scan_stream`/`scan` divergence can never drop a row.
//!
//! Correctness is the law here: the cap is applied ONLY when the pipeline
//! between the scan and the query-wide `LIMIT` neither reorders nor drops rows,
//! and the cap counts rows the executor actually ACCEPTS (post marker
//! suppression, post predicate) — never raw scan rows — so a suppressed row
//! tombstone / null-row marker or a predicate miss can never under-deliver.

use super::{
    build_row_from_scan, evaluate_predicates, ExecutionContext, ExecutionStep, QueryRow, Result,
    SSTablePredicate, SelectExecutor, TableId, TableSchema,
};
use crate::query::select_ast::SelectClause;

/// In-flight row bound for the capped streaming fallback scan (issue #1577).
///
/// The per-call buffer is `cap + 1` clamped to this ceiling: a small `LIMIT`
/// decodes only a few rows past the limit before the producer parks (so the
/// decode work — and the `PARTITION_HEADER_TRY_PARSES` / row-decode counters —
/// stays `O(limit)`, not `O(table)`), while a large `LIMIT` keeps the same
/// bounded footprint the streaming path already uses (issue #790).
const CAPPED_SCAN_STREAM_BUFFER: usize = 1024;

/// Compute the LIMIT/OFFSET pushdown cap for the materializing scan (issue #1577).
///
/// Returns `Some(limit + offset)` — the number of ACCEPTED (post-predicate,
/// post-marker) rows the scan must produce before the downstream `Limit` step
/// can slice the final `offset..offset+count` window — but ONLY when the plan is
/// pushdown-safe:
///
/// * there IS a `Limit` step, and
/// * there is NO step between the scan and that `Limit` that reorders, collapses,
///   or otherwise reduces the row multiset — `Sort`, `Aggregate`,
///   `PerPartitionLimit` — and NO residual `Filter` step (a `Filter` drops rows
///   the scan already yielded, so a raw scan cap could under-deliver), and
/// * the query is not `DISTINCT` (which may dedup rows after the scan).
///
/// Returns `None` otherwise, leaving the scan unbounded exactly as before.
///
/// # Why the cap preserves results
///
/// When none of those steps are present, the only step that may sit between the
/// scan and the `Limit` is `Project`, which neither reorders nor drops rows. So
/// the row sequence feeding `Limit` is byte-identical, in the same order, to the
/// unbounded scan's — just possibly shorter. `execute_limit` takes
/// `offset..offset+count`, a slice that depends solely on the first
/// `limit + offset` rows. Producing exactly that many ACCEPTED rows therefore
/// yields the identical final rows and ordering.
///
/// Pushed-down `SSTablePredicate`s are compatible: the caller counts rows it
/// ACCEPTS (those that pass the predicate), not raw scan rows, so the cap still
/// means "enough rows for the window".
pub(super) fn scan_pushdown_cap(
    steps: &[ExecutionStep],
    select_clause: &SelectClause,
) -> Option<usize> {
    // DISTINCT may dedup rows after the scan; never bound the scan ahead of a
    // potential row-reducing dedup.
    if matches!(select_clause, SelectClause::Distinct(_)) {
        return None;
    }

    let mut cap: Option<usize> = None;
    for step in steps {
        match step {
            // Any step that REORDERS or REDUCES the row multiset between the scan
            // and the query-wide LIMIT makes a raw scan bound unsafe: the final
            // window could then be drawn from rows the bounded scan never emitted.
            ExecutionStep::Sort { .. }
            | ExecutionStep::Aggregate { .. }
            | ExecutionStep::PerPartitionLimit { .. }
            | ExecutionStep::Filter { .. } => return None,
            ExecutionStep::Limit { count, offset } => {
                let count = usize::try_from(*count).unwrap_or(usize::MAX);
                let offset = offset
                    .map(|o| usize::try_from(o).unwrap_or(usize::MAX))
                    .unwrap_or(0);
                cap = Some(count.saturating_add(offset));
            }
            // Order- and count-preserving; safe on either side of the cap.
            ExecutionStep::SSTableScan { .. } | ExecutionStep::Project { .. } => {}
        }
    }
    cap
}

impl SelectExecutor {
    /// Full-scan fallback that stops DECODING once `cap` rows have been ACCEPTED
    /// (issue #1577, D1).
    ///
    /// The fast path consumes the lazy
    /// [`scan_stream`](crate::storage::StorageEngine::scan_stream) — definitionally
    /// in lockstep with the materializing `scan` (same token order, same
    /// cross-generation reconciliation) — and drops it once `cap` rows are
    /// accepted. Dropping the receiver closes the channel, so the producer stops
    /// parsing the tail: on a 1M-row table a `LIMIT 10` decodes on the order of
    /// `cap + buffer` rows, not a million (verified via the #1618
    /// `PARTITION_HEADER_TRY_PARSES` counter).
    ///
    /// The cap counts rows the executor ACCEPTS: a row suppressed by
    /// [`build_row_from_scan`] (a row tombstone / null-row `ScanRow::Marker`, Issue
    /// #191) or rejected by [`evaluate_predicates`] is skipped and never counted,
    /// so a suppressed marker or a predicate miss can never under-deliver.
    ///
    /// # Correctness: authoritative reconciliation
    ///
    /// If the stream yields FEWER than `cap` accepted rows before ending, that is
    /// EITHER a table with fewer than `cap` accepted rows OR a `scan_stream` /
    /// `scan` divergence (some SSTable formats — e.g. certain write-generated
    /// uncompressed BIG files — under-produce via the block-streaming path). The
    /// two are indistinguishable here, so this method DOES NOT trust a short
    /// stream: it re-runs the AUTHORITATIVE materializing `scan` (the exact rows
    /// the non-pushdown path returns) and takes its first `cap` accepted rows,
    /// guaranteeing a byte-identical result. A short stream costs one extra scan
    /// (of a table already known to hold `< cap` rows, or a divergent format);
    /// there is no decode-stop win in that branch, but correctness is the law.
    ///
    /// A stream that reaches a FULL `cap` is trusted (returned early): its rows are
    /// the token-first `cap` rows, identical to `scan` truncated to `cap` — the same
    /// contract the streaming executor already relies on for every result it emits.
    pub(super) async fn capped_fallback_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        schema_opt: Option<&TableSchema>,
        cap: usize,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // A `LIMIT 0` (cap == 0) can never accept a row; do not even open a scan.
        if cap == 0 {
            return Ok(Vec::new());
        }

        let buffer = cap.saturating_add(1).min(CAPPED_SCAN_STREAM_BUFFER);
        let mut scan_stream = self
            .storage
            .scan_stream(table, None, None, schema_opt, buffer)
            .await?;

        let mut results = Vec::new();
        while let Some(item) = scan_stream.recv().await {
            let (key, value) = item?;
            context.rows_processed += 1;
            context.scan_rows += 1;

            let Some(row) = build_row_from_scan(key, value, projection, schema_opt) else {
                continue;
            };

            if evaluate_predicates(&row, predicates)? {
                results.push(row);
                if results.len() >= cap {
                    // Decode-stop win: dropping the stream closes the channel and
                    // the producer stops parsing the remaining (unneeded) rows.
                    return Ok(results);
                }
            }
        }

        // Short stream: reconcile against the authoritative materializing scan so
        // a `scan_stream`/`scan` divergence can never drop a row.
        drop(scan_stream);
        let authoritative = self
            .storage
            .scan(table, None, None, None, schema_opt)
            .await?;
        let mut reconciled = Vec::with_capacity(cap.min(authoritative.len()));
        for (key, value) in authoritative {
            if reconciled.len() >= cap {
                break;
            }
            context.rows_processed += 1;
            context.scan_rows += 1;
            let Some(row) = build_row_from_scan(key, value, projection, schema_opt) else {
                continue;
            };
            if evaluate_predicates(&row, predicates)? {
                reconciled.push(row);
            }
        }
        Ok(reconciled)
    }
}

#[cfg(test)]
mod tests {
    use super::scan_pushdown_cap;
    use crate::query::select_ast::{
        ColumnRef, ComparisonExpression, ComparisonOperator, ComparisonRightSide, OrderByClause,
        OrderByItem, SelectClause, SelectExpression, SortDirection, WhereExpression,
    };
    use crate::query::select_optimizer::{
        AggregateComputation, AggregationPlan, ExecutionStep, SSTablePredicate,
    };
    use crate::types::{TableId, Value};

    fn col(name: &str) -> SelectExpression {
        SelectExpression::Column(ColumnRef {
            table: None,
            column: name.to_string(),
        })
    }

    fn scan() -> ExecutionStep {
        ExecutionStep::SSTableScan {
            table: TableId::new("ks.t"),
            predicates: Vec::<SSTablePredicate>::new(),
            projection: vec!["a".to_string()],
        }
    }

    fn limit(count: u64, offset: Option<u64>) -> ExecutionStep {
        ExecutionStep::Limit { count, offset }
    }

    fn all() -> SelectClause {
        SelectClause::All
    }

    fn order_by() -> ExecutionStep {
        ExecutionStep::Sort {
            order_by: OrderByClause {
                items: vec![OrderByItem {
                    expression: col("a"),
                    direction: SortDirection::Ascending,
                }],
            },
        }
    }

    #[test]
    fn limit_only_yields_cap_of_count_plus_offset() {
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(10, None)], &all()),
            Some(10)
        );
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(10, Some(5))], &all()),
            Some(15),
            "cap must be limit + offset so the downstream slice has enough rows"
        );
    }

    #[test]
    fn no_limit_step_means_no_pushdown() {
        assert_eq!(scan_pushdown_cap(&[scan()], &all()), None);
    }

    #[test]
    fn limit_zero_caps_at_zero() {
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(0, None)], &all()),
            Some(0)
        );
    }

    #[test]
    fn sort_disables_pushdown() {
        // ORDER BY needs every row before it can pick the top N.
        assert_eq!(
            scan_pushdown_cap(&[scan(), order_by(), limit(10, None)], &all()),
            None
        );
    }

    #[test]
    fn aggregate_disables_pushdown() {
        let agg = ExecutionStep::Aggregate {
            plan: AggregationPlan {
                group_by_columns: vec![],
                group_by_output_names: vec![],
                aggregates: Vec::<AggregateComputation>::new(),
            },
        };
        assert_eq!(
            scan_pushdown_cap(&[scan(), agg, limit(10, None)], &all()),
            None
        );
    }

    #[test]
    fn per_partition_limit_disables_pushdown() {
        // PER PARTITION LIMIT prunes rows per partition before the query LIMIT, so
        // a raw scan cap could stop before enough survive the per-partition prune.
        assert_eq!(
            scan_pushdown_cap(
                &[
                    scan(),
                    ExecutionStep::PerPartitionLimit { count: 2 },
                    limit(10, None)
                ],
                &all()
            ),
            None
        );
    }

    #[test]
    fn residual_filter_disables_pushdown() {
        // A residual Filter drops rows the scan already yielded, so a raw scan cap
        // could under-deliver the final window.
        let filter = ExecutionStep::Filter {
            expression: WhereExpression::Comparison(ComparisonExpression {
                left: col("a"),
                operator: ComparisonOperator::Equal,
                right: ComparisonRightSide::Value(SelectExpression::Literal(Value::Integer(1))),
            }),
        };
        assert_eq!(
            scan_pushdown_cap(&[scan(), filter, limit(10, None)], &all()),
            None
        );
    }

    #[test]
    fn distinct_disables_pushdown() {
        let distinct = SelectClause::Distinct(vec![col("a")]);
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(10, None)], &distinct),
            None
        );
    }

    #[test]
    fn project_after_limit_is_transparent() {
        // Project neither reorders nor drops rows; it must not block pushdown.
        let project = ExecutionStep::Project {
            columns: vec![col("a")],
        };
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(10, None), project], &all()),
            Some(10)
        );
    }

    #[test]
    fn overflow_saturates_not_panics() {
        assert_eq!(
            scan_pushdown_cap(&[scan(), limit(u64::MAX, Some(u64::MAX))], &all()),
            Some(usize::MAX)
        );
    }
}
