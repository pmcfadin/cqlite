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
use crate::types::{RowKey, ScanRow};

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

        // Metric accuracy (issue #1577, SUGGESTION-3): snapshot the scan counters
        // so the short-stream reconciliation below can charge the AUTHORITATIVE
        // re-scan exactly ONCE. Without this, the partial stream's per-row
        // increments PLUS the re-scan's increments both land in
        // `QUERY_ROWS_SCANNED` for the reconcile path, inflating rows-scanned.
        // Results are unaffected — this only fixes double-counting.
        let processed_baseline = context.rows_processed;
        let scan_rows_baseline = context.scan_rows;

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
                    // ── Trusted full-cap stream fast path (issue #1577) ──────────
                    //
                    // Decode-stop win: dropping the stream closes the channel and
                    // the producer stops parsing the remaining (unneeded) rows.
                    //
                    // INVARIANT — on-disk row order == token order — pinned here.
                    // We return the stream's first `cap` rows WITHOUT re-sorting or
                    // re-checking their order against the authoritative scan. This
                    // is sound ONLY because every SUPPORTED writer emits rows in
                    // token order, so `scan_stream` (which does NOT sort) yields the
                    // same order as the materializing `scan` (which token-sorts via
                    // `sort_by_token_order`):
                    //   * Cassandra 5.0 SSTable `Data.db` files are written
                    //     token-ordered on disk;
                    //   * CQLite's memtable is a token-ordered `BTreeMap`, so every
                    //     flushed generation is token-ordered;
                    //   * compaction output is a k-way TOKEN merge of token-ordered
                    //     inputs.
                    // The `debug_assert` below pins this: it re-runs the
                    // authoritative token-ordered `scan` and verifies the trusted
                    // prefix matches, so a FUTURE writer that emits rows out of
                    // token order trips in debug/tests rather than silently
                    // returning misordered rows. It is debug-only — never compiled
                    // into release, so it adds zero release perf cost.
                    //
                    // The guard is ALSO compiled out under the `work-counters`
                    // feature: that build is a measurement build whose sole job is
                    // to count decode work, and the guard's authoritative
                    // verification `scan` parses the whole fixture, inflating the
                    // very `PARTITION_HEADER_TRY_PARSES` counter the decode-stop
                    // test asserts. The token-order guard is a correctness check for
                    // normal debug/test runs and need not (must not) coexist with
                    // the measurement build. It still runs in a normal debug build
                    // (no `work-counters`), so the invariant protection is retained.
                    #[cfg(all(debug_assertions, not(feature = "work-counters")))]
                    self.debug_assert_trusted_prefix(
                        table, predicates, projection, schema_opt, cap, &results,
                    )
                    .await?;
                    return Ok(results);
                }
            }
        }

        // Short stream: reconcile against the authoritative materializing scan so
        // a `scan_stream`/`scan` divergence can never drop a row. Roll the scan
        // counters back to the pre-stream baseline first (SUGGESTION-3) so the
        // re-scan below is the only work charged to `QUERY_ROWS_SCANNED` for this
        // path — the partial stream's examined rows are not double-counted.
        drop(scan_stream);
        context.rows_processed = processed_baseline;
        context.scan_rows = scan_rows_baseline;
        let authoritative = self
            .storage
            .scan(table, None, None, None, schema_opt)
            .await?;
        collect_capped_accepted(
            authoritative,
            predicates,
            projection,
            schema_opt,
            cap,
            context,
        )
    }

    /// Debug-only guard for the trusted full-cap stream fast path in
    /// [`capped_fallback_scan`](Self::capped_fallback_scan) (issue #1577).
    ///
    /// Re-runs the AUTHORITATIVE materializing `scan` (whose rows are token-sorted
    /// via `sort_by_token_order`), builds its first-`cap` ACCEPTED row keys, and
    /// asserts they equal the trusted stream's row keys in order. A supported
    /// writer emits rows in token order, so the two prefixes match; a future
    /// writer that violates the invariant trips this `debug_assert` in debug/tests
    /// instead of silently returning misordered rows.
    ///
    /// Compiled only under `debug_assertions`, so it has zero release perf cost.
    /// Also compiled out under the `work-counters` measurement build: its
    /// authoritative verification `scan` parses the whole fixture and would
    /// pollute the decode-work counters that build exists to measure. The guard
    /// still runs in a normal debug build (no `work-counters`).
    #[cfg(all(debug_assertions, not(feature = "work-counters")))]
    async fn debug_assert_trusted_prefix(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        schema_opt: Option<&TableSchema>,
        cap: usize,
        trusted: &[QueryRow],
    ) -> Result<()> {
        let authoritative = self
            .storage
            .scan(table, None, None, None, schema_opt)
            .await?;
        let mut expected: Vec<RowKey> = Vec::with_capacity(cap);
        for (key, value) in authoritative {
            if expected.len() >= cap {
                break;
            }
            if let Some(row) = build_row_from_scan(key, value, projection, schema_opt) {
                if evaluate_predicates(&row, predicates)? {
                    expected.push(row.key);
                }
            }
        }
        let got: Vec<RowKey> = trusted.iter().map(|r| r.key.clone()).collect();
        debug_assert_eq!(
            got, expected,
            "issue #1577 invariant violated: the trusted full-cap `scan_stream` prefix \
             diverged from the authoritative token-ordered `scan` prefix. Every supported \
             writer must emit rows in token order (Cassandra 5.0 on-disk files are \
             token-ordered; CQLite's memtable is a token-ordered BTreeMap; compaction \
             output is k-way token-merged). A writer that violates this must sort before \
             emit, or the full-cap stream fast path returns rows in the wrong order."
        );
        Ok(())
    }
}

/// Collect the first `cap` ACCEPTED rows from an already-materialized scan
/// result, in scan order, counting each EXAMINED row exactly once in `context`
/// (issue #1577).
///
/// This is the shared body of the short-stream reconciliation branch of
/// [`SelectExecutor::capped_fallback_scan`]: after a short `scan_stream` it is
/// fed the AUTHORITATIVE materializing `scan` and returns the exact first-`cap`
/// window `execute_limit` will slice. A row suppressed by
/// [`build_row_from_scan`] (a marker / tombstone) or rejected by
/// [`evaluate_predicates`] is skipped and never counted toward `cap`, so the cap
/// means "enough ACCEPTED rows" — identical to the streaming accept loop, so the
/// reconciled result is byte-identical to the trusted stream's.
fn collect_capped_accepted(
    rows: Vec<(RowKey, ScanRow)>,
    predicates: &[SSTablePredicate],
    projection: &[String],
    schema_opt: Option<&TableSchema>,
    cap: usize,
    context: &mut ExecutionContext,
) -> Result<Vec<QueryRow>> {
    let mut out = Vec::with_capacity(cap.min(rows.len()));
    for (key, value) in rows {
        if out.len() >= cap {
            break;
        }
        context.rows_processed += 1;
        context.scan_rows += 1;
        let Some(row) = build_row_from_scan(key, value, projection, schema_opt) else {
            continue;
        };
        if evaluate_predicates(&row, predicates)? {
            out.push(row);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{collect_capped_accepted, scan_pushdown_cap, ExecutionContext};
    use crate::query::select_ast::{
        ColumnRef, ComparisonExpression, ComparisonOperator, ComparisonRightSide, OrderByClause,
        OrderByItem, SelectClause, SelectExpression, SortDirection, WhereExpression,
    };
    use crate::query::select_optimizer::{
        AggregateComputation, AggregationPlan, ExecutionStep, SSTablePredicate,
    };
    use crate::types::{RowKey, ScanRow, TableId, Value};
    use std::sync::Arc;

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

    // ── Short-stream reconciliation logic (issue #1577, IMPORTANT-1) ────────────
    //
    // `capped_fallback_scan` discards a SHORT `scan_stream` and reconciles by
    // re-running the authoritative materializing `scan`, feeding its rows to
    // `collect_capped_accepted`. That is the branch where a `scan_stream`/`scan`
    // divergence must NOT drop or misorder a row — but the integration fixtures
    // use single-reader tables whose stream reaches the cap (fast path), so the
    // reconciliation LOGIC never runs there. This unit test drives it directly
    // with a synthetic authoritative scan that holds MORE accepted rows than the
    // cap (the exact situation a divergent short stream leaves behind), and proves
    // it returns the correct first-`cap` ACCEPTED rows in scan order.

    fn exec_context() -> ExecutionContext {
        ExecutionContext {
            table_id: TableId::new("ks.t"),
            columns: Vec::new(),
            rows_processed: 0,
            scan_rows: 0,
            projection_flags: Default::default(),
            access_path: None,
            reverse_served: false,
        }
    }

    /// A live scan row carrying a single `name` text cell (no schema, so
    /// `build_row_from_scan` surfaces the cell verbatim and reconstructs no
    /// partition-key columns — accept/suppress is controlled purely by
    /// `Row` vs `Marker`).
    fn live(key: &[u8], name: &str) -> (RowKey, ScanRow) {
        (
            RowKey::new(key.to_vec()),
            ScanRow::Row(vec![(Arc::from("name"), Value::Text(name.to_string()))]),
        )
    }

    /// A suppressed marker row (row tombstone / null row): must be skipped and
    /// never counted toward the cap.
    fn marker(key: &[u8]) -> (RowKey, ScanRow) {
        (RowKey::new(key.to_vec()), ScanRow::Marker(Value::Null))
    }

    #[test]
    fn reconcile_returns_first_cap_accepted_rows_in_order() {
        // Authoritative scan has 5 live rows (more than the cap of 3), with a
        // suppressed marker interleaved after the first live row.
        let authoritative = vec![
            live(b"k0", "a"),
            marker(b"k0m"), // suppressed: must not consume a cap slot
            live(b"k1", "b"),
            live(b"k2", "c"),
            live(b"k3", "d"),
            live(b"k4", "e"),
        ];
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        let out = collect_capped_accepted(authoritative, &preds, &[], None, 3, &mut ctx)
            .expect("reconciliation must not error");

        // Exactly the first 3 ACCEPTED rows, in scan order (marker skipped).
        let keys: Vec<Vec<u8>> = out.iter().map(|r| r.key.0.clone()).collect();
        assert_eq!(
            keys,
            vec![b"k0".to_vec(), b"k1".to_vec(), b"k2".to_vec()],
            "reconciliation must return the first `cap` ACCEPTED rows in scan order"
        );
        // The marker was examined (and counted) but never filled a cap slot; the
        // loop stops examining once the cap is reached, so exactly 4 rows were
        // examined (k0, marker, k1, k2) — not the whole authoritative scan.
        assert_eq!(ctx.scan_rows, 4, "examined rows counted once, stops at cap");
        assert_eq!(ctx.rows_processed, 4);
    }

    #[test]
    fn reconcile_short_of_cap_returns_all_accepted() {
        // Fewer accepted rows than the cap: return every accepted row (a genuinely
        // small table — the non-divergent short-stream case).
        let authoritative = vec![live(b"k0", "a"), marker(b"k0m"), live(b"k1", "b")];
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        let out = collect_capped_accepted(authoritative, &preds, &[], None, 100, &mut ctx)
            .expect("reconciliation must not error");

        let keys: Vec<Vec<u8>> = out.iter().map(|r| r.key.0.clone()).collect();
        assert_eq!(keys, vec![b"k0".to_vec(), b"k1".to_vec()]);
        assert_eq!(ctx.scan_rows, 3, "all three entries examined once");
    }

    #[test]
    fn reconcile_cap_zero_returns_empty_without_examining() {
        let authoritative = vec![live(b"k0", "a"), live(b"k1", "b")];
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        let out = collect_capped_accepted(authoritative, &preds, &[], None, 0, &mut ctx)
            .expect("reconciliation must not error");

        assert!(out.is_empty(), "cap 0 accepts no rows");
        assert_eq!(ctx.scan_rows, 0, "cap 0 examines no rows");
    }
}
