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
    /// # Pre-materializing stream branches (roborev round-4)
    ///
    /// Some [`scan_stream`](crate::storage::StorageEngine::scan_stream) branches
    /// PRE-MATERIALIZE the whole reconciled result before returning the channel —
    /// the `write-support` cross-generation merge (more than one generation + a
    /// resolved schema, via `merge_generations_for_read`) and the whole-scan
    /// `tombstones` build. For those the storage layer decodes the ENTIRE table, so
    /// the lazy per-received-row accounting below would under-report
    /// `QUERY_ROWS_SCANNED` to ~`cap`. This method asks the storage layer
    /// ([`scan_stream_materializes`](crate::storage::StorageEngine::scan_stream_materializes),
    /// which owns the branch condition) up front and, when the stream would
    /// pre-materialize, routes through the fully-materializing `scan` +
    /// [`collect_capped_materialized`] (which charges the TRUE decoded count while
    /// the `cap` bounds `rows_processed` and the returned window). No decode-stop is
    /// possible in that case, so nothing is lost and results are byte-identical.
    ///
    /// The remainder of this method is the GENUINELY-LAZY single-generation path.
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
    /// Because that re-run `scan` fully materializes the table, this branch charges
    /// `QUERY_ROWS_SCANNED` the FULL decoded count via `collect_capped_materialized`
    /// (not just `cap`), so the scan-work metric reflects the real work even when
    /// the reconciled table holds more than `cap` accepted rows.
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

        // ── Pre-materializing stream branches (issue #1577, roborev round-4) ──────
        //
        // The lazy fast path below assumes `scan_stream` is LAZY: it charges
        // `context.scan_rows` per RECEIVED row and drops the stream at the cap, so
        // dropping the channel stops the producer decoding the tail. That is TRUE
        // only for the genuinely-streaming single-generation merge. But some
        // `scan_stream` branches PRE-MATERIALIZE the entire reconciled result before
        // handing back the channel — the `write-support` cross-generation merge
        // (`readers.len() > 1 && schema present`, via `merge_generations_for_read`)
        // and the whole-scan `tombstones` build. For those the storage layer decodes
        // the ENTIRE table regardless of the cap, so consuming lazily and charging
        // per-received-row would report only ~`cap` rows to `QUERY_ROWS_SCANNED`
        // while the true decode work is the full table — a metric regression.
        //
        // Ask the storage layer (which owns the branch condition — no duplicated
        // storage-internal logic here) whether `scan_stream` would pre-materialize
        // for this table + schema. If so, route through the fully-materializing
        // `scan` + the shared `collect_capped_materialized` accountant, which
        // charges the TRUE decoded count (`materialized.len()`) up front while the
        // `cap` still bounds `rows_processed` and the returned window. There is no
        // decode-stop to lose here (the storage already decoded everything), and the
        // authoritative `scan` yields byte-identical rows, so RESULTS and
        // LIMIT/OFFSET semantics are unchanged — only the accounting path differs.
        if self
            .storage
            .scan_stream_materializes(table, schema_opt)
            .await
        {
            let materialized = self
                .storage
                .scan(table, None, None, None, schema_opt)
                .await?;
            return collect_capped_materialized(
                materialized,
                Some(cap),
                predicates,
                context,
                |(key, value)| build_row_from_scan(key, value, projection, schema_opt),
            );
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
        // Metric accuracy (issue #1577, roborev round-3 finding): the re-run
        // `scan` FULLY MATERIALIZES/decodes every row of the table before
        // returning it. This branch is therefore NOT decode-bounded (unlike the
        // trusted full-cap stream fast path above, which drops the stream at the
        // cap). Route it through the SAME `collect_capped_materialized` accountant
        // the materializing `execute_sstable_scan` paths use, so it charges
        // `context.scan_rows` (→ `QUERY_ROWS_SCANNED`) with the TRUE decoded count
        // (`authoritative.len()`) UP FRONT — not just the `cap` rows it examines.
        // The `cap` still bounds `rows_processed` and the returned window, so
        // RESULTS and LIMIT/OFFSET semantics are unchanged; only the scan-work
        // metric is corrected. Before this, `collect_capped_accepted` counted only
        // up to `cap`, so a reconciliation over a table with more than `cap`
        // accepted rows under-reported `QUERY_ROWS_SCANNED` to at most `LIMIT + OFFSET`.
        collect_capped_materialized(
            authoritative,
            Some(cap),
            predicates,
            context,
            |(key, value)| build_row_from_scan(key, value, projection, schema_opt),
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
        use crate::types::RowKey;
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

/// Collect the ACCEPTED rows from an ALREADY-MATERIALIZED scan, applying an
/// optional LIMIT+OFFSET `cap` (issue #1577; roborev metric-accounting fix).
///
/// The storage layer decoded EVERY entry before returning `rows`, so this
/// charges `context.scan_rows` — the sole source of the `QUERY_ROWS_SCANNED`
/// metric — with the TRUE decoded count (`rows.len()`) UP FRONT. The `cap` only
/// bounds the per-row BUILD/predicate work (`context.rows_processed`) and the
/// size of the returned window; it must NOT shrink the scan-work metric, or the
/// metric would under-report the scan the storage layer actually performed.
///
/// The materializing `execute_sstable_scan` metadata / partition-targeted paths
/// AND the short-stream reconciliation branch of
/// [`SelectExecutor::capped_fallback_scan`] (which re-runs the fully-materializing
/// `scan`) all route through here, so the full-decode accounting is applied
/// uniformly and cannot drift per call site. This is deliberately distinct from
/// the TRULY decode-bounded trusted full-cap stream fast path in
/// `capped_fallback_scan`, which drops its bounded stream at the cap and so
/// legitimately charges `scan_rows` only for the rows it actually decoded.
///
/// `build` maps a materialized entry to an optional [`QueryRow`] (`None` = a
/// suppressed marker / tombstone, per [`build_row_from_scan`]); the metadata
/// caller uses it to attach per-cell metadata BEFORE predicate evaluation. A row
/// that `build` suppresses or [`evaluate_predicates`] rejects is skipped and
/// never counted toward `cap`, so the cap means "enough ACCEPTED rows" and can
/// never under-deliver a match.
pub(super) fn collect_capped_materialized<T>(
    rows: Vec<T>,
    cap: Option<usize>,
    predicates: &[SSTablePredicate],
    context: &mut ExecutionContext,
    mut build: impl FnMut(T) -> Option<QueryRow>,
) -> Result<Vec<QueryRow>> {
    // Metric accuracy (issue #1577): the WHOLE scan was materialized/decoded, so
    // charge the full decoded count regardless of the downstream cap. Charging
    // per-iteration inside the capped loop below would stop at the cap and make
    // `QUERY_ROWS_SCANNED` under-report to at most `LIMIT + OFFSET`.
    let total = rows.len();
    context.scan_rows += total as u64;

    let mut out = Vec::with_capacity(cap.map_or(total, |c| c.min(total)));
    for entry in rows {
        if let Some(c) = cap {
            if out.len() >= c {
                break;
            }
        }
        context.rows_processed += 1;
        let Some(row) = build(entry) else {
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
    use super::{
        build_row_from_scan, collect_capped_materialized, scan_pushdown_cap, ExecutionContext,
    };
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

    // ── Short-stream reconciliation logic (issue #1577, IMPORTANT-1 + roborev
    //    round-3 metric-accounting finding) ───────────────────────────────────
    //
    // `capped_fallback_scan` discards a SHORT `scan_stream` and reconciles by
    // re-running the authoritative FULLY-MATERIALIZING `scan`, feeding its rows to
    // `collect_capped_materialized` (the SAME accountant the materializing
    // `execute_sstable_scan` paths use). These unit tests drive that exact call
    // shape directly — a synthetic authoritative scan fed to
    // `collect_capped_materialized(authoritative, Some(cap), …, build_row_from_scan)`
    // — because the integration fixtures use single-reader tables whose stream
    // reaches the cap (fast path), so the reconciliation LOGIC never runs there.
    // They prove BOTH invariants: (a) results are the first-`cap` ACCEPTED rows in
    // scan order (the divergence-safety guarantee) AND (b) `scan_rows` is charged
    // the FULL decoded count `authoritative.len()`, not just `cap` (the round-3
    // finding — the re-run scan is NOT decode-bounded, so under-charging it to
    // `LIMIT + OFFSET` under-reported `QUERY_ROWS_SCANNED`).

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

    /// The exact call shape `capped_fallback_scan`'s reconciliation branch now
    /// makes: `collect_capped_materialized(authoritative, Some(cap), …,
    /// build_row_from_scan)`. Mirrors the branch (which maps the authoritative
    /// `(RowKey, ScanRow)` scan through `build_row_from_scan`).
    fn reconcile(
        authoritative: Vec<(RowKey, ScanRow)>,
        cap: usize,
        ctx: &mut ExecutionContext,
    ) -> Vec<super::QueryRow> {
        let preds: Vec<SSTablePredicate> = Vec::new();
        collect_capped_materialized(authoritative, Some(cap), &preds, ctx, |(key, value)| {
            build_row_from_scan(key, value, &[], None)
        })
        .expect("reconciliation must not error")
    }

    #[test]
    fn reconcile_charges_full_authoritative_count_when_over_cap() {
        // Authoritative scan has 5 live rows (MORE than the cap of 3), with a
        // suppressed marker interleaved after the first live row — the exact
        // situation a divergent short stream leaves for reconciliation.
        let authoritative = vec![
            live(b"k0", "a"),
            marker(b"k0m"), // suppressed: must not consume a cap slot
            live(b"k1", "b"),
            live(b"k2", "c"),
            live(b"k3", "d"),
            live(b"k4", "e"),
        ];
        let decoded = authoritative.len() as u64; // 6 — the real scan work
        let mut ctx = exec_context();

        let out = reconcile(authoritative, 3, &mut ctx);

        // Results stay CAPPED: exactly the first 3 ACCEPTED rows, in scan order
        // (marker skipped). The fix must NOT change output or LIMIT/OFFSET semantics.
        let keys: Vec<Vec<u8>> = out.iter().map(|r| r.key.0.clone()).collect();
        assert_eq!(
            keys,
            vec![b"k0".to_vec(), b"k1".to_vec(), b"k2".to_vec()],
            "reconciliation must return the first `cap` ACCEPTED rows in scan order"
        );
        // ROUND-3 FIX: the re-run `scan` fully decoded all 6 rows, so `scan_rows`
        // (→ `QUERY_ROWS_SCANNED`) must charge the FULL count (6), NOT the cap (3)
        // and NOT the pre-fix capped-examination count (4).
        assert_eq!(
            ctx.scan_rows, decoded,
            "reconciliation must charge QUERY_ROWS_SCANNED the full materialized \
             decode count, not the LIMIT+OFFSET cap"
        );
        assert!(
            ctx.scan_rows > out.len() as u64,
            "reconciled scan-work metric must exceed the returned/capped row count"
        );
        // Per-row BUILD work stays bounded by the cap (k0, marker, k1, k2 examined).
        assert_eq!(
            ctx.rows_processed, 4,
            "per-row build work is bounded by the cap even though scan_rows is full"
        );
    }

    #[test]
    fn reconcile_short_of_cap_returns_all_accepted() {
        // Fewer accepted rows than the cap: return every accepted row (a genuinely
        // small table — the non-divergent short-stream case). Full count == examined
        // count here, so both accounting styles agree; still pinned for regression.
        let authoritative = vec![live(b"k0", "a"), marker(b"k0m"), live(b"k1", "b")];
        let decoded = authoritative.len() as u64;
        let mut ctx = exec_context();

        let out = reconcile(authoritative, 100, &mut ctx);

        let keys: Vec<Vec<u8>> = out.iter().map(|r| r.key.0.clone()).collect();
        assert_eq!(keys, vec![b"k0".to_vec(), b"k1".to_vec()]);
        assert_eq!(
            ctx.scan_rows, decoded,
            "all three entries decoded and counted"
        );
    }

    #[test]
    fn reconcile_cap_zero_charges_full_scan_but_returns_empty() {
        // `capped_fallback_scan` guards `cap == 0` before ever re-scanning, so this
        // exercises the accountant directly: even at cap 0 the whole scan was
        // decoded, so the full count is charged while no rows are returned.
        let authoritative = vec![live(b"k0", "a"), live(b"k1", "b")];
        let decoded = authoritative.len() as u64;
        let mut ctx = exec_context();

        let out = reconcile(authoritative, 0, &mut ctx);

        assert!(out.is_empty(), "cap 0 accepts no rows");
        assert_eq!(
            ctx.scan_rows, decoded,
            "a materialized scan decoded every row even when the cap accepts none"
        );
        assert_eq!(ctx.rows_processed, 0, "cap 0 builds no rows");
    }

    // ── Materialized-scan metric accounting (issue #1577 roborev finding) ───────
    //
    // The metadata / partition-targeted scan paths receive an ALREADY-MATERIALIZED
    // `Vec` — the storage layer decoded EVERY row before returning it. The old
    // per-row `scan_rows += 1` inside the capped loop `break`s at the cap, so
    // `QUERY_ROWS_SCANNED` under-reported to at most `LIMIT + OFFSET` even though
    // the whole scan was decoded. `collect_capped_materialized` charges the FULL
    // decoded count up front; these tests pin that the metric reflects real scan
    // work, NOT the cap, while results + per-row build work stay correctly capped.

    #[test]
    fn materialized_charges_full_decoded_count_not_the_cap() {
        // 5 live rows + 1 suppressed marker = 6 rows the storage layer decoded.
        let materialized = vec![
            live(b"k0", "a"),
            marker(b"k0m"), // suppressed by build_row_from_scan
            live(b"k1", "b"),
            live(b"k2", "c"),
            live(b"k3", "d"),
            live(b"k4", "e"),
        ];
        let decoded = materialized.len() as u64;
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        // A LIMIT+OFFSET cap far below the decoded count.
        let out =
            collect_capped_materialized(materialized, Some(3), &preds, &mut ctx, |(key, value)| {
                build_row_from_scan(key, value, &[], None)
            })
            .expect("materialized collect must not error");

        // Results + per-row build work stay CAPPED (the fix must not change output
        // or LIMIT/OFFSET semantics).
        let keys: Vec<Vec<u8>> = out.iter().map(|r| r.key.0.clone()).collect();
        assert_eq!(
            keys,
            vec![b"k0".to_vec(), b"k1".to_vec(), b"k2".to_vec()],
            "cap must still bound the accepted-row window to the first `cap` rows"
        );
        assert_eq!(
            ctx.rows_processed, 4,
            "per-row BUILD work is bounded by the cap (k0, marker, k1, k2 examined)"
        );

        // The metric must reflect the FULL decoded scan (6), NOT the cap (3) and
        // NOT the capped-examination count (4) — this is the roborev fix.
        assert_eq!(
            ctx.scan_rows, decoded,
            "QUERY_ROWS_SCANNED must charge the full materialized decode count, not \
             the LIMIT+OFFSET cap"
        );
        assert!(
            ctx.scan_rows > out.len() as u64,
            "materialized scan-work metric must exceed the returned/capped row count"
        );
    }

    #[test]
    fn materialized_metadata_build_suppression_still_counts_full_scan() {
        // Mirror the metadata path: the `build` closure may attach per-cell
        // metadata and may suppress a marker (returning None). A suppressed row is
        // still part of the decoded scan, so it must remain counted in scan_rows.
        let materialized = vec![live(b"k0", "a"), marker(b"k0m"), live(b"k1", "b")];
        let decoded = materialized.len() as u64;
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        let out = collect_capped_materialized(
            materialized,
            Some(1),
            &preds,
            &mut ctx,
            // Metadata-shaped closure (build may return None for a suppressed row).
            |(key, value)| build_row_from_scan(key, value, &[], None),
        )
        .expect("materialized collect must not error");

        assert_eq!(out.len(), 1, "cap 1 returns exactly one accepted row");
        assert_eq!(
            ctx.scan_rows, decoded,
            "a marker suppressed by build is still a decoded row and stays counted"
        );
    }

    #[test]
    fn materialized_uncapped_counts_and_returns_all_accepted() {
        // With no cap (scan_cap == None) the full-count accounting must equal the
        // legacy per-row behaviour: every decoded row counted, every live row
        // returned.
        let materialized = vec![live(b"k0", "a"), marker(b"k0m"), live(b"k1", "b")];
        let decoded = materialized.len() as u64;
        let preds: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();

        let out =
            collect_capped_materialized(materialized, None, &preds, &mut ctx, |(key, value)| {
                build_row_from_scan(key, value, &[], None)
            })
            .expect("materialized collect must not error");

        assert_eq!(out.len(), 2, "both live rows returned when uncapped");
        assert_eq!(ctx.scan_rows, decoded, "every decoded row counted once");
        assert_eq!(
            ctx.rows_processed, decoded,
            "every decoded row build-examined"
        );
    }

    // ── Multi-generation pre-materializing accounting (issue #1577 roborev
    //    round-4 finding) ─────────────────────────────────────────────────────
    //
    // ORIGINAL BUG: `capped_fallback_scan`'s "trusted full-cap stream fast path"
    // assumed `scan_stream` is LAZY and charged `scan_rows` per RECEIVED row,
    // stopping at the cap. But the `write-support` cross-generation `scan_stream`
    // branch (>1 generation + schema present) PRE-MATERIALIZES the entire
    // reconciled result via `merge_generations_for_read` before returning the
    // channel. So a multi-generation `SELECT ... LIMIT n` decoded the WHOLE table
    // but `QUERY_ROWS_SCANNED` reported only ~n — a metric regression.
    //
    // This test builds a REAL 2-generation table (write path is byte-parity with
    // Cassandra, M5), calls `capped_fallback_scan` directly with a resolved schema,
    // and asserts the scan-work metric charges the FULL decoded count (not ~cap)
    // while the returned window stays correctly capped. It complements the
    // result-parity coverage in `tests/issue_1577_capped_fallback_branches.rs`
    // (which cannot observe `context.scan_rows`).
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn multi_generation_capped_scan_charges_full_decoded_count() {
        use crate::query::select_executor::SelectExecutor;
        use crate::schema::{Column, KeyColumn, SchemaManager, TableSchema};
        use crate::storage::write_engine::{
            CellOperation, Mutation, PartitionKey, TableId as WriteTableId, WriteEngine,
            WriteEngineConfig,
        };
        use crate::storage::StorageEngine;
        use crate::{Config, Platform};
        use std::collections::HashMap;
        use std::sync::Arc;
        use tempfile::TempDir;

        const KEYSPACE: &str = "test_capped_lib";
        const TABLE: &str = "items";
        const N_GENS: i32 = 2;
        const ROWS_PER_GEN: i32 = 5; // DISTINCT partitions per generation → no overlap.
        const CAP: usize = 3; // well below the total decoded row count.

        fn items_schema() -> TableSchema {
            TableSchema {
                keyspace: KEYSPACE.to_string(),
                table: TABLE.to_string(),
                partition_keys: vec![KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                }],
                clustering_keys: vec![],
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default: None,
                        is_static: false,
                    },
                    Column {
                        name: "value".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    },
                ],
                comments: HashMap::new(),
                dropped_columns: HashMap::new(),
            }
        }

        let tmp = TempDir::new().expect("tmp");
        let data_dir = tmp.path().join("data");
        let wal_dir = tmp.path().join("wal");

        // Build N_GENS SSTable generations, flushing between each so no compaction
        // merges them. Each generation gets ROWS_PER_GEN DISTINCT partitions
        // (`id = gen*100 + i`), so the reconciled table holds exactly
        // N_GENS * ROWS_PER_GEN rows — no cross-generation overlap.
        {
            let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), items_schema());
            let mut engine = WriteEngine::new(config).expect("write engine");
            for gen in 1..=N_GENS {
                for i in 0..ROWS_PER_GEN {
                    let id = gen * 100 + i;
                    let m = Mutation::new(
                        WriteTableId::new(KEYSPACE, TABLE),
                        PartitionKey::single("id", Value::Integer(id)),
                        None,
                        vec![CellOperation::Write {
                            column: "value".to_string(),
                            value: Value::Text(format!("v{id}")),
                        }],
                        1_000 + id as i64,
                        None,
                    );
                    engine.write_async(m).await.expect("write partition");
                }
                engine.flush().await.expect("flush generation");
            }
            let table_dir = data_dir.join(KEYSPACE).join(TABLE);
            for gen in 1..=N_GENS {
                assert!(
                    table_dir.join(format!("nb-{gen}-big-Data.db")).exists(),
                    "generation {gen} must exist on disk (multi-generation required)"
                );
            }
        }

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let storage = Arc::new(
            StorageEngine::open(
                &data_dir,
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .expect("open storage"),
        );

        let schema = items_schema();
        let table_id = TableId::new(format!("{KEYSPACE}.{TABLE}"));

        // Guard: the storage layer must report that `scan_stream` PRE-MATERIALIZES
        // for this multi-generation + schema table — the exact condition the fix
        // routes on. If this ever became false the test would be vacuous.
        assert!(
            storage
                .scan_stream_materializes(&table_id, Some(&schema))
                .await,
            "multi-generation + schema table must pre-materialize scan_stream \
             (else the round-4 metric bug cannot occur and this test is vacuous)"
        );

        // Oracle: the authoritative reconciled decode count (what the storage layer
        // actually decodes) — exact, never `>=`, so a 0/low-rows regression fails.
        let decoded = storage
            .scan(&table_id, None, None, None, Some(&schema))
            .await
            .expect("oracle scan")
            .len() as u64;
        assert_eq!(
            decoded,
            (N_GENS * ROWS_PER_GEN) as u64,
            "distinct partitions across generations → full decoded row count"
        );
        assert!(
            decoded > CAP as u64,
            "the table must hold more than `cap` rows to expose the metric bug"
        );

        let schema_mgr = Arc::new(
            SchemaManager::new_with_storage(Arc::clone(&storage), &config)
                .await
                .expect("schema manager"),
        );
        let executor = SelectExecutor::new(schema_mgr, Arc::clone(&storage));

        let projection: Vec<String> = Vec::new();
        let predicates: Vec<SSTablePredicate> = Vec::new();
        let mut ctx = exec_context();
        ctx.table_id = table_id.clone();

        let out = executor
            .capped_fallback_scan(
                &table_id,
                &predicates,
                &projection,
                Some(&schema),
                CAP,
                &mut ctx,
            )
            .await
            .expect("capped fallback scan");

        // RESULTS stay CAPPED — the fix must not change LIMIT/OFFSET semantics.
        assert_eq!(
            out.len(),
            CAP,
            "the returned window must remain bounded by the cap"
        );
        // ROUND-4 FIX: the multi-generation stream pre-materialized the whole table,
        // so `scan_rows` (→ QUERY_ROWS_SCANNED) must charge the FULL decoded count,
        // NOT ~cap as the lazy per-received-row fast path did before the fix.
        assert_eq!(
            ctx.scan_rows, decoded,
            "multi-generation capped scan must charge the FULL decoded count to \
             QUERY_ROWS_SCANNED, not the LIMIT cap"
        );
        assert!(
            ctx.scan_rows > out.len() as u64,
            "scan-work metric must exceed the capped/returned row count"
        );
        // Per-row BUILD work stays bounded by the cap.
        assert_eq!(
            ctx.rows_processed, CAP as u64,
            "per-row build work stays bounded by the cap even though scan_rows is full"
        );
    }
}
