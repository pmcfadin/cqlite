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
    /// any BTI (`da`) reader (whose trie-walk `bti_scan_with_metadata` decodes the
    /// whole index-less reconciled table before streaming) and the whole-scan
    /// `tombstones` build. (The `write-support` cross-generation merge is NOT one:
    /// since #1579 it streams lazily via `stream_generations_for_read`, so a bounded
    /// consumer decode-stops there.) For the pre-materializing branches the storage
    /// layer decodes the ENTIRE table, so
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
    /// This single-generation lazy path is the ONLY unsafe one (multi-generation
    /// returns above through the authoritative `scan`, so it is prefix-correct BY
    /// CONSTRUCTION), so its trusted prefix is protected by a RELEASE-active,
    /// cost-bounded `(token, key)`-monotonicity guard ([`prefix_is_token_ordered`]):
    /// on a detected `scan_stream`/`scan` divergence it logs loudly and falls back
    /// to the authoritative `scan` rather than ever returning the wrong rows.
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
        // for the genuinely-streaming single-generation merge AND, since #1579, for
        // the lazy `write-support` cross-generation merge (which streams via
        // `generation_merge::stream_generations_for_read`). But some `scan_stream`
        // branches PRE-MATERIALIZE the entire reconciled result before handing back
        // the channel — any BTI (`da`) reader (trie-walk `bti_scan_with_metadata`
        // decodes the whole index-less table before streaming) and the whole-scan
        // `tombstones` build. For those the storage layer decodes
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
                    // the producer stops parsing the remaining (unneeded) rows. So
                    // we return the stream's first `cap` rows WITHOUT re-running the
                    // authoritative `scan` (which would decode the whole table and
                    // defeat D1's entire purpose).
                    //
                    // INVARIANT — this GENUINELY-LAZY single-generation branch is
                    // the ONLY unsafe path. Here `scan` (materializing, token-sorted
                    // via `sort_by_token_order`) and `scan_stream` (a SEPARATE lazy
                    // pipeline that does NOT sort — it trusts on-disk order == token
                    // order) are kept in lockstep only by PARITY TESTS, not by
                    // construction. (By contrast, the multi-generation branch above
                    // returned early through the authoritative `scan`, so it is
                    // prefix-correct BY CONSTRUCTION and needs no guard here.)
                    // Returning the lazy prefix is sound ONLY because every SUPPORTED
                    // writer emits rows in token order:
                    //   * Cassandra 5.0 SSTable `Data.db` files are token-ordered on
                    //     disk;
                    //   * CQLite's memtable is a token-ordered `BTreeMap`, so every
                    //     flushed generation is token-ordered;
                    //   * compaction output is a k-way TOKEN merge of token-ordered
                    //     inputs.
                    //
                    // ── Release-active, cost-bounded guard (issue #1577, owner
                    //    2026-07-06) ─────────────────────────────────────────────
                    // A prior version verified this invariant ONLY under a
                    // `debug_assert`, so a RELEASE build ran with NO check and could
                    // silently return the WRONG rows if the two pipelines ever
                    // diverged. We now run a cheap O(cap) check on EVERY build: the
                    // authoritative `scan` emits rows in ascending `(token, key)`
                    // order, so if this trusted prefix is NOT itself
                    // `(token, key)`-monotonic it cannot be that authoritative
                    // prefix — a divergence. The check costs `cap` murmur3 hashes
                    // (`cap` == LIMIT+OFFSET, the work already done to receive the
                    // rows), so it NEVER decodes the tail and preserves the
                    // decode-stop win.
                    //
                    // On a detected divergence we NEVER return the possibly-wrong
                    // prefix: we log LOUDLY and fall back to the authoritative
                    // materializing `scan` (the exact rows the non-pushdown path
                    // returns), rolling the scan counters back to baseline first so
                    // the reconcile charges `QUERY_ROWS_SCANNED` exactly once. That
                    // fallback decodes the whole table, but it runs ONLY on the
                    // (should-be-impossible, for supported writers) divergence — the
                    // happy path stays decode-bounded.
                    //
                    // RESIDUAL RISK (named per the invariant doc): the O(cap) check
                    // proves the prefix is INTERNALLY token-monotonic, which catches
                    // any SYSTEMATIC reordering (insertion / clustering / reverse
                    // order all yield a non-monotonic prefix on realistic data). It
                    // does NOT catch a divergence that leaves the prefix
                    // coincidentally monotonic while a lower-token row sits just past
                    // the cap boundary (a single transposition straddling `cap`) —
                    // detecting that needs the tail we deliberately skip. The
                    // debug-only `debug_assert_trusted_prefix` below closes that
                    // residual gap exhaustively (full authoritative parity) in
                    // debug/test builds.
                    if !prefix_is_token_ordered(&results) {
                        log::error!(
                            "issue #1577 invariant violated in RELEASE-active guard: the trusted \
                             full-cap `scan_stream` prefix is not in ascending (token, key) order, \
                             so it cannot be the authoritative token-ordered `scan` prefix — a \
                             `scan_stream`/`scan` divergence on the single-generation lazy path. \
                             Every supported writer must emit rows in token order. Falling back to \
                             the authoritative materializing `scan` to guarantee correct rows."
                        );
                        drop(scan_stream);
                        return self
                            .reconcile_via_authoritative_scan(
                                table,
                                predicates,
                                projection,
                                schema_opt,
                                cap,
                                context,
                                processed_baseline,
                                scan_rows_baseline,
                            )
                            .await;
                    }

                    // Exhaustive debug-only guard: re-runs the authoritative
                    // token-ordered `scan` and verifies the WHOLE trusted prefix
                    // (catching the residual-risk case the O(cap) check cannot). It
                    // is compiled out of release (zero release cost) AND of the
                    // `work-counters` measurement build, whose sole job is to count
                    // decode work — the guard's verification `scan` parses the whole
                    // fixture and would pollute the `PARTITION_HEADER_TRY_PARSES`
                    // counter the decode-stop test asserts. It still runs in a normal
                    // debug build (no `work-counters`).
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
        // a `scan_stream`/`scan` divergence can never drop a row. The shared
        // `reconcile_via_authoritative_scan` rolls the scan counters back to the
        // pre-stream baseline first (SUGGESTION-3) so the re-scan is the only work
        // charged to `QUERY_ROWS_SCANNED` for this path — the partial stream's
        // examined rows are not double-counted.
        drop(scan_stream);
        self.reconcile_via_authoritative_scan(
            table,
            predicates,
            projection,
            schema_opt,
            cap,
            context,
            processed_baseline,
            scan_rows_baseline,
        )
        .await
    }

    /// Reconcile a bounded scan against the AUTHORITATIVE materializing `scan`
    /// (issue #1577) — the single fallback shared by BOTH non-decode-bounded
    /// branches of [`capped_fallback_scan`](Self::capped_fallback_scan): the
    /// SHORT-STREAM branch (the stream ended before the cap) and the
    /// RELEASE-active divergence guard (the trusted prefix was not token-ordered).
    ///
    /// It first rolls the scan counters back to the pre-stream baseline so the
    /// partial stream's per-row increments are not double-counted, then re-runs the
    /// fully-materializing `scan` (the exact rows the non-pushdown path returns) and
    /// routes it through the SAME `collect_capped_materialized` accountant the
    /// materializing `execute_sstable_scan` paths use. That charges
    /// `context.scan_rows` (→ `QUERY_ROWS_SCANNED`) with the TRUE decoded count
    /// (`authoritative.len()`) UP FRONT — not just the `cap` rows it examines —
    /// while the `cap` still bounds `rows_processed` and the returned window, so
    /// RESULTS and LIMIT/OFFSET semantics are byte-identical to the unbounded scan.
    /// This branch is NOT decode-bounded (unlike the trusted full-cap fast path,
    /// which drops its stream at the cap); it fully materializes the table, so it is
    /// only ever reached when decode-stop is impossible or must be abandoned for
    /// correctness.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile_via_authoritative_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        schema_opt: Option<&TableSchema>,
        cap: usize,
        context: &mut ExecutionContext,
        processed_baseline: u64,
        scan_rows_baseline: u64,
    ) -> Result<Vec<QueryRow>> {
        context.rows_processed = processed_baseline;
        context.scan_rows = scan_rows_baseline;
        let authoritative = self
            .storage
            .scan(table, None, None, None, schema_opt)
            .await?;
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

/// Cheap O(cap) release-active guard for the trusted full-cap `scan_stream`
/// prefix (issue #1577, owner 2026-07-06).
///
/// The authoritative materializing `scan` emits rows in ascending
/// `(token, key_bytes)` order (`kway_merge_token_order` / `sort_by_token_order`,
/// where `token = cassandra_murmur3_token(partition_key)`). A trusted lazy prefix
/// can only equal that authoritative prefix if it is ITSELF `(token, key)`-
/// monotonic, so this verifies exactly that — the comparison is NON-strict
/// (`<=`), because a partition's clustering rows share one `(token, key)` and
/// arrive contiguously (see `sort_by_token_order`'s stability note). Returns
/// `false` on the first descending adjacent pair (a `scan_stream`/`scan`
/// divergence on the single-generation lazy path).
///
/// Cost is `cap` murmur3 hashes — proportional to the rows already received, so it
/// never decodes the tail and preserves D1's decode-stop optimization. It cannot
/// see rows past the cap, so it catches SYSTEMATIC reordering but not a single
/// transposition straddling the cap boundary (see `capped_fallback_scan`'s
/// residual-risk note; the debug-only exhaustive guard closes that gap).
fn prefix_is_token_ordered(rows: &[QueryRow]) -> bool {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;
    let mut prev: Option<(i64, &[u8])> = None;
    for row in rows {
        let key = row.key.0.as_slice();
        let token = cassandra_murmur3_token(key);
        if let Some(prev) = prev {
            if prev > (token, key) {
                return false;
            }
        }
        prev = Some((token, key));
    }
    true
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
mod tests;
