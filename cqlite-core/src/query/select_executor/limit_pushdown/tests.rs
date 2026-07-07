use super::{
    build_row_from_scan, collect_capped_materialized, prefix_is_token_ordered, scan_pushdown_cap,
    ExecutionContext, QueryRow,
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

// ── RELEASE-active full-cap prefix guard (issue #1577, owner 2026-07-06) ────
//
// These pin `prefix_is_token_ordered`, the O(cap) check that runs on EVERY
// build (release included) for the single-generation lazy full-cap fast path.
// They are NOT gated behind `debug_assertions`, so a normal `cargo test`
// (release-representative for this pure logic) exercises the exact guard a
// release binary runs. FAIL-FIRST / REVERT-VERIFY: neuter the guard (make
// `prefix_is_token_ordered` always return `true`) and
// `prefix_guard_rejects_descending_token_prefix` fails — proving it, not the
// data, catches the divergence.

/// Build a live `QueryRow` for `key` via the same `build_row_from_scan` the
/// executor uses, so `row.key` carries the exact bytes the guard hashes.
fn guard_row(key: &[u8]) -> QueryRow {
    let (k, v) = live(key, "x");
    build_row_from_scan(k, v, &[], None).expect("a live scan row must build")
}

#[test]
fn prefix_guard_accepts_token_ordered_prefix() {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;
    // Distinct keys sorted into the EXACT `(token, key)` order the
    // authoritative `scan` emits (`kway_merge_token_order` /
    // `sort_by_token_order`).
    let mut keys: Vec<Vec<u8>> = (0u8..8).map(|i| vec![i, 0xAB, i.wrapping_mul(7)]).collect();
    keys.sort_by(|a, b| {
        cassandra_murmur3_token(a)
            .cmp(&cassandra_murmur3_token(b))
            .then_with(|| a.cmp(b))
    });
    let asc: Vec<QueryRow> = keys.iter().map(|k| guard_row(k)).collect();
    assert!(
        prefix_is_token_ordered(&asc),
        "an ascending (token, key) prefix IS the authoritative scan prefix and must pass"
    );
}

#[test]
fn prefix_guard_rejects_descending_token_prefix() {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;
    // Same distinct keys, REVERSED — a strictly descending `(token, key)`
    // sequence, i.e. a `scan_stream`/`scan` divergence on the single-gen lazy
    // path. This assertion FAILS if the guard is neutered to always-true
    // (the revert-verify), proving the guard — not the fixture — is load-bearing.
    let mut keys: Vec<Vec<u8>> = (0u8..8).map(|i| vec![i, 0xAB, i.wrapping_mul(7)]).collect();
    keys.sort_by(|a, b| {
        cassandra_murmur3_token(a)
            .cmp(&cassandra_murmur3_token(b))
            .then_with(|| a.cmp(b))
    });
    let desc: Vec<QueryRow> = keys.iter().rev().map(|k| guard_row(k)).collect();
    assert!(
        !prefix_is_token_ordered(&desc),
        "a descending (token, key) prefix cannot be the authoritative scan prefix — the \
             release guard must reject it so the caller falls back instead of returning wrong rows"
    );
}

#[test]
fn prefix_guard_allows_empty_single_and_equal_keys() {
    // Boundary cases the guard must ACCEPT (they are trivially token-ordered).
    assert!(
        prefix_is_token_ordered(&[]),
        "empty prefix is vacuously ordered"
    );
    assert!(
        prefix_is_token_ordered(&[guard_row(b"solo")]),
        "a single-row prefix is trivially ordered"
    );
    // Equal partition keys: a partition's clustering rows share one
    // `(token, key)` and arrive contiguously, so the NON-strict `<=` comparison
    // must accept the repeat (a strict `<` would false-positive here).
    let dup = vec![guard_row(b"dup"), guard_row(b"dup"), guard_row(b"dup")];
    assert!(
        prefix_is_token_ordered(&dup),
        "equal (token, key) rows (clustering rows of one partition) are token-ordered"
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

    let out = collect_capped_materialized(materialized, None, &preds, &mut ctx, |(key, value)| {
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
