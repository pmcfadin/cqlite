//! Issue #4222 — raw SSTable view, point-key row producer.
//!
//! Real Cassandra 5.0 (BIG/`nb`) fixture: `test_tomb.resurrection_gc_positive`
//! (`test-data/schemas/tombstone-parity.cql`, Table 5), 2 generations:
//!   * gen-1 (`nb-1`): pk=1 ck=1..5 all LIVE; pk=2 ck=1..3 all LIVE.
//!   * gen-2 (`nb-2`): pk=1 ck=2 ROW-tombstoned, pk=1 ck=3 CELL-tombstoned
//!     (`val` column only); pk=2 fully PARTITION-tombstoned (no rows).
//!
//! This single fixture exercises all three tombstone kinds the raw view's
//! column contract (design.md D7) must expose distinctly, plus the core
//! "one row per physical row per generation, no reconciliation" contract
//! (spec's `resurrection_gc_positive` scenario).
//!
//! Oracle: physical-dump parity (#1742) — expected values are read directly
//! from the committed `*-Data.db.jsonl` sstabledump goldens, not hardcoded
//! from CQLite's own prior output (#3041/#3042).
//!
//! Fixture discipline (#3220): resolved per TABLE via the shared resolver;
//! binaries are git-committed (see `test-data/datasets/sstables/test_tomb/`),
//! so this lane is `must_run` — a SKIP is a harness defect, not a legitimate
//! outcome. `CQLITE_DATASETS_ROOT` must point at a checkout carrying them
//! (the worktree doctrine: point it at the MAIN checkout's `test-data/datasets`).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use chrono::DateTime;
use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

const KEYSPACE: &str = "test_tomb";
const TABLE: &str = "resurrection_gc_positive";

async fn open_fixture_db() -> Database {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "committed fixture must resolve (issue #3220, fail-closed): {}",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let schema = schema_path("tombstone-parity.cql")
        .expect("committed schema tombstone-parity.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load, else the raw view would refuse with Error::Schema"
    );
    result.database
}

/// Parse an sstabledump JSONL RFC3339 timestamp into epoch MICROSECONDS —
/// the same unit `<col>_timestamp`/`row_timestamp` etc. report.
fn iso_to_micros(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp_micros()
}

/// Parse an sstabledump JSONL RFC3339 timestamp into epoch SECONDS — the
/// unit `<col>_local_deletion_time`/`row_local_deletion_time`/
/// `partition_deletion_time` report.
fn iso_to_secs(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp()
}

fn get<'a>(row: &'a QueryRow, col: &str) -> Option<&'a Value> {
    row.values.get(col)
}

fn text_of(row: &QueryRow, col: &str) -> Option<String> {
    match get(row, col) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    }
}

fn int_of(row: &QueryRow, col: &str) -> Option<i32> {
    match get(row, col) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }
}

fn bigint_of(row: &QueryRow, col: &str) -> Option<i64> {
    match get(row, col) {
        Some(Value::BigInt(i)) => Some(*i),
        _ => None,
    }
}

/// Spec: "A key updated across two generations yields one row per generation"
/// — `test_tomb.resurrection_gc_positive WHERE pk = 1` must return every
/// physical row from BOTH generations (5 live gen-1 rows + 2 gen-2 rows: one
/// row-tombstone, one cell-tombstone), never reconciled into fewer rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_key_yields_one_row_per_generation_no_reconciliation() {
    let db = open_fixture_db().await;
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1");
    let result = db
        .execute(&query)
        .await
        .expect("raw view point-key query must succeed for a resolvable table");

    assert_eq!(
        result.rows.len(),
        7,
        "pk=1 must yield 5 gen-1 live rows + 2 gen-2 rows (row-tombstone ck=2, \
         cell-tombstone ck=3) — got: {:?}",
        result
            .rows
            .iter()
            .map(|r| (
                bigint_of(r, "generation"),
                int_of(r, "ck"),
                text_of(r, "row_tombstone")
            ))
            .collect::<Vec<_>>()
    );

    let gen1_rows: Vec<&QueryRow> = result
        .rows
        .iter()
        .filter(|r| bigint_of(r, "generation") == Some(1))
        .collect();
    assert_eq!(gen1_rows.len(), 5, "gen-1 must contribute exactly 5 rows");
    for row in &gen1_rows {
        assert_eq!(text_of(row, "row_kind").as_deref(), Some("row"));
        assert_eq!(
            text_of(row, "row_tombstone"),
            None,
            "gen-1 rows are all live — no row tombstone"
        );
        assert!(
            text_of(row, "sstable")
                .unwrap_or_default()
                .starts_with("nb-1-"),
            "gen-1 rows must be sourced from the nb-1 SSTable"
        );
        assert_eq!(text_of(row, "format").as_deref(), Some("big"));
    }

    // gen-2, ck=2: ROW tombstone (deletion_info at the row level, no cells).
    let row_tombstone = result
        .rows
        .iter()
        .find(|r| bigint_of(r, "generation") == Some(2) && int_of(r, "ck") == Some(2))
        .expect("gen-2 ck=2 row-tombstone row must be present");
    assert_eq!(text_of(row_tombstone, "row_kind").as_deref(), Some("row"));
    assert_eq!(
        text_of(row_tombstone, "row_tombstone").as_deref(),
        Some("row"),
        "ck=2 in gen-2 is a whole-row delete"
    );
    assert_eq!(
        int_of(row_tombstone, "row_local_deletion_time"),
        Some(iso_to_secs("2026-06-24T22:59:14Z") as i32),
        "row_local_deletion_time must match the golden's local_delete_time byte-exact"
    );
    assert_eq!(
        text_of(row_tombstone, "val_tombstone"),
        None,
        "a row tombstone is recorded once at the row level, not duplicated per cell"
    );

    // gen-2, ck=3: CELL tombstone on `val` only — the golden's ck=3 row in
    // gen-2 carries just the `val` cell (tombstoned); `extra` has no cell at
    // all for this row in gen-2.
    let cell_tombstone = result
        .rows
        .iter()
        .find(|r| bigint_of(r, "generation") == Some(2) && int_of(r, "ck") == Some(3))
        .expect("gen-2 ck=3 cell-tombstone row must be present");
    assert_eq!(text_of(cell_tombstone, "row_kind").as_deref(), Some("row"));
    assert_eq!(
        text_of(cell_tombstone, "row_tombstone"),
        None,
        "ck=3 in gen-2 carries a CELL tombstone, not a row tombstone"
    );
    assert_eq!(
        text_of(cell_tombstone, "val_tombstone").as_deref(),
        Some("cell"),
        "val's cell tombstone kind must be reported"
    );
    assert_eq!(
        int_of(cell_tombstone, "val_local_deletion_time"),
        Some(iso_to_secs("2026-06-24T22:59:14Z") as i32),
        "val_local_deletion_time must match the golden's local_delete_time byte-exact"
    );
    assert_eq!(
        bigint_of(cell_tombstone, "val_timestamp"),
        Some(iso_to_micros("2021-01-02T00:00:00Z")),
        "val_timestamp must match the golden's tstamp byte-exact (microseconds)"
    );
}

/// Spec: "A partition tombstone is visible even when the generation holds no
/// live rows" — `pk = 2` is fully partition-deleted in gen-2 (0 rows on
/// disk), so the raw view must still emit exactly ONE synthetic row for that
/// generation carrying the partition deletion facts, alongside gen-1's 3 live
/// rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_tombstone_generation_still_yields_one_row() {
    let db = open_fixture_db().await;
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 2");
    let result = db
        .execute(&query)
        .await
        .expect("raw view point-key query must succeed");

    assert_eq!(
        result.rows.len(),
        4,
        "pk=2 must yield 3 gen-1 live rows + 1 gen-2 partition-tombstone row"
    );

    let partition_tombstone = result
        .rows
        .iter()
        .find(|r| bigint_of(r, "generation") == Some(2))
        .expect("gen-2's partition-tombstone row must be present even with zero on-disk rows");
    assert_eq!(
        text_of(partition_tombstone, "row_kind").as_deref(),
        Some("partition_tombstone")
    );
    assert_eq!(
        bigint_of(partition_tombstone, "partition_deletion_timestamp"),
        Some(iso_to_micros("2021-01-02T00:00:00Z")),
        "partition_deletion_timestamp must match the golden's marked_deleted byte-exact"
    );
    assert_eq!(
        bigint_of(partition_tombstone, "partition_deletion_time"),
        Some(iso_to_secs("2026-06-24T22:59:14Z")),
        "partition_deletion_time must match the golden's local_delete_time byte-exact"
    );
    // Every cell/clustering column is NULL on a partition-tombstone row
    // (AC2's "still yields one row" case).
    assert_eq!(get(partition_tombstone, "ck"), None);
    assert_eq!(get(partition_tombstone, "val"), None);

    let gen1_rows: Vec<&QueryRow> = result
        .rows
        .iter()
        .filter(|r| bigint_of(r, "generation") == Some(1))
        .collect();
    assert_eq!(
        gen1_rows.len(),
        3,
        "gen-1 must contribute exactly 3 live rows"
    );
}

/// Spec: "`SELECT DISTINCT sstable` answers 'which generations hold this
/// key'" (design.md D2, folding #4205's SSTable-generation enumeration into
/// this view). Literal SQL `DISTINCT` is a general query-engine capability
/// this raw-view slice does not add (the interception in `execute()` bypasses
/// the normal DISTINCT execution step entirely — see design.md D6); the
/// underlying fact the requirement cares about — a plain `sstable`/
/// `generation` projection carries enough information for a caller to derive
/// the distinct generation set trivially — is what this case demonstrates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sstable_generation_projection_answers_which_generations_hold_the_key() {
    let db = open_fixture_db().await;
    let query =
        format!("SELECT sstable, generation FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1");
    let result = db
        .execute(&query)
        .await
        .expect("projection query must succeed");

    let mut generations: Vec<i64> = result
        .rows
        .iter()
        .filter_map(|r| bigint_of(r, "generation"))
        .collect();
    generations.sort_unstable();
    generations.dedup();
    assert_eq!(
        generations,
        vec![1, 2],
        "pk=1 is present in both generations — every row must be tagged with its \
         real source generation"
    );
}

/// Roborev finding (issue #4222, High): the raw-view interception bypassed
/// the execution-step pipeline entirely, so `LIMIT`/`OFFSET` were silently
/// ignored — `SELECT * FROM ..._raw_sstable_data LIMIT 1` returned every
/// physical row instead of one. This is the spec's own first scenario's
/// shape ("`SELECT * FROM ...LIMIT 1`... returning at least one row").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_and_offset_are_honored() {
    let db = open_fixture_db().await;

    let limited = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1 LIMIT 1"
        ))
        .await
        .expect("LIMIT 1 must succeed");
    assert_eq!(
        limited.rows.len(),
        1,
        "LIMIT 1 must return EXACTLY one row, not the whole 7-row corpus for pk=1"
    );

    let unlimited = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1"
        ))
        .await
        .expect("unlimited query must succeed");
    assert_eq!(
        unlimited.rows.len(),
        7,
        "sanity: unlimited still returns all 7 rows"
    );

    let offset_query = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1 LIMIT 3 OFFSET 2"
        ))
        .await
        .expect("LIMIT+OFFSET must succeed");
    assert_eq!(
        offset_query.rows.len(),
        3,
        "LIMIT 3 OFFSET 2 must return exactly 3 rows"
    );
}

/// Roborev finding (issue #4222): ORDER BY / DISTINCT / aggregates over the
/// raw view are out of scope for this slice (design.md D4's JOIN-gap
/// precedent) and must fail CLOSED with a typed error, never silently
/// ignore the clause and return an unordered/unreduced result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_and_distinct_fail_closed_rather_than_silently_ignored() {
    let db = open_fixture_db().await;

    let order_by = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1 ORDER BY ck DESC"
        ))
        .await;
    assert!(
        order_by.is_err(),
        "ORDER BY over the raw view must fail closed, not silently return unordered rows"
    );

    let distinct = db
        .execute(&format!(
            "SELECT DISTINCT sstable FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1"
        ))
        .await;
    assert!(
        distinct.is_err(),
        "SELECT DISTINCT over the raw view must fail closed, not silently return duplicates"
    );
}

/// Roborev finding (issue #4222, Medium): a synthesized metadata-column name
/// colliding with a real base-table column must fail closed (D8), never
/// silently clobber one of the two. `resurrection_gc_positive` has no such
/// collision, so this asserts the POSITIVE case works; the collision case
/// itself is unit-tested directly against `raw_view_columns` in
/// `cqlite-core/src/query/select_executor/raw_view/columns.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selecting_an_unknown_column_fails_closed() {
    let db = open_fixture_db().await;
    let outcome = db
        .execute(&format!(
            "SELECT pk, this_column_does_not_exist FROM {KEYSPACE}.{TABLE}_raw_sstable_data \
             WHERE pk = 1"
        ))
        .await;
    assert!(
        outcome.is_err(),
        "SELECTing a column absent from the raw view's contract must fail closed, not \
         silently drop it from the result"
    );
}

/// Spec: "A dropped-column generation shows the on-disk column the current
/// schema no longer has" — the DELIVERED subset (see spec.md's DEFERRED
/// note): `test_tomb.dropped_regular_col`'s CURRENT registered schema still
/// declares `drop_col`, so decoding with it already surfaces gen-1's
/// `drop_col` cell correctly and correctly omits it from gen-2 (which
/// genuinely has no such column on-disk), with no special `dropped` marker
/// bookkeeping — that marker itself is deferred (roborev finding, #4222).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gen1_drop_col_visible_gen2_drop_col_absent() {
    let root = sstables_root_for_table("test_tomb", "dropped_regular_col").unwrap_or_else(|| {
        panic!(
            "committed fixture must resolve (issue #3220, fail-closed): {}",
            describe_search("test_tomb", "dropped_regular_col")
        )
    });
    let schema = schema_path("tombstone-parity.cql")
        .expect("committed schema tombstone-parity.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some("/test_tomb/".to_string()),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(result.schema_load_result.schemas_loaded > 0);
    let db = result.database;

    let query = "SELECT generation, ck, drop_col, keep_col \
                 FROM test_tomb.dropped_regular_col_raw_sstable_data WHERE pk = 1";
    let result = db
        .execute(query)
        .await
        .expect("raw view query must succeed");

    let gen1_ck1 = result
        .rows
        .iter()
        .find(|r| bigint_of(r, "generation") == Some(1) && int_of(r, "ck") == Some(1))
        .expect("gen-1 ck=1 row must be present");
    assert_eq!(
        text_of(gen1_ck1, "drop_col").as_deref(),
        Some("drop_a_1"),
        "gen-1's drop_col cell must be visible under the CURRENT (undropped) schema"
    );

    let gen2_ck4 = result
        .rows
        .iter()
        .find(|r| bigint_of(r, "generation") == Some(2) && int_of(r, "ck") == Some(4))
        .expect("gen-2 ck=4 row must be present");
    assert_eq!(
        get(gen2_ck4, "drop_col"),
        None,
        "gen-2 genuinely has no drop_col cell on-disk — must be absent, not fabricated as NULL"
    );
    assert_eq!(
        text_of(gen2_ck4, "keep_col").as_deref(),
        Some("keep_b_4"),
        "keep_col (never dropped) must still decode normally in gen-2"
    );
}

/// Roborev finding (issue #4222, High): `collect_sstable_predicates`
/// deliberately skips OR/NOT branches when building `plan.sstable_predicates`;
/// the base pipeline compensates with a residual `Filter` step this view's
/// early return never reaches. A WHERE clause containing OR/NOT anywhere in
/// its tree must fail closed rather than silently return every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn where_clause_with_or_fails_closed() {
    let db = open_fixture_db().await;
    let outcome = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1 OR pk = 2"
        ))
        .await;
    assert!(
        outcome.is_err(),
        "REGRESSION (issue #4222): a WHERE clause containing OR must fail closed, not \
         silently return every row of every partition"
    );
}

/// Roborev finding (issue #4222, Medium): `PER PARTITION LIMIT` must fail
/// closed like ORDER BY/DISTINCT/aggregates, never silently ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_partition_limit_fails_closed() {
    let db = open_fixture_db().await;
    let outcome = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data PER PARTITION LIMIT 1"
        ))
        .await;
    assert!(
        outcome.is_err(),
        "REGRESSION (issue #4222): PER PARTITION LIMIT must fail closed, not be silently \
         ignored"
    );
}

/// Roborev finding (issue #4222, High, round 2 of this class): OR/NOT are
/// NOT the only way a comparison silently fails to lower to a pushed-down
/// predicate — `!=` never lowers at all (`column_comparison_to_predicate`
/// has no `NotEqual` arm), even inside a perfectly pushable AND position,
/// so a naive "contains OR/NOT" check would miss it entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn where_clause_with_not_equal_fails_closed() {
    let db = open_fixture_db().await;
    let outcome = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1 AND val != 'x'"
        ))
        .await;
    assert!(
        outcome.is_err(),
        "REGRESSION (issue #4222): a WHERE clause containing != must fail closed — it never \
         lowers to a pushed-down predicate even inside an AND, so it would otherwise be \
         silently dropped and every row of the matched partition-key predicate returned \
         unfiltered"
    );
}
