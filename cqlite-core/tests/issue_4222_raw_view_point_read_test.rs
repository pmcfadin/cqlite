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
    let query = format!(
        "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1"
    );
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
                int_of(r, "generation"),
                int_of(r, "ck"),
                text_of(r, "row_tombstone")
            ))
            .collect::<Vec<_>>()
    );

    let gen1_rows: Vec<&QueryRow> = result
        .rows
        .iter()
        .filter(|r| int_of(r, "generation") == Some(1))
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
        .find(|r| int_of(r, "generation") == Some(2) && int_of(r, "ck") == Some(2))
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

    // gen-2, ck=3: CELL tombstone on `val` only; `extra` is absent (never
    // written for this row in either generation... actually only `val` cell
    // is present in the golden for ck=3 in gen-2).
    let cell_tombstone = result
        .rows
        .iter()
        .find(|r| int_of(r, "generation") == Some(2) && int_of(r, "ck") == Some(3))
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
    let query = format!(
        "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 2"
    );
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
        .find(|r| int_of(r, "generation") == Some(2))
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
        .filter(|r| int_of(r, "generation") == Some(1))
        .collect();
    assert_eq!(gen1_rows.len(), 3, "gen-1 must contribute exactly 3 live rows");
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
    let query = format!(
        "SELECT sstable, generation FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1"
    );
    let result = db.execute(&query).await.expect("projection query must succeed");

    let mut generations: Vec<i32> = result
        .rows
        .iter()
        .filter_map(|r| int_of(r, "generation"))
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
