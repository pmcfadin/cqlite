//! Issue #4222 — raw SSTable view, joinability via correlated queries.
//!
//! Design.md D4: literal SQL `JOIN` does not execute anywhere in this engine
//! (tracked separately, issue #4249) — "joinable" (AC5) is satisfied by
//! running the logical `SELECT` and the raw-view `SELECT` for the SAME key
//! and asserting they correlate on shared key-column values/types, and that
//! the logical row's non-key values equal the reconciled-winner physical
//! row's values.
//!
//! Fixture: `test_compactionparity.live_clustering`
//! (`test-data/schemas/compaction-parity.cql`), git-committed — `must_run`.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

const KEYSPACE: &str = "test_compactionparity";
const TABLE: &str = "live_clustering";

async fn open_fixture_db() -> Database {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "committed fixture must resolve (issue #3220, fail-closed): {}",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let schema = schema_path("compaction-parity.cql")
        .expect("committed schema compaction-parity.cql must be readable (#3148)");
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
        "the committed schema must load"
    );
    result.database
}

fn get<'a>(row: &'a QueryRow, col: &str) -> Option<&'a Value> {
    row.values.get(col)
}

/// Spec: "The logical row and its physical rows are correlated by shared key
/// values" — every physical row's key-column values equal the logical row's
/// key-column values exactly (same names, same types, same encoding), and
/// the logical row's non-key values equal the reconciled-winner physical
/// row's values — the correlation an external `JOIN ... USING (<pk>, <ck>)`
/// would perform if the engine supported it (design.md D4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logical_row_correlates_with_its_physical_rows_by_shared_keys() {
    let db = open_fixture_db().await;

    let logical_query = format!("SELECT id, ck, v FROM {KEYSPACE}.{TABLE} WHERE id = 1 AND ck = 0");
    let logical = db
        .execute(&logical_query)
        .await
        .expect("logical SELECT must succeed");
    assert_eq!(
        logical.rows.len(),
        1,
        "a fully-constrained pk+ck predicate must reconcile to exactly one logical row"
    );
    let logical_row = &logical.rows[0];

    let raw_query = format!(
        "SELECT id, ck, v, generation FROM {KEYSPACE}.{TABLE}_raw_sstable_data \
         WHERE id = 1 AND ck = 0"
    );
    let raw = db
        .execute(&raw_query)
        .await
        .expect("raw-view SELECT must succeed for the SAME key");
    assert!(
        !raw.rows.is_empty(),
        "the raw view must return at least the ONE generation that produced the logical winner"
    );

    // Every physical row's key columns must equal the logical row's key
    // columns exactly (same names, types, encoding).
    for physical in &raw.rows {
        assert_eq!(
            get(physical, "id"),
            get(logical_row, "id"),
            "physical row's id must equal the logical row's id"
        );
        assert_eq!(
            get(physical, "ck"),
            get(logical_row, "ck"),
            "physical row's ck must equal the logical row's ck"
        );
    }

    // The reconciled-winner physical row (the one with the highest
    // generation, since this is a single-writer LWW fixture with no
    // tombstones on this key) must match the logical row's `v` value.
    let winner = raw
        .rows
        .iter()
        .max_by_key(|r| match get(r, "generation") {
            Some(Value::BigInt(g)) => *g,
            _ => i64::MIN,
        })
        .expect("at least one physical row must be present");
    assert_eq!(
        get(winner, "v"),
        get(logical_row, "v"),
        "the reconciled-winner generation's physical value must equal the logical SELECT's value"
    );
}
