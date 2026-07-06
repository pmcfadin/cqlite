//! Issue #2069: a GLOBAL aggregate (a bare aggregate SELECT with NO GROUP BY)
//! over an EMPTY table must return exactly ONE row — matching Apache Cassandra
//! CQL semantics — not zero rows.
//!
//! Oracle (Cassandra 5.0 CQL semantics):
//!   - `COUNT(*)` / `COUNT(col)` over zero input rows = `0` (integer).
//!   - `MIN`/`MAX`/`SUM`/`AVG` over zero input rows = `NULL` (SUM of empty is
//!     NULL, NOT 0).
//!   - A global aggregate (no GROUP BY) ALWAYS produces exactly one row, even
//!     over zero input rows.
//!   - A GROUP BY query is DIFFERENT: it produces zero rows when there are no
//!     groups. This test pins that distinction as a regression guard.
//!
//! Both the O(1) streaming fold (`try_execute_global_aggregate`) and the buffered
//! path (`execute_aggregation`) must satisfy the rule; the buffered path is
//! exercised via a `LIMIT` clause that disqualifies the O(1) streaming fold.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_2069_global_aggregate_empty_table

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryResult;
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "agg2069_ks";
const TBL: &str = "metrics";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  i int,\n  t text\n);\n")
}

/// Build an EMPTY single-generation fixture (no rows) and open the full query
/// stack over it. An empty fixture registers the schema but flushes no SSTable.
async fn open_empty() -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            use cqlite_core::schema::parse_cql_schema;
            use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
            let config = WriteEngineConfig::new(data_dir, wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine");
            // An empty memtable flushes no generation; mirror the write path anyway.
            let _ = rt.block_on(engine.flush()).expect("flush");
            rt.block_on(engine.close()).expect("close");
        })
        .await
        .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    (result.database, temp_dir)
}

async fn run(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).await.expect("execute query")
}

/// Look up the aggregate value in the single result row by its OUTPUT column
/// name (the metadata name that pairs with the row value).
fn value_by_column(result: &QueryResult, col_name: &str) -> Value {
    assert_eq!(
        result.rows.len(),
        1,
        "global aggregate yields exactly one row"
    );
    result
        .rows
        .first()
        .and_then(|r| r.values.get(col_name))
        .cloned()
        .unwrap_or(Value::Null)
}

/// `SELECT COUNT(*) FROM <empty>` returns exactly one row whose value is 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_star_over_empty_table_is_one_row_zero() {
    let (db, _tmp) = open_empty().await;

    let result = run(&db, &format!("SELECT COUNT(*) FROM {KS}.{TBL}")).await;
    assert_eq!(
        result.rows.len(),
        1,
        "issue #2069: COUNT(*) over an empty table returns exactly one row"
    );
    let v = result.rows[0]
        .values
        .values()
        .next()
        .cloned()
        .expect("the one row has the COUNT value");
    assert_eq!(
        v,
        Value::BigInt(0),
        "issue #2069: COUNT(*) of empty input is 0"
    );
}

/// `SELECT MIN(i), MAX(i), SUM(i), AVG(i), COUNT(*) FROM <empty>` returns exactly
/// one row: the four extrema/sum/avg are NULL and COUNT is 0. The COUNT-vs-NULL
/// distinction is the crux — SUM of empty is NULL, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_aggregate_over_empty_table_null_extrema_zero_count() {
    let (db, _tmp) = open_empty().await;

    let result = run(
        &db,
        &format!("SELECT MIN(i), MAX(i), SUM(i), AVG(i), COUNT(*) FROM {KS}.{TBL}"),
    )
    .await;

    assert_eq!(
        result.rows.len(),
        1,
        "issue #2069: a global aggregate over an empty table returns exactly one row"
    );

    // Map each aggregate to its output column name so we assert per-function, not
    // by positional guesswork.
    let names: Vec<String> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(
        names.len(),
        5,
        "five aggregate output columns; got {names:?}"
    );

    // Independent of alias spelling: exactly one value must be BigInt(0) (COUNT)
    // and the other four must be NULL.
    let row = &result.rows[0];
    let mut zero_count = 0usize;
    let mut null_count = 0usize;
    for name in &names {
        match row
            .values
            .get(name.as_str())
            .cloned()
            .unwrap_or(Value::Null)
        {
            Value::BigInt(0) => zero_count += 1,
            Value::Null => null_count += 1,
            other => panic!(
                "issue #2069: unexpected value {other:?} for column {name:?}; \
                 empty global aggregate must be COUNT=0 + NULL extrema/sum/avg"
            ),
        }
    }
    assert_eq!(zero_count, 1, "exactly one COUNT(*)=0 value");
    assert_eq!(
        null_count, 4,
        "MIN/MAX/SUM/AVG of empty input are all NULL (SUM of empty is NULL, not 0)"
    );
}

/// The buffered aggregation path (`execute_aggregation`), not the O(1) fold,
/// must obey the same rule. A `LIMIT` clause adds a step that disqualifies the
/// streaming fold (`classify_global_aggregate` returns `None` for a `Limit`
/// step), so a `COUNT(*) ... LIMIT n` over an empty full scan routes through the
/// buffered path with zero input rows and must still emit one row with COUNT=0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_star_buffered_path_over_empty_is_one_row_zero() {
    let (db, _tmp) = open_empty().await;

    let result = run(&db, &format!("SELECT COUNT(*) FROM {KS}.{TBL} LIMIT 5")).await;
    assert_eq!(
        result.rows.len(),
        1,
        "issue #2069: buffered COUNT(*) over an empty table still returns one row"
    );
    let first_col = result.metadata.columns[0].name.clone();
    let v = value_by_column(&result, &first_col);
    assert_eq!(
        v,
        Value::BigInt(0),
        "issue #2069: buffered COUNT(*) of empty input is 0"
    );
}

/// Regression guard for the GLOBAL-vs-GROUP-BY distinction: a GROUP BY query over
/// an empty table produces ZERO rows (there are no groups), UNLIKE the global
/// aggregate. This must stay untouched by the #2069 fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_by_over_empty_table_is_zero_rows() {
    let (db, _tmp) = open_empty().await;

    let result = run(
        &db,
        &format!("SELECT id, COUNT(*) FROM {KS}.{TBL} GROUP BY id"),
    )
    .await;
    assert_eq!(
        result.rows.len(),
        0,
        "issue #2069: a GROUP BY query over an empty table returns zero rows \
         (no groups) — only the GROUP-BY-free global aggregate emits a single row"
    );
}
