//! Issue #4222 — roborev round 11, finding F4: an `IN (...)` list that lowers
//! only PARTIALLY must be REFUSED, never silently narrowed.
//!
//! The raw view's "every leaf fully lowered" guard compared the WHERE clause's
//! pushable-comparison leaf count against `plan.sstable_predicates.len()`, on
//! the assumption that each successfully-lowered leaf contributes exactly one
//! predicate. True in general — but `select_optimizer`'s `In` arm
//! `filter_map`s the value list through `literal_value` and emits a predicate
//! as long as AT LEAST ONE element is a literal. So `WHERE pk IN (1, 2 + 3)`
//! yielded 1 leaf and 1 predicate, the count check passed, and the restriction
//! had silently become `pk IN (1)`: the view returned a SUBSET of the correct
//! rows with NO error — exactly the failure class the fail-closed guard exists
//! to prevent.
//!
//! Fixture: `test_comp.uncompressed_table` (`test-data/schemas/compression-parity.cql`
//! Table 5, `PRIMARY KEY (pk, ck)`) — git-committed, so this lane is
//! `must_run` (#3220) and never skips.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

const KEYSPACE: &str = "test_comp";
const TABLE: &str = "uncompressed_table";

async fn open_fixture_db() -> Database {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "committed fixture must resolve (issue #3220, fail-closed): {}",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let schema = schema_path("compression-parity.cql")
        .expect("committed schema compression-parity.cql must be readable (#3148)");
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

/// The regression: a mixed-literal `IN` list is REFUSED with a typed error,
/// rather than executing as the narrowed `pk IN (1)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partially_literal_in_list_is_refused_not_silently_narrowed() {
    let db = open_fixture_db().await;

    let outcome = db
        .execute(&format!(
            "SELECT pk, ck FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk IN (1, ck)"
        ))
        .await;
    let err = outcome.expect_err(
        "REGRESSION (issue #4222 F4): `pk IN (1, ck)` lowers to the NARROWED \
         `pk IN (1)`, so it must be REFUSED — it previously returned a silent subset",
    );
    // The refusal must come from the raw view's fail-closed lowering guard,
    // NOT from the CQL parser rejecting the element — otherwise this case
    // would pass VACUOUSLY without ever exercising the guard. (Found the hard
    // way: the finding's own illustrative `IN (1, 2 + 3)` does NOT parse —
    // "Expected RightParen, found Plus" — so it would have proved nothing. A
    // bare column reference DOES parse via
    // `parse_in_expression` -> `parse_select_expression`, and is exactly the
    // non-`Literal` shape the optimizer's `filter_map` silently drops.)
    let text = err.to_string();
    assert!(
        text.contains("_raw_sstable_data"),
        "the refusal must be the raw view's lowering guard, not an unrelated parse \
         failure — got: {text}"
    );
}

/// The positive control that makes the case above meaningful: an ALL-literal
/// `IN` list lowers completely and must still EXECUTE, returning BOTH listed
/// values — the fix must reject PARTIAL lowering, not `IN` as a whole, and
/// must not narrow a fully-lowered list either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_all_literal_in_list_still_executes_and_returns_both_listed_values() {
    let db = open_fixture_db().await;

    let all_rows = db
        .execute(&format!(
            "SELECT pk, ck FROM {KEYSPACE}.{TABLE}_raw_sstable_data"
        ))
        .await
        .expect("a full scan over the committed fixture must succeed");
    let mut cks: Vec<i32> = all_rows
        .rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(cqlite_core::types::Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    cks.sort_unstable();
    cks.dedup();
    assert!(
        cks.len() >= 2,
        "this control needs at least two distinct clustering values in the fixture, \
         found {cks:?}"
    );
    let (a, b) = (cks[0], cks[1]);

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE ck IN ({a}, {b})"
        ))
        .await
        .expect("an all-literal IN list must still be accepted over the raw view");
    let mut got: Vec<i32> = result
        .rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(cqlite_core::types::Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    got.sort_unstable();
    got.dedup();
    assert_eq!(
        got,
        vec![a, b],
        "an all-literal IN list must return BOTH listed clustering values — the fix \
         must not narrow or reject a fully-lowered restriction"
    );
}
