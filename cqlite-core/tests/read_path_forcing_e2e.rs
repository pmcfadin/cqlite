//! Issue #1918: end-to-end wiring evidence for the read-path forcing knob.
//!
//! These tests exercise the knob through the REAL public read surface
//! (`Database::execute`, config-wired via `QueryConfig::forced_read_path`) — not a
//! helper-only unit test — and assert the observable contract via the per-query
//! `QueryResult.metadata.access_path` and the public `Error` surface:
//!
//!   * forced `full` on a targeted query records `FallbackFullScan{ForcedFullScan}`
//!     and returns the SAME rows as `auto`;
//!   * forced `point` on a non-targeted query (`SELECT *`, no partition key) fails
//!     closed with `Error::ForcedReadPathUnavailable`, returning NO rows;
//!   * forced `point` on a fully-constrained `WHERE pk = ?` runs the targeted path.
//!
//! Assertions read the PER-QUERY `metadata.access_path` (not the process-global
//! `access_path::last()` probe), so the three `#[tokio::test]`s are safe to run in
//! parallel. Against a committed INT-partition-key fixture; SKIP-loud when the
//! fixture is absent, fail-closed under `CQLITE_REQUIRE_FIXTURES=1`.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{AccessPath, FallbackReason};
use cqlite_core::query::result::QueryRow;
use cqlite_core::{Config, Database, Error};

/// A committed single-INT-partition-key fixture with surviving live rows.
const KEYSPACE: &str = "test_compaction_tombstone_ttl";
const TABLE: &str = "shadow_row_delete";
const SCHEMA: &str = "compaction-tombstone-ttl-parity.cql";
const PK_COLUMN: &str = "id";

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// TABLE-granular fixture-root resolution, shared with the sibling dataset lanes
// (issue #3220): this file used to carry a private, byte-identical copy of a
// KEYSPACE-granular `sstables_root` + `table_has_data` pair, which selects a corpus
// root that may not hold the table and then reports the table as absent.
#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

fn schema_path() -> Option<PathBuf> {
    datasets_root::schema_path(SCHEMA)
}

async fn open_db(root: &Path, schema: &Path, mode: Option<ReadPathMode>) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = mode;
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

fn normalize(rows: &[QueryRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let sorted: BTreeMap<&str, String> = row
                .values
                .iter()
                .map(|(k, v)| (k.as_ref(), format!("{v:?}")))
                .collect();
            format!("{sorted:?}")
        })
        .collect()
}

/// Resolve the fixture paths, or `None` (SKIP / fail-closed) when absent.
fn resolve() -> Option<(PathBuf, PathBuf)> {
    let Some(root) = sstables_root_for_table(KEYSPACE, TABLE) else {
        handle_absent(&describe_search(KEYSPACE, TABLE));
        return None;
    };
    let Some(schema) = schema_path() else {
        handle_absent("schema absent");
        return None;
    };
    Some((root, schema))
}

fn handle_absent(msg: &str) {
    if require_fixtures() {
        panic!("REQUIRE_FIXTURES: {KEYSPACE}.{TABLE}: {msg}");
    }
    eprintln!("SKIP {KEYSPACE}.{TABLE}: {msg}");
}

/// Discover the first live INT partition key via an unforced (auto) scan.
async fn first_live_pk(db: &Database) -> Option<i64> {
    let result = db
        .execute(&format!("SELECT {PK_COLUMN} FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("discovery SELECT succeeds");
    result.rows.iter().find_map(|row| {
        row.values.get(PK_COLUMN).and_then(|v| match v {
            cqlite_core::types::Value::Integer(i) => Some(*i as i64),
            cqlite_core::types::Value::BigInt(i) => Some(*i),
            _ => None,
        })
    })
}

#[tokio::test]
async fn forced_full_records_forced_fallback_and_matches_auto_rows() {
    let Some((root, schema)) = resolve() else {
        return;
    };
    let auto_db = open_db(&root, &schema, None).await;
    let Some(pk) = first_live_pk(&auto_db).await else {
        panic!("fixture {KEYSPACE}.{TABLE} has no live partition to point-query");
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE} WHERE {PK_COLUMN} = {pk}");

    // auto: baseline rows.
    let auto_rows = normalize(&auto_db.execute(&query).await.expect("auto SELECT").rows);

    // full: same query, forced full scan. Records the DISTINCT forced fallback,
    // and returns byte-identical rows/values/order to auto. Assert on the
    // PER-QUERY `metadata.access_path` (not the process-global probe, which races
    // across these parallel `#[tokio::test]`s).
    let full_db = open_db(&root, &schema, Some(ReadPathMode::Full)).await;
    let full_result = full_db.execute(&query).await.expect("full SELECT");
    assert_eq!(
        full_result.metadata.access_path,
        Some(AccessPath::FallbackFullScan {
            reason: FallbackReason::ForcedFullScan
        }),
        "forced full must record FallbackFullScan{{ForcedFullScan}}"
    );
    assert_eq!(
        auto_rows,
        normalize(&full_result.rows),
        "forced full must return the same rows/values/order as auto"
    );
}

#[tokio::test]
async fn forced_point_on_non_targeted_query_fails_closed() {
    let Some((root, schema)) = resolve() else {
        return;
    };
    let point_db = open_db(&root, &schema, Some(ReadPathMode::Point)).await;
    // A full-table SELECT (no partition key constraint) is NOT partition-targeted,
    // so forced point must fail closed rather than silently full-scan.
    let err = point_db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect_err("forced point on a full-table scan must fail closed");
    match err {
        Error::ForcedReadPathUnavailable { forced, reason } => {
            assert_eq!(forced, "point");
            assert_eq!(reason, "partition_key_not_fully_constrained");
        }
        other => panic!("expected ForcedReadPathUnavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn forced_point_on_full_pk_takes_targeted_path() {
    let Some((root, schema)) = resolve() else {
        return;
    };
    let auto_db = open_db(&root, &schema, None).await;
    let Some(pk) = first_live_pk(&auto_db).await else {
        panic!("fixture {KEYSPACE}.{TABLE} has no live partition to point-query");
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE} WHERE {PK_COLUMN} = {pk}");
    let auto_rows = normalize(&auto_db.execute(&query).await.expect("auto SELECT").rows);

    let point_db = open_db(&root, &schema, Some(ReadPathMode::Point)).await;
    let result = point_db
        .execute(&query)
        .await
        .expect("forced point on a fully-constrained pk must succeed");
    // Per-query metadata (race-free across the parallel tests).
    let path = result
        .metadata
        .access_path
        .clone()
        .expect("a path must be recorded");
    assert!(
        path.is_targeted(),
        "forced point on a full pk must take a targeted path, got {path:?}"
    );
    assert!(
        !path.is_full_scan(),
        "forced point must not fall back to a full scan, got {path:?}"
    );
    // And the rows still match auto (forcing governs routing only, not decoding).
    assert_eq!(auto_rows, normalize(&result.rows));
}
