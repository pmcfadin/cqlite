//! Issue #4628: SELECT over a table whose tombstone and data live in different
//! generations, with an ABSENT generation in between, must match the live Cassandra 5.0.8
//! CL=ALL read (`test-data/fixtures/issue_4628/live-select-consistency-all.txt`).
//!
//! The merger refilled runs by candidate index while absent candidates contribute no run,
//! so the point-read path stopped reading the data generation after its first row. Runs
//! under `Auto` (the user-facing path) and `Full` (the scan reference), per #3890 both ways.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures/issue_4628")
}

async fn open(mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![fixture().join("schema.cql")],
        data_dir: fixture().join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingestion succeeds")
    .database
}

/// `(key, live row count)` from the CL=ALL oracle.
const LIVE_ROWS: [(i32, usize); 6] = [(1, 0), (3, 4), (4, 5), (5, 6), (6, 1), (9, 6)];

#[tokio::test]
async fn select_matches_live_cl_all_for_cross_generation_tombstones() {
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open(mode).await;
        for (key, want) in LIVE_ROWS {
            let result = db
                .execute(&format!("SELECT * FROM fix4628.t WHERE id = {key}"))
                .await
                .unwrap_or_else(|e| panic!("SELECT key {key} ({name}) failed: {e}"));
            assert_eq!(
                result.rows.len(),
                want,
                "key {key} ({name}): row count differs from the live CL=ALL read"
            );
        }
    }
}
