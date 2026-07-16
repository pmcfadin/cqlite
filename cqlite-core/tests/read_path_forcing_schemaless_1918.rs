//! Issue #1918 (review fix): forced `full` MUST NOT silently diverge from `auto`
//! for a SCHEMA-LESS sole-pk point lookup.
//!
//! The forcing spec (`read-path-forcing/spec.md`) has an explicit SHALL: forced
//! `full`'s returned rows must be identical to `auto`'s. For a schema-less
//! `WHERE pk = <literal>` point read that only the specialized #1750 targeted seek
//! can serve, a general full scan CANNOT reconstruct the pk column from the row
//! bytes, so its per-row predicate backstop rejects every row and returns 0 rows —
//! a SILENT divergence from `auto` (which serves the row via the seek). Rather than
//! diverge silently, forced `full` now fails closed with
//! `Error::ForcedReadPathUnavailable`.
//!
//! This is the shape neither `point_vs_full_differential.rs` nor
//! `read_path_forcing_e2e.rs` catches (both always load a schema, so the full-scan
//! predicate backstop CAN reconstruct the pk and both return the row).
//!
//! Non-vacuous guard: `auto` and `point` return exactly the one matching row here,
//! so the `full` assertion is meaningfully "full alone cannot serve this without
//! diverging". Reverting the fix (letting forced `full` fall through to the full
//! scan) makes `full` return `Ok(0 rows)` and this test's error assertion fail.
//!
//! Against a committed single-INT-partition-key fixture; SKIP-loud when absent.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, Error, Value};

/// `test_compactionparity.live_no_clustering` is `id INT PRIMARY KEY, v TEXT` —
/// a true single-component `int` partition key with NO clustering columns, the
/// exact sole-pk shape the schema-less #1750 seek serves. Partition keys are 1..=4.
const INT_PK_TABLE: &str = "test_compactionparity.live_no_clustering";
const TABLE_FILTER: &str = "/test_compactionparity/live_no_clustering";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Open the int-pk fixture SCHEMA-LESS with a given forced read-path mode.
async fn setup_schemaless(mode: Option<ReadPathMode>) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let mut core_config = Config::default();
    core_config.query.forced_read_path = mode;
    let config = IngestionConfig {
        schema_paths: vec![],
        data_dir,
        version_hint: None,
        core_config,
        table_directory_filter: Some(TABLE_FILTER.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    Ok(result.database)
}

/// Skip cleanly unless the fixture is present with live rows (so a later 0/err is
/// meaningful, never a false pass on an empty dataset).
async fn present_or_skip(mode: Option<ReadPathMode>) -> Option<Database> {
    let db = match setup_schemaless(mode).await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("SKIP {INT_PK_TABLE}: {e}");
            return None;
        }
    };
    let probe = db
        .execute(&format!("SELECT * FROM {INT_PK_TABLE}"))
        .await
        .ok()?;
    if probe.rows.is_empty() {
        eprintln!("SKIP {INT_PK_TABLE}: present but 0 rows");
        return None;
    }
    Some(db)
}

/// `auto` serves the schema-less sole-pk point lookup: exactly the one matching row.
#[tokio::test]
async fn auto_schemaless_sole_pk_lookup_returns_the_row() {
    let Some(db) = present_or_skip(None).await else {
        return;
    };
    let hit = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 1"
        ))
        .await
        .expect("auto schema-less sole-pk lookup must succeed");
    assert_eq!(
        hit.rows.len(),
        1,
        "auto must serve the schema-less sole-pk lookup via the #1750 seek (1 row)",
    );
    assert_eq!(
        hit.rows[0].values.get("partition_key"),
        Some(&Value::Integer(1)),
    );
}

/// `point` (forced) also serves it via the seek: exactly the one matching row.
#[tokio::test]
async fn point_schemaless_sole_pk_lookup_returns_the_row() {
    let Some(db) = present_or_skip(Some(ReadPathMode::Point)).await else {
        return;
    };
    let hit = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 1"
        ))
        .await
        .expect("forced point schema-less sole-pk lookup must succeed");
    assert_eq!(
        hit.rows.len(),
        1,
        "forced point must serve the schema-less sole-pk lookup via the #1750 seek (1 row)",
    );
}

/// THE critical regression check (issue #1918 review fix): forced `full` on the
/// exact schema-less sole-pk shape MUST fail closed with `ForcedReadPathUnavailable`
/// — NOT silently return `Ok(0 rows)` (the pre-fix behavior that diverges from
/// `auto`/`point`, which both return the row above). Reverting the fix flips this
/// from `Err(...)` to `Ok(0 rows)` and this assertion fails.
#[tokio::test]
async fn full_schemaless_sole_pk_lookup_fails_closed_not_silent_zero() {
    let Some(db) = present_or_skip(Some(ReadPathMode::Full)).await else {
        return;
    };
    let err = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 1"
        ))
        .await
        .expect_err(
            "forced full on a schema-less sole-pk lookup must fail closed rather than \
             silently full-scan to 0 rows (spec: rows identical to auto)",
        );
    match err {
        Error::ForcedReadPathUnavailable { forced, reason } => {
            assert_eq!(forced, "full", "the forced mode that failed must be 'full'");
            assert_eq!(
                reason, "schema_less_sole_pk_lookup_requires_targeted_seek",
                "the reason must name the schema-less sole-pk seek requirement",
            );
        }
        other => panic!("expected ForcedReadPathUnavailable, got {other:?}"),
    }
}
