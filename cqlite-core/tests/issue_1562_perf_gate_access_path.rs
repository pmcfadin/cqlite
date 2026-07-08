//! Issue #1562 (Epic A read-perf-gate): the `read/get_partition_big` bench must
//! drive a *real* partition-targeted point read, not a scan proxy.
//!
//! The old `read/point_lookup` bench ran `SELECT * ... LIMIT 1` — a full scan
//! truncated to one row — so it could not detect a regression on the #949/#956
//! partition-targeted point path. This test pins the wiring the bench relies on:
//! a fully-constrained `WHERE id = <unquoted-uuid-literal>` on a UUID-PK table
//! resolves to `AccessPath::PartitionLookup` via the public `QueryResult`. If the
//! #949 fast path or the #956 UUID literal ever regresses, the reported access
//! path flips to a full-scan variant and this test (and the bench setup guard)
//! fails instead of silently benching a scan.
//!
//! # Why a *projected* SELECT (not `SELECT *`)
//!
//! `QueryEngine::execute` routes `SELECT` through the modern `SelectExecutor`
//! (which engages the #949 fast path and records `access_path`) EXCEPT for a
//! "simple id lookup": `cql.contains("WHERE id =") && whitespace_tokens <= 8`.
//! `SELECT * FROM <ks.tbl> WHERE id = <lit>` is exactly 8 tokens, so it falls
//! into the legacy `QueryExecutor` — an unconditional full scan that reports
//! `access_path = None` (see `FallbackReason::LegacyExecutorPath`). Projecting
//! two columns (`SELECT id, name ...`, 9 tokens) is a faithful fully-constrained
//! point read that routes through the modern executor and reports the real path.
//! This is the query shape the `read/get_partition_*` benches use.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::{Database, Value};

const QUALIFIED_TABLE: &str = "test_basic.simple_table";
const KEYSPACE_FILTER: &str = "/test_basic/";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent()?.join("schemas");
        if dir.exists() {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 hex string the parser
/// accepts as an unquoted literal.
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

fn uuid_value(row: &QueryRow, col: &str) -> Option<[u8; 16]> {
    match row.values.get(col) {
        Some(Value::Uuid(b)) => Some(*b),
        _ => None,
    }
}

#[tokio::test]
async fn get_partition_point_read_reports_partition_lookup_access_path() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Learn a real, present partition key from a full scan.
    let full = db
        .execute(&format!("SELECT id FROM {QUALIFIED_TABLE}"))
        .await
        .expect("full scan must succeed");
    let Some(first) = full.rows.first() else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let Some(id) = uuid_value(first, "id") else {
        panic!("first row `id` did not decode as Value::Uuid");
    };
    let literal = uuid_to_literal(&id);

    // The exact shape the bench issues: a fully-constrained UUID-PK point read.
    // Since issue #1750 every SELECT routes through the modern SelectExecutor
    // regardless of token count; the projected shape mirrors the bench.
    let res = db
        .execute(&format!(
            "SELECT id, name FROM {QUALIFIED_TABLE} WHERE id = {literal}"
        ))
        .await
        .unwrap_or_else(|e| panic!("targeted lookup for id={literal} failed: {e}"));

    assert!(
        !res.rows.is_empty(),
        "Issue #1562: point read WHERE id = {literal} must return the row, got [] — the bench \
         would be measuring 0 rows",
    );

    assert!(
        res.metadata.access_path.is_some(),
        "Issue #1562: point read reported no access_path (None) — a modern-executor SELECT \
         must always attach an access-path signal.",
    );

    assert_eq!(
        res.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #1562: a fully-constrained UUID-PK point read must report \
         AccessPath::PartitionLookup (the #949/#956 partition-targeted path). Got {:?} — the \
         get_partition bench would be silently measuring a full scan.",
        res.metadata.access_path,
    );
}
