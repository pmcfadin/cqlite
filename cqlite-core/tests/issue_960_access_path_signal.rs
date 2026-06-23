//! Issue #960 (Epic #951): the CQL SELECT path exposes an honest, assertable
//! access-path signal.
//!
//! Correct result rows do not prove a storage capability is wired into the CQL
//! path (#949 returned correct rows while still full-scanning). These tests
//! assert the *access path* a SELECT chose — not just its rows — via two
//! observable signals that #960 introduces:
//!   1. the result-attached `QueryResult.metadata.access_path`, and
//!   2. the test-accessible global probe `cqlite_core::query::access_path::last()`
//!      (mirrors `scan_for_key_call_count`, issue #831). The streaming path runs
//!      in a spawned task, so the probe — not the iterator metadata — is the
//!      signal there.
//!
//! Scope reminder (#960 vs #962): #960 only *reports* the path honestly. Paths
//! that still full-scan today (the WRITETIME/TTL metadata path) MUST report a
//! `FallbackFullScan { reason }`; a test here pins that current reality so #962
//! can later flip it without the change going unnoticed.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped
//! (not failed) when the data isn't present, matching the repo's other
//! dataset-backed integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath, FallbackReason};
use cqlite_core::query::result::{QueryResultIterator, QueryRow};
use cqlite_core::query::StreamingConfig;
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

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 hex unquoted literal.
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

/// Learn one real UUID partition key from a full scan, skipping if no data.
async fn one_present_uuid(db: &Database) -> Option<[u8; 16]> {
    let full = db
        .execute(&format!("SELECT id FROM {QUALIFIED_TABLE} LIMIT 1"))
        .await
        .ok()?;
    let first = full.rows.first()?;
    uuid_value(first, "id")
}

// ---------------------------------------------------------------------------
// 1. WHERE pk = <literal> reports PartitionLookup (NOT a full scan).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn where_pk_eq_literal_reports_partition_lookup() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = one_present_uuid(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let literal = uuid_to_literal(&id);

    let result = db
        .execute(&format!(
            "SELECT id, name FROM {QUALIFIED_TABLE} WHERE id = {literal}"
        ))
        .await
        .expect("targeted lookup must succeed");

    // Signal 1: result-attached metadata.
    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #960: a fully-constrained WHERE pk = <uuid> must report PartitionLookup on the \
         result metadata, got {:?}",
        result.metadata.access_path
    );
    // Signal 2: the global probe.
    assert_eq!(
        access_path::last(),
        Some(AccessPath::PartitionLookup),
        "Issue #960: the access-path probe must record PartitionLookup for WHERE pk = <uuid>",
    );
    assert!(
        !result.metadata.access_path.as_ref().unwrap().is_full_scan(),
        "a partition-targeted lookup must NOT be classified as a full scan",
    );
}

// ---------------------------------------------------------------------------
// 2. No usable restriction reports FullScan.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unrestricted_select_reports_full_scan_fallback() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(&format!("SELECT id, name FROM {QUALIFIED_TABLE}"))
        .await
        .expect("full scan must succeed");

    if result.rows.is_empty() {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // No WHERE clause => the partition key is not constrained at all. Honest
    // report is the documented fallback reason.
    let path = result
        .metadata
        .access_path
        .clone()
        .expect("a SELECT over a table must report an access path");
    assert!(
        path.is_full_scan(),
        "Issue #960: an unrestricted SELECT must report a full scan, got {path:?}",
    );
    assert_eq!(
        path,
        AccessPath::FallbackFullScan {
            reason: FallbackReason::PartitionKeyNotFullyConstrained,
        },
        "Issue #960: an unrestricted SELECT must report the \
         PartitionKeyNotFullyConstrained fallback reason",
    );
    assert_eq!(access_path::last(), Some(path));
}

// ---------------------------------------------------------------------------
// 3. A known fallback case reports FallbackFullScan with a documented reason.
//    The WRITETIME/TTL metadata path still full-scans today (#962 will flip it
//    to MetadataPartitionLookup). Pin that reality so the flip is noticed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn writetime_metadata_path_reports_metadata_scan_fallback() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = one_present_uuid(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let literal = uuid_to_literal(&id);

    // WRITETIME(name) forces the metadata-carrying scan, which always full-scans
    // today even with a fully-constrained partition key.
    let result = db
        .execute(&format!(
            "SELECT id, WRITETIME(name) FROM {QUALIFIED_TABLE} WHERE id = {literal}"
        ))
        .await
        .expect("WRITETIME metadata query must succeed");

    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::FallbackFullScan {
            reason: FallbackReason::MetadataScanPath,
        }),
        "Issue #960: the WRITETIME/TTL metadata projection path still full-scans today and MUST \
         report MetadataScanPath honestly (not a targeted lookup). #962 will flip this to \
         MetadataPartitionLookup; this assertion pins the current reality so that flip is \
         noticed. Got {:?}",
        result.metadata.access_path
    );
}

// ---------------------------------------------------------------------------
// 4. Streaming equivalent reports StreamingPartitionLookup via the probe.
// ---------------------------------------------------------------------------

async fn drain(mut it: QueryResultIterator) -> usize {
    let mut n = 0usize;
    while let Some(item) = it.next_async().await {
        if item.is_ok() {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn streaming_where_pk_eq_literal_reports_streaming_partition_lookup() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = one_present_uuid(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let literal = uuid_to_literal(&id);

    // Clear the probe, run the streaming query, and fully drain it so the spawned
    // producer task records its access path before we read the probe.
    access_path::reset();
    let it = db
        .execute_streaming(
            &format!("SELECT id, name FROM {QUALIFIED_TABLE} WHERE id = {literal}"),
            StreamingConfig::default(),
        )
        .await
        .expect("streaming targeted lookup must succeed");
    let _rows = drain(it).await;

    assert_eq!(
        access_path::last(),
        Some(AccessPath::StreamingPartitionLookup),
        "Issue #960: the streaming SELECT path must record StreamingPartitionLookup for a \
         fully-constrained WHERE pk = <uuid> (the streaming analogue of PartitionLookup). The \
         streaming scan runs in a spawned task, so the signal is the global probe, not the \
         iterator metadata.",
    );
}

#[tokio::test]
async fn streaming_unrestricted_select_reports_full_scan_fallback() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    access_path::reset();
    let it = db
        .execute_streaming(
            &format!("SELECT id, name FROM {QUALIFIED_TABLE}"),
            StreamingConfig::default(),
        )
        .await
        .expect("streaming full scan must succeed");
    let rows = drain(it).await;
    if rows == 0 {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let path = access_path::last().expect("streaming SELECT must record an access path");
    assert!(
        path.is_full_scan(),
        "Issue #960: an unrestricted streaming SELECT must report a full scan, got {path:?}",
    );
    assert_eq!(
        path,
        AccessPath::FallbackFullScan {
            reason: FallbackReason::PartitionKeyNotFullyConstrained,
        },
    );
}
