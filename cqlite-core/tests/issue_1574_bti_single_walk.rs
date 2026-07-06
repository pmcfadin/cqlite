//! Issue #1574 (Epic C / C3): a single-candidate BTI point read descends the
//! `Partitions.db` trie EXACTLY once.
//!
//! Before C3 the same key was walked twice per point read — once for the
//! candidate prune (`might_contain_partition`) and once for the seek
//! (`scan_single_partition_clustering`). C3 reuses the prune's resolution for the
//! seek, so `TRIE_WALKS` drops from 2 to 1. This is the wiring-evidence test: it
//! drives a real projected `WHERE id = <uuid>` point read through the public
//! `Database` API against the single-generation BTI `test_da/simple_table` fixture
//! and asserts the counter is exactly 1.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT` + the optional `test_da` corpus;
//! skips (never fails) when absent. Excluded under `tombstones` (that build serves
//! point reads by a full-scan filter rather than the targeted prune+seek path).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::{Database, Value};
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

/// True if `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db`.
fn fixture_data_present(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join(schema_file);
    if !schema_path.exists() {
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

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

/// Learn a present `id` UUID and build the projected point-read SQL (>8 tokens so
/// it routes through the modern SelectExecutor's partition-targeted path).
async fn learn_point_sql(db: &Database, table: &str) -> Option<String> {
    let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
    let first = scan.rows.first()?;
    let id = match first.values.get("id") {
        Some(Value::Uuid(b)) => *b,
        _ => return None,
    };
    Some(format!(
        "SELECT id, name FROM {table} WHERE id = {}",
        uuid_to_literal(&id)
    ))
}

/// Scenario: a single-candidate BTI point read descends the trie exactly once.
#[tokio::test]
#[serial]
async fn bti_single_candidate_point_read_walks_trie_once() {
    if !fixture_data_present("test_da", "simple_table") {
        eprintln!("Skipping (C3): optional BTI test_da/simple_table not present");
        return;
    }
    let Some(db) = setup("test_da", "da-test.cql").await else {
        eprintln!("Skipping (C3): could not ingest test_da");
        return;
    };
    let Some(point_sql) = learn_point_sql(&db, "test_da.simple_table").await else {
        panic!("C3: could not learn a present UUID key from test_da.simple_table");
    };

    // Reader is open (from the learning scan). Reset so we measure only the point
    // read's trie work.
    rwc::reset();
    assert_eq!(rwc::trie_walks(), 0, "reset must zero TRIE_WALKS");

    let res = db.execute(&point_sql).await.expect("BTI point read");
    assert!(
        !res.rows.is_empty(),
        "C3: BTI point read on a known-present key returned zero rows"
    );
    assert_eq!(
        rwc::trie_walks(),
        1,
        "C3: a single-candidate BTI point read must descend the Partitions.db trie \
         EXACTLY once (prune resolution reused by the seek); got {}",
        rwc::trie_walks()
    );
}

/// Scenario: a trie miss is still authoritative absence, walked once.
#[tokio::test]
#[serial]
async fn bti_absent_key_point_read_is_authoritative_absence() {
    if !fixture_data_present("test_da", "simple_table") {
        eprintln!("Skipping (C3): optional BTI test_da/simple_table not present");
        return;
    }
    let Some(db) = setup("test_da", "da-test.cql").await else {
        eprintln!("Skipping (C3): could not ingest test_da");
        return;
    };
    // Learn the real path is engaged first (opens the reader).
    let _ = learn_point_sql(&db, "test_da.simple_table").await;

    rwc::reset();
    // A UUID that is (with overwhelming probability) absent from the fixture.
    let absent = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    let res = db
        .execute(&format!(
            "SELECT id, name FROM test_da.simple_table WHERE id = {absent}"
        ))
        .await
        .expect("BTI absent-key point read");
    assert!(
        res.rows.is_empty(),
        "C3: absent key must resolve to no rows (authoritative trie absence)"
    );
    assert!(
        rwc::trie_walks() <= 1,
        "C3: an absent-key point read must descend the trie at most once; got {}",
        rwc::trie_walks()
    );
}
