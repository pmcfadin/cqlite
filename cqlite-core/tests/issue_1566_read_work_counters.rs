//! Issue #1566 (Epic A / A5): wiring-evidence for the cfg-gated read-work
//! counters. Proves each counter increments on the REAL public read path — not
//! only through the in-crate local-instance round-trip — by driving a cold open
//! and a point read through the public `Database` API with the counters reset.
//!
//! Compiled only with `--features work-counters` (the getters/`reset` and the
//! counter bodies live behind that feature; see `read_work_counters`). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when the fixture
//! is absent. Excluded under `tombstones` (that build serves point reads by a
//! full-scan filter rather than the targeted seek this evidences).
//!
//! The counters are a shared process-global, so every test here serializes on the
//! `serial_test` mutex (the existing counter-test convention) — a stale value from
//! a parallel test can never satisfy an assertion after a `reset`.

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

/// True if `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db` file.
/// Skip keys off fixture presence (not a 0-row result), so a present fixture that
/// yields 0 rows stays a hard failure.
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

/// Ingest a single fixture table and return a fresh `Database` (its reader pool is
/// empty — no `Data.db` `BlockSource` is open yet, so the first query is a genuine
/// cold open). Returns `None` when the fixture / schema is absent.
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

/// Format a 16-byte UUID as the canonical unquoted 8-4-4-4-12 literal the SELECT
/// parser accepts (issue #956).
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

/// Scan for one present `id` UUID and build the projected point-read SQL for it.
async fn learn_point_sql(db: &Database, table: &str) -> Option<String> {
    let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
    let first = scan.rows.first()?;
    let id = match first.values.get("id") {
        Some(Value::Uuid(b)) => *b,
        _ => return None,
    };
    // Projected (>8 tokens) so it routes through the modern SelectExecutor and the
    // #949/#956 partition-targeted fast path (see benches/tail_latency).
    Some(format!(
        "SELECT id, name FROM {table} WHERE id = {}",
        uuid_to_literal(&id)
    ))
}

/// Scenario: a cold open increments the file-open counter, and the read that
/// forces it decompresses at least one chunk. The reset happens BEFORE ingest, so
/// the ENTIRE cold path (the reader pool is empty; ingest/discovery and the first
/// query mint the `Data.db` `BlockSource` fd) is measured — readers are pooled, so
/// once opened a later query reuses the handle and would not re-`open(2)`.
#[tokio::test]
#[serial]
async fn cold_open_increments_file_opens_and_decompress() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (A5 wiring): test_basic/simple_table Data.db not present");
        return;
    }

    // Counters read zero immediately after reset (reset scenario, cold state) —
    // asserted before any open happens.
    rwc::reset();
    assert_eq!(rwc::file_opens(), 0, "reset must zero FILE_OPENS");
    assert_eq!(
        rwc::decompress_calls(),
        0,
        "reset must zero DECOMPRESS_CALLS"
    );

    // Cold path: ingest opens the reader pool, and the first query reads it.
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (A5 wiring): could not ingest test_basic");
        return;
    };
    let scan = db
        .execute("SELECT id FROM test_basic.simple_table")
        .await
        .expect("cold scan of test_basic.simple_table");
    assert!(
        !scan.rows.is_empty(),
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    assert!(
        rwc::file_opens() >= 1,
        "A5: a cold open must record at least one open(2) at the BlockSource open \
         sites; got {}",
        rwc::file_opens()
    );
    assert!(
        rwc::decompress_calls() >= 1,
        "A5: reading a compressed SSTable must record at least one chunk decompress; \
         got {}",
        rwc::decompress_calls()
    );
}

/// Scenario: a known single-chunk point read increments the decompress counter.
/// The reader is opened first (learning a present key), then counters are reset so
/// only the point read's work is measured.
#[tokio::test]
#[serial]
async fn point_read_increments_decompress() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (A5 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (A5 wiring): could not ingest test_basic");
        return;
    };
    let Some(point_sql) = learn_point_sql(&db, "test_basic.simple_table").await else {
        panic!("A5: could not learn a present UUID key from test_basic.simple_table");
    };

    // Reader is now open (from the learning scan); reset so we measure only the
    // point read's decompression work.
    rwc::reset();
    assert_eq!(
        rwc::decompress_calls(),
        0,
        "reset must zero DECOMPRESS_CALLS"
    );

    let res = db.execute(&point_sql).await.expect("point read");
    assert!(
        !res.rows.is_empty(),
        "A5: point read on a known-present key returned zero rows — #949/#956 regressed?"
    );
    assert!(
        rwc::decompress_calls() >= 1,
        "A5: a point read that decodes one compression chunk must record >= 1 \
         decompress; got {}",
        rwc::decompress_calls()
    );
}

/// Scenario: a BTI point read increments the trie-walk counter. Uses the optional
/// `test_da` BTI corpus; skips (never fails) when it is absent.
#[tokio::test]
#[serial]
async fn bti_point_read_increments_trie_walks() {
    if !fixture_data_present("test_da", "simple_table") {
        eprintln!("Skipping (A5 wiring): optional BTI test_da/simple_table not present");
        return;
    }
    let Some(db) = setup("test_da", "da-test.cql").await else {
        eprintln!("Skipping (A5 wiring): could not ingest test_da");
        return;
    };
    let Some(point_sql) = learn_point_sql(&db, "test_da.simple_table").await else {
        panic!("A5: could not learn a present UUID key from test_da.simple_table");
    };

    rwc::reset();
    assert_eq!(rwc::trie_walks(), 0, "reset must zero TRIE_WALKS");

    let res = db.execute(&point_sql).await.expect("BTI point read");
    assert!(
        !res.rows.is_empty(),
        "A5: BTI point read on a known-present key returned zero rows"
    );
    assert!(
        rwc::trie_walks() >= 1,
        "A5: a BTI point read must descend the Partitions.db trie at least once; \
         got {}",
        rwc::trie_walks()
    );
}

/// Scenario: a real read-path operation increments the seek counter. `SEEK_CALLS`
/// is wired across every production read-path seek site (the `block_io` compressed
/// chunk-read seek plus the `data_access` BTI/BIG point-lookup + scan seeks), so a
/// real scan and a real point read through the public `Database` API on the BIG
/// multi-chunk fixture must each bump it (consumer E4). This is the wiring evidence
/// the finding requires: without instrumenting the `data_access` seeks the counter
/// could stay 0 while real seeks happen, making the E4 guard unreliable.
#[tokio::test]
#[serial]
async fn read_path_increments_seek_calls() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (A5 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (A5 wiring): could not ingest test_basic");
        return;
    };

    // Reset, then a full scan (which reads the compressed chunks through the
    // production seek sites) must record at least one seek.
    rwc::reset();
    assert_eq!(rwc::seek_calls(), 0, "reset must zero SEEK_CALLS");
    let scan = db
        .execute("SELECT id FROM test_basic.simple_table")
        .await
        .expect("scan of test_basic.simple_table");
    assert!(
        !scan.rows.is_empty(),
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );
    assert!(
        rwc::seek_calls() >= 1,
        "A5: a real scan must record at least one production read-path seek; got {}",
        rwc::seek_calls()
    );

    // A targeted point read also traverses a production seek site.
    let Some(point_sql) = learn_point_sql(&db, "test_basic.simple_table").await else {
        panic!("A5: could not learn a present UUID key from test_basic.simple_table");
    };
    rwc::reset();
    assert_eq!(rwc::seek_calls(), 0, "reset must zero SEEK_CALLS");
    let res = db.execute(&point_sql).await.expect("point read");
    assert!(
        !res.rows.is_empty(),
        "A5: point read on a known-present key returned zero rows — #949/#956 regressed?"
    );
    assert!(
        rwc::seek_calls() >= 1,
        "A5: a real point read must record at least one production read-path seek; \
         got {}",
        rwc::seek_calls()
    );
}

/// Scenario: counters reset between operations — each reads zero immediately after
/// `reset` and reflects only the subsequent work. A pure-API check that does not
/// require a fixture.
#[tokio::test]
#[serial]
async fn reset_zeroes_all_counters() {
    // Drive some work if the fixture is present, else just prove reset zeroes.
    if let Some(db) = setup("test_basic", "basic-types.cql").await {
        if fixture_data_present("test_basic", "simple_table") {
            let _ = db.execute("SELECT id FROM test_basic.simple_table").await;
        }
    }
    rwc::reset();
    assert_eq!(rwc::trie_walks(), 0);
    assert_eq!(rwc::decompress_calls(), 0);
    assert_eq!(rwc::seek_calls(), 0);
    assert_eq!(rwc::file_opens(), 0);
}
