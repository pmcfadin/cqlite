//! Issue #1575 (Epic C / C4): the query partition key is Murmur3-hashed + BTI
//! byte-comparable-encoded EXACTLY ONCE per read, not once per candidate SSTable.
//!
//! # What this proves
//!
//! A multi-generation `WHERE pk = ?` point read prunes every candidate SSTable's
//! `Partitions.db` trie. On `main` the SAME query key was re-encoded (a Murmur3
//! hash and byte-comparable encoding) once per candidate — an N-generation fan-out
//! paid N identical hashes. C4 hoists the encode to ONCE per read (the encoding is
//! independent of which SSTable is being pruned), so `KEY_HASH_CALLS == 1` across
//! the fan-out. The per-SSTable trie WALK is unchanged (each candidate's trie must
//! still be consulted); only the redundant per-candidate rehash is removed.
//!
//! # Wiring evidence
//!
//! - **Reader-level fan-out** (the RED-on-main proof): N DISTINCT `SSTableReader`s on
//!   the real BTI `test_da/simple_table` fixture simulate an N-generation candidate
//!   set (each owns its own C3 same-key memo, so nothing coalesces the encode across
//!   them). The pre-C4 per-candidate path (`might_contain_partition`, retained)
//!   records `KEY_HASH_CALLS == N`; the C4 hoisted path
//!   (`might_contain_partition_encoded` fed one precomputed key) records exactly 1 —
//!   with byte-identical prune decisions (same admitted set).
//! - **Manager path** (public `Database` API): a real point read through the query
//!   engine hashes the key exactly once (`SSTableManager::prune_candidates` computes
//!   the BTI encoding a single time), and still returns the expected rows.
//!
//! Compiled only with `--features cli-helpers,work-counters` (the counter
//! getters/`reset` live behind `work-counters`; the ingest helper behind
//! `cli-helpers`). Requires `CQLITE_DATASETS_ROOT` + the optional `test_da` corpus;
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
use cqlite_core::storage::sstable::bti::encode_partition_key_for_bti_trie;
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Database, Value};
use serial_test::serial;

/// A simulated N-generation candidate fan-out. The C4 property (one hash, not one
/// per candidate) holds for any N > 1; 32 mirrors the audit's worst-case example.
const FANOUT: usize = 32;

/// `[0x22; 16]` is a present partition in `test_da/simple_table` (Data.db offset 0;
/// see `issue_755_bti_trie_point_lookup.rs`).
const PRESENT_KEY: [u8; 16] = [0x22u8; 16];

/// A UUID that is (with overwhelming probability) absent from the fixture.
const ABSENT_KEY: [u8; 16] = [0xEEu8; 16];

// ---------------------------------------------------------------------------
// Fixture helpers (mirror issue_831 / issue_1574)
// ---------------------------------------------------------------------------

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate the `da-*-bti-Data.db` for `test_da/simple_table`, requiring the sibling
/// `Partitions.db` trie, or `None` if the binary fixture is absent.
fn da_data_db_path() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables").join("test_da");
    let table_dir = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
        .map(|e| e.path())?;
    let has_partitions = std::fs::read_dir(&table_dir).ok()?.flatten().any(|e| {
        let s = e.file_name().to_string_lossy().to_string();
        s.starts_with("da-") && s.ends_with("-bti-Partitions.db")
    });
    if !has_partitions {
        return None;
    }
    std::fs::read_dir(&table_dir).ok()?.flatten().find_map(|e| {
        let s = e.file_name().to_string_lossy().to_string();
        (s.starts_with("da-") && s.ends_with("-bti-Data.db")).then(|| e.path())
    })
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = std::sync::Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform::new"),
    );
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("SSTableReader::open must succeed for BTI Data.db with Partitions.db present")
}

/// Open `n` INDEPENDENT readers (fresh, empty C3 memos each) on the same fixture —
/// a faithful stand-in for `n` distinct SSTable generations of the same table.
async fn open_fanout(data_db: &Path, n: usize) -> Vec<SSTableReader> {
    let mut readers = Vec::with_capacity(n);
    for _ in 0..n {
        readers.push(open_reader(data_db).await);
    }
    readers
}

// ---------------------------------------------------------------------------
// Reader-level fan-out: the RED-on-main proof (N hashes → 1)
// ---------------------------------------------------------------------------

/// Scenario: a present key is hashed once across the candidate fan-out.
#[tokio::test]
#[serial]
async fn present_key_hashed_once_across_fanout() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP (C4): test_da/simple_table BTI fixture not available");
        return;
    };

    // Two independent fan-outs so each measurement starts from empty memos.
    let set_old = open_fanout(&data_db, FANOUT).await;
    let set_new = open_fanout(&data_db, FANOUT).await;
    assert!(
        set_old.iter().chain(set_new.iter()).all(|r| r.is_bti()),
        "C4: the test_da/simple_table fixture must be a BTI reader"
    );

    // Pre-C4 per-candidate path (retained `might_contain_partition`): each distinct
    // candidate re-encodes the SAME key.
    rwc::reset();
    let hits_old = set_old
        .iter()
        .filter(|r| r.might_contain_partition(&PRESENT_KEY))
        .count();
    let per_candidate_hashes = rwc::key_hash_calls();

    // C4 hoisted path: encode ONCE, reuse the encoded key for every candidate.
    rwc::reset();
    let encoded = encode_partition_key_for_bti_trie(&PRESENT_KEY); // the ONLY hash
    let hits_new = set_new
        .iter()
        .filter(|r| r.might_contain_partition_encoded(&PRESENT_KEY, &encoded))
        .count();
    let hoisted_hashes = rwc::key_hash_calls();
    let hoisted_walks = rwc::trie_walks();

    // Byte-identical prune decision: a present key is admitted by every candidate on
    // both paths.
    assert_eq!(
        hits_old, FANOUT,
        "present key admitted by every candidate (old path)"
    );
    assert_eq!(
        hits_new, FANOUT,
        "C4 hoisted prune admits the identical candidate set"
    );

    assert_eq!(
        per_candidate_hashes, FANOUT as u64,
        "pre-C4 waste: the per-candidate prune re-hashes the SAME key once per \
         candidate (this is what C4 removes); got {per_candidate_hashes}"
    );
    assert_eq!(
        hoisted_hashes, 1,
        "C4: the query key is Murmur3-hashed + BTI-encoded EXACTLY ONCE across a \
         {FANOUT}-generation fan-out; got {hoisted_hashes}"
    );
    assert!(
        hoisted_walks >= FANOUT as u64,
        "the per-SSTable trie WALK is unchanged — every candidate's trie is still \
         consulted (only the hash is hoisted); got {hoisted_walks} for {FANOUT} candidates"
    );
}

/// Scenario: an absent key is also hashed once across the fan-out (definitive
/// absence per candidate, single hash).
#[tokio::test]
#[serial]
async fn absent_key_hashed_once_across_fanout() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP (C4): test_da/simple_table BTI fixture not available");
        return;
    };

    let set_new = open_fanout(&data_db, FANOUT).await;

    rwc::reset();
    let encoded = encode_partition_key_for_bti_trie(&ABSENT_KEY); // the ONLY hash
    let hits = set_new
        .iter()
        .filter(|r| r.might_contain_partition_encoded(&ABSENT_KEY, &encoded))
        .count();
    let hoisted_hashes = rwc::key_hash_calls();

    assert_eq!(
        hits, 0,
        "C4: an absent key is definitive trie absence in every candidate"
    );
    assert_eq!(
        hoisted_hashes, 1,
        "C4: an absent-key fan-out still hashes the query key exactly once; got {hoisted_hashes}"
    );
}

// ---------------------------------------------------------------------------
// Manager path: the public query engine hashes the key once (wiring evidence)
// ---------------------------------------------------------------------------

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

async fn setup_db() -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join("da-test.cql");
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
        core_config: Config::default(),
        table_directory_filter: Some("/test_da/".to_string()),
    };
    let result = ingest(config).await.ok()?;
    (result.schema_load_result.schemas_loaded > 0).then_some(result.database)
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

/// Scenario: a real point read through the query engine hashes the key once.
#[tokio::test]
#[serial]
async fn manager_point_read_hashes_key_once() {
    if da_data_db_path().is_none() {
        eprintln!("SKIP (C4): test_da/simple_table BTI fixture not available");
        return;
    }
    let Some(db) = setup_db().await else {
        eprintln!("SKIP (C4): could not ingest test_da");
        return;
    };

    // Learn a present id via a scan (also opens the reader).
    let scan = db
        .execute("SELECT id FROM test_da.simple_table")
        .await
        .expect("scan");
    let Some(Value::Uuid(id)) = scan.rows.first().and_then(|r| r.values.get("id")).cloned() else {
        panic!("C4: could not learn a present UUID key from test_da.simple_table");
    };
    let point_sql = format!(
        "SELECT id, name FROM test_da.simple_table WHERE id = {}",
        uuid_to_literal(&id)
    );

    // Measure only the point read's key hashing.
    rwc::reset();
    assert_eq!(rwc::key_hash_calls(), 0, "reset must zero KEY_HASH_CALLS");
    let res = db.execute(&point_sql).await.expect("BTI point read");

    assert!(
        !res.rows.is_empty(),
        "C4: BTI point read on a known-present key returned zero rows"
    );
    assert_eq!(
        rwc::key_hash_calls(),
        1,
        "C4: the manager point-read path hashes+encodes the query key exactly once; got {}",
        rwc::key_hash_calls()
    );
}
