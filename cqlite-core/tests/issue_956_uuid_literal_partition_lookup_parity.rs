//! Issue #956: unquoted-UUID literal WHERE matching + partition-targeted lookup.
//!
//! Before #956 the SELECT parser had no unquoted-UUID literal, so a
//! `WHERE id = 550e8400-...` against a UUID partition key produced no
//! `Value::Uuid` predicate. The full-scan filter never matched the row's
//! `Value::Uuid`, and the #949 partition-targeted fast path (which encodes the
//! predicate value to the on-disk key) could not engage. UUID is the single most
//! common Cassandra partition-key type, so this silently disabled point lookups
//! for the majority of real tables.
//!
//! This test exercises `test_basic.simple_table` (`id UUID PRIMARY KEY`):
//!  1. full-scan once to learn the real `id` values and a per-partition baseline,
//!  2. for each partition, run `WHERE id = <unquoted-uuid>` and assert the
//!     targeted result equals the full-scan-filtered baseline for that key,
//!  3. assert an absent UUID returns no rows.
//!
//! Because `id` is a single UUID partition key, the targeted query takes the
//! #949 partition-targeted path (the predicate value encodes to the raw 16-byte
//! key); proving targeted == full-scan rows is the end-to-end evidence that both
//! the parser literal and the fast-path coercion are correct.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped
//! (not failed) when the data isn't present, matching the repo's other
//! dataset-backed integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
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

/// Canonical, order-independent fingerprint of a row's columns for set comparison.
fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

#[tokio::test]
async fn uuid_literal_lookup_matches_full_scan_for_every_partition() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Full scan once: the reference. Group rows by their UUID partition key.
    let full = db
        .execute(&format!("SELECT id, name, age FROM {QUALIFIED_TABLE}"))
        .await
        .expect("full scan must succeed");

    if full.rows.is_empty() {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let mut by_partition: BTreeMap<[u8; 16], Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        let Some(id) = uuid_value(&row, "id") else {
            continue;
        };
        by_partition.entry(id).or_default().push(row);
    }

    assert!(
        !by_partition.is_empty(),
        "expected at least one UUID-keyed partition; the `id` column did not decode as Value::Uuid",
    );

    let mut checked = 0usize;
    for (id, expected_rows) in by_partition.iter() {
        let literal = uuid_to_literal(id);
        let targeted = db
            .execute(&format!(
                "SELECT id, name, age FROM {QUALIFIED_TABLE} WHERE id = {literal}"
            ))
            .await
            .unwrap_or_else(|e| panic!("targeted lookup for id={literal} failed: {e}"));

        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #956: WHERE id = {literal} (unquoted UUID literal) must equal the \
             full-scan rows for that partition",
        );

        checked += 1;
        if checked >= 50 {
            break;
        }
    }

    assert!(checked > 0, "expected at least one partition to validate");
    println!("Issue #956: validated unquoted-UUID-literal lookup parity for {checked} partitions");
}

#[tokio::test]
async fn uuid_literal_lookup_returns_matching_row_when_present() {
    // Stronger than the parity loop: prove a known-present key returns >= 1 row
    // (regression guard against a fast path that prunes the holding SSTable and
    // silently returns []).
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute(&format!("SELECT id, name FROM {QUALIFIED_TABLE} LIMIT 1"))
        .await
        .expect("scan must succeed");
    let Some(first) = full.rows.first() else {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    };
    let Some(id) = uuid_value(first, "id") else {
        panic!("first row `id` did not decode as Value::Uuid");
    };
    let literal = uuid_to_literal(&id);

    let targeted = db
        .execute(&format!(
            "SELECT id, name FROM {QUALIFIED_TABLE} WHERE id = {literal}"
        ))
        .await
        .unwrap_or_else(|e| panic!("targeted lookup for id={literal} failed: {e}"));

    assert!(
        !targeted.rows.is_empty(),
        "Issue #956: WHERE id = {literal} for a known-present UUID key must return the row, got []",
    );
    assert!(
        targeted
            .rows
            .iter()
            .all(|r| uuid_value(r, "id") == Some(id)),
        "every returned row must belong to the requested partition",
    );
}

#[tokio::test]
async fn uuid_literal_lookup_for_absent_key_is_empty() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // A syntactically valid UUID that is overwhelmingly unlikely to exist.
    let result = db
        .execute(&format!(
            "SELECT id FROM {QUALIFIED_TABLE} \
             WHERE id = ffffffff-ffff-ffff-ffff-ffffffffffff"
        ))
        .await
        .expect("absent-key lookup must succeed");

    assert!(
        result.rows.is_empty(),
        "Issue #956: a fully-constrained lookup on an absent UUID partition key must return no \
         rows, got {}",
        result.rows.len()
    );
}
