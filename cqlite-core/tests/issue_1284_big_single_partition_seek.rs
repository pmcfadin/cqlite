//! Issue #1284: the BIG (`nb`) single-partition seek
//! (`scan_single_partition_clustering` → `bti_decompress_and_parse_target_all`)
//! must ENGAGE for a `WHERE pk = ?` point lookup — for BOTH compressed and
//! uncompressed BIG SSTables — rather than silently returning zero rows and
//! falling back to a full scan.
//!
//! Two pre-existing failure modes the fix closes:
//!   1. The `table_ids_match_strict` guard rejected every decoded row when the
//!      SSTable's serialization-header keyspace/table differed from a fully
//!      qualified query id, so the seek decoded the partition but kept nothing →
//!      empty result, silent full-scan fallback.
//!   2. The chunk-targeted path was skipped for UNCOMPRESSED BIG SSTables
//!      (`compression_info = None`) and the whole-section fallback within it did
//!      not bound/decode them → the seek never engaged.
//!
//! The wiring oracle is the `work_counters::partitions_decoded()` counter: the
//! seek bumps it exactly once when it decodes the target partition DIRECTLY (the
//! targeted path). An internal `Ok(None)` → full-scan fallback leaves it at 0
//! (the full scan path bumps `partitions_parsed`, not `partitions_decoded`). So
//! `partitions_decoded == 1` proves the targeted seek actually engaged — not a
//! full scan that returned the same rows.
//!
//! The parity oracle: the rows the seek returns MUST equal the rows a full scan
//! returns filtered to the same partition key (byte-faithful, same rows).
//!
//! Fixtures (real Cassandra 5.0 BIG `nb` SSTables, single UUID partition key so
//! each partition is one row — narrow partitions exercise the point-lookup path):
//!   - compressed  : `test_basic.simple_table`      (Snappy)
//!   - uncompressed : `test_basic.uncompressed_table` (compression disabled)
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; SKIPS on fixture absence,
//! but a present-but-empty result is a HARD FAILURE (parity-is-truth).
//!
//! Excluded under `tombstones`: that build compiles out the targeted prune /
//! work-counter mutators (see `scan_single_partition_clustering` is
//! `cfg(not(tombstones))`), so the engagement signal is inapplicable there.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::work_counters;
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

/// True iff `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db`. The
/// skip keys off fixture PRESENCE, so a present fixture that returns 0 rows is a
/// hard failure, not a silent skip.
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
        table_directory_filter: Some("/test_basic/".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

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

/// Drive the shared oracle for one BIG fixture with a UUID partition key:
///   - learn a real partition key from a full scan,
///   - run `WHERE id = <uuid>` and assert the seek ENGAGED (`partitions_decoded
///     == 1`, NOT 0), the access path is targeted (NOT FallbackFullScan), and
///   - the rows EQUAL the full scan filtered to that key (parity-is-truth).
async fn assert_seek_engages_and_matches_full_scan(keyspace: &str, table: &str) {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping ({keyspace}.{table}): {e}");
            return;
        }
    };
    if !fixture_data_present(keyspace, table) {
        eprintln!("Skipping ({keyspace}.{table}): fixture Data.db not present");
        return;
    }

    // Full scan: the parity oracle + the source of a real partition key.
    let full = db
        .execute(&format!("SELECT * FROM {keyspace}.{table}"))
        .await
        .unwrap_or_else(|e| panic!("full scan {keyspace}.{table} failed: {e}"));
    assert!(
        !full.rows.is_empty(),
        "{keyspace}.{table}: fixture present but full scan returned 0 rows (parity-is-truth: a \
         present-but-empty fixture is a failure, not a skip)"
    );
    let Some(Value::Uuid(id)) = full.rows.first().and_then(|r| r.values.get("id").cloned()) else {
        panic!("{keyspace}.{table}: first row has no UUID `id` column");
    };

    // Expected rows = full scan filtered to this partition key.
    let expected: Vec<QueryRow> = full
        .rows
        .iter()
        .filter(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if *b == id))
        .cloned()
        .collect();
    assert!(
        !expected.is_empty(),
        "{keyspace}.{table}: full scan must contain the learned partition key"
    );

    // The targeted point lookup, with the work counters reset so the engagement
    // signal reflects exactly this query. Routed through `execute_with_params`
    // (a bound `WHERE id = ?`) so it takes the SELECT-executor partition-targeted
    // pipeline — `targeted_partition_rows` → `scan_partition_clustering` →
    // `scan_single_partition_clustering` — which is what this issue's seek lives
    // on. (Since issue #1750 a literal `WHERE id = <uuid>` takes the same modern
    // pipeline; the bound form is used here for the parameter-binding coverage.)
    work_counters::reset();
    let seek = db
        .execute_with_params(
            &format!("SELECT * FROM {keyspace}.{table} WHERE id = ?"),
            &[Value::Uuid(id)],
        )
        .await
        .unwrap_or_else(|e| panic!("seek {keyspace}.{table} failed: {e}"));
    let decoded = work_counters::partitions_decoded();

    // (a) WIRING: the targeted single-partition SEEK actually engaged. The seek
    // bumps `partitions_decoded` once when it decodes the target partition
    // directly; an internal `Ok(None)` → full-scan fallback leaves it at 0. So
    // `== 1` is hard evidence the targeted path ran, not a full scan that
    // happened to return the same rows.
    assert_eq!(
        decoded, 1,
        "Issue #1284: BIG WHERE pk = ? must ENGAGE the targeted single-partition seek for \
         {keyspace}.{table} (partitions_decoded == 1), got {decoded} — 0 means the seek \
         decoded nothing and silently fell back to a full scan",
    );

    // (a') The reported access path is a TARGETED partition lookup, never a
    // FallbackFullScan.
    let path = seek
        .metadata
        .access_path
        .clone()
        .expect("a WHERE pk = ? query records an access path");
    assert!(
        path.is_targeted() && !path.is_full_scan(),
        "Issue #1284: BIG WHERE pk = ? must report a targeted access path for {keyspace}.{table}, \
         got {path:?}",
    );
    assert_ne!(
        path,
        AccessPath::FallbackFullScan {
            reason: cqlite_core::query::access_path::FallbackReason::TombstonesBuildNoPrune,
        },
        "Issue #1284: the seek must not report a FallbackFullScan for {keyspace}.{table}",
    );

    // (b) PARITY: the seek returns EXACTLY the rows the full scan does for this
    // key (byte-faithful, same rows).
    assert!(
        !seek.rows.is_empty(),
        "Issue #1284: BIG WHERE pk = ? returned 0 rows for {keyspace}.{table} even though the \
         partition exists in the full scan — this is the headline regression",
    );
    assert_eq!(
        fingerprints(&seek.rows),
        fingerprints(&expected),
        "Issue #1284: the targeted seek rows must equal the full-scan rows filtered to the same \
         partition key for {keyspace}.{table}",
    );
}

/// Compressed (Snappy) BIG `nb` SSTable.
///
/// `#[serial(work_counters)]`: both cases reset and read the process-global
/// `work_counters` to prove the seek engaged (`partitions_decoded == 1`), so they
/// must not run concurrently within the test binary — an interleaved reset/read
/// would make the engagement signal nondeterministic. Shares the `work_counters`
/// serial group with the other counter-asserting tests in this crate.
#[tokio::test]
#[serial(work_counters)]
async fn big_compressed_where_pk_eq_engages_seek_and_matches_full_scan() {
    assert_seek_engages_and_matches_full_scan("test_basic", "simple_table").await;
}

/// Uncompressed BIG `nb` SSTable (`compression_info = None`).
///
/// Serialized on the `work_counters` group for the same reason as the compressed
/// case above: it resets and reads the process-global counters.
#[tokio::test]
#[serial(work_counters)]
async fn big_uncompressed_where_pk_eq_engages_seek_and_matches_full_scan() {
    assert_seek_engages_and_matches_full_scan("test_basic", "uncompressed_table").await;
}
