//! Issue #953 (Epic #951) regression: the within-SSTable seek must return ALL
//! clustering rows of the target partition, not just the first.
//!
//! The original #953 seek (`scan_single_partition`) reused
//! `bti_decompress_and_parse_target`, which BREAKs after the first emitted row
//! (correct for a `get()` point lookup that returns one `Value`). But
//! `scan_partition` must hand the query layer EVERY clustering row of the
//! partition so it can apply clustering predicates. For tables with MULTIPLE
//! clustering rows per partition, a fully-constrained `WHERE pk = ?` therefore
//! dropped every row after the first whenever the seek succeeded. The original
//! #953 byte-parity tests missed this because they used single-row-per-partition
//! tables (`test_basic.simple_table`, `test_da.simple_table`, both UUID-PK).
//!
//! This test pins the fix against MULTI-clustering-row fixtures in BOTH formats:
//!   - **BTI (`da`)** — `test_da.wide_table` (`PRIMARY KEY (pk, ck)`, int pk):
//!     3 partitions (pk = 1/2/3), each 300 rows (ck = 0..299), LZ4. The 300×~2 KiB
//!     payload partition spans many compression chunks, so the seek must stitch
//!     forward across chunks without truncating the partition mid-row.
//!   - **BIG (`nb`)** — `test_timeseries.sensor_data` (`PRIMARY KEY (sensor_id,
//!     timestamp)`, UUID pk): 10 partitions, ~200 rows each, LZ4. Offset resolved
//!     via `Index.db`.
//!
//! Each assertion proves the seek (`WHERE pk = <key>`) returns the SAME rows as
//! the full scan filtered to that key (count AND per-row fingerprint), so a
//! regression to break-after-first-row (returns 1) fails immediately.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests. Excluded under `tombstones` (that build compiles out the
//! seek; see `issue_953_within_sstable_seek.rs` for the same exclusion).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::Database;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        });
        if dir.is_some() {
            return dir;
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup(schema_file: &str, keyspace_filter: &str) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join(schema_file);
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
        table_directory_filter: Some(keyspace_filter.to_string()),
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
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

/// Drive the shared multi-row parity check: full-scan the table, group rows by
/// the partition key column, then for every partition with > 1 clustering row
/// assert the seek (`WHERE <pk_col> = <literal>`) returns byte-identical rows.
///
/// `pk_literal` formats a partition-key cell into the WHERE literal (UUID hex,
/// bare int, etc.). Returns `Some(n)` with the number of multi-row partitions
/// validated, or `None` when the fixture's Data.db is absent (0 rows) so the
/// caller can SKIP rather than fail — `test_da/wide_table` is a local fixture not
/// present in the published CI dataset, so this must not be a hard failure.
async fn assert_multirow_seek_parity<F>(
    db: &Database,
    qualified_table: &str,
    projection: &str,
    pk_col: &str,
    pk_literal: F,
) -> Option<usize>
where
    F: Fn(&QueryRow) -> Option<String>,
{
    let full = db
        .execute(&format!("SELECT {projection} FROM {qualified_table}"))
        .await
        .unwrap_or_else(|e| panic!("full scan of {qualified_table} must succeed: {e}"));
    if full.rows.is_empty() {
        eprintln!("Skipping {qualified_table}: full scan returned 0 rows (Data.db not fetched)");
        return None;
    }

    // Group full-scan rows by the partition-key literal, preserving the order
    // they appear in (the per-partition fingerprint set is order-insensitive).
    let mut by_partition: BTreeMap<String, Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        if let Some(lit) = pk_literal(&row) {
            by_partition.entry(lit).or_default().push(row);
        }
    }

    let mut multirow_checked = 0usize;
    for (literal, expected_rows) in by_partition.iter() {
        // Only the multi-clustering-row partitions exercise the bug.
        if expected_rows.len() <= 1 {
            continue;
        }

        let targeted = db
            .execute(&format!(
                "SELECT {projection} FROM {qualified_table} WHERE {pk_col} = {literal}"
            ))
            .await
            .unwrap_or_else(|e| panic!("seek {pk_col}={literal} failed: {e}"));

        // The crux of the regression: a break-after-first-row decoder returns 1.
        assert_eq!(
            targeted.rows.len(),
            expected_rows.len(),
            "Issue #953: WHERE {pk_col} = {literal} over a partition with {} clustering rows \
             must return ALL {} rows via the within-SSTable seek, but returned {}. The original \
             #953 decoder broke after the FIRST row (returns 1).",
            expected_rows.len(),
            expected_rows.len(),
            targeted.rows.len(),
        );
        // ...and they must be byte-identical to the full-scan rows for that key.
        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #953: seek rows for {pk_col} = {literal} must equal the full-scan rows",
        );
        multirow_checked += 1;
    }

    Some(multirow_checked)
}

/// BTI (`da`) format multi-row seek parity: `test_da.wide_table`, 3 partitions
/// of 300 rows each. The fix must stitch chunks forward across the whole wide
/// partition without truncating it mid-row.
#[tokio::test]
async fn bti_multirow_partition_seek_returns_all_rows() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping (BTI wide_table): {e}");
            return;
        }
    };

    // pk is an int; the WHERE literal is the bare integer. We read it from the
    // projected `pk` column via the QueryRow value map.
    let checked =
        assert_multirow_seek_parity(&db, "test_da.wide_table", "pk, ck, payload", "pk", |row| {
            match row.values.get("pk") {
                Some(cqlite_core::Value::Integer(i)) => Some(i.to_string()),
                _ => None,
            }
        })
        .await;
    let Some(checked) = checked else {
        eprintln!("Skipping (BTI wide_table): fixture not present in this dataset");
        return;
    };

    assert!(
        checked >= 1,
        "Issue #953 (BTI): expected at least one multi-clustering-row partition in \
         test_da.wide_table (3 partitions × 300 rows); validated {checked}",
    );
    println!("Issue #953 (BTI): multi-row seek == full-scan parity for {checked} partition(s)");
}

/// BIG (`nb`) format multi-row seek parity: `test_timeseries.sensor_data`,
/// ~10 partitions of ~200 rows each, offset resolved via `Index.db`.
#[tokio::test]
async fn big_multirow_partition_seek_returns_all_rows() {
    let db = match setup("time-series.cql", "/test_timeseries/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping (BIG sensor_data): {e}");
            return;
        }
    };

    let checked = assert_multirow_seek_parity(
        &db,
        "test_timeseries.sensor_data",
        "sensor_id, timestamp, temperature",
        "sensor_id",
        |row| match row.values.get("sensor_id") {
            Some(cqlite_core::Value::Uuid(b)) => {
                let h = |range: std::ops::Range<usize>| -> String {
                    b[range].iter().map(|x| format!("{x:02x}")).collect()
                };
                Some(format!(
                    "{}-{}-{}-{}-{}",
                    h(0..4),
                    h(4..6),
                    h(6..8),
                    h(8..10),
                    h(10..16)
                ))
            }
            _ => None,
        },
    )
    .await;
    let Some(checked) = checked else {
        eprintln!("Skipping (BIG sensor_data): fixture not present in this dataset");
        return;
    };

    assert!(
        checked >= 1,
        "Issue #953 (BIG): expected at least one multi-clustering-row partition in \
         test_timeseries.sensor_data (~10 partitions × ~200 rows); validated {checked}",
    );
    println!("Issue #953 (BIG): multi-row seek == full-scan parity for {checked} partition(s)");
}
