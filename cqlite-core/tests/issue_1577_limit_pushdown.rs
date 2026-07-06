//! Issue #1577 (Epic D / D1): correctness matrix for LIMIT/OFFSET pushdown into
//! the materializing scan.
//!
//! The pushdown must be RESULT-PRESERVING: for every query shape the rows and
//! their order must be byte-identical to the pre-change full-materialize path.
//! Each test here uses the UNBOUNDED query as the oracle and asserts the bounded
//! query returns exactly the corresponding slice — so a regression that dropped
//! or reordered rows (the failure mode a naive raw-row scan cap would cause when
//! a null-row marker or a predicate miss shortens the accepted set) fails loudly.
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when
//! the fixture is absent, and treats a present-but-0-rows result as a hard
//! failure (never a silent skip).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Database, Value};

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

/// Project each row's `id` UUID (the unique partition key of `simple_table`) in
/// result order — a stable, comparable identity for oracle-vs-bounded checks.
fn row_ids(rows: &[cqlite_core::QueryRow]) -> Vec<[u8; 16]> {
    rows.iter()
        .filter_map(|r| match r.values.get("id") {
            Some(Value::Uuid(b)) => Some(*b),
            _ => None,
        })
        .collect()
}

const KS: &str = "test_basic";
const TABLE: &str = "test_basic.simple_table";

/// Run the shared setup, or `None` to skip.
async fn simple_table_db() -> Option<Database> {
    if !fixture_data_present(KS, "simple_table") {
        eprintln!("Skipping (#1577): test_basic/simple_table Data.db not present");
        return None;
    }
    match setup(KS, "basic-types.cql").await {
        Some(db) => Some(db),
        None => {
            eprintln!("Skipping (#1577): could not ingest test_basic");
            None
        }
    }
}

/// LIMIT N (no OFFSET): the bounded result is exactly the first N rows of the
/// unbounded scan, in the same order.
#[tokio::test]
async fn limit_matches_first_n_of_full_scan() {
    let Some(db) = simple_table_db().await else {
        return;
    };

    let full = db
        .execute(&format!("SELECT * FROM {TABLE}"))
        .await
        .expect("full scan");
    let full_ids = row_ids(&full.rows);
    assert!(
        full_ids.len() > 20,
        "fixture must be large enough to exercise a real limit (got {} rows)",
        full_ids.len()
    );

    for n in [1usize, 5, 10] {
        let limited = db
            .execute(&format!("SELECT * FROM {TABLE} LIMIT {n}"))
            .await
            .expect("limited scan");
        assert_eq!(limited.rows.len(), n, "LIMIT {n} must return exactly {n}");
        assert_eq!(
            row_ids(&limited.rows),
            full_ids[..n].to_vec(),
            "LIMIT {n} rows must equal the first {n} full-scan rows, in order"
        );
    }
}

/// LIMIT N OFFSET K: exactly the `K..K+N` slice of the unbounded scan.
#[tokio::test]
async fn limit_offset_matches_full_scan_slice() {
    let Some(db) = simple_table_db().await else {
        return;
    };

    let full = db
        .execute(&format!("SELECT * FROM {TABLE}"))
        .await
        .expect("full scan");
    let full_ids = row_ids(&full.rows);
    assert!(full_ids.len() > 30, "fixture too small for OFFSET test");

    for (n, k) in [(5usize, 0usize), (5, 3), (10, 7), (3, 20)] {
        let q = format!("SELECT * FROM {TABLE} LIMIT {n} OFFSET {k}");
        let limited = db.execute(&q).await.expect("limit+offset scan");
        let expected = full_ids[k..(k + n)].to_vec();
        assert_eq!(
            limited.rows.len(),
            n,
            "LIMIT {n} OFFSET {k} must return {n} rows"
        );
        assert_eq!(
            row_ids(&limited.rows),
            expected,
            "LIMIT {n} OFFSET {k} must equal full-scan[{k}..{}]",
            k + n
        );
    }
}

/// LIMIT larger than the table returns the whole table, unchanged.
#[tokio::test]
async fn limit_larger_than_table_returns_all() {
    let Some(db) = simple_table_db().await else {
        return;
    };

    let full = db
        .execute(&format!("SELECT * FROM {TABLE}"))
        .await
        .expect("full scan");
    let big = db
        .execute(&format!("SELECT * FROM {TABLE} LIMIT 100000"))
        .await
        .expect("oversized limit");
    assert_eq!(
        row_ids(&big.rows),
        row_ids(&full.rows),
        "an oversized LIMIT must return every row, in full-scan order"
    );
}

/// LIMIT 0 returns no rows (and never a budget error).
#[tokio::test]
async fn limit_zero_returns_empty() {
    let Some(db) = simple_table_db().await else {
        return;
    };
    let zero = db
        .execute(&format!("SELECT * FROM {TABLE} LIMIT 0"))
        .await
        .expect("LIMIT 0 must succeed");
    assert_eq!(zero.rows.len(), 0, "LIMIT 0 must return no rows");
}

/// LIMIT with a residual (non-partition-key) predicate: the cap counts ACCEPTED
/// rows, so the bounded result equals the first N rows of the FILTERED full scan
/// — never the first N raw rows.
#[tokio::test]
async fn limit_with_predicate_counts_accepted_rows() {
    let Some(db) = simple_table_db().await else {
        return;
    };

    let filter = "age > 30";
    let full = db
        .execute(&format!("SELECT * FROM {TABLE} WHERE {filter}"))
        .await
        .expect("filtered full scan");
    let full_ids = row_ids(&full.rows);
    assert!(
        full_ids.len() > 6,
        "predicate must match enough rows to exercise LIMIT (matched {})",
        full_ids.len()
    );

    for n in [1usize, 3, 5] {
        let limited = db
            .execute(&format!("SELECT * FROM {TABLE} WHERE {filter} LIMIT {n}"))
            .await
            .expect("filtered limited scan");
        assert_eq!(
            limited.rows.len(),
            n,
            "WHERE {filter} LIMIT {n} must return {n} matching rows"
        );
        assert_eq!(
            row_ids(&limited.rows),
            full_ids[..n].to_vec(),
            "WHERE {filter} LIMIT {n} must equal the first {n} FILTERED rows"
        );
    }
}

/// Format a 16-byte UUID as the canonical unquoted 8-4-4-4-12 literal.
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

/// LIMIT + ORDER BY (clustering `version` DESC within a partition): the Sort step
/// disables the scan pushdown, so the result is the full-sort's first N rows —
/// identical to the unbounded ordered query truncated to N. Uses the
/// `event_store` clustering-key fixture and discovers a partition with several
/// clustering rows so the ORDER BY is valid CQL and the prefix check is meaningful.
#[tokio::test]
async fn limit_with_order_by_matches_full_sort_prefix() {
    if !fixture_data_present("test_timeseries", "event_store") {
        eprintln!("Skipping (#1577): test_timeseries/event_store Data.db not present");
        return;
    }
    let Some(db) = setup("test_timeseries", "time-series.cql").await else {
        eprintln!("Skipping (#1577): could not ingest test_timeseries");
        return;
    };

    // Discover a live `aggregate_id` partition holding several clustering rows.
    let probe = db
        .execute("SELECT aggregate_id, version FROM test_timeseries.event_store")
        .await
        .expect("probe scan");
    assert!(
        !probe.rows.is_empty(),
        "present fixture must return rows (0 = read regression, not a skip)"
    );
    let mut counts: std::collections::HashMap<[u8; 16], usize> = std::collections::HashMap::new();
    for r in &probe.rows {
        if let Some(Value::Uuid(b)) = r.values.get("aggregate_id") {
            *counts.entry(*b).or_default() += 1;
        }
    }
    let Some((agg, _)) = counts.iter().find(|(_, n)| **n >= 3) else {
        eprintln!("Skipping (#1577): no event_store partition with >= 3 clustering rows");
        return;
    };
    let agg_literal = uuid_to_literal(agg);

    let ordered = format!(
        "SELECT * FROM test_timeseries.event_store WHERE aggregate_id = {agg_literal} \
         ORDER BY version DESC"
    );

    let full = db.execute(&ordered).await.expect("ordered full scan");
    // `version` (BIGINT) in result order is the sort-key identity.
    let versions = |rows: &[cqlite_core::QueryRow]| -> Vec<i64> {
        rows.iter()
            .filter_map(|r| match r.values.get("version") {
                Some(Value::BigInt(v)) => Some(*v),
                _ => None,
            })
            .collect()
    };
    let full_versions = versions(&full.rows);
    assert!(
        full_versions.len() >= 3,
        "chosen partition must expose its clustering rows in DESC order"
    );

    let limited = db
        .execute(&format!("{ordered} LIMIT 2"))
        .await
        .expect("ordered limited scan");
    assert_eq!(limited.rows.len(), 2, "ORDER BY ... LIMIT 2 returns 2 rows");
    assert_eq!(
        versions(&limited.rows),
        full_versions[..2].to_vec(),
        "ORDER BY version DESC ... LIMIT 2 must equal the first 2 rows of the full DESC sort"
    );
}
