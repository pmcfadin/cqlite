//! Issue #4531 — clustering SLICES over `timeuuid` / `uuid` clustering columns of a
//! Cassandra-written BTI (`da`) table, compared with the `sstabledump` golden.
//!
//! Subject: `physical_byte_bounds_for_slice` builds a slice's `Rows.db` byte window with
//! `encode_clustering_bound_oss50_with_order`, which writes a `uuid`/`timeuuid` as its RAW 16
//! bytes; Cassandra's `UUIDType`/`TimeUUIDType.asComparableBytes` swizzle them (version-first /
//! timestamp-reordered), and that swizzled order is what the Cassandra-written row-index
//! separators are in. If the two orders disagree, block selection can drop blocks holding
//! matching rows.
//!
//! Fixture: `test-data/fixtures/issue_4531/test_uuid_bti/*` — Cassandra 5.0.8, BTI, one large
//! partition (pk=1, many `Rows.db` blocks, asserted below) and one small control partition
//! (pk=2); generator `test-data/scripts/generate-issue-4531-uuid-clustering-bti.sh`.
//!
//! Oracle: every expected row list is derived from the committed `sstabledump -l` golden
//! (`nb`-order = Cassandra clustering order), never from CQLite. Each slice runs under BOTH
//! forced read paths (`Point` = the `Rows.db` window; `Full` = whole partition + filter).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "lz4",
    not(feature = "tombstones")
))]

use std::io::Cursor;
use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::bti::{
    lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry, BtiPartitionLocation,
};
use cqlite_core::types::Value;
use cqlite_core::Database;

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_uuid_bti";
const SCHEMA: &str = "issue-4531-uuid-clustering-bti.cql";
/// `(table, clustering is DESC)`.
const TABLES: [(&str, bool); 3] = [
    ("timeuuid_clustering", false),
    ("timeuuid_clustering_desc", true),
    ("uuid_clustering", false),
];
const BIG_ROWS: usize = 480;
const SMALL_ROWS: usize = 8;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4531")
}

fn gen_dir(table: &str) -> PathBuf {
    let dirs = datasets_root::table_generation_dirs(&fixture_root(), KEYSPACE, table);
    assert_eq!(
        dirs.len(),
        1,
        "{table}: expected one generation dir under the committed fixture"
    );
    dirs[0].clone()
}

fn component(table: &str, suffix: &str) -> Vec<u8> {
    let dir = gen_dir(table);
    let path = std::fs::read_dir(&dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(suffix))
        })
        .unwrap_or_else(|| panic!("{table}: no *{suffix} in {dir:?}"));
    let b = std::fs::read(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    assert!(!b.is_empty(), "{path:?} is present but EMPTY");
    b
}

/// `(pk, ck)` for every golden `row`, in file (= Cassandra clustering) order.
fn golden(table: &str) -> Vec<(i32, String)> {
    let text = String::from_utf8(component(table, "-Data.db.jsonl")).expect("utf8 golden");
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("golden line is JSON");
        let pk: i32 = v
            .pointer("/partition/key/0")
            .and_then(|k| k.as_str())
            .and_then(|s| s.parse().ok())
            .expect("int partition key");
        for r in v["rows"].as_array().expect("rows") {
            if r["type"] == "row" {
                out.push((
                    pk,
                    r["clustering"][0].as_str().expect("uuid ck").to_lowercase(),
                ));
            }
        }
    }
    out
}

/// Row-index block count of partition `pk` (from `Partitions.db` -> `Rows.db` header).
fn block_count(table: &str, pk: i32) -> u32 {
    let mut cur = Cursor::new(component(table, "-Partitions.db"));
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &pk.to_be_bytes())
        .expect("Partitions.db lookup")
        .expect("pk present");
    let BtiPartitionLocation::RowsOffset(off) = loc else {
        return 0; // narrow partition: no row index
    };
    resolve_rows_db_entry(&component(table, "-Rows.db"), off as usize)
        .expect("Rows.db entry")
        .block_count as u32
}

async fn open_db(mode: ReadPathMode) -> Database {
    let mut core_config = cqlite_core::Config::default();
    core_config.query.forced_read_path = Some(mode);
    let schema = datasets_root::schema_path(SCHEMA).expect("committed schema");
    ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    })
    .await
    .expect("ingest")
    .database
}

fn hyphenated(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

async fn select_cks(db: &Database, table: &str, predicate: &str) -> Result<Vec<String>, String> {
    let q = format!("SELECT pk, ck FROM {KEYSPACE}.{table} WHERE {predicate}");
    let res = db
        .execute(&q)
        .await
        .map_err(|e| format!("`{q}` failed: {e}"))?;
    res.rows
        .iter()
        .map(|r| match r.values.get("ck") {
            Some(Value::Uuid(b)) => Ok(hyphenated(b)),
            other => Err(format!("`{q}`: ck decoded as {other:?}")),
        })
        .collect()
}

/// `(label, predicate, expected rows in clustering order)` for one table's big partition.
/// "Greater/less" are in the COLUMN's value order; the file list `l` is in CLUSTERING order,
/// which is ascending for an ASC column and descending for a DESC one.
fn cases(l: &[String], desc: bool) -> Vec<(String, String, Vec<String>)> {
    let n = l.len();
    let mut out = Vec::new();
    let (i, j) = (n / 4, n / 4 + n / 8);
    // value-order lo < hi  =>  index(lo) = i (ASC) or j (DESC)
    let (lo_i, hi_i) = if desc { (j, i) } else { (i, j) };
    let (lo, hi) = (&l[lo_i], &l[hi_i]);
    let between = l[i + 1..j].to_vec();
    out.push((
        "ck > lo AND ck < hi".into(),
        format!("pk = 1 AND ck > {lo} AND ck < {hi}"),
        between,
    ));
    let m = n / 2;
    let x = &l[m];
    let (gt, ge, lt, le) = if desc {
        (
            l[..m].to_vec(),
            l[..=m].to_vec(),
            l[m + 1..].to_vec(),
            l[m..].to_vec(),
        )
    } else {
        (
            l[m + 1..].to_vec(),
            l[m..].to_vec(),
            l[..m].to_vec(),
            l[..=m].to_vec(),
        )
    };
    out.push(("ck > mid".into(), format!("pk = 1 AND ck > {x}"), gt));
    out.push(("ck >= mid".into(), format!("pk = 1 AND ck >= {x}"), ge));
    out.push(("ck < mid".into(), format!("pk = 1 AND ck < {x}"), lt));
    out.push(("ck <= mid".into(), format!("pk = 1 AND ck <= {x}"), le));
    out.push((
        "ck = mid".into(),
        format!("pk = 1 AND ck = {x}"),
        vec![x.clone()],
    ));
    let (last, first) = (&l[n - 1], &l[0]);
    out.push((
        "ck = last".into(),
        format!("pk = 1 AND ck = {last}"),
        vec![last.clone()],
    ));
    out.push((
        "ck = first".into(),
        format!("pk = 1 AND ck = {first}"),
        vec![first.clone()],
    ));
    out
}

/// Run the whole matrix on one read path; one line per query, returns the mismatches.
async fn run_matrix(mode: ReadPathMode) -> Vec<String> {
    let db = open_db(mode).await;
    let mut bad = Vec::new();
    for (table, desc) in TABLES {
        let g = golden(table);
        let big: Vec<String> = g
            .iter()
            .filter(|(p, _)| *p == 1)
            .map(|(_, c)| c.clone())
            .collect();
        let small: Vec<String> = g
            .iter()
            .filter(|(p, _)| *p == 2)
            .map(|(_, c)| c.clone())
            .collect();
        assert_eq!(big.len(), BIG_ROWS, "{table}: golden big-partition rows");
        assert_eq!(
            small.len(),
            SMALL_ROWS,
            "{table}: golden small-partition rows"
        );
        let blocks = block_count(table, 1);
        assert!(
            blocks >= 8,
            "{table}: big partition must span several row-index blocks, got {blocks}"
        );
        eprintln!("[{mode:?}] {table}: big partition {BIG_ROWS} rows, {blocks} row-index blocks");

        // whole-partition controls
        let mut all = vec![
            (
                "pk = 1 (whole)".to_string(),
                "pk = 1".to_string(),
                big.clone(),
            ),
            (
                "pk = 2 (small, whole)".to_string(),
                "pk = 2".to_string(),
                small.clone(),
            ),
        ];
        all.extend(cases(&big, desc));
        for (label, pred, want) in all {
            let verdict = match select_cks(&db, table, &pred).await {
                Ok(got) if got == want => "PASS".to_string(),
                Ok(got) => format!(
                    "FAIL got {} rows, want {} (missing {}, extra {})",
                    got.len(),
                    want.len(),
                    want.iter().filter(|w| !got.contains(w)).count(),
                    got.iter().filter(|g| !want.contains(g)).count()
                ),
                Err(e) => format!("FAIL {e}"),
            };
            eprintln!("[{mode:?}] {table:<26} {label:<22} {verdict}");
            if verdict != "PASS" {
                bad.push(format!("[{mode:?}] {table} {label}: {verdict}"));
            }
        }
    }
    bad
}

/// Equality lookups isolate the ENCODER from the value COMPARATOR: `ck = x` needs no ordering
/// comparison, so any miss on the Point path is the `Rows.db` window (raw-byte bounds against
/// swizzled separators), not the predicate evaluation. Returns `(looked_up, missed)` per table.
async fn equality_misses(mode: ReadPathMode) -> Vec<String> {
    let db = open_db(mode).await;
    let mut bad = Vec::new();
    for (table, _) in TABLES {
        let big: Vec<String> = golden(table)
            .into_iter()
            .filter(|(p, _)| *p == 1)
            .map(|(_, c)| c)
            .collect();
        let mut looked = 0;
        let mut missed = 0;
        for x in big.iter().step_by(8) {
            looked += 1;
            match select_cks(&db, table, &format!("pk = 1 AND ck = {x}")).await {
                Ok(got) if got == vec![x.clone()] => {}
                _ => missed += 1,
            }
        }
        eprintln!("[{mode:?}] {table:<26} ck = <existing row>: {missed} of {looked} lookups returned the wrong rows");
        if missed > 0 {
            bad.push(format!(
                "[{mode:?}] {table}: {missed}/{looked} equality lookups of rows that EXIST missed"
            ));
        }
    }
    bad
}

#[tokio::test]
async fn full_path_equality_finds_every_row() {
    let bad = equality_misses(ReadPathMode::Full).await;
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[tokio::test]
async fn point_path_equality_finds_every_row() {
    let bad = equality_misses(ReadPathMode::Point).await;
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[tokio::test]
async fn full_path_slices_match_the_golden() {
    let bad = run_matrix(ReadPathMode::Full).await;
    assert!(
        bad.is_empty(),
        "full-scan path disagrees with the Cassandra golden:\n{}",
        bad.join("\n")
    );
}

#[tokio::test]
async fn point_path_slices_match_the_golden() {
    let bad = run_matrix(ReadPathMode::Point).await;
    assert!(
        bad.is_empty(),
        "targeted (Rows.db window) path disagrees with the Cassandra golden:\n{}",
        bad.join("\n")
    );
}
