//! Issue #4204 — `diagnose --deep` (spec R3.1, R3.2, R3.3).
//!
//! Every expectation is derived INDEPENDENTLY of `diagnose`'s scan:
//!  * partition START offsets come from the BIG `Index.db` entries (read with `IndexReader`, see
//!    `support/partition_start.rs`) and the last partition ends at the `Data.db` file length (these
//!    fixtures are uncompressed or read via their stated length) — never from the scan;
//!  * widths and raw tombstone counts come from the Cassandra-written `sstabledump` golden
//!    (`*-Data.db.jsonl`), counted by the rule of `SSTableMetadataViewer.printScannedOverview`
//!    (cassandra-5.0.8): partition-level deletion +1, each range-tombstone marker row +1, each
//!    cell tombstone +1 (a cell with `deletion_info` that is not a complex-column header); row
//!    deletions are rows with no tombstone count.
//!
//! The bucket edges are the module's documented constants; the histograms/rankings are rebuilt
//! here from those per-partition values.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/partition_start.rs"]
mod partition_start;

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::storage::sstable::diagnose::deep_scan::{
    HistogramBucket, RankedPartition, SIZE_BUCKET_LOWER_EDGES, WIDTH_BUCKET_LOWER_EDGES,
};
use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions};
use serde_json::Value as Json;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

const CASES: &[(&str, &str)] = &[
    ("test_wide_rows", "chat_messages"),
    ("test_wide_rows", "wide_partition_table"),
    ("test_wide_rows", "product_catalog"),
    ("test_tomb", "skipped_partition_delete"),
    ("test_tomb", "wide_range_tombstone"),
    ("test_tomb", "static_with_tombstones"),
    ("test_tomb", "resurrection_gc_positive"),
    ("test_tomb", "dropped_regular_col"),
];

struct Expected {
    sizes: Vec<u64>,
    widths: Vec<u64>,
    tombstones: Vec<u64>,
    keys: Vec<Vec<u8>>,
}

fn golden_objects(path: &Path) -> Vec<Json> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    let v: Vec<Json> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text).expect("json array")
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("jsonl"))
            .collect()
    };
    assert!(!v.is_empty(), "{path:?}: empty golden");
    v
}

fn cell_tombstones(cells: &Json) -> u64 {
    cells
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|c| {
                    c.get("deletion_info").is_some()
                        // a complex-column deletion HEADER has `marked_deleted` and no `path`
                        && (c.get("path").is_some() || c["deletion_info"].get("marked_deleted").is_none())
                })
                .count() as u64
        })
        .unwrap_or(0)
}

fn expected(data_db: &Path) -> Expected {
    let index = PathBuf::from(data_db.to_string_lossy().replace("-Data.db", "-Index.db"));
    let starts = partition_start::big_partition_starts(&index);
    // The data section's UNCOMPRESSED length: CompressionInfo.db's `data_length` when compressed
    // (Cassandra writes it), else the Data.db file length.
    let ci = PathBuf::from(
        data_db
            .to_string_lossy()
            .replace("-Data.db", "-CompressionInfo.db"),
    );
    let data_len = match std::fs::read(&ci) {
        Ok(b) => {
            CompressionInfo::parse(&b)
                .expect("CompressionInfo.db")
                .data_length
        }
        Err(_) => std::fs::metadata(data_db).expect("Data.db").len(),
    };
    let golden = golden_objects(&PathBuf::from(format!("{}.jsonl", data_db.display())));
    assert_eq!(
        golden.len(),
        starts.len(),
        "{data_db:?}: golden partitions vs Index.db entries"
    );
    let mut e = Expected {
        sizes: vec![],
        widths: vec![],
        tombstones: vec![],
        keys: vec![],
    };
    for (i, p) in golden.iter().enumerate() {
        let end = starts.get(i + 1).map_or(data_len, |s| s.1);
        e.sizes.push(end - starts[i].1);
        e.keys.push(starts[i].0.clone());
        let mut width = 0;
        let mut tombs = u64::from(p["partition"].get("deletion_info").is_some());
        for r in p["rows"].as_array().expect("rows") {
            match r["type"].as_str().expect("type") {
                "row" => {
                    width += 1;
                    tombs += cell_tombstones(&r["cells"]);
                }
                "static_block" => {
                    width += 1;
                    tombs += cell_tombstones(&r["cells"]);
                }
                _ => tombs += 1, // range_tombstone_bound / range_tombstone_boundary: one marker each
            }
        }
        e.widths.push(width);
        e.tombstones.push(tombs);
    }
    e
}

fn histogram(values: &[u64], edges: &[u64]) -> Vec<HistogramBucket> {
    let mut h: Vec<HistogramBucket> = edges
        .iter()
        .enumerate()
        .map(|(i, lo)| (*lo, edges.get(i + 1).copied().unwrap_or(u64::MAX), 0))
        .collect();
    for v in values {
        let b = h.iter_mut().rev().find(|b| v >= &b.0).expect("bucket");
        b.2 += 1;
    }
    h
}

/// Top-N as `(metric desc)`; keys compared as a SET within each metric group (ties have no
/// defined order in Cassandra's queue either).
fn assert_ranking(
    case: &str,
    what: &str,
    got: &[RankedPartition],
    keys: &[Vec<u8>],
    metric: &[u64],
    n: usize,
    skip_zero: bool,
) {
    let mut all: Vec<(u64, &Vec<u8>)> = metric
        .iter()
        .copied()
        .zip(keys)
        .filter(|(m, _)| !skip_zero || *m > 0)
        .collect();
    all.sort_by_key(|x| std::cmp::Reverse(x.0));
    let want_metrics: Vec<u64> = all.iter().take(n).map(|x| x.0).collect();
    let got_metrics: Vec<u64> = got.iter().map(|p| p.metric).collect();
    assert_eq!(got_metrics, want_metrics, "{case}: {what} metrics");
    // every returned key must really carry that metric
    for p in got {
        assert!(
            all.iter().any(|(m, k)| *m == p.metric && **k == p.key),
            "{case}: {what}: key {:02x?} does not have metric {}",
            p.key,
            p.metric
        );
    }
}

fn data_dbs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .collect();
    v.sort();
    v
}

/// R3.1 + R3.2 over every case, per generation, plus R3.3's no-write check on a temp copy.
#[tokio::test]
async fn deep_scan_matches_values_derived_from_the_goldens() {
    let mut cases_run = 0;
    let mut tomb_cases = 0;
    for (ks, table) in CASES {
        let dir = match datasets_root::resolve_table_generation_dir(ks, table) {
            Ok(d) => d,
            Err(e) if strict() => {
                panic!("{ks}.{table}: fixture missing under CQLITE_REQUIRE_FIXTURES: {e}")
            }
            Err(e) => {
                eprintln!("SKIP {ks}.{table}: {e}");
                continue;
            }
        };
        // R3.3: the run happens on a copy and must leave it byte-identical (no write anywhere).
        let tmp = tempfile::tempdir().expect("tempdir");
        for e in std::fs::read_dir(&dir).expect("dir") {
            let p = e.expect("entry").path();
            if p.is_file() {
                std::fs::copy(&p, tmp.path().join(p.file_name().expect("name"))).expect("copy");
            }
        }
        let listing = |d: &Path| -> Vec<(String, u64)> {
            let mut v: Vec<(String, u64)> = std::fs::read_dir(d)
                .expect("dir")
                .filter_map(|e| e.ok())
                .map(|e| {
                    (
                        e.file_name().to_string_lossy().into_owned(),
                        e.metadata().expect("meta").len(),
                    )
                })
                .collect();
            v.sort();
            v
        };
        let before = listing(tmp.path());

        let report = diagnose_table(
            tmp.path(),
            &DiagnoseOptions {
                now_secs: 1_794_545_000,
                deep: true,
                top_n: 5,
                gc_grace_seconds: Some(864_000),
                schema: None,
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{ks}.{table}: diagnose --deep failed: {e}"));
        assert_eq!(
            before,
            listing(tmp.path()),
            "{ks}.{table}: --deep must not create or modify any file"
        );

        let dbs = data_dbs(&dir);
        assert_eq!(
            report.generations.len(),
            dbs.len(),
            "{ks}.{table}: one entry per generation"
        );
        for db in &dbs {
            let e = expected(db);
            let name = db.file_name().expect("name").to_string_lossy().into_owned();
            let gen: u64 = name
                .split('-')
                .nth(1)
                .expect("gen")
                .parse()
                .expect("numeric");
            let g = report
                .generations
                .iter()
                .find(|g| g.generation == gen.to_string())
                .unwrap_or_else(|| panic!("{ks}.{table}: no generation {gen}"));
            let deep = g
                .deep
                .as_ref()
                .unwrap_or_else(|| panic!("{ks}.{table} gen {gen}: deep missing"));
            let case = format!("{ks}.{table} gen {gen}");
            assert_eq!(
                deep.partition_size_histogram,
                histogram(&e.sizes, SIZE_BUCKET_LOWER_EDGES),
                "{case}: partition-size histogram"
            );
            assert_eq!(
                deep.clustering_width_histogram,
                histogram(&e.widths, WIDTH_BUCKET_LOWER_EDGES),
                "{case}: clustering-width histogram"
            );
            assert_ranking(
                &case,
                "top largest",
                &deep.top_largest_partitions,
                &e.keys,
                &e.sizes,
                5,
                false,
            );
            assert_ranking(
                &case,
                "top tombstone-heaviest (raw)",
                &deep.top_tombstone_heaviest_partitions,
                &e.keys,
                &e.tombstones,
                5,
                true,
            );
            assert!(
                deep.top_largest_partitions.len() <= 5
                    && deep.top_tombstone_heaviest_partitions.len() <= 5
            );
            if e.tombstones.iter().any(|t| *t > 0) {
                tomb_cases += 1;
            }
            eprintln!(
                "{case}: {} partitions, sizes {:?}.., widths max {}, tombstones max {}",
                e.sizes.len(),
                e.sizes.iter().take(3).collect::<Vec<_>>(),
                e.widths.iter().max().unwrap_or(&0),
                e.tombstones.iter().max().unwrap_or(&0)
            );
        }
        cases_run += 1;
    }
    if strict() {
        assert_eq!(
            cases_run,
            CASES.len(),
            "every case must run under CQLITE_REQUIRE_FIXTURES"
        );
    }
    assert!(
        cases_run == 0 || tomb_cases >= 3,
        "the tombstone ranking must be exercised on real tombstones, got {tomb_cases} cases"
    );
}
