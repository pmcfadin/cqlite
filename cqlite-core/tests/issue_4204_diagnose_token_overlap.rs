//! Issue #4204 — token overlap per generation (spec R4.1, `design.md` D4).
//!
//! Real multi-generation tables with distinct key sets do not exist in the committed corpus, so
//! the test STITCHES one: two Cassandra-written single-generation fixtures are copied into one
//! temp table dir, the second renamed to the next generation number (`nb-1-big-*` → `nb-2-big-*`);
//! `diagnose` reads only Statistics.db/Summary.db/Partitions.db by file name, so this is a valid
//! two-generation table for the metadata-only computation.
//!
//! ORACLE: each generation's first/last partition key comes from its Cassandra-written
//! `sstabledump` golden (partitions are listed in token order), tokens from the shared Murmur3
//! helper, and the per-bucket intersection counts are computed HERE (`D4`: `K = 16` equal-width
//! buckets over the union of the spans). Data.db is never needed — it is replaced by an empty file.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use serde_json::Value as Json;

const K: usize = 16;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn fixture(ks: &str, table: &str) -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir(ks, table) {
        Ok(d) => Some(d),
        Err(e) if strict() => panic!("{ks}.{table}: missing under CQLITE_REQUIRE_FIXTURES: {e}"),
        Err(e) => {
            eprintln!("SKIP {ks}.{table}: {e}");
            None
        }
    }
}

fn key_bytes(v: &Json) -> Vec<u8> {
    let s = v.as_str().expect("string key");
    if let Ok(i) = s.parse::<i32>() {
        return i.to_be_bytes().to_vec();
    }
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32, "key {s:?} is neither an int nor a uuid");
    (0..16)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

/// `(first_token, last_token)` of the generation whose Data.db is `data_db`, from its golden.
fn golden_span(data_db: &Path) -> (i64, i64) {
    let text = std::fs::read_to_string(format!("{}.jsonl", data_db.display())).expect("golden");
    let objs: Vec<Json> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text).expect("array")
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("jsonl"))
            .collect()
    };
    let (first, last) = (
        objs.first().expect("non-empty golden"),
        objs.last().expect("non-empty golden"),
    );
    assert_eq!(
        first["partition"]["key"].as_array().map(Vec::len),
        Some(1),
        "single-column keys only"
    );
    (
        cassandra_murmur3_token(&key_bytes(&first["partition"]["key"][0])),
        cassandra_murmur3_token(&key_bytes(&last["partition"]["key"][0])),
    )
}

/// D4, re-implemented independently: K equal-width buckets over the union of the spans (the last
/// bucket extends to `hi`); a bucket counts every span that intersects it.
fn expected_counts(spans: &[(i64, i64)]) -> Vec<u64> {
    let lo = spans
        .iter()
        .map(|s| s.0.min(s.1) as i128)
        .min()
        .expect("spans");
    let hi = spans
        .iter()
        .map(|s| s.0.max(s.1) as i128)
        .max()
        .expect("spans");
    let w = ((hi - lo) / K as i128).max(1);
    (0..K)
        .map(|i| {
            let b_lo = lo + w * i as i128;
            let b_hi = if i + 1 == K {
                hi
            } else {
                lo + w * (i as i128 + 1) - 1
            };
            spans
                .iter()
                .filter(|s| (s.0.min(s.1) as i128) <= b_hi && (s.0.max(s.1) as i128) >= b_lo)
                .count() as u64
        })
        .collect()
}

fn stitch(a: &Path, b: &Path, b_gen: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    for (src, rename_to) in [(a, None), (b, Some(b_gen))] {
        // ONE generation per source: the components of the first `*-Data.db` (a fixture dir may
        // hold several generations, which would collide under the single renamed number).
        let data = data_db(src);
        let prefix = data
            .file_name()
            .expect("name")
            .to_string_lossy()
            .trim_end_matches("Data.db")
            .to_string();
        for e in std::fs::read_dir(src).expect("fixture dir") {
            let p = e.expect("entry").path();
            let name = p.file_name().expect("name").to_string_lossy().into_owned();
            if !p.is_file() || name.ends_with(".jsonl") || !name.starts_with(&prefix) {
                continue;
            }
            // `<ver>-<gen>-<fmt>-<Component>`: swap the generation number for the renamed one.
            let dest_name = match rename_to {
                Some(g) => {
                    let mut it = name.splitn(4, '-');
                    let (v, _g, f, c) = (
                        it.next().expect("v"),
                        it.next(),
                        it.next().expect("f"),
                        it.next().expect("c"),
                    );
                    format!("{v}-{g}-{f}-{c}")
                }
                None => name.clone(),
            };
            let dest = tmp.path().join(&dest_name);
            if dest_name.ends_with("-Data.db") {
                std::fs::write(&dest, b"").expect("empty Data.db");
            } else {
                std::fs::copy(&p, &dest).expect("copy");
            }
        }
    }
    tmp
}

fn data_db(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("Data.db")
}

async fn run(case: &str, a: (&str, &str), b: (&str, &str), b_gen: &str) {
    let (Some(da), Some(db)) = (fixture(a.0, a.1), fixture(b.0, b.1)) else {
        return;
    };
    let spans = vec![golden_span(&data_db(&da)), golden_span(&data_db(&db))];
    let stitched = stitch(&da, &db, b_gen);
    let report = diagnose_table(
        stitched.path(),
        &DiagnoseOptions {
            now_secs: 1_794_545_000,
            deep: false,
            top_n: 5,
            gc_grace_seconds: Some(864_000),
            schema: None,
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: {e}"));
    assert_eq!(report.generations.len(), 2, "{case}: two generations");
    assert_eq!(report.token_overlap.buckets, K);
    let want = expected_counts(&spans);
    assert_eq!(report.token_overlap.counts, want, "{case}: spans {spans:?}");
    assert!(
        want.iter().any(|c| *c == 2) || want.iter().any(|c| *c == 1),
        "{case}: non-vacuous"
    );
    eprintln!("{case}: spans {spans:?} -> {want:?}");
}

/// BIG (`nb`): spans come from `Summary.db`.
#[tokio::test]
async fn big_generations_token_overlap_matches_independent_intersection_counts() {
    run(
        "BIG",
        ("test_basic", "simple_table"),
        ("test_tomb", "skipped_partition_delete"),
        "2",
    )
    .await;
}

/// BTI (`da`): spans come from the `Partitions.db` footer (no Summary.db exists).
#[tokio::test]
async fn bti_generations_token_overlap_matches_independent_intersection_counts() {
    run(
        "BTI",
        ("test_da", "simple_table"),
        ("test_da", "wide_table"),
        "3",
    )
    .await;
}
