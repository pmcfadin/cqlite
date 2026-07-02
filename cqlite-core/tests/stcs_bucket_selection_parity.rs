//! STCS bucket-selection parity (issue #1407).
//!
//! Mirrors Apache Cassandra's `SizeTieredCompactionStrategyTest.java`
//! (`test/unit/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategyTest.java`,
//! cassandra-5.0.2) at the algorithm level: CQLite ships STCS
//! (`cqlite_core::storage::write_engine::STCSPolicy`), so its size-based bucketing
//! and candidate selection must match Cassandra's documented STCS rules
//! (`getBuckets` + `min_threshold`/`max_threshold` selection, controlled by
//! `bucket_low`/`bucket_high`/`min_sstable_size`).
//!
//! Evidence is canonical-semantic, not byte-parity: the fixture
//! `test-data/parity/stcs_bucket_selection_vectors.jsonl` encodes input SSTable
//! sizes and the expected selected candidate set for the Cassandra-default policy
//! (min_threshold=4, max_threshold=32, bucket_low=0.5, bucket_high=1.5,
//! min_sstable_size=50MB), each vector citing the documented STCS rule it
//! exercises. We compare on the SET of selected sizes (order-independent), which
//! is deterministic because every vector has at most one bucket meeting
//! min_threshold.

#![cfg(feature = "write-support")]

use std::path::PathBuf;

use cqlite_core::storage::write_engine::{MergePolicy, STCSPolicy};
use serde_json::Value;
use tempfile::TempDir;

const MB: u64 = 1024 * 1024;

fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../test-data/parity/stcs_bucket_selection_vectors.jsonl")
}

/// Create sparse temp files sized `sizes_mb[i]` MB; return the temp dir (kept
/// alive by the caller) and the candidate paths in input order.
fn make_sstables(sizes_mb: &[u64]) -> (TempDir, Vec<PathBuf>) {
    let dir = TempDir::new().expect("temp dir");
    let mut paths = Vec::new();
    for (i, &mb) in sizes_mb.iter().enumerate() {
        let path = dir.path().join(format!("nb-{}-big-Data.db", i + 1));
        let f = std::fs::File::create(&path).expect("create fixture sstable");
        f.set_len(mb * MB).expect("set fixture sstable size");
        paths.push(path);
    }
    (dir, paths)
}

fn to_mb_sorted(paths: &[PathBuf]) -> Vec<u64> {
    let mut sizes: Vec<u64> = paths
        .iter()
        .map(|p| std::fs::metadata(p).expect("stat selected sstable").len() / MB)
        .collect();
    sizes.sort_unstable();
    sizes
}

fn u64_list(v: &Value) -> Vec<u64> {
    v.as_array()
        .expect("array")
        .iter()
        .map(|n| n.as_u64().expect("u64"))
        .collect()
}

#[test]
fn stcs_bucket_selection_matches_documented_cassandra_rules() {
    let text = std::fs::read_to_string(vectors_path()).expect("read STCS vectors fixture");
    // Cassandra STCS defaults — the policy the fixture's expectations assume.
    let policy = STCSPolicy::default();

    let mut checked = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line).expect("parse vector line");
        let case = v["case"].as_str().expect("case name");
        let sizes_mb = u64_list(&v["sizes_mb"]);
        let mut expected = u64_list(&v["expected_selected_mb"]);
        expected.sort_unstable();

        let (_dir, paths) = make_sstables(&sizes_mb);
        let selected = policy.select_merge(&paths).expect("select_merge");
        let got = to_mb_sorted(&selected);

        assert_eq!(
            got,
            expected,
            "STCS selection mismatch for case '{case}': input sizes(MB)={sizes_mb:?}, \
             expected selected(MB)={expected:?}, got={got:?}. Rule: {}",
            v["cassandra_rule"].as_str().unwrap_or("")
        );
        checked += 1;
    }
    assert!(checked >= 6, "expected >= 6 STCS vectors, ran {checked}");
}
