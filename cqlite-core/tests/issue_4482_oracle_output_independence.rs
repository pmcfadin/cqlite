//! Issue #4482 (stage 1): CQLite's readers and comparators must be independent of
//! two oracle-output changes between Cassandra 5.0.2 and 5.0.8 (see
//! `test-data/fixtures/issue_4482/README.md` for provenance and the Cassandra
//! source citations):
//!
//!   1. `TOC.txt` is now written in lexicographic order (CASSANDRA-20494). A TOC is
//!      an inventory, not a sequence.
//!   2. `sstabledump`'s `position` is the scan reader's file pointer when the
//!      partition is serialized, not a stable partition start (first partition 0 in
//!      5.0.2, 18 in 5.0.8). It must never be compared.
//!
//! No fixture is regenerated: the 5.0.8 samples are two small text files, and the
//! binary SSTable is the committed 5.0.2 `issue_3722` generation, whose binaries a
//! 5.0.8 run reproduced byte for byte.

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use canonical_jsonl::{compare_documents, parse_document_str, CompareCtx};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::directory::{parse_toc_file_detailed, SSTableComponent};
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode};
use cqlite_core::Config;

fn repo_file(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(rel)
}

fn sample_dir() -> PathBuf {
    repo_file("test-data/fixtures/issue_4482/cassandra-5.0.8/test_udt_wide_fields")
}

/// The 5.0.2-written generation committed for #3722 (a single `nb-1-big` dir).
fn committed_generation() -> PathBuf {
    let ks = repo_file("test-data/fixtures/issue_3722/test_udt_wide_fields");
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| panic!("read {ks:?}: {e}"))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    assert_eq!(dirs.len(), 1, "expected exactly one table dir under {ks:?}");
    dirs.remove(0)
}

fn toc_lines(p: &Path) -> Vec<String> {
    std::fs::read_to_string(p)
        .unwrap_or_else(|e| panic!("read {p:?}: {e}"))
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn component_set(p: &Path) -> BTreeSet<String> {
    let (comps, unknown) =
        parse_toc_file_detailed(p).unwrap_or_else(|e| panic!("parse {p:?}: {e}"));
    assert!(unknown.is_empty(), "{p:?}: unknown components {unknown:?}");
    comps
        .iter()
        .map(|c| c.file_extension().to_string())
        .collect()
}

#[test]
fn sample_toc_really_is_5_0_8_sorted_and_parses_to_the_same_set_as_the_5_0_2_order() {
    let new_toc = sample_dir().join("nb-1-big-TOC.txt");
    let lines = toc_lines(&new_toc);
    let mut sorted = lines.clone();
    sorted.sort();
    assert_eq!(
        lines, sorted,
        "guard: the 5.0.8 sample TOC must be lexicographically sorted"
    );
    assert!(lines.len() >= 7, "guard: a non-trivial TOC, got {lines:?}");

    let old_toc = std::fs::read_dir(committed_generation())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-TOC.txt"))
        .expect("committed 5.0.2 TOC");
    let old_lines = toc_lines(&old_toc);
    assert_ne!(
        old_lines, lines,
        "guard: the 5.0.2 TOC must be in a different ORDER"
    );
    assert_eq!(component_set(&old_toc), component_set(&new_toc));
    assert!(component_set(&new_toc).contains(SSTableComponent::TOC.file_extension()));
}

#[test]
fn parse_toc_is_order_independent_for_every_permutation_class() {
    let tmp = tempfile::tempdir().unwrap();
    let base = toc_lines(&sample_dir().join("nb-1-big-TOC.txt"));
    let expected = component_set(&sample_dir().join("nb-1-big-TOC.txt"));
    let mut reversed = base.clone();
    reversed.reverse();
    for (name, lines) in [("sorted", base.clone()), ("reversed", reversed)] {
        let p = tmp.path().join(format!("{name}-TOC.txt"));
        std::fs::write(&p, lines.join("\n") + "\n").unwrap();
        assert_eq!(
            component_set(&p),
            expected,
            "{name} order changed the parsed set"
        );
    }
}

async fn verify_dir(dir: &Path) -> cqlite_core::storage::sstable::verify::VerifyReport {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    verify_sstable(dir, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({dir:?}): {e}"))
}

/// `verify` (TOC completeness in BOTH directions) on the committed generation with
/// its TOC replaced by the 5.0.8 sorted one. Because a 5.0.8 run reproduced this
/// generation's binaries byte for byte, this IS the 5.0.8 directory shape.
#[tokio::test]
async fn verify_accepts_the_committed_generation_under_an_alphabetically_sorted_toc() {
    let src = committed_generation();
    let tmp = tempfile::tempdir().unwrap();
    for e in std::fs::read_dir(&src).unwrap().flatten() {
        std::fs::copy(e.path(), tmp.path().join(e.file_name())).unwrap();
    }
    let baseline = verify_dir(tmp.path()).await;
    assert!(
        baseline.is_ok(),
        "baseline (5.0.2 TOC order) must verify: {:?}",
        baseline.findings
    );

    let toc = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-TOC.txt"))
        .unwrap();
    std::fs::copy(sample_dir().join("nb-1-big-TOC.txt"), &toc).unwrap();
    let sorted = verify_dir(tmp.path()).await;
    assert!(
        sorted.is_ok(),
        "sorted TOC must verify: {:?}",
        sorted.findings
    );
    let a: BTreeSet<_> = baseline.toc_components.iter().collect();
    let b: BTreeSet<_> = sorted.toc_components.iter().collect();
    assert_eq!(a, b, "verify must report the same TOC component set");
}

/// Rewrite every `"position":<n>` (partition and row) without a regex dependency.
fn rewrite_positions(s: &str, f: impl Fn(u64) -> u64) -> String {
    const KEY: &str = "\"position\":";
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(KEY) {
        out.push_str(&rest[..i + KEY.len()]);
        rest = &rest[i + KEY.len()..];
        let n_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        let n: u64 = rest[..n_len].parse().expect("numeric position");
        out.push_str(&f(n).to_string());
        rest = &rest[n_len..];
    }
    out.push_str(rest);
    out
}

#[test]
fn canonical_comparator_ignores_sstabledump_position() {
    let p = sample_dir().join("nb-1-big-Data.db.jsonl");
    let new = std::fs::read_to_string(&p).unwrap();
    // Guard: the sample really carries the 5.0.8 shape (non-zero first partition position).
    let first: serde_json::Value = serde_json::from_str(new.lines().next().unwrap()).unwrap();
    assert!(
        first["partition"]["position"].as_u64().unwrap() > 0,
        "guard: 5.0.8-style position"
    );

    let as_5_0_2 = rewrite_positions(&new, |_| 0);
    let shifted = rewrite_positions(&new, |n| n + 7919);
    assert_ne!(new, as_5_0_2);

    let doc_new = parse_document_str(&new, &p, true).expect("5.0.8 sample parses");
    assert!(!doc_new.partitions.is_empty(), "guard: non-empty document");
    for (label, text) in [
        ("positions zeroed (5.0.2 first-partition shape)", as_5_0_2),
        ("positions shifted", shifted),
    ] {
        let other = parse_document_str(&text, &p, true).expect("variant parses");
        let diffs = compare_documents(&CompareCtx::new("issue_4482", &p), &doc_new, &other);
        assert!(
            diffs.is_empty(),
            "{label}: position must not be compared: {diffs:?}"
        );
    }
}
