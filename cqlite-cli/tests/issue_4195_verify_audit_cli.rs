//! Issue #4195 — `cqlite verify --mode audit` end to end through the REAL
//! binary: checklist rendering (text + JSON), the exit-code contract (A10), the
//! `--deep` usage error, and that `--mode full` carries `"audit":null` (A12.2).
//!
//! Fixtures come from datasets-v4 (Cassandra 5.0.8). Every case fails closed when
//! its fixture is absent: there is no skip path and no suite-wide counter.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn sstables_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(r).join("sstables"));
    }
    roots.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repo root")
            .join("test-data/datasets/sstables"),
    );
    roots
}

/// First `<keyspace>/<table>-*` directory carrying a Data.db.
fn table_dir(ks: &str, table: &str) -> PathBuf {
    for root in sstables_roots() {
        let Ok(rd) = std::fs::read_dir(root.join(ks)) else {
            continue;
        };
        let mut dirs: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
        dirs.sort();
        for d in dirs {
            let name = d.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let has_data = std::fs::read_dir(&d)
                .map(|rd| {
                    rd.flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
                })
                .unwrap_or(false);
            if name.starts_with(&format!("{table}-")) && has_data {
                return d;
            }
        }
    }
    panic!(
        "fixture {ks}.{table} not found under {:?} (fetch datasets-v4)",
        sstables_roots()
    );
}

fn run(args: &[&str], dir: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("verify")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn cqlite verify")
}

fn stdout(o: &Output) -> String {
    String::from_utf8(o.stdout.clone()).expect("utf8")
}

const IDS: [&str; 8] = [
    "index-data-identity",
    "summary-index-correlation",
    "bloom-no-false-negative",
    "bti-trie-bounds",
    "toc-exact-listing",
    "digest-matches-data",
    "compression-coverage",
    "statistics-vs-scan",
];

#[test]
fn text_checklist_has_eight_rows_and_exit_zero_for_a_healthy_uncompressed_big_table() {
    // A10.2: Pass + Skip with zero Fail is a real pass.
    let o = run(
        &["--mode", "audit"],
        &table_dir("test_comp", "uncompressed_table"),
    );
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    let out = stdout(&o);
    for id in IDS {
        assert!(out.contains(id), "row {id} missing from:\n{out}");
    }
    assert!(
        out.contains("[SKIP] compression-coverage: no CompressionInfo: uncompressed"),
        "{out}"
    );
    assert!(
        out.contains("[SKIP] statistics-vs-scan: requires --deep"),
        "{out}"
    );
    assert!(
        out.contains("[SKIP] bti-trie-bounds: not applicable: BIG format"),
        "{out}"
    );
    assert!(out.contains("[PASS] index-data-identity"), "{out}");
    assert!(out.contains("invariants MEASURED of 8"), "{out}");
}

#[test]
fn json_checklist_rows_and_deep_flag() {
    let o = run(
        &["--mode", "audit", "--deep", "--out", "json"],
        &table_dir("test_basic", "simple_table"),
    );
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    let out = stdout(&o);
    assert!(
        out.contains("\"audit\":{\"deep\":true,\"measured\":"),
        "{out}"
    );
    for id in IDS {
        assert!(
            out.contains(&format!("\"invariant\":\"{id}\"")),
            "{id} missing: {out}"
        );
    }
    assert!(
        out.contains("\"invariant\":\"statistics-vs-scan\",\"verdict\":\"pass\""),
        "{out}"
    );
}

#[test]
fn a_failing_row_exits_two_and_names_the_finding() {
    let dir = {
        // The committed filter_db_bit_flip fixture (existing class FilterFalseNegative).
        let mut found = None;
        for r in sstables_roots() {
            let d = r
                .parent()
                .expect("root")
                .join("corruption/test_comp_corrupt/filter_db_bit_flip");
            if d.join("nb-1-big-Data.db").exists() {
                found = Some(d);
                break;
            }
        }
        found.expect("filter_db_bit_flip fixture (fetch datasets-v4)")
    };
    let o = run(&["--mode", "audit"], &dir);
    assert_eq!(o.status.code(), Some(2), "{}", stdout(&o));
    let out = stdout(&o);
    assert!(out.contains("[FAIL] bloom-no-false-negative"), "{out}");
    assert!(out.contains("[FilterFalseNegative]"), "{out}");
}

#[test]
fn a_vacuous_audit_exits_two_and_says_zero_measured() {
    // A10.1: a bare Data.db whose TOC names nothing else measures nothing.
    let tmp = tempfile::tempdir().expect("tmp");
    let src = table_dir("test_comp", "uncompressed_table");
    let data = std::fs::read_dir(&src)
        .expect("dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("Data.db");
    std::fs::copy(&data, tmp.path().join("nb-1-big-Data.db")).expect("copy");
    std::fs::write(tmp.path().join("nb-1-big-TOC.txt"), "Data.db\nTOC.txt\n").expect("toc");
    for out_fmt in ["text", "json"] {
        let o = run(&["--mode", "audit", "--out", out_fmt], tmp.path());
        assert_eq!(o.status.code(), Some(2), "{out_fmt}: {}", stdout(&o));
        let out = stdout(&o);
        if out_fmt == "text" {
            assert!(out.contains("0 invariants MEASURED"), "{out}");
        } else {
            assert!(out.contains("\"measured\":0"), "{out}");
        }
    }
}

#[test]
fn deep_without_audit_is_a_usage_error_before_any_verification() {
    for mode in ["quick", "full"] {
        // The path does not even exist: the usage error must come first.
        let o = run(
            &["--mode", mode, "--deep"],
            Path::new("/nonexistent/never/opened"),
        );
        assert_eq!(o.status.code(), Some(1), "{mode}");
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(
            err.contains("--deep requires --mode audit"),
            "{mode}: {err}"
        );
    }
}

#[test]
fn full_mode_renders_audit_null_and_nothing_else_changes() {
    let o = run(
        &["--mode", "full", "--out", "json"],
        &table_dir("test_comp", "lz4_table"),
    );
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    let out = stdout(&o);
    assert!(out.ends_with("\"audit\":null}\n"), "{out}");
    assert!(
        out.contains("\"mode\":\"full\"") && out.contains("\"ok\":true"),
        "{out}"
    );
}
