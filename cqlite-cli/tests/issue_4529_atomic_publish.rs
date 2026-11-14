//! Issue #4529 — `cqlite salvage` / `cqlite rebuild` publish by rename.
//!
//! Cassandra 5.0.8 discovers SSTables by file NAME (`Directories.SSTableLister.getFilter` ->
//! `Descriptor.fromFileWithComponent`, no TOC needed; `TOCComponent.loadOrCreate` rebuilds a
//! missing TOC) and refuses a set without `Data.db` (`SSTableReaderLoadingBuilder.build`:
//! "Data component is missing"). These tests kill the real binary at EVERY point of the publish
//! window (a debug-build seam, `CQLITE_TEST_PUBLISH_ABORT=<n>`: abort before the n-th rename) and
//! assert:
//! * AC1: nothing in `--out` parses as a Cassandra component name unless `Data.db` is among the
//!   final-named files (so Cassandra refuses it) or the whole set is published (== the reference
//!   run's file set);
//! * AC2: the leftover is a recognisable `.cqlite-publish-*.tmp` directory and a re-run refuses,
//!   naming it.
//!
//! Real Cassandra-written fixtures: `test_comp.lz4_table` (BIG) and `test_da.multiclustering_table`
//! (BTI). The in-process kill-at-every-boundary proof is #4498's `issue_4529_*` harness tests.

#![cfg(all(unix, feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

struct Fixture {
    relative: &'static str,
    schema_file: &'static str,
    table: &'static str,
    data_db: &'static str,
    rebuild_components: &'static str,
}
const BIG: Fixture = Fixture {
    relative: "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77",
    schema_file: "compression-parity.cql",
    table: "lz4_table",
    data_db: "nb-1-big-Data.db",
    rebuild_components: "index,summary,filter,digest,toc,statistics",
};
const BTI: Fixture = Fixture {
    relative: "sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf",
    schema_file: "multiclustering-table-bti.cql",
    table: "multiclustering_table",
    data_db: "da-2-bti-Data.db",
    rebuild_components: "filter,digest,toc,statistics",
};

#[derive(Clone, Copy)]
enum Verb {
    Salvage,
    Rebuild,
}

fn fixture_dir(f: &Fixture) -> PathBuf {
    let mut roots = Vec::new();
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(r));
    }
    roots.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-data/datasets"),
    );
    roots
        .into_iter()
        .map(|r| r.join(f.relative))
        .find(|d| d.join(f.data_db).is_file())
        .unwrap_or_else(|| panic!("fixture {} absent under every candidate root", f.relative))
}

fn run(verb: Verb, f: &Fixture, out: &Path, abort_at: Option<usize>) -> Output {
    let schema = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
        .join(f.schema_file);
    let input = fixture_dir(f);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.arg("--schema").arg(&schema);
    match verb {
        Verb::Salvage => {
            cmd.arg("salvage").arg(&input);
        }
        Verb::Rebuild => {
            cmd.arg("rebuild")
                .arg(input.join(f.data_db))
                .args(["--components", f.rebuild_components]);
        }
    }
    cmd.args(["--table", f.table, "--out"]).arg(out);
    cmd.args(["--out-format", "json"]);
    if let Some(n) = abort_at {
        cmd.env("CQLITE_TEST_PUBLISH_ABORT", n.to_string());
    }
    cmd.output().expect("spawn cqlite")
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            // Cassandra's lister lists ONE directory and does not descend into the dot-prefixed
            // staging directory; neither does this scan.
            if !e.file_name().to_string_lossy().starts_with('.') {
                walk(&p, out);
            }
        } else {
            out.push(p);
        }
    }
}

/// Cassandra's component-file grammar (`Descriptor.filenameTokens` + `Version.validate`):
/// exactly four `-` tokens, the first matching `[a-z]+`, the second an id, e.g. `nb-1-big-Data.db`.
fn cassandra_component_name(name: &str) -> bool {
    let t: Vec<&str> = name.split('-').collect();
    t.len() == 4
        && !t[0].is_empty()
        && t[0].chars().all(|c| c.is_ascii_lowercase())
        && !t[1].is_empty()
        && t[1].chars().all(|c| c.is_ascii_alphanumeric())
}

fn final_named(out: &Path) -> Vec<String> {
    let mut all = Vec::new();
    walk(out, &mut all);
    let mut v: Vec<String> = all
        .iter()
        .filter_map(|p| p.file_name()?.to_str().map(str::to_owned))
        .filter(|n| cassandra_component_name(n))
        .collect();
    v.sort();
    v
}

fn staging_dirs(out: &Path) -> Vec<String> {
    std::fs::read_dir(out)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_owned))
        .filter(|n| n.starts_with(".cqlite-publish-") && n.ends_with(".tmp"))
        .collect()
}

fn check(verb: Verb, f: &Fixture) {
    // Reference: an uninterrupted run.
    let tmp = TempDir::new().unwrap();
    let ref_out = tmp.path().join("ref");
    let r = run(verb, f, &ref_out, None);
    assert!(
        r.status.success(),
        "reference run failed: {}",
        String::from_utf8_lossy(&r.stderr)
    );
    let reference = final_named(&ref_out);
    assert!(
        reference.iter().any(|n| n.ends_with("-Data.db"))
            && reference.iter().any(|n| n.ends_with("-TOC.txt")),
        "reference set incomplete: {reference:?}"
    );
    assert!(
        staging_dirs(&ref_out).is_empty(),
        "a successful run leaves no staging dir"
    );
    let total = reference.len();

    // Kill before the n-th rename, n = 0..=total.
    for n in 0..=total {
        let out = tmp.path().join(format!("crash{n}"));
        let c = run(verb, f, &out, Some(n));
        assert!(
            c.status.code().is_none(),
            "n={n}: expected an abort, got {:?}",
            c.status
        );
        let left = final_named(&out);
        let has_data = left.iter().any(|x| x.ends_with("-Data.db"));
        // AC1: a final-named set exists only if Cassandra refuses it (no Data.db) or it is whole.
        assert!(
            left.is_empty() || !has_data || left == reference,
            "n={n}: Cassandra-discoverable partial set after a crash: {left:?} (reference {reference:?})"
        );
        if n < total {
            assert!(!has_data, "n={n}: Data.db must be renamed LAST");
            // AC2: the leftover is recognisable, and a re-run refuses naming it.
            assert_eq!(
                staging_dirs(&out).len(),
                1,
                "n={n}: one recognisable staging dir"
            );
            let again = run(verb, f, &out, None);
            assert_eq!(again.status.code(), Some(1), "n={n}: re-run must refuse");
            let err = String::from_utf8_lossy(&again.stderr);
            assert!(
                err.contains(".cqlite-publish-") && err.contains("interrupted run"),
                "n={n}: refusal must name the leftover: {err}"
            );
        }
    }
}

#[test]
fn rebuild_big_publish_window_never_leaves_a_discoverable_partial_set() {
    check(Verb::Rebuild, &BIG);
}
#[test]
fn rebuild_bti_publish_window_never_leaves_a_discoverable_partial_set() {
    check(Verb::Rebuild, &BTI);
}
#[test]
fn salvage_big_publish_window_never_leaves_a_discoverable_partial_set() {
    check(Verb::Salvage, &BIG);
}
#[test]
fn salvage_bti_publish_window_never_leaves_a_discoverable_partial_set() {
    check(Verb::Salvage, &BTI);
}
