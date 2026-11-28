//! Fixture helpers for the generation-discovery tests (#4626). Every resolver here PANICS when its
//! fixture is absent: a case that cannot find its data fails by name, it never skips (#3220).
//! Mutations are applied to COPIES in a tempdir; the shared corpus is never touched.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use super::datasets_root::{repo_root, sstables_root_for_table, table_generation_dirs};

/// The first usable generation directory of a committed corpus table, resolved by evidence.
pub fn corpus_table(keyspace: &str, table: &str) -> PathBuf {
    let root = sstables_root_for_table(keyspace, table)
        .unwrap_or_else(|| panic!("corpus table {keyspace}.{table} has no Data.db in any root"));
    table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("corpus table {keyspace}.{table} lost its directory"))
}

/// A Cassandra-written #4626 fixture table directory (`test-data/fixtures/issue_4626`).
pub fn fixture_table(state: &str, table: &str) -> PathBuf {
    let base = repo_root()
        .join("test-data/fixtures/issue_4626")
        .join(state)
        .join("gd4626");
    let prefix = format!("{table}-");
    let found = std::fs::read_dir(&base)
        .unwrap_or_else(|e| panic!("fixture state {state} missing at {}: {e}", base.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix))
        })
        .unwrap_or_else(|| {
            panic!(
                "fixture table {state}/{table} missing under {}",
                base.display()
            )
        });
    let has_data = std::fs::read_dir(&found)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
    assert!(
        has_data,
        "fixture {} has no Data.db (binaries not checked out?)",
        found.display()
    );
    found
}

/// Copy every regular file of `src` into `dst` (created), recursing into subdirectories.
pub fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().filter_map(|e| e.ok()) {
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

/// Copy the files of generation `from` (a `<version>-<id>-<format>` prefix) of `src` into `dst`
/// under the prefix `to`, keeping the component suffixes. Returns how many files were copied.
pub fn copy_generation_as(src: &Path, from: &str, dst: &Path, to: &str) -> usize {
    std::fs::create_dir_all(dst).unwrap();
    let from_dash = format!("{from}-");
    let mut copied = 0;
    for entry in std::fs::read_dir(src).unwrap().filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(&from_dash) {
            std::fs::copy(entry.path(), dst.join(format!("{to}-{rest}"))).unwrap();
            copied += 1;
        }
    }
    assert!(
        copied > 0,
        "no files with prefix {from_dash} in {}",
        src.display()
    );
    copied
}

/// `(relative path, full content)` of every file below `dir`, sorted: the byte-identity witness
/// for "nothing was overwritten" (a size comparison cannot see an overwrite by a same-size file).
pub fn content_signature(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for e in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                let rel = p.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.push((rel, std::fs::read(&p).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// `(relative path, size)` of every file below `dir`, sorted: a cheap LISTING witness only. Use
/// [`content_signature`] for any "unchanged" claim.
pub fn tree_signature(dir: &Path) -> Vec<(String, u64)> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, u64)>) {
        for e in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                let rel = p.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.push((rel, std::fs::metadata(&p).unwrap().len()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// A placeholder component for listing-only cases: non-empty so it is not an Orphan. Never parsed.
pub fn touch(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, b"x").unwrap();
}
