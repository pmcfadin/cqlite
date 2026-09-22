//! Shared helpers for the `issue_4197_rebuild_*` test files (issue #4197).
//! Not a `#[test]` module itself — included via `#[path = "support/rebuild_fixtures.rs"]`.
//!
//! Included into several INDEPENDENT test binaries, each of which uses only
//! a subset of these helpers — `dead_code` is expected per-binary, not a
//! real defect.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// `true` under `CQLITE_REQUIRE_FIXTURES=1` — a present-but-unfetched
/// dataset must fail closed rather than silently skip (CLAUDE.md doctrine).
pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Load the `CREATE TABLE` statement for `table` out of a committed CQL
/// schema file, mirroring `issue_4196_salvage_healthy_parity.rs`'s helper of
/// the same shape.
pub fn table_schema(
    schema_file: &str,
    table: &str,
    keyspace: &str,
) -> cqlite_core::schema::TableSchema {
    let schema_path = super::datasets_root::schema_path(schema_file).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = keyspace.to_string();
    t
}

/// The single `*-Data.db` under `dir` (a generation directory).
pub fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read fixture dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// Copy every file directly under `src` into a fresh subdirectory of `dst`,
/// returning that subdirectory. Used to obtain a disposable working copy of
/// a committed fixture generation so a test can delete/rebuild components
/// without ever touching the checked-out original.
pub fn copy_fixture_dir(src: &Path, dst: &Path) -> PathBuf {
    let working = dst.join("working");
    std::fs::create_dir_all(&working).expect("create working dir");
    for entry in std::fs::read_dir(src).expect("read src dir").flatten() {
        let path = entry.path();
        if path.is_file() {
            std::fs::copy(&path, working.join(entry.file_name())).expect("copy fixture file");
        }
    }
    working
}

/// Read component `suffix` (e.g. `"Digest.crc32"`) from generation directory
/// `dir`, given its `Data.db`-derived descriptor prefix.
pub fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir);
    let prefix = data
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string();
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in an output dir: {e}"))
}

pub fn component_exists(dir: &Path, suffix: &str) -> bool {
    let data = single_data_db(dir);
    let prefix = data
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string();
    dir.join(format!("{prefix}{suffix}")).exists()
}
