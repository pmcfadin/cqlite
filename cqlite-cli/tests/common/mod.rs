//! Common test utilities for CQLite CLI integration tests
//!
//! This module provides shared testing infrastructure used across multiple test suites.
//!
//! `#![allow(dead_code)]` because each cargo integration-test binary pulls in the
//! whole module via `mod common;` but exercises only the helpers it needs.

#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

pub mod golden_snapshots;

/// Crate root (`cqlite-cli/`).
pub fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Resolve the datasets root honoring `CQLITE_DATASETS_ROOT` (the CI/agent
/// convention), falling back to the in-repo `test-data/datasets`.
pub fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| crate_root().parent().unwrap().join("test-data/datasets"))
}

/// Directory holding the `.cql` schemas (`test-data/schemas`).
pub fn schemas_dir() -> PathBuf {
    crate_root().parent().unwrap().join("test-data/schemas")
}

/// Locate a `*-Data.db` for `test_basic/simple_table`, handling the UUID suffix
/// and the optional `sstables/` layer. Prefers the canonical `nb` generation,
/// then falls back to any `*-Data.db`. Returns `None` (→ graceful skip) when the
/// binary fixtures are not present.
pub fn find_simple_table_data_db() -> Option<PathBuf> {
    let root = datasets_root();
    let keyspace_dir = {
        let with_sstables = root.join("sstables").join("test_basic");
        if with_sstables.exists() {
            with_sstables
        } else {
            root.join("test_basic")
        }
    };

    let entries = fs::read_dir(&keyspace_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("simple_table-") {
            continue;
        }
        // Prefer the canonical nb generation; fall back to any *-Data.db.
        let candidate = entry.path().join("nb-1-big-Data.db");
        if candidate.exists() {
            return Some(candidate);
        }
        if let Ok(files) = fs::read_dir(entry.path()) {
            for f in files.flatten() {
                let fname = f.file_name();
                if fname.to_string_lossy().ends_with("-Data.db") {
                    return Some(f.path());
                }
            }
        }
    }
    None
}
