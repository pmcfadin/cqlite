//! Committed scope roots for the v1 audit (proposal.md "Scope (v1)").
//!
//! Analysis is restricted BY PATH to these roots. A `.rs` file outside them is
//! never parsed or reported, regardless of its content — scope is enforced by
//! path, not by content (spec requirement "A file outside the configured scope
//! roots is not analyzed").
//!
//! v1 scope:
//!   * `cqlite-core/src/storage/sstable/reader/data_access/**`
//!   * `cqlite-core/src/query/**`
//!   * `cqlite-flight/src/producer*.rs`, `cqlite-flight/src/streaming.rs`
//!
//! The wider surface (`export`, bindings, `tools/`, the write path) and rules 2
//! & 3 are deferred to follow-ups.

use std::path::{Path, PathBuf};

/// Directory roots whose `.rs` files are all in scope (recursively).
const SCOPE_DIRS: &[&str] = &[
    "cqlite-core/src/storage/sstable/reader/data_access",
    "cqlite-core/src/query",
];

/// A single in-scope file matcher rooted at a directory: files directly in
/// `dir` whose name starts with `prefix` and ends with `suffix`.
struct FileGlob {
    dir: &'static str,
    prefix: &'static str,
    suffix: &'static str,
}

const SCOPE_FILE_GLOBS: &[FileGlob] = &[
    FileGlob {
        dir: "cqlite-flight/src",
        prefix: "producer",
        suffix: ".rs",
    },
    FileGlob {
        dir: "cqlite-flight/src",
        prefix: "streaming",
        suffix: ".rs",
    },
];

/// The set of directories to walk for `.rs` files (each existing scope dir plus
/// the parent dir of each file-glob). Deduplicated, absolute under `repo_root`.
pub fn walk_dirs(repo_root: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for d in SCOPE_DIRS {
        dirs.push(repo_root.join(d));
    }
    for g in SCOPE_FILE_GLOBS {
        let p = repo_root.join(g.dir);
        if !dirs.contains(&p) {
            dirs.push(p);
        }
    }
    dirs
}

/// True when `rel` (a repo-root-relative path with `/` separators) is an
/// in-scope `.rs` source file. Excludes test files (`*_tests.rs` and `tests/`
/// dirs) so the audit targets production scan/producer paths, not test scaffolds.
pub fn in_scope(rel: &str) -> bool {
    if !rel.ends_with(".rs") {
        return false;
    }
    let file_name = rel.rsplit('/').next().unwrap_or(rel);
    if file_name.ends_with("_tests.rs") || file_name == "tests.rs" {
        return false;
    }
    if rel.contains("/tests/") {
        return false;
    }

    for d in SCOPE_DIRS {
        let with_slash = format!("{d}/");
        if rel.starts_with(&with_slash) {
            return true;
        }
    }
    for g in SCOPE_FILE_GLOBS {
        let parent = format!("{}/", g.dir);
        if let Some(name) = rel.strip_prefix(&parent) {
            if !name.contains('/') && name.starts_with(g.prefix) && name.ends_with(g.suffix) {
                return true;
            }
        }
    }
    false
}

/// Repo-root-relative, `/`-separated path for `abs` under `repo_root`.
pub fn rel_path(repo_root: &Path, abs: &Path) -> Option<String> {
    let rel = abs.strip_prefix(repo_root).ok()?;
    Some(rel.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_access_and_query_dirs_are_in_scope() {
        assert!(in_scope(
            "cqlite-core/src/storage/sstable/reader/data_access/sequential.rs"
        ));
        assert!(in_scope("cqlite-core/src/query/select_executor/mod.rs"));
    }

    #[test]
    fn flight_producer_and_streaming_files_are_in_scope() {
        assert!(in_scope("cqlite-flight/src/producer.rs"));
        assert!(in_scope("cqlite-flight/src/producer_stream.rs"));
        assert!(in_scope("cqlite-flight/src/streaming.rs"));
    }

    #[test]
    fn out_of_scope_paths_are_rejected() {
        // Wider surface deferred to follow-ups.
        assert!(!in_scope("cqlite-core/src/storage/mod.rs"));
        assert!(!in_scope("cqlite-flight/src/service.rs"));
        assert!(!in_scope("tools/format-validator/src/main.rs"));
        assert!(!in_scope("bindings/python/src/lib.rs"));
        // Non-rust file.
        assert!(!in_scope("cqlite-core/src/query/README.md"));
    }

    #[test]
    fn test_files_are_excluded() {
        assert!(!in_scope(
            "cqlite-core/src/query/engine_lock_hygiene_tests.rs"
        ));
        assert!(!in_scope(
            "cqlite-core/src/storage/sstable/reader/data_access/full_index_stream_tests.rs"
        ));
    }
}
