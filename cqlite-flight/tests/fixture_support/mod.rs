//! Shared fixture-path resolution for the `cqlite-flight` integration suite
//! (issue #2372).
//!
//! Every Flight integration test that reads a real corpus SSTable used to roll
//! its own `CQLITE_DATASETS_ROOT`-relative join and its own skip-on-presence
//! gate on the `-Data.db` binary (the repo ships only JSONL references, so a
//! table DIRECTORY exists even in a checkout that never ran `fetch-datasets.sh`
//! — only the presence of the actual `Data.db` file distinguishes "run the
//! assertions" from "skip"). This module centralises both so BIG (`nb-*-big`)
//! and BTI (`da-*-bti`) fixtures resolve one way.
//!
//! Each integration binary that needs a corpus path compiles this module
//! independently (`mod fixture_support;`), so an unused helper in any one binary
//! is expected — hence the crate-local `allow(dead_code)`.
#![allow(dead_code)]

use std::path::PathBuf;

/// `<CQLITE_DATASETS_ROOT>/sstables`, or `None` when the env var is unset.
///
/// This is the directory a `CqliteFlightService` is pointed at: it holds the
/// `<keyspace>/<table>[-<uuid>]/` SSTable directories, and the service resolves
/// the `<table>-<uuid>` leaf from a ticket's `keyspace`/`table`.
pub fn sstables_root() -> Option<PathBuf> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    Some(PathBuf::from(&root).join("sstables"))
}

/// Resolve `<sstables>/<keyspace>/<table_with_uuid>` and return it ONLY when the
/// `<component_prefix>-Data.db` binary is actually present — the skip-on-presence
/// gate every corpus test shares.
///
/// `component_prefix` is the SSTable generation tag, e.g. `"nb-1-big"` for a BIG
/// fixture or `"da-2-bti"` for a BTI fixture. Returns `None` when
/// `CQLITE_DATASETS_ROOT` is unset OR the `Data.db` binary is absent (an
/// unfetched checkout), so callers take the skip branch rather than a silent
/// 0-row false pass.
pub fn table_dir_if_present(
    keyspace: &str,
    table_with_uuid: &str,
    component_prefix: &str,
) -> Option<PathBuf> {
    let dir = sstables_root()?.join(keyspace).join(table_with_uuid);
    let data_db = dir.join(format!("{component_prefix}-Data.db"));
    if data_db.is_file() {
        Some(dir)
    } else {
        None
    }
}

/// `<CQLITE_DATASETS_ROOT>/sstables` when the env var is set, else the
/// in-repo `<workspace>/test-data/datasets/sstables` (issue #2373).
///
/// Corpora whose binaries are force-committed (`test_da`, `test_comp`) are
/// present in a STOCK checkout that never ran `fetch-datasets.sh`, so their
/// tests must not be silenced merely because `CQLITE_DATASETS_ROOT` is unset.
/// The env var still wins when set, so a caller pointing at a fetched corpus
/// elsewhere keeps working unchanged.
pub fn sstables_root_or_repo_default() -> PathBuf {
    if let Some(root) = sstables_root() {
        return root;
    }
    // `CARGO_MANIFEST_DIR` is `<workspace>/cqlite-flight` for this crate.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .unwrap_or_default()
        .join("test-data")
        .join("datasets")
        .join("sstables")
}

/// Resolve `<sstables>/<keyspace>/<table_prefix>-<generation-uuid>/` by NAME
/// PREFIX, returning it only when `<component_prefix>-Data.db` is a file
/// (issue #2373).
///
/// Unlike [`table_dir_if_present`], the trailing generation UUID is not
/// hardcoded, so the lookup survives a corpus regeneration that mints new
/// UUIDs. Mirrors `cqlite-core/tests/issue_1082_deflate_zlib.rs`'s
/// `fixture_dir`. Returns `None` when the keyspace dir, the table dir, or the
/// `Data.db` binary is absent — callers then skip (or hard-fail under
/// `CQLITE_REQUIRE_FIXTURES=1`) rather than take a silent 0-row false pass.
pub fn table_dir_by_prefix(
    keyspace: &str,
    table_prefix: &str,
    component_prefix: &str,
) -> Option<PathBuf> {
    let ks_dir = sstables_root_or_repo_default().join(keyspace);
    if !ks_dir.is_dir() {
        return None;
    }
    let prefix = format!("{table_prefix}-");
    std::fs::read_dir(&ks_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
                && p.join(format!("{component_prefix}-Data.db")).is_file()
        })
}
