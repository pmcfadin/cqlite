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

use std::path::{Path, PathBuf};

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

/// The in-repo `<workspace>/test-data/datasets/sstables` (issue #2373).
///
/// Corpora whose binaries are force-committed (`test_da`, `test_comp`) live
/// here in a STOCK checkout that never ran `fetch-datasets.sh`.
pub fn repo_sstables_root() -> PathBuf {
    // `CARGO_MANIFEST_DIR` is `<workspace>/cqlite-flight` for this crate.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .unwrap_or_default()
        .join("test-data")
        .join("datasets")
        .join("sstables")
}

/// Every `sstables/` root a force-committed fixture may live under, in lookup
/// order: `<CQLITE_DATASETS_ROOT>/sstables` first (when set), then the in-repo
/// default (issue #2373).
///
/// The fallback is deliberately PER-LOOKUP rather than "env wins globally": the
/// 1:1:1:1 worktree convention points `CQLITE_DATASETS_ROOT` at ANOTHER
/// checkout's `test-data/datasets`, which does not carry a corpus that was
/// force-committed on THIS branch. Choosing the env root globally would resolve
/// no fixture there and silently turn the whole suite into skips — the vacuous
/// pass these helpers exist to prevent.
pub fn candidate_sstables_roots() -> Vec<PathBuf> {
    let repo = repo_sstables_root();
    match sstables_root() {
        Some(env_root) if env_root != repo => vec![env_root, repo],
        Some(env_root) => vec![env_root],
        None => vec![repo],
    }
}

/// A fixture resolved under a specific `sstables/` root.
///
/// Carries the root it was ACTUALLY FOUND UNDER so a caller that must point a
/// service at a corpus root (e.g. `CqliteFlightService::new`) uses the same root
/// the assertions' golden came from, never a globally-chosen one.
pub struct ResolvedFixture {
    /// The `sstables/` root `dir` was found under.
    pub sstables_root: PathBuf,
    /// `<sstables_root>/<keyspace>/<table_prefix>-<generation-uuid>/`.
    pub dir: PathBuf,
}

/// Resolve `<sstables>/<keyspace>/<table_prefix>-<generation-uuid>/` by NAME
/// PREFIX across every candidate root, returning it only when
/// `<component_prefix>-Data.db` is a file (issue #2373).
///
/// Unlike [`table_dir_if_present`], the trailing generation UUID is not
/// hardcoded, so the lookup survives a corpus regeneration that mints new
/// UUIDs. Mirrors `cqlite-core/tests/issue_1082_deflate_zlib.rs`'s
/// `fixture_dir`. Returns `None` only when NO candidate root resolves the
/// keyspace dir, the table dir and the `Data.db` binary — callers then skip (or
/// hard-fail under `CQLITE_REQUIRE_FIXTURES=1`) rather than take a silent 0-row
/// false pass.
pub fn table_dir_by_prefix(
    keyspace: &str,
    table_prefix: &str,
    component_prefix: &str,
) -> Option<ResolvedFixture> {
    candidate_sstables_roots().into_iter().find_map(|root| {
        let dir = table_dir_under(&root, keyspace, table_prefix, component_prefix)?;
        Some(ResolvedFixture {
            sstables_root: root,
            dir,
        })
    })
}

/// Prefix lookup within ONE `sstables/` root.
fn table_dir_under(
    root: &Path,
    keyspace: &str,
    table_prefix: &str,
    component_prefix: &str,
) -> Option<PathBuf> {
    let ks_dir = root.join(keyspace);
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
