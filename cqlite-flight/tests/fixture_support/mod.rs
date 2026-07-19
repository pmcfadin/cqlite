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
