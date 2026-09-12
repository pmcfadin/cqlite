//! Shared fixture-resolution and Index.db-structural helpers for issue
//! #4196's salvage corpus test targets
//! (`issue_4196_salvage_corruption_corpus.rs`,
//! `issue_4196_salvage_oom_bounds.rs`) — factored out (round-12, campsite
//! rule / epic #1135) when the corruption-corpus file crossed the ~1500-line
//! test-file threshold and its OOM/bounds/preflight tests were split into
//! their own target. A PURE MOVE: every function here is byte-for-byte what
//! `issue_4196_salvage_corruption_corpus.rs` used to define locally: no
//! behavior changed by this split.
//!
//! Consumers declare this as `#[path = "support/salvage_corpus.rs"] mod
//! salvage_corpus;` alongside their OWN `#[path = "support/datasets_root.rs"]
//! mod datasets_root;` (referenced here via `super::datasets_root`, the same
//! sibling-module pattern `support/header_refusal.rs` already uses for
//! `super::datasets_root`/`super::fixture`).

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use super::datasets_root;

pub const CLEAN_KEYSPACE: &str = "test_comp";
pub const CORRUPT_KEYSPACE: &str = "test_comp_corrupt";
pub const CLEAN_TABLE_DIR: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";
pub const SCHEMA_FILE: &str = "compression-parity.cql";
pub const TABLE: &str = "lz4_table";

pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Every candidate BASE root (the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus) — the PARENT of what
/// `datasets_root::sstables_root_candidates()` returns (that helper already
/// appends `/sstables`, which this test also needs a `corruption/` SIBLING
/// of). Reuses the SAME env-var + checkout-fallback resolution
/// `sstables_root_for_table` uses (issue #3220) rather than trusting
/// `CQLITE_DATASETS_ROOT` alone with no checkout fallback.
pub fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(r) = datasets_root::fixture_roots::datasets_root_if_present() {
        roots.push(r);
    }
    let checkout = datasets_root::fixture_roots::checkout_test_data_dir().join("datasets");
    if !roots.contains(&checkout) {
        roots.push(checkout);
    }
    roots
}

/// The first candidate base root that actually carries BOTH the clean source
/// and the named corrupt fixture (issue #3220: never bind to a root that
/// holds one but not the other and silently skip).
pub fn resolve_root_with_corpus_fixture(corrupt_fixture: &str) -> Option<PathBuf> {
    candidate_base_roots().into_iter().find(|root| {
        usable(
            &root
                .join("sstables")
                .join(CLEAN_KEYSPACE)
                .join(CLEAN_TABLE_DIR),
        ) && usable(
            &root
                .join("corruption")
                .join(CORRUPT_KEYSPACE)
                .join(corrupt_fixture),
        )
    })
}

pub fn table_schema() -> cqlite_core::schema::TableSchema {
    table_schema_for(TABLE)
}

/// Like [`table_schema`], but for any table declared in `SCHEMA_FILE`
/// (`compression-parity.cql`) rather than always [`TABLE`] (`lz4_table`) —
/// added (round-13) for the uncompressed bad-chunk regression coverage,
/// which needs `uncompressed_table`'s schema from the SAME file.
pub fn table_schema_for(table: &str) -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(SCHEMA_FILE).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
        .unwrap_or_else(|| {
            cql.find(&format!("CREATE TABLE {table}"))
                .expect("CREATE TABLE statement")
        });
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = CLEAN_KEYSPACE.to_string();
    t
}

/// The clean `test_comp.uncompressed_table` source directory glob — mirrors
/// `issue_1396_uncompressed_crc_verify.rs::clean_source_data_db`'s directory
/// resolution (same fixture), but returns the DIRECTORY (every sibling
/// component, not just `Data.db`) since salvage needs the whole component
/// set (`Index.db`, `CRC.db`, `Statistics.db`, ...), not a single file.
///
/// Requires `nb-1-big-CRC.db` to be PRESENT, not just `*-Data.db` (roborev,
/// issue #4196, round-13 gate finding): a candidate root can carry a
/// same-named `uncompressed_table-*` directory with `Data.db` but WITHOUT
/// `CRC.db` (observed on a worktree checkout whose local `test-data/datasets`
/// predates this fixture's CRC.db provisioning — worktrees are documented to
/// lack some gitignored binaries, CLAUDE.md's "Test data in worktrees" note)
/// — accepting that directory as "usable" sent the chunk-CRC test straight
/// into a `read CRC.db: NotFound` panic instead of a graceful skip/fall
/// through to the next candidate root.
pub fn clean_uncompressed_table_dir(root: &Path) -> Option<PathBuf> {
    let base = root.join("sstables").join(CLEAN_KEYSPACE);
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?.to_string();
        if name.starts_with("uncompressed_table-") && entry.path().is_dir() {
            let dir = entry.path();
            if dir.join("nb-1-big-CRC.db").is_file() {
                return Some(dir);
            }
        }
    }
    None
}

pub fn skip_or_require(what: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
    true
}

/// `true` iff `dir` is present and carries a `*-Data.db`.
pub fn usable(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten().any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
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

/// Recursively verifies `dir` (and every subdirectory) contains no
/// `*-Data.db` file — `SSTableWriter` nests output at
/// `<out>/<keyspace>/<table>/`, so a top-level-only check passes vacuously
/// on a real `--out` tree (roborev, issue #4196, round-6 Medium finding 3;
/// mirrors `salvage_cli_tests.rs::walk_no_data_db`).
pub fn no_data_db_anywhere(dir: &Path) -> bool {
    if !dir.exists() {
        return true;
    }
    let Ok(rd) = std::fs::read_dir(dir) else {
        return true;
    };
    for e in rd.flatten() {
        let path = e.path();
        if path.is_dir() {
            if !no_data_db_anywhere(&path) {
                return false;
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return false;
        }
    }
    true
}

/// Every `(entry_start, key_field_end, entry_end)` triple for the BIG
/// Index.db entries in `bytes` — parsed structurally via the SAME layout
/// `parse_big_index_entry` uses
/// (`cqlite-core/src/storage/sstable/index_reader/parse.rs`:
/// `[key_len: u16][key][data_offset: vint][promoted_len: vint][promoted]`),
/// not fixed byte offsets, so a fixture regeneration with different key/
/// promoted-index sizes does not silently corrupt the wrong bytes. `key
/// portion` = `bytes[entry_start..key_field_end]` (the 2-byte length prefix
/// plus the raw key); `bytes[key_field_end..entry_end]` is everything else
/// (`data_offset`, `promoted_len`, the promoted-index payload) for that
/// entry.
pub fn split_big_index_entries(bytes: &[u8]) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let entry_start = pos;
        let key_len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        let key_field_end = pos + 2 + key_len;
        let (_offset, offset_consumed) =
            cqlite_core::parser::vint::decode_unsigned(&bytes[key_field_end..])
                .expect("well-formed data_offset VInt");
        let after_offset = key_field_end + offset_consumed;
        let (promoted_len, promoted_consumed) =
            cqlite_core::parser::vint::decode_unsigned(&bytes[after_offset..])
                .expect("well-formed promoted_len VInt");
        let after_promoted_len = after_offset + promoted_consumed;
        let entry_end = after_promoted_len + promoted_len as usize;
        out.push((entry_start, key_field_end, entry_end));
        pos = entry_end;
    }
    out
}
