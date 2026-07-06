//! Regression test for a roborev finding on issue #1581 (Epic D5, CLI query
//! bounded-streaming cutover): `execute_streaming`'s background scan task must
//! surface a mid-scan error through the channel as a terminal `Err` item, NOT
//! silently close the channel after only logging it.
//!
//! ## The bug
//!
//! `SelectExecutor::execute_streaming` spawns
//! `execute_streaming_background(..., tx, ...)` in a `tokio::spawn`. Before this
//! fix, the spawn closure did:
//! ```text
//! if let Err(e) = Self::execute_streaming_background(..., tx, ...).await {
//!     log::error!("Streaming execution error: {}", e);
//!     // channel just closes — consumer sees a clean `None` (EOF)
//! }
//! ```
//! A read/decode failure partway through a scan (e.g. a CRC-mismatched
//! compressed chunk, issue #1397) made `execute_streaming_background` return
//! `Err`, which was ONLY logged — the consumer's `next_async()` then returned
//! `None` (clean end-of-stream) instead of `Some(Err(_))`. The CLI's
//! `collect_query_result` (issue #1581) — and any other `execute_streaming`
//! consumer — therefore printed a TRUNCATED-BUT-SUCCESSFUL result and exited 0,
//! where the materializing `Database::execute()` path would have failed the
//! whole query. This is the silent-partial-result regression the streaming
//! cutover must NOT introduce.
//!
//! ## The fixture
//!
//! Reuses the SAME real, single-bit-flipped Cassandra 5.0.2 `Data.db` that
//! `issue_1397_corrupt_query_surface.rs` already proves makes the raw
//! `SSTableReader::scan_stream` primitive terminate with a typed corruption
//! `Err` mid-iteration (`corruption/test_comp_corrupt/data_db_bit_flip`,
//! `test_comp/lz4_table`, chunk 0). THIS test proves the error reaches the
//! `SelectExecutor::execute_streaming` -> `QueryEngine::execute_streaming`
//! consumer as `Some(Err(_))` — one layer up, through the SELECT-level
//! channel — rather than being swallowed into a silent `None`.
//!
//! The fixture is loaded directly via `StorageEngine::open_with_sstables` (no
//! keyspace/table schema needed — the corruption surfaces during chunk
//! decompression, before any schema-aware decode), keyed by the fixture's own
//! directory naming (`test_comp_corrupt.data_db_bit_flip`) rather than the
//! `test_comp.lz4_table` name the bytes were derived from; that identity is
//! irrelevant to this test, which only cares that the channel carries the error.
//!
//! Skip-clean when the corpus binary is absent (fetch via
//! `test-data/scripts/fetch-datasets.sh` /
//! `test-data/scripts/generate-corruption-corpus.sh`); `CQLITE_REQUIRE_FIXTURES=1`
//! turns that skip into a hard failure (issue #1094 doctrine).

#![cfg(feature = "state_machine")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::memory::MemoryManager;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::query::QueryEngine;
use cqlite_core::schema::SchemaManager;
use cqlite_core::storage::StorageEngine;
use cqlite_core::{Config, Platform};
use tempfile::TempDir;

/// Relative path of the corrupt COMPRESSED Data.db under the datasets root
/// (same fixture as issue_1397_corrupt_query_surface.rs).
const CORRUPT_DATA_DB_DIR: &str = "corruption/test_comp_corrupt/data_db_bit_flip";

/// `true` when the full-dataset/nightly lanes demand the corpus be present.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// Resolve the corrupt fixture's table DIRECTORY (not the Data.db file itself —
/// `StorageEngine::open_with_sstables` takes table directories), applying the
/// fail-closed gate: present -> `Some(dir)`; absent + `CQLITE_REQUIRE_FIXTURES=1`
/// -> panic; absent otherwise -> `None` (skip-clean).
fn corrupt_table_dir_or_gate() -> Option<PathBuf> {
    let dir = datasets_root().map(|r| r.join(CORRUPT_DATA_DB_DIR));
    match dir {
        Some(d) if d.join("nb-1-big-Data.db").is_file() => Some(d),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the corruption fixture is absent: \
                 {CORRUPT_DATA_DB_DIR}. Fetch the corpus \
                 (test-data/scripts/fetch-datasets.sh) / regenerate it \
                 (test-data/scripts/generate-corruption-corpus.sh)."
            );
            eprintln!(
                "SKIP: corruption fixture absent ({CORRUPT_DATA_DB_DIR}); \
                 set CQLITE_REQUIRE_FIXTURES=1 to enforce."
            );
            None
        }
    }
}

/// AC — a `SELECT * FROM <corrupt table>` run through
/// `QueryEngine::execute_streaming` (the same entry point
/// `Database::execute_streaming` / the CLI's `collect_query_result` use) must
/// terminate with a `Some(Err(_))` item, never a silent `None` end-of-stream,
/// and must not have yielded any `Ok` rows first (the corrupt chunk holds the
/// whole partition, matching the #1397 storage-level assertion one layer down).
#[tokio::test]
async fn streaming_query_surfaces_mid_scan_error_not_silent_truncation() {
    let Some(table_dir) = corrupt_table_dir_or_gate() else {
        return;
    };

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );
    // `StorageEngine::open_with_sstables` writes a manifest under this path; the
    // corrupt fixture's own directory naming (`test_comp_corrupt/data_db_bit_flip`,
    // no `-<uuid>` suffix) resolves to table key `test_comp_corrupt.data_db_bit_flip`
    // (see `extract_keyspace_and_table_name`) — no schema registration needed, since
    // the corruption surfaces during chunk decompression, before schema-aware decode.
    let manifest_dir = TempDir::new().expect("tempdir for storage manifest");
    let storage = Arc::new(
        StorageEngine::open_with_sstables(
            manifest_dir.path(),
            vec![table_dir],
            &config,
            platform,
            None,
        )
        .await
        .expect(
            "opening the (structurally valid) corrupt Data.db should succeed; \
             corruption is in a chunk payload, not the header (matches #1397)",
        ),
    );
    let schema = Arc::new(
        SchemaManager::new(manifest_dir.path())
            .await
            .expect("empty schema manager should construct"),
    );
    let memory = Arc::new(MemoryManager::new(&config).expect("memory manager should construct"));
    let engine =
        QueryEngine::new(storage, schema, memory, &config).expect("query engine should construct");

    let mut iter = engine
        .execute_streaming(
            "SELECT * FROM test_comp_corrupt.data_db_bit_flip",
            StreamingConfig::default(),
        )
        .await
        .expect("execute_streaming should accept the query synchronously (the error surfaces from the background scan, not here)");

    let mut ok_rows = 0usize;
    let mut terminal_err: Option<cqlite_core::Error> = None;
    while let Some(item) = iter.next_async().await {
        match item {
            Ok(_) => ok_rows += 1,
            Err(e) => {
                terminal_err = Some(e);
                break;
            }
        }
    }

    assert_eq!(
        ok_rows, 0,
        "the corrupt chunk holds the whole partition — no Ok rows may precede the \
         error (silent truncation would show ok_rows>0 then a clean end-of-stream)"
    );
    let err = terminal_err.expect(
        "REGRESSION (roborev finding on #1581): execute_streaming's background scan \
         hit a mid-scan error but the consumer saw a silent `None` end-of-stream \
         instead of `Some(Err(_))` — the spawn closure is swallowing the error \
         again instead of relaying it through the channel.",
    );
    // Same typed, non-recoverable corruption class the storage-level #1397 test
    // asserts — proves this is genuinely the CRC mismatch surfacing, not some
    // unrelated failure (e.g. a table-not-found error at a different layer).
    assert!(
        !err.is_recoverable(),
        "the chunk CRC-mismatch must remain a non-recoverable error at the SELECT layer, got: {err}"
    );
    assert!(
        err.to_string().to_uppercase().contains("CRC"),
        "the SELECT-layer error should still identify the CRC mismatch, got: {err}"
    );
}
