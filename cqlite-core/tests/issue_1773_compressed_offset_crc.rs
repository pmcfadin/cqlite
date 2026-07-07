//! Issue #1773 — the COMPRESSED offset-read point-lookup path
//! (`SSTableReader::read_value_at_offset` → `get_cached_data`) MUST validate the
//! authoritative inline per-chunk CRC32 before decompressing, surfacing the SAME
//! typed, non-recoverable `Error::InvalidFormat` that the scan / `scan_for_key`
//! paths already return (the #1411 guardrail).
//!
//! ## Why this exists (latent, defensive)
//!
//! For a compressed `nb` table the offset-read path is currently unreachable via
//! `get` (Index.db is keyed by Murmur3 digests, so `find_entry` misses and `get`
//! falls through to `scan_for_key`, whose CRC bypass #1411 already fixed). But the
//! offset path itself — reached directly here — used to read `size` raw bytes at an
//! offset and LZ4-decompress them WITHOUT stripping/validating the trailing 4-byte
//! inline per-chunk CRC32. A future change that makes `find_entry` hit for a
//! compressed table would re-introduce the exact #1411 bypass: a bit-flipped chunk
//! LZ4-decodes to garbage instead of surfacing the typed CRC error.
//!
//! These tests drive corrupt/clean input DIRECTLY through the public offset entry
//! `read_value_at_offset` (bypassing the digest-index miss that hides the path from
//! `get`), proving the CRC check is on the ACTUAL offset path.
//!
//! ## Fixture (real Cassandra 5.0.2 bytes, one deterministic bit flip)
//!
//! Reuses the #1411 oracle:
//! `corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db` — a single-bit
//! flip inside the FIRST LZ4 compressed chunk (chunk 0) of `test_comp/lz4_table`,
//! a single-partition table whose whole payload lives in chunk 0. Apache Cassandra
//! 5.0.2 `sstableverify -e` rejects this exact file.
//!
//! ## Fixture-gating (issue #1094 doctrine)
//!
//! Skip-clean when the corpus binary is absent; `CQLITE_REQUIRE_FIXTURES=1` turns
//! that skip into a hard failure. A fixture that is present but no longer corrupt
//! (regen rot) FAILS unconditionally — the corrupt test asserts `Err`, so an `Ok`
//! on a present fixture fails regardless of the env flag.

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Error, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Relative path of the corrupt COMPRESSED Data.db under the datasets root.
const CORRUPT_DATA_DB: &str = "corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db";

/// Relative path of the CLEAN source Data.db the corrupt fixture was derived from.
const CLEAN_DATA_DB: &str =
    "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db";

/// An offset+size that lands wholly inside chunk 0 (which holds the entire single
/// partition of `lz4_table`). The exact window is immaterial: chunk 0's inline CRC
/// covers the whole chunk, so any read touching it must validate that CRC.
const CHUNK0_OFFSET: u64 = 0;
const CHUNK0_SIZE: u32 = 16;

/// `true` when the full-dataset/nightly lanes demand the corpus be present.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Locate the datasets root, honoring `CQLITE_DATASETS_ROOT` with a worktree fallback.
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

/// Resolve a fixture path, applying the fail-closed gate (issue #1094):
/// present → `Some(path)`; absent + `CQLITE_REQUIRE_FIXTURES=1` → panic; else `None`.
fn fixture_or_gate(rel: &str) -> Option<PathBuf> {
    let path = datasets_root().map(|r| r.join(rel));
    match path {
        Some(p) if p.is_file() => Some(p),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the fixture is absent: {rel}. \
                 Fetch the corpus (test-data/scripts/fetch-datasets.sh)."
            );
            eprintln!("SKIP: fixture absent ({rel}); set CQLITE_REQUIRE_FIXTURES=1 to enforce.");
            None
        }
    }
}

async fn open_reader(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );
    SSTableReader::open(path, &config, platform).await.expect(
        "opening the (structurally valid) corrupt Data.db should succeed; \
         corruption is in a chunk payload, not the header",
    )
}

/// Assert an error is the typed, non-recoverable per-chunk CRC corruption that names
/// the corrupt chunk index + on-disk offset — identical to the scan/`scan_for_key`
/// paths' surfacing (issue #1411).
fn assert_typed_chunk_corruption(err: &Error) {
    assert!(
        !err.is_recoverable(),
        "chunk CRC-mismatch must be a non-recoverable error, got recoverable: {err}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("chunk 0"),
        "corruption error must name the chunk index ('chunk 0'), got: {msg}"
    );
    assert!(
        msg.to_lowercase().contains("offset") && msg.contains("0x0"),
        "corruption error must name the chunk offset (0x0), got: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("CRC"),
        "corruption error should identify the CRC mismatch, got: {msg}"
    );
}

/// Issue #1773 — the compressed OFFSET-READ path itself surfaces the typed per-chunk
/// CRC corruption. Drives `read_value_at_offset` directly (bypassing the digest-index
/// miss that hides this path from `get`), so this asserts the CRC check is on the
/// real offset path — NOT `Ok(None)`, NOT garbage `Ok(Some(_))`.
///
/// FAILS on pre-fix code: the old path read raw bytes and blindly decompressed,
/// yielding a generic decode error / garbage, never a "chunk 0 ... CRC" error.
#[tokio::test]
async fn compressed_offset_read_into_corrupt_chunk_errors_with_typed_crc() {
    let Some(path) = fixture_or_gate(CORRUPT_DATA_DB) else {
        return;
    };
    let reader = open_reader(&path).await;

    let result = reader
        .read_value_at_offset(CHUNK0_OFFSET, CHUNK0_SIZE)
        .await;

    match result {
        Err(err) => assert_typed_chunk_corruption(&err),
        Ok(None) => panic!(
            "compressed offset read into the corrupt chunk returned Ok(None); it must \
             return a typed per-chunk CRC corruption error (issue #1773)."
        ),
        Ok(Some(v)) => panic!(
            "compressed offset read into the corrupt chunk returned Ok(Some({v:?})) — \
             garbage from a bit-flipped chunk; it must return a typed CRC error (issue #1773)."
        ),
    }
}

/// Issue #1773 CLEAN-fixture control — the SAME offset read on the healthy source
/// SSTable returns `Ok(Some(_))`. Proves the corrupt-fixture `Err` above is the CRC
/// corruption surfacing on a decoded-then-CRC-validated chunk, not a read that fails
/// for every input (which would make the corrupt assertion vacuous).
#[tokio::test]
async fn compressed_offset_read_on_clean_chunk_returns_some() {
    let Some(path) = fixture_or_gate(CLEAN_DATA_DB) else {
        return;
    };
    let reader = open_reader(&path).await;

    match reader
        .read_value_at_offset(CHUNK0_OFFSET, CHUNK0_SIZE)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => panic!(
            "compressed offset read on the CLEAN lz4_table chunk returned Ok(None); \
             the chunk must decode (else the corrupt-fixture assertion is vacuous)."
        ),
        Err(e) => panic!(
            "compressed offset read on the CLEAN lz4_table chunk must succeed \
             (CRC valid, decodable), got Err: {e}"
        ),
    }
}
