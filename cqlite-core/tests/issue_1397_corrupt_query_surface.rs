//! Issue #1397 (Epic #1380): the PLAIN user-facing query surface must surface a
//! typed, non-recoverable corruption error when it reads a bit-flipped COMPRESSED
//! chunk — it must NOT silently return garbage or truncate.
//!
//! ## Why this exists
//!
//! The production per-chunk CRC32 check (`reader/block_io.rs:412-440`) is real and
//! unconditional, but before this suite its failure branch was only exercised
//! *indirectly* (issue #998 drives `ChunkDecompressor`; verify Check 5/7 drive the
//! verifier). Nothing asserted that a **user query** — `SSTableReader::scan`
//! (full scan), `SSTableReader::get` (point lookup), or `scan_stream` (streaming) —
//! returns the typed corruption error. A refactor that reroutes the query path
//! around `read_next_block_impl` (exactly what the offset-based point-lookup path
//! already does — see #1411) would pass every other test while silently returning
//! garbage. The scan/stream tests break on such a reroute. The point-lookup
//! expectation is captured as an `#[ignore]`d expected-behavior regression test
//! (asserting the typed corruption error) that is un-ignored when #1411 lands — we
//! do NOT codify today's `Ok(None)` defect.
//!
//! ## Fixture (real Cassandra 5.0.2 bytes, one deterministic bit flip)
//!
//! `corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db` — a single-bit
//! flip (byte offset 64, `0x61`→`0x60`) inside the FIRST LZ4 compressed chunk of
//! `test_comp/lz4_table`. That table is a single partition (`pk=1`) whose entire
//! payload lives in chunk 0, so the corruption is unavoidable on any read of the
//! data section. Apache Cassandra 5.0.2 `sstableverify -e` rejects this exact file
//! ("Data.db digest integrity check failed -> Invalid SSTable"), per the corpus
//! manifest (`corruption-manifest.yml`, fixture `data_db_bit_flip`).
//!
//! ## Error-variant note
//!
//! The scan/stream CRC-mismatch surfaces as `Error::InvalidFormat` (non-recoverable,
//! message naming the chunk index + offset), NOT `Error::Corruption`. Both variants
//! are non-recoverable (`error.rs`: `InvalidFormat => false`, `Corruption => false`),
//! and #1397's substance is "typed non-recoverable error naming chunk + offset", so
//! these tests assert those robust invariants rather than pinning the exact variant.
//! Unifying the CRC-mismatch to `Error::Corruption` is tracked as a secondary note on
//! #1411.
//!
//! ## Fixture-gating (issue #1094 doctrine, AC #5)
//!
//! Skip-clean when the corpus binary is absent; `CQLITE_REQUIRE_FIXTURES=1` turns
//! that skip into a hard failure. A fixture that is *present but no longer corrupt*
//! (regeneration rot) FAILS unconditionally — the scan/stream tests assert `Err`, so
//! an `Ok` result on a present fixture fails the suite regardless of the env flag.

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::{RowKey, TableId};
use cqlite_core::{Config, Error, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Relative path of the corrupt COMPRESSED Data.db under the datasets root.
const CORRUPT_DATA_DB: &str = "corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db";

/// Relative path of the CLEAN source Data.db the corrupt fixture was derived from
/// (`test_comp/lz4_table`). Used to prove the #1411 fix keeps a healthy point
/// lookup returning `Ok(Some(_))` — so the corrupt-fixture `Err` is genuinely the
/// corruption surfacing, not a lookup that fails for every input.
const CLEAN_DATA_DB: &str =
    "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db";

/// Fully-qualified table the corrupt fixture was derived from.
const TABLE: &str = "test_comp.lz4_table";

/// Partition key `pk = 1` serialized as a CQL `int` (4-byte big-endian). This is
/// the ONLY partition in `lz4_table` and its row lives in the corrupt chunk 0.
/// Verified against the CLEAN fixture: `get([0,0,0,1])` returns `Ok(Some(_))` there,
/// so the bloom/key encoding is correct — the corrupt-fixture divergence is a
/// read-path defect, not a wrong key.
const PK1_KEY_BYTES: [u8; 4] = [0, 0, 0, 1];

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

/// Resolve the corrupt Data.db path, applying the fail-closed gate (AC #5):
/// - present  → `Some(path)`
/// - absent + `CQLITE_REQUIRE_FIXTURES=1` → panic (hard failure)
/// - absent otherwise → `None` (skip-clean)
fn corrupt_data_db_or_gate() -> Option<PathBuf> {
    let path = datasets_root().map(|r| r.join(CORRUPT_DATA_DB));
    match path {
        Some(p) if p.is_file() => Some(p),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the corruption fixture is absent: {CORRUPT_DATA_DB}. \
                 Fetch the corpus (test-data/scripts/fetch-datasets.sh) / regenerate it \
                 (test-data/scripts/generate-corruption-corpus.sh)."
            );
            eprintln!("SKIP: corruption fixture absent ({CORRUPT_DATA_DB}); set CQLITE_REQUIRE_FIXTURES=1 to enforce.");
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

/// Assert an error is the typed, non-recoverable corruption class that names the
/// corrupt chunk index + on-disk offset. Shared by the full-scan and streaming
/// tests so both prove the SAME invariant reached the user.
fn assert_typed_chunk_corruption(err: &Error) {
    // Non-recoverable class (error.rs) — a retry cannot help a bad chunk.
    assert!(
        !err.is_recoverable(),
        "chunk CRC-mismatch must be a non-recoverable error, got recoverable: {err}"
    );
    let msg = err.to_string();
    // Message must name the failing chunk index (chunk 0) AND its byte offset so an
    // operator can locate the damage. Also confirms the error came from the inline
    // per-chunk CRC path (`block_io.rs`), not a generic parse failure.
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

/// AC #1 — full scan over the corrupt COMPRESSED fixture returns the typed,
/// non-recoverable error naming chunk index + offset, and yields NO rows first.
///
/// Also serves AC #5's "present-but-clean fails" guard: this test only skips when
/// the fixture is ABSENT; if it is present but no longer corrupt (regen rot),
/// `scan` returns `Ok` and the assertion below fails.
#[tokio::test]
async fn full_scan_over_corrupt_chunk_errors_before_yielding_rows() {
    let Some(path) = corrupt_data_db_or_gate() else {
        return;
    };
    let reader = open_reader(&path).await;
    let table_id = TableId::new(TABLE.to_string());

    let result = reader.scan(&table_id, None, None, None, None).await;

    match result {
        Ok(rows) => panic!(
            "FIXTURE ROT or read-path regression: full scan over the bit-flipped \
             chunk returned Ok with {} rows; it must return a corruption error. \
             (A refactor rerouting scan around read_next_block_impl would land here.)",
            rows.len()
        ),
        Err(err) => {
            assert_typed_chunk_corruption(&err);
            // `scan` returns a materialized Vec, so the Err path inherently yielded
            // zero rows for the corrupt chunk — there is no partial buffer to leak.
        }
    }
}

/// AC #3 — streaming/windowed scan (the stitch path, data_access/mod.rs:248-308)
/// surfaces the same typed error MID-ITERATION: the iterator terminates with an
/// `Err` item, not a silent truncation, and emits zero `Ok` rows beforehand.
#[tokio::test]
async fn streaming_scan_terminates_with_error_not_silent_truncation() {
    let Some(path) = corrupt_data_db_or_gate() else {
        return;
    };
    let reader = Arc::new(open_reader(&path).await);
    let table_id = TableId::new(TABLE.to_string());

    let mut rx = reader.scan_stream(table_id, None, None, None, 4);

    let mut ok_rows = 0usize;
    let mut terminal_err: Option<Error> = None;
    while let Some(item) = rx.recv().await {
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
        "the corrupt chunk holds the whole partition — no rows may be yielded before \
         the error (silent truncation would show ok_rows>0 then a clean end-of-stream)"
    );
    let err = terminal_err.expect(
        "streaming scan over the corrupt chunk must end with a terminal Err item, \
         NOT a silent end-of-stream (which would be undetectable truncation)",
    );
    assert_typed_chunk_corruption(&err);
}

/// AC #2 — point lookup whose target row lives in the corrupt chunk.
///
/// EXPECTED behavior (issue #1397): a typed, non-recoverable corruption error naming
/// the corrupt chunk index + offset — NOT `Ok(None)` and NOT garbage.
///
/// Fixed by issue #1411. The point lookup for `lz4_table` (a compressed `nb` table)
/// falls through the digest-index miss (issue #517) into `scan_for_key`, whose
/// chunk-stitching branch used to swallow EVERY `Err` from
/// `stitch_and_parse_all_chunks` as `Ok(None)` — including the authoritative
/// per-chunk CRC32 mismatch raised by `block_io::read_nb_format_chunk_data`. #1411
/// split the integrity stitch (`stitch_all_chunks`, whose errors now propagate) from
/// the schema-aware parse (which alone may soft-miss), so the corruption surfaces as
/// the SAME typed error the full-scan/stream paths return. The identical
/// `get([0,0,0,1])` returns `Ok(Some(_))` on the CLEAN fixture, so the divergence was
/// a read-path defect, not a wrong key.
#[tokio::test]
async fn point_lookup_into_corrupt_chunk_should_error_see_1411() {
    let Some(path) = corrupt_data_db_or_gate() else {
        return;
    };
    let reader = open_reader(&path).await;
    let table_id = TableId::new(TABLE.to_string());
    let key = RowKey::new(PK1_KEY_BYTES.to_vec());

    let result = reader.get(&table_id, &key).await;

    match result {
        Err(err) => assert_typed_chunk_corruption(&err),
        Ok(None) => panic!(
            "point lookup for pk=1 over the corrupt chunk returned Ok(None); it must \
             return a typed corruption error (see #1411)."
        ),
        Ok(Some(v)) => panic!(
            "point lookup over the corrupt chunk returned Ok(Some({v:?})) — garbage \
             from a bit-flipped chunk; it must return a typed corruption error."
        ),
    }
}

/// Issue #1411 CLEAN-fixture control — the SAME point lookup (`get([0,0,0,1])`) on
/// the healthy source SSTable returns `Ok(Some(_))`. This proves the corrupt-fixture
/// `Err` above is the CRC corruption surfacing, not a lookup that misses for every
/// input (which would make the corrupt-fixture assertion vacuous). Skip-clean when
/// the clean binary is absent; `CQLITE_REQUIRE_FIXTURES=1` makes absence a hard fail;
/// a present fixture that returns `Ok(None)`/`Err` fails unconditionally.
#[tokio::test]
async fn point_lookup_on_clean_chunk_returns_some_1411() {
    let path = datasets_root().map(|r| r.join(CLEAN_DATA_DB));
    let path = match path {
        Some(p) if p.is_file() => p,
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the clean source fixture is absent: {CLEAN_DATA_DB}. \
                 Fetch the corpus (test-data/scripts/fetch-datasets.sh)."
            );
            eprintln!("SKIP: clean source fixture absent ({CLEAN_DATA_DB}); set CQLITE_REQUIRE_FIXTURES=1 to enforce.");
            return;
        }
    };
    let reader = open_reader(&path).await;
    let table_id = TableId::new(TABLE.to_string());
    let key = RowKey::new(PK1_KEY_BYTES.to_vec());

    match reader.get(&table_id, &key).await {
        Ok(Some(_)) => {}
        Ok(None) => panic!(
            "point lookup for pk=1 on the CLEAN {TABLE} fixture returned Ok(None); the \
             partition must be found (else the corrupt-fixture assertion is vacuous)."
        ),
        Err(e) => {
            panic!("point lookup for pk=1 on the CLEAN {TABLE} fixture must succeed, got Err: {e}")
        }
    }
}

/// AC #5 (explicit) — the fail-closed gate itself: with `CQLITE_REQUIRE_FIXTURES=1`,
/// an absent fixture MUST hard-fail rather than skip. Verifies the gate helper's
/// contract without depending on the fixture being present in this process.
#[test]
fn require_fixtures_gate_hard_fails_on_absent_fixture() {
    // Only meaningful when the fixture is genuinely absent; when present the gate
    // returns the path (covered by the other tests). Probe presence directly so we
    // don't trip the gate's own panic here.
    let present = datasets_root()
        .map(|r| r.join(CORRUPT_DATA_DB).is_file())
        .unwrap_or(false);
    if present {
        eprintln!("fixture present — absent-path gate assertion is exercised elsewhere; skipping.");
        return;
    }

    if require_fixtures() {
        // In enforce mode, calling the gate on an absent fixture must panic.
        let panicked = std::panic::catch_unwind(corrupt_data_db_or_gate).is_err();
        assert!(
            panicked,
            "CQLITE_REQUIRE_FIXTURES=1 with an absent fixture must hard-fail (panic), not skip"
        );
    } else {
        // Default mode: absent fixture skips cleanly (returns None).
        assert!(
            corrupt_data_db_or_gate().is_none(),
            "absent fixture without enforce mode should skip (None)"
        );
    }
}
