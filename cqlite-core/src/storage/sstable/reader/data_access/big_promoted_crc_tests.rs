//! Issue #1396 (roborev Fix 1) — the BIG (`nb`) promoted-index / reverse-lookup
//! read path must verify uncompressed `CRC.db` chunks before parsing.
//!
//! [`SSTableReader::big_reverse_partition_rows`] is `pub(crate)`, so this proof
//! lives in-crate rather than in `tests/`. It routes through
//! `decompress_partition_window`, whose uncompressed read now flows through the
//! single CRC-checked accessor (`read_uncompressed_verified`). We prove BOTH:
//!
//! * the CLEAN uncompressed source drives the reverse path to `Ok(Some(rows))`
//!   (so the path is genuinely exercised, not short-circuited to `Ok(None)`), and
//! * the bit-flipped fixture (flip in Data.db chunk 1) returns a typed
//!   `Error::Corruption` naming that chunk — never `Ok(None)` / corrupt bytes.

use crate::storage::sstable::reader::SSTableReader;
use crate::{Config, Error, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// `pk INT, ck INT, body TEXT, PRIMARY KEY (pk, ck)` — the schema of the
/// `test_comp.uncompressed_table` wide partition the fixture derives from.
const SCHEMA_CQL: &str =
    "CREATE TABLE test_comp.uncompressed_table (pk int, ck int, body text, PRIMARY KEY (pk, ck))";

/// The single wide partition's key: `pk = INT 1` → 4-byte big-endian.
const PARTITION_KEY: &[u8] = &[0, 0, 0, 1];

const CORRUPT_DATA_DB: &str =
    "corruption/test_comp_corrupt/uncompressed_data_bit_flip/nb-1-big-Data.db";

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Whether the *corrupt corpus* is required to be present (hard-fail on absent),
/// as opposed to the generic fetched dataset (`require_fixtures`).
///
/// Issue #1799 (red main): the corrupt uncompressed fixture
/// (`corruption/test_comp_corrupt/uncompressed_data_bit_flip/nb-1-big-Data.db`)
/// is **not** part of the fetched dataset — its binaries are gitignored and are
/// only regenerated from a Cassandra container by the strict
/// `compression-corruption-parity` lane (`generate-corruption-corpus.sh`). The
/// generic `Core lib/doc tests` lane sets `CQLITE_REQUIRE_FIXTURES=1` for the
/// ~70 fetched-dataset tests but never provides this corpus, so gating this
/// test's hard assertion on `require_fixtures()` panicked deterministically
/// there. Gate the hard requirement on a *corpus-specific* flag that is set
/// only in the lane that actually regenerates the fixture; everywhere else an
/// absent corrupt fixture is a clean SKIP.
fn require_corrupt_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_CORRUPT_FIXTURES")
            .ok()
            .as_deref(),
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

fn corrupt_data_db() -> Option<PathBuf> {
    let path = datasets_root().map(|r| r.join(CORRUPT_DATA_DB));
    match path {
        Some(p) if p.exists() => Some(p),
        _ => {
            // The corrupt corpus is only regenerated in the strict
            // compression-corruption-parity lane (issue #1799); hard-fail only
            // when that lane explicitly requires it, otherwise SKIP clean.
            assert!(
                !require_corrupt_fixtures(),
                "CQLITE_REQUIRE_CORRUPT_FIXTURES=1 but the corrupt uncompressed fixture is \
                 absent: {CORRUPT_DATA_DB}"
            );
            eprintln!("SKIP: corrupt uncompressed fixture absent ({CORRUPT_DATA_DB}).");
            None
        }
    }
}

fn clean_source_data_db() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_comp");
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with("uncompressed_table-") {
            let candidate = entry.path().join("nb-1-big-Data.db");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

async fn open_reader(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("opening the structurally valid uncompressed fixture should succeed")
}

fn assert_typed_uncompressed_chunk_corruption(err: &Error) {
    assert!(
        matches!(err, Error::Corruption(_)),
        "uncompressed CRC mismatch must be Error::Corruption, got: {err}"
    );
    assert!(
        !err.is_recoverable(),
        "a bad chunk is non-recoverable, got recoverable: {err}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("chunk 1"),
        "corruption error must name the failing chunk ('chunk 1'), got: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("CRC"),
        "corruption error should identify the CRC mismatch, got: {msg}"
    );
}

/// The reverse path is genuinely exercised on the CLEAN source: it must resolve
/// the promoted index + partition window and return the partition's rows.
#[tokio::test]
async fn clean_reverse_partition_path_returns_rows() {
    let Some(path) = clean_source_data_db() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but the clean uncompressed_table source is absent"
        );
        eprintln!("SKIP: clean uncompressed_table source absent.");
        return;
    };
    let reader = open_reader(&path).await;
    let schema = crate::schema::parse_cql_schema(SCHEMA_CQL).expect("parse schema");

    let rows = reader
        .big_reverse_partition_rows(PARTITION_KEY, Some(&schema))
        .await
        .expect("clean reverse-partition read must succeed");
    let rows = rows.expect(
        "the reverse/promoted-index path must apply to this wide partition (Some), \
         otherwise the corrupt-fixture test below would not exercise the read",
    );
    assert!(
        !rows.is_empty(),
        "clean wide partition must yield rows via the reverse path"
    );
}

/// The bit-flipped fixture makes the promoted-index / reverse-lookup read of the
/// corrupt uncompressed chunk fail fast with a typed corruption error — never
/// `Ok(None)`, never corrupt bytes.
#[tokio::test]
async fn reverse_partition_path_over_corrupt_chunk_fails_fast() {
    let Some(path) = corrupt_data_db() else {
        return;
    };
    let reader = open_reader(&path).await;
    let schema = crate::schema::parse_cql_schema(SCHEMA_CQL).expect("parse schema");

    match reader
        .big_reverse_partition_rows(PARTITION_KEY, Some(&schema))
        .await
    {
        Ok(Some(rows)) => panic!(
            "FIXTURE ROT or read-path regression: reverse-partition read over the bit-flipped \
             uncompressed chunk returned Ok(Some) with {} rows; it must fail with corruption.",
            rows.len()
        ),
        Ok(None) => panic!(
            "reverse-partition read over the corrupt chunk returned Ok(None) — the CRC check was \
             bypassed / the read path silently swallowed the corruption."
        ),
        Err(err) => assert_typed_uncompressed_chunk_corruption(&err),
    }
}
