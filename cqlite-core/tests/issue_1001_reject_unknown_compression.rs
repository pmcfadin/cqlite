//! Issue #1001 (Epic #970): reject unknown/unsupported compression algorithms FAIL-FAST.
//!
//! Before this change, `CompressionAlgorithm::from(&str)` mapped any unrecognized
//! compressor name to `CompressionAlgorithm::None` (silent uncompressed fallback),
//! which meant a `CompressionInfo.db` naming a compressor CQLite cannot decompress
//! would be read as if its compressed chunks were raw bytes -> garbage, not an error.
//!
//! The contract now: an unknown/unsupported compressor name produces an explicit
//! `Error::UnsupportedFormat` at CompressionInfo.db parse / reader-open time, BEFORE
//! any Data.db chunk is read, and the error text includes the EXACT offending name.
//!
//! Run with:
//! ```text
//! env CQLITE_DATASETS_ROOT=/Users/pmcfadin/projects/cqlite-epic970/test-data/datasets \
//!   cargo test -p cqlite-core --test issue_1001_reject_unknown_compression
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::compression::{CompressionAlgorithm, CompressionInfo};
use cqlite_core::storage::sstable::compression_info::{
    is_supported_compressor_name, CompressionInfo as MetaCompressionInfo,
};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Error, Platform};

const BOGUS_NAME: &str = "FooCompressor";

/// Build a minimal but structurally-valid Cassandra 5.0 (`>= "na"`) CompressionInfo.db
/// blob, matching the deterministic layout parsed by `CompressionInfo::parse`:
///
/// writeUTF(name) | writeInt(option_count) | options | writeInt(chunk_length)
/// | writeInt(max_compressed_length) | writeLong(data_length) | writeInt(chunk_count)
/// | chunk_count * writeLong(offset)
fn make_compression_info_blob(
    algorithm: &str,
    chunk_length: u32,
    max_compressed_length: u32,
    data_length: u64,
    offsets: &[u64],
) -> Vec<u8> {
    let mut data = Vec::new();

    // writeUTF(algorithm): 2-byte BE length + UTF-8 bytes
    let name_bytes = algorithm.as_bytes();
    data.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    data.extend_from_slice(name_bytes);

    // writeInt(option_count) == 0
    data.extend_from_slice(&0u32.to_be_bytes());

    // writeInt(chunk_length)
    data.extend_from_slice(&chunk_length.to_be_bytes());

    // writeInt(max_compressed_length)
    data.extend_from_slice(&max_compressed_length.to_be_bytes());

    // writeLong(data_length)
    data.extend_from_slice(&data_length.to_be_bytes());

    // writeInt(chunk_count)
    data.extend_from_slice(&(offsets.len() as u32).to_be_bytes());

    // chunk offsets
    for &off in offsets {
        data.extend_from_slice(&off.to_be_bytes());
    }

    data
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

// =============================================================================
// ALWAYS-RUN synthetic tests (no dataset required)
// =============================================================================

/// The canonical metadata parser (`compression_info::CompressionInfo::parse`) — this is
/// the gate `SSTableReader::open` routes through BEFORE any row scan — must reject an
/// unknown compressor name, and the error text must contain the exact offending name.
#[test]
fn synthetic_canonical_parse_rejects_unknown_algorithm_fail_fast() {
    let blob = make_compression_info_blob(BOGUS_NAME, 16384, i32::MAX as u32, 32768, &[0, 8200]);

    // No row scan is even reachable: parse returns Err, so nothing downstream runs.
    let err = MetaCompressionInfo::parse(&blob)
        .expect_err("parse must FAIL-FAST on an unsupported compressor, not fall back to None");

    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "expected Error::UnsupportedFormat, got: {err:?}"
    );
    assert!(
        err.to_string().contains(BOGUS_NAME),
        "error text must name the exact offending algorithm `{BOGUS_NAME}`; got: {err}"
    );
}

/// The legacy binary parser (`compression::CompressionInfo::parse_binary`, used by the
/// `reader/compression.rs` discovery path) must also reject unknown names fail-fast.
#[test]
fn synthetic_legacy_parse_binary_rejects_unknown_algorithm_fail_fast() {
    // The legacy parser requires >= 20 bytes; the blob above is well over that.
    let blob = make_compression_info_blob(BOGUS_NAME, 16384, i32::MAX as u32, 32768, &[0, 8200]);

    let err = CompressionInfo::parse_binary(&blob)
        .expect_err("parse_binary must FAIL-FAST on an unsupported compressor");

    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "expected Error::UnsupportedFormat, got: {err:?}"
    );
    assert!(
        err.to_string().contains(BOGUS_NAME),
        "error text must name the exact offending algorithm `{BOGUS_NAME}`; got: {err}"
    );
}

/// `CompressionAlgorithm::parse` is the fallible name resolver. Known names map to the
/// right codec; an unknown name is an explicit error naming the offending string.
#[test]
fn synthetic_algorithm_parse_known_names_and_unknown_error() {
    // Cassandra simple names.
    assert_eq!(
        CompressionAlgorithm::parse("LZ4Compressor").unwrap(),
        CompressionAlgorithm::Lz4
    );
    assert_eq!(
        CompressionAlgorithm::parse("SnappyCompressor").unwrap(),
        CompressionAlgorithm::Snappy
    );
    assert_eq!(
        CompressionAlgorithm::parse("DeflateCompressor").unwrap(),
        CompressionAlgorithm::Deflate
    );
    assert_eq!(
        CompressionAlgorithm::parse("ZstdCompressor").unwrap(),
        CompressionAlgorithm::Zstd
    );
    // CQLite short names.
    assert_eq!(
        CompressionAlgorithm::parse("LZ4").unwrap(),
        CompressionAlgorithm::Lz4
    );
    // Explicit no-compression marker.
    assert_eq!(
        CompressionAlgorithm::parse("NONE").unwrap(),
        CompressionAlgorithm::None
    );
    // Fully-qualified class name is accepted.
    assert_eq!(
        CompressionAlgorithm::parse("org.apache.cassandra.io.compress.LZ4Compressor").unwrap(),
        CompressionAlgorithm::Lz4
    );

    // Unknown -> explicit error containing the offending name.
    let err = CompressionAlgorithm::parse(BOGUS_NAME)
        .expect_err("unknown name must error, never map to None");
    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "expected UnsupportedFormat, got {err:?}"
    );
    assert!(err.to_string().contains(BOGUS_NAME));
}

/// The supported-name predicate is the single source of truth used by the parsers.
#[test]
fn synthetic_is_supported_compressor_name() {
    assert!(is_supported_compressor_name("LZ4Compressor"));
    assert!(is_supported_compressor_name("SnappyCompressor"));
    assert!(is_supported_compressor_name("DeflateCompressor"));
    assert!(is_supported_compressor_name("ZstdCompressor"));
    // Fully-qualified class names resolve to the simple name.
    assert!(is_supported_compressor_name(
        "org.apache.cassandra.io.compress.ZstdCompressor"
    ));
    // Unknown.
    assert!(!is_supported_compressor_name(BOGUS_NAME));
    assert!(!is_supported_compressor_name("AesCompressor"));
}

/// A genuinely uncompressed SSTable has NO CompressionInfo.db; the metadata loader must
/// not invent one. We model that here: there is no CompressionInfo.db blob to parse, so
/// nothing rejects, and the canonical parse of a real (valid) blob still succeeds.
#[test]
fn synthetic_known_name_still_parses() {
    let blob =
        make_compression_info_blob("LZ4Compressor", 16384, i32::MAX as u32, 32768, &[0, 8200]);
    let info = MetaCompressionInfo::parse(&blob).expect("valid LZ4 blob must parse");
    assert_eq!(info.algorithm, "LZ4Compressor");
    assert_eq!(
        info.algorithm_enum().expect("known algorithm resolves"),
        CompressionAlgorithm::Lz4
    );
}

// =============================================================================
// Fixture-backed tests (skip cleanly if the dataset is absent)
// =============================================================================

fn find_data_file(table_dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(table_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
        })
}

/// Locate an LZ4-compressed table directory under `test_comp` (preferred) or `test_basic`.
fn find_lz4_table_dir(root: &Path) -> Option<PathBuf> {
    let candidates = [
        ("test_comp", "lz4_table"),
        ("test_basic", "simple_table"),
        ("test_basic", "compression_test_table"),
    ];
    let sstables = root.join("sstables");
    for (ks, prefix) in candidates {
        let ks_dir = sstables.join(ks);
        if !ks_dir.exists() {
            continue;
        }
        let hit = std::fs::read_dir(&ks_dir)
            .ok()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.is_dir()
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with(prefix))
                        .unwrap_or(false)
            });
        if let Some(dir) = hit {
            // Only accept dirs that actually carry a CompressionInfo.db (i.e. compressed).
            let has_ci = std::fs::read_dir(&dir)
                .ok()?
                .filter_map(|e| e.ok())
                .any(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.ends_with("-CompressionInfo.db"))
                        .unwrap_or(false)
                });
            if has_ci {
                return Some(dir);
            }
        }
    }
    None
}

/// Copy an entire SSTable component directory into a temp dir, returning the temp root.
fn copy_table_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name();
            std::fs::copy(&path, dst.join(name))?;
        }
    }
    Ok(())
}

/// Fixture variant: take a REAL LZ4 SSTable, flip its CompressionInfo.db algorithm name
/// to `FooCompressor`, then open via the full `SSTableReader::open` path and assert the
/// open fails fast with an UnsupportedFormat error naming `FooCompressor` — i.e. BEFORE
/// any row scan (open never returns a reader to scan with).
#[tokio::test]
async fn fixture_reader_open_rejects_mutated_unknown_algorithm() {
    let Some(root) = datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set; skipping fixture variant");
        return;
    };
    let Some(table_dir) = find_lz4_table_dir(&root) else {
        eprintln!("No compressed fixture table found; skipping fixture variant");
        return;
    };
    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("No Data.db in fixture table; skipping fixture variant");
        return;
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let dst = tmp.path().join("mutated");
    copy_table_dir(&table_dir, &dst).expect("copy table dir");

    // Find the copied CompressionInfo.db and mutate the algorithm name in place.
    let ci_path = std::fs::read_dir(&dst)
        .expect("read copied dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-CompressionInfo.db"))
                .unwrap_or(false)
        })
        .expect("copied CompressionInfo.db");

    let mut bytes = std::fs::read(&ci_path).expect("read CompressionInfo.db");
    // writeUTF: bytes[0..2] = name length (BE u16), bytes[2..2+len] = name.
    let name_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let original = String::from_utf8_lossy(&bytes[2..2 + name_len]).to_string();
    eprintln!("Mutating fixture algorithm `{original}` -> `{BOGUS_NAME}`");
    // `FooCompressor` is 13 bytes, same as `LZ4Compressor`; rewrite name + length to be safe.
    let new_name = BOGUS_NAME.as_bytes();
    let mut rebuilt = Vec::with_capacity(bytes.len());
    rebuilt.extend_from_slice(&(new_name.len() as u16).to_be_bytes());
    rebuilt.extend_from_slice(new_name);
    rebuilt.extend_from_slice(&bytes[2 + name_len..]);
    bytes = rebuilt;
    std::fs::write(&ci_path, &bytes).expect("write mutated CompressionInfo.db");

    let mutated_data_file = dst.join(data_file.file_name().expect("data file name"));

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    let result = SSTableReader::open(&mutated_data_file, &config, platform).await;

    let err = result.expect_err(
        "opening an SSTable whose CompressionInfo.db names an unsupported compressor \
         must FAIL-FAST, never silently fall back to uncompressed",
    );
    assert!(
        err.to_string().contains(BOGUS_NAME),
        "open error must name the exact offending algorithm `{BOGUS_NAME}`; got: {err}"
    );
}

/// Control: the SAME real fixture opens successfully when left unmutated (known LZ4 name),
/// proving the rejection above is specifically about the unknown name, not the copy/open
/// machinery. Also exercises the genuinely-uncompressed path indirectly: an SSTable with
/// no CompressionInfo.db is unaffected (it never enters the parse gate).
#[tokio::test]
async fn fixture_reader_open_known_algorithm_still_works() {
    let Some(root) = datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set; skipping fixture control");
        return;
    };
    let Some(table_dir) = find_lz4_table_dir(&root) else {
        eprintln!("No compressed fixture table found; skipping fixture control");
        return;
    };
    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("No Data.db in fixture table; skipping fixture control");
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("real, unmutated compressed SSTable must still open fine");
}
