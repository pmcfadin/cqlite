//! Issue #4194 (corruption-locator) — `VerifyFinding.location` over the real
//! Cassandra-written `test_comp_corrupt` corpus.
//!
//! Every expected partition set is computed HERE, independently, from the
//! CLEAN source's `Index.db` / `CompressionInfo.db` bytes — via small
//! self-contained parsers of this file's own, NEVER by calling
//! `verify_sstable`/`verify_location` on the corrupt copy and trusting its own
//! answer (issue #3041/#3042 doctrine: the oracle is Cassandra-written bytes
//! read independently, not CQLite's own behavior).
//!
//! # Declared gap — L1.4 is NOT covered here
//!
//! The OpenSpec scenario L1.4 (`corrupt_byte_fixture::stage_control_and_mutated`,
//! the "decodable-but-corrupt needle partition") is not reachable with this
//! change's scope. Measured directly (a throwaway probe against the real
//! `BIG_COMPOSITE` fixture): that mutation produces exactly one finding,
//! `VerifyErrorClass::RowScanFailed` (an invalid-UTF-8 clustering-value decode
//! failure surfacing through `classify_scan_error_class`'s generic fallback),
//! carrying NO byte offset anywhere in its message/error chain. `RowScanFailed`
//! is not one of the chunk/offset-anchored classes this change locates
//! (`ChunkDecompressionError`, `UncompressedChunkCrcMismatch`,
//! `ChunkOffsetOutOfBounds` — see the deviation note on
//! `l1_3_truncated_data_db_names_every_partition_past_new_eof` below), and
//! giving it one would mean plumbing a byte offset out of the row-decode path,
//! which does not exist today — materially larger than this issue's stated
//! scope ("no new boundary-source primitive"). Tracked as a follow-up rather
//! than guessed at.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    verify_sstable, PartitionResolution, VerifyErrorClass, VerifyMode,
};
use cqlite_core::Config;

// ---------------------------------------------------------------------------
// Fixture-gating (issue #1094 doctrine, mirrors sstable_parity_corruption_verify.rs
// and issue_1396_uncompressed_crc_verify.rs)
// ---------------------------------------------------------------------------

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

/// Resolve a directory under the datasets root, applying the fail-closed gate.
/// Returns `None` (after emitting a SKIP or panicking under
/// `CQLITE_REQUIRE_FIXTURES=1`) when the corpus is not usable.
fn dataset_dir_or_gate(rel: &str, what: &str) -> Option<PathBuf> {
    let path = datasets_root().map(|r| r.join(rel));
    match path {
        Some(p) if p.is_dir() && has_data_db(&p) => Some(p),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but {what} is unusable: {rel}. Regenerate the \
                 corpus (test-data/scripts/generate-corruption-corpus.sh)."
            );
            eprintln!("SKIP: {what} unusable ({rel}); set CQLITE_REQUIRE_FIXTURES=1 to enforce.");
            None
        }
    }
}

fn has_data_db(dir: &Path) -> bool {
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

/// The clean-source generation directory `<base>/<table>-*`, chosen
/// deterministically (lexicographically first matching entry).
fn clean_source_dir(base_keyspace: &str, table_prefix: &str) -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables").join(base_keyspace);
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?.to_string();
            (name.starts_with(table_prefix) && e.path().is_dir() && has_data_db(&e.path()))
                .then(|| e.path())
        })
        .collect();
    candidates.sort();
    candidates.into_iter().next()
}

async fn run_verify(dir: &Path) -> cqlite_core::storage::sstable::verify::VerifyReport {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    verify_sstable(dir, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({}): {e}", dir.display()))
}

// ---------------------------------------------------------------------------
// Independent oracle parsers — NOT the code under test (issue #3041/#3042).
// Both formats are documented in compression_info.rs's own module doc and
// index_reader/mod.rs's `PartitionIndexEntry` doc; these are separate,
// hand-rolled readers of the SAME on-disk format, not calls into
// `cqlite_core::storage::sstable::{compression_info, index_reader}`.
// ---------------------------------------------------------------------------

/// `(chunk_length, data_length, chunk_offsets)` from a `CompressionInfo.db`.
fn oracle_compression_info(path: &Path) -> (u64, u64, Vec<u64>) {
    let b = std::fs::read(path).expect("read CompressionInfo.db");
    let be16 = |o: usize| u16::from_be_bytes([b[o], b[o + 1]]) as usize;
    let be32 = |o: usize| u32::from_be_bytes(b[o..o + 4].try_into().unwrap()) as u64;
    let be64 = |o: usize| u64::from_be_bytes(b[o..o + 8].try_into().unwrap());

    let name_len = be16(0);
    let mut o = 2 + name_len;
    let option_count = be32(o) as usize;
    o += 4;
    for _ in 0..option_count {
        let kl = be16(o);
        o += 2 + kl;
        let vl = be16(o);
        o += 2 + vl;
    }
    let chunk_length = be32(o);
    o += 4;
    let _max_compressed_length = be32(o);
    o += 4;
    let data_length = be64(o);
    o += 8;
    let chunk_count = be32(o) as usize;
    o += 4;
    let chunk_offsets = (0..chunk_count).map(|i| be64(o + i * 8)).collect();
    (chunk_length, data_length, chunk_offsets)
}

/// One unsigned VInt (Cassandra's `VIntCoding`) from `b[at..]`, as
/// `(value, bytes_consumed)` — the leading-ones count of the first byte is the
/// extra-byte count.
fn read_unsigned_vint(b: &[u8], at: usize) -> (u64, usize) {
    let first = b[at];
    let extra = first.leading_ones() as usize;
    if extra == 0 {
        return (u64::from(first), 1);
    }
    let mask = 0xFFu8 >> (extra + 1);
    let mut v = u64::from(first & mask);
    for k in 1..=extra {
        v = (v << 8) | u64::from(b[at + k]);
    }
    (v, extra + 1)
}

/// Every `(raw partition key, LOGICAL Data.db position)` pair a BIG `Index.db`
/// declares, in on-disk order.
fn oracle_index_positions(path: &Path) -> Vec<(Vec<u8>, u64)> {
    let b = std::fs::read(path).expect("read Index.db");
    let mut out = Vec::new();
    let mut o = 0usize;
    while o + 2 <= b.len() {
        let key_len = u16::from_be_bytes([b[o], b[o + 1]]) as usize;
        o += 2;
        assert!(o + key_len <= b.len(), "Index.db entry declares an over-long key");
        let key = b[o..o + key_len].to_vec();
        o += key_len;
        let (position, n) = read_unsigned_vint(&b, o);
        o += n;
        let (promoted_size, n) = read_unsigned_vint(&b, o);
        o += n + promoted_size as usize;
        out.push((key, position));
    }
    assert!(!out.is_empty(), "Index.db of {} declares no partitions", path.display());
    out
}

/// Independently compute the expected `Resolved` partition-key set (lower-case
/// hex, sorted, deduped — matching `verify_location::resolve_partitions`'s
/// OUTPUT shape, not its logic) for `damaged` (a closed-open LOGICAL byte
/// range) against `positions` (sorted by the caller), bounded by `logical_len`.
fn expected_intersecting_keys(
    damaged: (u64, u64),
    positions: &[(Vec<u8>, u64)],
    logical_len: u64,
) -> Vec<String> {
    let mut sorted = positions.to_vec();
    sorted.sort_by_key(|(_, pos)| *pos);
    let mut hits = Vec::new();
    for (i, (key, start)) in sorted.iter().enumerate() {
        let end = sorted.get(i + 1).map(|(_, p)| *p).unwrap_or(logical_len);
        if damaged.0 < end && *start < damaged.1 {
            hits.push(hex(key));
        }
    }
    hits.sort();
    hits.dedup();
    hits
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn resolved_keys(res: &PartitionResolution) -> Vec<String> {
    match res {
        PartitionResolution::Resolved(keys) => {
            let mut v: Vec<String> = keys.iter().map(|k| k.key_hex.clone()).collect();
            v.sort();
            v
        }
        PartitionResolution::Unresolved(cause) => {
            panic!("expected Resolved, got Unresolved({cause})")
        }
    }
}

// ---------------------------------------------------------------------------
// L1.1 — compressed chunk CRC flip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l1_1_compressed_chunk_crc_flip_names_intersecting_partitions() {
    let Some(corrupt_dir) =
        dataset_dir_or_gate("corruption/test_comp_corrupt/data_db_bit_flip", "data_db_bit_flip")
    else {
        return;
    };
    let Some(clean_dir) = clean_source_dir("test_comp", "lz4_table-") else {
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but the clean lz4_table source is absent");
        eprintln!("SKIP: clean lz4_table source absent");
        return;
    };

    let (chunk_length, data_length, _offsets) =
        oracle_compression_info(&clean_dir.join("nb-1-big-CompressionInfo.db"));
    let positions = oracle_index_positions(&clean_dir.join("nb-1-big-Index.db"));
    // The manifest-pinned flip is at physical byte 64, inside chunk 0 — the
    // damaged LOGICAL range for chunk 0 is [0, chunk_length).
    let expected = expected_intersecting_keys((0, chunk_length), &positions, data_length);
    assert!(!expected.is_empty(), "oracle computed zero intersecting partitions for chunk 0");

    let report = run_verify(&corrupt_dir).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkDecompressionError)
        .unwrap_or_else(|| panic!("no ChunkDecompressionError finding in {:#?}", report.findings));
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkDecompressionError finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(0));
    assert_eq!(resolved_keys(&loc.partitions), expected);
}

// ---------------------------------------------------------------------------
// L1.2 — uncompressed chunk CRC flip (CRC.db grid)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l1_2_uncompressed_chunk_crc_flip_uses_crc_db_grid() {
    let Some(corrupt_dir) = dataset_dir_or_gate(
        "corruption/test_comp_corrupt/uncompressed_data_bit_flip",
        "uncompressed_data_bit_flip",
    ) else {
        return;
    };
    let Some(clean_dir) = clean_source_dir("test_comp", "uncompressed_table-") else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but the clean uncompressed_table source is absent"
        );
        eprintln!("SKIP: clean uncompressed_table source absent");
        return;
    };

    const CRC_CHUNK_SIZE: u64 = 64 * 1024;
    let positions = oracle_index_positions(&clean_dir.join("nb-1-big-Index.db"));
    let logical_len = std::fs::metadata(clean_dir.join("nb-1-big-Data.db"))
        .expect("stat clean Data.db")
        .len();
    // Manifest-pinned flip: physical byte 70000, inside chunk 1 = [65536, 131072).
    let chunk_index = 70_000u64 / CRC_CHUNK_SIZE;
    assert_eq!(chunk_index, 1);
    let damaged = (
        chunk_index * CRC_CHUNK_SIZE,
        (chunk_index + 1) * CRC_CHUNK_SIZE,
    );
    let expected = expected_intersecting_keys(damaged, &positions, logical_len);
    assert!(!expected.is_empty(), "oracle computed zero intersecting partitions for chunk 1");

    let report = run_verify(&corrupt_dir).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::UncompressedChunkCrcMismatch)
        .unwrap_or_else(|| panic!("no UncompressedChunkCrcMismatch finding in {:#?}", report.findings));
    let loc = finding
        .location
        .as_ref()
        .expect("UncompressedChunkCrcMismatch finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(1));
    assert_eq!(resolved_keys(&loc.partitions), expected);
}

// ---------------------------------------------------------------------------
// L1.3 — truncated Data.db
// ---------------------------------------------------------------------------

/// Deviation from design.md's literal L1 wording, recorded here rather than
/// silently: the real `data_db_truncation` fixture's per-chunk-anchored
/// finding is `ChunkOffsetOutOfBounds` (from `check_compression_info`'s
/// declared-offset-vs-Data.db-length bounds check), not `DigestMismatch`/
/// `UnexpectedEof` as design.md's L1 prose names — `DigestMismatch` there is a
/// WHOLE-FILE CRC with no per-chunk anchor at all. Verified directly against
/// the real corpus fixture (`cqlite verify --out json`) before writing this
/// test — see verify.rs's `check_compression_info` doc for the corresponding
/// production-code note.
#[tokio::test]
async fn l1_3_truncated_data_db_names_every_partition_past_new_eof() {
    let Some(corrupt_dir) = dataset_dir_or_gate(
        "corruption/test_comp_corrupt/data_db_truncation",
        "data_db_truncation",
    ) else {
        return;
    };
    let Some(clean_dir) = clean_source_dir("test_comp", "lz4_table-") else {
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but the clean lz4_table source is absent");
        eprintln!("SKIP: clean lz4_table source absent");
        return;
    };

    let (chunk_length, data_length, chunk_offsets) =
        oracle_compression_info(&clean_dir.join("nb-1-big-CompressionInfo.db"));
    let positions = oracle_index_positions(&clean_dir.join("nb-1-big-Index.db"));
    let truncated_len = std::fs::metadata(corrupt_dir.join("nb-1-big-Data.db"))
        .expect("stat truncated Data.db")
        .len();
    // The FIRST chunk whose declared physical offset no longer fits inside the
    // truncated file — every logical byte from that chunk's start onward is
    // past the corrupted file's actual size (design.md §D1's
    // "new_eof .. original_logical_length").
    let first_oob_chunk = chunk_offsets
        .iter()
        .position(|&off| off.saturating_add(4) > truncated_len)
        .expect("oracle expected at least one out-of-bounds chunk offset");
    let new_eof_logical = (first_oob_chunk as u64) * chunk_length;
    let expected =
        expected_intersecting_keys((new_eof_logical, data_length), &positions, data_length);
    assert!(!expected.is_empty(), "oracle computed zero partitions past the truncated EOF");

    let report = run_verify(&corrupt_dir).await;
    // Multiple ChunkOffsetOutOfBounds findings fire (one per bad chunk,
    // ascending); the FIRST is the most inclusive (broadest damaged range).
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkOffsetOutOfBounds)
        .unwrap_or_else(|| panic!("no ChunkOffsetOutOfBounds finding in {:#?}", report.findings));
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkOffsetOutOfBounds finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(first_oob_chunk));
    assert_eq!(resolved_keys(&loc.partitions), expected);
}

// ---------------------------------------------------------------------------
// L2.1 / L2.2 — a damaged boundary source poisons every OTHER location
//
// Neither `index_db_bit_flip_big` nor `bti_partitions_footer_flip` /
// `bti_rows_truncation` naturally produces a SECOND, Data.db-anchored finding
// on its own (measured directly: each yields exactly one finding — the
// boundary-source corruption itself). A combined fixture is staged here (both
// corrupted files from ALREADY-CAPTURED, byte-stable corpus fixtures — never a
// new byte-pattern search) to exercise the fail-closed poisoning this change
// adds.
// ---------------------------------------------------------------------------

fn copy_generation(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy fixture component");
        }
    }
}

/// Flip one bit of the first byte of `path` in place — no CRC recomputation:
/// the whole point is that a downstream integrity check (chunk CRC) fails.
fn bit_flip_first_byte(path: &Path) {
    let mut bytes = std::fs::read(path).expect("read file to flip");
    assert!(!bytes.is_empty(), "cannot bit-flip an empty file");
    bytes[0] ^= 0x01;
    std::fs::write(path, bytes).expect("write flipped file");
}

#[tokio::test]
async fn l2_1_corrupt_big_index_db_unresolves_every_location() {
    let Some(data_corrupt) =
        dataset_dir_or_gate("corruption/test_comp_corrupt/data_db_bit_flip", "data_db_bit_flip")
    else {
        return;
    };
    let Some(index_corrupt) = dataset_dir_or_gate(
        "corruption/test_comp_corrupt/index_db_bit_flip_big",
        "index_db_bit_flip_big",
    ) else {
        return;
    };

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-l2-1-")
        .tempdir()
        .expect("create staging temp dir");
    let combined = staging.path().join("nb-1-big");
    copy_generation(&data_corrupt, &combined);
    // Overwrite Index.db with the OTHER corpus fixture's corrupted copy — both
    // shared the same `lz4_table` clean source, so every sibling component
    // (base name, format) is compatible.
    std::fs::copy(
        index_corrupt.join("nb-1-big-Index.db"),
        combined.join("nb-1-big-Index.db"),
    )
    .expect("overlay corrupted Index.db");

    let report = run_verify(&combined).await;
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::IndexEntryCorrupt),
        "expected the combined fixture to still report IndexEntryCorrupt: {:#?}",
        report.findings
    );
    let chunk_finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkDecompressionError)
        .unwrap_or_else(|| panic!("no ChunkDecompressionError finding in {:#?}", report.findings));
    let loc = chunk_finding
        .location
        .as_ref()
        .expect("ChunkDecompressionError finding must carry a location");
    assert_eq!(
        loc.partitions,
        PartitionResolution::Unresolved(
            cqlite_core::storage::sstable::verify::BOUNDARY_SOURCE_UNREADABLE_CAUSE.to_string()
        )
    );
}

#[tokio::test]
async fn l2_2_corrupt_bti_boundary_source_unresolves_every_location() {
    // `bti_rows_truncation`, not `bti_partitions_footer_flip`: the footer-flip
    // fixture is only DETECTED via the FULL-mode Data.db-scan identity
    // cross-check (`bti_partition_identity_mismatch`), which never runs once
    // Data.db is ALSO corrupted (the scan itself errors first) — measured
    // directly, staging that combination yields no BTI boundary-source
    // finding at all. `bti_rows_truncation`'s `BtiTrieCorrupt` findings are
    // raised structurally in `check_bti_structure` (Check 4), BEFORE the
    // Data.db chunk-CRC check and the scan, so they survive pairing with an
    // independent Data.db corruption.
    let Some(rows_corrupt) = dataset_dir_or_gate(
        "corruption/test_comp_corrupt/bti_rows_truncation",
        "bti_rows_truncation",
    ) else {
        return;
    };

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-l2-2-")
        .tempdir()
        .expect("create staging temp dir");
    let combined = staging.path().join("da-2-bti");
    copy_generation(&rows_corrupt, &combined);
    // Additionally flip a byte inside the first compressed chunk of THIS
    // copy's own Data.db — no CRC recompute, so the chunk-CRC check fails too,
    // giving a second, Data.db-anchored finding to assert Unresolved on.
    bit_flip_first_byte(&combined.join("da-2-bti-Data.db"));

    let report = run_verify(&combined).await;
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::BtiTrieCorrupt),
        "expected the combined fixture to still report BtiTrieCorrupt: {:#?}",
        report.findings
    );
    let chunk_finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkDecompressionError)
        .unwrap_or_else(|| panic!("no ChunkDecompressionError finding in {:#?}", report.findings));
    let loc = chunk_finding
        .location
        .as_ref()
        .expect("ChunkDecompressionError finding must carry a location");
    assert_eq!(
        loc.partitions,
        PartitionResolution::Unresolved(
            cqlite_core::storage::sstable::verify::BOUNDARY_SOURCE_UNREADABLE_CAUSE.to_string()
        )
    );
}

// ---------------------------------------------------------------------------
// L2.3 — a healthy boundary source with no finding never fabricates an
// Unresolved marker
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l2_3_clean_fixture_has_no_findings_and_no_fabricated_location() {
    let Some(clean_dir) = clean_source_dir("test_comp", "lz4_table-") else {
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but the clean lz4_table source is absent");
        eprintln!("SKIP: clean lz4_table source absent");
        return;
    };
    let report = run_verify(&clean_dir).await;
    assert!(
        report.findings.is_empty(),
        "expected a clean baseline: {:#?}",
        report.findings
    );
}
