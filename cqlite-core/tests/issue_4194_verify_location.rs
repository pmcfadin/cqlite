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
    format_location, verify_sstable, PartitionResolution, PhysicalAnchor, VerifyErrorClass,
    VerifyMode, MAX_RESOLVED_KEYS,
};
use cqlite_core::Config;

// ---------------------------------------------------------------------------
// Fixture resolution + gating
//
// TABLE-granular BY CONTRACT (issue #3220, CLAUDE.md "Resolve fixture roots per
// TABLE, and assert per CASE"): the clean sources come from the sanctioned
// `support/datasets_root.rs` resolver, which walks EVERY candidate `sstables/`
// root and picks the one that actually carries `<keyspace>/<table>-*/…-Data.db`.
// This file previously hand-rolled the env-first, keyspace-directory form
// (`CQLITE_DATASETS_ROOT` if it is a directory, else the checkout, then
// `join("sstables").join(keyspace)`) — the exact selection #3220 removed: it
// COMMITS to a root chosen on the keyspace and then reports a table that a
// DIFFERENT candidate root holds as absent. Neither root is a superset of the
// other (#3104), so no fixed preference is right for every table.
//
// The corruption fixtures (`corruption/<keyspace>_corrupt/<case>/`) are not
// `<keyspace>/<table>-*` shaped, so they cannot go through
// `resolve_table_generation_dir`; they are resolved over the SAME candidate
// base-root list instead (mirroring `support/salvage_corpus.rs`'s
// `candidate_base_roots`/`resolve_root_with_corpus_fixture`, issue #4196 — the
// sibling lane on this same corpus), and a case needing BOTH halves binds them
// from ONE root so the oracle can never read a clean `Index.db` from one
// generation of the corpus against a corrupt copy derived from another.
//
// Gating follows #1094 doctrine: a missing fixture SKIPs loudly and
// `CQLITE_REQUIRE_FIXTURES=1` turns every such skip into a hard failure.
// ---------------------------------------------------------------------------

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{resolve_table_generation_dir, table_generation_dirs};

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Every candidate BASE root — the PARENT of what
/// `datasets_root::sstables_root_candidates()` returns, since the corruption
/// corpus is a `corruption/` SIBLING of `sstables/`. Same env-var + checkout
/// resolution `sstables_root_for_table` uses, never `CQLITE_DATASETS_ROOT`
/// alone.
fn candidate_base_roots() -> Vec<PathBuf> {
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

fn describe_base_roots() -> String {
    candidate_base_roots()
        .iter()
        .map(|r| r.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve one corruption-corpus fixture directory by EVIDENCE across every
/// candidate base root, applying the fail-closed gate.
fn dataset_dir_or_gate(rel: &str, what: &str) -> Option<PathBuf> {
    dataset_dirs_or_gate(&[rel], what).map(|mut dirs| dirs.remove(0))
}

/// Resolve SEVERAL corruption-corpus fixture directories that a single case
/// combines, all from ONE candidate base root — a case that overlays one
/// fixture's component onto another's generation must not mix two different
/// corpus generations. Returns `None` (after emitting a SKIP, or panicking
/// under `CQLITE_REQUIRE_FIXTURES=1`) when no single candidate root carries
/// every named fixture.
fn dataset_dirs_or_gate(rels: &[&str], what: &str) -> Option<Vec<PathBuf>> {
    let found = candidate_base_roots().into_iter().find_map(|root| {
        let dirs: Vec<PathBuf> = rels.iter().map(|rel| root.join(rel)).collect();
        dirs.iter().all(|p| has_data_db(p)).then_some(dirs)
    });
    let rel_list = rels.join(", ");
    match found {
        Some(dirs) => Some(dirs),
        None => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but {what} is unusable: no single candidate datasets \
                 root [{}] carries every one of [{rel_list}] with a *-Data.db. Regenerate the \
                 corpus (test-data/scripts/generate-corruption-corpus.sh).",
                describe_base_roots()
            );
            eprintln!(
                "SKIP: {what} unusable (no single candidate datasets root [{}] carries every one \
                 of [{rel_list}] with a *-Data.db); set CQLITE_REQUIRE_FIXTURES=1 to enforce.",
                describe_base_roots()
            );
            None
        }
    }
}

/// A corruption fixture and its CLEAN source table, bound from ONE candidate
/// base root: the oracle reads the clean generation's `Index.db` /
/// `CompressionInfo.db` and compares against findings on the corrupt COPY of
/// that same generation, so the two halves must come from the same corpus.
fn corrupt_and_clean_or_gate(
    rel: &str,
    what: &str,
    keyspace: &str,
    table: &str,
) -> Option<(PathBuf, PathBuf)> {
    let found = candidate_base_roots().into_iter().find_map(|root| {
        let corrupt = root.join(rel);
        if !has_data_db(&corrupt) {
            return None;
        }
        let clean = table_generation_dirs(&root.join("sstables"), keyspace, table)
            .into_iter()
            .next()?;
        Some((corrupt, clean))
    });
    match found {
        Some(pair) => Some(pair),
        None => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but no candidate datasets root [{}] carries BOTH \
                 {rel} ({what}) and the clean {keyspace}.{table} source. Regenerate the corpus \
                 (test-data/scripts/generate-corruption-corpus.sh).",
                describe_base_roots()
            );
            eprintln!(
                "SKIP: no candidate datasets root [{}] carries BOTH {rel} ({what}) and the clean \
                 {keyspace}.{table} source; set CQLITE_REQUIRE_FIXTURES=1 to enforce.",
                describe_base_roots()
            );
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

/// The clean-source generation directory of `<keyspace>.<table>`, resolved
/// TABLE-granularly across every candidate root and selected
/// deterministically, with the same #1094 gate.
fn clean_source_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    match resolve_table_generation_dir(keyspace, table) {
        Ok(dir) => Some(dir),
        Err(why) => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the clean {keyspace}.{table} source is absent: \
                 {why}"
            );
            eprintln!(
                "SKIP: clean {keyspace}.{table} source absent ({why}); set \
                 CQLITE_REQUIRE_FIXTURES=1 to enforce."
            );
            None
        }
    }
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
        assert!(
            o + key_len <= b.len(),
            "Index.db entry declares an over-long key"
        );
        let key = b[o..o + key_len].to_vec();
        o += key_len;
        let (position, n) = read_unsigned_vint(&b, o);
        o += n;
        let (promoted_size, n) = read_unsigned_vint(&b, o);
        o += n + promoted_size as usize;
        out.push((key, position));
    }
    assert!(
        !out.is_empty(),
        "Index.db of {} declares no partitions",
        path.display()
    );
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
        PartitionResolution::Resolved { keys, truncated } => {
            assert_eq!(
                *truncated, 0,
                "test fixtures never exceed MAX_RESOLVED_KEYS; a non-zero truncated count means \
                 the oracle's expectation is incomplete, not that this is fine"
            );
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
    let Some((corrupt_dir, clean_dir)) = corrupt_and_clean_or_gate(
        "corruption/test_comp_corrupt/data_db_bit_flip",
        "data_db_bit_flip",
        "test_comp",
        "lz4_table",
    ) else {
        return;
    };

    let (chunk_length, data_length, _offsets) =
        oracle_compression_info(&clean_dir.join("nb-1-big-CompressionInfo.db"));
    let positions = oracle_index_positions(&clean_dir.join("nb-1-big-Index.db"));
    // The manifest-pinned flip is at physical byte 64, inside chunk 0 — the
    // damaged LOGICAL range for chunk 0 is [0, chunk_length).
    let expected = expected_intersecting_keys((0, chunk_length), &positions, data_length);
    assert!(
        !expected.is_empty(),
        "oracle computed zero intersecting partitions for chunk 0"
    );

    let report = run_verify(&corrupt_dir).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkDecompressionError)
        .unwrap_or_else(|| {
            panic!(
                "no ChunkDecompressionError finding in {:#?}",
                report.findings
            )
        });
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkDecompressionError finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(0));
    assert_eq!(resolved_keys(&loc.partitions), expected);
    // POSITIVE CONTROL for roborev job 92 MEDIUM: a chunk CRC flip damages
    // bytes that ARE present, so this class keeps the damaged-extent reading.
    // Without this arm the DeclaredRecord assertions elsewhere would be
    // satisfied by classifying everything as declared.
    assert_eq!(
        loc.anchor,
        PhysicalAnchor::DamagedExtent,
        "a CRC flip's bytes are present on disk and genuinely damaged"
    );
    let rendered = format_location(loc);
    assert!(
        !rendered.contains("declared offset"),
        "a real damaged extent must NOT be rendered as a declared record: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// L1.2 — uncompressed chunk CRC flip (CRC.db grid)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l1_2_uncompressed_chunk_crc_flip_uses_crc_db_grid() {
    let Some((corrupt_dir, clean_dir)) = corrupt_and_clean_or_gate(
        "corruption/test_comp_corrupt/uncompressed_data_bit_flip",
        "uncompressed_data_bit_flip",
        "test_comp",
        "uncompressed_table",
    ) else {
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
    assert!(
        !expected.is_empty(),
        "oracle computed zero intersecting partitions for chunk 1"
    );

    let report = run_verify(&corrupt_dir).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::UncompressedChunkCrcMismatch)
        .unwrap_or_else(|| {
            panic!(
                "no UncompressedChunkCrcMismatch finding in {:#?}",
                report.findings
            )
        });
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
    let Some((corrupt_dir, clean_dir)) = corrupt_and_clean_or_gate(
        "corruption/test_comp_corrupt/data_db_truncation",
        "data_db_truncation",
        "test_comp",
        "lz4_table",
    ) else {
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
    assert!(
        !expected.is_empty(),
        "oracle computed zero partitions past the truncated EOF"
    );

    let report = run_verify(&corrupt_dir).await;
    // Multiple ChunkOffsetOutOfBounds findings fire (one per bad chunk,
    // ascending); the FIRST is the most inclusive (broadest damaged range).
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkOffsetOutOfBounds)
        .unwrap_or_else(|| {
            panic!(
                "no ChunkOffsetOutOfBounds finding in {:#?}",
                report.findings
            )
        });
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkOffsetOutOfBounds finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(first_oob_chunk));
    assert_eq!(resolved_keys(&loc.partitions), expected);
    // roborev job 92 MEDIUM: a truncation's physical range is the DECLARED
    // chunk offset, which lies past EOF — so it must be classified and
    // rendered as a declared record, never as a damaged byte extent. An
    // operator who reads "len 4 damaged" here goes to `dd` and gets nothing.
    assert_eq!(
        loc.anchor,
        PhysicalAnchor::DeclaredRecord,
        "a past-EOF declared offset is not a damaged extent"
    );
    let rendered = format_location(loc);
    assert!(
        rendered.contains("declared offset") && rendered.contains("does not fit"),
        "the rendered location must disclose that these bytes are absent: {rendered}"
    );
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
    // BOTH fixtures from ONE candidate root: this case overlays one's Index.db
    // onto the other's generation, so they must be copies of the same clean
    // `lz4_table` source, never two roots' independent corpus generations.
    let Some(dirs) = dataset_dirs_or_gate(
        &[
            "corruption/test_comp_corrupt/data_db_bit_flip",
            "corruption/test_comp_corrupt/index_db_bit_flip_big",
        ],
        "data_db_bit_flip + index_db_bit_flip_big",
    ) else {
        return;
    };
    let [data_corrupt, index_corrupt] = dirs.as_slice() else {
        panic!(
            "dataset_dirs_or_gate returned {} dirs for 2 rels",
            dirs.len()
        );
    };

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-l2-1-")
        .tempdir()
        .expect("create staging temp dir");
    let combined = staging.path().join("nb-1-big");
    copy_generation(data_corrupt, &combined);
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
        .unwrap_or_else(|| {
            panic!(
                "no ChunkDecompressionError finding in {:#?}",
                report.findings
            )
        });
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
        .unwrap_or_else(|| {
            panic!(
                "no ChunkDecompressionError finding in {:#?}",
                report.findings
            )
        });
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
// roborev round-1 MEDIUM finding — a BTI location that actually RESOLVES.
//
// Before this test, no case exercised a BTI `Resolved` outcome: `l2_2` only
// asserts the `Unresolved(BOUNDARY_SOURCE_UNREADABLE)` path, and L1.4 is a
// declared gap. Reuses `iterate_partitions_in_bti_file` /
// `resolve_rows_db_entry` — EXISTING, separately-tested production BTI trie
// primitives (the same ones `check_bti_structure`/the clustering read path
// already depend on) — as the independent oracle, since hand-rolling a
// from-scratch byte-comparable trie walker for this one test is a
// disproportionate undertaking; this is "reuse of already-validated
// infrastructure", not "testing the new location-resolution code against
// itself" (#3041/#3042 concerns the latter).
// ---------------------------------------------------------------------------

/// `(raw partition key, LOGICAL Data.db position)` for every `RowsOffset`
/// leaf in a BTI `Partitions.db`/`Rows.db` pair (`DataOffset` leaves are
/// skipped — their raw key is only recoverable through a Data.db scan, which
/// this oracle deliberately does not perform).
fn oracle_bti_rows_offset_positions(
    partitions_path: &Path,
    rows_path: &Path,
) -> Vec<(Vec<u8>, u64)> {
    use cqlite_core::storage::sstable::bti::{
        iterate_partitions_in_bti_file, resolve_rows_db_entry, BtiPartitionLocation,
    };
    use std::io::Cursor;

    let partitions_bytes = std::fs::read(partitions_path).expect("read Partitions.db");
    let rows_bytes = std::fs::read(rows_path).expect("read Rows.db");
    let mut cursor = Cursor::new(&partitions_bytes);
    let entries = iterate_partitions_in_bti_file(&mut cursor).expect("walk Partitions.db trie");

    let mut out = Vec::new();
    for (_, location) in entries {
        if let BtiPartitionLocation::RowsOffset(off) = location {
            let header =
                resolve_rows_db_entry(&rows_bytes, off as usize).expect("resolve Rows.db entry");
            let key_length =
                u16::from_be_bytes([rows_bytes[off as usize], rows_bytes[off as usize + 1]])
                    as usize;
            let key_start = off as usize + 2;
            let key = rows_bytes[key_start..key_start + key_length].to_vec();
            out.push((key, header.data_position));
        }
    }
    out
}

#[tokio::test]
async fn bti_compressed_chunk_crc_flip_resolves_via_rows_offset_leaves() {
    let Some(clean) = clean_source_dir("test_da", "wide_table") else {
        return;
    };
    if !clean.join("da-2-bti-CompressionInfo.db").is_file() {
        // roborev round-4 MEDIUM finding: this is the ONLY case covering a
        // BTI `Resolved` location (L2.2 covers only `Unresolved`; L1.4 is a
        // declared gap) — under CQLITE_REQUIRE_FIXTURES=1 this whole branch
        // must hard-fail, not silently vanish behind a green suite, matching
        // every other gate in this file (#1094 doctrine).
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but wide_table is not compressed (no CompressionInfo.db)"
        );
        eprintln!("SKIP: wide_table fixture is not compressed (no CompressionInfo.db)");
        return;
    }

    let (chunk_length, data_length, _offsets) =
        oracle_compression_info(&clean.join("da-2-bti-CompressionInfo.db"));
    let positions = oracle_bti_rows_offset_positions(
        &clean.join("da-2-bti-Partitions.db"),
        &clean.join("da-2-bti-Rows.db"),
    );
    let expected = expected_intersecting_keys((0, chunk_length), &positions, data_length);
    if expected.is_empty() {
        // Every leaf intersecting chunk 0 is a `DataOffset` leaf (or there are
        // none) on this fixture — not the shape this test targets. Refuse to
        // fabricate a pass over an untested branch; a future fixture swap
        // must re-derive this rather than silently green over a gap.
        panic!(
            "oracle computed zero RowsOffset-leaf partitions intersecting chunk 0 of wide_table \
             — this test needs a fixture where chunk 0 intersects at least one RowsOffset leaf; \
             re-derive against the current fixture rather than skip"
        );
    }

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-bti-resolve-")
        .tempdir()
        .expect("create staging temp dir");
    let staged = staging.path().join("da-2-bti");
    copy_generation(&clean, &staged);
    bit_flip_first_byte(&staged.join("da-2-bti-Data.db"));

    let report = run_verify(&staged).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkDecompressionError)
        .unwrap_or_else(|| {
            panic!(
                "no ChunkDecompressionError finding in {:#?}",
                report.findings
            )
        });
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkDecompressionError finding must carry a location");
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.chunk_index, Some(0));
    assert_eq!(resolved_keys(&loc.partitions), expected);
}

// ---------------------------------------------------------------------------
// L2.3 — a healthy boundary source with no finding never fabricates an
// Unresolved marker
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l2_3_clean_fixture_has_no_findings_and_no_fabricated_location() {
    let Some(clean_dir) = clean_source_dir("test_comp", "lz4_table") else {
        return;
    };
    let report = run_verify(&clean_dir).await;
    assert!(
        report.findings.is_empty(),
        "expected a clean baseline: {:#?}",
        report.findings
    );
}

// ---------------------------------------------------------------------------
// L5.1 — the Resolved set is CAPPED at MAX_RESOLVED_KEYS, end to end
//
// WHY A REAL 1000-PARTITION TABLE AND NOT A SYNTHETIC ONE. The cap only
// engages above 100 intersecting partitions, and every `test_comp_corrupt`
// fixture is far too small to reach it (hence `resolved_keys`'s
// `truncated == 0` assertion above). `test_basic.simple_table` is a real
// Cassandra-written generation carrying 1000 partitions — 10x the cap — so
// truncating a COPY of its Data.db drives the cap through the public
// `verify_sstable` surface with no synthetic fixture at all.
//
// The corruption corpus README's "CI consumes the DESCRIBED corruptions and
// never mutates bytes at test time" is respected: nothing here touches the
// shared corpus. The clean generation is copied into a tempdir first and the
// COPY is truncated, exactly as L2.1/L2.2 above copy-then-bit-flip.
// ---------------------------------------------------------------------------

/// Truncate `path` in place to `len` bytes. No CRC recomputation: a truncation
/// is precisely the case where the declared chunk offsets outrun the file.
fn truncate_to(path: &Path, len: u64) {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap_or_else(|e| panic!("open {} for truncation: {e}", path.display()));
    f.set_len(len)
        .unwrap_or_else(|e| panic!("truncate {} to {len}: {e}", path.display()));
}

/// The materialized key list and the `truncated` count of a CAPPED resolution.
/// Deliberately separate from `resolved_keys`, which asserts `truncated == 0`
/// for the small fixtures: this case exists to observe a NON-zero count.
fn capped_resolution(res: &PartitionResolution) -> (Vec<String>, usize) {
    match res {
        PartitionResolution::Resolved { keys, truncated } => {
            let mut v: Vec<String> = keys.iter().map(|k| k.key_hex.clone()).collect();
            v.sort();
            (v, *truncated)
        }
        PartitionResolution::Unresolved(cause) => {
            panic!("expected a capped Resolved set, got Unresolved({cause})")
        }
    }
}

#[tokio::test]
async fn l5_1_resolved_set_is_capped_and_names_the_omitted_count_end_to_end() {
    let Some(clean_dir) = clean_source_dir("test_basic", "simple_table") else {
        return;
    };

    let (chunk_length, data_length, chunk_offsets) =
        oracle_compression_info(&clean_dir.join("nb-1-big-CompressionInfo.db"));
    let positions = oracle_index_positions(&clean_dir.join("nb-1-big-Index.db"));
    assert!(
        chunk_offsets.len() >= 2,
        "simple_table must span >=2 compressed chunks to be truncatable mid-table; got {}",
        chunk_offsets.len()
    );

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-l5-1-")
        .tempdir()
        .expect("create staging temp dir");
    let staged = staging.path().join("nb-1-big");
    copy_generation(&clean_dir, &staged);

    // Keep EXACTLY the first compressed chunk, so every later chunk's declared
    // offset is past the new EOF and the damaged logical range starts at
    // chunk 1 — spanning essentially the whole 1000-partition table.
    let keep = chunk_offsets[1];
    truncate_to(&staged.join("nb-1-big-Data.db"), keep);

    let first_oob_chunk = chunk_offsets
        .iter()
        .position(|&off| off.saturating_add(4) > keep)
        .expect("oracle expected at least one out-of-bounds chunk offset");
    let new_eof_logical = (first_oob_chunk as u64) * chunk_length;
    let expected =
        expected_intersecting_keys((new_eof_logical, data_length), &positions, data_length);
    assert!(
        expected.len() > MAX_RESOLVED_KEYS,
        "this case is only meaningful above the cap: oracle computed {} intersecting \
         partitions, need > {MAX_RESOLVED_KEYS}",
        expected.len()
    );

    let report = run_verify(&staged).await;
    let finding = report
        .findings
        .iter()
        .find(|f| f.class == VerifyErrorClass::ChunkOffsetOutOfBounds)
        .unwrap_or_else(|| {
            panic!(
                "no ChunkOffsetOutOfBounds finding in {:#?}",
                report.findings
            )
        });
    let loc = finding
        .location
        .as_ref()
        .expect("ChunkOffsetOutOfBounds finding must carry a location");
    assert_eq!(
        loc.anchor,
        PhysicalAnchor::DeclaredRecord,
        "a truncation's physical anchor is a declared record, not a damaged extent"
    );
    let (keys, truncated) = capped_resolution(&loc.partitions);
    // AFFIRMATIVE evidence, not a bare pass: this case is only meaningful if it
    // really drove >100 intersecting partitions through the public surface, so
    // the measured counts are DISCLOSED rather than left to be inferred from a
    // green tick (the 0-rows-when-present trap, CLAUDE.md test doctrine).
    eprintln!(
        "L5.1 MEASURED: oracle intersecting={} materialized={} truncated={} (cap={})",
        expected.len(),
        keys.len(),
        truncated,
        MAX_RESOLVED_KEYS
    );

    // The cap holds AT the limit, the omitted count names the remainder
    // exactly, and the two together account for every intersecting partition
    // the independent oracle found — so a silently-dropped entry is caught.
    assert_eq!(
        keys.len(),
        MAX_RESOLVED_KEYS,
        "resolved set must materialize exactly MAX_RESOLVED_KEYS keys"
    );
    assert_eq!(
        truncated,
        expected.len() - MAX_RESOLVED_KEYS,
        "truncated count must name every intersecting partition not materialized"
    );
    // Every materialized key is one the oracle independently expects.
    let unexpected: Vec<&String> = keys.iter().filter(|k| !expected.contains(k)).collect();
    assert!(
        unexpected.is_empty(),
        "resolved set contains {} key(s) the independent oracle did not expect: {unexpected:?}",
        unexpected.len()
    );
}
