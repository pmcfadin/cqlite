//! Issue #4196 (spec R2, R4) — for a damaged input the set of lost partitions
//! equals the set the format itself says is untrustworthy, and a damaged
//! BOUNDARY SOURCE refuses rather than resyncs.
//!
//! # Oracle (design D6)
//!
//! `test-data/datasets/corruption/test_comp_corrupt/*` — real Cassandra 5.0
//! fixtures with exactly ONE component byte-flipped/truncated, captured
//! alongside Cassandra's OWN `sstableverify` verdict
//! (`corruption-manifest.yml`, issue #1236/#999). The expected loss set is
//! computed HERE, independently, from the CLEAN source's `Index.db` entries
//! (`corrupt_byte_fixture::index_partition_positions`, an on-disk-format
//! walk, not code under test) and `CompressionInfo.db`'s chunk table — never
//! from `salvage_sstable`'s own behaviour.
//!
//! Skip-clean when the corruption corpus is absent; `CQLITE_REQUIRE_FIXTURES=1`
//! (#1094 doctrine) turns that into a hard failure.

// `not(tombstones)`: see the matching note in
// `issue_4196_salvage_healthy_parity.rs`.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::storage::write_engine::salvage::{
    salvage_sstable, LossClass, RefusalReason, SalvageOptions, SalvageReport,
};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;

const CLEAN_KEYSPACE: &str = "test_comp";
const CORRUPT_KEYSPACE: &str = "test_comp_corrupt";
const CLEAN_TABLE_DIR: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";
const SCHEMA_FILE: &str = "compression-parity.cql";
const TABLE: &str = "lz4_table";

fn require_fixtures_strict() -> bool {
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

/// The first candidate base root that actually carries BOTH the clean source
/// and the named corrupt fixture (issue #3220: never bind to a root that
/// holds one but not the other and silently skip).
fn resolve_root_with_corpus_fixture(corrupt_fixture: &str) -> Option<PathBuf> {
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

fn table_schema() -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(SCHEMA_FILE).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE}"))
        .unwrap_or_else(|| {
            cql.find(&format!("CREATE TABLE {TABLE}"))
                .expect("CREATE TABLE statement")
        });
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = CLEAN_KEYSPACE.to_string();
    t
}

fn skip_or_require(what: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
    true
}

/// `true` iff `dir` is present and carries a `*-Data.db`.
fn usable(dir: &Path) -> bool {
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

fn single_data_db(dir: &Path) -> PathBuf {
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
fn no_data_db_anywhere(dir: &Path) -> bool {
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

fn hex_of(b: &[u8]) -> String {
    hex::encode(b)
}

/// The chunk index (compressed-domain `CompressionInfo.chunk_offsets`)
/// containing `compressed_byte_offset`.
fn compressed_chunk_index(chunk_offsets: &[u64], compressed_byte_offset: u64) -> usize {
    chunk_offsets
        .iter()
        .rposition(|&off| off <= compressed_byte_offset)
        .expect("byte offset precedes every chunk start")
}

/// R2.1 — compressed chunk CRC flip: the lost partition set is exactly the
/// partitions whose decompressed-domain offset falls in the flipped chunk.
#[tokio::test]
async fn compressed_chunk_crc_flip_loses_exactly_the_intersecting_partitions() {
    let Some(root) = resolve_root_with_corpus_fixture("data_db_bit_flip") else {
        skip_or_require(
            "data_db_bit_flip corpus fixture",
            &format!(
                "no candidate root carries BOTH sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR} and \
                 corruption/{CORRUPT_KEYSPACE}/data_db_bit_flip; searched {:?}",
                candidate_base_roots()
            ),
        );
        return;
    };
    let clean_dir = root
        .join("sstables")
        .join(CLEAN_KEYSPACE)
        .join(CLEAN_TABLE_DIR);
    let corrupt_dir = root
        .join("corruption")
        .join(CORRUPT_KEYSPACE)
        .join("data_db_bit_flip");

    // Independent expected-loss computation (design D6): CompressionInfo.db's
    // chunk table + Index.db's partition positions, from the CLEAN source.
    let ci_bytes = std::fs::read(clean_dir.join("nb-1-big-CompressionInfo.db"))
        .expect("read CompressionInfo.db");
    let ci = CompressionInfo::parse(&ci_bytes).expect("parse CompressionInfo.db");
    // PINNED constant, not derived on this run: `corruption-manifest.yml`'s
    // `data_db_bit_flip` entry records `byte_offset: 64` for the
    // ORIGINAL/CORRUPTED bytes it captured (`original_sha256`/
    // `corrupted_sha256`) — a fact about THAT committed corpus generation,
    // not something this test parses at run time. If the corpus is ever
    // regenerated with a different mutation site, this constant (and the
    // manifest's `byte_offset` field) must be updated together; a
    // divergence would be caught by `expected_lost` coming back empty below
    // (the assertion immediately following), not silently.
    let manifest_byte_offset: u64 = 64;
    let bad_chunk = compressed_chunk_index(&ci.chunk_offsets, manifest_byte_offset);

    let mut clean_positions = fixture::index_partition_positions(&clean_dir);
    clean_positions.sort_by_key(|(_, pos)| *pos);
    // Range-based expected-loss derivation (roborev, issue #4196, round-5
    // Low finding 6): a partition is lost if its BYTE RANGE intersects the
    // bad chunk, not merely if its START offset does — matching
    // `chunks_for_range`'s own intersection rule (`chunks.rs`), which is
    // what `salvage_sstable` actually applies. A start-offset-only
    // membership test happens to agree with range intersection on THIS
    // fixture (exactly one partition) but would silently diverge on a
    // multi-partition fixture where a partition starts in one chunk and
    // extends into the bad one.
    let chunk_length = ci.chunk_length as usize;
    let mut expected_lost: BTreeSet<String> = BTreeSet::new();
    for i in 0..clean_positions.len() {
        let (key, pos) = &clean_positions[i];
        let end = clean_positions
            .get(i + 1)
            .map(|(_, next_pos)| *next_pos)
            .unwrap_or(ci.data_length as usize);
        let start_chunk = pos / chunk_length;
        let last_byte = end.saturating_sub(1).max(*pos);
        let end_chunk = last_byte / chunk_length;
        if (start_chunk..=end_chunk).contains(&bad_chunk) {
            expected_lost.insert(hex_of(key));
        }
    }
    assert!(
        !expected_lost.is_empty(),
        "expected-loss computation found zero intersecting partitions for chunk {bad_chunk} — \
         the fixture or the derivation changed"
    );

    let schema = table_schema();
    let corrupt_data_db = single_data_db(&corrupt_dir);
    let temp = TempDir::new().expect("tempdir");
    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage must not error on a damaged Data.db (a classified loss, not an Err)");

    // This corpus fixture (`test_comp.lz4_table`) happens to hold exactly ONE
    // partition, whose decompressed offset (0) falls inside the flipped
    // chunk — so the independently-derived expected-loss set can be EITHER a
    // proper subset (partial recovery, spec R2.1) or the WHOLE partition set
    // (total loss, spec R5.2's "nothing decodable refuses"), and the correct
    // refusal state follows from which. Both are asserted here rather than
    // assuming the corpus always demonstrates the partial case.
    if expected_lost.len() == clean_positions.len() {
        let refusal = report
            .refused
            .as_ref()
            .expect("every partition lost must REFUSE (spec R5.2)");
        assert_eq!(refusal.reason, RefusalReason::NothingDecodable);
    } else {
        assert!(
            report.refused.is_none(),
            "a partial chunk-CRC loss must not REFUSE; got {:?}",
            report.refused
        );
    }
    assert_eq!(
        report.partitions.total,
        clean_positions.len(),
        "boundary enumeration total must equal the clean Index.db's partition count \
         (Index.db is untouched by this fixture)"
    );

    let actual_lost: BTreeSet<String> = report.losses.iter().map(|l| l.key_hex.clone()).collect();
    assert_eq!(
        actual_lost, expected_lost,
        "lost-partition key set differs from the independently-derived chunk-intersection set"
    );
    for loss in &report.losses {
        assert_eq!(
            loss.class,
            LossClass::ChunkCrc,
            "every loss from a chunk-CRC-only corruption must classify chunk-crc; got {:?} for \
             key {}",
            loss.class,
            loss.key_hex
        );
    }
    assert_eq!(
        report.partitions.recovered,
        report.partitions.total - report.losses.len()
    );

    assert!(
        report
            .component_findings
            .iter()
            .any(|f| f.class == "ChunkDecompressionError"),
        "component_findings must name ChunkDecompressionError; got {:?}",
        report.component_findings
    );

    eprintln!(
        "[issue_4196] data_db_bit_flip: {} of {} partition(s) lost to chunk {bad_chunk} \
         (independently derived), matching salvage's own loss set exactly.",
        report.losses.len(),
        report.partitions.total
    );
}

/// R4.1 — a damaged boundary source (`Index.db`) refuses; no `Data.db` is
/// written under `--out`.
#[tokio::test]
async fn damaged_index_db_refuses_with_the_rebuild_remedy() {
    let Some(root) = resolve_root_with_corpus_fixture("index_db_bit_flip_big") else {
        skip_or_require(
            "index_db_bit_flip_big corpus fixture",
            &format!(
                "no candidate root carries BOTH sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR} and \
                 corruption/{CORRUPT_KEYSPACE}/index_db_bit_flip_big; searched {:?}",
                candidate_base_roots()
            ),
        );
        return;
    };
    let corrupt_dir = root
        .join("corruption")
        .join(CORRUPT_KEYSPACE)
        .join("index_db_bit_flip_big");

    let schema = table_schema();
    let corrupt_data_db = single_data_db(&corrupt_dir);
    let temp = TempDir::new().expect("tempdir");
    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage must not error on a damaged boundary source (a Refusal, not an Err)");

    let refusal = report
        .refused
        .as_ref()
        .expect("a damaged Index.db must produce a refusal");
    assert_eq!(refusal.reason, RefusalReason::BoundarySourceUnreadable);
    assert!(
        refusal.remedy.contains("rebuild"),
        "remedy must name `rebuild`; got {:?}",
        refusal.remedy
    );
    assert!(
        no_data_db_anywhere(&out_root),
        "--out must contain no Data.db after a refusal"
    );

    eprintln!("[issue_4196] index_db_bit_flip_big: salvage refused as expected ({refusal:?}).");
}

/// Roborev, issue #4196 (batched finding b) — a component OTHER than the
/// boundary source being unreadable (`CompressionInfo.db`, `Statistics.db`)
/// must produce a classified [`RefusalReason::ComponentUnreadable`] with a
/// remedy, not a hard `Err` `salvage_sstable` callers previously had to
/// `?`-propagate (which meant NO manifest was ever produced for one of the
/// likeliest damage modes this tool exists for). Shared by both fixtures
/// below: assert the classified-refusal shape and that `--out` holds no
/// `Data.db`, WITHOUT asserting a specific remedy component name (design D3
/// does not specify one — only that the reason and remedy are both named).
async fn assert_component_unreadable_refusal(
    corrupt_fixture: &str,
    out_root: &std::path::Path,
) -> SalvageReport {
    let root = resolve_root_with_corpus_fixture(corrupt_fixture)
        .unwrap_or_else(|| panic!("caller must have already skip-checked {corrupt_fixture}"));
    let corrupt_dir = root
        .join("corruption")
        .join(CORRUPT_KEYSPACE)
        .join(corrupt_fixture);
    let schema = table_schema();
    let corrupt_data_db = single_data_db(&corrupt_dir);
    let report = salvage_sstable(
        &corrupt_data_db,
        out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on a damaged {corrupt_fixture} component (a \
                 classified Refusal, not an Err): {e:#}"
        )
    });

    let refusal = report.refused.as_ref().unwrap_or_else(|| {
        panic!("a damaged non-boundary component must still produce a refusal; report={report:?}")
    });
    assert_eq!(
        refusal.reason,
        RefusalReason::ComponentUnreadable,
        "expected ComponentUnreadable (the boundary source itself is untouched by this fixture); \
         got {:?}",
        refusal.reason
    );
    assert!(
        !refusal.remedy.is_empty(),
        "remedy must be named, not empty"
    );
    assert!(
        no_data_db_anywhere(out_root),
        "--out must contain no Data.db after a refusal"
    );
    report
}

/// A corrupt `CompressionInfo.db` (bad chunk-table offset) is a component
/// failure salvage meets while OPENING the reader (compressed-input chunk
/// metadata is read eagerly) — must classify, not hard-error.
#[tokio::test]
async fn damaged_compression_info_db_refuses_as_classified() {
    if resolve_root_with_corpus_fixture("compression_info_bad_offset").is_none() {
        skip_or_require(
            "compression_info_bad_offset corpus fixture",
            &format!(
                "no candidate root carries BOTH sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR} and \
                 corruption/{CORRUPT_KEYSPACE}/compression_info_bad_offset; searched {:?}",
                candidate_base_roots()
            ),
        );
        return;
    }
    let temp = TempDir::new().expect("tempdir");
    let out_root = temp.path().join("out");
    let report =
        assert_component_unreadable_refusal("compression_info_bad_offset", &out_root).await;
    eprintln!(
        "[issue_4196] compression_info_bad_offset: salvage refused as expected ({report:?})."
    );
}

/// A corrupt `Statistics.db` header is a component failure salvage meets
/// while classifying the input's repair state (`classify_inputs`) — must
/// classify, not hard-error.
#[tokio::test]
async fn damaged_statistics_db_refuses_as_classified() {
    if resolve_root_with_corpus_fixture("statistics_db_header_damage").is_none() {
        skip_or_require(
            "statistics_db_header_damage corpus fixture",
            &format!(
                "no candidate root carries BOTH sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR} and \
                 corruption/{CORRUPT_KEYSPACE}/statistics_db_header_damage; searched {:?}",
                candidate_base_roots()
            ),
        );
        return;
    }
    let temp = TempDir::new().expect("tempdir");
    let out_root = temp.path().join("out");
    let report =
        assert_component_unreadable_refusal("statistics_db_header_damage", &out_root).await;
    eprintln!(
        "[issue_4196] statistics_db_header_damage: salvage refused as expected ({report:?})."
    );
}

/// Roborev, issue #4196 (round-4 Medium finding 3) — the SAME
/// `ComponentUnreadable` classification for a corrupt `CRC.db` (the
/// uncompressed sibling of `CompressionInfo.db`/`Statistics.db` above): the
/// committed corruption corpus has no dedicated `CRC.db`-corruption fixture
/// (`digest_crc32_mismatch` is a DIFFERENT component, the whole-file
/// `Digest.crc32`, not the per-chunk `CRC.db` sidecar `uncompressed_chunk_preflight`
/// reads), so this test synthesizes the corruption itself: a healthy
/// `test_basic.uncompressed_table` generation copied verbatim except its
/// `CRC.db`, replaced with 2 garbage bytes — `CrcDb::open` rejects anything
/// under its mandatory 4-byte chunk-size header with a typed
/// `Error::Corruption`, guaranteed regardless of chunk-size/content.
#[tokio::test]
async fn damaged_crc_db_refuses_as_classified() {
    const KEYSPACE: &str = "test_basic";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));

    let schema_path = datasets_root::schema_path("basic-types.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    std::fs::create_dir_all(&corrupt_dir).expect("create corrupt input dir");
    for entry in std::fs::read_dir(&fixture_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-CRC.db") {
            // The corruption under test: too short for CrcDb::open's
            // mandatory 4-byte chunk-size header.
            std::fs::write(corrupt_dir.join(&name), [0xff, 0x00]).expect("write corrupt CRC.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on a damaged CRC.db (a classified Refusal, not an \
                 Err): {e:#}"
        )
    });
    let refusal = report.refused.as_ref().unwrap_or_else(|| {
        panic!("a damaged CRC.db must still produce a refusal; report={report:?}")
    });
    assert_eq!(
        refusal.reason,
        RefusalReason::ComponentUnreadable,
        "expected ComponentUnreadable; got {:?}",
        refusal.reason
    );
    assert!(
        !refusal.remedy.is_empty(),
        "remedy must be named, not empty"
    );
    assert!(
        no_data_db_anywhere(&out_root),
        "--out must contain no Data.db after a refusal"
    );
    eprintln!("[issue_4196] damaged CRC.db: salvage refused as expected ({report:?}).");
}

/// Roborev, issue #4196 (round-9, spec R2.3 — `LossClass::Truncated` had no
/// test anywhere in the change: the corpus's ONLY truncation fixture,
/// `data_db_truncation`, is `test_comp.lz4_table` (COMPRESSED) — measured
/// (round 9) to classify `chunk-crc`, NOT `truncated`: `compressed_chunk_preflight`
/// walks every declared chunk BEFORE the per-partition loop ever runs, and a
/// chunk whose bytes no longer exist past the truncation point fails to
/// READ at all, landing in `bad_chunks` — the per-partition loop's
/// `chunk-crc` short-circuit fires before `recover_one_partition` is ever
/// reached, so `LossClass::Truncated`'s own code path is unreachable for a
/// COMPRESSED truncation, and a NAIVE uncompressed byte-truncation has the
/// SAME problem one layer down: truncating mid-row makes the row parser
/// return a hard decode `Err` (measured: "row_size=317 ... exceeds
/// available data"), landing in `PartitionAtOffsetOutcome::DecodeError` →
/// `LossClass::Decode`, not `Truncated` — `Truncated` is reached ONLY via
/// `decode_partition_at_offset_for_salvage`'s EARLY `offset_usize >= end`
/// check (`point_compaction.rs`), before any parsing is attempted at all.
///
/// This test therefore does NOT truncate `Data.db` — it leaves a healthy
/// `test_comp.uncompressed_table` generation's `Data.db`/`CRC.db` byte-for-
/// byte UNCHANGED (so the chunk-CRC pre-flight finds nothing to flag at
/// all: zero `bad_chunks`, zero `component_findings`) and instead
/// re-encodes a COPY of `Index.db` with its ONE entry's `data_offset` VInt
/// field changed from its true value (`0`) to `200000` — comfortably past
/// the real, unmutated `Data.db`'s 195018-byte length — while leaving the
/// entry's key bytes and the promoted-index payload untouched byte-for-byte
/// (`parse_big_index_entry`'s layout,
/// `cqlite-core/src/storage/sstable/index_reader/parse.rs`:
/// `[key_len: u16][key][data_offset: vint][promoted_len: vint][promoted]`).
/// `enumerate_boundaries`'s `check_strictly_ascending` guard trivially
/// passes (a single-entry list has no adjacent pair to violate), and
/// `decode_partition_at_offset_for_salvage`'s uncompressed branch then hits
/// `offset_usize(200000) >= end(195018 == section_len, since this is the
/// only/last entry)` on the FIRST line, before a single byte of `Data.db`
/// is ever read — the cleanest, most deterministic route to `Truncated`
/// this codebase has, and the reason NO Data.db mutation is needed at all.
///
/// R2.3's "all earlier partitions are recovered" narrows to the same
/// single-partition illustration `data_db_bit_flip`'s test already accepted
/// in round 6 (this fixture's Index.db, like `lz4_table`'s, names exactly
/// one partition) — the classification/derivation logic under test is
/// exercised identically regardless of partition count.
#[tokio::test]
async fn index_entry_offset_past_eof_classifies_truncated() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));

    let schema_path =
        datasets_root::schema_path("compression-parity.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    let clean_data_db = single_data_db(&fixture_dir);
    let clean_data_len = std::fs::metadata(&clean_data_db)
        .expect("stat clean Data.db")
        .len();
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");

    // Rebuild the ONE entry with a fabricated `data_offset` comfortably past
    // EOF, keeping the key + promoted-index payload byte-for-byte. Parsed
    // structurally (not hardcoded byte offsets) so this test does not
    // silently mis-corrupt a different field if the fixture is regenerated
    // with a differently-sized key.
    let key_len = u16::from_be_bytes([clean_index_bytes[0], clean_index_bytes[1]]) as usize;
    let key_end = 2 + key_len;
    let (_orig_offset, consumed) =
        cqlite_core::parser::vint::decode_unsigned(&clean_index_bytes[key_end..])
            .expect("clean Index.db entry has a well-formed data_offset VInt");
    let rest_after_offset = &clean_index_bytes[key_end + consumed..];
    const FABRICATED_OFFSET: u64 = 200_000;
    assert!(
        FABRICATED_OFFSET > clean_data_len,
        "{KEYSPACE}.{TABLE_NAME}: fabricated offset {FABRICATED_OFFSET} must exceed the clean \
         Data.db's real length {clean_data_len}, or this test proves nothing"
    );
    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len() + 8);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..key_end]);
    cqlite_core::storage::serialization::vint::encode_unsigned(
        FABRICATED_OFFSET,
        &mut corrupt_index_bytes,
    );
    corrupt_index_bytes.extend_from_slice(rest_after_offset);

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    std::fs::create_dir_all(&corrupt_dir).expect("create corrupt input dir");
    for entry in std::fs::read_dir(&fixture_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-Index.db") {
            std::fs::write(corrupt_dir.join(&name), &corrupt_index_bytes)
                .expect("write corrupted Index.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on an out-of-range Index.db offset (a Loss, not an \
             Err): {e:#}"
        )
    });

    assert_eq!(
        report.partitions.total, 1,
        "{KEYSPACE}.{TABLE_NAME}: expected exactly one partition in this fixture's Index.db; \
         got {}",
        report.partitions.total
    );
    assert_eq!(
        report.losses.len(),
        1,
        "expected exactly one loss (the file's only partition); got {:?}",
        report.losses
    );
    assert_eq!(
        report.losses[0].class,
        LossClass::Truncated,
        "expected LossClass::Truncated; got {:?} (component findings: {:?})",
        report.losses[0],
        report.component_findings
    );
    assert!(
        report.component_findings.is_empty(),
        "Data.db/CRC.db were never touched — no chunk-CRC finding should fire; got {:?}",
        report.component_findings
    );
    assert!(
        no_data_db_anywhere(&out_root),
        "--out must contain no Data.db when the file's only partition is a total loss"
    );
    eprintln!(
        "[issue_4196] Index.db offset past EOF: salvage classified the loss as {:?} as \
         expected.",
        report.losses[0].class
    );
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
fn split_big_index_entries(bytes: &[u8]) -> Vec<(usize, usize, usize)> {
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

/// Roborev, issue #4196 (round-9, spec R4.2 — `LossClass::KeyMismatch` had
/// no test anywhere in the change).
///
/// Spec R4.2's literal wording ("a temp copy of a healthy BIG fixture with
/// ONE Index.db entry's POSITION pointed at a DIFFERENT partition's
/// header") describes redirecting an entry's `data_offset`. That construction
/// is geometrically IMPOSSIBLE against a well-formed, `check_strictly_ascending`-
/// enforced boundary source (`boundaries.rs`): that guard requires the WHOLE
/// entry sequence strictly increasing in `data_offset`, so no two entries can
/// EVER share a value, and any single entry's redirected offset is bounded
/// by its OWN immediate neighbours — i.e. it can only be moved somewhere
/// inside the numeric gap `(entries[i-1].data_offset, entries[i+1].data_offset)`,
/// which is EXACTLY where partition `i`'s own true header already lives, and
/// nowhere else. There is no position a redirected offset can occupy that
/// both satisfies strict ascending AND lands on a DIFFERENT, separately-
/// enumerated partition's real header. (Checked at both the first and last
/// entry too: entry 0 has no lower-bound neighbour but is upper-bounded by
/// entry 1 — nothing precedes partition 0 in the data section, so that gap
/// contains only partition 0's own header; the last entry has no upper
/// bound but IS lower-bounded by its predecessor, which — being the
/// SECOND-TO-LAST entry — leaves no smaller, already-enumerated partition's
/// header still reachable above it either.)
///
/// This test instead achieves the IDENTICAL decoder-observable property the
/// spec scenario exists to exercise — `decode_partition_at_offset_for_salvage`
/// finds a key AT THE GIVEN, VALID, in-range OFFSET that disagrees with the
/// boundary source's DECLARED key for that slot — via the functionally
/// equivalent construction of swapping two entries' KEY portions instead
/// (offsets are NEVER touched, so `check_strictly_ascending` sees the
/// UNMODIFIED, still-valid clean sequence and never refuses). Entry 0 and
/// entry 1's `[key_len][key]` byte spans are swapped in a copy of
/// `test_basic.multi_partition_table`'s Index.db (a real, ~90-partition
/// compressed BIG fixture, `basic-types.cql`); `Data.db`/`CompressionInfo.db`
/// stay byte-for-byte unchanged. Both swapped slots decode their TRUE
/// partition (offsets untouched) against the WRONG declared key -> BOTH
/// classify `key-mismatch`. A THIRD, entirely untouched entry (entry 2)
/// proves the spec's "the [other] partition is still recovered from its own
/// index entry exactly once" property: an unrelated boundary-source
/// corruption does not disturb a partition it never touched.
#[tokio::test]
async fn swapped_index_entry_keys_classify_key_mismatch() {
    const KEYSPACE: &str = "test_basic";
    const TABLE_NAME: &str = "multi_partition_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "multi_partition_table fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));

    let schema_path = datasets_root::schema_path("basic-types.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    let clean_data_db = single_data_db(&fixture_dir);
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");
    let entries = split_big_index_entries(&clean_index_bytes);
    assert!(
        entries.len() >= 3,
        "{KEYSPACE}.{TABLE_NAME}: need at least 3 partitions (two to swap, one untouched \
         control); found {}",
        entries.len()
    );
    let (e0_start, e0_key_end, e0_end) = entries[0];
    let (e1_start, e1_key_end, e1_end) = entries[1];
    assert_eq!(
        e0_end, e1_start,
        "entries[0] and entries[1] must be adjacent for this splice to be a pure key swap"
    );

    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len());
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..e0_start]);
    // entry 0's slot: entry 1's key + entry 0's own (unchanged) offset/promoted-index.
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[e1_start..e1_key_end]);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[e0_key_end..e0_end]);
    // entry 1's slot: entry 0's key + entry 1's own (unchanged) offset/promoted-index.
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[e0_start..e0_key_end]);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[e1_key_end..e1_end]);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[e1_end..]);
    assert_eq!(
        corrupt_index_bytes.len(),
        clean_index_bytes.len(),
        "a pure key swap must not change the file's total length"
    );

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    std::fs::create_dir_all(&corrupt_dir).expect("create corrupt input dir");
    for entry in std::fs::read_dir(&fixture_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-Index.db") {
            std::fs::write(corrupt_dir.join(&name), &corrupt_index_bytes)
                .expect("write corrupted Index.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!("salvage must not hard-error on swapped Index.db keys (a Loss, not an Err): {e:#}")
    });

    let total = entries.len();
    assert_eq!(
        report.partitions.total, total,
        "expected the untouched partition count; got {}",
        report.partitions.total
    );
    assert_eq!(
        report.losses.len(),
        2,
        "expected exactly the two swapped slots as losses; got {:?}",
        report.losses
    );
    for loss in &report.losses {
        assert_eq!(
            loss.class,
            LossClass::KeyMismatch,
            "expected LossClass::KeyMismatch for the swapped slots; got {:?}",
            loss
        );
    }
    // R4.2's "the [other] partition is still recovered from its own index
    // entry exactly once": every partition OTHER than the two swapped slots
    // must be untouched, so `recovered` accounts for exactly `total - 2`.
    assert_eq!(
        report.partitions.recovered,
        total - 2,
        "every UNSWAPPED partition must still recover cleanly from its own, untouched entry"
    );
    assert!(
        report.refused.is_none(),
        "a partial loss on 2 of {total} partitions must not refuse the whole generation; got \
         {:?}",
        report.refused
    );
    eprintln!(
        "[issue_4196] swapped Index.db entry keys: {} of {total} partitions classified \
         key-mismatch as expected; {} recovered.",
        report.losses.len(),
        report.partitions.recovered
    );
}
