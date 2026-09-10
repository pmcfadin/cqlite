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
    salvage_sstable, LossClass, RefusalReason, SalvageOptions,
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

    let clean_positions = fixture::index_partition_positions(&clean_dir);
    let expected_lost: BTreeSet<String> = clean_positions
        .iter()
        .filter(|(_key, pos)| *pos / ci.chunk_length as usize == bad_chunk)
        .map(|(key, _pos)| hex_of(key))
        .collect();
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
        !out_root.exists()
            || std::fs::read_dir(&out_root)
                .map(|rd| rd
                    .flatten()
                    .all(|e| !e.file_name().to_string_lossy().ends_with("-Data.db")))
                .unwrap_or(true),
        "--out must contain no Data.db after a refusal"
    );

    eprintln!("[issue_4196] index_db_bit_flip_big: salvage refused as expected ({refusal:?}).");
}
