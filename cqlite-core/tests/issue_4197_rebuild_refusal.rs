//! Issue #4197 (spec R5) — rebuild REFUSES, writing nothing, when a
//! chunk-CRC check over `Data.db` fails, a partition fails to decode
//! structurally while walking it, or the boundary walk finds the SAME
//! partition key at two different on-disk offsets; and names `salvage`
//! (#4196) as the remedy.
//!
//! Oracle: `test_comp_corrupt/data_db_bit_flip` — a Cassandra-verified
//! corrupt fixture (`cassandra_verdict: corrupt`,
//! `corruption-manifest.yml`), captured against the REAL Apache Cassandra
//! 5.0.2 `sstableverify --extended --force` outcome. Not a synthetic
//! CQLite-fabricated corruption.
//!
//! The duplicate-partition-key case (roborev job 124) has no committed
//! fixture — no writer, Cassandra's or CQLite's, will produce one: both
//! reject a non-increasing `(token, key)` step at write time
//! (`SSTableWriter::write_partition` returns `InvalidInput`). It is
//! synthesized here by DOUBLING a healthy single-partition `Data.db`, which
//! yields exactly the on-disk shape Cassandra's own `Verifier` reports as
//! "Key out of order": the same key, twice, at two ascending offsets. The
//! authority for the shape is Cassandra's verifier contract, not CQLite's
//! own code; the fixture is merely the cheapest way to reach it.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, Component, RebuildOptions, RefusalReason,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{require_fixtures_strict, single_data_db, table_schema};

/// The CLEAN source the corruption corpus's `data_db_bit_flip` was derived
/// from — `test_comp.lz4_table`, whose whole `nb-1-big-*` component set is
/// git-tracked.
const CLEAN_KEYSPACE: &str = "test_comp";
const CLEAN_TABLE: &str = "lz4_table";

/// The chunk-aligned `Data.db` offset a refusal for `data_db_bit_flip` MUST
/// report, derived INDEPENDENTLY of rebuild (design.md §D6: "at the exact
/// chunk offset the clean source's `CompressionInfo.db` chunk table identifies
/// as flipped"):
///
///   1. byte-diff the corrupt `Data.db` against the clean committed source's —
///      the corpus's bit flip touches exactly one byte, and this asserts that;
///   2. parse the CLEAN `CompressionInfo.db` chunk-offset table (Cassandra
///      wrote it) and find the chunk whose compressed extent contains that
///      byte;
///   3. the expected refusal offset is that chunk's own start offset.
///
/// Returns `None` only when the clean source is unreachable, so the strong
/// assertion degrades to the weak one rather than silently vanishing.
fn expected_refusal_chunk_offset(corrupt_data_db: &Path) -> Option<(usize, u64)> {
    let clean_root = datasets_root::sstables_root_for_table(CLEAN_KEYSPACE, CLEAN_TABLE)?;
    let clean_dir = datasets_root::table_generation_dirs(&clean_root, CLEAN_KEYSPACE, CLEAN_TABLE)
        .into_iter()
        .next()?;
    let clean_data = std::fs::read(single_data_db(&clean_dir)).ok()?;
    let corrupt_data = std::fs::read(corrupt_data_db).ok()?;
    assert_eq!(
        clean_data.len(),
        corrupt_data.len(),
        "a BIT FLIP fixture must be the same length as its clean source; this fixture is not the \
         derivative this derivation assumes"
    );
    let flipped: Vec<usize> = (0..clean_data.len())
        .filter(|&i| clean_data[i] != corrupt_data[i])
        .collect();
    assert_eq!(
        flipped.len(),
        1,
        "data_db_bit_flip must differ from its clean source in exactly ONE byte; got {:?}",
        flipped
    );
    let flipped_at = flipped[0];

    let info_path = clean_dir.join(
        single_data_db(&clean_dir)
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.replace("Data.db", "CompressionInfo.db"))?,
    );
    let info = cqlite_core::storage::sstable::compression_info::CompressionInfo::parse(
        &std::fs::read(info_path).ok()?,
    )
    .ok()?;
    let (index, offset) = info
        .chunk_offsets
        .iter()
        .enumerate()
        .filter(|(_, &offset)| offset as usize <= flipped_at)
        .next_back()
        .map(|(i, &o)| (i, o))?;
    eprintln!(
        "[issue_4197] independent derivation: byte {flipped_at} differs from the clean source and \
         falls in chunk {index} (compressed offset {offset}) per the clean \
         CompressionInfo.db's own chunk table"
    );
    Some((index, offset))
}

/// The corruption corpus lives beside `sstables/` under
/// `corruption/test_comp_corrupt/<fixture>/`, not resolvable through
/// `datasets_root::sstables_root_for_table` (that helper walks
/// `sstables/<keyspace>/<table>-*`, a different tree shape). Resolve
/// directly against each dataset-root candidate instead.
fn corrupt_fixture_dir(name: &str) -> Option<PathBuf> {
    for sstables_root in datasets_root::sstables_root_candidates() {
        // `sstables_root_candidates` names paths ending in `.../sstables` —
        // `corruption/` is its SIBLING under the same dataset root, per
        // `fetch-datasets.sh`'s own layout.
        let Some(dataset_root) = sstables_root.parent() else {
            continue;
        };
        let candidate = dataset_root
            .join("corruption")
            .join("test_comp_corrupt")
            .join(name);
        if candidate.join("nb-1-big-Data.db").is_file() {
            return Some(candidate);
        }
    }
    None
}

#[tokio::test]
async fn rebuild_refuses_on_a_cassandra_verified_corrupt_data_db() {
    const FIXTURE: &str = "data_db_bit_flip";
    let Some(fixture_dir) = corrupt_fixture_dir(FIXTURE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the corruption fixture {FIXTURE} is absent under \
                 any dataset-root candidate: {:?}",
                datasets_root::sstables_root_candidates()
            );
        }
        eprintln!("[issue_4197] corruption fixture {FIXTURE} absent; skipping");
        return;
    };

    let schema = table_schema("compression-parity.cql", "lz4_table", "test_comp");
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let input_sha_before = sha256_of(&data_db);
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(&data_db, &schema, &[Component::Index], &options)
        .await
        .expect("rebuild_components returns Ok even when refusing (design D3)");

    let refusal = report
        .refused
        .unwrap_or_else(|| panic!("expected a refusal for a Cassandra-verified corrupt Data.db"));
    assert_eq!(refusal.reason, RefusalReason::DataCorrupt);
    assert!(
        refusal.remedy.to_lowercase().contains("salvage"),
        "remedy must name salvage (#4196); got: {}",
        refusal.remedy
    );
    assert!(
        report.regenerated.is_empty(),
        "a refused run must regenerate nothing; got {:?}",
        report.regenerated
    );

    // R5.1 — the EXACT chunk offset, not merely "a reason and a remedy".
    // Expected value derived independently from the clean committed source +
    // its own Cassandra-written CompressionInfo.db chunk table (design D6),
    // never from rebuild's own output.
    match expected_refusal_chunk_offset(&data_db) {
        Some((chunk_index, chunk_offset)) => {
            assert_eq!(
                refusal.offset,
                Some(chunk_offset),
                "the refusal must name the exact Data.db offset of the chunk the clean source's \
                 CompressionInfo.db identifies as flipped (chunk {chunk_index}); remedy={}",
                refusal.remedy
            );
            assert!(
                refusal
                    .remedy
                    .contains(&format!("chunk {chunk_index} failed CRC validation")),
                "the remedy must name the failing chunk INDEX so an operator can locate it; \
                 expected chunk {chunk_index}, got: {}",
                refusal.remedy
            );
            assert!(
                refusal.remedy.contains(&format!("{chunk_offset:#x}")),
                "the remedy must state the chunk offset in the same hex form the verifier uses \
                 ({chunk_offset:#x}); got: {}",
                refusal.remedy
            );
        }
        None => {
            // The clean source is git-tracked, so this is reachable only on a
            // broken checkout. Still assert an offset was reported at all —
            // never let the strong assertion vanish into nothing.
            assert!(
                refusal.offset.is_some(),
                "a data-corrupt refusal must always report the Data.db offset it was detected at"
            );
            eprintln!(
                "[issue_4197] WARNING: the clean source {CLEAN_KEYSPACE}.{CLEAN_TABLE} is \
                 unreachable, so the EXACT-offset assertion degraded to a presence check"
            );
        }
    }

    // R5.2 — the input is never modified, and nothing lands under --out.
    assert_eq!(
        input_sha_before,
        sha256_of(&data_db),
        "rebuild must never modify its input Data.db, even on refusal"
    );
    assert!(
        !out.exists()
            || std::fs::read_dir(&out)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
        "a refused run must write NOTHING to --out"
    );

    eprintln!(
        "[issue_4197] {FIXTURE}: rebuild correctly refused a Cassandra-verified corrupt \
         Data.db, naming salvage as the remedy: {}",
        refusal.remedy
    );
}

// ---------------------------------------------------------------------------
// Duplicate partition key at two offsets (roborev job 124)
// ---------------------------------------------------------------------------

const DUP_KEYSPACE: &str = "test_ks";
const DUP_TABLE: &str = "dup_key_table";
const DUP_TIMESTAMP: i64 = 1_759_713_125_977_357;

fn dup_schema() -> TableSchema {
    TableSchema {
        keyspace: DUP_KEYSPACE.to_string(),
        table: DUP_TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write a healthy generation holding exactly ONE partition, and return its
/// `Data.db` path (under `SSTableWriter`'s own `<keyspace>/<table>/` nesting).
fn write_single_partition_generation(dir: &Path) -> PathBuf {
    let schema = dup_schema();
    let mut writer = SSTableWriter::new(dir.to_path_buf(), 1, &schema).expect("writer");
    let mutation = Mutation::new(
        TableId::new(DUP_KEYSPACE, DUP_TABLE),
        PartitionKey::single("pk", Value::Integer(7)),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("only"),
        }],
        DUP_TIMESTAMP,
        None,
    );
    let key = mutation.decorated_key(&schema).expect("decorated key");
    writer
        .write_partition(key, vec![mutation])
        .expect("write_partition");
    let info = tokio::runtime::Handle::current()
        .block_on(async { writer.finish().await })
        .expect("finish");
    info.data_path
}

/// R5 / roborev job 124 — a `Data.db` in which one partition key appears at
/// TWO different on-disk offsets must be REFUSED as corrupt, never silently
/// under-enumerated (the pre-fix boundary walk deduped by raw key, so the
/// second occurrence vanished and every derived component was computed over a
/// partition set that does not match the file).
///
/// Narrow partitions on purpose: a single-row partition carries NO
/// promoted-index payload, so the `blocks.len() >= 2` re-encoded-span
/// cross-check never runs and cannot be what catches this. The refusal has to
/// come from the boundary walk itself.
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_refuses_a_data_db_with_one_partition_key_at_two_offsets() {
    let temp = TempDir::new().expect("tempdir");
    let reference = temp.path().join("reference");
    std::fs::create_dir_all(&reference).expect("create reference dir");
    let data_db = tokio::task::spawn_blocking({
        let reference = reference.clone();
        move || write_single_partition_generation(&reference)
    })
    .await
    .expect("join");
    let generation = data_db.parent().expect("generation dir").to_path_buf();

    // Double the data section: the SAME partition, byte for byte, at offset 0
    // and again at `len`. An `nb` Data.db is headerless (the serialization
    // header lives in Statistics.db), so a concatenation of two partition
    // extents is a well-formed two-partition data section.
    let one = std::fs::read(&data_db).expect("read Data.db");
    assert!(
        !one.is_empty(),
        "the writer must have produced a non-empty Data.db"
    );
    let mut doubled = one.clone();
    doubled.extend_from_slice(&one);
    std::fs::write(&data_db, &doubled).expect("write doubled Data.db");

    // Delete the Index.db so the request is the tool's headline use case, and
    // the CRC.db/Digest.crc32 the ORIGINAL (undoubled) Data.db was checksummed
    // against — otherwise the uncompressed read path's own CRC.db check refuses
    // first, on a stale checksum, and this test would never reach the boundary
    // walk it exists to exercise (verified: it refused with "uncompressed CRC32
    // mismatch for chunk 0" before these two lines were added).
    let prefix = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Data.db name")
        .trim_end_matches("Data.db")
        .to_string();
    for stale in ["Index.db", "CRC.db", "Digest.crc32"] {
        let _ = std::fs::remove_file(generation.join(format!("{prefix}{stale}")));
    }

    let out = temp.path().join("out");
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(&data_db, &dup_schema(), &[Component::Index], &options)
        .await
        .expect("a corrupt Data.db is a REFUSAL (report.refused), never an Err");

    let refusal = report.refused.as_ref().unwrap_or_else(|| {
        panic!(
            "a Data.db carrying one partition key at two offsets must be refused, not \
             silently under-enumerated; report={report:?}"
        )
    });
    assert_eq!(
        refusal.reason,
        RefusalReason::DataCorrupt,
        "a repeated partition key is Data.db corruption (Cassandra's Verifier: \"Key out of \
         order\"), not an unreproducible encoding; {refusal:?}"
    );
    assert_eq!(
        refusal.offset,
        Some(one.len() as u64),
        "the refusal must name the offset of the REPEAT (the second occurrence), which is \
         where the original single-partition extent ended; remedy={}",
        refusal.remedy
    );
    assert!(
        refusal.remedy.to_lowercase().contains("salvage"),
        "remedy must name salvage (#4196); got: {}",
        refusal.remedy
    );
    assert!(
        report.regenerated.is_empty(),
        "a refused run must regenerate nothing; got {:?}",
        report.regenerated
    );
    assert!(
        !out.join(format!("{prefix}Index.db")).exists(),
        "a refused run must leave no partially-streamed Index.db behind"
    );
}

fn sha256_of(path: &Path) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    Sha256::digest(bytes).to_vec()
}
