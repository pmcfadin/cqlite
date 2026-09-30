//! Issue #4197 (spec R2/R4.1) — rebuild derives promoted-index byte offsets
//! against the `SerializationHeader.EncodingStats` baseline the input was
//! ACTUALLY encoded with, recovered from its own `Statistics.db`; and when
//! that baseline is unrecoverable it REFUSES rather than shipping desynced
//! offsets.
//!
//! # Why this fixture is synthesized rather than taken from the corpus
//!
//! The defect being pinned needs an SSTable whose TRUE baseline is LOWER
//! than anything derivable from its own decodable content. Every committed
//! fixture has the two coinciding (no tombstones, no TTLs, no inherited
//! minima), which is exactly why
//! `issue_4197_rebuild_index_parity.rs` passed while the offsets were being
//! measured against a re-derived baseline.
//!
//! That state is not exotic — it is what Cassandra PRODUCES at compaction:
//! `SerializationHeader.make(metadata, sstables)` merges the input SSTables'
//! `EncodingStats` (`EncodingStats.merge`, `cassandra-5.0.8`), so the output's
//! baseline is the minimum across INPUTS, not a recount of the rows that
//! survived the merge. A row carrying the minimum can also be shadow-dropped
//! by reconciliation before any reader sees it. CQLite reaches the same state
//! through the public `SSTableWriter::pre_seed_encoding_baselines` (the #729
//! two-pass flush API), which is what this file uses.
//!
//! # Oracle
//!
//! The ORIGINAL `Index.db` of the same generation — written by the writer
//! that also wrote `Data.db`, from the promoted-index blocks it measured
//! while emitting those exact bytes. This is a consistency oracle over ONE
//! encoding (does rebuild reproduce the offsets the file was written with?),
//! not a round-trip over a shared framing assumption: a framing error common
//! to CQLite's reader and writer would leave both sides of this comparison
//! equal and the test green, which is precisely why the Cassandra-written
//! corpus parity test in `issue_4197_rebuild_index_parity.rs` stays the
//! authority on framing (`docs/development/test-oracles.md` §2).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, Component, RebuildOptions, RefusalReason,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KEYSPACE: &str = "test_ks";
const TABLE: &str = "baseline_table";

/// Every row's write timestamp. The whole point of the fixture is that the
/// file's baseline sits far BELOW this.
const ROW_TIMESTAMP: i64 = 1_759_713_125_977_357;

/// How far below the content minimum the fixture's real baseline sits. Large
/// enough that the per-row timestamp delta changes VInt WIDTH (a delta of 0
/// costs 1 byte; a delta of 10^9 costs 5), so the promoted-index block
/// boundaries genuinely move when the wrong baseline is used — a difference
/// of a few micros would be absorbed inside the same VInt width and the test
/// would be vacuous.
const INHERITED_BASELINE_GAP: i64 = 1_000_000_000;

/// Partitions in the synthetic generation. Each one is wide enough (see
/// [`BLOB_LEN`] × [`ROWS_PER_PARTITION`]) to cross Cassandra's 64 KiB
/// `column_index_size` threshold, so `Index.db` carries a real promoted-index
/// payload — the ONLY part of `Index.db` whose bytes depend on the encoding
/// baseline at all (a narrow partition's entry is just key + data offset,
/// both baseline-independent).
const PARTITIONS: i32 = 3;
const ROWS_PER_PARTITION: i32 = 40;
const BLOB_LEN: usize = 2048;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![cqlite_core::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: cqlite_core::schema::ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "blob".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn row(pk: i32, ck: i32) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(
            cqlite_core::storage::write_engine::mutation::ClusteringKey::single(
                "ck",
                Value::Integer(ck),
            ),
        ),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Blob(vec![(pk as u8).wrapping_add(ck as u8); BLOB_LEN].into()),
        }],
        ROW_TIMESTAMP,
        None,
    )
}

/// Write a complete, self-consistent generation whose `EncodingStats`
/// baseline is [`INHERITED_BASELINE_GAP`] micros BELOW every timestamp
/// actually present in it, and return its `Data.db` path (which sits under
/// `SSTableWriter`'s own `<keyspace>/<table>/` nesting, NOT directly in
/// `dir`).
fn write_inherited_baseline_generation(dir: &Path) -> PathBuf {
    let schema = schema();
    let mut writer = SSTableWriter::new(dir.to_path_buf(), 1, &schema).expect("writer");
    // The #729 two-pass API: locks the whole-SSTable delta-encoding baseline
    // BEFORE any partition is written, exactly as a compaction that inherited
    // a lower minimum from its inputs would. `i32::MAX` for LDT/TTL is the
    // ordinary "no deletions, no TTLs" sentinel this content would produce
    // anyway — only the timestamp baseline is deliberately lowered.
    writer.pre_seed_encoding_baselines(ROW_TIMESTAMP - INHERITED_BASELINE_GAP, i32::MAX, i32::MAX);

    // Partitions must be written in ascending TOKEN order.
    let mut partitions: Vec<(
        cqlite_core::storage::write_engine::mutation::DecoratedKey,
        i32,
    )> = (0..PARTITIONS)
        .map(|pk| {
            (
                row(pk, 0).decorated_key(&schema).expect("decorated key"),
                pk,
            )
        })
        .collect();
    partitions.sort_by_key(|(key, _)| key.token);

    for (key, pk) in partitions {
        let mutations: Vec<Mutation> = (0..ROWS_PER_PARTITION).map(|ck| row(pk, ck)).collect();
        writer
            .write_partition(key, mutations)
            .expect("write_partition");
    }
    let info = tokio::runtime::Handle::current()
        .block_on(async { writer.finish().await })
        .expect("finish");
    info.data_path
}

fn prefix_of(data_db: &Path) -> String {
    data_db
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Data.db name")
        .trim_end_matches("Data.db")
        .to_string()
}

/// Copy the whole generation into a sibling working directory so a test can
/// delete components without touching the reference copy.
fn copy_generation(src_dir: &Path, dst_dir: &Path) {
    std::fs::create_dir_all(dst_dir).expect("create working dir");
    for entry in std::fs::read_dir(src_dir)
        .expect("read generation dir")
        .flatten()
    {
        let path = entry.path();
        if path.is_file() {
            std::fs::copy(&path, dst_dir.join(entry.file_name())).expect("copy component");
        }
    }
}

/// F1 part 1 — the promoted-index offsets are measured against the ORIGINAL
/// `Statistics.db`'s baseline, so `Index.db` comes back byte-identical even
/// though no derivation from `Data.db`'s content could have produced that
/// baseline.
#[tokio::test(flavor = "multi_thread")]
async fn index_parity_when_the_true_baseline_is_below_the_content_minimum() {
    let temp = TempDir::new().expect("tempdir");
    let reference = temp.path().join("reference");
    std::fs::create_dir_all(&reference).expect("create reference dir");
    let data_db = tokio::task::spawn_blocking({
        let reference = reference.clone();
        move || write_inherited_baseline_generation(&reference)
    })
    .await
    .expect("join");
    let reference = data_db.parent().expect("generation dir").to_path_buf();
    let prefix = prefix_of(&data_db);

    let original_index = std::fs::read(reference.join(format!("{prefix}Index.db")))
        .expect("the writer must have produced an Index.db");
    // Non-vacuity: without a promoted-index payload, Index.db bytes do not
    // depend on the encoding baseline at all and this test could not fail.
    // An entry with NO payload costs at most ~24 bytes here (2-byte key
    // length + 4-byte int key + a VInt data offset + the zero
    // promoted-size), so comfortably exceeding that per partition means the
    // payloads are really there.
    assert!(
        original_index.len() > PARTITIONS as usize * 24,
        "fixture is too narrow to carry promoted-index payloads ({} bytes for {PARTITIONS} \
         partitions) — this test would be vacuous",
        original_index.len()
    );

    let working = temp.path().join("working");
    copy_generation(&reference, &working);
    std::fs::remove_file(working.join(format!("{prefix}Index.db"))).expect("delete Index.db");

    let out = temp.path().join("out");
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(
        &working.join(format!("{prefix}Data.db")),
        &schema(),
        &[Component::Index],
        &options,
    )
    .await
    .expect("rebuild_components must succeed on a healthy generation");
    assert!(report.refused.is_none(), "refused: {:?}", report.refused);
    // Substantive property FIRST: the bytes. (The classification assert
    // below is a report-surface check; asserting it first would mask a byte
    // mismatch behind a provenance-label failure.)
    let rebuilt_index =
        std::fs::read(out.join(format!("{prefix}Index.db"))).expect("rebuilt Index.db");
    if original_index != rebuilt_index {
        let n = original_index.len().max(rebuilt_index.len());
        let at = (0..n).find(|&i| original_index.get(i) != rebuilt_index.get(i));
        panic!(
            "Index.db mismatch (original {} bytes, rebuilt {} bytes, first diff at {at:?}) — a \
             re-derived baseline would land {INHERITED_BASELINE_GAP} micros above the real one \
             and narrow every row's timestamp VInt",
            original_index.len(),
            rebuilt_index.len()
        );
    }

    assert_eq!(
        report
            .classification
            .get("index")
            .and_then(|f| f.get("encoding_stats_baseline"))
            .map(String::as_str),
        Some("recovered"),
        "the manifest must name where the baseline came from; report={report:?}"
    );
}

/// F1 part 2 — with the original `Statistics.db` gone, the baseline is
/// genuinely unrecoverable; rebuild must REFUSE the `index` request (the
/// re-encoded span will not match the on-disk span) instead of writing
/// offsets that silently point at the wrong bytes.
#[tokio::test(flavor = "multi_thread")]
async fn index_refuses_when_the_baseline_cannot_be_recovered() {
    let temp = TempDir::new().expect("tempdir");
    let reference = temp.path().join("reference");
    std::fs::create_dir_all(&reference).expect("create reference dir");
    let data_db = tokio::task::spawn_blocking({
        let reference = reference.clone();
        move || write_inherited_baseline_generation(&reference)
    })
    .await
    .expect("join");
    let reference = data_db.parent().expect("generation dir").to_path_buf();
    let prefix = prefix_of(&data_db);

    let working = temp.path().join("working");
    copy_generation(&reference, &working);
    std::fs::remove_file(working.join(format!("{prefix}Index.db"))).expect("delete Index.db");
    std::fs::remove_file(working.join(format!("{prefix}Statistics.db")))
        .expect("delete Statistics.db");

    let out = temp.path().join("out");
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(
        &working.join(format!("{prefix}Data.db")),
        &schema(),
        &[Component::Index],
        &options,
    )
    .await
    .expect("an unrecoverable baseline is a REFUSAL (report.refused), never an Err");
    let refusal = report
        .refused
        .as_ref()
        .unwrap_or_else(|| panic!("must refuse rather than ship desynced offsets; {report:?}"));
    assert_eq!(
        refusal.reason,
        RefusalReason::ReencodeMismatch,
        "a healthy-but-unreproducible encoding is not corruption; {refusal:?}"
    );
    assert!(
        refusal.remedy.contains("Statistics.db"),
        "the remedy must name the component that carries the baseline: {}",
        refusal.remedy
    );
    assert!(
        !report.regenerated.iter().any(|c| c == "index"),
        "a refused run must claim nothing; report={report:?}"
    );
    assert!(
        !out.join(format!("{prefix}Index.db")).exists(),
        "a refused run must leave no partially-streamed Index.db behind"
    );
}
