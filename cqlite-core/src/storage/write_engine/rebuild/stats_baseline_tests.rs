//! `StatsMetadata` minima vs `SerializationHeader.EncodingStats` are TWO
//! values, and a rebuild must keep them apart (issue #4197, roborev job 124).
//!
//! # The two quantities
//!
//! * `SerializationHeader.EncodingStats.minTimestamp`/`minLocalDeletionTime`/
//!   `minTTL` is the whole-SSTable DELTA-ENCODING BASELINE every row's VInt in
//!   `Data.db` was measured against. At compaction Cassandra merges it FORWARD
//!   from the input SSTables' own headers
//!   (`SerializationHeader.make(metadata, sstables)` → `EncodingStats.merge`,
//!   `cassandra-5.0.8`), so it can sit strictly BELOW anything present in the
//!   output's own rows. Rebuild RECOVERS it verbatim from the original
//!   `Statistics.db`; nothing can derive it (see `components`'s module doc).
//! * `StatsMetadata.minTimestamp`/`minLocalDeletionTime`/`minTTL` is
//!   Cassandra's `MetadataCollector` fold over the cells and tombstones
//!   actually WRITTEN.
//!
//! Rebuild wrote the recovered BASELINE into the accumulator fields that also
//! drive `StatsMetadata`, so for any input whose baseline sits below its own
//! content minimum the regenerated `Statistics.db` claimed, as its STATS
//! minimum, a timestamp no row in the file carries — and the manifest labelled
//! it `recovered`, implying a verbatim copy of the original, which is true of
//! the header and false of STATS. `StatisticsMetadata::encoding_stats_baseline`
//! now carries the baseline separately; the pass-2 fold establishes the STATS
//! minima from the rows it decodes.
//!
//! # Why in-crate rather than `tests/`
//!
//! The two halves are observed through two crate-internal seams, for the same
//! reason `components_keyrange_tests.rs` is in-crate: the `nb` STATS body is
//! not re-readable field-by-field through any public API (no reader surfaces
//! `StatsMetadata.minTimestamp`; `StatisticsReader`'s `timestamp_stats`
//! deliberately reports the HEADER's EncodingStats, since that is what the row
//! decoder needs). So the STATS side is read through
//! [`super::rebuild_components_capturing_stats`] — the documented test seam for
//! exactly this — and the HEADER side is read back off the regenerated file's
//! own bytes with the crate's `read_encoding_stats_baseline` parser.
//!
//! # Fixture
//!
//! `SSTableWriter::pre_seed_encoding_baselines` (the #729 two-pass flush API)
//! locks a baseline [`INHERITED_BASELINE_GAP`] micros BELOW every timestamp in
//! the generation, exactly as a compaction inheriting a lower minimum from its
//! inputs would — the same construction
//! `tests/issue_4197_rebuild_baseline_provenance.rs` uses. Note that CQLite's
//! own writer holds ONE triple for both quantities, so this generation's
//! ORIGINAL `Statistics.db` carries the seeded baseline in its STATS body too;
//! the original is therefore the oracle for the HEADER half only, and the
//! content minimum (a known constant here) is the oracle for the STATS half.

use super::{rebuild_components_capturing_stats, Component, RebuildOptions};
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::sstable::writer::{SSTableWriter, StatisticsMetadata};
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KEYSPACE: &str = "test_ks";
const TABLE: &str = "baseline_split_table";

/// Every row's write timestamp — the content minimum a full decode must find.
const ROW_TIMESTAMP: i64 = 1_759_713_125_977_357;

/// How far BELOW the content minimum the fixture's real encoding baseline
/// sits. Large enough that confusing the two is unmistakable in a failure
/// message (and large enough to change VInt widths, as the sibling
/// index-parity fixture requires).
const INHERITED_BASELINE_GAP: i64 = 1_000_000_000;

const PARTITIONS: i32 = 3;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
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

fn row(pk: i32) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text(format!("v{pk}")),
        }],
        ROW_TIMESTAMP,
        None,
    )
}

/// Write a self-consistent generation whose `EncodingStats` baseline is
/// [`INHERITED_BASELINE_GAP`] micros below every timestamp it contains.
fn write_inherited_baseline_generation(dir: &Path) -> PathBuf {
    let schema = schema();
    let mut writer = SSTableWriter::new(dir.to_path_buf(), 1, &schema).expect("writer");
    // `i32::MAX` for LDT/TTL is the ordinary "no deletions, no TTLs" sentinel
    // this content would produce anyway; only the timestamp baseline is
    // deliberately lowered.
    writer.pre_seed_encoding_baselines(ROW_TIMESTAMP - INHERITED_BASELINE_GAP, i32::MAX, i32::MAX);

    let mut keys: Vec<_> = (0..PARTITIONS)
        .map(|pk| row(pk).decorated_key(&schema).expect("decorated key"))
        .collect();
    keys.sort_by_key(|k| k.token);
    for key in keys {
        let pk = i32::from_be_bytes([key.key[0], key.key[1], key.key[2], key.key[3]]);
        writer
            .write_partition(key, vec![row(pk)])
            .expect("write_partition");
    }
    tokio::runtime::Handle::current()
        .block_on(async { writer.finish().await })
        .expect("finish")
        .data_path
}

#[tokio::test(flavor = "multi_thread")]
async fn stats_minima_are_folded_from_content_while_the_header_keeps_the_recovered_baseline() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("gen");
    std::fs::create_dir_all(&root).expect("create gen dir");
    let data_db = tokio::task::spawn_blocking({
        let root = root.clone();
        move || write_inherited_baseline_generation(&root)
    })
    .await
    .expect("join");
    let generation = data_db.parent().expect("generation dir").to_path_buf();
    let prefix = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Data.db name")
        .trim_end_matches("Data.db")
        .to_string();

    // Non-vacuity, on the FIXTURE: the original header really does carry a
    // baseline below the content minimum. Read with the same parser the
    // rebuild's own recovery uses, against the file Cassandra's reader would
    // read. Without this the test could pass on a fixture whose two values
    // coincide, proving nothing.
    let original_header = std::fs::read(generation.join(format!("{prefix}Statistics.db")))
        .expect("the writer must have produced a Statistics.db");
    let (original_baseline_ts, _, _) =
        crate::parser::enhanced_statistics_parser::read_encoding_stats_baseline(&original_header)
            .expect("the original SerializationHeader must carry an EncodingStats baseline");
    assert_eq!(
        original_baseline_ts,
        ROW_TIMESTAMP - INHERITED_BASELINE_GAP,
        "fixture precondition: the original header's baseline must sit {INHERITED_BASELINE_GAP} \
         micros below every row's timestamp"
    );

    let options = RebuildOptions {
        out_dir: temp.path().join("out"),
        statistics_recovery_source: None,
    };
    let mut stats = StatisticsMetadata::default();
    let report = rebuild_components_capturing_stats(
        &data_db,
        &schema(),
        &[Component::Statistics],
        &options,
        &mut stats,
    )
    .await
    .expect("rebuild must succeed on a healthy generation");
    assert!(report.refused.is_none(), "refused: {:?}", report.refused);

    // THE POPULATION: every partition of this fixture survives reconciliation,
    // so a fold really did run over all of them. A zero count would make the
    // minima below vacuous (an unfolded sentinel, not a measurement).
    assert_eq!(
        stats.partition_count as i64, PARTITIONS as i64,
        "every partition must be counted, or the fold below measured nothing"
    );

    // (1) STATS minTimestamp == what a full decode actually finds, NOT the
    //     seeded header baseline.
    assert_eq!(
        stats.min_timestamp,
        ROW_TIMESTAMP,
        "StatsMetadata.minTimestamp must be the minimum over the rows actually written \
         (Cassandra's MetadataCollector fold), not the inherited delta-encoding baseline \
         {}",
        ROW_TIMESTAMP - INHERITED_BASELINE_GAP
    );
    assert_ne!(
        stats.min_timestamp, original_baseline_ts,
        "the two values must DIFFER for this fixture — if they are equal the separation is \
         not being exercised"
    );

    // (2) The delta-encoding baseline is still carried, separately, and is the
    //     RECOVERED one.
    let baseline = stats
        .encoding_stats_baseline
        .expect("a `statistics` rebuild must record the EncodingStats baseline it used");
    assert_eq!(
        baseline.min_timestamp, original_baseline_ts,
        "the EncodingStats baseline must be the original header's own value, verbatim"
    );

    // (3) The REGENERATED file's SerializationHeader carries that baseline —
    //     the unchanged Data.db delta-decodes against it, so this is the half
    //     that must never become a content fold.
    let rebuilt_header = std::fs::read(options.out_dir.join(format!("{prefix}Statistics.db")))
        .expect("rebuilt Statistics.db");
    let (rebuilt_baseline_ts, _, _) =
        crate::parser::enhanced_statistics_parser::read_encoding_stats_baseline(&rebuilt_header)
            .expect("the rebuilt SerializationHeader must carry an EncodingStats baseline");
    assert_eq!(
        rebuilt_baseline_ts, original_baseline_ts,
        "the regenerated SerializationHeader must preserve the recovered baseline; a content \
         fold here would leave the UNCHANGED Data.db decoding against a baseline it was never \
         encoded with"
    );

    // (4) The manifest says which is which: the six aggregates are a fold
    //     (`recomputed`), the header baseline is a verbatim recovery.
    let classification = report
        .classification
        .get("statistics")
        .unwrap_or_else(|| panic!("no `statistics` classification in report={report:?}"));
    assert_eq!(
        classification.get("min_timestamp").map(String::as_str),
        Some("recomputed"),
        "a fold over decoded content is `recomputed`, never `recovered`; report={report:?}"
    );
    assert_eq!(
        classification
            .get("encoding_stats_baseline")
            .map(String::as_str),
        Some("recovered"),
        "the header baseline came verbatim from the original Statistics.db; report={report:?}"
    );
}
