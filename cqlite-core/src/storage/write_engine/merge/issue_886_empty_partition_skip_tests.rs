//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::{DecoratedKey, PartitionKey};
use std::collections::HashMap;

/// Single-column `int` partition-key schema with one regular `name` column.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "i886".to_string(),
        table: "phantom".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![crate::schema::Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Valid on-disk partition-key bytes for `id = n` under [`schema`], built
/// through the shared codec so the merge→writer path can decode them.
fn pk_bytes(schema: &TableSchema, n: i32) -> Vec<u8> {
    PartitionKey::single("id", Value::Integer(n))
        .to_bytes(schema)
        .expect("encode int partition key")
}

/// An in-memory run yielding a fixed list of `MergeEntry`s in order.
struct VecIterator(std::vec::IntoIter<MergeEntry>);
impl SSTableRowIterator for VecIterator {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.0.next().map(Ok)
    }
}

fn merger_over(entries: Vec<MergeEntry>, schema: TableSchema) -> KWayMerger {
    KWayMerger {
        runs: vec![RunReader::new(Box::new(VecIterator(entries.into_iter())))],
        heap: BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
        trace: NoTrace,
    }
}

/// END-TO-END writer-path test (#886): a partition whose ONLY merged row is a
/// metadata-only no-op must produce NO partition in the output SSTable, must
/// not be counted in `MergeStats.output_partitions`/`output_rows`, and must
/// not register a key in Index/Filter/Summary/statistics — proven by the
/// writer's authoritative on-disk `SSTableInfo.partition_count`. A sibling
/// NORMAL partition in the same merge must still be written unchanged.
#[tokio::test]
async fn empty_partition_is_skipped_on_writer_path() {
    let schema = schema();

    // Partition token 1: ONLY a truly-empty no-op (empty Live, no metadata).
    // It reconciles to nothing, so after filtering this partition's mutations
    // are empty. (A range-deletion carrier is no longer a no-op under #933, so
    // a genuinely-empty entry is used here to exercise the phantom-skip path.)
    let meta_only = MergeEntry::new(
        0,
        DecoratedKey::new(1, pk_bytes(&schema, 1)),
        None,
        0,
        RowData::Live { cells: vec![] },
    );
    assert!(
        meta_only.is_metadata_only_no_op(),
        "test precondition: the token-1 entry must be a metadata-only no-op"
    );

    // Partition token 2: a NORMAL live row with a real cell — must be written.
    let live = MergeEntry::new(
        0,
        DecoratedKey::new(2, pk_bytes(&schema, 2)),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("survivor".to_string()),
                100,
            )],
        },
    );

    let merger = merger_over(vec![meta_only, live], schema.clone());

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        temp_dir.path().to_path_buf(),
        1,
        &schema,
    )
    .expect("create writer");
    // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` streams
    // partitions through `begin_partition_incremental`, which requires
    // pre-seeded encoding baselines — matching the one real production
    // caller (`compact_sstables_with_registry`). No tombstones/TTLs in
    // this data, so seed a safe floor below every cell timestamp.
    writer.pre_seed_encoding_baselines(0, i32::MAX, i32::MAX);

    let stats = merger.merge(&mut writer).expect("merge must succeed");

    // The metadata-only partition contributes nothing to the output stats.
    assert_eq!(
        stats.output_partitions, 1,
        "only the normal partition counts as an output partition (no phantom)"
    );
    assert_eq!(
        stats.output_rows, 1,
        "only the normal partition's single live row counts toward output rows"
    );

    // The writer's on-disk partition counter is incremented exactly once per
    // `write_partition` call. If the phantom partition had been written it
    // would be 2; the skip keeps it at 1.
    let info = writer.finish().await.expect("finish must succeed");
    assert_eq!(
        info.partition_count, 1,
        "the output SSTable must contain exactly ONE partition; a phantom EMPTY \
             partition for the metadata-only-only key would make this 2"
    );
}

/// Guard: a partition with real content is unaffected — both a normal live
/// partition AND a metadata-only no-op coexisting in the SAME partition (the
/// live row survives) still writes that one partition.
#[tokio::test]
async fn partition_with_real_content_is_still_written() {
    let schema = schema();

    let live = MergeEntry::new(
        0,
        DecoratedKey::new(7, pk_bytes(&schema, 7)),
        None,
        200,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("keep-me".to_string()),
                200,
            )],
        },
    );

    let merger = merger_over(vec![live], schema.clone());

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        temp_dir.path().to_path_buf(),
        1,
        &schema,
    )
    .expect("create writer");
    // Issue #1668 stage 5c-iv part 2: see the sibling test above for why
    // this pre-seed is required.
    writer.pre_seed_encoding_baselines(0, i32::MAX, i32::MAX);

    let stats = merger.merge(&mut writer).expect("merge must succeed");
    assert_eq!(stats.output_partitions, 1);
    assert_eq!(stats.output_rows, 1);

    let info = writer.finish().await.expect("finish must succeed");
    assert_eq!(
        info.partition_count, 1,
        "a normal partition must still be written"
    );
}
