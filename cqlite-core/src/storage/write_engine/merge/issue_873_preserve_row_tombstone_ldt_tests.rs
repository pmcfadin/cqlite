//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::DecoratedKey;
use std::collections::HashMap;

fn unclustered_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks873".to_string(),
        table: "t873".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn tombstone_entry(run_index: usize, deletion_time: i64, ldt: i32) -> MergeEntry {
    MergeEntry::new(
        run_index,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        deletion_time,
        RowData::Tombstone {
            deletion_time,
            local_deletion_time: ldt,
        },
    )
}

/// `reconcile_cluster` must carry the winning tombstone's source LDT through
/// to the rebuilt surviving tombstone instead of hardcoding 0, and the LDT it
/// keeps must be the one paired with the winning (max) `deletion_time` — NOT
/// re-derived from that timestamp.
#[test]
fn reconcile_cluster_preserves_winning_tombstone_ldt() {
    // deletion_time chosen so it does NOT equal LDT (which is GC-clock seconds):
    // a logical-timestamp delete where micros and the wall-clock LDT diverge.
    let deletion_time = 1_000_000_000_000_000i64; // micros
    let source_ldt = 1_700_000_000i32; // seconds (wall clock)

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, deletion_time, source_ldt)],
        &HashMap::new(),
        None,
    )
    .expect("a surviving row tombstone must be emitted");

    match merged.row_data {
        RowData::Tombstone {
            deletion_time: dt,
            local_deletion_time,
        } => {
            assert_eq!(dt, deletion_time, "deletion_time must survive");
            assert_eq!(
                local_deletion_time, source_ldt,
                "the source LDT must be preserved, not reset to 0 nor derived from the timestamp"
            );
        }
        other => panic!("expected Tombstone, got {:?}", other),
    }
}

/// When several row tombstones reconcile, the LDT kept must pair with the
/// winning (max) `deletion_time`, not the max LDT and not the first-seen LDT.
#[test]
fn reconcile_cluster_keeps_ldt_paired_with_max_deletion_time() {
    // Older delete (smaller deletion_time) but LARGER LDT; newer delete
    // (larger deletion_time) with a SMALLER LDT. The winner is the newer
    // delete, so its (smaller) LDT must be the one carried through.
    let older = tombstone_entry(0, 100_000_000, 1_900_000_000);
    let newer = tombstone_entry(1, 200_000_000, 1_500_000_000);

    let merged =
        KWayMerger::<NoTrace>::reconcile_cluster(None, vec![older, newer], &HashMap::new(), None)
            .expect("a surviving row tombstone must be emitted");

    match merged.row_data {
        RowData::Tombstone {
            deletion_time,
            local_deletion_time,
        } => {
            assert_eq!(deletion_time, 200_000_000, "the newer delete must win");
            assert_eq!(
                local_deletion_time, 1_500_000_000,
                "the LDT carried must be the one paired with the winning deletion_time"
            );
        }
        other => panic!("expected Tombstone, got {:?}", other),
    }
}

/// `merge_entry_to_mutation` must thread a row tombstone's LDT onto the
/// produced mutation so the writer emits it verbatim instead of re-deriving it
/// from `timestamp_micros`. A live row keeps `local_deletion_time == None`.
#[test]
fn merge_entry_to_mutation_threads_row_tombstone_ldt() {
    let schema = unclustered_schema();
    let deletion_time = 1_000_000_000_000_000i64;
    let source_ldt = 1_700_000_000i32;

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(
        tombstone_entry(0, deletion_time, source_ldt),
        &schema,
    )
    .expect("conversion should succeed");

    assert_eq!(
        mutation.local_deletion_time,
        Some(source_ldt),
        "the row tombstone's source LDT must be threaded onto the mutation"
    );
    assert_eq!(
        mutation.effective_local_deletion_time(),
        source_ldt,
        "the writer must use the preserved LDT, not the timestamp-derived one"
    );
    // The timestamp-derived LDT would be deletion_time / 1_000_000 =
    // 1_000_000_000, which must NOT be what the writer would stamp.
    assert_ne!(
        mutation.effective_local_deletion_time() as i64,
        deletion_time / 1_000_000,
        "the LDT must NOT be re-derived from the deletion timestamp"
    );
}

/// A row tombstone whose LDT is the `0` "not surfaced" placeholder (the legacy
/// `from_legacy_value` fallback and pre-V5 row tombstones) must NOT pin an
/// explicit LDT — doing so would lose the writer's timestamp-derived fallback
/// and could trip the below-baseline guard against a nonzero pre-seeded
/// `min_local_deletion_time`, regressing previously-valid compactions (#946).
#[test]
fn merge_entry_to_mutation_placeholder_ldt_stays_unset() {
    let schema = unclustered_schema();
    let deletion_time = 1_000_000_000_000_000i64;

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(
        // ldt = 0 is the reader's "no LDT surfaced" sentinel, not a real delete.
        tombstone_entry(0, deletion_time, 0),
        &schema,
    )
    .expect("conversion should succeed");

    assert_eq!(
        mutation.local_deletion_time, None,
        "a placeholder (0) LDT must leave the mutation LDT unset so the writer \
             keeps deriving it from the timestamp, exactly as before #873"
    );
}

/// A live (non-tombstone) row must leave the mutation LDT unset so the writer
/// keeps its historical timestamp-derived behavior for cell tombstones.
#[test]
fn merge_entry_to_mutation_live_row_leaves_ldt_none() {
    let schema = unclustered_schema();
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new(
                "value".to_string(),
                Value::text("alive".to_string()),
                100,
            )],
        },
    );

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");
    assert_eq!(
        mutation.local_deletion_time, None,
        "a live row must not pin an explicit LDT"
    );
}

/// Issue #932: when a row tombstone (older) reconciles with a surviving cell
/// (newer) for the same `(pk, ck)`, `reconcile_cluster` must emit a LIVE entry
/// carrying the surviving cell AND the coexisting `row_deletion` (so the
/// deletion keeps shadowing older cells of other columns in SSTables not part
/// of a partial compaction). Pre-#932 the row deletion was DROPPED.
#[test]
fn reconcile_cluster_attaches_row_deletion_when_cells_survive() {
    let deletion_time = 100i64;
    let source_ldt = 1_700_000_000i32;
    // A row tombstone at ts=100 and a live `name` cell at ts=300 (> 100).
    let tomb = tombstone_entry(1, deletion_time, source_ldt);
    let live = MergeEntry::new(
        0,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        300,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("new".to_string()),
                300,
            )],
        },
    );

    let merged =
        KWayMerger::<NoTrace>::reconcile_cluster(None, vec![tomb, live], &HashMap::new(), None)
            .expect("a live coexistence row must be emitted");

    match &merged.row_data {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1, "the surviving name cell must remain");
            assert_eq!(cells[0].column, "name");
            assert_eq!(cells[0].timestamp, 300);
        }
        other => panic!("expected a Live coexistence row, got {:?}", other),
    }
    assert_eq!(
        merged.row_deletion,
        Some((deletion_time, source_ldt)),
        "the coexisting row deletion (and its source LDT) must be preserved on the live entry"
    );
}

/// Issue #932: an older cell shadowed by the coexisting row deletion must be
/// dropped from the surviving set (a cell at ts <= row_del is shadowed) while
/// the deletion is still carried for the newer survivor.
#[test]
fn reconcile_cluster_shadows_older_cell_under_coexisting_deletion() {
    let deletion_time = 100i64;
    let tomb = tombstone_entry(1, deletion_time, 1_700_000_000);
    // Older `score` cell at ts=50 (<= 100, must be shadowed) and a newer
    // `name` cell at ts=300 (> 100, must survive), in two runs.
    let older = MergeEntry::new(
        2,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        50,
        RowData::Live {
            cells: vec![CellData::new("score".to_string(), Value::Integer(999), 50)],
        },
    );
    let newer = MergeEntry::new(
        0,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        300,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("new".to_string()),
                300,
            )],
        },
    );

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tomb, older, newer],
        &HashMap::new(),
        None,
    )
    .expect("a live coexistence row must be emitted");

    match &merged.row_data {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1, "only the newer cell survives");
            assert_eq!(
                cells[0].column, "name",
                "the older `score` cell is shadowed"
            );
        }
        other => panic!("expected a Live coexistence row, got {:?}", other),
    }
    assert_eq!(
        merged.row_deletion.map(|(dt, _)| dt),
        Some(deletion_time),
        "the row deletion must be preserved to keep shadowing across SSTables"
    );
}

/// Issue #932: `merge_entry_to_mutation` must emit the coexisting row deletion
/// as `Mutation::row_tombstone` (decoupled from the row's liveness
/// `timestamp_micros`) so the writer re-emits a `HAS_DELETION` row holding both
/// the deletion and the surviving cells.
#[test]
fn merge_entry_to_mutation_emits_coexisting_row_tombstone() {
    let schema = unclustered_schema();
    let deletion_time = 100i64;
    let source_ldt = 1_700_000_000i32;
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        300,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("new".to_string()),
                300,
            )],
        },
    )
    .with_row_deletion(deletion_time, source_ldt);

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");

    assert_eq!(
        mutation.row_tombstone,
        Some((deletion_time, source_ldt)),
        "the coexisting row deletion must be emitted as Mutation::row_tombstone"
    );
    assert_eq!(
        mutation.timestamp_micros, 300,
        "the mutation's liveness timestamp stays the row write time, NOT the deletion time"
    );
    assert!(
            mutation
                .operations
                .iter()
                .any(|op| matches!(op, crate::storage::write_engine::mutation::CellOperation::Write { column, .. } if column == "name")),
            "the surviving cell must still be emitted alongside the row tombstone"
        );
}

/// An in-memory run yielding a fixed list of `MergeEntry`s in order, so the
/// readback test can drive the FULL production merge→writer path
/// (`merge_entry_to_mutation` + `write_partition`) rather than a hand-rolled
/// shortcut.
struct VecIterator(std::vec::IntoIter<MergeEntry>);
impl SSTableRowIterator for VecIterator {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.0.next().map(Ok)
    }
}

/// Readback regression: a row tombstone whose `deletion_time` (micros) and
/// `local_deletion_time` (GC-clock seconds) intentionally DIFFER must survive
/// a full compaction rewrite (merge → SSTableWriter → on-disk Data.db →
/// reader) with BOTH values intact. The reader re-surfaces the row-tombstone
/// `deletion_time` (markedForDeleteAt, micros) on the compaction read path;
/// the LDT is the on-disk `localDeletionTime` the writer must have encoded
/// without underflowing the unsigned delta.
#[tokio::test]
async fn row_tombstone_ldt_survives_compaction_rewrite() {
    use crate::platform::Platform;
    use crate::storage::sstable::reader::compaction_row::CompactionRowData;
    use crate::Config;
    use std::sync::Arc;

    let schema = unclustered_schema();
    let deletion_time = 1_000_000_000_000_000i64; // micros
    let source_ldt = 1_700_000_000i32; // seconds, deliberately != deletion_time/1e6

    let merger = KWayMerger {
        runs: vec![RunReader::new(Box::new(VecIterator(
            vec![tombstone_entry(0, deletion_time, source_ldt)].into_iter(),
        )))],
        heap: std::collections::BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema: schema.clone(),
        schema_arc: std::sync::Arc::new(schema.clone()),
        _egress_slot: None,
        trace: NoTrace,
    };

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        temp_dir.path().to_path_buf(),
        1,
        &schema,
    )
    .expect("create writer");
    // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` now streams
    // partitions through `begin_partition_incremental`, which requires
    // pre-seeded encoding baselines (it cannot buffer a whole partition
    // to discover the minimum LDT/timestamp before emitting bytes,
    // unlike `write_partition`) — exactly what the one real production
    // caller (`compact_sstables_with_registry`) always does. Seed with
    // this test's own known minimums, matching that real usage.
    writer.pre_seed_encoding_baselines(deletion_time, source_ldt, i32::MAX);

    // Drives merge_partition_rows → merge_entry_to_mutation → write_partition
    // (the production rewrite path). Must not underflow the LDT delta.
    merger
        .merge(&mut writer)
        .expect("merge+write must succeed (no LDT underflow)");
    let info = writer.finish().await.expect("finish must succeed");

    // Read the row tombstone back from the on-disk SSTable and assert the LDT
    // round-tripped (the writer encoded the preserved value, not 0 / derived).
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .expect("open written SSTable");
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("compaction iterator");
    let mut saw_tombstone = false;
    for row in rows {
        if let CompactionRowData::Tombstone {
            deletion_time: dt,
            local_deletion_time,
            ..
        } = row.row_data
        {
            saw_tombstone = true;
            assert_eq!(dt, deletion_time, "deletion_time must round-trip");
            assert_eq!(
                local_deletion_time, source_ldt,
                "local_deletion_time must round-trip the preserved source value"
            );
        }
    }
    assert!(
        saw_tombstone,
        "the rewritten SSTable must contain the row tombstone"
    );
}
