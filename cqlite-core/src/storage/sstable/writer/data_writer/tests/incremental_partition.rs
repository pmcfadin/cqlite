//! Correctness proof for `IncrementalPartitionWriter` (issue #1668, stage
//! 5c-iv, part 1): fed the SAME logical partition ONE PIECE AT A TIME
//! (static row, then one clustering row at a time, with incremental marker
//! interleaving), it must produce BYTE-IDENTICAL Data.db output — including
//! the returned promoted-index blocks and emit counts — to today's
//! whole-slice `DataWriter::write_partition_with_index_blocks`.
//!
//! NOT yet wired to any production caller — these tests exercise the new
//! entry point directly, in isolation, matching the "prove before wiring"
//! precedent of every prior sub-stage (stage 2's `StreamingMerger`, stage
//! 5b's `schema_ordered_pop_all`, stage 5c-i's schema-aware heap).

use super::super::*;
use super::support::*;
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, PartitionKey, PartitionTombstone,
    RangeTombstone, TableId,
};
use crate::types::Value;

fn key(token: i64, bytes: Vec<u8>) -> DecoratedKey {
    DecoratedKey::new(token, bytes)
}

fn row_mutation(schema: &TableSchema, ck: i32, value: &str, ts: i64) -> Mutation {
    let table_id = TableId::new(&schema.keyspace, &schema.table);
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
    let column = if schema.columns.iter().any(|c| c.name == "regular_val") {
        "regular_val"
    } else {
        "v"
    };
    let ops = vec![CellOperation::Write {
        column: column.to_string(),
        value: Value::Text(value.to_string()),
    }];
    Mutation::new(table_id, pk, Some(ck_key), ops, ts, None)
}

fn static_mutation(schema: &TableSchema, value: &str, ts: i64) -> Mutation {
    let table_id = TableId::new(&schema.keyspace, &schema.table);
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "static_val".to_string(),
        value: Value::Text(value.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, ts, None)
}

/// Resolve the static ops for `mutations` via the incremental tracker
/// (stage 5c-ii) — the SAME resolution `collect_static_operations` performs
/// internally for the whole-slice path, exposed here so the incremental
/// entry point's caller can supply an ALREADY-RESOLVED set (matching how a
/// real streaming caller would use it, per the stage-5c-iv design note that
/// compaction's static carrier is always a single, already-reconciled entry).
fn resolve_static_ops(
    mutations: &[Mutation],
    schema: &TableSchema,
    shadow_floor: Option<i64>,
) -> Vec<StaticMergedOp> {
    let mut tracker = StaticOpsTracker::new();
    for m in mutations {
        tracker.feed(m, schema, shadow_floor);
    }
    tracker.finish()
}

fn blocks_eq(a: &[PromotedIndexBlock], b: &[PromotedIndexBlock]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(x, y)| {
            x.first_name == y.first_name
                && x.last_name == y.last_name
                && x.offset == y.offset
                && x.width == y.width
                && x.oss50_separator == y.oss50_separator
        })
}

/// Drive the OLD whole-slice path and return `(buffer_bytes, offset, blocks,
/// emit)` for comparison.
fn run_whole_slice(
    schema: &TableSchema,
    key: &DecoratedKey,
    mutations: &[Mutation],
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
) -> (Vec<u8>, u64, Vec<PromotedIndexBlock>, PartitionEmitCounts) {
    let mut writer = DataWriter::new(create_test_stats());
    let (offset, blocks, emit) = writer
        .write_partition_with_index_blocks(
            key,
            mutations,
            schema,
            partition_tombstone,
            range_tombstones,
        )
        .expect("whole-slice write must succeed");
    (writer_buffer(&writer), offset, blocks, emit)
}

/// Drive the NEW incremental path (static row resolved upfront via
/// `StaticOpsTracker`, then one `feed_row` per clustering mutation) and
/// return the SAME shape for comparison.
fn run_incremental(
    schema: &TableSchema,
    key: &DecoratedKey,
    clustering_mutations: &[Mutation],
    static_ops: &[StaticMergedOp],
    first_mutation_ts: i64,
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
) -> (Vec<u8>, u64, Vec<PromotedIndexBlock>, PartitionEmitCounts) {
    let mut writer = DataWriter::new(create_test_stats());
    let schema_has_static = schema.columns.iter().any(|c| c.is_static);
    let mut session = writer
        .begin_partition_incremental(key, partition_tombstone, range_tombstones, schema)
        .expect("begin_partition_incremental must succeed");
    if schema_has_static {
        session
            .feed_static_row(static_ops, first_mutation_ts, schema)
            .expect("feed_static_row must succeed");
    }
    for m in clustering_mutations {
        session.feed_row(m, schema).expect("feed_row must succeed");
    }
    let (offset, blocks, emit) = session.finish(schema).expect("finish must succeed");
    (writer_buffer(&writer), offset, blocks, emit)
}

/// Read back the writer's in-memory buffer (private field, but this test
/// module is a descendant of `data_writer` so field access is allowed).
fn writer_buffer(writer: &DataWriter) -> Vec<u8> {
    writer.buffer.clone()
}

#[test]
fn incremental_matches_whole_slice_for_plain_rows() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let mutations: Vec<Mutation> = (0..5)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, None, &[]);

    assert_eq!(old_bytes, new_bytes, "Data.db bytes must be identical");
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

#[test]
fn incremental_matches_whole_slice_for_static_and_regular_rows() {
    let schema = create_static_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let static_m = static_mutation(&schema, "us-east", 500);
    let clustering: Vec<Mutation> = (0..4)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    // Whole-slice path sees ALL mutations (static + clustering) together.
    let mut all_mutations = vec![static_m.clone()];
    all_mutations.extend(clustering.iter().cloned());
    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &all_mutations, None, &[]);

    // Incremental path resolves statics upfront (matching compaction's own
    // guarantee that the static carrier is a single, already-reconciled
    // entry — stage 5c-iv's design note).
    let static_ops = resolve_static_ops(&[static_m.clone()], &schema, None);
    let (new_bytes, new_offset, new_blocks, new_emit) = run_incremental(
        &schema,
        &key,
        &clustering,
        &static_ops,
        static_m.timestamp_micros,
        None,
        &[],
    );

    assert_eq!(old_bytes, new_bytes, "Data.db bytes must be identical");
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

#[test]
fn incremental_matches_whole_slice_for_schema_has_static_but_partition_writes_none() {
    let schema = create_static_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let clustering: Vec<Mutation> = (0..3)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &clustering, None, &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &clustering, &[], 0, None, &[]);

    assert_eq!(
        old_bytes, new_bytes,
        "the minimal empty static-row prelude must be byte-identical"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

#[test]
fn incremental_matches_whole_slice_for_range_tombstone_interleaved_with_rows() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let mutations: Vec<Mutation> = (0..5)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(1))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        deletion_time: 900, // older than the covered rows' writetimes: rows survive
        local_deletion_time: 90,
    };
    let range_tombstones = vec![rt];

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &range_tombstones);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, None, &range_tombstones);

    assert_eq!(
        old_bytes, new_bytes,
        "Data.db bytes must be identical with a range tombstone interleaved"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

#[test]
fn incremental_matches_whole_slice_for_range_tombstone_shadowing_a_row() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    // ck=1's write is OLDER than the covering range tombstone: shadowed,
    // contributes nothing in EITHER path.
    let mutations = vec![
        row_mutation(&schema, 0, "row-0", 1000),
        row_mutation(&schema, 1, "row-1-shadowed", 100),
        row_mutation(&schema, 2, "row-2", 1000),
    ];
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(1))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(1))),
        deletion_time: 500,
        local_deletion_time: 50,
    };
    let range_tombstones = vec![rt];

    let (old_bytes, _, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &range_tombstones);
    let (new_bytes, _, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, None, &range_tombstones);

    assert_eq!(
        old_bytes, new_bytes,
        "the shadowed row must be dropped identically in both paths"
    );
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
    assert_eq!(
        old_emit.rows, 2,
        "only the 2 surviving rows must be counted"
    );
}

#[test]
fn incremental_matches_whole_slice_for_partition_tombstone() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    // ck=0 predates the partition tombstone (shadowed); ck=1 postdates it
    // (survives).
    let mutations = vec![
        row_mutation(&schema, 0, "row-0-shadowed", 100),
        row_mutation(&schema, 1, "row-1-survives", 2000),
    ];
    let pt = PartitionTombstone {
        deletion_time: 1000,
        local_deletion_time: 100,
    };

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, Some(&pt), &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, Some(&pt), &[]);

    assert_eq!(
        old_bytes, new_bytes,
        "the partition-tombstone header and shadowing must be byte-identical"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
    assert_eq!(old_emit.rows, 1, "only the surviving row must be counted");
}

/// A wide partition (large cell values across many rows) crossing the 64
/// KiB promoted-index block boundary at least once — proves the incremental
/// path's block tracking (moved verbatim from the whole-slice loop) agrees
/// with the whole-slice path's block boundaries exactly, not just the raw
/// bytes.
#[test]
fn incremental_matches_whole_slice_for_wide_partition_with_promoted_index_blocks() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let big_value = "x".repeat(4000);
    let mutations: Vec<Mutation> = (0..25)
        .map(|ck| row_mutation(&schema, ck, &big_value, 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, None, &[]);

    assert_eq!(
        old_bytes, new_bytes,
        "wide-partition Data.db bytes must be identical"
    );
    assert_eq!(old_offset, new_offset);
    assert!(
        old_blocks.len() >= 2,
        "fixture must actually cross the 64 KiB promoted-index boundary \
         (got {} blocks) — otherwise this test doesn't exercise block \
         tracking at all",
        old_blocks.len()
    );
    assert!(
        blocks_eq(&old_blocks, &new_blocks),
        "promoted-index block boundaries must match exactly: old={old_blocks:?} new={new_blocks:?}"
    );
    assert_eq!(old_emit, new_emit);
}

/// Roborev blocker #1 (issue #1668): two ADJACENT range tombstones —
/// `[0, 3)` (deletion_time 500) then `[3, 6]` (deletion_time 800, a
/// DIFFERENT deletion time so `coalesce_range_tombstones` upstream in the
/// merge layer would never fold them into one range first) — whose close
/// bound (`Exclusive(3)`) and the next range's open bound (`Inclusive(3)`)
/// meet at the SAME clustering value with complementary inclusivity. The
/// whole-slice path (`write_partition_with_index_blocks`) always runs
/// `coalesce_boundaries` and persists this as ONE `PartitionItem::Boundary`
/// (Cassandra's own on-disk `RangeTombstoneBoundaryMarker` shape); before
/// this fix, `IncrementalPartitionWriter` drained its pre-sorted marker list
/// one at a time and never coalesced, persisting two separate bound markers
/// instead — a byte-format parity regression this test pins.
#[test]
fn incremental_matches_whole_slice_for_adjacent_range_tombstones_forming_a_boundary() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    // Every row predates BOTH tombstones except ck=7, which sits outside
    // both ranges and must survive in both paths.
    let mutations: Vec<Mutation> = (0..8)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 100))
        .collect();
    let range_tombstones = vec![
        RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(0))),
            end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(3))),
            deletion_time: 500,
            local_deletion_time: 50,
        },
        RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
            end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(6))),
            deletion_time: 800,
            local_deletion_time: 80,
        },
    ];

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &range_tombstones);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_incremental(&schema, &key, &mutations, &[], 0, None, &range_tombstones);

    assert_eq!(
        old_bytes, new_bytes,
        "adjacent range tombstones must coalesce into an IDENTICAL single \
         boundary marker in both the whole-slice and incremental paths"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
    assert_eq!(
        old_emit.rows, 1,
        "only ck=7 survives both tombstone ranges"
    );
}

/// Combined fixture: static row + range tombstone + partition tombstone +
/// several regular rows, some shadowed — the full feature combination.
#[test]
fn incremental_matches_whole_slice_for_combined_static_tombstones_and_rows() {
    let schema = create_static_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let static_m = static_mutation(&schema, "eu-west", 5000);
    let pt = PartitionTombstone {
        deletion_time: 1000,
        local_deletion_time: 100,
    };
    let clustering = vec![
        row_mutation(&schema, 0, "row-0-shadowed-by-partition", 500),
        row_mutation(&schema, 1, "row-1-survives", 2000),
        row_mutation(&schema, 2, "row-2-survives", 3000),
        row_mutation(&schema, 3, "row-3-shadowed-by-range", 2100),
    ];
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        deletion_time: 2500,
        local_deletion_time: 250,
    };
    let range_tombstones = vec![rt];

    let mut all_mutations = vec![static_m.clone()];
    all_mutations.extend(clustering.iter().cloned());
    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &all_mutations, Some(&pt), &range_tombstones);

    let static_ops = resolve_static_ops(&[static_m.clone()], &schema, Some(pt.deletion_time));
    let (new_bytes, new_offset, new_blocks, new_emit) = run_incremental(
        &schema,
        &key,
        &clustering,
        &static_ops,
        static_m.timestamp_micros,
        Some(&pt),
        &range_tombstones,
    );

    assert_eq!(
        old_bytes, new_bytes,
        "combined static+range+partition-tombstone fixture must be byte-identical"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
    assert_eq!(
        old_emit.rows, 3,
        "1 static row + ck=1 and ck=2 (the only clustering rows surviving \
         both the partition and range tombstones) — emit.rows counts the \
         static row too"
    );
}
