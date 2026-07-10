//! Correctness proof for `StreamingPartitionSession` (issue #1668, stage
//! 5c-iv part 3): the cross-call resumable session must produce
//! BYTE-IDENTICAL Data.db output to today's whole-slice
//! `write_partition_with_index_blocks` — same as
//! `IncrementalPartitionWriter` (stage 5c-iv part 1) already proves — AND a
//! partition fed in two separate batches (simulating a
//! `maintenance_step_inner` budget pause between them, stage 4) must produce
//! byte-identical output to one fed straight through in a single batch.
//!
//! NOT yet wired to any production caller — these tests exercise the new
//! entry point directly, in isolation, matching the "prove before wiring"
//! precedent of every prior sub-stage.

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

fn writer_buffer(writer: &DataWriter) -> Vec<u8> {
    writer.buffer.clone()
}

/// Drive the OLD whole-slice path — identical helper to
/// `incremental_partition.rs`'s (kept local; private to a sibling module).
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

/// Drive the NEW cross-call-resumable session, feeding `clustering_mutations`
/// in ONE unbroken batch (no simulated pause) — the baseline "does the new
/// session match the whole-slice path at all" proof.
fn run_streaming(
    schema: &TableSchema,
    key: &DecoratedKey,
    clustering_mutations: &[Mutation],
    static_ops: &[StaticMergedOp],
    first_mutation_ts: i64,
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
) -> (Vec<u8>, u64, Vec<PromotedIndexBlock>, PartitionEmitCounts) {
    run_streaming_split(
        schema,
        key,
        clustering_mutations,
        clustering_mutations.len(), // no split: one batch of everything
        static_ops,
        first_mutation_ts,
        partition_tombstone,
        range_tombstones,
    )
}

/// Drive the NEW cross-call-resumable session, feeding the first
/// `split_at` clustering mutations, then — simulating a
/// `maintenance_step_inner` budget pause and a LATER resuming call — feeding
/// the rest. Nothing is retained between the two batches except the plain
/// `session`/`writer` VALUES themselves (moved back and forth exactly as
/// `ActiveMerge` would hold them across two separate function calls); no
/// special "pause"/"resume" conversion happens because
/// `StreamingPartitionSession` never borrows anything.
fn run_streaming_split(
    schema: &TableSchema,
    key: &DecoratedKey,
    clustering_mutations: &[Mutation],
    split_at: usize,
    static_ops: &[StaticMergedOp],
    first_mutation_ts: i64,
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
) -> (Vec<u8>, u64, Vec<PromotedIndexBlock>, PartitionEmitCounts) {
    let mut writer = DataWriter::new(create_test_stats());
    let schema_has_static = schema.columns.iter().any(|c| c.is_static);
    let mut session = writer
        .begin_streaming_partition(key, partition_tombstone, range_tombstones, schema)
        .expect("begin_streaming_partition must succeed");
    if schema_has_static {
        session
            .feed_static_row(&mut writer, static_ops, first_mutation_ts, schema)
            .expect("feed_static_row must succeed");
    }

    // "Call 1": feed the first batch, then simulate returning control to an
    // outer caller by dropping every local variable that isn't `session`
    // itself and re-deriving `writer`'s access from scratch in "call 2"
    // below — there is nothing else TO drop, since `session` owns
    // everything it needs.
    for m in &clustering_mutations[..split_at] {
        session
            .feed_row(&mut writer, m, schema)
            .expect("feed_row (batch 1) must succeed");
    }

    // "Call 2": resume with the SAME `session`/`writer` values (as
    // `ActiveMerge` would hand them back on the next `maintenance_step_inner`
    // call) and finish draining.
    for m in &clustering_mutations[split_at..] {
        session
            .feed_row(&mut writer, m, schema)
            .expect("feed_row (batch 2) must succeed");
    }

    let (offset, blocks, emit) = session
        .finish(&mut writer, schema)
        .expect("finish must succeed");
    (writer_buffer(&writer), offset, blocks, emit)
}

#[test]
fn streaming_matches_whole_slice_for_plain_rows() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let mutations: Vec<Mutation> = (0..5)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_streaming(&schema, &key, &mutations, &[], 0, None, &[]);

    assert_eq!(old_bytes, new_bytes, "Data.db bytes must be identical");
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

#[test]
fn streaming_matches_whole_slice_for_static_and_regular_rows() {
    let schema = create_static_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let static_m = static_mutation(&schema, "us-east", 500);
    let clustering: Vec<Mutation> = (0..4)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    let mut all_mutations = vec![static_m.clone()];
    all_mutations.extend(clustering.iter().cloned());
    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &all_mutations, None, &[]);

    let static_ops = resolve_static_ops(&[static_m.clone()], &schema, None);
    let (new_bytes, new_offset, new_blocks, new_emit) = run_streaming(
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
fn streaming_matches_whole_slice_for_range_tombstone_interleaved_with_rows() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let mutations: Vec<Mutation> = (0..5)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(1))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        deletion_time: 900,
        local_deletion_time: 90,
    };
    let range_tombstones = vec![rt];

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &range_tombstones);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_streaming(&schema, &key, &mutations, &[], 0, None, &range_tombstones);

    assert_eq!(
        old_bytes, new_bytes,
        "Data.db bytes must be identical with a range tombstone interleaved"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
}

/// Roborev blocker #1 (issue #1668): two ADJACENT range tombstones with
/// DIFFERENT deletion times (so an upstream `coalesce_range_tombstones` pass
/// would never fold them first) must coalesce into ONE `Boundary` marker in
/// `StreamingPartitionSession` too, exactly matching the whole-slice path —
/// see `incremental_partition.rs`'s identical test for the full rationale.
/// Exercised BOTH unbroken and split mid-partition (right at the boundary's
/// own clustering point) to prove the fix survives a
/// `maintenance_step_inner` budget pause landing exactly there.
#[test]
fn streaming_matches_whole_slice_for_adjacent_range_tombstones_forming_a_boundary() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
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
    let (unbroken_bytes, unbroken_offset, unbroken_blocks, unbroken_emit) =
        run_streaming(&schema, &key, &mutations, &[], 0, None, &range_tombstones);

    assert_eq!(
        old_bytes, unbroken_bytes,
        "adjacent range tombstones must coalesce into an IDENTICAL single \
         boundary marker in the streaming path"
    );
    assert_eq!(old_offset, unbroken_offset);
    assert!(blocks_eq(&old_blocks, &unbroken_blocks));
    assert_eq!(old_emit, unbroken_emit);
    assert_eq!(old_emit.rows, 1, "only ck=7 survives both tombstone ranges");

    // Split mid-partition right at the shared boundary point (after ck=2,
    // before ck=3) — the pause lands exactly where the two markers coalesce.
    let (split_bytes, split_offset, split_blocks, split_emit) = run_streaming_split(
        &schema,
        &key,
        &mutations,
        3,
        &[],
        0,
        None,
        &range_tombstones,
    );
    assert_eq!(
        old_bytes, split_bytes,
        "splitting the batch right at the boundary's own clustering point \
         must still coalesce identically"
    );
    assert_eq!(old_offset, split_offset);
    assert!(blocks_eq(&old_blocks, &split_blocks));
    assert_eq!(old_emit, split_emit);
}

#[test]
fn streaming_matches_whole_slice_for_partition_tombstone() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
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
        run_streaming(&schema, &key, &mutations, &[], 0, Some(&pt), &[]);

    assert_eq!(
        old_bytes, new_bytes,
        "the partition-tombstone header and shadowing must be byte-identical"
    );
    assert_eq!(old_offset, new_offset);
    assert!(blocks_eq(&old_blocks, &new_blocks));
    assert_eq!(old_emit, new_emit);
    assert_eq!(old_emit.rows, 1, "only the surviving row must be counted");
}

/// A wide partition crossing the 64 KiB promoted-index block boundary at
/// least once — proves the owning session's block tracking agrees with the
/// whole-slice path exactly, not just the raw bytes.
#[test]
fn streaming_matches_whole_slice_for_wide_partition_with_promoted_index_blocks() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let big_value = "x".repeat(4000);
    let mutations: Vec<Mutation> = (0..25)
        .map(|ck| row_mutation(&schema, ck, &big_value, 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &[]);
    let (new_bytes, new_offset, new_blocks, new_emit) =
        run_streaming(&schema, &key, &mutations, &[], 0, None, &[]);

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

// ---------------------------------------------------------------------
// Cross-call pause/resume proofs (issue #1668, stage 5c-iv part 3's whole
// reason for existing over `IncrementalPartitionWriter`): a partition fed
// in TWO separate batches — simulating a `maintenance_step_inner` budget
// pause between them — must produce byte-identical output to one fed in a
// single unbroken batch, for every fixture shape above.
// ---------------------------------------------------------------------

#[test]
fn streaming_split_mid_partition_matches_unbroken_batch_for_plain_rows() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let mutations: Vec<Mutation> = (0..7)
        .map(|ck| row_mutation(&schema, ck, &format!("row-{ck}"), 1000 + ck as i64))
        .collect();

    let (unbroken_bytes, unbroken_offset, unbroken_blocks, unbroken_emit) =
        run_streaming(&schema, &key, &mutations, &[], 0, None, &[]);

    // Split after every possible row count (1..len) — a pause can land
    // between ANY two cluster groups, not just a convenient midpoint.
    for split_at in 1..mutations.len() {
        let (split_bytes, split_offset, split_blocks, split_emit) =
            run_streaming_split(&schema, &key, &mutations, split_at, &[], 0, None, &[]);
        assert_eq!(
            unbroken_bytes, split_bytes,
            "pausing after row {split_at} must not change the output bytes"
        );
        assert_eq!(unbroken_offset, split_offset);
        assert!(blocks_eq(&unbroken_blocks, &split_blocks));
        assert_eq!(unbroken_emit, split_emit);
    }
}

#[test]
fn streaming_split_mid_partition_matches_unbroken_batch_for_static_and_tombstones() {
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
        row_mutation(&schema, 4, "row-4-survives", 4000),
    ];
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(3))),
        deletion_time: 2500,
        local_deletion_time: 250,
    };
    let range_tombstones = vec![rt];
    let static_ops = resolve_static_ops(&[static_m.clone()], &schema, Some(pt.deletion_time));

    let (unbroken_bytes, unbroken_offset, unbroken_blocks, unbroken_emit) = run_streaming(
        &schema,
        &key,
        &clustering,
        &static_ops,
        static_m.timestamp_micros,
        Some(&pt),
        &range_tombstones,
    );

    for split_at in 1..clustering.len() {
        let (split_bytes, split_offset, split_blocks, split_emit) = run_streaming_split(
            &schema,
            &key,
            &clustering,
            split_at,
            &static_ops,
            static_m.timestamp_micros,
            Some(&pt),
            &range_tombstones,
        );
        assert_eq!(
            unbroken_bytes, split_bytes,
            "pausing after row {split_at} must not change the output bytes, \
             even with a static row + partition tombstone + range tombstone"
        );
        assert_eq!(unbroken_offset, split_offset);
        assert!(blocks_eq(&unbroken_blocks, &split_blocks));
        assert_eq!(unbroken_emit, split_emit);
    }

    // Sanity: the fixture actually exercises shadowing on both sides, else
    // the split proof above would pass vacuously.
    assert_eq!(
        unbroken_emit.rows, 4,
        "1 static row + ck=1, ck=2, ck=4 (ck=0 shadowed by the partition \
         tombstone, ck=3 shadowed by the range tombstone)"
    );
}

#[test]
fn streaming_split_mid_partition_matches_whole_slice_for_wide_partition() {
    let schema = clustering_test_schema();
    let key = key(1, vec![0, 0, 0, 1]);
    let big_value = "x".repeat(4000);
    let mutations: Vec<Mutation> = (0..25)
        .map(|ck| row_mutation(&schema, ck, &big_value, 1000 + ck as i64))
        .collect();

    let (old_bytes, old_offset, old_blocks, old_emit) =
        run_whole_slice(&schema, &key, &mutations, None, &[]);

    // Pause in the MIDDLE of a promoted-index block's row run, not just at a
    // block boundary — proves the paused/resumed block-tracking state
    // (`current_block_first_ck`/`block_start_buf_offset`/etc.) survives the
    // simulated call boundary correctly.
    let (split_bytes, split_offset, split_blocks, split_emit) =
        run_streaming_split(&schema, &key, &mutations, 12, &[], 0, None, &[]);

    assert_eq!(
        old_bytes, split_bytes,
        "pausing mid-block must still reproduce the whole-slice bytes exactly"
    );
    assert_eq!(old_offset, split_offset);
    assert!(blocks_eq(&old_blocks, &split_blocks));
    assert_eq!(old_emit, split_emit);
}
