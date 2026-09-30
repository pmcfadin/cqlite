//! `Statistics.db` key-range population invariant (issue #4197 F6).
//!
//! `StatisticsMetadata::update_key_range` assigns `first_key` ONLY while it is
//! still `None` and always overwrites `last_key`, which is correct exactly
//! because `SSTableWriter` feeds it, in order, from a clean slate. Rebuild
//! used to PRE-SEED both ends from `entries` — the RAW boundary walk —
//! BEFORE pass 2 then called `update_key_range` for the partitions that
//! actually survive reconciliation, so `first_key` was frozen from the raw
//! walk while `last_key` came from the counted population. This file pins the
//! post-fix property: both ends and `partition_count` are produced by the
//! SAME pass-2 call site, over the SAME population.
//!
//! In-crate (rather than a `tests/` integration file) for ONE reason: the
//! `nb` STATS body does not serialize `firstKey`/`lastKey` at all — only
//! `build_stats_component_da` does — so these two fields cannot be read back
//! out of a rebuilt `nb` generation at all; see
//! [`super::rebuild_components_capturing_stats`]'s doc comment.
//!
//! # Reachability of the divergent state — MEASURED, not assumed
//!
//! The pre-seed only produced two DIFFERENT populations when an ENUMERATED
//! partition reconciled to nothing (`decode_one_partition` → `Ok(None)`).
//! Four writable shapes were tried while building this test and NONE of them
//! reaches that state on a CQLite-written generation:
//!
//!   * a rowless partition (header + end-of-partition marker only) — written
//!     to `Data.db`, but `distinct_partition_keys_with_positions` does not
//!     ENUMERATE it, so the raw walk never sees it either;
//!   * a partition-tombstone-ONLY partition (the ordinary
//!     `DELETE FROM t WHERE pk=?` shape, which this test uses) — enumerated
//!     AND counted: the decode preserves the marker and `merge` re-emits it
//!     as a carrier;
//!   * a row fully shadowed by a higher-timestamp partition tombstone —
//!     likewise counted (the decode primitive runs with read shadowing OFF,
//!     `build_v5_parser(false)`, because salvage/rebuild mirror the PHYSICAL
//!     file rather than SELECT semantics);
//!   * a TTL-expired row — likewise counted, for the same reason.
//!
//! So `Ok(None)` is a defensively-handled outcome, and F6 is a
//! latent-correctness fix — it deletes the second source of truth and
//! restores `update_key_range`'s documented precondition — rather than a live
//! wrong-output bug with a constructible fixture. This test therefore guards
//! the invariant going FORWARD; it is NOT red-without-the-fix, and saying so
//! explicitly is part of the point.

use super::{rebuild_components_capturing_stats, Component, RebuildOptions};
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::sstable::writer::{SSTableWriter, StatisticsMetadata};
use crate::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, PartitionTombstone, TableId,
};
use crate::types::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KEYSPACE: &str = "test_ks";
const TABLE: &str = "keyrange_table";
const ROW_TIMESTAMP: i64 = 1_759_713_125_977_357;
const DELETE_TIMESTAMP: i64 = 1_759_713_125_977_400;

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

fn live_row(pk: i32) -> Mutation {
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

/// A `DELETE FROM t WHERE pk = ?` — a partition whose Data.db content is a
/// deletion marker and nothing else.
fn partition_delete(pk: i32) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        Vec::new(),
        DELETE_TIMESTAMP,
        None,
    );
    m.partition_tombstone = Some(PartitionTombstone {
        deletion_time: DELETE_TIMESTAMP,
        local_deletion_time: 1_759_713_125,
    });
    m
}

/// Write a generation whose LOWEST-token partition carries only a partition
/// tombstone, followed by ordinary live partitions. Returns its `Data.db`
/// path plus every partition key in on-disk (token) order.
fn write_generation(dir: &Path) -> (PathBuf, Vec<Vec<u8>>) {
    let schema = schema();
    let mut writer = SSTableWriter::new(dir.to_path_buf(), 1, &schema).expect("writer");

    // Token order, not `pk` order, decides on-disk order.
    let mut keys: Vec<_> = (0..4)
        .map(|pk| live_row(pk).decorated_key(&schema).expect("decorated key"))
        .collect();
    keys.sort_by_key(|k| k.token);

    let mut on_disk_order: Vec<Vec<u8>> = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let pk = i32::from_be_bytes([key.key[0], key.key[1], key.key[2], key.key[3]]);
        let mutations = if i == 0 {
            vec![partition_delete(pk)]
        } else {
            vec![live_row(pk)]
        };
        on_disk_order.push(key.key.clone());
        writer
            .write_partition(key.clone(), mutations)
            .expect("write_partition");
    }
    let info = tokio::runtime::Handle::current()
        .block_on(async { writer.finish().await })
        .expect("finish");
    (info.data_path, on_disk_order)
}

#[tokio::test(flavor = "multi_thread")]
async fn statistics_key_range_comes_from_the_counted_population() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("gen");
    std::fs::create_dir_all(&root).expect("create gen dir");
    let (data_db, on_disk_order) = tokio::task::spawn_blocking({
        let root = root.clone();
        move || write_generation(&root)
    })
    .await
    .expect("join");

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

    // THE POPULATION. Every partition of this fixture survives reconciliation
    // (module doc, reachability note), so `partition_count` must equal the
    // full on-disk set — anything less means the fixture is measuring a
    // different population than the two assertions below believe.
    assert_eq!(
        stats.partition_count as usize,
        on_disk_order.len(),
        "every partition here — including the partition-tombstone-only one — must be counted"
    );
    assert_eq!(
        stats.first_key.as_deref(),
        Some(&on_disk_order[0][..]),
        "first_key must be the first COUNTED partition"
    );
    assert_eq!(
        stats.last_key.as_deref(),
        Some(&on_disk_order[on_disk_order.len() - 1][..]),
        "last_key must be the last COUNTED partition"
    );
    // Live control: the DELETE partition really is present and really was
    // folded, so the population above is not trivially "the live rows only".
    assert!(
        stats.has_partition_level_deletions,
        "the DELETE partition must have been folded; without it this fixture degenerates into \
         four ordinary live partitions"
    );
}
