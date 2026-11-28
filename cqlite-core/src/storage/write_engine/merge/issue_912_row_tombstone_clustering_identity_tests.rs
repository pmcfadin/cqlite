//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::storage::sstable::reader::compaction_row::{
    CompactionRow, CompactionRowData, SimpleCell,
};
use crate::types::RowKey;
use std::collections::HashMap;

fn clustered_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks912".to_string(),
        table: "t912".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn row_tombstone(ck: i64) -> CompactionRowData {
    CompactionRowData::Tombstone {
        deletion_time: 1_000 + ck,
        local_deletion_time: 42,
        clustering: vec![("ck".to_string(), Value::Integer(ck as i32))],
    }
}

/// Core fix: a row tombstone's captured clustering prefix yields a concrete
/// `ClusteringKey`, and two tombstones at distinct clustering keys produce
/// DISTINCT keys (pre-#912 both returned `None`).
#[test]
fn tombstone_clustering_identity_is_distinct() {
    let schema = clustered_schema();

    let ck5 = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
        &row_tombstone(5),
        &schema,
    );
    let ck9 = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
        &row_tombstone(9),
        &schema,
    );

    assert_eq!(
        ck5,
        Some(ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(5))],
        }),
        "row tombstone must carry its clustering identity (#912)"
    );
    assert_ne!(
        ck5, ck9,
        "distinct clustering-row tombstones must not share a bucket"
    );
}

/// A tombstone with no captured clustering (a static row, or an unclustered
/// table; a PARTIAL prefix is refused upstream, #3809) keeps the `None` bucket.
#[test]
fn tombstone_without_clustering_falls_into_none_bucket() {
    let schema = clustered_schema();
    let bare = CompactionRowData::Tombstone {
        deletion_time: 1,
        local_deletion_time: 0,
        clustering: Vec::new(),
    };
    assert_eq!(
        SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(&bare, &schema),
        None
    );
}

/// A live clustering row still resolves its clustering key from its surfaced
/// simple cells (unchanged path), matching the tombstone's key for the same
/// `ck` so they share a reconcile bucket.
#[test]
fn live_and_tombstone_same_ck_share_bucket() {
    let schema = clustered_schema();
    let live = CompactionRowData::Live {
        simple: vec![SimpleCell {
            column: "ck".to_string(),
            value: Value::Integer(5),
            timestamp: 10,
            ttl: None,
            local_deletion_time: None,
        }],
        complex: Vec::new(),
        row_deletion: None,
        row_liveness: Default::default(),
    };
    let live_ck = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(&live, &schema);
    let tomb_ck = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
        &row_tombstone(5),
        &schema,
    );
    assert_eq!(
        live_ck, tomb_ck,
        "same ck => same bucket for live row and its tombstone"
    );
    assert!(live_ck.is_some());
}

/// End-to-end through `build_merge_entry`: two row-tombstone `CompactionRow`s
/// in one partition produce two MergeEntries in DISTINCT clustering buckets, so
/// `merge_partition_rows` keeps BOTH tombstones. Pre-#912 both collapsed into
/// the `None` bucket and only one survived.
#[test]
fn two_row_tombstones_do_not_collapse_in_merge() {
    let schema = clustered_schema();
    let pk = RowKey::new(vec![0, 0, 0, 7]);

    let e5 = SSTableRowIteratorAdapter::build_merge_entry(
        0,
        CompactionRow {
            key: pk.clone(),
            row_timestamp: 1_005,
            row_data: row_tombstone(5),
        },
        &schema,
    )
    .expect("build_merge_entry");
    let e9 = SSTableRowIteratorAdapter::build_merge_entry(
        1,
        CompactionRow {
            key: pk.clone(),
            row_timestamp: 1_009,
            row_data: row_tombstone(9),
        },
        &schema,
    )
    .expect("build_merge_entry");

    assert_ne!(
        e5.clustering_key, e9.clustering_key,
        "two distinct row tombstones must land in distinct buckets (#912)"
    );

    let mut merger = KWayMerger {
        runs: vec![],
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
    let merged = merger
        .merge_partition_rows(vec![e5, e9])
        .expect("merge_partition_rows");

    assert_eq!(
        merged.len(),
        2,
        "both clustering-row tombstones must survive; pre-#912 they collapsed to one"
    );
    assert!(
        merged
            .iter()
            .all(|m| matches!(m.row_data, RowData::Tombstone { .. })),
        "both surviving entries must be row tombstones"
    );
}
