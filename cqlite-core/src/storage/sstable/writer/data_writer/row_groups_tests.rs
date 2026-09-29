//! Unit tests for [`super`] (`row_groups.rs`) — issue #4246 roborev round-8
//! finding 2.
//!
//! These pin the three properties the six former copies each relied on
//! implicitly, so the shared extraction cannot regress one of them silently.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, PartitionKey, TableId,
};
use crate::types::Value;

fn col(name: &str, ty: &str, is_static: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static,
    }
}

/// A STATIC-BEARING schema, so `is_static_row_mutation` can actually classify
/// a `clustering_key: None` mutation as a static carrier.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int", false),
            col("ck", "int", false),
            col("v", "text", false),
            col("s", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn ck(n: i32) -> ClusteringKey {
    ClusteringKey::single("ck", Value::Integer(n))
}

fn row(n: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ck(n)),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("x"),
        }],
        ts,
        None,
    )
}

fn static_carrier(ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "s".to_string(),
            value: Value::text("s"),
        }],
        ts,
        None,
    )
}

fn rt(from: i32, to: i32, deletion_time: i64) -> RangeTombstone {
    RangeTombstone {
        start: ClusteringBound::Inclusive(ck(from)),
        end: ClusteringBound::Inclusive(ck(to)),
        deletion_time,
        local_deletion_time: 2_000_000_000,
    }
}

/// The LOAD-BEARING property: `clustering_row_mutations` partitions a
/// partition's mutations into exactly two disjoint, exhaustive sets — the
/// clustering rows it returns, and the static carriers it drops. If the two
/// ever overlapped, a mutation would be folded into persisted statistics
/// twice (the clustering-group fold AND the static-carrier fold), and
/// `update_local_deletion_time` is not idempotent.
#[test]
fn clustering_row_mutations_partitions_disjointly_and_exhaustively() {
    let schema = schema();
    let mutations = vec![
        row(1, 10),
        static_carrier(20),
        row(2, 30),
        static_carrier(40),
    ];

    let kept = clustering_row_mutations(&mutations, &schema);
    assert_eq!(
        kept.len(),
        2,
        "only the two clustering rows survive the filter"
    );
    assert!(
        kept.iter().all(|m| m.clustering_key.is_some()),
        "a static carrier must never appear in the clustering-row set"
    );

    let dropped = mutations
        .iter()
        .filter(|m| is_static_row_mutation(m, &schema))
        .count();
    assert_eq!(
        kept.len() + dropped,
        mutations.len(),
        "the two sets must be exhaustive — a mutation in NEITHER would have \
         its statistics folded nowhere at all"
    );
}

/// `resolve_shadow_floor` takes the MAXIMUM over the partition floor and
/// every COVERING range tombstone, and ignores non-covering ones.
#[test]
fn resolve_shadow_floor_takes_the_max_of_covering_tombstones_only() {
    let schema = schema();
    let key = ck(5);
    let tombstones = vec![
        rt(1, 3, 999), // does NOT cover ck=5
        rt(4, 6, 50),  // covers, lower
        rt(5, 9, 70),  // covers, higher — the winner
    ];

    assert_eq!(
        resolve_shadow_floor(None, &tombstones, Some(&key), &schema),
        Some(70),
        "the highest COVERING range tombstone wins; the non-covering @999 \
         must be ignored"
    );
    assert_eq!(
        resolve_shadow_floor(Some(100), &tombstones, Some(&key), &schema),
        Some(100),
        "a higher partition floor dominates every covering range tombstone"
    );
    assert_eq!(
        resolve_shadow_floor(None, &[], Some(&key), &schema),
        None,
        "nothing shadowing the key yields no floor at all (NOT a zero floor, \
         which would shadow every non-positive timestamp)"
    );
    assert_eq!(
        resolve_shadow_floor(None, &tombstones, Some(&ck(2)), &schema),
        Some(999),
        "ck=2 is covered by the FIRST tombstone only"
    );
}

/// Grouping is by ADJACENCY over an already-sorted slice, and each group is
/// visited with its OWN shadow floor.
#[test]
fn for_each_clustering_row_group_groups_adjacently_with_per_group_floors() {
    let schema = schema();
    let mutations = vec![row(1, 10), row(1, 20), row(2, 30), row(5, 40)];
    let kept = clustering_row_mutations(&mutations, &schema);
    let tombstones = vec![rt(1, 1, 77)];

    let mut seen: Vec<(usize, Option<i64>)> = Vec::new();
    for_each_clustering_row_group(&kept, &schema, None, &tombstones, |group, floor| {
        seen.push((group.len(), floor));
    });

    assert_eq!(
        seen,
        vec![(2, Some(77)), (1, None), (1, None)],
        "ck=1's two adjacent mutations form ONE group carrying the covering \
         tombstone's floor; ck=2 and ck=5 are separate groups with none"
    );
}

/// An empty clustering-row set visits nothing — the degenerate partition
/// (statics and/or markers only) must not panic on `group[0]`.
#[test]
fn for_each_clustering_row_group_visits_nothing_when_there_are_no_rows() {
    let schema = schema();
    let mutations = vec![static_carrier(10)];
    let kept = clustering_row_mutations(&mutations, &schema);

    let mut visits = 0usize;
    for_each_clustering_row_group(&kept, &schema, Some(5), &[], |_, _| visits += 1);
    assert_eq!(visits, 0, "0 RECOGNISED clustering-row groups");
}
