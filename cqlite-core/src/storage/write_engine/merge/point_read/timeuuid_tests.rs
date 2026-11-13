//! Issue #4534 — a point read over several generations whose clustering column is a `timeuuid`
//! must order each run by the SAME typed order the merge heap pops by.
//!
//! Two v1 UUIDs share ONE timestamp (identical msb) and differ only in the lsb, whose first
//! byte crosses 0x7f/0x80. `UUIDType` compares the lsb UNSIGNED (0x7f < 0x80) but
//! `TimeUUIDType` compares it SIGNED (0x80 = -128 < 0x7f), so Cassandra's `timeuuid` order is
//! `[B(0x80..), A(0x7f..)]` — the reverse of the schema-free order the runs used to be sorted
//! with. The generations hold different subsets of the rows (so the runs have different heads);
//! the newest generation holding each row wins, every row exactly once, in Cassandra order.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
use crate::storage::write_engine::merge::MergeStep;
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

/// v1 UUID, fixed timestamp fields, lsb first byte `lsb0`.
fn v1(lsb0: u8) -> [u8; 16] {
    let mut u = [0u8; 16];
    u[0..4].copy_from_slice(&0x1234_5678u32.to_be_bytes()); // time_low
    u[4..6].copy_from_slice(&0x9abcu16.to_be_bytes()); // time_mid
    u[6..8].copy_from_slice(&(0x1000u16 | 0x0def).to_be_bytes()); // version 1 + time_hi
    u[8] = lsb0;
    u[9..].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7]);
    u
}

fn schema() -> TableSchema {
    let col = |n: &str, t: &str| Column {
        name: n.to_string(),
        data_type: t.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
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
            data_type: "timeuuid".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![col("id", "int"), col("ck", "timeuuid"), col("name", "text")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write(engine: &mut WriteEngine, ck: [u8; 16], name: &str, ts: i64) {
    engine
        .write(Mutation::new(
            TableId::new("ks", "t"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Uuid(ck))),
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::text(name.to_string()),
            }],
            ts,
            None,
        ))
        .unwrap();
}

#[test]
fn equal_timestamp_timeuuids_point_read_once_each_in_cassandra_order() {
    let (a, b) = (v1(0x7f), v1(0x80));
    // The raw-byte and `uuid` order put `a` first; Cassandra's `timeuuid` order puts `b` first.
    assert!(a < b, "precondition: raw order is [a, b]");

    let temp = TempDir::new().unwrap();
    let schema = schema();
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut paths = Vec::new();
    for generation in 1..=3i64 {
        write(
            &mut engine,
            a,
            &format!("a-g{generation}"),
            generation * 1000,
        );
        write(
            &mut engine,
            b,
            &format!("b-g{generation}"),
            generation * 1000,
        );
        paths.push(rt.block_on(engine.flush()).unwrap().unwrap().data_path);
    }
    paths.reverse(); // newest first, the order the builders expect

    let key = PartitionKey::single("id", Value::Integer(1))
        .to_bytes(&schema)
        .unwrap();
    let merger = build_single_partition_merger(paths, &[key], &schema, ScanCancel::default())
        .expect("probe")
        .expect("key present");
    let mut merger = merger;
    let mut partitions = Vec::new();
    while let MergeStep::Partition { rows, .. } = merger.step().expect("merge step") {
        partitions.push(rows);
    }
    assert_eq!(partitions.len(), 1, "one partition, emitted once");
    let rows = &partitions[0];
    let cks: Vec<[u8; 16]> = rows
        .iter()
        .map(
            |r| match &r.clustering_key.as_ref().expect("ck").columns[0].1 {
                Value::Uuid(u) => *u,
                other => panic!("clustering value {other:?}"),
            },
        )
        .collect();
    assert_eq!(
        cks,
        vec![b, a],
        "each row exactly once, in Cassandra timeuuid order"
    );
    let names: Vec<String> = rows.iter().map(|r| format!("{:?}", r.row_data)).collect();
    assert!(
        names[0].contains("b-g3") && names[1].contains("a-g3"),
        "newest generation wins: {names:?}"
    );
}

/// The invariant the point-read builders rely on: a run handed to the schema-aware merge heap
/// must be ascending under THE HEAP'S order. The old schema-free `entries.sort()` is not (negative
/// control, asserted on purpose); `sort_entries_schema_ordered` is.
#[test]
fn run_presort_is_ascending_under_the_heap_order_not_the_schema_free_order() {
    use super::super::model::{CellData, MergeEntry, RowData};
    use super::super::schema_order::{schema_entry_cmp, sort_entries_schema_ordered};
    use crate::storage::write_engine::mutation::DecoratedKey;

    let (a, b) = (v1(0x7f), v1(0x80));
    let schema = schema();
    let entry = |ck: [u8; 16]| {
        MergeEntry::new(
            0,
            DecoratedKey::new(7, vec![7]),
            Some(ClusteringKey::single("ck", Value::Uuid(ck))),
            1000,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".into(),
                    Value::text("x".to_string()),
                    1000,
                )],
            },
        )
    };
    let is_heap_sorted = |v: &[MergeEntry]| {
        v.windows(2)
            .all(|w| schema_entry_cmp(&w[0], &w[1], &schema) != std::cmp::Ordering::Greater)
    };

    let mut schema_free = vec![entry(b), entry(a)];
    schema_free.sort();
    assert!(
        !is_heap_sorted(&schema_free),
        "negative control: the schema-free sort puts `a` first, which the typed heap does not"
    );

    let mut typed = vec![entry(a), entry(b)];
    sort_entries_schema_ordered(&mut typed, &schema);
    assert!(is_heap_sorted(&typed));
    let order: Vec<[u8; 16]> = typed
        .iter()
        .map(|e| match &e.clustering_key.as_ref().unwrap().columns[0].1 {
            Value::Uuid(u) => *u,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(order, vec![b, a]);
}
