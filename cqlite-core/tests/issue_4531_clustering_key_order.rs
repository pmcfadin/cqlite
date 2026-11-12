//! Issue #4534 — the WRITE/COMPACTION-side clustering order for `uuid` / `timeuuid`.
//!
//! `ClusteringKey::compare(other, schema)` is the comparator `write_engine::merge` sorts merged
//! rows with (compaction output order, `SchemaOrderedEntry`), and the memtable flush uses it to
//! order a partition's rows before writing `Data.db`. Before the fix it compared `Value::Uuid`
//! RAW BYTES, so CQLite-written (flushed or compacted) SSTables carried uuid/timeuuid clustering
//! rows in an order Cassandra rejects (`sstable verify`: out-of-order rows).
//!
//! Oracle: the Cassandra-written fixture's `sstabledump -l` golden lists each partition's rows in
//! Cassandra's clustering order. Sorting the SHUFFLED keys with `ClusteringKey::compare` must
//! reproduce that order exactly, for ASC and DESC, `uuid` (mixed v1/v4) and `timeuuid`. A negative
//! control proves the raw-byte order differs from the golden on the same data, i.e. the pre-fix
//! comparator would have mis-ordered it.

use std::path::PathBuf;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::mutation::ClusteringKey;
use cqlite_core::types::Value;

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_uuid_bti";
/// `(table, cql type, order)`.
const TABLES: [(&str, &str, ClusteringOrder); 3] = [
    ("timeuuid_clustering", "timeuuid", ClusteringOrder::Asc),
    (
        "timeuuid_clustering_desc",
        "timeuuid",
        ClusteringOrder::Desc,
    ),
    ("uuid_clustering", "uuid", ClusteringOrder::Asc),
];

fn golden_keys(table: &str) -> Vec<[u8; 16]> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4531");
    let dirs = datasets_root::table_generation_dirs(&root, KEYSPACE, table);
    assert_eq!(dirs.len(), 1, "{table}: one generation dir");
    let text = std::fs::read_to_string(dirs[0].join("da-2-bti-Data.db.jsonl")).expect("golden");
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("json");
        if v["partition"]["key"][0] != "1" {
            continue;
        }
        for r in v["rows"].as_array().expect("rows") {
            if r["type"] == "row" {
                let s = r["clustering"][0].as_str().expect("ck").replace('-', "");
                let mut u = [0u8; 16];
                for (i, b) in u.iter_mut().enumerate() {
                    *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex");
                }
                out.push(u);
            }
        }
    }
    assert_eq!(out.len(), 480, "{table}: golden pk=1 rows");
    out
}

fn schema(cql_type: &str, order: ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: cql_type.into(),
            position: 0,
            order,
        }],
        columns: vec![Column {
            name: "payload".into(),
            data_type: "text".into(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn key(u: [u8; 16]) -> ClusteringKey {
    ClusteringKey::single("ck", Value::Uuid(u))
}

/// Deterministic shuffle (no RNG dependency): sort by a multiplicative hash of the index.
fn shuffled(keys: &[[u8; 16]]) -> Vec<[u8; 16]> {
    let mut v: Vec<(u64, [u8; 16])> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| ((i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15), *k))
        .collect();
    v.sort_by_key(|(h, _)| *h);
    v.into_iter().map(|(_, k)| k).collect()
}

#[test]
fn schema_aware_clustering_compare_reproduces_cassandra_order() {
    for (table, cql_type, order) in TABLES {
        let golden = golden_keys(table);
        let schema = schema(cql_type, order.clone());
        let mut v = shuffled(&golden);
        assert_ne!(v, golden, "{table}: shuffle must actually permute");
        v.sort_by(|a, b| key(*a).compare(&key(*b), &schema).expect("compare"));
        assert_eq!(
            v, golden,
            "{table}: ClusteringKey::compare order != Cassandra's clustering order"
        );

        // negative control: the pre-fix raw-byte order is NOT Cassandra's order here.
        let mut raw = golden.clone();
        raw.sort();
        if order == ClusteringOrder::Desc {
            raw.reverse();
        }
        assert_ne!(
            raw, golden,
            "{table}: raw byte order coincides with Cassandra's - fixture would not detect the bug"
        );
    }
}
