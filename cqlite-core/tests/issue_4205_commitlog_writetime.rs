//! Issue #4205 — CommitLog writetime / position decode against REAL Cassandra-written
//! segments (the CLI tests cover the rendered report; this pins the library surface).
//!
//! Oracles:
//!   * `commitlog_find` fixture (cassandra:5.0.8, explicit `USING TIMESTAMP`): every
//!     `users` mutation's reconstructed writetime EQUALS the inserted timestamp and its
//!     `MutationIter` position EQUALS the position an independent Python frame walker
//!     recorded — the proof that `min_timestamp(stats + TIMESTAMP_EPOCH) + delta` is the real
//!     convention, not CQLite agreeing with itself;
//!   * the older `commitlog` fixture (no explicit timestamps): every writetime lies in the
//!     window after the segment's own creation time (the segment id is a creation timestamp
//!     in ms) — a bound that is wrong by 40 years if the epoch were forgotten.

#![cfg(all(feature = "state_machine", not(feature = "tombstones")))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::storage::commitlog::{
    mutation_writetime, parse_table_id, ColumnSpec, CommitLogReader, CommitLogSchema, SchemaSet,
    Writetime,
};
use serde_json::Value;

fn td() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
}

fn schemas(table_id: &str) -> (SchemaSet, [u8; 16]) {
    let id = parse_table_id(table_id).expect("table id");
    let mut set: SchemaSet = HashMap::new();
    set.insert(
        id,
        CommitLogSchema {
            keyspace: "commitlog_test".into(),
            table: "users".into(),
            partition_key: vec![ColumnSpec::new("id", "int")],
            clustering: vec![],
            columns: vec![
                ColumnSpec::new("name", "text"),
                ColumnSpec::new("age", "int"),
            ],
        },
    );
    (set, id)
}

fn read_json(p: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(p).unwrap_or_else(|e| panic!("{p:?}: {e}")))
        .expect("json")
}

#[test]
fn commitlog_find_fixture_writetimes_and_positions_equal_the_ground_truth() {
    let root = td().join("commitlog_find");
    let gt = read_json(&root.join("commitlog-find-ground-truth.json"));
    let (set, id) = schemas(gt["table_id"].as_str().expect("table_id"));
    let seg = root
        .join("commitlog")
        .join(gt["segment"]["file"].as_str().expect("file"));
    let reader = CommitLogReader::open_with_schemas(&seg, set).expect("open segment");
    let mut got: Vec<(u64, i64, i32)> = Vec::new();
    for (position, res) in reader.mutations_with_positions() {
        let m = res.expect("decode");
        for u in m.updates.iter().filter(|u| u.table_id == id) {
            let key = i32::from_be_bytes(u.partition_key.clone().try_into().expect("int key"));
            match mutation_writetime(u) {
                Writetime::Known(t) => got.push((position.expect("record position"), t, key)),
                other => panic!("measurable insert reported {other:?}"),
            }
        }
    }
    let want: Vec<(u64, i64, i32)> = gt["inserts"]
        .as_array()
        .expect("inserts")
        .iter()
        .map(|i| {
            (
                i["position"].as_u64().expect("position"),
                i["timestamp"].as_i64().expect("timestamp"),
                i32::try_from(i["id"].as_i64().expect("id")).expect("id fits"),
            )
        })
        .collect();
    assert_eq!(want.len(), 6, "fixture shape");
    assert_eq!(got, want);
}

#[test]
fn older_fixture_writetimes_follow_the_segment_creation_time() {
    let root = td().join("commitlog");
    let gt = read_json(&root.join("commitlog-ground-truth.json"));
    let (set, id) = schemas(gt["table_id"].as_str().expect("table_id"));
    let seg_id_us = gt["segment"]["id"].as_i64().expect("segment id") * 1000;
    let file = gt["segment"]["files"]["clean"]
        .as_str()
        .expect("clean file");
    let reader = CommitLogReader::open_with_schemas(root.join(file), set).expect("open");
    let mut times = Vec::new();
    for (_, res) in reader.mutations_with_positions() {
        for u in res
            .expect("decode")
            .updates
            .iter()
            .filter(|u| u.table_id == id)
        {
            match mutation_writetime(u) {
                Writetime::Known(t) => times.push(t),
                other => panic!("measurable insert reported {other:?}"),
            }
        }
    }
    assert_eq!(times.len(), 5, "five inserts");
    assert!(
        times.windows(2).all(|w| w[0] <= w[1]),
        "file order is write order: {times:?}"
    );
    for t in &times {
        assert!(
            (seg_id_us..=seg_id_us + 600_000_000).contains(t),
            "writetime {t} outside [segment creation, +10min] of {seg_id_us}"
        );
    }
}
