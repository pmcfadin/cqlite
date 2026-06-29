//! Issue #1186 — promoted-index clustering-prefix header width parity.
//!
//! # The question this suite answers
//!
//! For an identical logical schema (single `ck int` clustering column), the
//! REAL Cassandra 5.0 wide-partition fixture
//! `test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Index.db`
//! serializes each promoted-index `firstName`/`lastName` clustering prefix as a
//! fixed **6 bytes**:
//!
//! ```text
//! 04 00 <4-byte big-endian int>
//! └┬ └┬ └──────┬──────┘
//!  │  │        └─ the clustering value (raw int, no length prefix; fixed width)
//!  │  └────────── the 1-byte clustering "values header" (2 bits/column;
//!  │              0x00 == one PRESENT column)
//!  └───────────── ClusteringPrefix.Kind.ordinal() == CLUSTERING == 4
//! ```
//!
//! Verified directly from the fixture bytes: every one of the 10 IndexInfo
//! blocks (×3 partitions) begins each name with the constant prefix `04 00`,
//! then the 4-byte int. (See the hand-decode in the issue investigation.)
//!
//! This is exactly what Cassandra `IndexInfo.Serializer.serialize()` emits: it
//! serializes `firstName`/`lastName` via `ClusteringPrefix.serializer.serialize`,
//! which writes a **leading kind byte** (`Kind.ordinal()`), unlike the Data.db
//! row clustering prefix (written by the values-only `Clustering.serializer`,
//! NO kind byte).
//!
//! Before the #1186 fix, CQLite's writer reused the Data.db row clustering-prefix
//! form (`serialize_clustering_prefix_to_vec` → `00 <int>` = 5 bytes) for the
//! promoted-index names, OMITTING the leading `0x04` CLUSTERING kind byte. That
//! made CQLite-written wide-partition `Index.db` promoted indexes byte-incompatible
//! with Cassandra (a Cassandra reader would mis-frame every clustering prefix).
//!
//! This suite drives the production `WriteEngine` to flush a real wide partition
//! and asserts the promoted-index clustering prefix is the Cassandra 6-byte
//! `[04][00][int]` form — byte-for-byte. It FAILS on the 5-byte divergence and
//! passes once the writer prepends the CLUSTERING kind byte.
//!
//! Reference: the real Cassandra fixture above (the parity oracle). The decoded
//! per-block clustering ints (0,41 / 42,73 / … see fixture) are reproduced by the
//! writer for the same payload shape; here we only need the prefix *framing*.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use cqlite_core::Config;

/// `ClusteringPrefix.Kind.CLUSTERING.ordinal()` in Cassandra 5.0
/// (`org.apache.cassandra.db.ClusteringPrefix.Kind`). A full clustering key
/// (row name) is always kind `CLUSTERING`; bounds use other ordinals (e.g.
/// `EXCL_END_INCL_START_BOUNDARY = 2`, `INCL_END_EXCL_START_BOUNDARY = 5`).
const CLUSTERING_KIND_BYTE: u8 = 0x04;

/// `pk int, ck int, blob_col text`: one wide partition of int-clustered rows.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "wide".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "blob_col".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn mutation(ck: i32, payload: &str, ts: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "wide");
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let cluster = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "blob_col".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, pk, Some(cluster), ops, ts, None)
}

/// Schema-driven prefix length for a single `int` clustering serialized in the
/// Cassandra promoted-index form: `[kind 1B][values-header 1B][int 4B]` = 6 bytes.
/// Authoritative (Issue #28), not a heuristic.
fn cassandra_ck_prefix_len(slice: &[u8]) -> cqlite_core::Result<usize> {
    const LEN: usize = 6;
    if slice.len() < LEN {
        return Err(cqlite_core::error::Error::Corruption(format!(
            "ck promoted-index prefix needs {LEN} bytes, slice has {}",
            slice.len()
        )));
    }
    Ok(LEN)
}

#[tokio::test]
async fn promoted_index_clustering_prefix_is_cassandra_6_byte_form() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = WriteEngineConfig::new(tmp.path().join("data"), tmp.path().join("wal"), schema());
    let mut engine = WriteEngine::new(config).unwrap();

    // ~512 bytes × 300 rows ≈ 150 KiB → several IndexInfo blocks.
    let payload = "x".repeat(512);
    for ck in 0..300i32 {
        engine
            .write(mutation(ck, &payload, 1_000 + ck as i64))
            .unwrap();
    }

    let info = engine
        .flush()
        .await
        .expect("flush failed")
        .expect("flush produced no SSTable");
    let index_path = info.index_path.expect("BIG format must produce Index.db");

    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("open Index.db");

    let entries = reader.get_partition_entries();
    assert_eq!(entries.len(), 1, "exactly one wide partition");

    let promoted = entries[0]
        .promoted_index
        .as_ref()
        .expect("wide partition must carry a promoted index");

    // Decode using the AUTHORITATIVE Cassandra 6-byte prefix length. This decode
    // itself fails if the writer emitted a 5-byte prefix (the block framing then
    // mis-aligns and either the prefix or the trailing offsets array is wrong).
    let decoded = promoted
        .decode(&cassandra_ck_prefix_len)
        .expect("promoted index must decode with the Cassandra 6-byte clustering prefix");

    assert!(
        decoded.count >= 2,
        "wide partition must produce >=2 IndexInfo blocks, got {}",
        decoded.count
    );

    // Byte-for-byte: every firstName/lastName is exactly `[04][00][4-byte int]`.
    for (n, block) in decoded.entries.iter().enumerate() {
        for (which, name) in [("first_name", &block.first_name), ("last_name", &block.last_name)]
        {
            assert_eq!(
                name.len(),
                6,
                "block {n} {which}: clustering prefix must be 6 bytes (Cassandra \
                 [kind][header][int]), got {} bytes: {name:02x?}",
                name.len()
            );
            assert_eq!(
                name[0], CLUSTERING_KIND_BYTE,
                "block {n} {which}: byte 0 must be the CLUSTERING kind {CLUSTERING_KIND_BYTE:#04x} \
                 (Cassandra ClusteringPrefix.serializer writes the kind byte), got {:#04x}: \
                 {name:02x?}",
                name[0]
            );
            assert_eq!(
                name[1], 0x00,
                "block {n} {which}: byte 1 must be the 1-byte clustering values header 0x00 \
                 (single PRESENT column), got {:#04x}: {name:02x?}",
                name[1]
            );
            // The remaining 4 bytes are the raw big-endian int clustering value.
            let v = i32::from_be_bytes([name[2], name[3], name[4], name[5]]);
            assert!(
                (0..300).contains(&v),
                "block {n} {which}: decoded ck {v} outside the written range 0..300"
            );
        }
    }

    // Block bounds are ordered exactly as Cassandra would frame them.
    for w in decoded.entries.windows(2) {
        let prev_last = i32::from_be_bytes([
            w[0].last_name[2],
            w[0].last_name[3],
            w[0].last_name[4],
            w[0].last_name[5],
        ]);
        let next_first = i32::from_be_bytes([
            w[1].first_name[2],
            w[1].first_name[3],
            w[1].first_name[4],
            w[1].first_name[5],
        ]);
        assert!(
            prev_last <= next_first,
            "block clustering bounds must be ordered: prev last {prev_last} > next first {next_first}"
        );
    }

    engine.close().await.ok();
}
