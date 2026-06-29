//! Issue #993 — wide-partition promoted-index boundary parity (end-to-end).
//!
//! Writes a single partition larger than the 64 KiB column-index granularity
//! through the real `WriteEngine`, flushes to a BIG ("nb") SSTable, then reads the
//! produced `Index.db` back through `IndexReader`. The wide-partition entry MUST
//! carry a promoted index whose payload was CAPTURED (not decoded-away/discarded)
//! and whose IndexInfo block offsets are recoverable and monotonically increasing.
//!
//! This exercises the full writer → reader round-trip for the promoted index that
//! the read path previously skipped. Synthetic (no datasets required).

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

/// `pk int, ck int, blob_col text` — a clustering table whose rows we pack into a
/// single partition until it crosses 64 KiB.
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
    let pk = PartitionKey::single("pk", Value::Integer(1)); // ALL rows in one partition
    let cluster = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "blob_col".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, pk, Some(cluster), ops, ts, None)
}

#[tokio::test]
async fn wide_partition_emits_capturable_promoted_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = WriteEngineConfig::new(tmp.path().join("data"), tmp.path().join("wal"), schema());
    let mut engine = WriteEngine::new(config).unwrap();

    // ~512 bytes of payload per row × 300 rows ≈ 150 KiB → well over the 64 KiB
    // column-index granularity, forcing two or more IndexInfo blocks.
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

    let index_path = info
        .index_path
        .expect("BIG format must produce an Index.db");

    // ── Read the Index.db back through the production reader ──
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
        .expect("wide partition must carry a promoted index (Issue #993)");

    // The raw payload was captured, not discarded.
    assert!(
        !promoted.is_empty(),
        "promoted payload must be captured non-empty"
    );

    // The schema-free block count is recoverable and indicates 2+ blocks.
    let count = promoted.block_count();
    assert!(
        count >= 2,
        "wide partition must produce >=2 IndexInfo blocks, got {count}"
    );

    // Full decode: clustering key is a single `int` (4 fixed bytes). Each promoted-
    // index `ClusteringPrefix` is serialized in Cassandra's `IndexInfo` form
    // (`ClusteringPrefix.serializer.serialize`), which prepends the `Kind.ordinal()`
    // byte: `[kind 0x04 CLUSTERING][values-header 0x00][4 value bytes]` = **6 bytes**.
    // This matches the real Cassandra 5.0 NB wide-partition fixture byte-for-byte
    // (see CK_PREFIX_LEN == 6 in issue_993_wide_partition_promoted_index_parity.rs).
    // Issue #1186 reconciled the writer to emit this 6-byte form — previously it
    // emitted only the 5-byte values-only Data.db row prefix (missing the leading
    // 0x04 CLUSTERING kind byte), making CQLite-written promoted indexes byte-
    // incompatible with Cassandra. This round-trip now proves writer↔reader self-
    // consistency AT THE CASSANDRA WIDTH; byte-for-byte writer parity with Cassandra
    // is asserted in issue_1186_promoted_index_clustering_prefix_parity.rs.
    let prefix_len = |slice: &[u8]| -> cqlite_core::Result<usize> {
        if slice.len() < 6 {
            return Err(cqlite_core::error::Error::Corruption(
                "short int clustering prefix".to_string(),
            ));
        }
        Ok(6)
    };
    let decoded = promoted.decode(&prefix_len).expect("decode promoted index");

    assert_eq!(decoded.count as usize, decoded.entries.len());
    assert_eq!(decoded.offsets.len(), decoded.entries.len());

    // First block starts at offset 0 in the IndexInfo region.
    assert_eq!(decoded.offsets[0], 0, "first IndexInfo block starts at 0");

    // Per-partition block offsets are strictly increasing and bounds are ordered.
    for w in decoded.entries.windows(2) {
        assert!(
            w[1].offset > w[0].offset,
            "block offsets must be monotonically increasing: {} !> {}",
            w[1].offset,
            w[0].offset
        );
    }
    // Trailing offsets array entries are non-decreasing.
    for w in decoded.offsets.windows(2) {
        assert!(
            w[1] >= w[0],
            "IndexInfo offsets array must be non-decreasing"
        );
    }

    engine.close().await.ok();
}
