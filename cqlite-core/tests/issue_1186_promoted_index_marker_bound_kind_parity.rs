//! Issue #1186 (roborev MEDIUM) — promoted-index **marker** bound-kind parity.
//!
//! # The question this suite answers
//!
//! A range-tombstone **marker** is an unfiltered too. When a marker becomes an
//! IndexInfo block's `firstName`/`lastName`, Cassandra serializes the marker's
//! **actual bound kind** ordinal via `ClusteringBoundOrBoundary.Serializer`, NOT
//! the row `CLUSTERING` kind (`0x04`). The authoritative Cassandra 5.0
//! `ClusteringPrefix.Kind` ordinals (`org.apache.cassandra.db.ClusteringPrefix`)
//! used on disk for markers are:
//!
//! ```text
//! 0 = EXCL_END_BOUND      1 = INCL_START_BOUND
//! 6 = INCL_END_BOUND      7 = EXCL_START_BOUND
//! ```
//!
//! (`2 = EXCL_END_INCL_START_BOUNDARY`, `3 = STATIC_CLUSTERING`, `4 = CLUSTERING`,
//! `5 = INCL_END_EXCL_START_BOUNDARY`.) These are the SAME ordinals the writer
//! already emits on-disk in `DataWriter::write_range_bound` (verified against the
//! Cassandra range-marker fixtures under issue #717), so this suite reuses them as
//! the oracle: the promoted-index marker name's kind byte MUST equal the on-disk
//! marker's kind byte.
//!
//! # Oracle
//!
//! We assert against the **authoritative Cassandra `ClusteringPrefix.Kind`
//! ordinal** directly (documented above; identical to the on-disk
//! `write_range_bound` constants the project verified byte-for-byte against real
//! Cassandra range-tombstone fixtures). No Cassandra fixture is known to place a
//! marker *exactly* on a promoted-index block boundary, so we construct that
//! situation deterministically (a range tombstone whose inclusive-START bound sits
//! at the very first clustering value sorts BEFORE the first row and therefore
//! becomes block 0's `firstName`). This is not a self-referential round-trip: the
//! expected byte (`INCL_START_BOUND = 1`) is the Cassandra ordinal, and the
//! pre-fix writer emitted `0x04` (CLUSTERING) here, which this test rejects.
//!
//! Before the fix: marker names reused `serialize_clustering_prefix_for_index`
//! (kind `0x04`). After: markers route through `serialize_marker_bound_prefix_for_index`
//! (kind = the bound ordinal). FAILS-before / PASSES-after.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use cqlite_core::Config;

/// `ClusteringPrefix.Kind.INCL_START_BOUND.ordinal()` in Cassandra 5.0 — the kind
/// of an inclusive range-tombstone START (open) bound on disk.
const INCL_START_BOUND: u8 = 1;
/// `ClusteringPrefix.Kind.CLUSTERING.ordinal()` — the (wrong-for-markers) kind the
/// pre-fix writer emitted.
const CLUSTERING_KIND_BYTE: u8 = 0x04;

/// `pk int, ck int, blob_col text`: one wide partition of int-clustered rows.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "wide_rt".to_string(),
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

fn row_mutation(ck: i32, payload: &str, ts: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "wide_rt");
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let cluster = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "blob_col".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, pk, Some(cluster), ops, ts, None)
}

/// A range-tombstone-only mutation on the SAME partition, covering
/// `[Inclusive(start), Inclusive(end)]`. The inclusive-START bound at the first
/// clustering value sorts before the first row → becomes block 0's `firstName`.
fn range_tombstone_mutation(start: i32, end: i32, ts: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "wide_rt");
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let mut m = Mutation::new(table_id, pk, None, Vec::new(), ts, None);
    m.range_tombstones = vec![RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(start))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(end))),
        deletion_time: ts,
        local_deletion_time: (ts / 1_000_000) as i32,
    }];
    m
}

/// Authoritative, kind-aware promoted-index `ClusteringPrefix` length for this
/// single-`int` schema, mirroring Cassandra `ClusteringPrefix.serializer.serialize`
/// (`ClusteringPrefix.java:462-475`):
///
/// - A CLUSTERING (row) name is `Clustering.serializer` form:
///   `[kind 1B][values-without-size]` = `[0x04][header 1B][int 4B]` = **6 bytes**.
/// - A BOUND name (range-tombstone marker) is `ClusteringBoundOrBoundary.serializer`
///   form (`ClusteringBoundOrBoundary.java:103-108`):
///   `[kind 1B][size 2B BE][values-without-size]`. For a single-`int` bound that is
///   `[kind][00 01][header 1B][int 4B]` = **8 bytes**; for an empty bound (size 0)
///   it is `[kind][00 00]` = **3 bytes** (no header/values follow).
///
/// This is the schema-driven length a production caller computes — there is no
/// heuristic fallback (Issue #28). It dispatches on the leading kind byte exactly
/// as Cassandra's `IndexInfo.Serializer.deserialize` does.
fn cassandra_prefix_len(slice: &[u8]) -> cqlite_core::Result<usize> {
    let corrupt = |need: usize| {
        cqlite_core::error::Error::Corruption(format!(
            "promoted-index prefix needs {need} bytes, slice has {}",
            slice.len()
        ))
    };
    let &kind = slice.first().ok_or_else(|| corrupt(1))?;
    if kind == CLUSTERING_KIND_BYTE {
        // [kind][header 1B][int 4B]
        const LEN: usize = 6;
        if slice.len() < LEN {
            return Err(corrupt(LEN));
        }
        return Ok(LEN);
    }
    // Bound: [kind][size 2B BE][...]
    if slice.len() < 3 {
        return Err(corrupt(3));
    }
    let size = u16::from_be_bytes([slice[1], slice[2]]);
    if size == 0 {
        return Ok(3); // empty bound: kind + size short only
    }
    // One int value: header 1B + int 4B follow the size short.
    let len = 3 + 1 + 4 * size as usize;
    if slice.len() < len {
        return Err(corrupt(len));
    }
    Ok(len)
}

#[tokio::test]
async fn promoted_index_marker_name_uses_bound_kind_not_clustering() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = WriteEngineConfig::new(tmp.path().join("data"), tmp.path().join("wal"), schema());
    let mut engine = WriteEngine::new(config).unwrap();

    // ~512 bytes × 300 rows ≈ 150 KiB → several IndexInfo blocks.
    let payload = "x".repeat(512);
    for ck in 0..300i32 {
        engine
            .write(row_mutation(ck, &payload, 1_000 + ck as i64))
            .unwrap();
    }
    // Range tombstone whose inclusive-START bound is at ck=0 (the first row's
    // clustering value). It sorts before the row → block 0 firstName is this
    // marker bound. Its on-disk kind is INCL_START_BOUND (1).
    //
    // The RT deletion_time (500) is BELOW every row timestamp (1000..1300) so it
    // shadows NO rows — the partition stays wide (>=2 IndexInfo blocks) AND the
    // marker is still emitted as an unfiltered, exercising the promoted-index
    // marker path. (A higher RT timestamp would purge the rows and collapse the
    // promoted index — see DataWriter::merge_clustering_rows shadow_floor.)
    engine.write(range_tombstone_mutation(0, 299, 500)).unwrap();

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
    let decoded = promoted
        .decode(&cassandra_prefix_len)
        .expect("promoted index must decode with the Cassandra 6-byte prefix");
    assert!(
        decoded.count >= 2,
        "wide partition must produce >=2 IndexInfo blocks, got {}",
        decoded.count
    );

    // Block 0's firstName is the inclusive-START range-tombstone marker bound.
    // Cassandra serializes a BOUND name via `ClusteringBoundOrBoundary.serializer`
    // (`ClusteringBoundOrBoundary.java:103-108`): the kind byte, then a 2-byte
    // big-endian `size` short (number of clustering values), then the
    // values-without-size blob. For a single-`int` bound that is the 8 bytes
    // `[kind 1B][size 2B][header 1B][int 4B]` — NOT the 6-byte
    // CLUSTERING/`Clustering.serializer` form (kind + header + int, no size short).
    // The 2-byte size short is the roborev MEDIUM fix: the pre-fix writer emitted
    // the Clustering form (no size short) for bound names, one short too few.
    let first_name = &decoded.entries[0].first_name;
    assert_eq!(
        first_name.len(),
        8,
        "block 0 firstName (single-int marker bound) must be 8 bytes \
         [kind][size 2B][header][int], got {}: {first_name:02x?}",
        first_name.len()
    );
    assert_ne!(
        first_name[0], CLUSTERING_KIND_BYTE,
        "block 0 firstName is a range-tombstone marker; its kind byte must NOT be \
         CLUSTERING ({CLUSTERING_KIND_BYTE:#04x}) — that is the pre-fix bug: {first_name:02x?}"
    );
    assert_eq!(
        first_name[0], INCL_START_BOUND,
        "block 0 firstName must carry the Cassandra INCL_START_BOUND ordinal \
         ({INCL_START_BOUND:#04x}) for an inclusive-start range-tombstone bound, got {:#04x}: \
         {first_name:02x?}",
        first_name[0]
    );
    let size = u16::from_be_bytes([first_name[1], first_name[2]]);
    assert_eq!(
        size, 1,
        "block 0 firstName bound size short must be 1 (one clustering value): {first_name:02x?}"
    );
    assert_eq!(
        first_name[3], 0x00,
        "block 0 firstName values header must be 0x00 (single PRESENT column): {first_name:02x?}"
    );
    let value = i32::from_be_bytes([first_name[4], first_name[5], first_name[6], first_name[7]]);
    assert_eq!(
        value, 0,
        "block 0 firstName bound value must be ck=0, got {value}"
    );

    engine.close().await.ok();
}
