//! CQLite-WRITTEN fixture builders shared by the #3058 forced-path differential
//! (campsite split of `issue_3058_forced_path_differential.rs`, epic #1135).
//!
//! These build SSTables through the real `WriteEngine`, so the differential can
//! exercise reconciliation shapes no committed Cassandra fixture carries (a partition
//! deletion, a range tombstone, a live-then-expiring TTL cell, a static-only
//! partition, a static partition whose only clustering row is deleted).
//!
//! SCOPE REMINDER, since these are CQLITE-written: their ROW CONTENT is never a
//! Cassandra oracle (it is invariant to a uniform serialization error, and subject to
//! the write-side #1074 for statics — issue #3042). They prove ARM-INVARIANCE over
//! bytes whose layout differs from Cassandra's. Cassandra-parity assertions live in
//! `query_semantics_flight_parity.rs` and `issue_3095_flight_static_columns.rs`.

#![allow(dead_code)]

use std::path::PathBuf;

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::mutation::{
    ClusteringBound, PartitionTombstone, RangeTombstone,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

pub const KS: &str = "diff_ks";
pub const TBL: &str = "shapes";
pub const DDL: &str =
    "CREATE TABLE diff_ks.shapes (pk int, ck int, v text, w text, PRIMARY KEY (pk, ck))";

pub fn shapes_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            col("v", "text", true),
            col("w", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

pub fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

pub fn base(pk: i32, ck: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        ops,
        ts,
        None,
    )
}

pub fn write_v(pk: i32, ck: i32, v: &str, ts: i64) -> Mutation {
    base(
        pk,
        ck,
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::text(v),
        }],
        ts,
    )
}

/// `v` live, `w` written with a TTL that expires at `write_secs + ttl`.
pub fn write_v_and_ttl_w(
    pk: i32,
    ck: i32,
    v: &str,
    w: &str,
    ts: i64,
    ttl: u32,
    ldt: i32,
) -> Mutation {
    base(
        pk,
        ck,
        vec![
            CellOperation::Write {
                column: "v".into(),
                value: Value::text(v),
            },
            CellOperation::WriteWithTtl {
                column: "w".into(),
                value: Value::text(w),
                ttl_seconds: ttl,
                local_deletion_time: Some(ldt),
            },
        ],
        ts,
    )
}

/// Base write timestamp (micros) and the derived pinned `now` values. Both
/// pinned instants are CONSTANTS, never a wall-clock read (issue #2642): the
/// fixture's TTL local-deletion-times are stamped explicitly, so the expiry
/// decision is a pure function of these constants.
pub const T_BASE_SECS: i64 = 1_700_000_000;
pub const T_BASE_MICROS: i64 = T_BASE_SECS * 1_000_000;
/// Before the TTL cell expires.
pub const NOW_BEFORE_EXPIRY: i64 = T_BASE_SECS + 100;
/// After it expires (its LDT is `T_BASE_SECS + 600`).
pub const NOW_AFTER_EXPIRY: i64 = T_BASE_SECS + 1_000;
pub const TTL_LDT: i32 = (T_BASE_SECS + 600) as i32;

/// One SSTable holding: a live row, a row with a live-TTL cell, a cell
/// tombstone, a whole-row deletion, a range tombstone and a partition deletion.
pub async fn build_shapes_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, shapes_schema());
    let mut engine = WriteEngine::new(config).expect("engine");

    // pk=1: a plain live row, a row whose `w` carries a TTL, a row whose `w` was
    // deleted (cell tombstone), and a row deleted outright (row tombstone).
    engine.write(write_v(1, 1, "live", T_BASE_MICROS)).unwrap();
    engine
        .write(write_v_and_ttl_w(
            1,
            2,
            "ttl-row",
            "expires",
            T_BASE_MICROS,
            600,
            TTL_LDT,
        ))
        .unwrap();
    // ck=3: two live columns (the multi-column shape).
    //
    // (Issue #3094 has since RE-ENABLED the CQLite-written simple cell tombstone as
    // the `ck=5` shape below. The exclusion note that used to sit here justified
    // itself with a claim that is measurably FALSE and is recorded as such so it is
    // not reinstated: it said BOTH arms surfaced the deleted cell as a raw
    // `Value::Tombstone` the Arrow encoder rejected, hence "not an arm divergence".
    // Reverting the #3094 read-path drop and re-running this differential fails on
    // the BYPASS arm ONLY — `column 'w': expected Text value, got Tombstone(..)` —
    // because the merge arm has always dropped simple cell tombstones in
    // `write_engine::merge::read_assembly::assemble_read_cells`. So the pre-#3094
    // defect WAS an arm divergence, of the same shape as #3140; the two differ only
    // in who WROTE the tombstone (#3140 Cassandra, #3094 CQLite), which is why
    // #3140's `BypassReason::StaticColumnsWithDeletions` fail-closed guard is
    // independent of this and stays pinned by `statics/select-star`.)
    engine
        .write(base(
            1,
            3,
            vec![
                CellOperation::Write {
                    column: "v".into(),
                    value: Value::text("two-column"),
                },
                CellOperation::Write {
                    column: "w".into(),
                    value: Value::text("also-live"),
                },
            ],
            T_BASE_MICROS,
        ))
        .unwrap();
    // ck=5: the SIMPLE CELL TOMBSTONE shape (issue #3094, re-enabled here once the
    // read path stopped surfacing a deleted cell as a raw `Value::Tombstone` that
    // the Arrow encoder rejected). `v` stays live, `w` is deleted by a
    // strictly-later cell tombstone, so both arms must return the row with `w` NULL.
    // The two arms reach that answer through DIFFERENT code, which is what makes the
    // shape worth pinning here: Flight's merge arm drops the tombstone while
    // assembling the reconciled row (`write_engine::merge::read_assembly::
    // assemble_read_cells`), whereas the fast/bypass arm relies on the
    // single-generation decoder's own per-cell drop (`row_decoder`'s
    // `PartitionShadow::cell_tombstone_dropped`) — the half that was missing before
    // #3094 and that only the bypass arm exposes.
    engine
        .write(base(
            1,
            5,
            vec![
                CellOperation::Write {
                    column: "v".into(),
                    value: Value::text("cell-tomb-row"),
                },
                CellOperation::Write {
                    column: "w".into(),
                    value: Value::text("to-be-deleted"),
                },
            ],
            T_BASE_MICROS,
        ))
        .unwrap();
    engine
        .write(base(
            1,
            5,
            vec![CellOperation::Delete {
                column: "w".into(),
                local_deletion_time: Some(T_BASE_SECS as i32),
            }],
            T_BASE_MICROS + 10,
        ))
        .unwrap();
    engine
        .write(write_v(1, 4, "doomed", T_BASE_MICROS))
        .unwrap();
    engine
        .write(base(
            1,
            4,
            vec![CellOperation::DeleteRow],
            T_BASE_MICROS + 10,
        ))
        .unwrap();

    // pk=2: rows covered by a RANGE tombstone plus one outside it.
    engine
        .write(write_v(2, 10, "rt-covered", T_BASE_MICROS))
        .unwrap();
    engine
        .write(write_v(2, 11, "rt-covered", T_BASE_MICROS))
        .unwrap();
    engine
        .write(write_v(2, 99, "rt-survivor", T_BASE_MICROS))
        .unwrap();
    let mut rt = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(2)),
        None,
        vec![],
        T_BASE_MICROS + 10,
        None,
    );
    rt.range_tombstones = vec![RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(10))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(11))),
        deletion_time: T_BASE_MICROS + 10,
        local_deletion_time: T_BASE_SECS as i32,
    }];
    engine.write(rt).unwrap();

    // pk=3: entirely covered by a PARTITION deletion.
    engine.write(write_v(3, 1, "gone", T_BASE_MICROS)).unwrap();
    let mut pt = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(3)),
        None,
        vec![],
        T_BASE_MICROS + 10,
        None,
    );
    pt.partition_tombstone = Some(PartitionTombstone {
        deletion_time: T_BASE_MICROS + 10,
        local_deletion_time: T_BASE_SECS as i32,
    });
    engine.write(pt).unwrap();

    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}

// ---------------------------------------------------------------------------
// V5_0Uncompressed-CLASSIFIED clustered fixture (issue #3097)
// ---------------------------------------------------------------------------

pub const UNCOMP_TBL: &str = "clustered_uncomp";
pub const UNCOMP_DDL: &str = "CREATE TABLE diff_ks.clustered_uncomp \
     (pk blob, ck int, v text, PRIMARY KEY (pk, ck))";

pub fn uncomp_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: UNCOMP_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "blob".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "blob", false),
            col("ck", "int", false),
            col("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// Build a CQLite-written clustered SSTable that the reader classifies as
/// `V5_0Uncompressed` (issue #3097) — the ONLY classification whose merge-arm
/// enumeration takes the non-chunk-stitching Summary-guided branch
/// (`stream_partitions_summary_guided`), where the pre-#3097 code ignored the
/// caller's schema and decoded clustering columns under the header schema's
/// placeholder name (surfacing `ck` as NULL).
///
/// The reader picks that classification when a headerless `nb` Data.db begins
/// with the four bytes of the `V5_0Uncompressed` magic (`00 10 04 5e`) and no
/// `CompressionInfo.db` exists (`reader/header.rs`). We force it deterministically
/// (no heuristics, #28) with a 16-byte `blob` partition key prefixed `04 5e …`:
/// the first partition's on-disk bytes are `00` (flags) `10` (16-byte key length)
/// `04 5e` (key head). A real Cassandra `nb` fixture instead classifies as
/// `V5_0NewBig` (chunk-stitching, which already honours the caller schema), so
/// this write-path fixture is the only way to reach the buggy arm on the Flight
/// surface.
pub async fn build_uncomp_clustered_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal"), uncomp_schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    // Enough partitions to clear the default min_index_interval, so Summary.db
    // carries samples and the merge arm's Summary-guided branch actually runs.
    for i in 0u16..400 {
        let mut key = vec![0x04u8, 0x5e];
        key.extend_from_slice(&[0u8; 12]);
        key.extend_from_slice(&i.to_be_bytes());
        engine
            .write(Mutation::new(
                TableId::new(KS, UNCOMP_TBL),
                PartitionKey::single("pk", Value::Blob(key.into())),
                Some(ClusteringKey::single("ck", Value::Integer(i as i32))),
                vec![CellOperation::Write {
                    column: "v".into(),
                    value: Value::text(format!("v{i}")),
                }],
                T_BASE_MICROS + i as i64,
                None,
            ))
            .unwrap();
    }
    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}

pub const STATIC_TBL: &str = "statics";
pub const STATIC_DDL: &str = "CREATE TABLE diff_ks.statics \
     (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))";

pub fn statics_schema() -> TableSchema {
    let mut s = col("s", "text", true);
    s.is_static = true;
    TableSchema {
        keyspace: KS.into(),
        table: STATIC_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            s,
            col("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// One SSTable with (a) a partition holding a static cell AND clustering rows,
/// and (b) a partition holding ONLY a static cell (no clustering row).
pub async fn build_statics_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, statics_schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    let static_write = |pk: i32, val: &str| {
        Mutation::new(
            TableId::new(KS, STATIC_TBL),
            PartitionKey::single("pk", Value::Integer(pk)),
            None,
            vec![CellOperation::Write {
                column: "s".into(),
                value: Value::text(val),
            }],
            T_BASE_MICROS,
            None,
        )
    };
    let row_write = |pk: i32, ck: i32, v: &str| {
        Mutation::new(
            TableId::new(KS, STATIC_TBL),
            PartitionKey::single("pk", Value::Integer(pk)),
            Some(ClusteringKey::single("ck", Value::Integer(ck))),
            vec![CellOperation::Write {
                column: "v".into(),
                value: Value::text(v),
            }],
            T_BASE_MICROS,
            None,
        )
    };
    engine.write(static_write(1, "s1")).unwrap();
    engine.write(row_write(1, 1, "v11")).unwrap();
    engine.write(row_write(1, 2, "v12")).unwrap();
    engine.write(static_write(2, "s2-only")).unwrap();
    // pk=3 (issue #3095 B1): a live static row whose ONLY clustering row is then
    // ROW-DELETED in the same generation. Cassandra's `partition.hasNext()` is
    // evaluated over the already-filtered iterator, so this partition counts as
    // having no rows and returns its static content as ONE row — and the two arms
    // reach that through completely different mechanisms (the merge arm drops
    // `RowData::Tombstone` in `entry_to_row`; the single-generation decoder must
    // build the row-tombstone display row from the row's OWN cells so the static
    // value cannot revive it). Only a differential can show they agree.
    engine.write(static_write(3, "s3-rows-deleted")).unwrap();
    engine.write(row_write(3, 1, "doomed")).unwrap();
    engine
        .write(Mutation::new(
            TableId::new(KS, STATIC_TBL),
            PartitionKey::single("pk", Value::Integer(3)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::DeleteRow],
            T_BASE_MICROS + 10,
            None,
        ))
        .unwrap();
    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}
