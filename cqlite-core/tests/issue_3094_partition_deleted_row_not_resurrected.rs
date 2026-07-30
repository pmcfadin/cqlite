//! Issue #3094 (blocker guard) — a row covered by a PARTITION DELETION must stay
//! hidden even when the only thing the row ever wrote was a CELL TOMBSTONE.
//!
//! ## The shape
//!
//! ```cql
//! UPDATE t SET w = null WHERE pk = 1 AND ck = 2 USING TIMESTAMP 1000   -- UPDATE-only:
//!                                     -- no pk-liveness marker (row_header.timestamp = None),
//!                                     -- the row's ONLY cell is a tombstone for `w`
//! DELETE FROM t WHERE pk = 1 USING TIMESTAMP 5000                       -- partition tombstone
//! SELECT * FROM t WHERE pk = 1                                          -- Cassandra: 0 rows
//! ```
//!
//! Cassandra returns NOTHING: the partition deletion at 5000 deletes every cell
//! written at or before it (`DeletionTime.deletes(ts) = ts <= markedForDeleteAt`),
//! the row has no liveness marker, and a row with no live data is never returned.
//!
//! ## What can regress here
//!
//! CQLite hides such a row via the ROW-level shadow decision
//! (`RowHeader::shadowed_by_deletion_at`), which compares the row's aggregated
//! maximum write timestamp against the covering deletion. That aggregate is the
//! ONLY evidence that the row's data predates the deletion; when nothing folds
//! into it the aggregate is the `i64::MIN` "no authoritative timestamp" sentinel
//! and the fail-safe (no-heuristics, #28) keeps the row VISIBLE. So a row whose
//! only cell is a tombstone resurrects as an all-null phantom row *from inside a
//! deleted partition* unless the tombstone's own effective write timestamp is
//! folded into that aggregate. A cell tombstone therefore contributes to
//! SHADOWING EVIDENCE while never contributing to LIVENESS — the two are separate
//! properties and this test pins the first one.
//!
//! ## Oracle choice (#3042)
//!
//! The property is read-time RECONCILIATION (row visibility), not on-disk
//! framing, so a CQLite-written row body is a legitimate fixture here — the same
//! justification `issue_3094_cell_tombstone_null.rs` records. The one byte range
//! that IS a framing concern, the partition-header `DeletionTime`, is NOT produced
//! by CQLite's writer: it is patched in place from the format specification
//! (`docs/sstables-definitive-guide/chapters/05-data-db-format.md`, "Partition
//! Header Format": `u16 key_length | key | i32 localDeletionTime BE | i64
//! markedForDeleteAt BE`, from Cassandra's `SortedTablePartitionWriter` +
//! `DeletionTime.Serializer`), exactly as `issue_1741_singlegen_tombstone_ttl_shadow.rs`
//! does. A coexisting partition-delete + covered-row fixture cannot be produced by
//! flushing both mutations: flush-time reconciliation purges the covered row, which
//! would make a "0 rows" assertion vacuous.
//!
//! ## Anti-vacuity
//!
//! The UNPATCHED copy of the very same bytes is read first and must return exactly
//! ONE row, proving the row is physically present and decodable. Only then is the
//! partition-header deletion patched in and the read repeated, which must return
//! ZERO rows. A broken/empty fixture fails the first assertion instead of silently
//! satisfying the second.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_3094_partition_deleted_row_not_resurrected

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine"
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::parse_cql_schema;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

const KS: &str = "pdel_ks";
const TBL: &str = "resurrect";

/// Pinned clocks — CONSTANTS, never a wall-clock read (#2642). Nothing here
/// carries a TTL; the pin exists purely to make the read deterministic.
const T_BASE_SECS: i64 = 1_700_000_000;
/// The UPDATE's write timestamp (µs) — the cell tombstone's own timestamp.
const T_UPDATE_MICROS: i64 = T_BASE_SECS * 1_000_000;
/// A cell tombstone written strictly AFTER the partition deletion below — the
/// second test's shape: a LIVE liveness marker at `T_UPDATE_MICROS`, a covering
/// partition deletion, and then a cell tombstone NEWER than that deletion.
const T_CELL_TOMB_AFTER_DELETE_MICROS: i64 = T_UPDATE_MICROS + 10_000_000;
/// That cell tombstone's `localDeletionTime` (seconds).
const T_CELL_TOMB_AFTER_DELETE_LDT: i32 = (T_BASE_SECS + 10) as i32;
/// The partition deletion's `markedForDeleteAt` (µs) — strictly NEWER than the
/// cell tombstone above, so it covers the whole row.
const T_PARTITION_DELETE_MICROS: i64 = T_UPDATE_MICROS + 5_000_000;
/// The partition deletion's `localDeletionTime` (seconds) — a real epoch second,
/// never the `i32::MAX` LIVE sentinel.
const T_PARTITION_DELETE_LDT: i32 = (T_BASE_SECS + 5) as i32;
/// The read clock (seconds).
const PINNED_NOW: i64 = T_BASE_SECS + 100;

/// Both tests in this file set `CQLITE_TTL_NOW_OVERRIDE_SECS` to the SAME
/// [`PINNED_NOW`] and neither clears it: they share one test binary/process, so a
/// set/clear pair would race the sibling test's read. Nothing here carries a TTL,
/// so the value only makes the read clock deterministic.
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  v text,\n  w text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

/// `UPDATE {TBL} SET w = null WHERE pk = 1 AND ck = 2` — an UPDATE-only mutation,
/// so the flushed row carries NO pk-liveness marker and its only cell is a
/// tombstone for `w`.
fn update_set_w_null() -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(2))),
        vec![CellOperation::Delete {
            column: "w".to_string(),
            local_deletion_time: Some(T_BASE_SECS as i32),
        }],
        T_UPDATE_MICROS,
        None,
    )
}

/// `INSERT INTO {TBL} (pk, ck) VALUES (1, 3)` — a PURE primary-key insert, which
/// creates the row LIVENESS MARKER (`HAS_TIMESTAMP`) and no data cells.
fn insert_liveness_marker_only() -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(3))),
        vec![],
        T_UPDATE_MICROS,
        None,
    )
}

/// `UPDATE {TBL} SET w = null WHERE pk = 1 AND ck = 3 USING TIMESTAMP
/// {T_CELL_TOMB_AFTER_DELETE_MICROS}` — the SAME row as
/// [`insert_liveness_marker_only`], adding a cell tombstone that is NEWER than the
/// partition deletion patched in by the second test.
fn update_set_w_null_after_delete() -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(3))),
        vec![CellOperation::Delete {
            column: "w".to_string(),
            local_deletion_time: Some(T_CELL_TOMB_AFTER_DELETE_LDT),
        }],
        T_CELL_TOMB_AFTER_DELETE_MICROS,
        None,
    )
}

/// Flush `mutations` into one SSTable and return `(tempdir, data_dir)`. `data_dir`
/// holds `<KS>/<TBL>/nb-1-big-*.db`.
async fn build_fixture(mutations: Vec<Mutation>) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");

    for m in mutations {
        engine.write(m).expect("write mutation");
    }
    engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced no SSTable");
    engine.close().await.expect("close engine");

    (temp, data_dir)
}

/// Copy `<data_dir>/<KS>/<TBL>` into `<dst_root>/<KS>/<TBL>`, dropping the
/// integrity sidecars (`Digest.crc32`, `CRC.db`) so an in-place `Data.db` patch is
/// accepted — the reader warn-and-proceeds without them (#1741, decision D4).
fn copy_table_dir(data_dir: &Path, dst_root: &Path) -> PathBuf {
    let src = data_dir.join(KS).join(TBL);
    let dst = dst_root.join(KS).join(TBL);
    std::fs::create_dir_all(&dst).expect("mkdir dst");
    for entry in std::fs::read_dir(&src).expect("read fixture dir").flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-Digest.crc32") || name_str.ends_with("-CRC.db") {
            continue;
        }
        if entry.path().is_file() {
            std::fs::copy(entry.path(), dst.join(&name)).expect("copy component");
        }
    }
    dst
}

/// Overwrite the FIRST partition's header `DeletionTime` with a real partition
/// tombstone. Layout (`nb`, no `hasUIntDeletionTime`): `u16 key_length BE | key |
/// i32 localDeletionTime BE | i64 markedForDeleteAt BE` — a fixed 12-byte
/// non-delta `DeletionTime`, so this is a same-width in-place patch (no offset in
/// Index.db/Summary.db moves).
///
/// The 12 bytes are asserted to hold the LIVE sentinel (`localDeletionTime =
/// i32::MAX`, `markedForDeleteAt = i64::MIN`; guide Ch.5 "Partition Header Format",
/// from `SortedTablePartitionWriter` + `DeletionTime.Serializer`) BEFORE they are
/// overwritten. Without that assertion a mis-derived `del_off` would leave the real
/// `DeletionTime` LIVE and merely corrupt the row body, so the read would return 0
/// rows because the row FAILED TO DECODE — a green test pinning nothing. The
/// assertion is what makes this a pin rather than a placebo.
fn patch_partition_deletion(table_dir: &Path) {
    let data_path = data_db_path(table_dir);
    let mut bytes = std::fs::read(&data_path).expect("read Data.db");
    assert!(bytes.len() > 2, "Data.db too small to hold a partition key");
    let key_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let del_off = 2 + key_len;
    assert!(
        bytes.len() >= del_off + 12,
        "Data.db too small to hold a partition-deletion field (key_len={key_len})"
    );
    assert_eq!(
        &bytes[del_off..del_off + 4],
        &i32::MAX.to_be_bytes(),
        "del_off={del_off} (key_len={key_len}) does not point at a LIVE partition \
         DeletionTime: localDeletionTime must be the i32::MAX LIVE sentinel before patching"
    );
    assert_eq!(
        &bytes[del_off + 4..del_off + 12],
        &i64::MIN.to_be_bytes(),
        "del_off={del_off} (key_len={key_len}) does not point at a LIVE partition \
         DeletionTime: markedForDeleteAt must be the i64::MIN LIVE sentinel before patching"
    );
    bytes[del_off..del_off + 4].copy_from_slice(&T_PARTITION_DELETE_LDT.to_be_bytes());
    bytes[del_off + 4..del_off + 12].copy_from_slice(&T_PARTITION_DELETE_MICROS.to_be_bytes());
    std::fs::write(&data_path, &bytes).expect("write patched Data.db");
}

fn data_db_path(table_dir: &Path) -> PathBuf {
    std::fs::read_dir(table_dir)
        .expect("read table dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("fixture Data.db")
}

async fn open_db(data_dir: &Path, schema_path: &Path) -> Database {
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path.to_path_buf()],
        data_dir: data_dir.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "fixture schema must load"
    );
    result.database
}

async fn select_row_count(data_dir: &Path, schema_path: &Path) -> usize {
    let db = open_db(data_dir, schema_path).await;
    let result = db
        .execute(&format!("SELECT * FROM {KS}.{TBL}"))
        .await
        .expect("SELECT must succeed");
    result.rows.len()
}

#[tokio::test]
async fn partition_deleted_update_only_row_is_not_resurrected() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let (temp, data_dir) = build_fixture(vec![update_set_w_null()]).await;
    let schema_path = temp.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    // Two independent copies of the SAME flushed bytes.
    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    // (1) DECODE PROBE (anti-vacuity) — asserts CQLite's CURRENT behaviour, NOT
    //     Cassandra parity. Without the partition tombstone this row still has no
    //     liveness marker and no live cell: Cassandra purges its lone cell
    //     tombstone (`Filter.applyToRow` → `row.purge(PURGE_ALL)`, guide Ch.11)
    //     and drops the resulting EMPTY row, so a real `SELECT` returns 0 rows.
    //     CQLite returns 1 because it has no `Row.hasLiveData` purge yet — tracked
    //     as issue #3121, which will INVERT this expectation to 0.
    //     Its purpose here is solely to prove the row is physically present and
    //     decodable, so the 0-row assertion below is not vacuous.
    let live_rows = select_row_count(&live_root, &schema_path).await;
    assert_eq!(
        live_rows, 1,
        "decode probe (current CQLite behaviour, NOT Cassandra parity — #3121): the \
         UPDATE-only row must be physically present and decodable BEFORE the partition \
         tombstone is patched in — got {live_rows} rows"
    );

    // (2) THE PIN: patch a partition tombstone strictly NEWER than the row's cell
    // tombstone over the very same bytes. Cassandra returns 0 rows.
    patch_partition_deletion(&deleted_table_dir);
    let deleted_rows = select_row_count(&deleted_root, &schema_path).await;

    assert_eq!(
        deleted_rows, 0,
        "issue #3094: a row whose ONLY cell is a tombstone written at {T_UPDATE_MICROS}µs \
         is entirely covered by the partition deletion at {T_PARTITION_DELETE_MICROS}µs, so a \
         Cassandra SELECT returns 0 rows — the read path resurrected {deleted_rows} \
         all-null phantom row(s) from a DELETED partition. A decoded cell tombstone must \
         defeat the `i64::MIN` no-authoritative-timestamp fail-safe, even though it never \
         counts as liveness."
    );
}

/// Issue #3094 (round-2 blocker): a CELL TOMBSTONE must NEVER RAISE the row's
/// shadow-evidence maximum.
///
/// ## The shape (a Cassandra-writable ordering the first test does not cover)
///
/// ```cql
/// INSERT INTO t (pk, ck) VALUES (1, 3) USING TIMESTAMP 1_700_000_000_000_000  -- liveness marker,
///                                                                            -- NO data cell
/// DELETE FROM t WHERE pk = 1 USING TIMESTAMP 1_700_000_005_000_000            -- partition tombstone
/// UPDATE t SET w = null WHERE pk = 1 AND ck = 3
///   USING TIMESTAMP 1_700_000_010_000_000                                     -- cell tombstone,
///                                                                            -- NEWER than the delete
/// SELECT * FROM t WHERE pk = 1                                               -- Cassandra: 0 rows
/// ```
///
/// Cassandra returns NOTHING. The partition deletion at `…005…` deletes the row's
/// liveness marker (`DeletionTime.deletes(ts) = ts <= markedForDeleteAt`). The `w`
/// cell tombstone at `…010…` genuinely SURVIVES the deletion — but it is a
/// tombstone, so `Filter.applyToRow` → `row.purge(…, PURGE_ALL, …)` removes it and
/// the now-empty row is dropped before the client sees it (guide Ch.11
/// "Merging, tombstones and shadowing"; `Filter.java` at `cassandra-5.0.8`). A
/// tombstone can never make a row VISIBLE.
///
/// ## What regresses here
///
/// The row-level decision `RowHeader::shadowed_by_deletion_at` compares
/// `max(liveness_ts, max_data_cell_ts)` against the covering deletion. If deleted-cell
/// evidence is folded into `max_data_cell_ts` — even as a "fallback" that only applies
/// when live-cell evidence is absent — then for THIS row (`agg_max_cell_ts = None`,
/// because a pure-PK insert writes no cell) the aggregate becomes the tombstone's
/// `…010…`, the `max()` with the liveness `…000…` raises the row maximum ABOVE the
/// covering deletion, and the row is emitted as an all-null phantom row from a DELETED
/// partition. Tombstone evidence must therefore only ever defeat the `i64::MIN`
/// "no authoritative timestamp" fail-safe (a PRESENCE fact), never contribute a
/// timestamp to the maximum.
#[tokio::test]
async fn partition_deleted_liveness_row_with_newer_cell_tombstone_is_not_resurrected() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let (temp, data_dir) = build_fixture(vec![
        insert_liveness_marker_only(),
        update_set_w_null_after_delete(),
    ])
    .await;
    let schema_path = temp.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    // (1) ANTI-VACUITY, and here it IS Cassandra parity: with no partition deletion
    //     the row's liveness marker is live, so `SELECT` returns exactly one row
    //     (v = null, w = null).
    let live_rows = select_row_count(&live_root, &schema_path).await;
    assert_eq!(
        live_rows, 1,
        "fixture sanity: the liveness-marker row must be present and decodable BEFORE \
         the partition tombstone is patched in — got {live_rows} rows"
    );

    // (2) THE PIN: a partition deletion BETWEEN the liveness marker and the cell
    //     tombstone. Cassandra returns 0 rows.
    patch_partition_deletion(&deleted_table_dir);
    let deleted_rows = select_row_count(&deleted_root, &schema_path).await;

    assert_eq!(
        deleted_rows, 0,
        "issue #3094 (round-2 blocker): liveness marker @{T_UPDATE_MICROS}µs, partition \
         deletion @{T_PARTITION_DELETE_MICROS}µs, cell tombstone \
         @{T_CELL_TOMB_AFTER_DELETE_MICROS}µs. The deletion covers every LIVE piece of \
         the row (its liveness marker); the surviving `w` tombstone is purged and the \
         empty row dropped, so a Cassandra SELECT returns 0 rows — got {deleted_rows}. \
         Cell-tombstone evidence must not RAISE the row's shadow maximum above the \
         covering deletion."
    );
}
