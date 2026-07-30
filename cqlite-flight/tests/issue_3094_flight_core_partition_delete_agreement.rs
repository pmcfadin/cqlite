//! Issue #3094 (round-5 blocker) — Flight's `do_get` and core's `SSTableManager::
//! scan` must return the SAME rows for a partition-deleted row that carries a
//! NEWER cell tombstone.
//!
//! ## The shape (two generations, one patched partition deletion)
//!
//! ```cql
//! -- generation 1 (flush 1)
//! INSERT INTO t (pk, ck) VALUES (1, 3) USING TIMESTAMP 1_700_000_000_000_000  -- liveness marker,
//!                                                                            -- NO data cell
//! -- generation 2 (flush 2), whose partition header carries
//! DELETE FROM t WHERE pk = 1 USING TIMESTAMP 1_700_000_005_000_000            -- partition tombstone
//! UPDATE t SET w = null WHERE pk = 1 AND ck = 3
//!   USING TIMESTAMP 1_700_000_010_000_000                                     -- cell tombstone,
//!                                                                            -- NEWER than the delete
//! UPDATE t SET v = 'live' WHERE pk = 1 AND ck = 9
//!   USING TIMESTAMP 1_700_000_020_000_000                                     -- survives the delete
//! SELECT * FROM t WHERE pk = 1                                                -- Cassandra: 1 row, ck = 9
//! ```
//!
//! Cassandra's authority for `ck = 3` being absent is `BTreeRow.filter` at
//! `cassandra-5.0.8`: under an active (partition) deletion it computes
//! `if (activeDeletion.deletes(newInfo.timestamp())) newInfo = LivenessInfo.EMPTY;`
//! — the primary-key liveness marker is dropped by comparing the MARKER's OWN
//! timestamp against `markedForDeleteAt` (`DeletionTime.deletes(long ts)` is
//! `ts <= markedForDeleteAt`), never the row's newest cell. The `w` cell tombstone
//! at `…010…` genuinely survives the deletion, but a tombstone can never make a row
//! visible: it is purged by `Filter.applyToRow` and the emptied row is dropped.
//! `ck = 9`'s live `v` at `…020…` outlives the deletion, so it IS returned.
//!
//! ## What this pins — the CORE-vs-FLIGHT divergence
//!
//! Core's multi-generation read path applies `ReadShadow::filter_live` /
//! `partition_live_rows` after the `KWayMerger`, and (as of this PR) correctly
//! returns exactly `ck = 9`. Flight's `do_get` producer drives the same
//! `KWayMerger` but decides visibility itself in `producer.rs::entry_to_row`
//! (`has_live_data_cell || row_liveness.marker_live_at(now)`) — it never consults
//! the partition cover. So the phantom all-null `ck = 3` row leaked through Flight
//! while core hid it: Flight 2 rows, core 1 row, Cassandra 1 row.
//!
//! The fix is in the ONE place both consumers share, `KWayMerger::
//! apply_partition_shadowing`, whose `marker_live` tested the RECONCILED ROW's
//! timestamp (raised to `…010…` by the tombstone cell) instead of the marker's own.
//! Tightening it to also require `row_liveness.marker_timestamp > markedForDeleteAt`
//! makes the merger drop the deleted marker, so `entry_to_row` sees no marker and
//! no live data cell and hides the row — Flight and core converge on Cassandra's
//! answer.
//!
//! Note the forced-`bypass` fast path (#3058) is NOT an escape hatch here: bypass
//! requires a single reader, so a two-generation table always takes the merge arm.
//! The probe assertion below proves that is what ran.
//!
//! ## Both `do_get` row producers
//!
//! `entry_to_row` is shared by `drive_merge` (the row stream) AND
//! `drive_aggregate` (pushed-down aggregation, #841), so the same phantom corrupts
//! a `count(*)`. Both are asserted: the row stream must yield exactly `ck = 9`, and
//! the pushed-down `count(*)` must be `1`, not `2`.
//!
//! ## Oracle choice (#3042)
//!
//! Row visibility under a partition deletion is a read-time RECONCILIATION
//! property, not an on-disk framing property, so CQLite-written row bodies are a
//! legitimate fixture — the justification recorded by the core siblings
//! (`issue_3094_multigen_partition_deleted_row_not_resurrected.rs`). The one byte
//! range that IS a framing concern, the partition-header `DeletionTime`, is NOT
//! produced by CQLite's writer: it is patched in from the format specification
//! (`docs/sstables-definitive-guide/chapters/05-data-db-format.md`, "Partition
//! Header Format": `u16 key_length | key | i32 localDeletionTime BE | i64
//! markedForDeleteAt BE`), exactly as those siblings do. This test is NOT a
//! symmetric CQLite-write/CQLite-read round trip of a framing property.
//!
//! ## Anti-vacuity
//!
//! Every assertion is made TWICE — once against an UNPATCHED copy of the very same
//! two generations (where both rows are physically present, so a broken or empty
//! fixture fails loudly instead of trivially satisfying a "1 row" expectation), and
//! then against the patched copy. The unpatched expectation is a decode probe of
//! CQLite's current behaviour; the patched one is the Cassandra-parity claim.
//!
//! ## Isolation
//!
//! `CQLITE_TTL_NOW_OVERRIDE_SECS` is PROCESS-GLOBAL, so this file holds exactly ONE
//! `#[test]` that runs every case sequentially.
//!
//! Run with:
//!   cargo test -p cqlite-flight \
//!     --test issue_3094_flight_core_partition_delete_agreement

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TableId as CqlTableId, Value};
use cqlite_core::{Config, ScanRow};
use cqlite_flight::service::CqliteFlightService;

const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

const KS: &str = "pdel_flight_ks";
const TBL: &str = "resurrect";

/// Pinned clocks — CONSTANTS, never a wall-clock read (#2642). Nothing here
/// carries a TTL; the pins exist purely to make the read deterministic.
const T_BASE_SECS: i64 = 1_700_000_000;
/// The pure-PK `INSERT`'s write timestamp (µs) = the row liveness marker's ts.
const T_INSERT_MICROS: i64 = T_BASE_SECS * 1_000_000;
/// The partition deletion's `markedForDeleteAt` (µs) — strictly NEWER than the
/// liveness marker, strictly OLDER than the cell tombstone.
const T_PARTITION_DELETE_MICROS: i64 = T_INSERT_MICROS + 5_000_000;
/// That deletion's `localDeletionTime` (seconds) — a real epoch second, never the
/// `i32::MAX` LIVE sentinel.
const T_PARTITION_DELETE_LDT: i32 = (T_BASE_SECS + 5) as i32;
/// The `w` cell tombstone's write timestamp (µs) — strictly NEWER than the
/// partition deletion, so `apply_partition_shadowing` keeps the cell.
const T_CELL_TOMB_MICROS: i64 = T_INSERT_MICROS + 10_000_000;
/// That cell tombstone's `localDeletionTime` (seconds).
const T_CELL_TOMB_LDT: i32 = (T_BASE_SECS + 10) as i32;
/// The SIBLING row's live `v` write timestamp (µs) — newer than the deletion, so
/// that row SURVIVES and turns every assertion into a row-SET check rather than a
/// bare "0 rows", which a whole-partition over-hide would also satisfy.
const T_SIBLING_LIVE_MICROS: i64 = T_INSERT_MICROS + 20_000_000;
/// The read clock (seconds), pinned so the read is deterministic (#2642).
const PINNED_NOW: i64 = T_BASE_SECS + 100;

/// Clustering value of the phantom row (marker older than the deletion, cell
/// tombstone newer).
const CK_PHANTOM: i32 = 3;
/// Clustering value of the surviving sibling row.
const CK_SURVIVOR: i32 = 9;

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  v text,\n  w text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

fn ddl() -> String {
    format!("CREATE TABLE {KS}.{TBL} (pk int, ck int, v text, w text, PRIMARY KEY (pk, ck))")
}

/// `INSERT INTO t (pk, ck) VALUES (1, {ck}) USING TIMESTAMP {ts}` — a PURE
/// primary-key insert, which creates the row LIVENESS MARKER (`HAS_TIMESTAMP`)
/// and no data cells.
fn insert_liveness_marker_only(ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![],
        ts,
        None,
    )
}

/// `UPDATE t SET w = null WHERE pk = 1 AND ck = {ck} USING TIMESTAMP
/// {T_CELL_TOMB_MICROS}` — a cell tombstone NEWER than the partition deletion
/// patched in below.
fn update_set_w_null(ck: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Delete {
            column: "w".to_string(),
            local_deletion_time: Some(T_CELL_TOMB_LDT),
        }],
        T_CELL_TOMB_MICROS,
        None,
    )
}

/// `UPDATE t SET v = 'live' WHERE pk = 1 AND ck = {ck} USING TIMESTAMP
/// {T_SIBLING_LIVE_MICROS}` — a live data cell strictly NEWER than the partition
/// deletion, so this row must survive it.
fn update_v_live(ck: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("live".to_string()),
        }],
        T_SIBLING_LIVE_MICROS,
        None,
    )
}

/// Flush `gen1` into generation 1 and `gen2` into generation 2 (two separate
/// flushes, no compaction), returning `(tempdir, data_dir)`.
async fn build_two_generation_fixture(
    schema: &TableSchema,
    gen1: Vec<Mutation>,
    gen2: Vec<Mutation>,
) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    for (n, batch) in [gen1, gen2].into_iter().enumerate() {
        for m in batch {
            engine.write(m).expect("write mutation");
        }
        engine
            .flush()
            .await
            .unwrap_or_else(|e| panic!("flush {} failed: {e}", n + 1))
            .unwrap_or_else(|| panic!("flush {} produced no SSTable", n + 1));
    }

    engine.close().await.expect("close engine");
    (temp, data_dir)
}

/// Copy `<data_dir>/<KS>/<TBL>` (ALL generations) into `<dst_root>/<KS>/<TBL>`,
/// dropping the integrity sidecars (`Digest.crc32`, `CRC.db`) so an in-place
/// `Data.db` patch is accepted — the reader warn-and-proceeds without them
/// (#1741, decision D4).
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

/// Every `*-Data.db` in `table_dir`, sorted by SSTable GENERATION ascending
/// (`<version>-<generation>-big-Data.db`). The generation is read from the
/// authoritative filename field, never guessed from directory order (#28).
fn data_db_paths_by_generation(table_dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(table_dir)
        .expect("read table dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .map(|p| {
            let name = p
                .file_name()
                .and_then(|n| n.to_str())
                .expect("utf-8 component name")
                .to_string();
            let generation = name
                .split('-')
                .nth(1)
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("no generation field in component name '{name}'"));
            (generation, p)
        })
        .collect();
    found.sort_by_key(|(generation, _)| *generation);
    found
}

/// Overwrite the NEWEST generation's first-partition header `DeletionTime` with a
/// real partition tombstone. Layout (`nb`, no `hasUIntDeletionTime`): `u16
/// key_length BE | key | i32 localDeletionTime BE | i64 markedForDeleteAt BE` — a
/// fixed 12-byte non-delta `DeletionTime`, so this is a same-width in-place patch
/// (no offset in Index.db/Summary.db moves).
///
/// The 12 bytes are asserted to hold the LIVE sentinel (`localDeletionTime =
/// i32::MAX`, `markedForDeleteAt = i64::MIN`; guide Ch.5 "Partition Header
/// Format") BEFORE they are overwritten. Without that assertion a mis-derived
/// `del_off` would leave the real `DeletionTime` LIVE and merely corrupt the row
/// body, so the read would return fewer rows because a row FAILED TO DECODE — a
/// green test pinning nothing.
fn patch_newest_generation_partition_deletion(table_dir: &Path) {
    let generations = data_db_paths_by_generation(table_dir);
    assert_eq!(
        generations.len(),
        2,
        "fixture must hold exactly two generations, found {generations:?}"
    );
    let (_, data_path) = generations.last().expect("newest generation");
    let mut bytes = std::fs::read(data_path).expect("read Data.db");
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
    std::fs::write(data_path, &bytes).expect("write patched Data.db");
}

/// The `ck` clustering value of every live row CORE's multi-generation scan
/// returns (`SSTableManager::scan` ⇒ `merge_generations_for_read` ⇒
/// `partition_live_rows` ⇒ `ReadShadow::filter_live`), in emission order.
async fn core_ck_values(root: &Path, schema: &TableSchema) -> Vec<i32> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    // `cqlite-flight` depends on cqlite-core with DEFAULT features, so
    // `state_machine` is always on here and the registry argument is
    // unconditional (unlike in cqlite-core's own tests, which are feature-gated).
    let manager = SSTableManager::new(root, &config, platform, None)
        .await
        .expect("SSTableManager open");

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    manager
        .scan(&table_id, None, None, None, Some(schema))
        .await
        .expect("multi-generation scan must not error")
        .iter()
        .map(|(_, row)| match row {
            ScanRow::Row(cells) => cells
                .iter()
                .find_map(|(name, value)| match (name.as_ref(), value) {
                    ("ck", Value::Integer(v)) => Some(*v),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("emitted row carries no `ck` clustering value")),
            other => panic!("unexpected scan row shape: {other:?}"),
        })
        .collect()
}

fn scan_ticket() -> serde_json::Value {
    serde_json::json!({ "keyspace": KS, "table": TBL, "ddl": ddl() })
}

fn count_star_ticket() -> serde_json::Value {
    serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": ddl(),
        "aggregation": {
            "group_by": [],
            "aggregates": [{ "func": "Count", "column": null, "output": "cnt" }]
        }
    })
}

/// Drain `do_get` into its record batches, surfacing a terminal stream error as a
/// message instead of a panic (a phantom row can also manifest as an Arrow
/// coercion failure, which must stay observable).
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_batches(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> Result<Vec<RecordBatch>, String> {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = match svc.do_get(Request::new(Ticket::new(bytes))).await {
        Ok(r) => r.into_inner(),
        Err(status) => return Err(format!("do_get rpc: {}", status.message())),
    };
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => batches.push(batch),
            Err(e) => return Err(format!("stream: {e}")),
        }
    }
    Ok(batches)
}

/// The `ck` value of every row Flight's `do_get` row stream yields, in emission
/// order — the FLIGHT-side twin of [`core_ck_values`].
fn ck_values_of(batches: &[RecordBatch]) -> Vec<i32> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of("ck")
            .expect("do_get batch must carry the `ck` clustering column");
        let col = batch.column(idx);
        let ints = col
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("`ck int` must map to Arrow Int32");
        for r in 0..batch.num_rows() {
            assert!(
                !ints.is_null(r),
                "a clustering `ck` value must never be null"
            );
            out.push(ints.value(r));
        }
    }
    out
}

/// The single global `count(*)` partial Flight returns for an empty-`group_by`
/// aggregation ticket.
fn count_star_of(batches: &[RecordBatch]) -> i64 {
    let mut totals = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of("cnt")
            .expect("aggregate batch must carry the `cnt` output column");
        let col = batch.column(idx);
        let counts = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) must map to Arrow Int64");
        for r in 0..batch.num_rows() {
            assert!(!counts.is_null(r), "a count(*) partial must never be null");
            totals.push(counts.value(r));
        }
    }
    assert_eq!(
        totals.len(),
        1,
        "an empty-group_by aggregation yields exactly one global partial row, got {totals:?}"
    );
    totals[0]
}

/// Read one root through BOTH surfaces at the pinned `now` and return
/// `(core ck values, flight ck values, flight count(*), mergers built)`.
async fn read_both_surfaces(root: &Path, schema: &TableSchema) -> (Vec<i32>, Vec<i32>, i64, u64) {
    let core = core_ck_values(root, schema).await;

    let svc = CqliteFlightService::new(root.to_path_buf(), 8192);
    let before = ReadPathProbe::snapshot();
    let scan = do_get_batches(&svc, &scan_ticket())
        .await
        .unwrap_or_else(|e| panic!("do_get row stream must not error: {e}"));
    let mergers = ReadPathProbe::snapshot().delta_since(&before).mergers_built;
    let flight = ck_values_of(&scan);

    let agg = do_get_batches(&svc, &count_star_ticket())
        .await
        .unwrap_or_else(|e| panic!("do_get count(*) must not error: {e}"));
    let count = count_star_of(&agg);

    (core, flight, count, mergers)
}

#[tokio::test]
async fn flight_do_get_agrees_with_core_on_a_partition_deleted_row() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, data_dir) = build_two_generation_fixture(
        &schema,
        vec![insert_liveness_marker_only(CK_PHANTOM, T_INSERT_MICROS)],
        vec![update_set_w_null(CK_PHANTOM), update_v_live(CK_SURVIVOR)],
    )
    .await;

    // Two independent copies of the SAME two flushed generations.
    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    // Precondition: a genuine multi-generation directory. `readers.len() > 1`, so
    // the #3058 bypass fast path CANNOT be taken and both surfaces drive the
    // `KWayMerger` — the shared code the fix lands in.
    assert_eq!(
        data_db_paths_by_generation(&live_root.join(KS).join(TBL)).len(),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );

    let mut failures: Vec<String> = Vec::new();

    // ---- (1) ANTI-VACUITY: the UNPATCHED generations ------------------------
    // Both rows are physically present and decodable, and the two surfaces agree,
    // BEFORE any partition tombstone exists. Without this a "1 row" expectation
    // below would be satisfied by an empty or undecodable fixture.
    let (live_core, live_flight, live_count, live_mergers) =
        read_both_surfaces(&live_root, &schema).await;
    if live_mergers == 0 {
        failures.push(
            "the unpatched Flight read did not build a merger (mergers=0) — a \
             two-generation directory must take the KWayMerger arm, so this test would \
             not be exercising the shared code path at all"
                .to_string(),
        );
    }
    if live_core != vec![CK_PHANTOM, CK_SURVIVOR] {
        failures.push(format!(
            "fixture sanity (core): both merged rows must be present and decodable \
             BEFORE the partition tombstone is patched in — expected \
             [{CK_PHANTOM}, {CK_SURVIVOR}], got {live_core:?}"
        ));
    }
    if live_flight != live_core {
        failures.push(format!(
            "fixture sanity: Flight and core must already agree with NO partition \
             deletion present — core {live_core:?}, flight {live_flight:?}"
        ));
    }
    if live_count != live_core.len() as i64 {
        failures.push(format!(
            "fixture sanity: the pushed-down count(*) must equal the row count with no \
             partition deletion present — rows {}, count(*) {live_count}",
            live_core.len()
        ));
    }

    // ---- (2) THE PIN: the patched partition deletion ------------------------
    patch_newest_generation_partition_deletion(&deleted_table_dir);
    let (del_core, del_flight, del_count, del_mergers) =
        read_both_surfaces(&deleted_root, &schema).await;
    if del_mergers == 0 {
        failures.push(
            "the patched Flight read did not build a merger (mergers=0) — the \
             divergence this pins lives in the KWayMerger consumers"
                .to_string(),
        );
    }
    if del_core != vec![CK_SURVIVOR] {
        failures.push(format!(
            "core regression: under a partition deletion @{T_PARTITION_DELETE_MICROS}µs the \
             surviving row set is exactly [{CK_SURVIVOR}] (ck={CK_PHANTOM}'s liveness marker \
             @{T_INSERT_MICROS}µs is deleted by `DeletionTime.deletes`; its surviving `w` \
             tombstone @{T_CELL_TOMB_MICROS}µs is purged, never promoted to visible) — got \
             {del_core:?}"
        ));
    }
    if del_flight != vec![CK_SURVIVOR] {
        failures.push(format!(
            "issue #3094 (round-5 blocker): Flight `do_get` must return exactly \
             [{CK_SURVIVOR}] under the partition deletion @{T_PARTITION_DELETE_MICROS}µs, \
             matching Cassandra and core — got {del_flight:?}. \
             `[{CK_PHANTOM}, {CK_SURVIVOR}]` is the all-null phantom row leaking through \
             `producer.rs::entry_to_row`, which decides visibility WITHOUT the partition \
             cover: `KWayMerger::apply_partition_shadowing` must drop a liveness marker \
             whose OWN timestamp is `<= markedForDeleteAt` (`BTreeRow.filter`: \
             `activeDeletion.deletes(newInfo.timestamp())`), not one whose reconciled ROW \
             timestamp was raised by a newer tombstone cell. `[]` would be an over-hide of \
             the whole partition."
        ));
    }
    if del_flight != del_core {
        failures.push(format!(
            "issue #3094: Flight `do_get` and core `SSTableManager::scan` DIVERGE on the \
             same bytes at the same pinned now — core {del_core:?}, flight {del_flight:?}. \
             Both drive the same KWayMerger, so the visibility decision must be shared, not \
             re-derived per consumer."
        ));
    }
    // The aggregate producer (`drive_aggregate`) shares `entry_to_row`, so the
    // phantom corrupts a `count(*)` even when no row bytes are ever emitted.
    if del_count != 1 {
        failures.push(format!(
            "issue #3094: the pushed-down count(*) over the deleted partition must be 1 \
             (only ck={CK_SURVIVOR} survives), got {del_count} — `drive_aggregate` folds the \
             same phantom row `drive_merge` would emit"
        ));
    }

    assert!(
        failures.is_empty(),
        "issue #3094 Flight-vs-core partition-delete agreement failures:\n{}",
        failures.join("\n\n")
    );

    drop(temp);
}
