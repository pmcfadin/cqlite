//! Issue #3094 (round-4 blocker) — the MULTI-GENERATION twin of
//! `issue_3094_partition_deleted_row_not_resurrected::partition_deleted_liveness_row_with_newer_cell_tombstone_is_not_resurrected`.
//!
//! ## The shape (identical to the single-gen pin, spread over two SSTables)
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
//! SELECT * FROM t WHERE pk = 1                                               -- Cassandra: 0 rows
//! ```
//!
//! Cassandra returns NOTHING, exactly as in the single-generation case: the
//! partition deletion at `…005…` deletes every LIVE piece of the row (here just its
//! liveness marker — `DeletionTime.deletes(ts) = ts <= markedForDeleteAt`), and the
//! `w` cell tombstone that genuinely SURVIVES the deletion is then purged by
//! `Filter.applyToRow` → `row.purge(…, PURGE_ALL, …)`, leaving an empty row that is
//! dropped before the client sees it (guide Ch.11 "Merging, tombstones and
//! shadowing"; `Filter.java` at `cassandra-5.0.8`). A tombstone can never make a row
//! VISIBLE.
//!
//! ## What regresses here — a fast-vs-merge DIVERGENCE
//!
//! A table directory with >1 generation routes through the `KWayMerger`
//! (`generation_merge`), whose post-merge read-visibility filter (`ReadShadow::
//! filter_live`) skips every `Value::Tombstone` cell BEFORE the row aggregate is
//! folded. Historically that skip carried NO presence flag onward, and the row-level
//! decision `merged_row_shadowed_by_partition` hardcoded `has_deleted_data_cell:
//! false`. So the merged row's evidence was the `i64::MIN` "no authoritative
//! timestamp" sentinel, the no-heuristics fail-safe (#28) kept the row VISIBLE, and
//! the retained pk/ck pseudo-cells made `!row_cells.is_empty()` true — one all-null
//! phantom row out of a DELETED partition, where the single-generation path already
//! returned 0. The fix threads the PRESENCE fact (never a timestamp) from that skip
//! into the row decision, plus the row's SURVIVING liveness-marker timestamp, which
//! `merged_row_shadowed_by_partition` also hardcoded away (`timestamp: None`) — so
//! both paths agree.
//!
//! The trigger is narrow, hence this dedicated pin: the cell tombstone's write ts
//! must be STRICTLY NEWER than the partition deletion's `markedForDeleteAt` (an
//! older one is dropped by the merger's own `apply_partition_shadowing`), and the
//! table must have clustering columns.
//!
//! ## Oracle choice (#3042)
//!
//! The property is read-time RECONCILIATION (row visibility), not on-disk framing,
//! so CQLite-written row bodies are a legitimate fixture — the justification the
//! single-gen sibling records. The one byte range that IS a framing concern, the
//! partition-header `DeletionTime`, is NOT produced by CQLite's writer: it is patched
//! in place from the format specification
//! (`docs/sstables-definitive-guide/chapters/05-data-db-format.md`, "Partition Header
//! Format": `u16 key_length | key | i32 localDeletionTime BE | i64 markedForDeleteAt
//! BE`, from Cassandra's `SortedTablePartitionWriter` + `DeletionTime.Serializer`),
//! exactly as `issue_1741_singlegen_tombstone_ttl_shadow.rs` and the single-gen
//! sibling do.
//!
//! ## The three pins and which half of the fix each needs
//!
//! The fix has two halves, both required, and each pin below is sensitive to a
//! specific one — verified by seeding the divergence, never inferred from green runs:
//!
//! | pin | shape | reverting PRESENCE | reverting the MARKER ts |
//! |---|---|---|---|
//! | `…liveness_row_with_newer_cell_tombstone…` | marker OLDER than the deletion | still passes | fails |
//! | `…markerless_tombstone_row_is_hidden_but_its_live_sibling…` | marker-LESS row | fails (`[2, 9]`) | still passes |
//! | `…hides_the_markerless_row_and_keeps_the_newer_marked_one` | both, one deletion | fails (`[2, 4]`) | fails (`[]`) |
//!
//! The third is therefore the load-bearing one: presence alone would hide a row whose
//! liveness marker OUTLIVES the deletion (a row Cassandra returns), and the marker
//! timestamp alone leaves the marker-less phantom resurrected.
//!
//! ## All FOUR multi-generation drivers (issue #3129 AC2)
//!
//! The three pins above drive `merge_generations_for_read` only. Three further public
//! entry points reach a multi-generation merge — the WRITETIME/TTL metadata
//! projection, the streaming scan, and the partition-seeking point read — and the
//! metadata one carries its OWN copy of the emission loop (`push_metadata_rows`). The
//! `…AC2 — driver N of 4` pins at the bottom of this file assert the same property on
//! each, so the four paths can never diverge silently. See the banner comment there.
//!
//! ## Anti-vacuity
//!
//! Every pin reads the UNPATCHED copy of the very same generations first and asserts
//! the exact rows it physically holds, so a broken/empty fixture or a read error fails
//! loudly there instead of silently satisfying the post-patch assertion. Only then is
//! the partition deletion patched in and the read repeated. Where an unpatched
//! expectation includes a marker-less tombstone-only row it is a DECODE PROBE of
//! CQLite's CURRENT behaviour, not a parity claim: Cassandra purges that row's lone
//! tombstone and drops it even with no deletion present (tracked as #3121). The
//! post-patch expectations are Cassandra parity.
//!
//! Run with:
//!   cargo test --package cqlite-core --features write-support \
//!     --test issue_3094_multigen_partition_deleted_row_not_resurrected

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TableId as CqlTableId, Value};
use cqlite_core::{Config, ScanRow};

const KS: &str = "pdel_multigen_ks";
const TBL: &str = "resurrect";

/// Pinned clocks — CONSTANTS, never a wall-clock read (#2642). Nothing here carries
/// a TTL; the pins exist purely to make the read deterministic.
const T_BASE_SECS: i64 = 1_700_000_000;
/// The pure-PK `INSERT`'s write timestamp (µs) = the row liveness marker's ts.
const T_INSERT_MICROS: i64 = T_BASE_SECS * 1_000_000;
/// The `w` cell tombstone's write timestamp (µs) — strictly NEWER than the partition
/// deletion below, so the merger's `apply_partition_shadowing` keeps it.
const T_CELL_TOMB_MICROS: i64 = T_INSERT_MICROS + 10_000_000;
/// That cell tombstone's `localDeletionTime` (seconds).
const T_CELL_TOMB_LDT: i32 = (T_BASE_SECS + 10) as i32;
/// The partition deletion's `markedForDeleteAt` (µs) — strictly NEWER than the
/// liveness marker, strictly OLDER than the cell tombstone.
const T_PARTITION_DELETE_MICROS: i64 = T_INSERT_MICROS + 5_000_000;
/// The partition deletion's `localDeletionTime` (seconds) — a real epoch second,
/// never the `i32::MAX` LIVE sentinel.
const T_PARTITION_DELETE_LDT: i32 = (T_BASE_SECS + 5) as i32;
/// A SIBLING row's live `v` write timestamp (µs) — newer than everything above, so
/// that row survives the partition deletion and pins the surviving row SET.
const T_SIBLING_LIVE_MICROS: i64 = T_INSERT_MICROS + 20_000_000;
/// The read clock (seconds), pinned so the read is deterministic (#2642).
const PINNED_NOW: i64 = T_BASE_SECS + 100;
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  v text,\n  w text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

/// `INSERT INTO {TBL} (pk, ck) VALUES (1, {ck}) USING TIMESTAMP {ts}` — a PURE
/// primary-key insert, which creates the row LIVENESS MARKER (`HAS_TIMESTAMP`) and
/// no data cells.
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

/// `UPDATE {TBL} SET w = null WHERE pk = 1 AND ck = 3 USING TIMESTAMP
/// {T_CELL_TOMB_MICROS}` — the SAME row, adding a cell tombstone NEWER than the
/// partition deletion patched in below.
fn update_set_w_null() -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(3))),
        vec![CellOperation::Delete {
            column: "w".to_string(),
            local_deletion_time: Some(T_CELL_TOMB_LDT),
        }],
        T_CELL_TOMB_MICROS,
        None,
    )
}

/// `UPDATE {TBL} SET w = null WHERE pk = 1 AND ck = {ck} USING TIMESTAMP
/// {T_CELL_TOMB_MICROS}` on a row that was NEVER inserted — the MARKER-LESS shape
/// (`row_header.timestamp = None`), whose only non-key cell is a tombstone.
fn update_set_w_null_markerless(ck: i32) -> Mutation {
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

/// `UPDATE {TBL} SET v = 'live' WHERE pk = 1 AND ck = {ck} USING TIMESTAMP
/// {T_SIBLING_LIVE_MICROS}` — a sibling row of the SAME partition whose live data
/// cell is strictly NEWER than the partition deletion, so it must SURVIVE. Its
/// presence turns the second test's assertion into a row-SET check rather than a
/// bare "0 rows", which a whole-partition over-hide would also satisfy.
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
/// `Data.db` patch is accepted — the reader warn-and-proceeds without them (#1741,
/// decision D4).
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
/// The newest generation is the one holding the cell tombstone, so the patched file
/// is exactly what Cassandra would have flushed for `DELETE FROM t WHERE pk = 1;
/// UPDATE t SET w = null …` — a realistic two-SSTable layout, not a synthetic one.
///
/// The 12 bytes are asserted to hold the LIVE sentinel (`localDeletionTime =
/// i32::MAX`, `markedForDeleteAt = i64::MIN`; guide Ch.5 "Partition Header Format")
/// BEFORE they are overwritten. Without that assertion a mis-derived `del_off` would
/// leave the real `DeletionTime` LIVE and merely corrupt the row body, so the read
/// would return 0 rows because the row FAILED TO DECODE — a green test pinning
/// nothing. The assertion is what makes this a pin rather than a placebo.
fn patch_newest_generation_partition_deletion(table_dir: &Path) {
    let generations = data_db_paths_by_generation(table_dir);
    assert_eq!(
        generations.len(),
        2,
        "fixture must hold exactly two generations, found {:?}",
        generations
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

/// The fixture's single partition key value (`pk = 1`).
const PK_VALUE: i32 = 1;

fn table_id() -> CqlTableId {
    CqlTableId::from(format!("{KS}.{TBL}").as_str())
}

/// Open an `SSTableManager` over `root` and assert it registered EXACTLY the two
/// generations the fixture flushed.
///
/// This is the shared ANTI-VACUITY precondition for every driver below: each
/// multi-generation branch is guarded by a reader/candidate count (`reader_list.len()
/// > 1`, `readers.len() > 1`, `candidates.len() > 1`), and every one of them falls
/// back SILENTLY to a non-reconciling per-reader concatenation when the count is 1.
/// A fixture that lost a generation — or a manager that failed to register one —
/// would therefore take the fallback and could satisfy a naive expectation without
/// the merge path ever running. Failing here instead makes that loud.
async fn open_manager(root: &Path) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let manager = SSTableManager::new(
        root,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager open");

    let registered = manager.list_sstables().await.len();
    assert_eq!(
        registered, 2,
        "anti-vacuity: the manager must register BOTH generations, else every \
         multi-generation driver falls back to the non-reconciling per-reader \
         concatenation and the pin below would prove nothing — registered {registered}"
    );
    manager
}

/// The `ck` clustering value carried by one emitted scan row.
fn ck_of(row: &ScanRow) -> i32 {
    match row {
        ScanRow::Row(cells) => cells
            .iter()
            .find_map(|(name, value)| match (name.as_ref(), value) {
                ("ck", Value::Integer(v)) => Some(*v),
                _ => None,
            })
            .unwrap_or_else(|| panic!("emitted row carries no `ck` clustering value")),
        other => panic!("unexpected scan row shape: {other:?}"),
    }
}

/// Scan `root` through the manager's MULTI-GENERATION merge path (`candidates > 1`
/// ⇒ `merge_generations_for_read` ⇒ `partition_live_rows` ⇒ `ReadShadow::
/// filter_live`) and return the number of live rows.
async fn multigen_row_count(root: &Path, schema: &TableSchema) -> usize {
    let manager = open_manager(root).await;
    let rows = manager
        .scan(&table_id(), None, None, None, Some(schema))
        .await
        .expect("multi-generation scan must not error");
    rows.len()
}

/// The `ck` clustering value of every live row `root`'s multi-generation scan
/// returns, in emission order — so an assertion can name the surviving row SET
/// instead of only its cardinality.
async fn multigen_ck_values(root: &Path, schema: &TableSchema) -> Vec<i32> {
    let manager = open_manager(root).await;
    manager
        .scan(&table_id(), None, None, None, Some(schema))
        .await
        .expect("multi-generation scan must not error")
        .iter()
        .map(|(_, row)| ck_of(row))
        .collect()
}

#[tokio::test]
async fn multigen_partition_deleted_liveness_row_with_newer_cell_tombstone_is_not_resurrected() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, data_dir) = build_two_generation_fixture(
        &schema,
        vec![insert_liveness_marker_only(3, T_INSERT_MICROS)],
        vec![update_set_w_null()],
    )
    .await;

    // Two independent copies of the SAME two flushed generations.
    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    // Precondition: a genuine multi-generation directory (the KWayMerger path).
    assert_eq!(
        data_db_paths_by_generation(&live_root.join(KS).join(TBL)).len(),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );

    // (1) ANTI-VACUITY, and here it IS Cassandra parity: with no partition deletion
    //     the row's liveness marker is live, so the merged `SELECT` returns exactly
    //     one row (v = null, w = null).
    let live_rows = multigen_row_count(&live_root, &schema).await;
    assert_eq!(
        live_rows, 1,
        "fixture sanity: the merged liveness-marker row must be present and decodable \
         across both generations BEFORE the partition tombstone is patched in — got \
         {live_rows} rows"
    );

    // (2) THE PIN: a partition deletion BETWEEN the liveness marker and the cell
    //     tombstone. Cassandra returns 0 rows.
    patch_newest_generation_partition_deletion(&deleted_table_dir);
    let deleted_rows = multigen_row_count(&deleted_root, &schema).await;

    assert_eq!(
        deleted_rows, 0,
        "issue #3094 (round-4 blocker): gen1 liveness marker @{T_INSERT_MICROS}µs, gen2 \
         partition deletion @{T_PARTITION_DELETE_MICROS}µs + cell tombstone \
         @{T_CELL_TOMB_MICROS}µs. The deletion covers every LIVE piece of the row; the \
         surviving `w` tombstone is purged and the empty row dropped, so a Cassandra \
         SELECT returns 0 rows — the MULTI-GENERATION merge path resurrected \
         {deleted_rows} all-null phantom row(s) from a DELETED partition, diverging from \
         the single-generation path which already returns 0. `ReadShadow::filter_live` \
         must thread the cell tombstone's PRESENCE (never its timestamp) into \
         `merged_row_shadowed_by_partition`."
    );

    drop(temp);
}

/// Issue #3129 AC1 — the MARKER-LESS variant of the same defect, and the one that
/// proves the fix hides exactly the phantom row rather than over-hiding the partition.
///
/// ```cql
/// -- generation 1
/// UPDATE t SET w = null WHERE pk = 1 AND ck = 2
///   USING TIMESTAMP 1_700_000_010_000_000   -- marker-less: no INSERT ever happened
/// -- generation 2, whose partition header carries
/// DELETE FROM t WHERE pk = 1 USING TIMESTAMP 1_700_000_005_000_000
/// UPDATE t SET v = 'live' WHERE pk = 1 AND ck = 9
///   USING TIMESTAMP 1_700_000_020_000_000   -- survives the deletion
/// SELECT * FROM t WHERE pk = 1              -- Cassandra: exactly ONE row, ck = 9
/// ```
///
/// `ck = 2` has no liveness marker and no live data cell, so Cassandra never yields it
/// (its lone `w` tombstone is purged and the emptied row dropped). `ck = 9` carries a
/// live `v` written strictly AFTER the deletion, so it IS returned. Asserting the
/// surviving row SET — `[9]`, not "0 rows" — is what distinguishes the correct fix
/// from a presence rule that wrongly hides every row of a deleted partition.
#[tokio::test]
async fn multigen_partition_deleted_markerless_tombstone_row_is_hidden_but_its_live_sibling_is_not()
{
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, data_dir) = build_two_generation_fixture(
        &schema,
        vec![update_set_w_null_markerless(2)],
        vec![update_v_live(9)],
    )
    .await;

    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    assert_eq!(
        data_db_paths_by_generation(&live_root.join(KS).join(TBL)).len(),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );

    // (1) ANTI-VACUITY (a DECODE PROBE, not a parity claim): both rows are physically
    //     present and decodable before the deletion is patched in. Cassandra would
    //     return only `ck = 9` even here — it purges `ck = 2`'s lone tombstone and
    //     drops the emptied row — so the `ck = 2` entry pinned below is CQLite's
    //     CURRENT marker-less behaviour, tracked as #3121. Its purpose is solely to
    //     prove the fixture is non-empty and both generations merged.
    let live_cks = multigen_ck_values(&live_root, &schema).await;
    assert_eq!(
        live_cks,
        vec![2, 9],
        "decode probe (current CQLite behaviour, NOT Cassandra parity — #3121): both \
         merged rows must be present and decodable BEFORE the partition tombstone is \
         patched in — got {live_cks:?}"
    );

    // (2) THE PIN: the partition deletion hides the marker-less tombstone-only row and
    //     NOTHING else. Cassandra returns exactly one row, `ck = 9`.
    patch_newest_generation_partition_deletion(&deleted_table_dir);
    let deleted_cks = multigen_ck_values(&deleted_root, &schema).await;

    assert_eq!(
        deleted_cks,
        vec![9],
        "issue #3129 AC1: under a partition deletion @{T_PARTITION_DELETE_MICROS}µs, the \
         marker-less row ck=2 (only cell a `w` tombstone @{T_CELL_TOMB_MICROS}µs) has no \
         live data and no liveness marker, so Cassandra never yields it; the sibling \
         ck=9 has a live `v` @{T_SIBLING_LIVE_MICROS}µs that OUTLIVES the deletion and \
         must be returned. Expected [9], got {deleted_cks:?} — `[2, 9]` is the phantom \
         resurrection, `[]` would be an over-hide of the whole partition."
    );

    drop(temp);
}

/// Issue #3129 AC3 (the both-halves differential) — ONE partition deletion, TWO rows
/// that each carry a surviving cell tombstone, and OPPOSITE correct answers. This is
/// the pin that fails if EITHER half of the fix is reverted.
///
/// ```cql
/// -- generation 1
/// INSERT INTO t (pk, ck) VALUES (1, 4)
///   USING TIMESTAMP 1_700_000_020_000_000   -- marker NEWER than the deletion below
/// -- generation 2, whose partition header carries
/// DELETE FROM t WHERE pk = 1 USING TIMESTAMP 1_700_000_005_000_000
/// UPDATE t SET w = null WHERE pk = 1 AND ck = 2
///   USING TIMESTAMP 1_700_000_010_000_000   -- marker-LESS row
/// UPDATE t SET w = null WHERE pk = 1 AND ck = 4
///   USING TIMESTAMP 1_700_000_010_000_000   -- same tombstone on the marked row
/// SELECT * FROM t WHERE pk = 1              -- Cassandra: exactly ONE row, ck = 4
/// ```
///
/// Cassandra's rule (`DeletionTime.deletes(ts) = ts <= markedForDeleteAt`, and a row
/// with no live data and no live primary-key liveness marker is never yielded) gives:
///
/// - `ck = 2` — no marker, no live cell ⇒ HIDDEN. Only the tombstone's PRESENCE proves
///   the row is genuinely reduced rather than a truncated parse, so this row is the
///   one that needs the presence bit.
/// - `ck = 4` — its liveness marker at `…020…` OUTLIVES the deletion at `…005…` ⇒
///   VISIBLE (all columns null). Cassandra returns it; the surviving `w` tombstone is
///   merely purged. This row is the one that needs the marker TIMESTAMP, because
///   presence alone would hide it.
///
/// Revert-verify (both directions, each observed): forcing
/// `has_deleted_data_cell: false` yields `[2, 4]` (the phantom resurrects); forcing
/// `timestamp: None` yields `[]` (the marked row is wrongly hidden — a NEW defect the
/// presence bit would have introduced on its own). Only the two together give `[4]`.
#[tokio::test]
async fn multigen_partition_delete_hides_the_markerless_row_and_keeps_the_newer_marked_one() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, data_dir) = build_two_generation_fixture(
        &schema,
        vec![insert_liveness_marker_only(4, T_SIBLING_LIVE_MICROS)],
        vec![
            update_set_w_null_markerless(2),
            update_set_w_null_markerless(4),
        ],
    )
    .await;

    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    assert_eq!(
        data_db_paths_by_generation(&live_root.join(KS).join(TBL)).len(),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );

    // (1) ANTI-VACUITY decode probe: both rows are physically present and decodable
    //     before the deletion is patched in (`ck = 2` only because CQLite does not yet
    //     purge a marker-less tombstone-only row — #3121).
    let live_cks = multigen_ck_values(&live_root, &schema).await;
    assert_eq!(
        live_cks,
        vec![2, 4],
        "decode probe (current CQLite behaviour, NOT Cassandra parity — #3121): both \
         merged rows must be present and decodable BEFORE the partition tombstone is \
         patched in — got {live_cks:?}"
    );

    // (2) THE PIN: one deletion, opposite answers.
    patch_newest_generation_partition_deletion(&deleted_table_dir);
    let deleted_cks = multigen_ck_values(&deleted_root, &schema).await;

    assert_eq!(
        deleted_cks,
        vec![4],
        "issue #3129 AC3: under one partition deletion @{T_PARTITION_DELETE_MICROS}µs, \
         ck=2 (marker-less, only a `w` tombstone @{T_CELL_TOMB_MICROS}µs) must be HIDDEN \
         while ck=4 (liveness marker @{T_SIBLING_LIVE_MICROS}µs, strictly newer than the \
         deletion) must remain VISIBLE. Expected [4], got {deleted_cks:?} — `[2, 4]` means \
         tombstone PRESENCE is not reaching the row decision, `[]` means the surviving \
         marker TIMESTAMP is not."
    );

    drop(temp);
}

// ════════════════════════════════════════════════════════════════════════════════
// Issue #3129 AC2 — the SAME property on the other THREE multi-generation drivers
// ════════════════════════════════════════════════════════════════════════════════
//
// The three pins above all drive `merge_generations_for_read`. Three further public
// entry points reach a multi-generation merge, and each has its OWN emission loop or
// its own merger construction:
//
// | driver                                      | manager entry point            |
// |---------------------------------------------|--------------------------------|
// | `merge_generations_for_read_with_metadata`   | `scan_with_cell_metadata`      |
// | `stream_generations_for_read`                | `scan_stream`                  |
// | `seek_merge_generations_for_read`            | `scan_partition_clustering`    |
//
// `push_metadata_rows` is a SECOND COPY of the emission loop (not of the shadow
// logic): it and `partition_live_rows` both call the identical
// `ReadShadow::filter_live`, where the #3122 presence-bit fix lives, and those are its
// only two callers in the crate. The streaming and seeking drivers reuse
// `partition_live_rows` verbatim. So all four drivers are already CORRECT — these pins
// close a COVERAGE gap, and their job is to make any future divergence between the
// four emission paths (or a regression in the one shared `filter_live`) fail loudly on
// every driver rather than only on the materializing one.
//
// Each pin uses the AC1 fixture verbatim (marker-less `ck = 2` in generation 1, live
// `ck = 9` in generation 2, partition deletion patched into generation 2's header) and
// asserts the same Cassandra answer, `[9]`, evaluated at the PINNED `now`.
//
// ## Why the core assertion is itself the fallback detector
//
// Every one of these drivers falls back SILENTLY to a per-reader CONCATENATION when
// its multi-generation guard is not met. That fallback is not reconciling: generation
// 1 (no partition deletion in its own header) still yields `ck = 2`, and generation 2
// yields `ck = 9`, so a fallback returns `[2, 9]` — which is exactly the phantom
// resurrection these pins forbid. A prune/registration-induced fallback therefore
// FAILS the assertion loudly instead of passing it vacuously. `open_manager` asserts
// the two generations were registered on top of that, and each pin additionally reads
// the UNPATCHED copy of the very same generations first.

/// The partition key BYTES for `pk = 1` — a single `int` partition column, so the
/// key is its 4-byte big-endian value (Cassandra's `Int32Type` encoding; guide App.B).
/// Cross-checked against the key the merge path actually emits before it is used.
fn partition_key_bytes() -> Vec<u8> {
    PK_VALUE.to_be_bytes().to_vec()
}

/// Build the AC1 fixture — generation 1: the MARKER-LESS `ck = 2` row whose only
/// non-key cell is a `w` tombstone; generation 2: the live `ck = 9` sibling whose `v`
/// is written strictly AFTER the partition deletion patched into that generation's
/// header. Returns `(tempdir, unpatched root, patched root)`.
async fn build_markerless_ac1_fixture(
    schema: &TableSchema,
) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (temp, data_dir) = build_two_generation_fixture(
        schema,
        vec![update_set_w_null_markerless(2)],
        vec![update_v_live(9)],
    )
    .await;

    let live_root = temp.path().join("live");
    let deleted_root = temp.path().join("deleted");
    copy_table_dir(&data_dir, &live_root);
    let deleted_table_dir = copy_table_dir(&data_dir, &deleted_root);

    assert_eq!(
        data_db_paths_by_generation(&live_root.join(KS).join(TBL)).len(),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );
    patch_newest_generation_partition_deletion(&deleted_table_dir);

    (temp, live_root, deleted_root)
}

/// Drive `SSTableManager::scan_with_cell_metadata` (the WRITETIME/TTL projection) over
/// `root`, returning `(ck, WRITETIME(v))` per emitted row in emission order.
///
/// The `WRITETIME(v)` component is deliberately part of the pinned value: it is
/// produced by `push_metadata_rows` from the merge WINNER's cell, so a pin that
/// asserts it proves the metadata emission loop really ran and really carried
/// authoritative per-cell metadata, not that a row merely appeared.
async fn multigen_metadata_ck_and_v_writetime(
    root: &Path,
    schema: &TableSchema,
) -> Vec<(i32, Option<i64>)> {
    let manager = open_manager(root).await;
    manager
        .scan_with_cell_metadata(&table_id(), None, None, None, Some(schema))
        .await
        .expect("multi-generation metadata scan must not error")
        .iter()
        .map(|(_, row, meta)| (ck_of(row), meta.get("v").map(|m| m.write_timestamp_micros)))
        .collect()
}

/// Drive `SSTableManager::scan_stream` (the STREAMING multi-generation merge) over
/// `root` and drain it to completion, returning the `ck` of every emitted row in
/// emission order. A producer that dies mid-stream surfaces as an `Err` item, which
/// panics here rather than truncating the result set into a silent pass.
#[cfg(not(feature = "tombstones"))]
async fn multigen_stream_ck_values(root: &Path, schema: &TableSchema) -> Vec<i32> {
    let manager = open_manager(root).await;
    let mut stream = manager
        .scan_stream(&table_id(), None, None, Some(schema), 64)
        .await
        .expect("multi-generation scan_stream must construct");
    let mut out = Vec::new();
    while let Some(item) = stream.recv().await {
        let (_, row) =
            item.expect("streamed row must be Ok — a dead producer is NOT an end of stream");
        out.push(ck_of(&row));
    }
    out
}

/// Drive `SSTableManager::scan_partition_clustering` (the partition-SEEKING
/// multi-generation merge) over `root` for `pk = 1`, returning
/// `(ck values, clustering_seek_engaged)`.
#[cfg(not(feature = "tombstones"))]
async fn multigen_seek_ck_values(root: &Path, schema: &TableSchema) -> (Vec<i32>, bool) {
    let manager = open_manager(root).await;
    let (rows, engaged) = manager
        .scan_partition_clustering(&table_id(), &partition_key_bytes(), None, Some(schema))
        .await
        .expect("multi-generation partition seek must not error");
    (rows.iter().map(|(_, row)| ck_of(row)).collect(), engaged)
}

/// Issue #3129 AC2 — driver 2 of 4: the WRITETIME/TTL metadata projection
/// (`SSTableManager::scan_with_cell_metadata` ⇒
/// `generation_merge::merge_generations_for_read_with_metadata` ⇒
/// `push_metadata_rows` ⇒ `ReadShadow::filter_live`).
///
/// `push_metadata_rows` is a second, independent copy of the emission loop, so the
/// materializing pins above do not cover it. Same fixture, same Cassandra answer.
#[tokio::test]
async fn multigen_metadata_projection_partition_deleted_markerless_tombstone_row_is_hidden() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, live_root, deleted_root) = build_markerless_ac1_fixture(&schema).await;

    // (1) ANTI-VACUITY (a DECODE PROBE of current CQLite behaviour, not a parity
    //     claim — #3121): with NO partition deletion both rows are physically present
    //     and decodable through the METADATA merge, and the surviving `v` carries its
    //     real write timestamp. A broken fixture, a lost generation or a metadata map
    //     that silently came back empty fails here.
    let live = multigen_metadata_ck_and_v_writetime(&live_root, &schema).await;
    assert_eq!(
        live,
        vec![(2, None), (9, Some(T_SIBLING_LIVE_MICROS))],
        "decode probe: BEFORE the partition tombstone is patched in, the metadata merge \
         must emit both merged rows, and ck=9's `v` must carry its authoritative \
         WRITETIME @{T_SIBLING_LIVE_MICROS}µs — got {live:?}"
    );

    // (2) THE PIN: the marker-less tombstone-only row is not resurrected, and the live
    //     sibling keeps both its value AND its authoritative WRITETIME.
    let deleted = multigen_metadata_ck_and_v_writetime(&deleted_root, &schema).await;
    assert_eq!(
        deleted,
        vec![(9, Some(T_SIBLING_LIVE_MICROS))],
        "issue #3129 AC2 (metadata driver): under a partition deletion \
         @{T_PARTITION_DELETE_MICROS}µs the marker-less row ck=2 (only cell a `w` \
         tombstone @{T_CELL_TOMB_MICROS}µs) has no live data and no liveness marker, so \
         Cassandra never yields it, while ck=9's live `v` @{T_SIBLING_LIVE_MICROS}µs \
         OUTLIVES the deletion. Expected [(9, Some({T_SIBLING_LIVE_MICROS}))], got \
         {deleted:?} — a `(2, _)` entry is the phantom resurrection reaching the \
         WRITETIME/TTL projection through `push_metadata_rows`, `[]` an over-hide of the \
         whole partition."
    );

    drop(temp);
}

/// Issue #3129 AC2 — driver 3 of 4: the STREAMING merge
/// (`SSTableManager::scan_stream` ⇒ `generation_merge::stream_generations_for_read` ⇒
/// `partition_live_rows` ⇒ `ReadShadow::filter_live`).
///
/// `scan` and `scan_stream` must agree row-for-row (issue #957); this pins that they
/// also agree on read-time row VISIBILITY under a partition deletion.
#[cfg(not(feature = "tombstones"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multigen_streaming_scan_partition_deleted_markerless_tombstone_row_is_hidden() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, live_root, deleted_root) = build_markerless_ac1_fixture(&schema).await;

    // (1) ANTI-VACUITY (decode probe, #3121): the stream really carries both merged
    //     rows before the deletion is patched in — so a stream that ended early, or
    //     one that never engaged the merge, fails here rather than making the pin
    //     below trivially true.
    let live_cks = multigen_stream_ck_values(&live_root, &schema).await;
    assert_eq!(
        live_cks,
        vec![2, 9],
        "decode probe (current CQLite behaviour, NOT Cassandra parity — #3121): the \
         streaming merge must emit both merged rows BEFORE the partition tombstone is \
         patched in — got {live_cks:?}"
    );

    // (2) THE PIN.
    let deleted_cks = multigen_stream_ck_values(&deleted_root, &schema).await;
    assert_eq!(
        deleted_cks,
        vec![9],
        "issue #3129 AC2 (streaming driver): under a partition deletion \
         @{T_PARTITION_DELETE_MICROS}µs, `scan_stream` must emit exactly the rows `scan` \
         does — the marker-less ck=2 (only a `w` tombstone @{T_CELL_TOMB_MICROS}µs) \
         HIDDEN, the live sibling ck=9 @{T_SIBLING_LIVE_MICROS}µs EMITTED. Expected [9], \
         got {deleted_cks:?} — `[2, 9]` is the phantom resurrection reaching the \
         streaming path (and a `scan`/`scan_stream` divergence), `[]` an over-hide."
    );

    drop(temp);
}

/// Issue #3129 AC2 — driver 4 of 4: the partition-SEEKING merge
/// (`SSTableManager::scan_partition_clustering` ⇒
/// `generation_merge::seek_merge_generations_for_read` ⇒ `partition_live_rows` ⇒
/// `ReadShadow::filter_live`), i.e. the point-read path a `WHERE pk = ?` query takes.
///
/// Extra care here: this driver's multi-generation branch is entered only when MORE
/// THAN ONE candidate survives bloom/BTI pruning, and otherwise it drops silently to a
/// per-reader concat. Both generations physically contain `pk = 1`, and a bloom filter
/// has no false negatives, so both MUST admit the key — but the pin does not rely on
/// that argument alone: `open_manager` asserts both generations registered, the
/// unpatched read asserts what the fixture really holds, and the concat fallback would
/// return `[2, 9]`, failing the pin loudly.
#[cfg(not(feature = "tombstones"))]
#[tokio::test]
async fn multigen_partition_seek_partition_deleted_markerless_tombstone_row_is_hidden() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, live_root, deleted_root) = build_markerless_ac1_fixture(&schema).await;

    // The partition key the seek is given must be the key the merge path actually
    // emits — otherwise the seek would target a partition that does not exist and
    // return an empty result, satisfying a "0 rows" expectation for the wrong reason.
    let emitted_keys: Vec<Vec<u8>> = open_manager(&live_root)
        .await
        .scan(&table_id(), None, None, None, Some(&schema))
        .await
        .expect("multi-generation scan must not error")
        .iter()
        .map(|(key, _)| key.as_bytes().to_vec())
        .collect();
    assert!(
        !emitted_keys.is_empty() && emitted_keys.iter().all(|k| *k == partition_key_bytes()),
        "the seek target must be the SAME partition key the merge path emits (pk = \
         {PK_VALUE} as 4-byte BE) — emitted {emitted_keys:?}"
    );

    // (1) ANTI-VACUITY (decode probe, #3121): the seek really reaches both merged rows
    //     before the deletion is patched in.
    let (live_cks, live_engaged) = multigen_seek_ck_values(&live_root, &schema).await;
    assert_eq!(
        live_cks,
        vec![2, 9],
        "decode probe (current CQLite behaviour, NOT Cassandra parity — #3121): the \
         seeking merge must reach both merged rows BEFORE the partition tombstone is \
         patched in — got {live_cks:?}"
    );
    assert!(
        !live_engaged,
        "the cross-generation seeking merge decodes full partitions and must report \
         clustering_seek_engaged = false (it is the honest non-engaged signal for the \
         merge branch); a `true` here means a DIFFERENT branch served the read"
    );

    // (2) THE PIN.
    let (deleted_cks, deleted_engaged) = multigen_seek_ck_values(&deleted_root, &schema).await;
    assert_eq!(
        deleted_cks,
        vec![9],
        "issue #3129 AC2 (seeking driver): under a partition deletion \
         @{T_PARTITION_DELETE_MICROS}µs, the point-read path `WHERE pk = {PK_VALUE}` must \
         hide the marker-less ck=2 (only a `w` tombstone @{T_CELL_TOMB_MICROS}µs) and \
         keep the live sibling ck=9 @{T_SIBLING_LIVE_MICROS}µs. Expected [9], got \
         {deleted_cks:?} — `[2, 9]` is either the phantom resurrection or a \
         prune-induced fall back to the NON-reconciling per-reader concat, `[]` an \
         over-hide."
    );
    assert!(
        !deleted_engaged,
        "clustering_seek_engaged must stay false for the cross-generation merge branch"
    );

    drop(temp);
}
