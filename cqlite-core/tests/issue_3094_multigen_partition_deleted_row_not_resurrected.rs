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
//! into the row decision, so both paths agree.
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
//! ## Anti-vacuity
//!
//! The UNPATCHED copy of the very same two generations is read first and must return
//! exactly ONE row — which here IS Cassandra parity (the pure-PK `INSERT`'s liveness
//! marker is live, so `SELECT` returns one all-null row). That proves the fixture is
//! non-empty, the two generations really are merged, and the read does not error.
//! Only then is the partition deletion patched in and the read repeated, which must
//! return ZERO rows. A broken/empty fixture fails the first assertion instead of
//! silently satisfying the second.
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
use cqlite_core::Config;

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
/// The read clock (seconds), pinned so the read is deterministic (#2642).
const PINNED_NOW: i64 = T_BASE_SECS + 100;
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  v text,\n  w text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
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
        T_INSERT_MICROS,
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

/// Flush the liveness marker into generation 1 and the cell tombstone into
/// generation 2 (two separate flushes, no compaction), returning
/// `(tempdir, data_dir)`.
async fn build_two_generation_fixture(schema: &TableSchema) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    engine.write(insert_liveness_marker_only()).expect("write 1");
    engine
        .flush()
        .await
        .expect("flush 1")
        .expect("flush 1 produced no SSTable");

    engine.write(update_set_w_null()).expect("write 2");
    engine
        .flush()
        .await
        .expect("flush 2")
        .expect("flush 2 produced no SSTable");

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

/// Scan `root` through the manager's MULTI-GENERATION merge path (`candidates > 1`
/// ⇒ `merge_generations_for_read` ⇒ `partition_live_rows` ⇒ `ReadShadow::
/// filter_live`) and return the number of live rows.
async fn multigen_row_count(root: &Path, schema: &TableSchema) -> usize {
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

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let rows = manager
        .scan(&table_id, None, None, None, Some(schema))
        .await
        .expect("multi-generation scan must not error");
    rows.len()
}

#[tokio::test]
async fn multigen_partition_deleted_liveness_row_with_newer_cell_tombstone_is_not_resurrected() {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let (temp, data_dir) = build_two_generation_fixture(&schema).await;

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
