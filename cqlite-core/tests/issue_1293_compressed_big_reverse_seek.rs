//! Issue #1293 — fail-closed CI coverage for the COMPRESSED BIG ("nb")
//! reverse/seek window path.
//!
//! The #1184 CI fixture (`issue_1184_big_promoted_read_seek.rs`) is built with the
//! production `WriteEngine`, which writes an **uncompressed** Data.db. It therefore
//! only exercises the *uncompressed* arm of
//! `BigPromotedSelector::decompress_partition_window` (the `stitch_all_chunks` /
//! `compression_reader == None` branch). The **compressed** arm — chunk-stitching
//! over `CompressionInfo.db` offsets + LZ4 `pull_reverse_chunk` decompress — is the
//! production-dominant path (real Cassandra SSTables are LZ4-compressed by default)
//! yet was only covered by the skip-on-absence real-fixture test, so a regression in
//! the compressed window arithmetic could pass CI green.
//!
//! This test closes that gap fail-closed: it builds the SAME multi-block wide BIG
//! partition with the write engine, then **wraps the write-engine Data.db output
//! through the compressing writer** (`CompressedDataWriter` + `CompressionInfoWriter`,
//! 16 KiB chunks → many promoted-index blocks per partition), producing a genuine
//! LZ4-compressed BIG SSTable with a `CompressionInfo.db` sidecar. The reader then
//! auto-detects compression and drives the COMPRESSED arm of
//! `decompress_partition_window` + `pull_reverse_chunk`.
//!
//! Fail-closed: the fixture is generated in-test (no fetched binaries, no
//! skip-on-absence). If compression is not actually engaged, or the window
//! arithmetic regresses on either path, the test FAILS rather than skips.
//!
//! Coverage (compressed fixture):
//!   1. Compression is genuinely engaged — `CompressionInfo.db` exists and the
//!      reader reports the SSTable as compressed (no-heuristics guard against a
//!      silent uncompressed fallback that would invalidate the whole test).
//!   2. Forward seek — `WHERE pk=1 AND ck>100 AND ck<140` returns exactly the live
//!      `ck` in `(100,140)`, decoding strictly fewer rows than the partition total
//!      (the compressed window is bounded, not a full-partition decompress).
//!   3. Forward vs reverse — `ORDER BY ck DESC` returns the identical clustering set
//!      as ASC in exact reverse order, driven by a back-to-front compressed block
//!      walk (`reverse_blocks_decoded > 1`) with per-iteration memory bounded to one
//!      block (`reverse_peak_block_rows` far below the partition total) AND at least
//!      one chunk actually decompressed.

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    feature = "lz4",
    not(feature = "tombstones")
))]

use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::sstable::writer::{
    create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "test_1293";
const TBL: &str = "wide_compressed";

/// ck 0..999 EXCEPT the gap 30..39 (the CI-runnable analogue of the real fixture's
/// range tombstone straddling a promoted-index block boundary).
const N_CK: i32 = 1000;
const GAP_LO: i32 = 30;
const GAP_HI: i32 = 40; // exclusive

/// ~512 B/row × 1000 rows ≈ 500 KB → many promoted-index blocks.
const PAYLOAD_LEN: usize = 512;

/// Uncompressed chunk size for the re-compressed Data.db. Deliberately small (16
/// KiB, Cassandra's legacy default) so the ~500 KB partition spans MANY compressed
/// chunks — the reverse path then provably walks several chunks back-to-front
/// (`reverse_blocks_decoded > 1`) rather than decompressing the whole partition in
/// one chunk.
const CHUNK_SIZE: usize = 16 * 1024;

/// Process-global probes (work counters + access-path) — serialize the tests.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  payload text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

fn live_cks() -> Vec<i32> {
    (0..N_CK)
        .filter(|c| !(GAP_LO..GAP_HI).contains(c))
        .collect()
}

fn write_row(pk: i32, ck: i32, payload: &str, ts: i64) -> Mutation {
    let table_id = TableId::new(KS, TBL);
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Integer(ck)));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, partition_key, clustering_key, ops, ts, None)
}

/// Build the wide partition (pk=1) with the production write engine into `data_dir`.
fn build_uncompressed_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path) {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let payload = "p".repeat(PAYLOAD_LEN);
    let mut ts = 1_000_000i64;
    for ck in live_cks() {
        engine
            .write(write_row(1, ck, &payload, ts))
            .expect("write wide row");
        ts += 1;
    }

    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush must produce an SSTable");
    rt.block_on(engine.close()).expect("close engine");
}

/// Locate the flushed `*-Data.db` and derive its `<base>` (e.g. `nb-1-big`).
fn find_data_db(data_dir: &std::path::Path) -> (std::path::PathBuf, String) {
    for entry in walkdir(data_dir) {
        let name = entry
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        if let Some(base) = name.strip_suffix("-Data.db") {
            return (entry.clone(), base.to_string());
        }
    }
    panic!("no *-Data.db produced under {}", data_dir.display());
}

/// Minimal recursive file walk (avoids a walkdir dev-dep).
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    out
}

/// Wrap the uncompressed write-engine Data.db through the compressing writer:
/// re-chunk + LZ4-compress its bytes, overwrite Data.db with the compressed stream,
/// emit the `<base>-CompressionInfo.db` sidecar, and drop the now-stale uncompressed
/// `<base>-CRC.db` (compressed BIG carries per-chunk CRCs inline). The reader
/// auto-detects the sidecar and routes the BIG promoted-index reverse/seek path
/// through its COMPRESSED arm.
///
/// This is sound because the CQLite uncompressed-BIG Data.db is HEADERLESS (data
/// starts at byte 0) and Index.db offsets are in the uncompressed domain — exactly
/// the invariants the compressed reader assumes (CompressionInfo chunk offsets are
/// relative to Data.db byte 0).
fn compress_data_db_in_place(data_path: &std::path::Path, base: &str) {
    let uncompressed = std::fs::read(data_path).expect("read uncompressed Data.db");
    assert!(
        uncompressed.len() > CHUNK_SIZE * 4,
        "fixture invariant: uncompressed Data.db ({} B) must span many {CHUNK_SIZE}-B chunks",
        uncompressed.len()
    );

    let compressor = create_compressor(CompressionAlgorithm::Lz4).expect("lz4 compressor");
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, CHUNK_SIZE);
    writer.write(&uncompressed).expect("compress Data.db");
    let (compressed, metadata) = writer.finish().expect("finish compression");
    assert!(
        metadata.chunk_count() > 4,
        "fixture invariant: must produce several compressed chunks, got {}",
        metadata.chunk_count()
    );
    assert_eq!(
        metadata.data_length as usize,
        uncompressed.len(),
        "compressed data_length must equal the uncompressed Data.db size"
    );

    std::fs::write(data_path, &compressed).expect("overwrite Data.db with compressed bytes");

    let parent = data_path.parent().expect("Data.db parent dir");
    let info_path = parent.join(format!("{base}-CompressionInfo.db"));
    CompressionInfoWriter::new(info_path.clone())
        .write(&metadata)
        .expect("write CompressionInfo.db");
    assert!(info_path.exists(), "CompressionInfo.db must be written");

    // The uncompressed-BIG CRC.db describes the old uncompressed chunking and no
    // longer matches; compressed BIG stores per-chunk CRCs inline in Data.db.
    let crc_path = parent.join(format!("{base}-CRC.db"));
    let _ = std::fs::remove_file(&crc_path);
}

async fn open_compressed_db() -> (TempDir, Arc<Database>, std::path::PathBuf) {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let schema_path = temp.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            build_uncompressed_fixture(&data_dir, &wal_dir);
            let (data_path, base) = find_data_db(&data_dir);
            compress_data_db_in_place(&data_path, &base);
        })
        .await
        .expect("fixture build task");
    }

    let (data_path, base) = {
        let data_dir = data_dir.clone();
        tokio::task::spawn_blocking(move || find_data_db(&data_dir))
            .await
            .expect("locate data db")
    };
    let info_path = data_path
        .parent()
        .unwrap()
        .join(format!("{base}-CompressionInfo.db"));

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest compressed wide-partition fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must load"
    );
    (temp, Arc::new(result.database), info_path)
}

fn cks(rows: &[QueryRow]) -> Vec<i32> {
    let mut out: Vec<i32> = rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

fn cks_in_order(rows: &[QueryRow]) -> Vec<i32> {
    rows.iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect()
}

// ─────────────── 1. Compression genuinely engaged ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_fixture_actually_compressed() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db, info_path) = open_compressed_db().await;

    // Fail-closed: if the sidecar is missing the reader would silently fall back to
    // the uncompressed arm and the rest of the suite would prove nothing.
    assert!(
        info_path.exists(),
        "Issue #1293: CompressionInfo.db must exist so the reader routes through the \
         COMPRESSED BIG arm — its absence means the fixture is not compressed: {}",
        info_path.display()
    );

    // And the partition still reads back in full through the compressed arm.
    let full = db
        .execute(&format!("SELECT pk, ck FROM {KS}.{TBL} WHERE pk = 1"))
        .await
        .expect("full partition read over compressed Data.db");
    assert_eq!(
        full.rows.len(),
        live_cks().len(),
        "Issue #1293: compressed pk=1 must read back {} live rows",
        live_cks().len()
    );
    drop(temp);
}

// ─────────────── 2. Forward seek (compressed arm) ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_forward_clustering_slice_is_bounded() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db, _info) = open_compressed_db().await;

    work_counters::reset();
    cqlite_core::query::access_path::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck > 100 AND ck < 140"
        ))
        .await
        .expect("forward slice query over compressed Data.db");
    let rows_decoded = work_counters::rows_decoded();
    let chunks = work_counters::chunks_decompressed();

    let expected: Vec<i32> = (101..140).collect();
    assert_eq!(
        cks(&res.rows),
        expected,
        "Issue #1293: compressed ck in (100,140) must return ck=101..=139"
    );
    assert_eq!(
        res.metadata.access_path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1293: an engaged compressed BIG promoted-index slice must report \
         ClusteringSlice, got {:?}",
        res.metadata.access_path
    );
    assert!(
        rows_decoded > 0 && rows_decoded < live_cks().len() as u64,
        "Issue #1293: rows_decoded ({rows_decoded}) must be > 0 and strictly below the \
         partition's {} rows — a regression to full-partition decompress reads them all",
        live_cks().len()
    );
    // The compressed window arithmetic must have decompressed at least one chunk
    // (and, because the slice is bounded, NOT every chunk in the partition).
    assert!(
        chunks > 0,
        "Issue #1293: the compressed arm must decompress at least one chunk for the slice, \
         got chunks_decompressed={chunks}"
    );
    drop(temp);
}

// ─────────────── 3. Forward vs reverse (compressed block walk) ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_reverse_matches_forward_via_chunk_walk() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db, _info) = open_compressed_db().await;

    let asc = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 ORDER BY ck ASC"
        ))
        .await
        .expect("asc query over compressed Data.db");
    let asc_order = cks_in_order(&asc.rows);

    work_counters::reset();
    let desc = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 ORDER BY ck DESC"
        ))
        .await
        .expect("desc query over compressed Data.db");
    let blocks = work_counters::reverse_blocks_decoded();
    let peak = work_counters::reverse_peak_block_rows();
    let chunks = work_counters::chunks_decompressed();
    let desc_order = cks_in_order(&desc.rows);

    // Identical set, exact reverse ordering — proven over the compressed arm.
    let mut asc_sorted = asc_order.clone();
    asc_sorted.sort_unstable();
    assert_eq!(asc_sorted, live_cks(), "ASC must be the full live ck set");
    assert_eq!(
        desc_order,
        asc_order.iter().rev().copied().collect::<Vec<_>>(),
        "Issue #1293: compressed DESC must be the exact reverse of the ASC ordering — \
         a regression in `pull_reverse_chunk` / the compressed `decompress_partition_window` \
         arm drops or reorders rows"
    );

    // Back-to-front block walk drove it, bounded to one block per iteration, and the
    // compressed arm actually decompressed chunks.
    assert!(
        blocks > 1,
        "Issue #1293: compressed reverse must decode multiple promoted-index blocks \
         back-to-front, got reverse_blocks_decoded={blocks}"
    );
    assert!(
        peak > 0 && peak < live_cks().len() as u64,
        "Issue #1293: per-iteration block buffer ({peak}) must be bounded to one block, \
         far below the partition's {} rows",
        live_cks().len()
    );
    assert!(
        chunks > 1,
        "Issue #1293: the compressed reverse walk must decompress multiple chunks \
         (`pull_reverse_chunk`), got chunks_decompressed={chunks}"
    );
    drop(temp);
}

// ─────────────── 4. Boundary across a clustering gap (compressed) ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_ranged_read_across_gap_keeps_adjacent_rows() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db, _info) = open_compressed_db().await;

    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck >= 25 AND ck <= 45"
        ))
        .await
        .expect("boundary slice query over compressed Data.db");

    let returned = cks(&res.rows);
    let expected: Vec<i32> = (25..=45)
        .filter(|c| !(GAP_LO..GAP_HI).contains(c))
        .collect();
    assert_eq!(
        returned, expected,
        "Issue #1293: compressed ck in [25,45] must return 25..29 and 40..45 (gap 30..39 \
         absent), keeping the rows adjacent to the gap (ck 29 and ck 40)"
    );
    assert!(returned.contains(&29) && returned.contains(&40));
    assert!(!returned.iter().any(|c| (GAP_LO..GAP_HI).contains(c)));
    drop(temp);
}
