//! Issue #1579 (Epic D / D3): streaming the multi-generation merge must hold
//! only O(window) rows resident — one reconciled partition at a time — NOT
//! collect the whole reconciled table before emitting.
//!
//! This is the memory guard. It uses the cfg-gated streaming-merge probe
//! (`cqlite_core::storage::sstable::stream_merge_probe`) to observe the PEAK
//! number of rows the streaming producer held resident at one instant — the
//! no-heuristics way to prove O(window): observe the *work*, not just the
//! (identical) row set.
//!
//! On `main`/pre-D3 `scan_stream` over >1 generation collected the ENTIRE
//! reconciled result into a `Vec`, sorted it, and only then dribbled it — so the
//! resident high-water mark was `merged.len()`, proportional to the table, and
//! this guard flips red as the fixture grows. After D3 the driver feeds each
//! stepped partition straight into the channel, so the peak is one partition's
//! width — FLAT between a small and a large fixture, and O(1) for single-row
//! partitions.
//!
//! The counter getter + reset live behind the `work-counters` feature; the
//! counter is a shared process-global, so every test here serializes on a shared
//! mutex.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine,work-counters \
//!     --test issue_1579_streaming_multigen_memory

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::stream_merge_probe;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use serial_test::serial;
use tempfile::TempDir;

const KS: &str = "stream_mem_ks";
const TBL: &str = "rows";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  v int\n);\n")
}

fn write_mutation(id: i32, v: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "v".to_string(),
        value: Value::Integer(v),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn delete_mutation(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        TableId::new(KS, TBL),
        pk,
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_string_lossy();
            n.ends_with("-big-Data.db") || n.ends_with("-Data.db")
        })
        .count()
}

/// Build a TWO-generation fixture of `n` single-row partitions. gen2 overwrites
/// every 5th partition and deletes every 7th (so reconciliation — not mere
/// concatenation — runs), leaving two Data.db files on disk.
async fn open_two_gen(n: i32) -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            use cqlite_core::schema::parse_cql_schema;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
            let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine");

            // gen1: n rows.
            for id in 0..n {
                engine.write(write_mutation(id, id, 100)).expect("write g1");
            }
            rt.block_on(engine.flush())
                .expect("flush g1")
                .expect("g1 must produce an SSTable");

            // gen2: overwrites + deletes across generations.
            for id in (0..n).step_by(5) {
                engine
                    .write(write_mutation(id, id * 2, 200))
                    .expect("write g2 overwrite");
            }
            for id in (0..n).step_by(7) {
                engine
                    .write(delete_mutation(id, 300))
                    .expect("write g2 del");
            }
            rt.block_on(engine.flush())
                .expect("flush g2")
                .expect("g2 must produce an SSTable");
            rt.block_on(engine.close()).expect("close");

            assert_eq!(
                count_data_files(&data_dir.join(KS).join(TBL)),
                2,
                "fixture must produce exactly 2 generations (no compaction)"
            );
        })
        .await
        .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    (result.database, temp_dir)
}

/// Drain the whole stream (buffer_size = 1 to force per-row backpressure so the
/// producer cannot race ahead and hide its resident window) and return the peak
/// resident rows the streaming merge held.
async fn stream_peak_resident(db: &Database) -> u64 {
    stream_merge_probe::reset();
    let config = StreamingConfig {
        buffer_size: 1,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(&format!("SELECT * FROM {KS}.{TBL}"), config)
        .await
        .expect("execute_streaming");
    let mut count = 0u64;
    while let Some(row) = iter.next_async().await {
        row.expect("streamed row Ok");
        count += 1;
    }
    assert!(count > 0, "fixture must yield rows");
    stream_merge_probe::peak_resident()
}

/// Streaming a multi-generation table holds O(window) rows resident: the peak is
/// FLAT across a 40x size difference and O(1) in absolute terms (single-row
/// partitions ⇒ one row at a time). On `main`/pre-D3 the eager collect+sort held
/// the WHOLE table, so the peak was proportional to row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn streaming_multigen_merge_is_o_window() {
    let (small_db, _s) = open_two_gen(40).await;
    let (large_db, _l) = open_two_gen(1600).await;

    let small_peak = stream_peak_resident(&small_db).await;
    let large_peak = stream_peak_resident(&large_db).await;

    // Flat across a 40x size difference — the discriminating property. On `main`
    // this is ~40 vs ~1600 (proportional to rows); after D3 it is 1 vs 1.
    assert_eq!(
        small_peak, large_peak,
        "Issue #1579: streaming multi-generation merge peak resident rows must be FLAT \
         across fixture sizes (small={small_peak}, large={large_peak}); a size-proportional \
         peak means the whole reconciled table was materialized before streaming"
    );
    // And O(1) in absolute terms (one single-row partition resident at a time).
    assert!(
        large_peak <= 1,
        "Issue #1579: streaming 1600 single-row partitions must hold O(1) rows resident, \
         got {large_peak}"
    );
}
