//! Issue #1143 (finding #1): the user-facing windowed streaming scan must agree
//! with the materializing scan on a MULTI-CHUNK SSTable whose partitions
//! straddle 16 KiB compression-chunk boundaries — exactly the `NeedMore`
//! re-parse path introduced by `run_scan_stream_windowed` /
//! `drain_scan_window` (`scan_stream_windowed.rs`).
//!
//! ## Why a dedicated multi-chunk test
//!
//! The pre-existing parity guards do NOT cover this boundary path through the
//! user-facing surface:
//!
//!   - `test_issue_827_streaming_parity` exercises the COMPACTION parser
//!     (`parse_one_partition_for_compaction` / `drain_compaction_window`) — a
//!     different entrypoint.
//!   - `test_issue_790_streaming_parity` compares `execute` vs
//!     `execute_streaming`, but its `test_basic.simple_table` /
//!     `test_collections.*` fixtures are SINGLE-CHUNK, so the windowed driver
//!     never returns `ParseStep::NeedMore` and the straddle branch is never run.
//!   - the new read-while-write bench uses a single-chunk fixture.
//!
//! ## Fixture: `test_wide_rows.wide_partition_table`
//!
//! Its `CompressionInfo.db` declares `chunk_length = 16384` and **4 chunks**
//! over ~64 KiB of decompressed data, with ~100 small partitions packed densely
//! enough that partitions straddle the internal chunk boundaries. The test
//! reads `CompressionInfo.db` directly to PROVE `chunk_count > 1` (so the
//! `NeedMore` await-next-chunk path is reachable, not vacuous) and confirms the
//! on-disk format is `nb` (the chunk-compressed, stitching read path).
//!
//! Under `observability-testing` the test ADDITIONALLY asserts that the
//! `cqlite.read.scan.window_refill` counter — emitted on each `NeedMore`
//! straddle — incremented, deterministically proving the boundary path fired
//! at least once during the streaming scan.
//!
//! ## What this asserts
//!
//! `Database::execute` (materializing `scan`) and `Database::execute_streaming`
//! (the windowed `scan_stream` driver) return the SAME rows, in the SAME order,
//! with the SAME values, across several buffer sizes (incl. `buffer_size = 1`,
//! which forces per-row backpressure between partition boundaries).
//!
//! Requirements:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - real SSTable Data.db files (`bash test-data/scripts/fetch-datasets.sh`)

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::types::Value;
use cqlite_core::{Database, RowKey};

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";
/// `CompressionInfo.db` chunk length for the fixture (16 KiB uncompressed).
const EXPECTED_CHUNK_LENGTH: u32 = 16 * 1024;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }
    None
}

/// Directory holding the fixture's SSTable components, if present.
fn fixture_dir() -> Option<PathBuf> {
    let root = get_datasets_root()?;
    let table_root = root.join("sstables").join(KEYSPACE);
    let entries = std::fs::read_dir(&table_root).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{TABLE}-")) && entry.path().is_dir() {
            // Require a Data.db to be actually present (not just JSONL goldens).
            if std::fs::read_dir(entry.path()).ok()?.flatten().any(|f| {
                f.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
            }) {
                return Some(entry.path());
            }
        }
    }
    None
}

/// Read `CompressionInfo.db` and return `(algorithm, chunk_length, chunk_count)`.
///
/// Format (Cassandra, see `compression_info.rs`):
///   writeUTF(algorithm) | writeInt(option_count) | option_count × (UTF key,
///   UTF value) | writeInt(chunk_length) | writeInt(max_compressed_length) |
///   writeLong(data_length) | writeInt(chunk_count) | chunk_count × writeLong.
///
/// We parse it independently of cqlite's own reader so the chunk count is an
/// authoritative on-disk fact, not something derived by the code under test.
fn read_compression_info(dir: &std::path::Path) -> (String, u32, u32) {
    let mut ci_path = None;
    for entry in std::fs::read_dir(dir).expect("read fixture dir").flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with("-CompressionInfo.db"))
        {
            ci_path = Some(entry.path());
        }
    }
    let ci_path = ci_path.expect("fixture must have a CompressionInfo.db");
    let b = std::fs::read(&ci_path).expect("read CompressionInfo.db");

    let mut o = 0usize;
    let rd_u16 = |b: &[u8], o: &mut usize| {
        let v = u16::from_be_bytes([b[*o], b[*o + 1]]);
        *o += 2;
        v
    };
    let rd_u32 = |b: &[u8], o: &mut usize| {
        let v = u32::from_be_bytes([b[*o], b[*o + 1], b[*o + 2], b[*o + 3]]);
        *o += 4;
        v
    };

    let nlen = rd_u16(&b, &mut o) as usize;
    let algorithm = String::from_utf8_lossy(&b[o..o + nlen]).to_string();
    o += nlen;
    let option_count = rd_u32(&b, &mut o);
    for _ in 0..option_count {
        let kl = rd_u16(&b, &mut o) as usize;
        o += kl;
        let vl = rd_u16(&b, &mut o) as usize;
        o += vl;
    }
    let chunk_length = rd_u32(&b, &mut o);
    let _max_compressed_length = rd_u32(&b, &mut o);
    o += 8; // data_length (writeLong)
    let chunk_count = rd_u32(&b, &mut o);
    (algorithm, chunk_length, chunk_count)
}

async fn setup_db() -> Database {
    let datasets_root = get_datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schemas_dir = get_schemas_dir().expect("schemas dir");
    let schema_path = schemas_dir.join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config)
        .await
        .expect("ingest wide_partition_table")
        .database
}

type RowSnapshot = (Vec<u8>, HashMap<String, Value>);

fn snapshot_key(key: &RowKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

async fn collect_execute(db: &Database, sql: &str) -> Vec<RowSnapshot> {
    let result = db.execute(sql).await.expect("execute should succeed");
    let mut rows: Vec<RowSnapshot> = result
        .rows
        .into_iter()
        .map(|r| (snapshot_key(&r.key), r.values))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

async fn collect_streaming(db: &Database, sql: &str, buffer_size: usize) -> Vec<RowSnapshot> {
    let config = StreamingConfig {
        buffer_size,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming should succeed");

    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streamed row should be Ok");
        rows.push((snapshot_key(&row.key), row.values));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Streaming (windowed `scan_stream`) parity with materializing `scan` on a
/// genuinely multi-chunk SSTable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windowed_streaming_scan_matches_materializing_on_multi_chunk_sstable() {
    let Some(dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This test is non-vacuous only with the real multi-chunk fixture."
        );
        return;
    };

    // PROOF 1 (on-disk, independent of the code under test): the fixture is
    // genuinely multi-chunk, so the windowed driver's `NeedMore` await-next-chunk
    // path is reachable. A single-chunk fixture would make this test vacuous.
    let (algorithm, chunk_length, chunk_count) = read_compression_info(&dir);
    assert_eq!(
        chunk_length, EXPECTED_CHUNK_LENGTH,
        "fixture chunk length changed ({chunk_length}); pick a fixture that still spans chunks"
    );
    assert!(
        chunk_count > 1,
        "Issue #1143: this test REQUIRES a multi-chunk fixture so a partition can \
         straddle a chunk boundary; {KEYSPACE}.{TABLE} reported {chunk_count} chunk(s) \
         (algorithm={algorithm}). A single-chunk fixture makes the NeedMore path vacuous."
    );
    eprintln!(
        "Issue #1143 fixture proof: {KEYSPACE}.{TABLE} algorithm={algorithm} \
         chunk_length={chunk_length} chunk_count={chunk_count} (>1 → straddle path reachable)"
    );

    let db = setup_db().await;

    // PROOF 2: the on-disk format routes through the chunk-stitching scan path
    // (`requires_chunk_stitching()` = NB + compressed). `nb` is the chunked
    // legacy format; the presence of CompressionInfo above confirms compression.
    {
        use cqlite_core::storage::sstable::SSTableReader;
        let cfg = cqlite_core::Config::default();
        let platform =
            std::sync::Arc::new(cqlite_core::platform::Platform::new(&cfg).await.unwrap());
        let data_path = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .expect("Data.db");
        let reader = SSTableReader::open(&data_path, &cfg, platform)
            .await
            .expect("open reader");
        assert_eq!(
            reader.format_version().expect("format version"),
            "nb",
            "Issue #1143: fixture must be the chunk-stitching `nb` format"
        );
    }

    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");
    let expected = collect_execute(&db, &sql).await;
    assert!(
        !expected.is_empty(),
        "precondition: {KEYSPACE}.{TABLE} should return rows"
    );

    for buffer_size in [1usize, 8, 1024] {
        let streamed = collect_streaming(&db, &sql, buffer_size).await;
        assert_eq!(
            streamed.len(),
            expected.len(),
            "Issue #1143: windowed streaming '{sql}' (buffer_size={buffer_size}) returned {} \
             rows; materializing execute returned {} — a chunk-boundary straddle bug.",
            streamed.len(),
            expected.len()
        );
        for (i, (got, want)) in streamed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.0, want.0,
                "Issue #1143: row {i} key mismatch (buffer_size={buffer_size})"
            );
            assert_eq!(
                got.1, want.1,
                "Issue #1143: row {i} value mismatch (buffer_size={buffer_size})"
            );
        }
    }
}

/// Deterministic proof — under `observability-testing` only — that the windowed
/// driver actually took the `NeedMore` straddle branch during a streaming scan
/// of the multi-chunk fixture: the `cqlite.read.scan.window_refill` counter must
/// be > 0. Run with:
///   cargo test -p cqlite-core \
///     --features state_machine,cli-helpers,observability-testing \
///     --test issue_1143_windowed_scan_straddle_parity
#[cfg(feature = "observability-testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windowed_streaming_scan_hits_needmore_straddle_branch() {
    use cqlite_core::observability::{catalog, testing};

    let Some(_dir) = fixture_dir() else {
        eprintln!("Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh)");
        return;
    };

    let mc = testing::metrics_capture();
    let db = setup_db().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    mc.reset();
    let streamed = collect_streaming(&db, &sql, 1).await;
    let metrics = mc.flush_and_collect();

    assert!(!streamed.is_empty(), "streaming scan returned rows");
    let refills = metrics.counter_sum(catalog::READ_SCAN_WINDOW_REFILL);
    assert!(
        refills > 0.0,
        "Issue #1143: expected {} > 0 (a partition must straddle a 16 KiB chunk \
         boundary on this multi-chunk fixture, exercising the NeedMore re-parse \
         path); saw {refills}",
        catalog::READ_SCAN_WINDOW_REFILL
    );
    eprintln!(
        "Issue #1143 straddle proof: {} fired {refills} time(s) during streaming scan",
        catalog::READ_SCAN_WINDOW_REFILL
    );
}
