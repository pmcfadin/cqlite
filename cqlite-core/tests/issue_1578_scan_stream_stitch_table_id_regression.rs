//! Issue #1578 (Epic D / D2), roborev Finding 2: prove the STITCHING (chunk-
//! spanning, compressed `nb`) branch of `run_scan_stream_windowed` no longer
//! filters by `table_ids_match` — reintroducing that filter must fail this test
//! loudly (a silent all-rows-dropped regression, not a subtle value diff).
//!
//! ## Why a dedicated corpus-backed test
//!
//! Every #1578 fixture (write-engine output, `issue_1578_*.rs`) is tiny and
//! UNCOMPRESSED, so it takes the PLAIN block-by-block `scan_stream` path
//! (`requires_chunk_stitching()` is `V5CompressedLegacy` + `nb` only) — which
//! still applies `table_ids_match` unchanged. Those fixtures can never exercise
//! the STITCHING branch this issue's fix touched, so they cannot catch a
//! reintroduced filter there.
//!
//! This test instead uses the real, local-only Cassandra 5.0
//! `test_big.wide_partition` fixture (114 LZ4 chunks over ~1.8 MB — genuinely
//! multi-chunk, confirmed by reading `CompressionInfo.db` directly, independent
//! of the code under test) and drives `SSTableReader::scan_stream` DIRECTLY
//! (bypassing the ingest/executor layer entirely) with a `table_id` that
//! deliberately DIVERGES from the reader's own header-derived table_id. Before
//! this fix, the stitching branch's `table_ids_match` guard silently dropped
//! EVERY row whose parsed entry table_id disagreed with the query — exactly
//! what happens for a CQLite-written `nb` SSTable, whose header keyspace/table
//! can diverge from the query. This test asserts the mismatched-table_id call
//! still returns the full row count (matching an independent oracle: the
//! ordinary `Database` path with the CORRECT table_id).
//!
//! ## Skip vs fail-closed (issue #1856 pattern)
//!
//! The fixture's `-Data.db`/`-CompressionInfo.db` binaries are local-only (not
//! in the fetchable dataset asset). Absent, this test SKIPS (honest eprintln),
//! never panics — EXCEPT under `CQLITE_PARITY_REQUIRE_DATASETS=1` (the required
//! parity gate), where absence is a fail-closed panic so that gate can never
//! green-pass without actually running this regression.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, TableId};
use tempfile::TempDir;

const REAL_FIXTURE_REL: &str = "sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294";
const SCHEMA_CQL: &str =
    "CREATE TABLE test_big.wide_partition (pk int, ck int, payload text, PRIMARY KEY (pk, ck));\n";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn real_fixture_data_db() -> Option<PathBuf> {
    let root = datasets_root()?;
    let data_db = root.join(REAL_FIXTURE_REL).join("nb-2-big-Data.db");
    data_db.exists().then_some(data_db)
}

/// CI fail-closed switch (issue #1856). Locally (env unset) an absent fixture
/// skips; the required parity gate sets this so absence hard-fails instead of
/// silently green-passing.
fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn skip_or_fail_closed(reason: &str) {
    if parity_datasets_required() {
        panic!(
            "stitched_scan_stream_ignores_table_id_mismatch: \
             CQLITE_PARITY_REQUIRE_DATASETS=1 but {reason} — required parity gate cannot \
             green-pass without running fail-closed (issue #1856)"
        );
    }
    eprintln!("Skipping (stitched scan_stream table_id regression): {reason}");
}

/// Read `CompressionInfo.db` (sibling of `data_db`) and return `chunk_count`,
/// independent of the code under test — an authoritative on-disk fact proving
/// this fixture is genuinely multi-chunk, so the STITCHING branch (not the
/// plain non-stitching path) is what `scan_stream` actually takes.
fn chunk_count(data_db: &std::path::Path) -> u32 {
    let info_path = data_db.with_file_name(
        data_db
            .file_name()
            .and_then(|n| n.to_str())
            .expect("Data.db file name")
            .replace("-Data.db", "-CompressionInfo.db"),
    );
    let b = std::fs::read(&info_path).expect("read CompressionInfo.db");
    let mut o = 0usize;
    let nlen = u16::from_be_bytes([b[o], b[o + 1]]) as usize;
    o += 2 + nlen;
    let option_count = u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
    o += 4;
    for _ in 0..option_count {
        let kl = u16::from_be_bytes([b[o], b[o + 1]]) as usize;
        o += 2 + kl;
        let vl = u16::from_be_bytes([b[o], b[o + 1]]) as usize;
        o += 2 + vl;
    }
    o += 4 + 4 + 8; // chunk_length + max_compressed_length + data_length
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

/// THE regression: `SSTableReader::scan_stream` driven with a table_id that
/// deliberately diverges from the reader's own header-derived id must still
/// return every row when the fixture takes the STITCHING branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stitched_scan_stream_ignores_table_id_mismatch() {
    let Some(data_db) = real_fixture_data_db() else {
        skip_or_fail_closed("test_big.wide_partition -Data.db binary not present");
        return;
    };

    // PROOF (on-disk, independent of the code under test): genuinely
    // multi-chunk, so this exercises the stitching branch, not the plain path.
    let chunks = chunk_count(&data_db);
    assert!(
        chunks > 1,
        "Issue #1578 Finding 2: test_big.wide_partition must be multi-chunk to \
         exercise the STITCHING branch (got {chunks} chunk(s)) — a single-chunk \
         fixture would make this regression test vacuous"
    );

    // Oracle: the REAL row count via the ordinary Database path (correct
    // table_id, ingested normally) — the authoritative full-table row count.
    let temp = TempDir::new().unwrap();
    let schema_path = temp.path().join("wide_partition.cql");
    std::fs::write(&schema_path, SCHEMA_CQL).unwrap();
    let root = datasets_root().expect("CQLITE_DATASETS_ROOT set (checked above)");
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("sstables"),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some("/test_big/".to_string()),
    })
    .await
    .expect("ingest real test_big.wide_partition");
    let oracle_rows = result
        .database
        .execute("SELECT pk, ck, payload FROM test_big.wide_partition")
        .await
        .expect("oracle full scan")
        .rows
        .len();
    assert!(oracle_rows > 0, "oracle full scan must return rows");

    // Open the reader DIRECTLY (bypassing ingest/executor) and confirm the
    // on-disk format is the chunk-stitching `nb` format.
    let cfg = Config::default();
    let platform = Arc::new(Platform::new(&cfg).await.unwrap());
    let reader = SSTableReader::open(&data_db, &cfg, platform)
        .await
        .expect("open reader");
    assert_eq!(
        reader.format_version().expect("format version"),
        "nb",
        "Issue #1578 Finding 2: fixture must be the chunk-stitching `nb` format"
    );

    // THE regression: drive `scan_stream` with a table_id that DELIBERATELY
    // diverges from the fixture's own header-derived id
    // ("test_big.wide_partition"). Before this fix, the stitching branch's
    // `table_ids_match` guard silently dropped every row here.
    let wrong_table_id = TableId::new("totally_different_ks.totally_different_table");
    let mut rx = Arc::new(reader).scan_stream(wrong_table_id, None, None, None, 64);

    let mut mismatched_rows = 0usize;
    while let Some(item) = rx.recv().await {
        item.expect("scan_stream row");
        mismatched_rows += 1;
    }

    assert_eq!(
        mismatched_rows, oracle_rows,
        "Issue #1578 Finding 2: scan_stream() with a MISMATCHED table_id returned \
         {mismatched_rows} rows, but the oracle (correct table_id) returned \
         {oracle_rows} — reintroducing `table_ids_match` on the stitching branch \
         would silently drop rows whenever the parsed entry table_id (header- \
         derived) diverges from the query, exactly the CQLite-written-nb-SSTable \
         bug this fix removed"
    );
    eprintln!(
        "Issue #1578 Finding 2: stitched scan_stream returned {mismatched_rows} rows \
         with a mismatched table_id (chunk_count={chunks}), matching the {oracle_rows}-row oracle"
    );
}
