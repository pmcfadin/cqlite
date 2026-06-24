//! Issue #953 (Epic #951) MEDIUM: the within-SSTable seek must bound its
//! DECOMPRESSION window to the target partition's extent — never stitch the
//! `Data.db` section to EOF.
//!
//! The multi-row #953 fix (`scan_single_partition` →
//! `bti_decompress_and_parse_target_all`) initially buffered EVERY chunk from the
//! target partition's chunk to EOF before parsing, as a workaround for a
//! mid-stream truncation bug (a row truncated at the buffer tail tripping a bogus
//! next-partition boundary). For a point lookup near the START of a large SSTable
//! that materializes nearly the whole file — turning a within-SSTable seek into
//! near full-table I/O. `partitions_decoded` stays 1 in that regime, so it does
//! NOT catch the blowup; this test adds the `chunks_decompressed` work counter and
//! asserts the seek's decompression is bounded.
//!
//! Fixture: `test_da.wide_table` (BTI `da`, LZ4) — 3 partitions (pk = 1/2/3) of
//! 300 rows × ~2 KiB payload each. Its `Data.db` decompresses to ~1.85 MiB across
//! **115** compression chunks (16 KiB each); each partition spans ~38 chunks.
//!
//!   - Looking up pk = 1 (the FIRST partition) must decompress only the chunks
//!     covering pk = 1 plus the few needed to detect pk = 2's header — roughly one
//!     partition's chunk span, NOT all 115. Under the old to-EOF stitch it
//!     decompressed ~all 115 (this test FAILS against that code).
//!   - Looking up pk = 3 (the LAST partition) legitimately reads to EOF (there is
//!     no following partition to bound it), so its chunk count is the natural
//!     upper reference. pk = 1 must be STRICTLY and SUBSTANTIALLY less.
//!
//! Correctness is co-asserted: pk = 1 still returns all 300 rows and
//! `partitions_decoded == 1` (no regression to break-after-first-row, no
//! over-read).
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; skipped (not failed) when
//! absent. Excluded under `tombstones` (that build compiles out the seek and the
//! `work_counters` mutators).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::Database;

/// Total compression chunks in the `test_da.wide_table` `Data.db` (read from its
/// `CompressionInfo.db`: 1,857,615-byte uncompressed section / 16 KiB chunks).
/// Under the buggy to-EOF stitch a pk = 1 lookup decompresses ~all of these.
const TOTAL_CHUNKS: u64 = 115;

/// Upper bound on chunks the pk = 1 seek may decompress.
///
/// Each of the 3 partitions spans ~38 of the 115 chunks (~620 KiB / 16 KiB). A
/// bounded pk = 1 lookup decompresses its own span plus the handful needed to see
/// pk = 2's first-row header — well under ~45 in practice. `60` gives generous
/// headroom over the observed count while staying FAR below `TOTAL_CHUNKS = 115`
/// (so a regression to the to-EOF stitch, which decompresses ~115, fails loudly)
/// AND below the pk = 3-to-EOF reference measured at runtime. The constant is a
/// fraction of the file, independent of where in the file the partition lives, so
/// it enforces the essential property: a head-of-file lookup does NOT read the
/// whole file.
const MAX_CHUNKS_HEAD_LOOKUP: u64 = 60;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("wide-table-bti.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_da/".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Run a `WHERE pk = <pk>` seek with the work counters reset, returning
/// `(row_count, chunks_decompressed, partitions_decoded)`.
async fn seek_pk(db: &Database, pk: i64) -> (usize, u64, u64) {
    work_counters::reset();
    let result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM test_da.wide_table WHERE pk = {pk}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seek pk={pk} failed: {e}"));
    (
        result.rows.len(),
        work_counters::chunks_decompressed(),
        work_counters::partitions_decoded(),
    )
}

#[tokio::test]
async fn head_of_file_seek_bounds_decompression_window() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping (BTI wide_table bound test): {e}");
            return;
        }
    };

    // ── pk = 3 (LAST partition): legitimately reads to EOF; the reference cost. ──
    let (rows3, chunks3, parts3) = seek_pk(&db, 3).await;
    if rows3 == 0 {
        // test_da/wide_table is a local fixture not present in the published CI
        // dataset; skip rather than fail when its Data.db is absent.
        eprintln!("Skipping (BTI wide_table bound test): 0 rows (Data.db not fetched)");
        return;
    }
    assert_eq!(rows3, 300, "pk=3 must return all 300 rows");
    assert_eq!(parts3, 1, "pk=3 decodes exactly one partition");

    // ── pk = 1 (FIRST partition): the head-of-file lookup under test. ────────────
    let (rows1, chunks1, parts1) = seek_pk(&db, 1).await;

    // Correctness co-assertions: the bound must not regress multi-row decode.
    assert_eq!(
        rows1, 300,
        "Issue #953: WHERE pk=1 must still return ALL 300 rows after bounding the \
         window (no break-after-first-row, no over-/under-read)"
    );
    assert_eq!(
        parts1, 1,
        "Issue #953: pk=1 decodes exactly one partition (partitions_decoded), got {parts1}"
    );

    println!(
        "Issue #953 bound: pk=1 decompressed {chunks1} chunks; pk=3 (to-EOF) decompressed \
         {chunks3} chunks; file total {TOTAL_CHUNKS} chunks."
    );

    // The crux: a head-of-file lookup must NOT read the whole file. Under the old
    // to-EOF stitch pk=1 decompresses ~all 115 chunks; the bound keeps it to ~one
    // partition's span.
    assert!(
        chunks1 <= MAX_CHUNKS_HEAD_LOOKUP,
        "Issue #953 MEDIUM: WHERE pk=1 (head of file) decompressed {chunks1} chunks, exceeding \
         the per-partition bound {MAX_CHUNKS_HEAD_LOOKUP}. A count near {TOTAL_CHUNKS} means the \
         seek stitched Data.db to EOF — turning a within-SSTable seek into near full-table I/O \
         (the regression this gate forbids)."
    );
    assert!(
        chunks1 < TOTAL_CHUNKS,
        "Issue #953: pk=1 ({chunks1} chunks) must decompress strictly fewer than the file's \
         {TOTAL_CHUNKS} chunks"
    );
    // pk=1 (head) must be substantially cheaper than pk=3 (which reads to EOF from
    // ~the file's last third). Equality would mean the head lookup also read the
    // tail.
    assert!(
        chunks1 < chunks3,
        "Issue #953: a head-of-file lookup (pk=1: {chunks1} chunks) must decompress strictly \
         fewer chunks than the last partition's to-EOF read (pk=3: {chunks3} chunks); equality \
         indicates pk=1 also read to EOF"
    );
    // Sanity: a single wide partition does span MANY chunks, so the seek is not
    // trivially reading one chunk — the bound is meaningful, not vacuous.
    assert!(
        chunks1 >= 2,
        "pk=1 spans multiple chunks (~620 KiB / 16 KiB), so the seek must decompress >= 2 \
         chunks; got {chunks1}"
    );
}
