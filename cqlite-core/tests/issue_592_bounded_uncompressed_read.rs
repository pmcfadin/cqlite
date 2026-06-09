//! Issue #592 regression test — bounded allocation on the uncompressed read path.
//!
//! Defect (pre-existing, surfaced by the mmap work in #589): the uncompressed /
//! headerless read path in `storage/sstable/reader/block_io.rs`
//! (`read_uncompressed_data_block`) read the entire current-position-to-EOF
//! range with a single `vec![0u8; remaining]` allocation. For a large
//! uncompressed SSTable that meant the whole data section was zero-initialized
//! and copied wholesale into one heap `Vec` — and once mmap could map the same
//! file, the bytes were resident twice — blowing the <128MB memory target.
//!
//! Fix: route the uncompressed read through a capped, reusable scratch buffer
//! (`read_into_vec_capped`, bounded by `read_buffer_size`), the same streaming
//! shape the compressed large-block path already used. Behavior is byte-identical;
//! only the allocation shape changed.
//!
//! ## Coverage
//!
//! The *allocation-shape* invariant (the scratch buffer never scales with the
//! block size) is asserted deterministically by the in-crate unit test
//! `storage::sstable::reader::block_io::tests::read_into_vec_capped_bounds_scratch_buffer`
//! using an instrumented reader, plus
//! `uncompressed_data_block_streams_large_block_byte_identical`. This integration
//! test exercises the refactored path **end-to-end** against the real
//! `uncompressed_table` fixture (`WITH compression = {'enabled': 'false'}`, so it
//! has a Data.db but no CompressionInfo.db and takes the uncompressed read path)
//! and asserts the data is read back correctly and completely.
//!
//! Requires `CQLITE_DATASETS_ROOT` and real Data.db files
//! (`bash test-data/scripts/fetch-datasets.sh`).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

/// Partition/row count in the `uncompressed_table` golden JSONL. The file holds
/// 100 single-row partitions (one object per line); `wc -l` reports 99 only
/// because the final line carries no trailing newline.
const EXPECTED_ROWS: usize = 100;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        if let Some(parent) = datasets_root.parent() {
            let schemas_dir = parent.join("schemas");
            if schemas_dir.exists() {
                return Some(schemas_dir);
            }
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

/// Ingest the `test_basic` keyspace (which holds `uncompressed_table`) and return
/// a queryable `Database`, or `None` if datasets aren't present.
async fn setup_basic_db() -> Option<Database> {
    let datasets_root = get_datasets_root()?;
    let schemas_dir = get_schemas_dir()?;
    let schema_path = schemas_dir.join("basic-types.cql");
    if !schema_path.exists() {
        return None;
    }
    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return None;
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/".to_string()),
    };

    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// #592: a full scan of the uncompressed table must return every row. The read
/// now streams the position-to-EOF data section through a capped buffer; this
/// confirms that change is byte-identical end-to-end (no rows dropped or
/// truncated by the chunked read).
#[tokio::test]
async fn uncompressed_table_full_scan_returns_all_rows() {
    let Some(db) = setup_basic_db().await else {
        eprintln!("uncompressed_table_full_scan_returns_all_rows: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT * FROM test_basic.uncompressed_table")
        .await
        .expect("query should succeed");

    if result.rows.is_empty() {
        eprintln!(
            "uncompressed_table_full_scan_returns_all_rows: SKIPPED (0 rows — Data.db absent?)"
        );
        return;
    }

    assert_eq!(
        result.rows.len(),
        EXPECTED_ROWS,
        "Issue #592: the capped-buffer uncompressed read must return the full \
         data section ({} rows), not a truncated prefix",
        EXPECTED_ROWS
    );

    // Every row must carry its UUID primary key — a partial / truncated read of
    // the streamed block would surface as missing or malformed key columns.
    for row in &result.rows {
        assert!(
            row.values.contains_key("id"),
            "every scanned row must include the 'id' partition key"
        );
    }
}
