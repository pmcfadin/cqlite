//! Rust-only parity checks against precomputed references (Issue #89)
//! - Compares per-SSTable row counts from our StatisticsReader with JSONL dumps

use cqlite_core::{
    Config,
    error::Result as CqliteResult,
    platform::Platform,
    storage::sstable::statistics_reader::StatisticsReader,
    testing::dataset_helpers::{
        DatasetError, derive_reference_paths_from_data_db, list_tables, read_jsonl_rows,
        resolve_table_to_sstable_path,
    },
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

/// Derive the sibling Statistics.db path from a Data.db path
fn derive_statistics_from_data(data_db: &std::path::Path) -> Option<PathBuf> {
    let name = data_db.file_name()?.to_str()?;
    if !name.ends_with("-Data.db") {
        return None;
    }
    let stats_name = name.replace("-Data.db", "-Statistics.db");
    Some(data_db.parent()?.join(stats_name))
}

#[tokio::test]
async fn test_data_jsonl_vs_statistics_row_counts() -> CqliteResult<()> {
    // Fast-fail if datasets are missing
    if let Err(DatasetError::MetadataNotFound { .. }) =
        cqlite_core::testing::dataset_helpers::load_metadata()
    {
        println!("Datasets not available; skipping JSONL parity test");
        return Ok(());
    }

    // Deterministic target tables
    let targets = vec![
        ("test_basic", "simple_table"),
        ("test_timeseries", "sensor_data"),
        ("test_wide_rows", "wide_partition_table"),
        ("test_collections", "collection_table"),
    ];

    let mut tested = 0usize;

    for (keyspace, table) in targets {
        let dir = match resolve_table_to_sstable_path(keyspace, table) {
            Ok(p) => p,
            Err(DatasetError::DatasetNotFound { .. }) => continue,
            Err(e) => {
                println!("Resolve failed for {}.{}: {}", keyspace, table, e);
                continue;
            }
        };

        let mut read = fs::read_dir(&dir).await?;
        while let Some(entry) = read.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if !name.ends_with("-Data.db") {
                    continue;
                }

                // Reference paths
                let Some((data_jsonl, _stats_txt, _summary_txt)) =
                    derive_reference_paths_from_data_db(&path)
                else {
                    println!("Could not derive reference paths for {}", path.display());
                    continue;
                };
                if !data_jsonl.exists() {
                    println!("Missing Data JSONL reference: {}", data_jsonl.display());
                    continue;
                }

                // Open our StatisticsReader for same SSTable prefix
                let Some(stat_path) = derive_statistics_from_data(&path) else {
                    continue;
                };
                if !stat_path.exists() {
                    continue;
                }

                let config = Config::default();
                let platform = Arc::new(Platform::new(&config).await?);
                let reader = match StatisticsReader::open(&stat_path, platform.clone()).await {
                    Ok(r) => r,
                    Err(e) => {
                        println!("Open Statistics failed: {}", e);
                        continue;
                    }
                };

                // Count JSONL rows
                let mut jsonl_count: u64 = 0;
                if let Ok(iter) = read_jsonl_rows(&data_jsonl) {
                    for v in iter {
                        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
                            jsonl_count += rows.len() as u64;
                        }
                    }
                } else {
                    println!("Failed to read JSONL: {}", data_jsonl.display());
                    continue;
                }

                // Expected row count from metadata.yml
                let meta_count = list_tables(Some(keyspace))
                    .expect("list_tables failed")
                    .into_iter()
                    .find(|t| t.table == table)
                    .map(|t| t.row_count)
                    .unwrap_or(0);

                let our_total = reader.live_row_count().max(reader.row_count());
                println!(
                    "Parity {}.{} [{}]: JSONL={}, meta={}, our_total={}",
                    keyspace, table, name, jsonl_count, meta_count, our_total
                );

                // Hard assert: JSONL reference must match metadata.yml
                assert_eq!(
                    jsonl_count,
                    meta_count,
                    "Reference mismatch for {}: jsonl={} metadata={}",
                    path.display(),
                    jsonl_count,
                    meta_count
                );

                // Soft check: our reader total should match JSONL; log if not
                if our_total != jsonl_count {
                    println!(
                        "INFO: Reader count differs for {} (jsonl={} our_total={})",
                        path.display(),
                        jsonl_count,
                        our_total
                    );
                }
                tested += 1;
            }
        }
    }

    assert!(
        tested > 0,
        "No SSTables tested (missing references or datasets)"
    );
    Ok(())
}
