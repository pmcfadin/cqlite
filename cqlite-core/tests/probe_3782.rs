//! TEMPORARY measurement probe for issue #3782. Not for merge as-is.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

fn datasets_root() -> PathBuf {
    PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT"))
}

fn schemas_dir() -> PathBuf {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.parent().unwrap().join("test-data").join("schemas")
}

async fn setup(keyspace: &str, schema_file: &str, data_dir: PathBuf) -> Database {
    let config = IngestionConfig {
        schema_paths: vec![schemas_dir().join(schema_file)],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    ingest(config).await.expect("ingest").database
}

/// Q4: how many mid-stream (`at_final_chunk == false`) `None`s does a broad read
/// of WELL-FORMED corpus tables produce?
#[tokio::test]
async fn q4_wellformed_corpus_broad_read() {
    let root = datasets_root().join("sstables");
    let cases: &[(&str, &str, &[&str])] = &[
        ("test_basic", "basic-types.cql", &["simple_table", "composite_key_table", "multi_partition_table", "uncompressed_table", "static_columns_table", "compression_test_table", "ttl_test_table"]),
        ("test_wide_rows", "wide-rows.cql", &["wide_partition_table", "many_columns_table", "large_blob_table", "chat_messages", "document_versions", "product_catalog", "sparse_data_table", "multi_metric_timeseries"]),
        ("test_timeseries", "time-series.cql", &["sensor_data", "app_metrics", "user_activity", "stock_prices", "log_entries", "event_store", "user_sessions", "tick_data", "time_bucketed_counters"]),
        ("test_collections", "collections.cql", &["list_table", "set_table", "map_table", "nested_collections", "frozen_collections", "tuple_table", "udt_table", "complex_nested_table"]),
        ("test_comp", "compression-parity.cql", &["lz4_table", "snappy_table", "deflate_table", "zstd_table", "uncompressed_table", "short_final_chunk"]),
        ("test_da", "da-test.cql", &["simple_table", "collection_table", "ttl_table"]),
        ("test_big", "wide-table-bti.cql", &["wide_partition"]),
    ];
    cqlite_core::probe3782::reset();
    let mut total_rows = 0usize;
    let mut read = 0usize;
    for (ks, schema, tables) in cases {
        if !root.join(ks).exists() { eprintln!("PROBE3782 SKIP keyspace {ks}"); continue; }
        let db = setup(ks, schema, root.clone()).await;
        for t in *tables {
            match db.execute(&format!("SELECT * FROM {ks}.{t}")).await {
                Ok(r) => { total_rows += r.rows.len(); read += 1; eprintln!("PROBE3782 read {ks}.{t} rows={}", r.rows.len()); }
                Err(e) => eprintln!("PROBE3782 ERR {ks}.{t}: {e}"),
            }
        }
    }
    eprintln!("PROBE3782 total tables read={read} total_rows={total_rows}");
    assert!(total_rows > 0, "0-rows-when-present guard");
    cqlite_core::probe3782::dump("q4-wellformed");
}
