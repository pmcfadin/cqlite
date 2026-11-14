//! #4237: a `TIMEUUID` column's query metadata carries its schema `CqlType`, so exports map it to
//! the Arrow UUID extension. The primitive-type parser used to list `time` before `timeuuid`, so
//! `TIMEUUID` failed to parse and the column silently degraded to `DataType::Text` /
//! `cql_type: None` (exported as Utf8 by Parquet and Vortex).

#![cfg(all(feature = "lz4", feature = "cli-helpers"))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::CqlType;
use cqlite_core::types::DataType;
use cqlite_core::Config;

#[path = "support/datasets_root.rs"]
mod datasets_root;

#[tokio::test]
async fn timeuuid_and_uuid_columns_carry_their_schema_types_in_query_metadata() {
    let root =
        datasets_root::sstables_root_for_table("test_basic", "simple_table").unwrap_or_else(|| {
            panic!(
                "fixture missing: {}",
                datasets_root::describe_search("test_basic", "simple_table")
            )
        });
    let db = ingest(IngestionConfig {
        schema_paths: vec![datasets_root::schema_path("basic-types.cql").expect("schema")],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .unwrap_or_else(|e| panic!("ingest failed: {e:?}"))
    .database;
    let result = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .unwrap_or_else(|e| panic!("SELECT failed: {e:?}"));
    let col = |name: &str| {
        result
            .metadata
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} missing from the metadata"))
    };
    assert_eq!(
        col("session_id").cql_type,
        Some(CqlType::TimeUuid),
        "timeuuid cql_type"
    );
    assert_eq!(
        col("session_id").data_type,
        DataType::Uuid,
        "timeuuid data_type"
    );
    assert_eq!(col("id").cql_type, Some(CqlType::Uuid), "uuid cql_type");
    assert_eq!(
        col("work_time").cql_type,
        Some(CqlType::Time),
        "`time` still parses as time"
    );
}
