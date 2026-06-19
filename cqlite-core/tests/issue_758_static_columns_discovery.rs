//! Issue #758 (Epic #756): static columns must be classified in *discovered*
//! schemas (no CQL file) using the authoritative Statistics.db SerializationHeader.
//!
//! The SerializationHeader distinguishes the static-column set from the regular
//! column set (definitive guide Ch.7 / Appendix B). Schema discovery must honor
//! that distinction instead of hardcoding `is_static: false`.
//!
//! Fixture: `test_basic.static_columns_table` — `static_data TEXT STATIC`.
//!
//! Requires the `state_machine` feature for schema discovery + query execution.

#![cfg(feature = "state_machine")]

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn static_columns_data_path() -> PathBuf {
    let datasets_root =
        std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT must be set");
    Path::new(&datasets_root).join(
        "sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    )
}

async fn open_reader(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    SSTableReader::open(path, &config, platform)
        .await
        .expect("Failed to open SSTable reader")
}

/// AC1: the discovered schema (no CQL file) marks `static_data` as `is_static: true`,
/// and the regular columns as `is_static: false`.
#[tokio::test]
async fn discovered_schema_classifies_static_columns() {
    let path = static_columns_data_path();
    let reader = open_reader(&path).await;

    let schema = reader
        .schema()
        .expect("Discovered schema should be available for static_columns_table");

    let static_data = schema
        .get_column("static_data")
        .expect("static_data column should be present in the discovered schema");
    assert!(
        static_data.is_static,
        "static_data must be classified as a STATIC column from the SerializationHeader, \
         got is_static=false (column metadata: {:?})",
        static_data
    );

    // Regular columns must remain non-static. Require their presence so a
    // discovered schema that silently drops a column fails the test instead of
    // skipping the assertion (roborev job 41).
    for name in ["row_data", "row_value"] {
        let col = schema.get_column(name).unwrap_or_else(|| {
            panic!("regular column {name} must be present in discovered schema")
        });
        assert!(
            !col.is_static,
            "regular column {} must not be classified static",
            name
        );
    }
}

/// AC2: query results for static columns via the *discovered* schema match the
/// classification produced by the *explicit* CQL schema (parity). When a CQL file
/// is provided it remains authoritative; the discovered schema must agree on the
/// static/regular split for the user-visible columns.
#[tokio::test]
async fn discovered_static_classification_matches_explicit_cql() {
    use cqlite_core::schema::cql_parser::parse_cql_schema;

    // Explicit CQL definition (authoritative) — mirrors test-data/schemas/basic-types.cql.
    let cql = r#"
        CREATE TABLE static_columns_table (
            partition_key UUID,
            clustering_key TIMESTAMP,
            static_data TEXT STATIC,
            row_data TEXT,
            row_value INT,
            PRIMARY KEY (partition_key, clustering_key)
        );
    "#;
    let explicit = parse_cql_schema(cql).expect("CQL schema should parse");

    let path = static_columns_data_path();
    let reader = open_reader(&path).await;
    let discovered = reader
        .schema()
        .expect("Discovered schema should be available for static_columns_table");

    // For every non-key column the explicit CQL knows about, the discovered
    // schema must both expose it and agree on the static classification.
    // Requiring presence (rather than skipping absent columns) ensures a
    // discovered schema that drops a regular/static column fails the test
    // (roborev job 41). Partition/clustering columns are tracked separately
    // from `columns` in the discovered schema, so they are excluded here.
    let key_names: std::collections::HashSet<&str> = explicit
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .chain(explicit.clustering_keys.iter().map(|k| k.name.as_str()))
        .collect();
    for col in &explicit.columns {
        if key_names.contains(col.name.as_str()) {
            continue;
        }
        let disc = discovered.get_column(&col.name).unwrap_or_else(|| {
            panic!(
                "discovered schema is missing column {} present in explicit CQL",
                col.name
            )
        });
        assert_eq!(
            disc.is_static, col.is_static,
            "is_static mismatch for column {}: discovered={}, explicit-cql={}",
            col.name, disc.is_static, col.is_static
        );
    }

    // And specifically the static column must be present and flagged in both.
    assert!(
        explicit
            .get_column("static_data")
            .map(|c| c.is_static)
            .unwrap_or(false),
        "explicit CQL schema should mark static_data static (test precondition)"
    );
    assert!(
        discovered
            .get_column("static_data")
            .map(|c| c.is_static)
            .unwrap_or(false),
        "discovered schema must mark static_data static to match explicit CQL"
    );
}

/// AC3: TableSchema::from_sstable_header preserves the header's static-column
/// classification (the authoritative-metadata plumbing point).
#[tokio::test]
async fn from_sstable_header_preserves_static_flag() {
    let path = static_columns_data_path();
    let reader = open_reader(&path).await;
    let header = reader.header();

    // Sanity: the header columns (populated from Statistics.db SerializationHeader)
    // must include a column flagged static.
    assert!(
        header.columns.iter().any(|c| c.is_static),
        "SSTable header columns should include at least one static column"
    );

    let schema =
        TableSchema::from_sstable_header(header).expect("from_sstable_header should succeed");
    let static_data = schema
        .get_column("static_data")
        .expect("static_data column should be present");
    assert!(
        static_data.is_static,
        "from_sstable_header must propagate is_static from the SerializationHeader"
    );
}
