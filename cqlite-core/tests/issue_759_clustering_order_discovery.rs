//! Issue #759 (Epic #756) regression test — clustering order (ASC/DESC)
//! discovery from the Statistics.db serialization header.
//!
//! Cassandra's serialization header encodes clustering-column types as
//! comparator class names. A DESC clustering column is wrapped in
//! `org.apache.cassandra.db.marshal.ReversedType(...)`. That wrapping is the
//! documented, authoritative signal for descending order (definitive guide
//! Ch.7 / Appendix B). Previously `TableSchema::from_sstable_header` defaulted
//! every clustering column to ASC.
//!
//! Fixture: `test_wide_rows.wide_partition_table` declares
//!   `CLUSTERING ORDER BY (clustering_col1 DESC, clustering_col2 ASC,
//!    clustering_col3 DESC, clustering_col4 ASC, clustering_col5 DESC)`
//! and its Statistics.db header wraps col1/col3/col5 in `ReversedType(...)`.
//!
//! Requires `CQLITE_DATASETS_ROOT` and real Data.db files
//! (`bash test-data/scripts/fetch-datasets.sh`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::ClusteringOrder;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Config;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate the Data.db component for the wide_partition_table fixture.
fn wide_partition_data_db(root: &Path) -> Option<PathBuf> {
    let table_dir = root
        .join("sstables")
        .join("test_wide_rows")
        .join("wide_partition_table-6d6d0f80a25111f0a3fef1a551383fb9");
    let data = table_dir.join("nb-1-big-Data.db");
    data.exists().then_some(data)
}

#[tokio::test]
async fn clustering_order_extracted_from_reversed_type() {
    let Some(root) = datasets_root() else {
        eprintln!("Skipping: CQLITE_DATASETS_ROOT not set");
        return;
    };
    let Some(data_db) = wide_partition_data_db(&root) else {
        eprintln!("Skipping: wide_partition_table Data.db not present (fetch-datasets.sh)");
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open sstable reader");

    let schema = reader
        .schema()
        .expect("schema should be discovered from header");

    // Sanity: 5 clustering columns in declared key order.
    assert_eq!(
        schema.clustering_keys.len(),
        5,
        "expected 5 clustering columns, got {:?}",
        schema
            .clustering_keys
            .iter()
            .map(|c| (&c.name, &c.order))
            .collect::<Vec<_>>()
    );

    // Declared: col1 DESC, col2 ASC, col3 DESC, col4 ASC, col5 DESC.
    // The header positions are canonical; assert order by position.
    let expected = [
        ClusteringOrder::Desc,
        ClusteringOrder::Asc,
        ClusteringOrder::Desc,
        ClusteringOrder::Asc,
        ClusteringOrder::Desc,
    ];
    for (idx, want) in expected.iter().enumerate() {
        let col = &schema.clustering_keys[idx];
        assert_eq!(
            col.order, *want,
            "clustering column #{} ({}) expected {:?}, got {:?}",
            idx, col.name, want, col.order
        );
    }

    // ReversedType unwrapping must NOT disturb inner-type deserialization:
    // the discovered data types are the inner CQL types, not "ReversedType".
    for col in &schema.clustering_keys {
        assert!(
            !col.data_type.to_lowercase().contains("reversed"),
            "clustering column {} leaked ReversedType into data_type: {}",
            col.name,
            col.data_type
        );
    }
}
