//! Issue #1327: the WriteEngine's `StatisticsWriter` must populate the
//! `estimatedPartitionSize` (and `estimatedCellPerPartitionCount`)
//! EstimatedHistogram so a write-produced Statistics.db reports the correct
//! authoritative `partition_count`.
//!
//! Ground truth is Apache Cassandra: a real Cassandra-written SSTable records
//! one observation per partition in the leading `estimatedPartitionSize`
//! histogram, so `Σ bucket counts == partition_count`. That is exactly what the
//! read-side authoritative decode (`read_table_counts`, issue #944) sums.
//!
//! Before this fix `StatisticsWriter::write_estimated_histogram` emitted a fixed
//! EMPTY histogram (2 buckets, count 0), so `read_table_counts` returned
//! `partition_count == 0` for every write-engine-produced SSTable.
//!
//! ## Wiring-evidence
//!
//! The assertion drives the full public write -> read surface:
//! `SSTableWriter` writes an SSTable (BIG `nb` and BTI `da`), then the raw
//! Statistics.db bytes are decoded with the AUTHORITATIVE version gates parsed
//! from the on-disk descriptor via `VersionGates::from_path`. No synthetic
//! buffers, no heuristics.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::Path;

use cqlite_core::parser::repair_metadata::read_table_counts;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::version_gate::VersionGates;
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableInfo, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

fn simple_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "simple".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn live_row(pk: i32, payload: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "simple"),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload.to_string()),
        }],
        ts,
        None,
    )
}

/// Write `n` distinct single-row partitions into a fresh SSTable of `format`.
async fn write_n_partitions(dir: &Path, n: i32, format: SSTableFormat) -> SSTableInfo {
    let schema = simple_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), 1, &schema, 16, format).unwrap();

    // Partitions must be written in ascending token order, so collect + sort by
    // the decorated key's token first.
    let mut partitions: Vec<(_, Vec<Mutation>)> = (0..n)
        .map(|pk| {
            let m = live_row(pk, &format!("payload-{pk}"), 1_000_000 + pk as i64);
            let key = m.decorated_key(&schema).unwrap();
            (key, vec![m])
        })
        .collect();
    partitions.sort_by(|a, b| a.0.token.cmp(&b.0.token));

    for (key, muts) in partitions {
        writer.write_partition(key, muts).unwrap();
    }
    writer.finish().await.unwrap()
}

/// Decode the write-produced Statistics.db with authoritative gates parsed from
/// the on-disk Data.db descriptor and assert the partition count matches.
fn assert_partition_count(info: &SSTableInfo, expected: u64) {
    let stats_bytes = std::fs::read(&info.stats_path).expect("read Statistics.db");
    let gates = VersionGates::from_path(&info.data_path).expect("gates from descriptor");
    let counts = read_table_counts(&stats_bytes, Some(&gates)).expect("decode table counts");
    assert_eq!(
        counts.partition_count, expected,
        "Σ estimatedPartitionSize histogram bucket counts must equal the number of \
         partitions actually written ({expected}); got {} — the write-engine \
         Statistics.db EstimatedHistogram is populated (issue #1327)",
        counts.partition_count
    );
}

#[tokio::test]
async fn big_nb_write_produces_authoritative_partition_count() {
    let dir = TempDir::new().unwrap();
    let n = 7;
    let info = write_n_partitions(dir.path(), n, SSTableFormat::Big).await;
    assert_partition_count(&info, n as u64);
}

#[tokio::test]
async fn bti_da_write_produces_authoritative_partition_count() {
    let dir = TempDir::new().unwrap();
    let n = 5;
    let info = write_n_partitions(dir.path(), n, SSTableFormat::Bti).await;
    assert_partition_count(&info, n as u64);
}

#[tokio::test]
async fn single_partition_write_produces_partition_count_one() {
    let dir = TempDir::new().unwrap();
    let info = write_n_partitions(dir.path(), 1, SSTableFormat::Big).await;
    assert_partition_count(&info, 1);
}
