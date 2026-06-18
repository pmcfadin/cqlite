//! Compaction-merge → Arrow record batch producer.
//!
//! Drives `cqlite_core`'s k-way compaction merge over a set of SSTables, leaving
//! the inputs untouched, and converts the merged rows into Arrow [`RecordBatch`]es
//! using the shared `cqlite_core::export::arrow_convert` conversion.
//!
//! Each merged row is reconstructed into a `QueryRow` via the read path's
//! `build_row_from_scan`, so the Flight output is byte-for-byte the same shape as
//! a `SELECT` over the same data — partition-key columns decoded from the row key,
//! clustering and regular columns taken from the decoded cells, row/cell
//! tombstones suppressed.
//!
//! Phase 1 collects all batches in memory; this matches the merge engine, which
//! already drains every input SSTable into memory (see `merge.rs` issue #591).
//! True streaming is a later optimization.

use std::path::{Path, PathBuf};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use cqlite_core::export::{build_arrow_schema, rows_to_record_batch, ArrowConvertError};
use cqlite_core::query::{build_row_from_scan, ColumnInfo, QueryRow};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::storage::write_engine::merge::{MergeStep, RowData};
use cqlite_core::storage::write_engine::KWayMerger;
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

/// Errors produced while merging SSTables into Arrow batches.
#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    /// A column's CQL type string could not be parsed.
    #[error("invalid CQL type for column '{column}': {source}")]
    InvalidColumnType {
        /// Column whose type failed to parse.
        column: String,
        /// Underlying parse error.
        source: cqlite_core::Error,
    },
    /// The k-way merge engine failed.
    #[error("compaction merge failed: {0}")]
    Merge(cqlite_core::Error),
    /// CQL → Arrow conversion failed.
    #[error(transparent)]
    Convert(#[from] ArrowConvertError),
    /// Listing SSTable files failed.
    #[error("failed to list SSTables in {path}: {source}")]
    Discovery {
        /// Directory that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}

/// Source of the SSTable `Data.db` files to merge for one table.
///
/// Abstracted as a trait so the producer can be tested against a fixed file list
/// and so Phase 3 can swap in a snapshot-directory source without touching the
/// merge logic (Dependency Inversion).
pub trait SstableSource {
    /// Return the `Data.db` paths to merge, newest generation first.
    fn data_paths(&self) -> Result<Vec<PathBuf>, ProducerError>;
}

/// Lists `*-Data.db` files directly under a table directory.
pub struct DirSource {
    /// Directory holding the table's SSTable components.
    pub dir: PathBuf,
}

impl DirSource {
    /// Create a source over `dir` (e.g. `<data>/<keyspace>/<table>`).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

impl SstableSource for DirSource {
    fn data_paths(&self) -> Result<Vec<PathBuf>, ProducerError> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .map_err(|source| ProducerError::Discovery {
                path: self.dir.clone(),
                source,
            })?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .collect();
        // Newest generation first. The merger reconciles by per-row timestamp;
        // generation order only breaks exact-timestamp ties, but a deterministic
        // ordering keeps results stable across runs.
        paths.sort_by_key(|p| std::cmp::Reverse(generation_of(p)));
        Ok(paths)
    }
}

/// Best-effort parse of the generation number from a Cassandra SSTable file name
/// such as `nb-12-big-Data.db` → `12`. Returns 0 when not parseable.
fn generation_of(path: &Path) -> u64 {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|name| name.split('-').find_map(|seg| seg.parse::<u64>().ok()))
        .unwrap_or(0)
}

/// Produces Arrow record batches from a compaction merge of a table's SSTables.
pub struct MergeProducer {
    schema: TableSchema,
    columns: Vec<ColumnInfo>,
    batch_size: usize,
}

impl MergeProducer {
    /// Build a producer for `schema`, emitting record batches of at most
    /// `batch_size` rows.
    pub fn new(schema: TableSchema, batch_size: usize) -> Result<Self, ProducerError> {
        let columns = schema_columns(&schema)?;
        Ok(Self {
            schema,
            columns,
            batch_size: batch_size.max(1),
        })
    }

    /// The Arrow schema clients should expect (for `GetFlightInfo`/`GetSchema`).
    pub fn arrow_schema(&self) -> Result<ArrowSchema, ProducerError> {
        Ok(build_arrow_schema(&self.columns)?)
    }

    /// The ordered Arrow column metadata.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Merge `source`'s SSTables and return the resulting Arrow batches.
    pub fn produce(
        &self,
        source: &dyn SstableSource,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let paths = source.data_paths()?;
        self.produce_from_paths(paths)
    }

    /// Merge the given SSTable `Data.db` paths and return Arrow batches.
    pub fn produce_from_paths(
        &self,
        paths: Vec<PathBuf>,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let mut batches = Vec::new();
        if paths.is_empty() {
            return Ok(batches);
        }

        let mut merger =
            KWayMerger::new(paths, &self.schema).map_err(ProducerError::Merge)?;
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);

        while let MergeStep::Partition { key, rows } =
            merger.step().map_err(ProducerError::Merge)?
        {
            for entry in rows {
                if let Some(row) = self.entry_to_row(&key.key, entry.row_data) {
                    buffer.push(row);
                    if buffer.len() >= self.batch_size {
                        batches.push(self.flush_buffer(&mut buffer)?);
                    }
                }
            }
        }

        if !buffer.is_empty() {
            batches.push(self.flush_buffer(&mut buffer)?);
        }
        Ok(batches)
    }

    /// Reconstruct one logical row from a merged entry, or `None` for a row
    /// tombstone. Cell tombstones are dropped so the column reads as null.
    fn entry_to_row(&self, partition_key: &[u8], row_data: RowData) -> Option<QueryRow> {
        let cells = match row_data {
            RowData::Live { cells } => cells,
            // Whole-row deletion: suppress from output.
            RowData::Tombstone { .. } => return None,
        };

        let map_entries: Vec<(Value, Value)> = cells
            .into_iter()
            // A cell tombstone leaves the column absent → null in Arrow output,
            // matching the CLI's "emit null for tombstoned cells" behaviour.
            .filter(|c| !matches!(c.value, Value::Tombstone(_)))
            .map(|c| (Value::Text(c.column), c.value))
            .collect();

        let key = RowKey(partition_key.to_vec());
        build_row_from_scan(key, Value::Map(map_entries), &[], Some(&self.schema))
    }

    fn flush_buffer(&self, buffer: &mut Vec<QueryRow>) -> Result<RecordBatch, ProducerError> {
        let batch = rows_to_record_batch(&self.columns, buffer)?;
        buffer.clear();
        Ok(batch)
    }
}

/// Build ordered, de-duplicated Arrow column metadata from a table schema.
///
/// Column order is partition keys, then clustering keys, then the remaining
/// regular columns — a stable, key-first order for the downstream SQL engine.
/// Every column carries its authoritative `CqlType` (no heuristics, issue #28).
pub fn schema_columns(schema: &TableSchema) -> Result<Vec<ColumnInfo>, ProducerError> {
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::new();

    let mut push = |name: &str, type_str: &str| -> Result<(), ProducerError> {
        if !seen.insert(name.to_string()) {
            return Ok(());
        }
        let cql_type =
            CqlType::parse(type_str).map_err(|source| ProducerError::InvalidColumnType {
                column: name.to_string(),
                source,
            })?;
        columns.push(ColumnInfo {
            name: name.to_string(),
            data_type: flat_data_type(&cql_type),
            nullable: true,
            position: columns.len(),
            table_name: Some(schema.table.clone()),
            cql_type: Some(cql_type),
        });
        Ok(())
    };

    for k in &schema.partition_keys {
        push(&k.name, &k.data_type)?;
    }
    for c in &schema.clustering_keys {
        push(&c.name, &c.data_type)?;
    }
    for col in &schema.columns {
        push(&col.name, &col.data_type)?;
    }
    Ok(columns)
}

/// Map a `CqlType` to the flat `DataType` fallback carried by `ColumnInfo`.
///
/// The Arrow converter prefers `ColumnInfo.cql_type` (always `Some` here), so this
/// is only a structural placeholder; types without a flat equivalent (date, time,
/// decimal, varint, duration, inet, counter) fall back to `Text` and are never
/// actually used for conversion.
fn flat_data_type(cql: &CqlType) -> DataType {
    match cql {
        CqlType::Boolean => DataType::Boolean,
        CqlType::TinyInt => DataType::TinyInt,
        CqlType::SmallInt => DataType::SmallInt,
        CqlType::Int => DataType::Integer,
        CqlType::BigInt | CqlType::Counter => DataType::BigInt,
        CqlType::Float => DataType::Float32,
        CqlType::Double => DataType::Float,
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => DataType::Text,
        CqlType::Blob => DataType::Blob,
        CqlType::Timestamp => DataType::Timestamp,
        CqlType::Uuid | CqlType::TimeUuid => DataType::Uuid,
        CqlType::List(_) => DataType::List,
        CqlType::Set(_) => DataType::Set,
        CqlType::Map(_, _) => DataType::Map,
        CqlType::Tuple(_) => DataType::Tuple,
        CqlType::Udt(_, _) => DataType::Udt,
        CqlType::Frozen(_) => DataType::Frozen,
        // No flat equivalent — cql_type drives conversion, so Text is unused.
        CqlType::Decimal
        | CqlType::Date
        | CqlType::Time
        | CqlType::Inet
        | CqlType::Duration
        | CqlType::Varint
        | CqlType::Custom(_) => DataType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{build_sstables, delete_row, simple_schema, total_rows, write_row};
    use cqlite_core::schema::{ClusteringColumn, Column};

    #[test]
    fn schema_columns_orders_pk_then_clustering_then_regular() {
        let mut schema = simple_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "text".into(),
            position: 0,
            order: Default::default(),
        }];
        schema.columns.insert(
            1,
            Column {
                name: "ck".into(),
                data_type: "text".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
        );
        let cols = schema_columns(&schema).unwrap();
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "ck", "name", "score"]);
        // Every column carries its authoritative CQL type.
        assert!(cols.iter().all(|c| c.cql_type.is_some()));
    }

    #[test]
    fn produces_all_rows_from_single_sstable() {
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 5);
        // Arrow schema has the 3 declared columns in key-first order.
        let arrow_schema = producer.arrow_schema().unwrap();
        let field_names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(field_names, vec!["id", "name", "score"]);
    }

    #[test]
    fn merge_resolves_last_write_wins_across_sstables() {
        let schema = simple_schema();
        // SSTable A: id=1 name="old" ts=100. SSTable B: id=1 name="new" ts=200.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![write_row(1, "old", 1, 100)], vec![write_row(1, "new", 2, 200)]],
        );

        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 1, "one partition after merge");

        let batch = &batches[0];
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "new", "newer timestamp wins");
    }

    #[test]
    fn row_tombstones_are_suppressed() {
        let schema = simple_schema();
        // A writes ids 1,2,3; B deletes id=2 with a newer timestamp.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100), write_row(3, "c", 3, 100)],
                vec![delete_row(2, 200)],
            ],
        );

        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 2, "deleted row 2 is gone");
    }

    #[test]
    fn batch_size_splits_output() {
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let producer = MergeProducer::new(schema, 4).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 10);
        assert!(batches.len() >= 3, "10 rows / batch_size 4 → ≥3 batches");
        assert!(batches.iter().all(|b| b.num_rows() <= 4));
    }

    #[test]
    fn empty_source_yields_no_batches() {
        let schema = simple_schema();
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce_from_paths(vec![]).unwrap();
        assert!(batches.is_empty());
    }
}
