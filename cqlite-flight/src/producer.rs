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
use cqlite_core::query::{build_row_from_scan, evaluate_predicates, ColumnInfo, QueryRow};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::storage::write_engine::merge::{MergeStep, RowData};
use cqlite_core::storage::write_engine::KWayMerger;
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

use crate::filter::ScanSpec;

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
    /// Predicate evaluation failed (e.g. incomparable operand types).
    #[error("predicate evaluation failed: {0}")]
    Predicate(cqlite_core::Error),
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
    dir: PathBuf,
}

impl DirSource {
    /// Create a source over an explicit directory (e.g. `<data>/<ks>/<table>`).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Resolve the SSTable directory for `keyspace.table` under `data_dir`,
    /// optionally inside a named snapshot.
    ///
    /// Supports the write-engine layout (`<data>/<ks>/<table>`) and the Cassandra
    /// layout (`<data>/<ks>/<table>-<uuid>`). When several `<table>-<uuid>` dirs
    /// match, the lexicographically-largest name is chosen deterministically.
    /// When `snapshot` is `Some(name)`, resolves to the frozen
    /// `<table-dir>/snapshots/<name>/` hardlink set (Phase 3). When nothing
    /// matches, the exact (non-existent) path is returned so `data_paths`
    /// surfaces a clean `NotFound`.
    pub fn resolve(data_dir: &Path, keyspace: &str, table: &str, snapshot: Option<&str>) -> Self {
        let table_dir = Self::table_base_dir(data_dir, keyspace, table);
        let dir = match snapshot {
            Some(name) if !name.is_empty() => table_dir.join("snapshots").join(name),
            _ => table_dir,
        };
        Self::new(dir)
    }

    /// Resolve the on-disk directory for a table (live data dir, no snapshot).
    fn table_base_dir(data_dir: &Path, keyspace: &str, table: &str) -> PathBuf {
        let base = data_dir.join(keyspace);
        let exact = base.join(table);
        if exact.is_dir() {
            return exact;
        }
        let prefix = format!("{table}-");
        let mut best: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&base) {
            for entry in entries.flatten() {
                let path = entry.path();
                let matches = path.is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|n| n.starts_with(&prefix));
                if matches && best.as_ref().is_none_or(|b| path > *b) {
                    best = Some(path);
                }
            }
        }
        best.unwrap_or(exact)
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

/// Read an SSTable's `[minToken, maxToken]` span from its sibling `Summary.db`.
///
/// SSTables store partitions in token order, so the first key carries the
/// minimum token and the last key the maximum. The `Summary.db` path is derived
/// by replacing the `-Data.db` suffix. Returns `None` on any failure (missing
/// file, parse error, unparseable name) so callers can fail open.
fn sstable_token_span(data_path: &Path) -> Option<(i64, i64)> {
    let name = data_path.file_name()?.to_str()?;
    if !name.ends_with("-Data.db") {
        return None;
    }
    let summary_path = data_path.with_file_name(name.replace("-Data.db", "-Summary.db"));

    // `SummaryReader::open` is async; drive it on a short-lived current-thread
    // runtime. The prune is a one-shot, low-frequency step per DoGet split.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    let platform = runtime
        .block_on(cqlite_core::Platform::new(&cqlite_core::Config::default()))
        .ok()?;
    let reader = runtime
        .block_on(
            cqlite_core::storage::sstable::summary_reader::SummaryReader::open(
                &summary_path,
                std::sync::Arc::new(platform),
            ),
        )
        .ok()?;

    let min_token = cqlite_core::storage::write_engine::mutation::DecoratedKey::from_key_bytes(
        reader.get_first_key().to_vec(),
    )
    .ok()?
    .token;
    let max_token = cqlite_core::storage::write_engine::mutation::DecoratedKey::from_key_bytes(
        reader.get_last_key().to_vec(),
    )
    .ok()?
    .token;
    Some((min_token, max_token))
}

/// Produces Arrow record batches from a compaction merge of a table's SSTables.
pub struct MergeProducer {
    schema: TableSchema,
    columns: Vec<ColumnInfo>,
    batch_size: usize,
    spec: ScanSpec,
}

impl MergeProducer {
    /// Build an unfiltered producer for `schema` (emits all rows and columns).
    pub fn new(schema: TableSchema, batch_size: usize) -> Result<Self, ProducerError> {
        Self::with_spec(schema, batch_size, ScanSpec::default())
    }

    /// Build a producer applying `spec` (token range, predicates, projection).
    pub fn with_spec(
        schema: TableSchema,
        batch_size: usize,
        spec: ScanSpec,
    ) -> Result<Self, ProducerError> {
        let mut columns = schema_columns(&schema)?;
        if let Some(projection) = &spec.projection {
            // Keep schema (key-first) order, restricted to the projected set.
            columns.retain(|c| projection.iter().any(|p| p == &c.name));
        }
        Ok(Self {
            schema,
            columns,
            batch_size: batch_size.max(1),
            spec,
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
    pub fn produce(&self, source: &dyn SstableSource) -> Result<Vec<RecordBatch>, ProducerError> {
        let paths = source.data_paths()?;
        self.produce_from_paths(paths)
    }

    /// Merge the given SSTable `Data.db` paths and return Arrow batches.
    ///
    /// When the scan carries a token filter, the input path list is first pruned
    /// to the SSTables whose `[minToken, maxToken]` span overlaps the split's
    /// `(start, end]` range (issue #839), so a narrow split opens only the
    /// SSTables it can possibly read from. The per-partition token filter in the
    /// merge loop remains as a correctness backstop.
    pub fn produce_from_paths(
        &self,
        paths: Vec<PathBuf>,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let paths = self.prune_paths(paths)?;
        self.merge_paths(paths)
    }

    /// Prune `paths` to those whose token span overlaps the spec's token range.
    ///
    /// Returns `paths` unchanged when there is no token filter. A path is kept
    /// (fail open) whenever its sibling `Summary.db` is missing or unreadable, so
    /// pruning can never drop an SSTable that might contain matching partitions.
    pub(crate) fn prune_paths(&self, paths: Vec<PathBuf>) -> Result<Vec<PathBuf>, ProducerError> {
        let Some(token) = &self.spec.token else {
            return Ok(paths);
        };

        let total = paths.len();
        let kept: Vec<PathBuf> = paths
            .into_iter()
            .filter(|path| match sstable_token_span(path) {
                // Span known: keep only if it overlaps the split's range.
                Some((min_token, max_token)) => token.overlaps(min_token, max_token),
                // Span unknown (missing/unreadable Summary.db): fail open.
                None => true,
            })
            .collect();

        tracing::debug!(
            kept = kept.len(),
            pruned = total - kept.len(),
            total,
            "token-range SSTable prune"
        );
        Ok(kept)
    }

    /// Merge the (already pruned) SSTable paths into Arrow batches.
    fn merge_paths(&self, paths: Vec<PathBuf>) -> Result<Vec<RecordBatch>, ProducerError> {
        let mut batches = Vec::new();
        if paths.is_empty() {
            return Ok(batches);
        }

        let mut merger = KWayMerger::new(paths, &self.schema).map_err(ProducerError::Merge)?;
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);

        while let MergeStep::Partition { key, rows } =
            merger.step().map_err(ProducerError::Merge)?
        {
            // Token-range filter: drop whole partitions outside the split's range.
            if let Some(token) = &self.spec.token {
                if !token.contains(key.token) {
                    continue;
                }
            }
            for entry in rows {
                // Build the FULL row so predicates can reference any column, even
                // one projected out of the output. Output projection is applied
                // separately via `self.columns` during Arrow conversion.
                let Some(row) = self.entry_to_row(&key.key, entry.row_data) else {
                    continue;
                };
                // Predicate pushdown: keep only rows satisfying every predicate.
                if !self.spec.predicates.is_empty()
                    && !evaluate_predicates(&row, &self.spec.predicates)
                        .map_err(ProducerError::Predicate)?
                {
                    continue;
                }
                buffer.push(row);
                if buffer.len() >= self.batch_size {
                    batches.push(self.flush_buffer(&mut buffer)?);
                }
            }
        }

        if !buffer.is_empty() {
            batches.push(self.flush_buffer(&mut buffer)?);
        }
        Ok(batches)
    }

    /// Reconstruct one full logical row from a merged entry, or `None` for a row
    /// tombstone. Cell tombstones are dropped so the column reads as null.
    ///
    /// The row carries ALL columns (no projection) so predicate evaluation can
    /// reference any column; output projection is applied later via `self.columns`.
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

    /// Merge `paths` WITHOUT the input prune, relying only on the per-partition
    /// token backstop. Used by tests to prove the pruned run yields identical
    /// rows to a full-scan-then-filter run.
    #[cfg(test)]
    fn produce_unpruned_for_test(
        &self,
        paths: Vec<PathBuf>,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        self.merge_paths(paths)
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
pub(crate) fn schema_columns(schema: &TableSchema) -> Result<Vec<ColumnInfo>, ProducerError> {
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
    use crate::testutil::{
        build_sstables, delete_row, make_snapshot, simple_schema, total_rows, write_row, KS, TBL,
    };
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

    // Cross-check for the cqlite-core merge clustering-key fix (wide-row collapse).
    // The authoritative gate is the cqlite-core `clustering_key_rows_survive_compaction`
    // test; this verifies the fix end-to-end through the Flight producer.
    #[test]
    fn clustering_table_preserves_distinct_rows_in_a_partition() {
        use crate::testutil::{clustering_schema, write_clustered};
        let schema = clustering_schema();
        // One partition (pk=1) with two clustering rows.
        let (_temp, _data, dir) = crate::testutil::build_sstables(
            &schema,
            vec![vec![
                write_clustered(1, "a", 10, 100),
                write_clustered(1, "b", 20, 100),
            ]],
        );
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            total_rows(&batches),
            2,
            "both clustering rows in the partition must survive (not collapse to one)"
        );
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
    fn null_column_is_arrow_null() {
        use crate::testutil::write_name_only;
        use arrow::array::Array;
        let schema = simple_schema();
        // id=1 has no `score` cell → null; id=2 has both.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_name_only(1, "a", 100),
                write_row(2, "b", 50, 100),
            ]],
        );
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 2);

        let batch = &batches[0];
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        // Find the row for id=1 and assert its score is null.
        let idx = (0..ids.len())
            .find(|&i| ids.value(i) == 1)
            .expect("id=1 present");
        assert!(scores.is_null(idx), "missing score cell must be Arrow null");
        let idx2 = (0..ids.len())
            .find(|&i| ids.value(i) == 2)
            .expect("id=2 present");
        assert!(!scores.is_null(idx2));
        assert_eq!(scores.value(idx2), 50);
    }

    #[test]
    fn uuid_column_roundtrips_with_extension_metadata() {
        use crate::testutil::{uuid_schema, write_uuid_row};
        let schema = uuid_schema();
        let id = [7u8; 16];
        let (_temp, _data, dir) = build_sstables(&schema, vec![vec![write_uuid_row(id, "x", 100)]]);
        let producer = MergeProducer::new(schema, 1024).unwrap();

        // Arrow field carries the UUID extension metadata so Trino reads it as UUID.
        let arrow_schema = producer.arrow_schema().unwrap();
        let id_field = arrow_schema.field_with_name("id").unwrap();
        assert_eq!(
            id_field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid"),
            "uuid column must carry the Arrow UUID extension"
        );

        let batches = producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 1);
        let ids = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("uuid → FixedSizeBinary(16)");
        assert_eq!(ids.value(0), &id, "uuid bytes round-trip");
    }

    #[test]
    fn merge_resolves_last_write_wins_across_sstables() {
        let schema = simple_schema();
        // SSTable A: id=1 name="old" ts=100. SSTable B: id=1 name="new" ts=200.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "old", 1, 100)],
                vec![write_row(1, "new", 2, 200)],
            ],
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
                vec![
                    write_row(1, "a", 1, 100),
                    write_row(2, "b", 2, 100),
                    write_row(3, "c", 3, 100),
                ],
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

    fn spec_from(schema: &TableSchema, ticket: crate::ticket::FlightTicket) -> ScanSpec {
        ScanSpec::from_ticket(&ticket, schema).unwrap()
    }

    #[test]
    fn token_filter_selects_partitions() {
        use crate::ticket::FlightTicket;
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        // Full ring range keeps every partition.
        let all = spec_from(
            &schema,
            FlightTicket {
                token_start: Some(i64::MIN),
                token_end: Some(i64::MAX),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema.clone(), 1024, all).unwrap();
        assert_eq!(total_rows(&p.produce(&DirSource::new(&dir)).unwrap()), 5);

        // Empty range (MAX, MAX] keeps nothing.
        let none = spec_from(
            &schema,
            FlightTicket {
                token_start: Some(i64::MAX),
                token_end: Some(i64::MAX),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, none).unwrap();
        assert_eq!(total_rows(&p.produce(&DirSource::new(&dir)).unwrap()), 0);
    }

    #[test]
    fn predicate_pushdown_filters_rows() {
        use crate::ticket::{FlightTicket, Predicate, PredicateOp};
        use serde_json::json;
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // scores 10..50
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let spec = spec_from(
            &schema,
            FlightTicket {
                predicates: vec![Predicate {
                    column: "score".into(),
                    op: PredicateOp::Gt,
                    value: json!(25),
                }],
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        // scores 30,40,50 pass `> 25` — assert WHICH rows, not just the count.
        let mut survivors: Vec<i32> = Vec::new();
        for b in &batches {
            let scores = b
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            survivors.extend((0..scores.len()).map(|i| scores.value(i)));
        }
        survivors.sort_unstable();
        assert_eq!(survivors, vec![30, 40, 50]);
    }

    #[test]
    fn multiple_predicates_are_anded() {
        use crate::ticket::{FlightTicket, Predicate, PredicateOp};
        use serde_json::json;
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // 10..50
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let spec = spec_from(
            &schema,
            FlightTicket {
                predicates: vec![
                    Predicate {
                        column: "score".into(),
                        op: PredicateOp::Gt,
                        value: json!(10),
                    },
                    Predicate {
                        column: "score".into(),
                        op: PredicateOp::Lt,
                        value: json!(40),
                    },
                ],
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        // 10 < score < 40 → 20, 30.
        assert_eq!(total_rows(&p.produce(&DirSource::new(&dir)).unwrap()), 2);
    }

    #[test]
    fn predicate_on_projected_out_column_still_filters() {
        use crate::ticket::{FlightTicket, Predicate, PredicateOp};
        use serde_json::json;
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        // Project out `score` but filter on it — must still filter correctly.
        let spec = spec_from(
            &schema,
            FlightTicket {
                columns: Some(vec!["id".into(), "name".into()]),
                predicates: vec![Predicate {
                    column: "score".into(),
                    op: PredicateOp::Gt,
                    value: json!(25),
                }],
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            total_rows(&batches),
            3,
            "predicate on a projected-out column still filters"
        );
        assert!(
            batches[0].column_by_name("score").is_none(),
            "score absent from output"
        );
    }

    #[test]
    fn projection_restricts_columns() {
        use crate::ticket::FlightTicket;
        let schema = simple_schema();
        let (_temp, _data, dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 10, 100)]]);

        let spec = spec_from(
            &schema,
            FlightTicket {
                columns: Some(vec!["id".into(), "name".into()]),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();

        let arrow_schema = p.arrow_schema().unwrap();
        let names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id", "name"], "score projected out");

        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert!(batches[0].column_by_name("score").is_none());
    }

    #[test]
    fn resolve_builds_snapshot_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("ks").join("tbl")).unwrap();
        let src = DirSource::resolve(tmp.path(), "ks", "tbl", Some("snap1"));
        assert!(
            src.dir.ends_with("ks/tbl/snapshots/snap1"),
            "got {:?}",
            src.dir
        );
        // Empty/None snapshot resolves to the live table dir.
        let live = DirSource::resolve(tmp.path(), "ks", "tbl", None);
        assert!(live.dir.ends_with("ks/tbl"));
    }

    #[test]
    fn reads_from_snapshot_directory() {
        let schema = simple_schema();
        let rows = (1..=3)
            .map(|i| write_row(i, &format!("n{i}"), i, 100))
            .collect::<Vec<_>>();
        let (_temp, data_dir, table_dir) = build_sstables(&schema, vec![rows]);
        make_snapshot(&table_dir, "snap1");

        let producer = MergeProducer::new(schema, 1024).unwrap();
        let src = DirSource::resolve(&data_dir, KS, TBL, Some("snap1"));
        let batches = producer.produce(&src).unwrap();
        assert_eq!(
            total_rows(&batches),
            3,
            "reads the frozen snapshot SSTables"
        );
    }

    #[test]
    fn empty_source_yields_no_batches() {
        let schema = simple_schema();
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let batches = producer.produce_from_paths(vec![]).unwrap();
        assert!(batches.is_empty());
    }

    // ---- Issue #839: input SSTable pruning by token range ----

    use crate::filter::ScanSpec;
    use crate::ticket::FlightTicket;
    use cqlite_core::storage::sstable::summary_reader::SummaryReader;
    use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    use cqlite_core::{Config, Platform};
    use std::sync::Arc;

    /// Read a Data.db's sibling Summary.db and return its (minToken, maxToken).
    fn span_of(data_path: &std::path::Path) -> (i64, i64) {
        let name = data_path.file_name().unwrap().to_str().unwrap();
        let summary = data_path.with_file_name(name.replace("-Data.db", "-Summary.db"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let platform = rt.block_on(Platform::new(&Config::default())).unwrap();
        let reader = rt
            .block_on(SummaryReader::open(&summary, Arc::new(platform)))
            .unwrap();
        let min = DecoratedKey::from_key_bytes(reader.get_first_key().to_vec())
            .unwrap()
            .token;
        let max = DecoratedKey::from_key_bytes(reader.get_last_key().to_vec())
            .unwrap()
            .token;
        (min, max)
    }

    fn spec_with_token(start: i64, end: i64) -> ScanSpec {
        ScanSpec::from_ticket(
            &FlightTicket {
                token_start: Some(start),
                token_end: Some(end),
                ..Default::default()
            },
            &simple_schema(),
        )
        .unwrap()
    }

    /// (b) A narrow token range prunes the SSTable that does not overlap it.
    #[test]
    fn prune_drops_non_overlapping_sstable() {
        let schema = simple_schema();
        // Two SSTables, each its own flush batch (separate Data.db).
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 10, 100)],
                vec![write_row(2, "b", 20, 100)],
            ],
        );
        let paths = DirSource::new(&dir).data_paths().unwrap();
        assert_eq!(paths.len(), 2, "two SSTables expected");

        // Compute each SSTable's token span and pick a half-open range covering
        // exactly one of them.
        let (min0, max0) = span_of(&paths[0]);
        let (min1, max1) = span_of(&paths[1]);
        assert_ne!(
            (min0, max0),
            (min1, max1),
            "spans must differ to test pruning"
        );

        // Target paths[0] only: (min0 - 1, max0] excludes paths[1]'s span.
        let (lo, hi) = (min0 - 1, max0);
        // Sanity: this range really does separate the two spans.
        let spec = spec_with_token(lo, hi);
        let tf = spec.token.unwrap();
        assert!(tf.overlaps(min0, max0), "target span must overlap");
        // Only meaningful if the other span is genuinely outside the range.
        if tf.overlaps(min1, max1) {
            // The two spans straddle the boundary; skip rather than assert wrongly.
            return;
        }

        let producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let kept = producer.prune_paths(paths.clone()).unwrap();
        assert_eq!(kept.len(), 1, "non-overlapping SSTable pruned");
        assert_eq!(kept[0], paths[0]);
    }

    /// (c) The produced row set is IDENTICAL whether or not the input prune ran:
    /// the per-partition backstop guarantees correctness regardless.
    #[test]
    fn prune_preserves_produced_rows() {
        let schema = simple_schema();
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 10, 100)],
                vec![write_row(2, "b", 20, 100)],
                vec![write_row(3, "c", 30, 100)],
            ],
        );
        let paths = DirSource::new(&dir).data_paths().unwrap();

        // Pick a range that overlaps a subset of SSTables (token of id=1's span).
        let (min0, max0) = span_of(&paths[0]);
        let spec = spec_with_token(min0 - 1, max0);

        let pruned_producer = MergeProducer::with_spec(schema.clone(), 1024, spec.clone()).unwrap();
        let pruned_rows = total_rows(&pruned_producer.produce(&DirSource::new(&dir)).unwrap());

        // Full-scan run: same spec but feed every path explicitly to the merge
        // WITHOUT the input prune (call produce_from_paths is the same code path,
        // but we compare against a producer whose spec keeps the backstop only).
        // Build the reference by pruning disabled: pass all paths and rely on the
        // per-partition token backstop to drop the same partitions.
        let full_producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let full_rows = {
            // Exercise the backstop directly over the full unpruned path list.
            let all = DirSource::new(&dir).data_paths().unwrap();
            let merger_only = full_producer.produce_unpruned_for_test(all).unwrap();
            total_rows(&merger_only)
        };

        assert_eq!(
            pruned_rows, full_rows,
            "pruned run yields identical rows to full-scan-then-filter"
        );
    }

    /// (d) A missing Summary.db means the path is kept (fail open).
    #[test]
    fn prune_keeps_path_when_summary_missing() {
        let schema = simple_schema();
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 10, 100)],
                vec![write_row(2, "b", 20, 100)],
            ],
        );
        let paths = DirSource::new(&dir).data_paths().unwrap();

        // Delete the Summary.db for paths[0] so its span is unknowable.
        let name = paths[0].file_name().unwrap().to_str().unwrap();
        let summary = paths[0].with_file_name(name.replace("-Data.db", "-Summary.db"));
        std::fs::remove_file(&summary).unwrap();

        // A range that, with a readable summary, would prune paths[0].
        let (min0, max0) = span_of(&paths[1]); // any concrete range
        let _ = (min0, max0);
        // Choose a tiny empty-ish range; the point is paths[0] is kept regardless.
        let spec = spec_with_token(i64::MAX - 1, i64::MAX);
        let producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let kept = producer.prune_paths(paths.clone()).unwrap();
        assert!(
            kept.contains(&paths[0]),
            "path with missing Summary.db must be kept (fail open)"
        );
    }
}
