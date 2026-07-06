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

use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use cqlite_core::export::{build_arrow_schema, rows_to_record_batch, ArrowConvertError};
use cqlite_core::query::{build_row_from_scan, ColumnInfo, QueryRow};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::storage::write_engine::merge::{MergeStep, RowData};
use cqlite_core::storage::write_engine::KWayMerger;
use cqlite_core::types::{DataType, RowCells, ScanRow, Value};
use cqlite_core::RowKey;

use crate::agg::{AggError, AggPlan};
use crate::cancel::CancelFlag;
use crate::filter::ScanSpec;
use crate::ticket::Aggregation;

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
    /// The aggregation spec was invalid (bad column, Sum on non-numeric, …).
    #[error("invalid aggregation: {0}")]
    Aggregation(#[from] AggError),
    /// The merge was cancelled cooperatively (issue #1473) — e.g. the `do_get`
    /// client disconnected mid-stream, dropping the driving future. Maps to a
    /// clean gRPC `Aborted` status; no partial result is returned.
    #[error("merge cancelled")]
    Cancelled,
    /// The resolved SSTable directory escaped the data directory — e.g. via a
    /// symlink inside the data dir (issue #1430). Charset validation already
    /// blocks `../`/absolute fields; this is the canonicalization backstop.
    #[error("unsafe path for {field}: escapes the data directory")]
    UnsafePath {
        /// Which ticket field produced the escaping path (`table`/`snapshot`).
        field: &'static str,
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

    /// The resolved SSTable directory this source reads from. Used by the
    /// `table_stats` action (issue #944) to gather per-SSTable statistics from
    /// the same directory `do_get` would merge.
    pub fn into_dir(self) -> PathBuf {
        self.dir
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
    ///
    /// As defense in depth (issue #1430) the resolved directory is verified to
    /// stay within `data_dir` after resolving symlinks; an escape yields
    /// [`ProducerError::UnsafePath`]. Callers should still validate the ticket
    /// fields with [`crate::pathsafe`] at parse time — that is the primary guard.
    pub fn resolve(
        data_dir: &Path,
        keyspace: &str,
        table: &str,
        snapshot: Option<&str>,
    ) -> Result<Self, ProducerError> {
        let table_dir = Self::table_base_dir(data_dir, keyspace, table);
        let dir = match snapshot {
            Some(name) if !name.is_empty() => table_dir.join("snapshots").join(name),
            _ => table_dir,
        };
        let field = if matches!(snapshot, Some(name) if !name.is_empty()) {
            "snapshot"
        } else {
            "table"
        };
        crate::pathsafe::assert_within(field, data_dir, &dir)
            .map_err(|_| ProducerError::UnsafePath { field })?;
        Ok(Self::new(dir))
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
            // Per-file containment (issue #1430): legit SSTable components live
            // DIRECTLY in the resolved dir (Cassandra snapshots are hardlinks,
            // which canonicalize under the dir). The directory-level guard in
            // `resolve` only vets the enumeration dir; a SYMLINK inside an
            // otherwise-valid dir can still resolve outside `data_dir`. Exclude
            // (fail-closed) any entry whose canonicalized target escapes the dir
            // so it is never opened/merged.
            .filter(
                |p| match crate::pathsafe::assert_within("sstable", &self.dir, p) {
                    Ok(()) => true,
                    Err(reason) => {
                        tracing::debug!(
                            path = %p.display(),
                            %reason,
                            "excluding SSTable whose resolved path escapes the data directory"
                        );
                        false
                    }
                },
            )
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
    /// Aggregation pushdown plan (issue #841). When `Some`, the producer emits
    /// PARTIAL aggregate rows under [`Self::partial_columns`] instead of full
    /// rows; when `None` the row path is unchanged.
    agg: Option<AggPlan>,
    /// Partial-output column metadata, present iff [`Self::agg`] is `Some`.
    partial_columns: Option<Vec<ColumnInfo>>,
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
            agg: None,
            partial_columns: None,
        })
    }

    /// Attach an aggregation spec (issue #841), validating it against the table
    /// schema. When set, [`Self::arrow_schema`] and the produced batches switch
    /// to the PARTIAL aggregate schema. Consumes and returns `self` for chaining.
    pub fn with_aggregation(mut self, aggregation: &Aggregation) -> Result<Self, ProducerError> {
        let plan = AggPlan::build(aggregation, &self.schema)?;
        let partial = plan.partial_columns(&self.schema)?;
        self.agg = Some(plan);
        self.partial_columns = Some(partial);
        Ok(self)
    }

    /// The Arrow schema clients should expect (for `GetFlightInfo`/`GetSchema`).
    ///
    /// Each field is augmented with the `cqlite:pushdown` metadata key declaring
    /// how the server can push predicates on that column (`"full"`, `"equality"`,
    /// or `"none"`) — see [`pushdown_capability`]. The Trino connector reads this
    /// to gate pushdown per column, since several CQL types (inet, duration,
    /// varint, …) surface as Arrow UTF-8/other shapes indistinguishable from
    /// genuine `text` by Arrow type alone. Field order, names, types, and any
    /// existing metadata (e.g. the uuid extension) are preserved.
    pub fn arrow_schema(&self) -> Result<ArrowSchema, ProducerError> {
        // With aggregation, the output is the PARTIAL schema (group-by columns
        // then aggregate outputs); otherwise it is the projected row schema.
        let output_columns = self.output_columns();
        let base = build_arrow_schema(output_columns)?;
        let fields: Vec<ArrowField> = base
            .fields()
            .iter()
            .zip(output_columns.iter())
            .map(|(field, column)| {
                let capability = column
                    .cql_type
                    .as_ref()
                    .map(pushdown_capability)
                    .unwrap_or("none");
                let mut metadata = field.metadata().clone();
                metadata.insert("cqlite:pushdown".to_string(), capability.to_string());
                field.as_ref().clone().with_metadata(metadata)
            })
            .collect();
        Ok(ArrowSchema::new_with_metadata(
            fields,
            base.metadata().clone(),
        ))
    }

    /// The ordered Arrow column metadata for the produced output: the PARTIAL
    /// aggregate columns when aggregation is set, else the projected row columns.
    pub fn columns(&self) -> &[ColumnInfo] {
        self.output_columns()
    }

    /// The output column set: partial aggregate columns under aggregation, else
    /// the projected row columns.
    fn output_columns(&self) -> &[ColumnInfo] {
        match &self.partial_columns {
            Some(partial) => partial,
            None => &self.columns,
        }
    }

    /// Merge `source`'s SSTables and return the resulting Arrow batches.
    pub fn produce(&self, source: &dyn SstableSource) -> Result<Vec<RecordBatch>, ProducerError> {
        self.produce_cancellable(source, &CancelFlag::new())
    }

    /// Like [`Self::produce`], but cooperatively cancellable (issue #1473): the
    /// merge loop polls `cancel` between partition steps and aborts early with
    /// [`ProducerError::Cancelled`] when it is set. Used by `do_get` so a client
    /// disconnect (which drops the driving future and cancels the flag) stops the
    /// CPU-bound merge instead of letting it run to completion on a blocking-pool
    /// thread.
    pub fn produce_cancellable(
        &self,
        source: &dyn SstableSource,
        cancel: &CancelFlag,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let paths = source.data_paths()?;
        let paths = self.prune_paths(paths)?;
        self.merge_paths(paths, cancel)
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
        self.merge_paths(paths, &CancelFlag::new())
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
    ///
    /// With an aggregation plan this branches into [`Self::aggregate_paths`],
    /// which feeds the SAME surviving rows (token-pruned, predicate-filtered,
    /// tombstone-suppressed, LWW-reconciled) into the accumulator and emits
    /// PARTIAL rows. Without one, it emits full rows in `batch_size` chunks.
    fn merge_paths(
        &self,
        paths: Vec<PathBuf>,
        cancel: &CancelFlag,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        if let Some(plan) = &self.agg {
            return self.aggregate_paths(plan, paths, cancel);
        }

        let mut batches = Vec::new();
        if paths.is_empty() {
            return Ok(batches);
        }

        let mut merger = KWayMerger::new(paths, &self.schema).map_err(ProducerError::Merge)?;
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);

        while let MergeStep::Partition { key, rows } =
            merger.step().map_err(ProducerError::Merge)?
        {
            // Cooperative cancellation (issue #1473): checked at the top of every
            // partition step, so a cancel (e.g. client disconnect) stops the merge
            // within one partition rather than draining every remaining partition.
            if cancel.is_cancelled() {
                return Err(ProducerError::Cancelled);
            }
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
                // Predicate pushdown: evaluate the nested filter tree with SQL
                // Kleene logic and keep the row only when it is definitely True
                // (Unknown and False both reject — WHERE semantics, issue #834).
                if let Some(filter) = &self.spec.filter {
                    if !filter.keeps(&row) {
                        continue;
                    }
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

    /// Aggregate path (issue #841): stream every surviving row — through the same
    /// token-prune + predicate filter as the row path — directly into the
    /// accumulator state, then emit the PARTIAL batch(es) under the partial
    /// schema. Rows are NOT buffered: only per-group accumulator state is kept,
    /// so memory scales with the group count, not the input row count.
    ///
    /// A global aggregation (`group_by` empty) always emits exactly one row,
    /// even over zero input rows. A grouped aggregation emits one row per group.
    fn aggregate_paths(
        &self,
        plan: &AggPlan,
        paths: Vec<PathBuf>,
        cancel: &CancelFlag,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        // Aggregate POST-reconciliation so partials match a SELECT's row set.
        let mut state = plan.new_state();

        if !paths.is_empty() {
            let mut merger = KWayMerger::new(paths, &self.schema).map_err(ProducerError::Merge)?;
            while let MergeStep::Partition { key, rows } =
                merger.step().map_err(ProducerError::Merge)?
            {
                // Cooperative cancellation (issue #1473): see `merge_paths`.
                if cancel.is_cancelled() {
                    return Err(ProducerError::Cancelled);
                }
                if let Some(token) = &self.spec.token {
                    if !token.contains(key.token) {
                        continue;
                    }
                }
                for entry in rows {
                    let Some(row) = self.entry_to_row(&key.key, entry.row_data) else {
                        continue;
                    };
                    if let Some(filter) = &self.spec.filter {
                        if !filter.keeps(&row) {
                            continue;
                        }
                    }
                    plan.accumulate_row(&mut state, &row)?;
                }
            }
        }

        let partial_rows = plan.finish(state);
        if partial_rows.is_empty() {
            // Grouped aggregation over zero input → no rows (global always emits).
            return Ok(Vec::new());
        }
        let columns = self.output_columns();
        Ok(vec![rows_to_record_batch(columns, &partial_rows)?])
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

        // Issue #1334: build the SAME `ScanRow::Row` carrier every scan producer
        // builds, so `build_row_from_scan` disassembles it into real column values
        // (previously this emitted `Value::Map`, which fell through to the non-row
        // fallback and silently dropped every Flight column value — roborev H2).
        let row_cells: RowCells = cells
            .into_iter()
            // A cell tombstone leaves the column absent → null in Arrow output,
            // matching the CLI's "emit null for tombstoned cells" behaviour.
            .filter(|c| !matches!(c.value, Value::Tombstone(_)))
            .map(|c| (std::sync::Arc::from(c.column.as_str()), c.value))
            .collect();

        let key = RowKey(partition_key.to_vec());
        build_row_from_scan(key, ScanRow::Row(row_cells), &[], Some(&self.schema))
    }

    /// Merge `paths` WITHOUT the input prune, relying only on the per-partition
    /// token backstop. Used by tests to prove the pruned run yields identical
    /// rows to a full-scan-then-filter run.
    #[cfg(test)]
    fn produce_unpruned_for_test(
        &self,
        paths: Vec<PathBuf>,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        self.merge_paths(paths, &CancelFlag::new())
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

/// Declare how the server can push predicates on a column of this CQL type.
///
/// The capability is aligned EXACTLY with what
/// [`crate::filter`]`::json_to_value` can lower a JSON operand into:
/// - `"full"` — every operator (Equal, In, ordering Gt/Gte/Lt/Lte, Prefix) is
///   safe. These are the types `json_to_value` parses into a directly
///   comparable [`Value`]: the integer family (TinyInt/SmallInt/Int/BigInt),
///   `Counter` and `Timestamp` (both lowered from a JSON integer), `Float`/
///   `Double`, `Boolean`, and the textual family (Text/Ascii/Varchar).
/// - `"equality"` — `json_to_value` lowers `Uuid`/`TimeUuid` to `Value::Uuid`,
///   which only supports exact match (Equal/In/IsNull). Ordering and prefix on a
///   uuid would compare by uuid bytes, not by the VARCHAR surface form, so they
///   must stay a Trino residual.
/// - `"none"` — everything else (`Inet`, `Duration`, `Varint`, `Decimal`,
///   `Blob`, `Date`, `Time`, and the collection/tuple/UDT/custom types):
///   `json_to_value` rejects them, so nothing can be pushed.
///
/// `Frozen(inner)` is unwrapped recursively (it never changes comparability).
pub(crate) fn pushdown_capability(ty: &CqlType) -> &'static str {
    match ty {
        CqlType::Frozen(inner) => pushdown_capability(inner),
        CqlType::Boolean
        | CqlType::TinyInt
        | CqlType::SmallInt
        | CqlType::Int
        | CqlType::BigInt
        | CqlType::Counter
        | CqlType::Float
        | CqlType::Double
        | CqlType::Timestamp
        | CqlType::Text
        | CqlType::Ascii
        | CqlType::Varchar => "full",
        CqlType::Uuid | CqlType::TimeUuid => "equality",
        CqlType::Decimal
        | CqlType::Blob
        | CqlType::Date
        | CqlType::Time
        | CqlType::Inet
        | CqlType::Duration
        | CqlType::Varint
        | CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(_, _)
        | CqlType::Tuple(_)
        | CqlType::Udt(_, _)
        | CqlType::Custom(_) => "none",
    }
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
    use crate::cancel::CancelFlag;
    use cqlite_core::schema::{ClusteringColumn, Column};

    /// Issue #1473: the merge loop must observe cooperative cancellation and
    /// abort with [`ProducerError::Cancelled`] instead of draining every
    /// partition. A `do_get` client that disconnects mid-stream drops the
    /// driving future, which cancels the flag; the CPU-bound merge must then
    /// stop rather than run to completion and pin a blocking-pool thread.
    ///
    /// Fails to compile on `main` (no `produce_cancellable`/`CancelFlag` and no
    /// `Cancelled` variant exist there) — i.e. it fails on current main.
    #[test]
    fn merge_aborts_when_cancel_flag_is_set() {
        let schema = simple_schema();
        // Several partitions across two SSTables so an un-cancelled merge has
        // real per-partition work (multiple merge steps) to do.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 10, 100), write_row(2, "b", 20, 100)],
                vec![write_row(3, "c", 30, 100), write_row(4, "d", 40, 100)],
            ],
        );
        let producer = MergeProducer::new(schema, 8192).unwrap();

        // Baseline: an un-cancelled merge produces every partition's row.
        let fresh = CancelFlag::new();
        let batches = producer
            .produce_cancellable(&DirSource::new(&dir), &fresh)
            .expect("un-cancelled merge succeeds");
        assert_eq!(total_rows(&batches), 4, "all four partitions produced");

        // Cancelled: the loop aborts at the first partition step (a bounded 0
        // partitions of work), returning Cancelled rather than the full result.
        let cancelled = CancelFlag::new();
        cancelled.cancel();
        let err = producer
            .produce_cancellable(&DirSource::new(&dir), &cancelled)
            .expect_err("cancelled merge aborts");
        assert!(
            matches!(err, ProducerError::Cancelled),
            "expected ProducerError::Cancelled, got {err:?}"
        );
    }

    #[test]
    fn pushdown_capability_aligns_with_json_to_value() {
        use cqlite_core::schema::CqlType;
        // Full: ordering + equality + prefix are all safe.
        assert_eq!(pushdown_capability(&CqlType::Text), "full");
        assert_eq!(pushdown_capability(&CqlType::BigInt), "full");
        // Equality-only: uuid/timeuuid lower to Value::Uuid (exact match only).
        assert_eq!(pushdown_capability(&CqlType::Uuid), "equality");
        // Frozen unwraps to its inner type's capability.
        assert_eq!(
            pushdown_capability(&CqlType::Frozen(Box::new(CqlType::Uuid))),
            "equality"
        );
        // None: json_to_value rejects these, so nothing is pushable.
        assert_eq!(pushdown_capability(&CqlType::Inet), "none");
        assert_eq!(pushdown_capability(&CqlType::Duration), "none");
    }

    #[test]
    fn arrow_schema_tags_each_field_with_pushdown_capability() {
        // simple_schema: id (uuid? -> check), name (text), score (int).
        let schema = simple_schema();
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let arrow_schema = producer.arrow_schema().unwrap();
        // Every field carries the pushdown metadata key.
        for field in arrow_schema.fields() {
            assert!(
                field.metadata().contains_key("cqlite:pushdown"),
                "field {} missing pushdown metadata",
                field.name()
            );
        }
        // name (text) is full; score (int) is full.
        assert_eq!(
            arrow_schema
                .field_with_name("name")
                .unwrap()
                .metadata()
                .get("cqlite:pushdown")
                .map(String::as_str),
            Some("full")
        );
        assert_eq!(
            arrow_schema
                .field_with_name("score")
                .unwrap()
                .metadata()
                .get("cqlite:pushdown")
                .map(String::as_str),
            Some("full")
        );
    }

    #[test]
    fn arrow_schema_preserves_uuid_extension_alongside_pushdown() {
        use crate::testutil::uuid_schema;
        let schema = uuid_schema();
        let producer = MergeProducer::new(schema, 1024).unwrap();
        let arrow_schema = producer.arrow_schema().unwrap();
        let id_field = arrow_schema.field_with_name("id").unwrap();
        // Existing uuid extension metadata survives the augmentation...
        assert_eq!(
            id_field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid")
        );
        // ...and the uuid column declares equality-only pushdown.
        assert_eq!(
            id_field
                .metadata()
                .get("cqlite:pushdown")
                .map(String::as_str),
            Some("equality")
        );
    }

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

    /// Issue #1334 / roborev H2: the Flight producer's row path must return the
    /// REAL column values, not drop them. Before the carrier unification the
    /// producer emitted a `Value::Map` that fell through `build_row_from_scan`'s
    /// non-row fallback and silently lost every column value. This produces two
    /// fully-populated rows and asserts both the text (`name`) and int (`score`)
    /// column values survive end-to-end through `produce`.
    #[test]
    fn flight_row_path_returns_real_column_values() {
        use arrow::array::Array;
        let schema = simple_schema();
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "alice", 42, 100),
                write_row(2, "bob", 7, 100),
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
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("name column must be a populated string array, not dropped (H2)");
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        let idx1 = (0..ids.len()).find(|&i| ids.value(i) == 1).unwrap();
        assert!(!names.is_null(idx1), "H2: name value must not be dropped");
        assert_eq!(names.value(idx1), "alice");
        assert_eq!(scores.value(idx1), 42);

        let idx2 = (0..ids.len()).find(|&i| ids.value(i) == 2).unwrap();
        assert_eq!(names.value(idx2), "bob");
        assert_eq!(scores.value(idx2), 7);
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
        let src = DirSource::resolve(tmp.path(), "ks", "tbl", Some("snap1")).expect("resolve");
        assert!(
            src.dir.ends_with("ks/tbl/snapshots/snap1"),
            "got {:?}",
            src.dir
        );
        // Empty/None snapshot resolves to the live table dir.
        let live = DirSource::resolve(tmp.path(), "ks", "tbl", None).expect("resolve");
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
        let src = DirSource::resolve(&data_dir, KS, TBL, Some("snap1")).expect("resolve");
        let batches = producer.produce(&src).unwrap();
        assert_eq!(
            total_rows(&batches),
            3,
            "reads the frozen snapshot SSTables"
        );
    }

    /// Issue #1430 (roborev per-file follow-up): `data_paths` enumerates files
    /// DIRECTLY in the resolved dir, but a SYMLINK inside an otherwise-valid dir
    /// can resolve OUTSIDE `data_dir`. Such an entry must be excluded (fail-closed)
    /// and never returned for merging. A hardlink-style legit component (a real
    /// file in the dir, which canonicalizes under the dir) must still be served.
    #[test]
    #[cfg(unix)]
    fn data_paths_excludes_symlink_escaping_the_dir() {
        use std::os::unix::fs::symlink;
        let dir_tmp = tempfile::TempDir::new().unwrap();
        let outside_tmp = tempfile::TempDir::new().unwrap();
        let dir = dir_tmp.path();

        // A legit Data.db living directly in the table dir.
        let legit = dir.join("nb-1-big-Data.db");
        std::fs::write(&legit, b"legit").unwrap();

        // A secret file OUTSIDE the tree, reachable only via a symlink placed
        // inside the (valid) table dir with a legit-looking Data.db name.
        let secret = outside_tmp.path().join("secret-Data.db");
        std::fs::write(&secret, b"secret").unwrap();
        let escaping = dir.join("nb-99-big-Data.db");
        symlink(&secret, &escaping).unwrap();

        let paths = DirSource::new(dir).data_paths().unwrap();

        // Only the legit component survives; the escaping symlink is excluded.
        let canon_legit = legit.canonicalize().unwrap();
        let canon_secret = secret.canonicalize().unwrap();
        let canon_paths: Vec<PathBuf> = paths
            .iter()
            .map(|p| p.canonicalize().unwrap_or_else(|_| p.clone()))
            .collect();
        assert!(
            canon_paths.contains(&canon_legit),
            "legit component must be served, got {paths:?}"
        );
        assert!(
            !canon_paths.contains(&canon_secret),
            "symlink escaping the data dir must NOT be returned, got {paths:?}"
        );
        assert_eq!(paths.len(), 1, "only the legit component survives");
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

    // ---- Issue #834: nested predicate pushdown (OR/NOT/IS NULL) ----

    /// Collect the surviving partition-key `id` values across all batches.
    fn surviving_ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids = Vec::new();
        for b in batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            ids.extend((0..col.len()).map(|i| col.value(i)));
        }
        ids.sort_unstable();
        ids
    }

    /// `(score > 10 AND name = 'x') OR name IS NULL` — asserts the EXACT
    /// surviving rows, exercising AND, OR and IS NULL together.
    #[test]
    fn nested_or_with_is_null_keeps_exact_rows() {
        use crate::testutil::write_score_only;
        use crate::ticket::{FlightTicket, PredicateExpr, PredicateOp};
        use serde_json::json;

        let schema = simple_schema();
        // id=1: score=20,name="x"  → left branch TRUE  → kept
        // id=2: score=20,name="y"  → left FALSE, name present → reject
        // id=3: score=5, name="x"  → left FALSE (score), name present → reject
        // id=4: score=99 (no name) → left UNKNOWN(name), name IS NULL TRUE → kept
        // id=5: score=5  (no name) → left FALSE? score<10 so AND FALSE; IS NULL TRUE → kept
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "x", 20, 100),
                write_row(2, "y", 20, 100),
                write_row(3, "x", 5, 100),
                write_score_only(4, 99, 100),
                write_score_only(5, 5, 100),
            ]],
        );

        let filter = PredicateExpr::Or {
            exprs: vec![
                PredicateExpr::And {
                    exprs: vec![
                        PredicateExpr::Compare {
                            column: "score".into(),
                            op: PredicateOp::Gt,
                            value: json!(10),
                        },
                        PredicateExpr::Compare {
                            column: "name".into(),
                            op: PredicateOp::Equal,
                            value: json!("x"),
                        },
                    ],
                },
                PredicateExpr::IsNull {
                    column: "name".into(),
                },
            ],
        };
        let spec = spec_from(
            &schema,
            FlightTicket {
                filter: Some(filter),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            surviving_ids(&batches),
            vec![1, 4, 5],
            "left-branch match (1) plus both name-is-null rows (4,5)"
        );
    }

    /// `NOT (score > 10)` must NOT keep rows where `score` is NULL: `score > 10`
    /// is UNKNOWN there, `NOT UNKNOWN` is UNKNOWN, and WHERE rejects UNKNOWN.
    /// This is the case the old "missing column → false" logic got wrong.
    #[test]
    fn not_over_null_column_follows_sql_semantics() {
        use crate::testutil::write_name_only;
        use crate::ticket::{FlightTicket, PredicateExpr, PredicateOp};
        use serde_json::json;

        let schema = simple_schema();
        // id=1: score=20        → score>10 TRUE  → NOT FALSE  → reject
        // id=2: score=5         → score>10 FALSE → NOT TRUE   → keep
        // id=3: name only, score NULL → score>10 UNKNOWN → NOT UNKNOWN → reject
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "a", 20, 100),
                write_row(2, "b", 5, 100),
                write_name_only(3, "c", 100),
            ]],
        );

        let filter = PredicateExpr::Not {
            expr: Box::new(PredicateExpr::Compare {
                column: "score".into(),
                op: PredicateOp::Gt,
                value: json!(10),
            }),
        };
        let spec = spec_from(
            &schema,
            FlightTicket {
                filter: Some(filter),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            surviving_ids(&batches),
            vec![2],
            "only score=5 survives; NULL-score row rejected (NOT UNKNOWN = UNKNOWN)"
        );
    }

    /// `name IS NULL OR score > 1000` over a NULL-score row: the OR's first
    /// disjunct is TRUE for name-null rows, so an UNKNOWN second disjunct does
    /// not matter (True dominates). And a non-null-name row with low score is
    /// rejected (False OR UNKNOWN = UNKNOWN → reject).
    #[test]
    fn or_with_null_column_matches_sql() {
        use crate::testutil::write_score_only;
        use crate::ticket::{FlightTicket, PredicateExpr, PredicateOp};
        use serde_json::json;

        let schema = simple_schema();
        // id=1: score only, name NULL → name IS NULL TRUE → keep (score>1000 UNKNOWN, dominated)
        // id=2: score only, name NULL → name IS NULL TRUE → keep
        // id=3: name="x", score NULL  → name IS NULL FALSE, score>1000 UNKNOWN → UNKNOWN → reject
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_score_only(1, 50, 100),
                write_score_only(2, 50, 100),
                crate::testutil::write_name_only(3, "x", 100),
            ]],
        );

        let filter = PredicateExpr::Or {
            exprs: vec![
                PredicateExpr::IsNull {
                    column: "name".into(),
                },
                PredicateExpr::Compare {
                    column: "score".into(),
                    op: PredicateOp::Gt,
                    value: json!(1000),
                },
            ],
        };
        let spec = spec_from(
            &schema,
            FlightTicket {
                filter: Some(filter),
                ..Default::default()
            },
        );
        let p = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(surviving_ids(&batches), vec![1, 2]);
    }

    /// v1 back-compat: a flat `predicates` list (no `filter`) yields identical
    /// results to the equivalent explicit `And` filter tree.
    #[test]
    fn v1_flat_predicates_match_explicit_and_tree() {
        use crate::ticket::{FlightTicket, Predicate, PredicateExpr, PredicateOp};
        use serde_json::json;

        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // scores 10..50
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        // v1: two flat predicates 10 < score < 40.
        let v1 = spec_from(
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
        let v1_ids = surviving_ids(
            &MergeProducer::with_spec(schema.clone(), 1024, v1)
                .unwrap()
                .produce(&DirSource::new(&dir))
                .unwrap(),
        );

        // v2: the same constraint as an explicit And tree.
        let v2 = spec_from(
            &schema,
            FlightTicket {
                filter: Some(PredicateExpr::And {
                    exprs: vec![
                        PredicateExpr::Compare {
                            column: "score".into(),
                            op: PredicateOp::Gt,
                            value: json!(10),
                        },
                        PredicateExpr::Compare {
                            column: "score".into(),
                            op: PredicateOp::Lt,
                            value: json!(40),
                        },
                    ],
                }),
                ..Default::default()
            },
        );
        let v2_ids = surviving_ids(
            &MergeProducer::with_spec(schema, 1024, v2)
                .unwrap()
                .produce(&DirSource::new(&dir))
                .unwrap(),
        );

        assert_eq!(v1_ids, v2_ids, "v1 flat predicates == explicit And tree");
        assert_eq!(v1_ids, vec![2, 3], "scores 20,30 → ids 2,3");
    }

    // ---- Issue #841: aggregation pushdown over merged SSTables ----

    use crate::ticket::{AggFunc, AggregateSpec, Aggregation};
    use arrow::array::Array;

    /// Build a producer carrying `aggregation` over `schema`/`spec`.
    fn agg_producer(
        schema: TableSchema,
        spec: ScanSpec,
        aggregation: Aggregation,
    ) -> MergeProducer {
        MergeProducer::with_spec(schema, 1024, spec)
            .unwrap()
            .with_aggregation(&aggregation)
            .unwrap()
    }

    fn count_star(output: &str) -> AggregateSpec {
        AggregateSpec {
            func: AggFunc::Count,
            column: None,
            output: output.into(),
        }
    }

    fn agg_on(func: AggFunc, column: &str, output: &str) -> AggregateSpec {
        AggregateSpec {
            func,
            column: Some(column.into()),
            output: output.into(),
        }
    }

    fn i64_col(batch: &RecordBatch, name: &str) -> arrow::array::Int64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .clone()
    }

    fn i32_col(batch: &RecordBatch, name: &str) -> arrow::array::Int32Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .clone()
    }

    /// Global `count(*)` over N rows → exactly one partial row, count = N.
    #[test]
    fn global_count_star_counts_all_rows() {
        let schema = simple_schema();
        let rows = (1..=7)
            .map(|i| write_row(i, &format!("n{i}"), i, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![count_star("agg0")],
        };
        let p = agg_producer(schema, ScanSpec::default(), agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 1, "global aggregation → one row");
        let counts = i64_col(&batches[0], "agg0");
        assert_eq!(counts.value(0), 7);
        assert!(!counts.is_null(0), "Count is never null");
    }

    /// Global count(col)/sum/min/max with a NULL-score row present: count(score)
    /// excludes the null and sum/min/max skip it.
    #[test]
    fn global_aggregates_skip_null_inputs() {
        use crate::testutil::write_name_only;
        let schema = simple_schema();
        // scores 10,20,30 plus one row whose score is null (name only).
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "a", 10, 100),
                write_row(2, "b", 20, 100),
                write_row(3, "c", 30, 100),
                write_name_only(4, "d", 100),
            ]],
        );

        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![
                count_star("agg0"),
                agg_on(AggFunc::Count, "score", "agg1"),
                agg_on(AggFunc::Sum, "score", "agg2"),
                agg_on(AggFunc::Min, "score", "agg3"),
                agg_on(AggFunc::Max, "score", "agg4"),
            ],
        };
        let p = agg_producer(schema, ScanSpec::default(), agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 1);
        let b = &batches[0];
        assert_eq!(
            i64_col(b, "agg0").value(0),
            4,
            "count(*) counts the null row"
        );
        assert_eq!(
            i64_col(b, "agg1").value(0),
            3,
            "count(score) excludes the null"
        );
        // Sum over an int source is Int64.
        assert_eq!(i64_col(b, "agg2").value(0), 60, "10+20+30");
        // Min/Max keep the source (int) type → Int32.
        assert_eq!(i32_col(b, "agg3").value(0), 10);
        assert_eq!(i32_col(b, "agg4").value(0), 30);
    }

    fn f64_col(batch: &RecordBatch, name: &str) -> arrow::array::Float64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .clone()
    }

    /// #902: `SumDouble` (the avg numerator) over an integer column emits a
    /// Float64 partial and totals in f64, so a running sum past i64::MAX does not
    /// overflow the way a checked-i64 `Sum` would. Here it just verifies the wire
    /// type and value through the real merge/Arrow path.
    #[test]
    fn sum_double_emits_float64_over_integer_column() {
        let schema = simple_schema();
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "a", 10, 100),
                write_row(2, "b", 20, 100),
                write_row(3, "c", 30, 100),
            ]],
        );

        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![
                agg_on(AggFunc::SumDouble, "score", "agg_sum"),
                agg_on(AggFunc::Count, "score", "agg_cnt"),
            ],
        };
        let p = agg_producer(schema, ScanSpec::default(), agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(total_rows(&batches), 1);
        let b = &batches[0];
        // SumDouble → Float64 (not Int64), value 60.0; Count → 3. The connector
        // divides these to 20.0 for avg(score).
        assert_eq!(f64_col(b, "agg_sum").value(0), 60.0);
        assert_eq!(i64_col(b, "agg_cnt").value(0), 3);
    }

    /// Global aggregation over EMPTY input → one row: count = 0, sum/min/max null.
    #[test]
    fn global_aggregate_over_empty_input_emits_zero_row() {
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        // A token range that excludes everything: (MAX, MAX].
        let spec = spec_from(
            &schema,
            FlightTicket {
                token_start: Some(i64::MAX),
                token_end: Some(i64::MAX),
                ..Default::default()
            },
        );
        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![
                count_star("agg0"),
                agg_on(AggFunc::Count, "score", "agg1"),
                agg_on(AggFunc::Sum, "score", "agg2"),
                agg_on(AggFunc::Min, "score", "agg3"),
                agg_on(AggFunc::Max, "score", "agg4"),
            ],
        };
        let p = agg_producer(schema, spec, agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            total_rows(&batches),
            1,
            "global emits one row even on empty"
        );
        let b = &batches[0];
        assert_eq!(i64_col(b, "agg0").value(0), 0, "count(*) = 0");
        assert_eq!(i64_col(b, "agg1").value(0), 0, "count(score) = 0");
        assert!(i64_col(b, "agg2").is_null(0), "sum null on empty");
        assert!(i32_col(b, "agg3").is_null(0), "min null on empty");
        assert!(i32_col(b, "agg4").is_null(0), "max null on empty");
    }

    /// GROUP BY a low-cardinality column → one row per group with correct
    /// per-group count/sum/min/max; a NULL group key forms its own group.
    #[test]
    fn group_by_emits_one_row_per_group_including_null_key() {
        use crate::testutil::write_score_only;
        let schema = simple_schema();
        // group "x": scores 10, 30 ; group "y": score 20 ; NULL name: score 99.
        let (_temp, _data, dir) = build_sstables(
            &schema,
            vec![vec![
                write_row(1, "x", 10, 100),
                write_row(2, "y", 20, 100),
                write_row(3, "x", 30, 100),
                write_score_only(4, 99, 100), // name is null → its own group
            ]],
        );

        let agg = Aggregation {
            group_by: vec!["name".into()],
            aggregates: vec![
                count_star("c"),
                agg_on(AggFunc::Sum, "score", "s"),
                agg_on(AggFunc::Min, "score", "mn"),
                agg_on(AggFunc::Max, "score", "mx"),
            ],
        };
        let p = agg_producer(schema, ScanSpec::default(), agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            total_rows(&batches),
            3,
            "groups: x, y, and the null-name group"
        );

        // Collect per-group results keyed by name (None = the NULL group).
        let b = &batches[0];
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .clone();
        let c = i64_col(b, "c");
        let s = i64_col(b, "s");
        let mn = i32_col(b, "mn");
        let mx = i32_col(b, "mx");

        use std::collections::HashMap;
        let mut by_group: HashMap<Option<String>, (i64, i64, i32, i32)> = HashMap::new();
        for i in 0..b.num_rows() {
            let key = if names.is_null(i) {
                None
            } else {
                Some(names.value(i).to_string())
            };
            by_group.insert(key, (c.value(i), s.value(i), mn.value(i), mx.value(i)));
        }

        assert_eq!(by_group[&Some("x".into())], (2, 40, 10, 30));
        assert_eq!(by_group[&Some("y".into())], (1, 20, 20, 20));
        assert_eq!(
            by_group[&None],
            (1, 99, 99, 99),
            "the null-name row forms its own group"
        );
    }

    /// Aggregation composes with a predicate filter and token pruning: only rows
    /// surviving `score > 10` (and the split's range) feed the accumulator.
    #[test]
    fn aggregation_composes_with_predicate_and_token_prune() {
        use crate::ticket::{Predicate, PredicateOp};
        use serde_json::json;
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // scores 10..50
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        // Full ring + score > 10 → scores 20,30,40,50 survive.
        let spec = spec_from(
            &schema,
            FlightTicket {
                token_start: Some(i64::MIN),
                token_end: Some(i64::MAX),
                predicates: vec![Predicate {
                    column: "score".into(),
                    op: PredicateOp::Gt,
                    value: json!(10),
                }],
                ..Default::default()
            },
        );
        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![
                count_star("agg0"),
                agg_on(AggFunc::Sum, "score", "agg1"),
                agg_on(AggFunc::Min, "score", "agg2"),
                agg_on(AggFunc::Max, "score", "agg3"),
            ],
        };
        let p = agg_producer(schema, spec, agg);
        let batches = p.produce(&DirSource::new(&dir)).unwrap();
        let b = &batches[0];
        assert_eq!(i64_col(b, "agg0").value(0), 4, "4 rows pass > 10");
        assert_eq!(i64_col(b, "agg1").value(0), 140, "20+30+40+50");
        assert_eq!(i32_col(b, "agg2").value(0), 20);
        assert_eq!(i32_col(b, "agg3").value(0), 50);
    }

    /// The partial RecordBatch schema's column names and Arrow types match the
    /// contract: group-by columns keep their mapped type, Count→Int64,
    /// Sum(int)→Int64, Min/Max(int)→Int32.
    #[test]
    fn partial_schema_matches_contract() {
        use arrow::datatypes::DataType as ArrowDataType;
        let schema = simple_schema();
        let agg = Aggregation {
            group_by: vec!["name".into()],
            aggregates: vec![
                count_star("agg0"),
                agg_on(AggFunc::Sum, "score", "agg1"),
                agg_on(AggFunc::Min, "score", "agg2"),
                agg_on(AggFunc::Max, "score", "agg3"),
            ],
        };
        let p = agg_producer(schema, ScanSpec::default(), agg);
        let s = p.arrow_schema().unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["name", "agg0", "agg1", "agg2", "agg3"],
            "group-by column then aggregate outputs in order"
        );
        assert_eq!(
            s.field_with_name("name").unwrap().data_type(),
            &ArrowDataType::Utf8
        );
        assert_eq!(
            s.field_with_name("agg0").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            s.field_with_name("agg1").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            s.field_with_name("agg2").unwrap().data_type(),
            &ArrowDataType::Int32
        );
        assert_eq!(
            s.field_with_name("agg3").unwrap().data_type(),
            &ArrowDataType::Int32
        );
        // Count is non-nullable; sum/min/max are nullable.
        assert!(!s.field_with_name("agg0").unwrap().is_nullable());
        assert!(s.field_with_name("agg1").unwrap().is_nullable());
    }

    /// Sum on a non-numeric source column is a bad spec → ProducerError.
    #[test]
    fn sum_on_text_column_is_rejected() {
        let schema = simple_schema();
        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![agg_on(AggFunc::Sum, "name", "agg0")],
        };
        let result = MergeProducer::with_spec(schema, 1024, ScanSpec::default())
            .unwrap()
            .with_aggregation(&agg);
        match result {
            Err(ProducerError::Aggregation(_)) => {}
            other => panic!("expected Aggregation error, got {:?}", other.map(|_| ())),
        }
    }
}
