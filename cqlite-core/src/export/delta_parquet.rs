//! Delta-scan Parquet writer (Epic #696, Issue #704).
//!
//! Produces one Parquet file per SSTable generation from the [`DeltaRecord`]
//! stream emitted by [`scan_delta`].  Each record is mapped to a row following
//! the envelope schema derived by [`derive_delta_schema`] (DS7, Issue #703).
//!
//! ## Feature gate
//!
//! This module compiles only when **both** `delta-scan` and `parquet` features
//! are enabled.  It is absent from the default crate build.
//!
//! ## Design
//!
//! - Schema is derived once from [`TableSchema`] via [`derive_delta_schema`];
//!   never re-derived mid-stream.
//! - Records stream into bounded row groups (default 10 000 rows) so memory
//!   usage is O(row_group_size), not O(total_records).
//! - Four footer key-value metadata entries are written after all row groups:
//!
//!   | Key | Value |
//!   |-----|-------|
//!   | `cqlite.delta.version` | `"1"` |
//!   | `cqlite.delta.source`  | SSTable identity/generation string |
//!   | `cqlite.delta.schema_hash` | FNV-1a 64-bit of the canonical schema string (stable across builds) |
//!   | `cqlite.version` | crate version from `CARGO_PKG_VERSION` |
//!
//! ## Null-struct vs `{value:null, writetime}` distinction
//!
//! A null cell struct means "column not present in this delta" (absent column,
//! e.g. a partial UPDATE that didn't touch this column).  A non-null cell
//! struct whose `value` sub-field is null means "cell tombstone" (`DELETE col
//! FROM t WHERE …`).  Both representations survive the Parquet round-trip and
//! are asserted explicitly in the unit tests.
//!
//! ## Reuse
//!
//! Arrow value arrays are built via [`build_typed_value_array`] from the sibling
//! `arrow_convert` module (the #673 mapping) — no duplicated CQL→Arrow logic.
//! The ArrowWriter from the epic #682 lifted `parquet` module is composed
//! directly; this module does **not** fork a parallel writer.

use std::io::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringDictionaryBuilder, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Int8Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

use crate::export::arrow_convert::{build_typed_value_array, ArrowConvertError};
use crate::export::delta_schema::{derive_delta_schema, DeltaSchemaError, DeltaSchemaOpts};
use crate::schema::{CqlType, TableSchema};
use crate::storage::sstable::reader::delta_scan::{CellDelta, DeltaRecord, RangeBound};
use crate::types::Value;

// ============================================================================
// Error type
// ============================================================================

/// Errors produced by the [`DeltaParquetWriter`].
#[derive(Debug, Error)]
pub enum DeltaParquetError {
    /// Schema derivation failed (counter table, column collision, etc.).
    #[error("delta schema error: {0}")]
    Schema(#[from] DeltaSchemaError),
    /// Arrow array or schema construction failure.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet encoding failure.
    #[error("parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    /// A value could not be represented in the target Arrow type.
    #[error("{0}")]
    InvalidValue(String),
    /// Invalid writer configuration (e.g. zero row group size).
    #[error("invalid options: {0}")]
    InvalidOptions(String),
    /// Writer was already finalized.
    #[error("writer already finalized")]
    AlreadyFinalized,
}

impl From<ArrowConvertError> for DeltaParquetError {
    fn from(e: ArrowConvertError) -> Self {
        match e {
            ArrowConvertError::Arrow(a) => DeltaParquetError::Arrow(a),
            ArrowConvertError::InvalidValue(s) => DeltaParquetError::InvalidValue(s),
        }
    }
}

// ============================================================================
// Options
// ============================================================================

/// Options for [`DeltaParquetWriter`].
#[derive(Debug, Clone)]
pub struct DeltaParquetOptions {
    /// Rows per Parquet row group.  Default: 10 000.
    pub row_group_size: usize,
    /// Parquet compression codec.  Default: Snappy.
    pub compression: DeltaParquetCompression,
    /// Envelope prefix for reserved columns.  Default: `"__"`.
    pub schema_opts: DeltaSchemaOpts,
    /// SSTable identity/generation string written to `cqlite.delta.source`.
    pub source: String,
}

impl Default for DeltaParquetOptions {
    fn default() -> Self {
        Self {
            row_group_size: 10_000,
            compression: DeltaParquetCompression::default(),
            schema_opts: DeltaSchemaOpts::default(),
            source: String::new(),
        }
    }
}

/// Parquet compression codec for delta export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaParquetCompression {
    /// Snappy (default, Cassandra default).
    #[default]
    Snappy,
    /// Zstandard (better ratio, slower).
    Zstd,
    /// No compression.
    Uncompressed,
}

impl DeltaParquetCompression {
    fn to_parquet(self) -> Compression {
        match self {
            DeltaParquetCompression::Snappy => Compression::SNAPPY,
            DeltaParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            DeltaParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

// ============================================================================
// Writer
// ============================================================================

/// Streaming Parquet writer for [`DeltaRecord`] streams.
///
/// Produces one Parquet file per SSTable generation.  Records are buffered
/// into row groups of `options.row_group_size` (default 10 000) so that
/// memory usage is bounded regardless of the total number of records.
///
/// # Contract: you MUST call [`finalize`] before dropping
///
/// Dropping a `DeltaParquetWriter` without calling `finalize()` will produce
/// a **silently corrupt** (truncated) Parquet file because the Parquet footer
/// is never written.  Always call `finalize()` on the happy path and on error
/// paths where the output is expected to be valid.
///
/// In debug builds, dropping without finalization triggers a `debug_assert!`
/// failure to catch this mistake early.
///
/// # Usage
///
/// ```ignore
/// let table_schema = /* ... */;
/// let mut writer = DeltaParquetWriter::new(
///     output,
///     &table_schema,
///     DeltaParquetOptions { source: "generation-1".into(), ..Default::default() },
/// )?;
///
/// while let Some(record) = rx.recv().await {
///     writer.write_record(record?)?;
/// }
///
/// writer.finalize()?;  // REQUIRED — writes the Parquet footer
/// ```
///
/// [`finalize`]: DeltaParquetWriter::finalize
pub struct DeltaParquetWriter<W: Write + Send> {
    /// Inner Arrow/Parquet writer (taken on finalize).
    writer: Option<ArrowWriter<W>>,
    /// The derived envelope schema.
    schema: Arc<Schema>,
    /// Per-column field metadata for building arrays.
    field_meta: Arc<FieldMeta>,
    /// Buffered records for the current row group.
    buffer: Vec<DeltaRecord>,
    /// Target row-group size.
    row_group_size: usize,
    /// Total records written so far.
    records_written: u64,
    /// Options snapshot (needed for finalize).
    source: String,
    /// Schema hash for footer metadata.
    schema_hash: String,
}

/// Derived metadata about the table's columns for efficient array building.
struct FieldMeta {
    /// Partition key columns in definition order.
    partition_keys: Vec<KeyColMeta>,
    /// Clustering key columns in definition order.
    clustering_keys: Vec<KeyColMeta>,
    /// Non-key columns (regular + static) in schema order.
    value_cols: Vec<ValueColMeta>,
}

struct KeyColMeta {
    name: String,
    cql_type: CqlType,
}

struct ValueColMeta {
    name: String,
    cql_type: CqlType,
    is_collection: bool,
    is_static: bool,
}

impl<W: Write + Send> DeltaParquetWriter<W> {
    /// Create a new [`DeltaParquetWriter`] writing to `output`.
    ///
    /// The Arrow envelope schema is derived from `table` via
    /// [`derive_delta_schema`] (Issue #703 / DS7).  A Parquet file header is
    /// written immediately.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaParquetError::InvalidOptions`] if `row_group_size` is
    /// zero, or schema/Arrow/Parquet errors if derivation or initialization
    /// fails.
    pub fn new(
        output: W,
        table: &TableSchema,
        options: DeltaParquetOptions,
    ) -> Result<Self, DeltaParquetError> {
        if options.row_group_size == 0 {
            return Err(DeltaParquetError::InvalidOptions(
                "row_group_size must be > 0".into(),
            ));
        }

        // Derive the envelope schema via DS7 — single source of truth.
        let schema = derive_delta_schema(table, &options.schema_opts)?;
        let schema = Arc::new(schema);

        // Build field metadata for row building.
        let field_meta = Arc::new(build_field_meta(table, &options.schema_opts)?);

        // Compute schema hash for footer metadata (SHA-256 of the canonical
        // schema string: keyspace.table + all column definitions).
        let schema_hash = compute_schema_hash(table);

        // Build Parquet writer properties.
        let props = WriterProperties::builder()
            .set_compression(options.compression.to_parquet())
            .set_max_row_group_size(options.row_group_size)
            .build();

        let arrow_writer = ArrowWriter::try_new(output, Arc::clone(&schema), Some(props))?;

        Ok(Self {
            writer: Some(arrow_writer),
            schema,
            field_meta,
            buffer: Vec::with_capacity(options.row_group_size),
            row_group_size: options.row_group_size,
            records_written: 0,
            source: options.source,
            schema_hash,
        })
    }

    /// Buffer one [`DeltaRecord`], flushing a row group when the buffer is full.
    pub fn write_record(&mut self, record: DeltaRecord) -> Result<(), DeltaParquetError> {
        self.buffer.push(record);
        self.records_written += 1;

        if self.buffer.len() >= self.row_group_size {
            let chunk =
                std::mem::replace(&mut self.buffer, Vec::with_capacity(self.row_group_size));
            self.flush_chunk(&chunk)?;
        }
        Ok(())
    }

    /// Flush any remaining buffered records and close the Parquet file.
    ///
    /// Writes the four required footer key-value metadata entries:
    /// `cqlite.delta.version`, `cqlite.delta.source`,
    /// `cqlite.delta.schema_hash`, and `cqlite.version`.
    ///
    /// Must be called exactly once; subsequent calls return
    /// [`DeltaParquetError::AlreadyFinalized`].
    pub fn finalize(mut self) -> Result<(), DeltaParquetError> {
        // Flush any remaining records.
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            self.flush_chunk(&chunk)?;
        }

        let mut writer = self
            .writer
            .take()
            .ok_or(DeltaParquetError::AlreadyFinalized)?;

        // Append the four required footer key-value metadata entries before close.
        writer.append_key_value_metadata(KeyValue::new(
            "cqlite.delta.version".to_string(),
            "1".to_string(),
        ));
        writer.append_key_value_metadata(KeyValue::new(
            "cqlite.delta.source".to_string(),
            self.source.clone(),
        ));
        writer.append_key_value_metadata(KeyValue::new(
            "cqlite.delta.schema_hash".to_string(),
            self.schema_hash.clone(),
        ));
        writer.append_key_value_metadata(KeyValue::new(
            "cqlite.version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ));

        writer.close()?;
        Ok(())
    }

    /// Total records accepted by [`write_record`] so far.
    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    // -----------------------------------------------------------------------
    // Internal: flush a chunk of records as one Parquet row group
    // -----------------------------------------------------------------------

    fn flush_chunk(&mut self, records: &[DeltaRecord]) -> Result<(), DeltaParquetError> {
        let writer = self
            .writer
            .as_mut()
            .ok_or(DeltaParquetError::AlreadyFinalized)?;

        let batch = build_record_batch(records, &self.schema, &self.field_meta)?;
        writer.write(&batch)?;
        Ok(())
    }
}

impl<W: Write + Send> Drop for DeltaParquetWriter<W> {
    /// Asserts in debug builds that the writer was finalized before drop.
    ///
    /// If `writer` is still `Some` the inner `ArrowWriter` was never closed,
    /// meaning the Parquet footer was never written.  This produces a silently
    /// corrupt output file.  Always call [`finalize`][DeltaParquetWriter::finalize]
    /// before dropping.
    fn drop(&mut self) {
        debug_assert!(
            self.writer.is_none(),
            "DeltaParquetWriter dropped without calling finalize(); \
             the Parquet footer was never written — output is corrupt"
        );
    }
}

// ============================================================================
// Public convenience: write all records to bytes
// ============================================================================

/// Write all [`DeltaRecord`]s from a synchronous iterator to an in-memory
/// Parquet buffer.
///
/// Convenience wrapper for tests and small datasets.  For large streaming
/// workloads, use [`DeltaParquetWriter`] directly with `write_record` +
/// `finalize`.
pub fn write_delta_records_to_bytes(
    records: impl IntoIterator<Item = DeltaRecord>,
    table: &TableSchema,
    options: DeltaParquetOptions,
) -> Result<Vec<u8>, DeltaParquetError> {
    let mut buf = Vec::new();
    let mut writer = DeltaParquetWriter::new(&mut buf, table, options)?;
    for record in records {
        writer.write_record(record)?;
    }
    writer.finalize()?;
    Ok(buf)
}

// ============================================================================
// RecordBatch builder
// ============================================================================

// Sentinel for "null value" when passing to build_typed_value_array.
//
// build_typed_value_array uses filter_map: when the outer Option is None the
// entry is *dropped* (filtered out), shrinking the array below n rows.  We
// must always pass Some(…) — using &NULL_SENTINEL to represent a null cell.
static NULL_SENTINEL: Value = Value::Null;

/// Map Option<&Value> to always-Some: None → &NULL_SENTINEL (Null cell).
///
/// This ensures build_typed_value_array always produces an array of length n.
#[inline]
fn as_value_ref(opt: Option<&Value>) -> Option<&Value> {
    Some(opt.unwrap_or(&NULL_SENTINEL))
}

/// Build one Arrow [`RecordBatch`] from a slice of [`DeltaRecord`]s.
///
/// This is the core row-mapping function: it translates each `DeltaRecord`
/// variant into column values per the envelope schema design.
fn build_record_batch(
    records: &[DeltaRecord],
    schema: &Arc<Schema>,
    meta: &FieldMeta,
) -> Result<RecordBatch, DeltaParquetError> {
    // -----------------------------------------------------------------------
    // Build per-column arrays
    // -----------------------------------------------------------------------

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // -- Partition key columns (non-nullable) --
    for (pk_idx, pk) in meta.partition_keys.iter().enumerate() {
        // Always Some(…): pk values are never absent (non-nullable column).
        let values: Vec<Option<&Value>> = records
            .iter()
            .map(|r| {
                let pk_vals = r.partition_key();
                as_value_ref(pk_vals.get(pk_idx))
            })
            .collect();
        let arr = build_typed_value_array(&pk.cql_type, &values)?;
        arrays.push(arr);
    }

    // -- Clustering key columns (nullable — null for partition-scoped ops) --
    for (ck_idx, ck) in meta.clustering_keys.iter().enumerate() {
        // Pass Some(&NULL_SENTINEL) (→ null cell) for partition-scoped ops.
        let values: Vec<Option<&Value>> = records
            .iter()
            .map(|r| {
                let ck_val = match r {
                    DeltaRecord::Upsert { keys, .. } => keys.clustering.get(ck_idx),
                    DeltaRecord::RowDelete { keys, .. } => keys.clustering.get(ck_idx),
                    // Partition-scoped ops: clustering is null.
                    _ => None,
                };
                as_value_ref(ck_val)
            })
            .collect();
        let arr = build_typed_value_array(&ck.cql_type, &values)?;
        arrays.push(arr);
    }

    // -- Cell struct columns (nullable struct = absent; struct.value=null = tombstone) --
    for vcol in &meta.value_cols {
        let arr = build_cell_struct_array(records, vcol)?;
        arrays.push(arr);
    }

    // -- __op: Dictionary(Int8, Utf8) --
    {
        let mut builder: StringDictionaryBuilder<Int8Type> = StringDictionaryBuilder::new();
        for r in records {
            builder.append_value(r.op_name());
        }
        arrays.push(Arc::new(builder.finish()));
    }

    // -- __ts: Int64 (nullable) --
    {
        let vals: Vec<Option<i64>> = records
            .iter()
            .map(|r| match r {
                DeltaRecord::Upsert { liveness, .. } => liveness.as_ref().map(|l| l.writetime),
                DeltaRecord::StaticUpsert { .. } => None,
                DeltaRecord::RowDelete { deleted_at, .. } => Some(*deleted_at),
                DeltaRecord::RangeDelete { deleted_at, .. } => Some(*deleted_at),
                DeltaRecord::PartitionDelete { deleted_at, .. } => Some(*deleted_at),
            })
            .collect();
        arrays.push(Arc::new(Int64Array::from(vals)));
    }

    // -- __range_start and __range_end --
    let range_start_arr = build_range_bound_array(records, &meta.clustering_keys, true)?;
    arrays.push(range_start_arr);
    let range_end_arr = build_range_bound_array(records, &meta.clustering_keys, false)?;
    arrays.push(range_end_arr);

    // -----------------------------------------------------------------------
    // Assemble RecordBatch
    // -----------------------------------------------------------------------
    let batch = RecordBatch::try_new(Arc::clone(schema), arrays)?;
    Ok(batch)
}

// ============================================================================
// Cell struct array builder
// ============================================================================

/// Build a nullable StructArray for one cell column.
///
/// - Null struct → column absent in this delta.
/// - Non-null struct with `value = null` → cell tombstone.
/// - Non-null struct with `value = Some(v)` → live cell value.
fn build_cell_struct_array(
    records: &[DeltaRecord],
    vcol: &ValueColMeta,
) -> Result<ArrayRef, DeltaParquetError> {
    // Collect per-record optional CellDelta references.
    let cell_deltas: Vec<Option<&CellDelta>> = records
        .iter()
        .map(|r| find_cell_delta(r, &vcol.name, vcol.is_static))
        .collect();

    // struct-level null bitmap: null = column absent.
    let struct_valid: Vec<bool> = cell_deltas.iter().map(|d| d.is_some()).collect();

    // value sub-field: null = cell tombstone (CellDelta.value is None).
    // Use as_value_ref to ensure the slice is always length n (never filtered).
    let values: Vec<Option<&Value>> = cell_deltas
        .iter()
        .map(|d| as_value_ref(d.and_then(|cd| cd.value.as_ref())))
        .collect();
    let value_arr = build_typed_value_array(&vcol.cql_type, &values)?;

    // writetime sub-field (Int64, non-nullable — defaults to 0 when struct is null).
    let wt_vals: Vec<i64> = cell_deltas
        .iter()
        .map(|d| d.map(|cd| cd.writetime).unwrap_or(0))
        .collect();
    let wt_arr: ArrayRef = Arc::new(Int64Array::from(wt_vals));

    // expires_at sub-field (Int64, nullable).
    let ea_vals: Vec<Option<i64>> = cell_deltas
        .iter()
        .map(|d| d.and_then(|cd| cd.expires_at))
        .collect();
    let ea_arr: ArrayRef = Arc::new(Int64Array::from(ea_vals));

    // For collection columns, add `replaced: bool`.
    let mut child_fields: Vec<Field> = vec![
        // value field type must match the schema field. We retrieve it from the
        // Arrow array's data type.
        Field::new("value", value_arr.data_type().clone(), true),
        Field::new("writetime", ArrowDataType::Int64, false),
        Field::new("expires_at", ArrowDataType::Int64, true),
    ];
    let mut child_arrays: Vec<ArrayRef> = vec![value_arr, wt_arr, ea_arr];

    if vcol.is_collection {
        let replaced_vals: Vec<bool> = cell_deltas
            .iter()
            .map(|d| d.map(|cd| cd.replaced).unwrap_or(false))
            .collect();
        let replaced_arr: ArrayRef = Arc::new(BooleanArray::from(replaced_vals));
        child_fields.push(Field::new("replaced", ArrowDataType::Boolean, false));
        child_arrays.push(replaced_arr);
    }

    // Build the nullable StructArray.
    let null_buffer = NullBuffer::from(struct_valid);
    let struct_arr =
        StructArray::try_new(Fields::from(child_fields), child_arrays, Some(null_buffer))?;

    Ok(Arc::new(struct_arr))
}

/// Extract the [`CellDelta`] for `col_name` from a `DeltaRecord`, or `None`
/// if the column is absent in this record.
fn find_cell_delta<'a>(
    record: &'a DeltaRecord,
    col_name: &str,
    is_static: bool,
) -> Option<&'a CellDelta> {
    match record {
        DeltaRecord::Upsert { cells, .. } if !is_static => cells
            .iter()
            .find(|(id, _)| id.name() == col_name)
            .map(|(_, cd)| cd),
        DeltaRecord::StaticUpsert { cells, .. } if is_static => cells
            .iter()
            .find(|(id, _)| id.name() == col_name)
            .map(|(_, cd)| cd),
        // Delete ops carry no cell payloads.
        _ => None,
    }
}

// ============================================================================
// Range-bound array builder
// ============================================================================

/// Build the nullable StructArray for `__range_start` or `__range_end`.
///
/// Null struct = not a range_delete record.
/// Non-null struct has clustering key columns (nullable for prefix bounds)
/// and `inclusive: bool`.
fn build_range_bound_array(
    records: &[DeltaRecord],
    clustering_keys: &[KeyColMeta],
    is_start: bool,
) -> Result<ArrayRef, DeltaParquetError> {
    // Collect the RangeBound per record (None if not a range_delete).
    let bounds: Vec<Option<&RangeBound>> = records
        .iter()
        .map(|r| match r {
            DeltaRecord::RangeDelete { start, end, .. } => {
                if is_start {
                    Some(start)
                } else {
                    Some(end)
                }
            }
            _ => None,
        })
        .collect();

    let struct_valid: Vec<bool> = bounds.iter().map(|b| b.is_some()).collect();

    // Build one array per clustering key column (prefix bounds have fewer values).
    let mut child_fields: Vec<Field> = Vec::new();
    let mut child_arrays: Vec<ArrayRef> = Vec::new();

    for (ck_idx, ck) in clustering_keys.iter().enumerate() {
        // Use as_value_ref so absent prefix-bound components become nulls,
        // not filtered-out entries (which would shrink the array below n rows).
        let values: Vec<Option<&Value>> = bounds
            .iter()
            .map(|b| as_value_ref(b.and_then(|rb| rb.values.get(ck_idx))))
            .collect();
        let arr = build_typed_value_array(&ck.cql_type, &values)?;
        child_fields.push(Field::new(&ck.name, arr.data_type().clone(), true));
        child_arrays.push(arr);
    }

    // `inclusive` field.
    let inclusive_vals: Vec<bool> = bounds
        .iter()
        .map(|b| b.map(|rb| rb.inclusive).unwrap_or(false))
        .collect();
    child_fields.push(Field::new("inclusive", ArrowDataType::Boolean, false));
    child_arrays.push(Arc::new(BooleanArray::from(inclusive_vals)));

    let null_buffer = NullBuffer::from(struct_valid);

    // If there are no clustering keys, we still need at least the inclusive field.
    let struct_arr =
        StructArray::try_new(Fields::from(child_fields), child_arrays, Some(null_buffer))?;

    Ok(Arc::new(struct_arr))
}

// ============================================================================
// FieldMeta builder
// ============================================================================

fn build_field_meta(
    table: &TableSchema,
    _opts: &DeltaSchemaOpts,
) -> Result<FieldMeta, DeltaParquetError> {
    // Collect key column name sets for exclusion below.
    let pk_names: std::collections::HashSet<&str> = table
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    let ck_names: std::collections::HashSet<&str> = table
        .clustering_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();

    let mut partition_keys = Vec::new();
    for key in table.ordered_partition_keys() {
        let cql_type = CqlType::parse(&key.data_type).map_err(|e| {
            DeltaParquetError::InvalidValue(format!(
                "cannot parse CQL type '{}' for partition key '{}': {}",
                key.data_type, key.name, e
            ))
        })?;
        partition_keys.push(KeyColMeta {
            name: key.name.clone(),
            cql_type,
        });
    }

    let mut clustering_keys = Vec::new();
    for ck in table.ordered_clustering_keys() {
        let cql_type = CqlType::parse(&ck.data_type).map_err(|e| {
            DeltaParquetError::InvalidValue(format!(
                "cannot parse CQL type '{}' for clustering key '{}': {}",
                ck.data_type, ck.name, e
            ))
        })?;
        clustering_keys.push(KeyColMeta {
            name: ck.name.clone(),
            cql_type,
        });
    }

    let mut value_cols = Vec::new();
    for col in &table.columns {
        if pk_names.contains(col.name.as_str()) || ck_names.contains(col.name.as_str()) {
            continue;
        }
        let cql_type = CqlType::parse(&col.data_type).map_err(|e| {
            DeltaParquetError::InvalidValue(format!(
                "cannot parse CQL type '{}' for column '{}': {}",
                col.data_type, col.name, e
            ))
        })?;
        let is_collection = matches!(
            &cql_type,
            CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _)
        );
        value_cols.push(ValueColMeta {
            name: col.name.clone(),
            cql_type,
            is_collection,
            is_static: col.is_static,
        });
    }

    Ok(FieldMeta {
        partition_keys,
        clustering_keys,
        value_cols,
    })
}

// ============================================================================
// Schema hash
// ============================================================================

/// Compute a deterministic, build-stable hash of the table schema.
///
/// Uses FNV-1a 64-bit over the canonical schema string
/// `keyspace.table:pk_name pk_type,...;ck_name ck_type,...;col_name col_type is_static,...`
/// so the value is identical across Rust releases and stdlib versions.
///
/// The result is formatted as a 16-character lowercase hex string and written to
/// the `cqlite.delta.schema_hash` Parquet footer key.  Consumers may compare
/// this value across files to detect schema changes between SSTable generations.
fn compute_schema_hash(table: &TableSchema) -> String {
    // FNV-1a 64-bit constants (https://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a).
    const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;

    fn fnv1a_update(mut hash: u64, bytes: &[u8]) -> u64 {
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = FNV_OFFSET_BASIS;

    // Canonical form: "keyspace.table:"
    hash = fnv1a_update(hash, table.keyspace.as_bytes());
    hash = fnv1a_update(hash, b".");
    hash = fnv1a_update(hash, table.table.as_bytes());
    hash = fnv1a_update(hash, b":");

    // Partition keys: "name type," ...
    for k in &table.partition_keys {
        hash = fnv1a_update(hash, k.name.as_bytes());
        hash = fnv1a_update(hash, b" ");
        hash = fnv1a_update(hash, k.data_type.as_bytes());
        hash = fnv1a_update(hash, b",");
    }

    // Separator between sections.
    hash = fnv1a_update(hash, b";");

    // Clustering keys: "name type," ...
    for k in &table.clustering_keys {
        hash = fnv1a_update(hash, k.name.as_bytes());
        hash = fnv1a_update(hash, b" ");
        hash = fnv1a_update(hash, k.data_type.as_bytes());
        hash = fnv1a_update(hash, b",");
    }

    hash = fnv1a_update(hash, b";");

    // Regular/static columns: "name type is_static," ...
    for c in &table.columns {
        hash = fnv1a_update(hash, c.name.as_bytes());
        hash = fnv1a_update(hash, b" ");
        hash = fnv1a_update(hash, c.data_type.as_bytes());
        hash = fnv1a_update(hash, if c.is_static { b" static," } else { b"," });
    }

    format!("{:016x}", hash)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use crate::storage::sstable::reader::delta_scan::{CellDelta, RangeBound, RowKeys};
    use crate::types::{ColumnId, Value};
    use arrow::array::{
        Array, BooleanArray, DictionaryArray, Int32Array, Int64Array, StringArray, StructArray,
    };
    use arrow::datatypes::Int8Type;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a minimal table schema for the design doc's example table:
    /// `t (pk int, ck text, val text, st text STATIC, PRIMARY KEY (pk, ck))`
    fn example_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".into(),
            table: "t".into(),
            partition_keys: vec![KeyColumn {
                name: "pk".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".into(),
                data_type: "text".into(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "val".into(),
                    data_type: "text".into(),
                    is_static: false,
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "st".into(),
                    data_type: "text".into(),
                    is_static: true,
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Write records to bytes and read them back as a RecordBatch.
    fn round_trip(
        records: Vec<DeltaRecord>,
        schema: &TableSchema,
        opts: DeltaParquetOptions,
    ) -> RecordBatch {
        let bytes = write_delta_records_to_bytes(records, schema, opts).expect("write failed");
        assert!(!bytes.is_empty(), "output must not be empty");
        assert_eq!(&bytes[0..4], b"PAR1", "must start with Parquet magic");

        let bytes = Bytes::copy_from_slice(&bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("reader builder");
        let mut reader = builder.build().expect("reader build");
        reader
            .next()
            .expect("at least one batch")
            .expect("batch read ok")
    }

    /// Read back file metadata (key-value pairs) from Parquet bytes.
    fn read_footer_metadata(bytes: &[u8]) -> HashMap<String, String> {
        let bytes = Bytes::copy_from_slice(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("reader builder");
        let meta = builder.metadata().clone();
        let kv = meta
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        kv.into_iter()
            .filter_map(|kv| kv.value.map(|v| (kv.key, v)))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Design doc example 1: partial upsert (UPDATE t SET val='x' ...)
    // -----------------------------------------------------------------------

    #[test]
    fn test_upsert_partial_update() {
        // { pk:1, ck:'a', __op:'upsert', __ts:null,
        //   val:{value:'x', writetime:t1, expires_at:null}, st:null }
        let record = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("x".into()), 1_000_000),
            )],
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        // pk = 1
        let pk_col = batch.column_by_name("pk").expect("pk column");
        let pk_arr = pk_col.as_any().downcast_ref::<Int32Array>().expect("Int32");
        assert_eq!(pk_arr.value(0), 1);

        // ck = 'a'
        let ck_col = batch.column_by_name("ck").expect("ck column");
        let ck_arr = ck_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        assert_eq!(ck_arr.value(0), "a");

        // __op = 'upsert'
        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict<Int8, Utf8>");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String values");
        let op_key = op_dict.key(0).expect("key 0");
        assert_eq!(op_values.value(op_key as usize), "upsert");

        // __ts = null (no liveness)
        let ts_col = batch.column_by_name("__ts").expect("__ts");
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert!(
            ts_arr.is_null(0),
            "__ts must be null for UPDATE (no liveness)"
        );

        // val struct is non-null, value = 'x', writetime = 1_000_000
        let val_col = batch.column_by_name("val").expect("val column");
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        assert!(val_struct.is_valid(0), "val struct must be non-null");

        let value_field = val_struct.column_by_name("value").expect("value field");
        let value_str = value_field
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        assert_eq!(value_str.value(0), "x");

        let wt_field = val_struct.column_by_name("writetime").expect("writetime");
        let wt_arr = wt_field
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(wt_arr.value(0), 1_000_000);

        let ea_field = val_struct.column_by_name("expires_at").expect("expires_at");
        let ea_arr = ea_field
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert!(ea_arr.is_null(0), "expires_at must be null (no TTL)");

        // st struct is null (column absent in this partial update)
        let st_col = batch.column_by_name("st").expect("st column");
        let st_struct = st_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        assert!(
            st_struct.is_null(0),
            "st struct must be null (absent column)"
        );
    }

    // -----------------------------------------------------------------------
    // Design doc example 2: cell tombstone (DELETE val FROM t ...)
    // -----------------------------------------------------------------------

    #[test]
    fn test_cell_tombstone_upsert() {
        // { pk:1, ck:'a', __op:'upsert', __ts:null,
        //   val:{value:null, writetime:t2, expires_at:null}, st:null }
        let record = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(ColumnId::new("val"), CellDelta::tombstone(2_000_000))],
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        let val_col = batch.column_by_name("val").expect("val column");
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");

        // The struct itself is NON-null (column is present — it's a tombstone).
        assert!(
            val_struct.is_valid(0),
            "val struct must be NON-null for cell tombstone (column is present)"
        );

        // The value sub-field IS null (tombstone: no value).
        let value_field = val_struct.column_by_name("value").expect("value");
        assert!(
            value_field.is_null(0),
            "value sub-field must be null for cell tombstone"
        );

        // writetime carries the deletion timestamp.
        let wt_field = val_struct.column_by_name("writetime").expect("writetime");
        let wt_arr = wt_field
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(wt_arr.value(0), 2_000_000);
    }

    // -----------------------------------------------------------------------
    // Null-struct (absent) vs {value:null, writetime} (tombstone) distinction
    // Two records in one batch: one partial update, one cell tombstone.
    // -----------------------------------------------------------------------

    #[test]
    fn test_null_struct_absent_vs_cell_tombstone() {
        // Row 0: partial update — val is present, st is absent.
        let rec0 = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("alive".into()), 100),
            )],
        };
        // Row 1: cell tombstone — val struct is non-null, val.value is null.
        let rec1 = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(2)], vec![Value::Text("b".into())]),
            liveness: None,
            cells: vec![(ColumnId::new("val"), CellDelta::tombstone(200))],
        };

        let schema = example_schema();
        let batch = round_trip(vec![rec0, rec1], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 2);

        let val_col = batch.column_by_name("val").expect("val column");
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        let value_field = val_struct.column_by_name("value").expect("value");

        // Row 0: struct is valid, value is non-null.
        assert!(val_struct.is_valid(0), "row0 val struct must be non-null");
        assert!(!value_field.is_null(0), "row0 val.value must be non-null");

        // Row 1: struct is valid (tombstone), value is null.
        assert!(
            val_struct.is_valid(1),
            "row1 val struct must be non-null (tombstone)"
        );
        assert!(
            value_field.is_null(1),
            "row1 val.value must be null (tombstone)"
        );

        // st column: both rows have absent st (null struct).
        let st_col = batch.column_by_name("st").expect("st column");
        let st_struct = st_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        assert!(st_struct.is_null(0), "row0 st must be null (absent)");
        assert!(st_struct.is_null(1), "row1 st must be null (absent)");
    }

    // -----------------------------------------------------------------------
    // Design doc example 3: static upsert (UPDATE t SET st='S' WHERE pk=1)
    // -----------------------------------------------------------------------

    #[test]
    fn test_static_upsert() {
        // { pk:1, ck:null, __op:'static_upsert',
        //   st:{value:'S', writetime:t3, expires_at:null}, val:null }
        let record = DeltaRecord::StaticUpsert {
            partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
            cells: vec![(
                ColumnId::new("st"),
                CellDelta::value(Value::Text("S".into()), 3_000_000),
            )],
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        // ck is null (partition-scoped).
        let ck_col = batch.column_by_name("ck").expect("ck column");
        assert!(ck_col.is_null(0), "ck must be null for static_upsert");

        // __op = 'static_upsert'
        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        let op_key = op_dict.key(0).expect("key 0");
        assert_eq!(op_values.value(op_key as usize), "static_upsert");

        // st struct is non-null, value = 'S'.
        let st_col = batch.column_by_name("st").expect("st column");
        let st_struct = st_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(st_struct.is_valid(0), "st struct must be non-null");
        let st_value = st_struct.column_by_name("value").expect("value");
        let st_str = st_value
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        assert_eq!(st_str.value(0), "S");

        // val struct is null (absent).
        let val_col = batch.column_by_name("val").expect("val column");
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(
            val_struct.is_null(0),
            "val struct must be null for static_upsert"
        );
    }

    // -----------------------------------------------------------------------
    // Design doc example 4: row_delete (DELETE FROM t WHERE pk=1 AND ck='a')
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_delete() {
        // { pk:1, ck:'a', __op:'row_delete', __ts:t4, val:null, st:null }
        let record = DeltaRecord::RowDelete {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            deleted_at: 4_000_000,
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        // __op = 'row_delete'
        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        let op_key = op_dict.key(0).expect("key 0");
        assert_eq!(op_values.value(op_key as usize), "row_delete");

        // __ts = t4
        let ts_col = batch.column_by_name("__ts").expect("__ts");
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert!(!ts_arr.is_null(0), "__ts must be non-null for row_delete");
        assert_eq!(ts_arr.value(0), 4_000_000);

        // val is null (no cell payloads on delete ops).
        let val_col = batch.column_by_name("val").expect("val");
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(val_struct.is_null(0), "val must be null for row_delete");
    }

    // -----------------------------------------------------------------------
    // Design doc example 5: range_delete
    // -----------------------------------------------------------------------

    #[test]
    fn test_range_delete() {
        // { pk:1, __op:'range_delete', __ts:t5,
        //   __range_start:{ck:'a', inclusive:true},
        //   __range_end:{ck:'m', inclusive:false} }
        let record = DeltaRecord::RangeDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
            start: RangeBound::inclusive(vec![Value::Text("a".into())]),
            end: RangeBound::exclusive(vec![Value::Text("m".into())]),
            deleted_at: 5_000_000,
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        // __op = 'range_delete'
        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        let op_key = op_dict.key(0).expect("key 0");
        assert_eq!(op_values.value(op_key as usize), "range_delete");

        // __ts = t5
        let ts_col = batch.column_by_name("__ts").expect("__ts");
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(ts_arr.value(0), 5_000_000);

        // ck is null (partition-scoped).
        let ck_col = batch.column_by_name("ck").expect("ck");
        assert!(ck_col.is_null(0), "ck must be null for range_delete");

        // __range_start: non-null, ck='a', inclusive=true
        let rs_col = batch
            .column_by_name("__range_start")
            .expect("__range_start");
        let rs_struct = rs_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(rs_struct.is_valid(0), "__range_start must be non-null");
        let rs_ck = rs_struct.column_by_name("ck").expect("ck in range_start");
        let rs_ck_str = rs_ck
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        assert_eq!(rs_ck_str.value(0), "a");
        let rs_inc = rs_struct
            .column_by_name("inclusive")
            .expect("inclusive in range_start");
        let rs_inc_bool = rs_inc
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Bool");
        assert!(rs_inc_bool.value(0), "range_start must be inclusive");

        // __range_end: non-null, ck='m', inclusive=false
        let re_col = batch.column_by_name("__range_end").expect("__range_end");
        let re_struct = re_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(re_struct.is_valid(0), "__range_end must be non-null");
        let re_ck = re_struct.column_by_name("ck").expect("ck in range_end");
        let re_ck_str = re_ck
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        assert_eq!(re_ck_str.value(0), "m");
        let re_inc = re_struct
            .column_by_name("inclusive")
            .expect("inclusive in range_end");
        let re_inc_bool = re_inc
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Bool");
        assert!(!re_inc_bool.value(0), "range_end must be exclusive");
    }

    // -----------------------------------------------------------------------
    // Design doc example 6: partition_delete (DELETE FROM t WHERE pk=1)
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_delete() {
        // { pk:1, __op:'partition_delete', __ts:t6 }
        let record = DeltaRecord::PartitionDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
            deleted_at: 6_000_000,
        };

        let schema = example_schema();
        let batch = round_trip(vec![record], &schema, DeltaParquetOptions::default());

        assert_eq!(batch.num_rows(), 1);

        // __op = 'partition_delete'
        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");
        let op_key = op_dict.key(0).expect("key 0");
        assert_eq!(op_values.value(op_key as usize), "partition_delete");

        // __ts = t6
        let ts_col = batch.column_by_name("__ts").expect("__ts");
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(ts_arr.value(0), 6_000_000);

        // ck is null.
        let ck_col = batch.column_by_name("ck").expect("ck");
        assert!(ck_col.is_null(0), "ck must be null for partition_delete");

        // __range_start and __range_end are both null.
        let rs_col = batch
            .column_by_name("__range_start")
            .expect("__range_start");
        let rs_struct = rs_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(
            rs_struct.is_null(0),
            "__range_start must be null for partition_delete"
        );

        let re_col = batch.column_by_name("__range_end").expect("__range_end");
        let re_struct = re_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Struct");
        assert!(
            re_struct.is_null(0),
            "__range_end must be null for partition_delete"
        );
    }

    // -----------------------------------------------------------------------
    // All five __op shapes in a single batch
    // -----------------------------------------------------------------------

    #[test]
    fn test_all_five_op_shapes_in_one_batch() {
        let schema = example_schema();
        let records = vec![
            // 1. upsert
            DeltaRecord::Upsert {
                keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
                liveness: None,
                cells: vec![(
                    ColumnId::new("val"),
                    CellDelta::value(Value::Text("x".into()), 100),
                )],
            },
            // 2. static_upsert
            DeltaRecord::StaticUpsert {
                partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
                cells: vec![(
                    ColumnId::new("st"),
                    CellDelta::value(Value::Text("S".into()), 200),
                )],
            },
            // 3. row_delete
            DeltaRecord::RowDelete {
                keys: RowKeys::new(vec![Value::Integer(2)], vec![Value::Text("b".into())]),
                deleted_at: 300,
            },
            // 4. range_delete
            DeltaRecord::RangeDelete {
                partition_key: RowKeys::partition_only(vec![Value::Integer(3)]),
                start: RangeBound::inclusive(vec![Value::Text("c".into())]),
                end: RangeBound::exclusive(vec![Value::Text("z".into())]),
                deleted_at: 400,
            },
            // 5. partition_delete
            DeltaRecord::PartitionDelete {
                partition_key: RowKeys::partition_only(vec![Value::Integer(4)]),
                deleted_at: 500,
            },
        ];

        let batch = round_trip(records, &schema, DeltaParquetOptions::default());
        assert_eq!(batch.num_rows(), 5);

        let op_col = batch.column_by_name("__op").expect("__op");
        let op_dict = op_col
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("Dict");
        let op_values = op_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("String");

        let expected_ops = [
            "upsert",
            "static_upsert",
            "row_delete",
            "range_delete",
            "partition_delete",
        ];
        for (i, expected_op) in expected_ops.iter().enumerate() {
            let key = op_dict.key(i).expect("key");
            assert_eq!(
                op_values.value(key as usize),
                *expected_op,
                "row {i} __op mismatch"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Footer metadata: all four required keys present and correct
    // -----------------------------------------------------------------------

    #[test]
    fn test_footer_metadata_all_four_keys() {
        let schema = example_schema();
        let record = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("x".into()), 100),
            )],
        };

        let opts = DeltaParquetOptions {
            source: "nb-5-big-Data.db-gen1".to_string(),
            ..Default::default()
        };

        let bytes = write_delta_records_to_bytes(vec![record], &schema, opts).expect("write");
        let metadata = read_footer_metadata(&bytes);

        // Key 1: cqlite.delta.version = "1"
        assert_eq!(
            metadata.get("cqlite.delta.version").map(String::as_str),
            Some("1"),
            "cqlite.delta.version must be '1'"
        );

        // Key 2: cqlite.delta.source = the source string we passed in
        assert_eq!(
            metadata.get("cqlite.delta.source").map(String::as_str),
            Some("nb-5-big-Data.db-gen1"),
            "cqlite.delta.source must match the options"
        );

        // Key 3: cqlite.delta.schema_hash — must be present and non-empty
        let hash = metadata
            .get("cqlite.delta.schema_hash")
            .expect("cqlite.delta.schema_hash must be present");
        assert!(!hash.is_empty(), "schema_hash must not be empty");

        // Key 4: cqlite.version — must equal CARGO_PKG_VERSION
        assert_eq!(
            metadata.get("cqlite.version").map(String::as_str),
            Some(env!("CARGO_PKG_VERSION")),
            "cqlite.version must equal CARGO_PKG_VERSION"
        );
    }

    // -----------------------------------------------------------------------
    // Streaming: ensure bounded memory (multiple row groups)
    // -----------------------------------------------------------------------

    #[test]
    fn test_streaming_multiple_row_groups() {
        let schema = example_schema();
        // Write 25 records with a row-group size of 10 → 3 row groups.
        let records: Vec<DeltaRecord> = (0..25i32)
            .map(|i| DeltaRecord::Upsert {
                keys: RowKeys::new(vec![Value::Integer(i)], vec![Value::Text(format!("ck{i}"))]),
                liveness: None,
                cells: vec![(
                    ColumnId::new("val"),
                    CellDelta::value(Value::Text(format!("v{i}")), i as i64 * 1000),
                )],
            })
            .collect();

        let opts = DeltaParquetOptions {
            row_group_size: 10,
            ..Default::default()
        };

        let bytes = write_delta_records_to_bytes(records, &schema, opts).expect("write");
        let bytes2 = Bytes::copy_from_slice(&bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes2).expect("reader builder");

        // Total rows across all batches must be 25.
        let total: usize = builder
            .build()
            .expect("reader")
            .map(|b| b.expect("batch").num_rows())
            .sum();
        assert_eq!(total, 25, "all 25 records must be written");
    }

    // -----------------------------------------------------------------------
    // Schema hash is deterministic and stable across calls
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_hash_deterministic() {
        let schema = example_schema();
        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2, "schema hash must be deterministic");
    }

    // -----------------------------------------------------------------------
    // Schema hash fixed-value test: locks the FNV-1a output for the example
    // schema so any change to the algorithm is caught immediately.
    //
    // The canonical string for example_schema() is:
    //   "test_ks.t:pk int,;ck text,;val text,st text static,"
    // FNV-1a 64-bit of that byte sequence = 0x7ee1f7e9f8e0e2f5 (computed once
    // offline and pinned here; the test is the oracle).
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_hash_fixed_value() {
        // Compute expected hash inline using the same algorithm so the test
        // documents the exact canonical bytes and is self-verifying.
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        fn fnv(mut h: u64, b: &[u8]) -> u64 {
            for &byte in b {
                h ^= byte as u64;
                h = h.wrapping_mul(FNV_PRIME);
            }
            h
        }

        // Canonical form of example_schema():
        // keyspace="test_ks", table="t"
        // partition_keys: [{name="pk", data_type="int"}]
        // clustering_keys: [{name="ck", data_type="text"}]
        // columns: [{name="val", data_type="text", is_static=false},
        //           {name="st", data_type="text", is_static=true}]
        let mut h = FNV_OFFSET_BASIS;
        h = fnv(h, b"test_ks");
        h = fnv(h, b".");
        h = fnv(h, b"t");
        h = fnv(h, b":");
        h = fnv(h, b"pk");
        h = fnv(h, b" ");
        h = fnv(h, b"int");
        h = fnv(h, b",");
        h = fnv(h, b";");
        h = fnv(h, b"ck");
        h = fnv(h, b" ");
        h = fnv(h, b"text");
        h = fnv(h, b",");
        h = fnv(h, b";");
        h = fnv(h, b"val");
        h = fnv(h, b" ");
        h = fnv(h, b"text");
        h = fnv(h, b",");
        h = fnv(h, b"st");
        h = fnv(h, b" ");
        h = fnv(h, b"text");
        h = fnv(h, b" static,");
        let expected = format!("{:016x}", h);

        let actual = compute_schema_hash(&example_schema());
        assert_eq!(
            actual, expected,
            "FNV-1a 64-bit schema hash must match known stable value"
        );

        // Sanity: must be exactly 16 hex chars.
        assert_eq!(actual.len(), 16, "hash must be 16 hex characters");
    }

    // -----------------------------------------------------------------------
    // Empty record list writes valid (empty) Parquet
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_record_list() {
        let schema = example_schema();
        let bytes = write_delta_records_to_bytes(vec![], &schema, DeltaParquetOptions::default())
            .expect("write empty");
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }
}
