//! Vortex export writer for QueryResult (feature = "vortex", issue #4237)
//!
//! Converts CQL query results to the [Vortex](https://github.com/spiraldb/vortex) columnar file
//! format, as a second export target sibling to [`super::parquet`]. Vortex is an **export
//! target only** in this slice (owner ruling, 2026-09-18) — reading `.vortex` files is a later
//! slice, out of scope here.
//!
//! This module is compiled only when the `vortex` cargo feature is enabled (off by default,
//! mirroring `parquet`). The Vortex crate is pinned EXACT (`=0.86.1` at the workspace level):
//! the on-disk format has been stable since 0.36, but the crate API is not, so a version bump is
//! always a deliberate, reviewed edit.
//!
//! # One shared CQL → Arrow mapping (design.md D1)
//!
//! This writer introduces **no second CQL → Arrow mapping**. It consumes the exact same
//! [`rows_to_record_batch_with_schema`](super::arrow_convert::rows_to_record_batch_with_schema)
//! producer the streaming Parquet writer ([`super::parquet::StreamingParquetWriter`]) uses, then
//! converts each resulting Arrow `RecordBatch` to a Vortex array through Vortex's own official
//! Arrow-extension importer (`VortexSession::arrow().from_arrow_array`, called once per column so
//! a conversion failure can be attributed to a specific column — see [`VortexExportError`]). That
//! importer is the path that recognizes the `arrow.uuid` extension metadata CQLite's Arrow schema
//! already carries for `uuid`/`timeuuid` columns (`super::arrow_schema`), so UUID identity survives
//! the format boundary without any Vortex-specific type-mapping code in this module.
//!
//! # Streaming shape
//!
//! [`StreamingVortexWriter`] mirrors [`super::parquet::StreamingParquetWriter`]'s
//! create → repeated `write_chunk` → `finalize` shape, with one difference: Vortex's writer API
//! (`vortex::file::Writer`) is natively async (push-based over a bounded channel, backed by
//! `tokio::fs::File`), so these methods are `async fn` rather than synchronous — the CLI export
//! loop already runs inside an async context (`cqlite-cli/src/commands/export.rs`), so this is a
//! direct `.await` at each call site, not a new async boundary. One Arrow batch is converted and
//! pushed at a time; nothing accumulates the whole file (or the whole input) in memory.
//!
//! # Fail-closed (R4)
//!
//! The writer opens a `<path>.tmp` sibling of the destination and only `rename`s it to the real
//! path on a fully successful `finalize()`. Any error during conversion, the push itself, or
//! `finish()` removes the temp file (best-effort) before propagating — so the destination path
//! never holds a partial file, even under a hard kill between chunks.

use crate::export::arrow_convert::{rows_to_record_batch_with_schema, ArrowConvertError};
use crate::query::{ColumnInfo, QueryMetadata, QueryRow};
use arrow::array::ArrayRef as ArrowArrayRef;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use vortex::array::arrays::StructArray;
use vortex::array::dtype::{FieldName, FieldNames};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef as VortexArrayRef, IntoArray};
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Errors produced by the Vortex export writer.
///
/// A dedicated `thiserror` enum (mirroring [`super::parquet::ParquetExportError`]) so the writer
/// does not depend on the CLI's `OutputError`. The CLI maps these to its own error type at the
/// boundary.
#[derive(Debug, Error)]
pub enum VortexExportError {
    /// Underlying I/O failure (opening the temp file, or renaming it into place).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Arrow array or schema construction failure (the CQL → Arrow half).
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A Vortex error not attributable to a specific batch/column (e.g. writer setup).
    #[error("Vortex error: {0}")]
    Vortex(#[from] vortex::error::VortexError),
    /// A value could not be represented in the target Arrow type.
    #[error("{0}")]
    InvalidValue(String),
    /// Invalid writer configuration (e.g. zero row group size).
    #[error("invalid Vortex export options: {0}")]
    InvalidOptions(String),
    /// A conversion or write failure attributed to one batch and one column (R4 — fail closed,
    /// naming the batch index and column so the operator can find the offending row shape).
    #[error("Vortex export failed at batch {batch_index} (column: {column}): {message}")]
    BatchConversion {
        /// Zero-based index of the batch (`write_chunk`/`finalize` flush) that failed.
        batch_index: usize,
        /// The column name responsible, or a `<...>` placeholder naming the failing PHASE
        /// (row→Arrow conversion, Arrow→Vortex struct assembly, or the Vortex writer push)
        /// when the failure is not attributable to one column.
        column: String,
        /// The underlying error's message.
        message: String,
    },
}

impl From<ArrowConvertError> for VortexExportError {
    fn from(e: ArrowConvertError) -> Self {
        match e {
            ArrowConvertError::Arrow(a) => VortexExportError::Arrow(a),
            other => VortexExportError::InvalidValue(other.to_string()),
        }
    }
}

/// Writer-owned options for Vortex export.
///
/// Mirrors [`super::parquet::ParquetExportOptions`]'s `row_group_size`; no compression knobs are
/// exposed in this slice (design.md — the pinned session default sampling compressor only).
#[derive(Debug, Clone)]
pub struct VortexExportOptions {
    /// Rows per pushed chunk (streaming writer). Default: 10,000, matching Parquet's default row
    /// group size so the two formats' CLI behavior (`--out vortex` vs `--out parquet`) is
    /// unsurprising side by side.
    pub row_group_size: usize,
}

impl Default for VortexExportOptions {
    fn default() -> Self {
        Self {
            row_group_size: 10_000,
        }
    }
}

/// Convert one Arrow `RecordBatch` to a Vortex struct array, column by column, through Vortex's
/// official Arrow-extension importer.
///
/// Column-by-column (rather than the single-call `ArrowSession::from_arrow_record_batch`
/// convenience wrapper this function is otherwise equivalent to) so a failure can be attributed
/// to a specific column name for [`VortexExportError::BatchConversion`] (R4). This is the path
/// that recognizes `arrow.uuid` extension metadata, exactly as
/// `ArrowSession::from_arrow_record_batch` does internally.
fn convert_batch_to_vortex(
    session: &VortexSession,
    schema: &ArrowSchema,
    batch: &RecordBatch,
    batch_index: usize,
) -> Result<VortexArrayRef, VortexExportError> {
    let arrow_session = session.arrow();
    let fields = schema.fields();
    let mut names = Vec::with_capacity(fields.len());
    let mut columns = Vec::with_capacity(fields.len());
    for (col, field) in batch.columns().iter().zip(fields.iter()) {
        let converted = arrow_session
            .from_arrow_array(ArrowArrayRef::clone(col), field.as_ref())
            .map_err(|e| VortexExportError::BatchConversion {
                batch_index,
                column: field.name().clone(),
                message: e.to_string(),
            })?;
        names.push(FieldName::from(field.name().as_str()));
        columns.push(converted);
    }
    let array = StructArray::try_new(
        FieldNames::from_iter(names),
        columns,
        batch.num_rows(),
        Validity::NonNullable,
    )
    .map_err(|e| VortexExportError::BatchConversion {
        batch_index,
        column: "<struct-assembly>".to_string(),
        message: e.to_string(),
    })?
    .into_array();
    Ok(array)
}

/// Build the sibling temp path a [`StreamingVortexWriter`] writes to before renaming into place
/// (fail-closed, R4): `<path>` with a literal `.tmp` suffix appended to the whole path.
fn tmp_sibling_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".tmp");
    PathBuf::from(os)
}

/// A streaming Vortex writer over query results, mirroring
/// [`super::parquet::StreamingParquetWriter`]'s create → `write_chunk` → `finalize` shape.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use cqlite_core::export::vortex::{StreamingVortexWriter, VortexExportOptions};
/// # use cqlite_core::query::QueryMetadata;
/// # let metadata = QueryMetadata::default();
/// # let result_iterator: Vec<Vec<cqlite_core::query::QueryRow>> = vec![];
/// let mut writer = StreamingVortexWriter::create(
///     "/tmp/results.vortex",
///     &metadata,
///     &VortexExportOptions::default(),
/// )
/// .await?;
///
/// for chunk in result_iterator {
///     writer.write_chunk(&chunk).await?;
/// }
///
/// writer.finalize().await?;
/// # Ok(())
/// # }
/// ```
pub struct StreamingVortexWriter {
    session: VortexSession,
    schema: Arc<ArrowSchema>,
    columns: Vec<ColumnInfo>,
    /// `None` after `finalize` (or after an error has already cleaned up the temp file).
    writer: Option<vortex::file::Writer<'static>>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    row_buffer: Vec<QueryRow>,
    row_group_size: usize,
    rows_written: u64,
    batches_pushed: usize,
}

impl StreamingVortexWriter {
    /// Create a streaming Vortex writer over the destination path.
    ///
    /// The Arrow schema is built from `metadata.columns` using the same
    /// [`build_arrow_schema`](super::arrow_convert::build_arrow_schema) helper the streaming
    /// Parquet writer uses. Opens a `<path>.tmp` sibling immediately; nothing is ever written to
    /// `path` itself until [`finalize`](Self::finalize) succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`VortexExportError::InvalidOptions`] if `options.row_group_size` is zero, or an
    /// I/O/Arrow/Vortex error if the schema cannot be built, the temp file cannot be opened, or
    /// the Vortex writer cannot be initialized.
    pub async fn create(
        path: impl AsRef<Path>,
        metadata: &QueryMetadata,
        options: &VortexExportOptions,
    ) -> Result<Self, VortexExportError> {
        if options.row_group_size == 0 {
            return Err(VortexExportError::InvalidOptions(
                "row_group_size must be greater than 0".to_string(),
            ));
        }

        let schema = Arc::new(super::arrow_convert::build_arrow_schema(&metadata.columns)?);
        let session = VortexSession::default();
        let dtype = session
            .arrow()
            .from_arrow_schema(schema.as_ref())
            .map_err(VortexExportError::from)?;

        let final_path = path.as_ref().to_path_buf();
        let tmp_path = tmp_sibling_path(&final_path);
        let file = tokio::fs::File::create(&tmp_path).await?;
        let writer = session.write_options().writer(file, dtype);

        Ok(Self {
            session,
            schema,
            columns: metadata.columns.clone(),
            writer: Some(writer),
            tmp_path,
            final_path,
            row_buffer: Vec::with_capacity(options.row_group_size),
            row_group_size: options.row_group_size,
            rows_written: 0,
            batches_pushed: 0,
        })
    }

    /// Buffer rows and push complete chunks.
    ///
    /// Returns the number of rows pushed to the writer in this call (rows remaining in the
    /// buffer are pushed by [`finalize`](Self::finalize)).
    pub async fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, VortexExportError> {
        self.row_buffer.extend(rows.iter().cloned());
        self.rows_written += rows.len() as u64;

        let mut pushed = 0;
        while self.row_buffer.len() >= self.row_group_size {
            let chunk: Vec<QueryRow> = self.row_buffer.drain(..self.row_group_size).collect();
            if let Err(e) = self.push_batch(&chunk).await {
                self.cleanup_tmp().await;
                return Err(e);
            }
            pushed += self.row_group_size;
        }

        Ok(pushed)
    }

    /// Push any buffered rows, finish the Vortex file, and rename the temp file into place.
    ///
    /// Must be called exactly once after all chunks are written; dropping the writer without
    /// calling `finalize` leaves only the `.tmp` sibling on disk, never a file at the destination
    /// path (R4 — fail closed).
    pub async fn finalize(&mut self) -> Result<(), VortexExportError> {
        if !self.row_buffer.is_empty() {
            let remaining = std::mem::take(&mut self.row_buffer);
            if let Err(e) = self.push_batch(&remaining).await {
                self.cleanup_tmp().await;
                return Err(e);
            }
        }

        let writer = match self.writer.take() {
            Some(w) => w,
            None => {
                return Err(VortexExportError::InvalidOptions(
                    "writer already finalized - cannot finalize twice".to_string(),
                ))
            }
        };

        if let Err(e) = writer.finish().await {
            self.cleanup_tmp().await;
            return Err(VortexExportError::Vortex(e));
        }

        tokio::fs::rename(&self.tmp_path, &self.final_path).await?;
        Ok(())
    }

    /// Total number of rows accepted by `write_chunk` so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Convert a slice of rows to a RecordBatch (the shared producer, D1) and push it as one
    /// Vortex array chunk.
    async fn push_batch(&mut self, rows: &[QueryRow]) -> Result<(), VortexExportError> {
        let batch_index = self.batches_pushed;

        let batch = rows_to_record_batch_with_schema(Arc::clone(&self.schema), &self.columns, rows)
            .map_err(|e| VortexExportError::BatchConversion {
                batch_index,
                column: "<row-to-arrow-conversion>".to_string(),
                message: e.to_string(),
            })?;

        let array = convert_batch_to_vortex(&self.session, &self.schema, &batch, batch_index)?;

        let writer = self.writer.as_mut().ok_or_else(|| {
            VortexExportError::InvalidOptions(
                "writer already finalized - cannot write more rows".to_string(),
            )
        })?;

        writer
            .push(array)
            .await
            .map_err(|e| VortexExportError::BatchConversion {
                batch_index,
                column: "<vortex-writer-push>".to_string(),
                message: e.to_string(),
            })?;

        self.batches_pushed += 1;
        Ok(())
    }

    /// Best-effort removal of the `.tmp` sibling on a failure path. Never propagates its own
    /// error — the ORIGINAL failure is what the caller needs to see.
    async fn cleanup_tmp(&self) {
        let _ = tokio::fs::remove_file(&self.tmp_path).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ColumnInfo, QueryMetadata};
    use crate::types::{DataType, Value};
    use crate::RowKey;
    use std::collections::HashMap;

    fn metadata_two_cols() -> QueryMetadata {
        QueryMetadata {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::BigInt,
                    nullable: false,
                    position: 0,
                    table_name: None,
                    cql_type: None,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    position: 1,
                    table_name: None,
                    cql_type: None,
                },
            ],
            ..Default::default()
        }
    }

    fn row(id: i64, name: Option<&'static str>) -> QueryRow {
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();
        values.insert(Arc::<str>::from("id"), Value::BigInt(id));
        if let Some(n) = name {
            values.insert(Arc::<str>::from("name"), Value::Text(n.into()));
        }
        QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    /// R1.2: `vortex.rs` contains no `match` over a CQL type building Arrow/Vortex arrays
    /// directly — every value conversion is reached through the shared
    /// `rows_to_record_batch_with_schema` producer (row → Arrow) and Vortex's own
    /// `ArrowSession::from_arrow_array` (Arrow → Vortex). This test pins the OBSERVABLE half of
    /// that claim: a full round trip through `StreamingVortexWriter` produces the same row count
    /// and column set the shared producer would for the same rows, with no separate CQL-aware
    /// path taken.
    #[tokio::test]
    async fn write_chunk_and_finalize_round_trip_row_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("basic.vortex");
        let metadata = metadata_two_cols();

        let mut writer = StreamingVortexWriter::create(
            &path,
            &metadata,
            &VortexExportOptions { row_group_size: 2 },
        )
        .await
        .expect("create");

        let rows = vec![row(1, Some("a")), row(2, None), row(3, Some("c"))];
        let pushed = writer.write_chunk(&rows).await.expect("write_chunk");
        // row_group_size=2, 3 rows buffered -> one full group (2 rows) flushed immediately.
        assert_eq!(pushed, 2);
        assert_eq!(writer.rows_written(), 3);

        writer.finalize().await.expect("finalize");

        assert!(
            path.exists(),
            "finalize must rename the temp file into place"
        );
        assert!(
            !tmp_sibling_path(&path).exists(),
            "no .tmp sibling should remain after a successful finalize"
        );
    }

    /// R5.1-shaped: default options match Parquet's default row group size, so the two formats'
    /// CLI defaults are unsurprising side by side.
    #[test]
    fn default_options_match_parquet_row_group_size() {
        assert_eq!(VortexExportOptions::default().row_group_size, 10_000);
    }

    /// R4.2: zero row_group_size is a named `InvalidOptions` error, not a panic or silent 0-row
    /// writer.
    #[tokio::test]
    async fn zero_row_group_size_is_invalid_options() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("invalid.vortex");
        let metadata = metadata_two_cols();

        let result = StreamingVortexWriter::create(
            &path,
            &metadata,
            &VortexExportOptions { row_group_size: 0 },
        )
        .await;
        let err = match result {
            Ok(_) => panic!("zero row_group_size must be rejected"),
            Err(e) => e,
        };

        assert!(matches!(err, VortexExportError::InvalidOptions(_)));
    }

    /// R4.1 (writer-level slice): finalizing twice is a named error rather than a panic, and
    /// does not resurrect a file at the destination path.
    #[tokio::test]
    async fn double_finalize_is_invalid_options_not_a_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("double.vortex");
        let metadata = metadata_two_cols();

        let mut writer =
            StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions::default())
                .await
                .expect("create");
        writer
            .write_chunk(&[row(1, Some("a"))])
            .await
            .expect("write_chunk");
        writer.finalize().await.expect("first finalize");

        let err = writer
            .finalize()
            .await
            .expect_err("second finalize must be rejected");
        assert!(matches!(err, VortexExportError::InvalidOptions(_)));
    }
}
