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
//! The writer atomically creates a unique hidden `.<name>.<pid>.<n>.<t>.tmp` sibling of the destination (`create_new`; never a predictable name) and only `rename`s it to the real
//! path on a fully successful `finalize()`. Any error during conversion, the push itself, or
//! `finish()` removes the temp file (best-effort) before propagating; a [`Drop`] impl covers the
//! remaining case — an error from OUTSIDE this writer's own methods (e.g. the caller's chunk-pull
//! deadline) that unwinds past it without ever calling `finalize`. So the destination path never
//! holds a partial file, even if the writer is abandoned or the PROCESS is killed between chunks.
//! (This is process-kill safety, not power-loss durability: there is no `fsync` before the
//! rename, so an OS/power failure mid-write is a separate, unaddressed concern.)

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
    /// The writer hit a terminal failure earlier (its temp file is already removed); every
    /// later `write_chunk`/`finalize` returns this instead of touching a closed writer.
    #[error("Vortex writer is poisoned by an earlier failure; the export was abandoned")]
    Poisoned,
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

/// Atomically create a UNIQUE same-directory temp file for `path` (fail-closed, R4).
///
/// A predictable `<path>.tmp` opened with a truncating create lets concurrent exports clobber
/// each other and lets a pre-existing symlink at that name truncate an unrelated file. Here
/// the name carries the pid, a process-wide counter and a timestamp, and the file is opened
/// with `create_new` (`O_CREAT|O_EXCL`, which refuses an existing entry INCLUDING a symlink),
/// retrying with a fresh name on `AlreadyExists`. The returned path is the exact file that is
/// later renamed into place or removed.
fn create_unique_tmp(path: &Path) -> std::io::Result<(std::fs::File, PathBuf)> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    for _ in 0..32 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let tmp_name = format!(
            ".{name}.{}.{}.{nanos:x}.tmp",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let tmp = dir.map_or_else(|| PathBuf::from(&tmp_name), |d| d.join(&tmp_name));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => return Ok((f, tmp)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not create a unique Vortex temp file after 32 attempts",
    ))
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
    /// The temp file this writer still OWNS; `None` once it was renamed into place or
    /// removed, so `Drop` never unlinks a path another process may since have created.
    tmp_path: Option<PathBuf>,
    final_path: PathBuf,
    row_buffer: Vec<QueryRow>,
    row_group_size: usize,
    rows_written: u64,
    batches_pushed: usize,
    /// Set on the first terminal failure; later calls return [`VortexExportError::Poisoned`].
    poisoned: bool,
}

impl StreamingVortexWriter {
    /// Create a streaming Vortex writer over the destination path.
    ///
    /// The Arrow schema is built from `metadata.columns` using the same
    /// [`build_arrow_schema`](super::arrow_convert::build_arrow_schema) helper the streaming
    /// Parquet writer uses. Opens a `<path>.tmp` sibling immediately; nothing is ever written to
    /// `path` itself until [`finalize`](Self::finalize) succeeds.
    ///
    /// # Panics
    ///
    /// Must be called from inside a Tokio runtime — it uses `tokio::fs::File` and Vortex's own
    /// writer looks up the ambient Tokio handle internally. Calling it outside one panics.
    ///
    /// # `!Send`
    ///
    /// The returned `StreamingVortexWriter` is `!Send` (it holds a `vortex::file::Writer`, whose
    /// background layout task is driven inline rather than via `tokio::spawn`). It can be `await`ed
    /// freely within a single task, but cannot be sent to another task or held across a `tokio::spawn`
    /// boundary.
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
        let (std_file, tmp_path) = create_unique_tmp(&final_path)?;
        let file = tokio::fs::File::from_std(std_file);
        let writer = session.write_options().writer(file, dtype);

        Ok(Self {
            session,
            schema,
            columns: metadata.columns.clone(),
            writer: Some(writer),
            tmp_path: Some(tmp_path),
            final_path,
            row_buffer: Vec::with_capacity(options.row_group_size),
            row_group_size: options.row_group_size,
            rows_written: 0,
            batches_pushed: 0,
            poisoned: false,
        })
    }

    /// Buffer rows and push complete chunks.
    ///
    /// Returns the number of rows pushed to the writer in this call (rows remaining in the
    /// buffer are pushed by [`finalize`](Self::finalize)).
    pub async fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, VortexExportError> {
        if self.poisoned {
            return Err(VortexExportError::Poisoned);
        }
        // After `finalize` the writer is gone: rows accepted now could never reach the file,
        // so refuse them before buffering or counting (no silent loss).
        if self.writer.is_none() {
            return Err(VortexExportError::InvalidOptions(
                "writer already finalized - cannot write more rows".to_string(),
            ));
        }
        self.row_buffer.extend(rows.iter().cloned());
        self.rows_written += rows.len() as u64;

        let mut pushed = 0;
        while self.row_buffer.len() >= self.row_group_size {
            let chunk: Vec<QueryRow> = self.row_buffer.drain(..self.row_group_size).collect();
            if let Err(e) = self.push_batch(&chunk).await {
                self.abandon().await;
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
        if self.poisoned {
            return Err(VortexExportError::Poisoned);
        }
        if !self.row_buffer.is_empty() {
            let remaining = std::mem::take(&mut self.row_buffer);
            if let Err(e) = self.push_batch(&remaining).await {
                self.abandon().await;
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
            self.abandon().await;
            return Err(VortexExportError::Vortex(e));
        }

        // `tokio::fs::rename` is `std::fs::rename` on a blocking thread, which REPLACES an existing
        // destination on every platform (Unix `rename(2)`; Windows `MoveFileExW` with
        // `MOVEFILE_REPLACE_EXISTING`, per the std docs). Pinned by
        // `export_replaces_a_preexisting_destination`.
        //
        // Write-error audit (roborev r2): `finish()` awaits vortex's `write.flush()` (vortex-file
        // `writer.rs`), and tokio's `File::flush` surfaces a failed in-flight write — so an I/O
        // error such as ENOSPC fails the export HERE. This writer never calls `sync_all` or relies
        // on drop to report errors (the paths where tokio parks the error), and the file handle
        // is owned by vortex's writer, so no other terminal path exists. Pinned on Linux by
        // `write_error_on_dev_full_fails_finish`.
        let Some(tmp_path) = self.tmp_path.clone() else {
            return Err(VortexExportError::Poisoned);
        };
        if let Err(e) = tokio::fs::rename(&tmp_path, &self.final_path).await {
            self.abandon().await;
            return Err(e.into());
        }
        self.tmp_path = None;
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

    /// Terminal-failure path: DROP the writer (closing its open temp file) BEFORE unlinking
    /// the temp, then poison. Never propagates its own error — the ORIGINAL failure is what
    /// the caller needs to see.
    async fn abandon(&mut self) {
        drop(self.writer.take());
        if let Some(tmp_path) = self.tmp_path.take() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        self.poisoned = true;
    }
}

/// Fail-closed cleanup for every path `write_chunk`/`finalize`'s own `abandon` calls above
/// do NOT cover (review finding, R4): an error from the CALLER's side of `write_chunk` — e.g.
/// `collect_chunk_within`'s deadline/query-timeout path in `cqlite-cli/src/commands/export.rs`
/// — propagates via `?` straight past this writer with no chance to run its own async cleanup.
/// Without this, R4's "leaves no readable `.vortex` file at the destination path" claim held
/// only for errors the writer's OWN methods observed, not for one that unwound through it.
///
/// Ensures the (possibly still-open) `vortex::file::Writer` is dropped BEFORE the temp file is
/// unlinked — required on Windows (an open file cannot be deleted there), a no-op on POSIX.
/// Dropping an in-progress `Writer` aborts its background layout task cleanly (it is a `Send`
/// task spawned on the session's Tokio handle, not orphaned work) rather than deadlocking.
///
/// After a SUCCESSFUL `finalize()` the temp file no longer exists (renamed to `final_path`), so
/// this `remove_file` call is a harmless not-found; it is the cleanup path ONLY when `finalize`
/// never ran to completion.
impl Drop for StreamingVortexWriter {
    fn drop(&mut self) {
        drop(self.writer.take());
        if let Some(tmp_path) = self.tmp_path.take() {
            let _ = std::fs::remove_file(&tmp_path);
        }
    }
}

#[cfg(test)]
#[path = "vortex_tests.rs"]
mod tests;
