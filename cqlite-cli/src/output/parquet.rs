//! CLI adapter for the core Parquet export writer (Epic #682)
//!
//! The Parquet writer implementation lives in `cqlite_core::export::parquet`
//! (behind cqlite-core's `parquet` feature, which this crate enables).  This
//! module is a thin boundary layer that:
//!
//! - exposes the historical CLI-facing API (`ParquetWriter::write` taking an
//!   [`OutputConfig`], `create_streaming_parquet_writer*` taking a row group
//!   size) so existing call sites and tests are unchanged,
//! - implements the CLI [`StreamingWriter`] trait on top of the core
//!   streaming writer, and
//! - maps [`ParquetExportError`] to the CLI [`OutputError`] (as an I/O error,
//!   matching the pre-lift error rendering).
//!
//! Output bytes are produced exclusively by the core writer; the adapter adds
//! no formatting of its own, so CLI `--out parquet` output is byte-identical
//! to the pre-lift implementation (see `tests/parquet_golden_tests.rs`).

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use cqlite_core::export::parquet::{
    ParquetExportError, ParquetExportOptions, ParquetWriter as CoreParquetWriter,
    StreamingParquetWriter as CoreStreamingParquetWriter,
};
use cqlite_core::query::{QueryMetadata, QueryResult, QueryRow};
use std::error::Error as StdError;
use std::fs::File;
use std::io::Write;

/// Map a core Parquet export error to the CLI output error type.
///
/// The pre-lift implementation wrapped all Parquet failures as
/// `OutputError::Io`, so the mapping preserves the historical rendering
/// ("I/O error: ...").
fn map_parquet_err(e: ParquetExportError) -> OutputError {
    OutputError::Io(std::io::Error::other(e.to_string()))
}

/// Build core export options from the CLI output configuration.
fn options_from_config(config: &OutputConfig) -> ParquetExportOptions {
    ParquetExportOptions {
        row_limit: config.limit,
        ..Default::default()
    }
}

/// Parquet writer for QueryResult (CLI facade)
///
/// Delegates to [`cqlite_core::export::parquet::ParquetWriter`].
/// Unlike JSON/CSV writers, this returns `Vec<u8>` (binary data).
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write QueryResult to Parquet binary format
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to Parquet
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    ///
    /// Binary Parquet data or error
    pub fn write(
        result: &QueryResult,
        config: &OutputConfig,
    ) -> Result<Vec<u8>, Box<dyn StdError>> {
        CoreParquetWriter::write(result, &options_from_config(config))
            .map_err(|e| Box::new(e) as Box<dyn StdError>)
    }
}

/// Streaming Parquet writer for memory-efficient export of large datasets
///
/// CLI wrapper around [`cqlite_core::export::parquet::StreamingParquetWriter`]
/// implementing the CLI [`StreamingWriter`] trait.
pub struct StreamingParquetWriter<W: Write + Send> {
    inner: CoreStreamingParquetWriter<W>,
}

impl<W: Write + Send> StreamingWriter for StreamingParquetWriter<W> {
    fn write_header(&mut self, _metadata: &QueryMetadata) -> Result<(), OutputError> {
        // The core writer builds its Arrow schema and writes the Parquet file
        // header at construction time (the constructors below take the query
        // metadata), so there is nothing left to do here.
        Ok(())
    }

    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError> {
        self.inner.write_chunk(rows).map_err(map_parquet_err)
    }

    fn finalize(&mut self) -> Result<(), OutputError> {
        self.inner.finalize().map_err(map_parquet_err)
    }

    fn rows_written(&self) -> u64 {
        self.inner.rows_written()
    }
}

/// Create a StreamingParquetWriter that writes to a file
///
/// This is a convenience function that handles the file creation and
/// ArrowWriter initialization.
pub fn create_streaming_parquet_writer(
    file: File,
    metadata: &QueryMetadata,
    row_group_size: usize,
) -> Result<StreamingParquetWriter<File>, OutputError> {
    create_streaming_parquet_writer_from_writer(file, metadata, row_group_size)
}

/// Create a StreamingParquetWriter that writes to any `W: Write + Send`.
///
/// Both constructors delegate to the same core constructor, so the schema
/// produced by the streaming path is always identical to the schema produced
/// by the batch `ParquetWriter`.
pub fn create_streaming_parquet_writer_from_writer<W: Write + Send>(
    output: W,
    metadata: &QueryMetadata,
    row_group_size: usize,
) -> Result<StreamingParquetWriter<W>, OutputError> {
    let options = ParquetExportOptions {
        row_group_size,
        ..Default::default()
    };
    let inner =
        CoreStreamingParquetWriter::new(output, metadata, &options).map_err(map_parquet_err)?;
    Ok(StreamingParquetWriter { inner })
}
