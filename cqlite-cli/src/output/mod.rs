//! Output formatting for QueryResult
//!
//! This module provides writers that adapt QueryResult to various output formats
//! (table, JSON, CSV) with stable, cqlsh-compatible formatting.
//!
//! ## Contract
//!
//! All writers follow the QUERY_RESULT_CONTRACT.md specification:
//! - Column order determined by `metadata.columns`
//! - Null values handled consistently
//! - Format-specific conventions (e.g., row count footer for tables)

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(feature = "state_machine")]
pub mod csv;
#[cfg(feature = "state_machine")]
pub mod json;
/// One CQL value rendered for the JSON egress (issue #3644 item 3).
#[cfg(feature = "state_machine")]
mod json_cell;
#[cfg(feature = "state_machine")]
pub mod parquet;
#[cfg(feature = "state_machine")]
pub mod table;
pub mod value_fmt;

#[cfg(feature = "state_machine")]
pub use csv::{CSVWriter, StreamingCSVWriter};
#[cfg(feature = "state_machine")]
pub use json::{JSONWriter, StreamingJSONWriter};
#[cfg(feature = "state_machine")]
#[allow(unused_imports)]
pub use parquet::{
    create_streaming_parquet_writer, create_streaming_parquet_writer_from_writer, ParquetWriter,
};
#[cfg(feature = "state_machine")]
#[allow(unused_imports)]
pub use table::TableWriter;
#[allow(unused_imports)]
pub use value_fmt::ValueFormatter;

// ============================================================================
// Output Target Types (Issue #279)
// ============================================================================

/// Target destination for query output
#[derive(Debug, Clone, Default)]
pub enum OutputTarget {
    /// Write to stdout (default, backward compatible)
    #[default]
    Stdout,
    /// Write to a file at the specified path
    File(PathBuf),
}

impl OutputTarget {
    /// Returns true if this is a file target
    pub fn is_file(&self) -> bool {
        matches!(self, OutputTarget::File(_))
    }

    /// Get the file path if this is a file target
    #[allow(dead_code)]
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            OutputTarget::File(p) => Some(p),
            OutputTarget::Stdout => None,
        }
    }
}

/// Error type for output operations
#[derive(Debug)]
pub enum OutputError {
    /// File I/O error
    Io(std::io::Error),
    /// Directory does not exist
    DirectoryNotFound(PathBuf),
    /// File already exists and overwrite not allowed
    FileExists(PathBuf),
    /// Parquet requires file output
    ParquetRequiresFile,
}

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputError::Io(e) => write!(f, "I/O error: {}", e),
            OutputError::DirectoryNotFound(p) => {
                write!(f, "Directory not found: {}", p.display())
            }
            OutputError::FileExists(p) => {
                write!(
                    f,
                    "File already exists: {}. Use --overwrite to replace.",
                    p.display()
                )
            }
            OutputError::ParquetRequiresFile => {
                write!(
                    f,
                    "Parquet format requires file output.\n\n\
                     Use --output/-o to specify an output file:\n\
                     cqlite --out parquet --output results.parquet -e 'SELECT * FROM table'"
                )
            }
        }
    }
}

impl std::error::Error for OutputError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OutputError::Io(e) => Some(e),
            _ => None,
        }
    }
}

// ============================================================================
// Streaming Writer Interface (Issue #280)
// ============================================================================

#[cfg(feature = "state_machine")]
use cqlite_core::query::{QueryMetadata, QueryRow};

/// Trait for writers that support streaming/chunked output
///
/// Unlike batch writers that receive a complete `QueryResult`, streaming writers
/// process data incrementally via `write_header()`, `write_chunk()`, and `finalize()`.
/// This enables processing of arbitrarily large result sets within the 128MB memory budget.
///
/// # Memory Budget
///
/// To stay within the 128MB target:
/// - Chunk sizes should typically be 5,000-10,000 rows
/// - For large blob/text columns, use smaller chunks (1,000-5,000)
/// - Parquet writers buffer rows for row groups; default is 10,000 rows
///
/// # Contract
///
/// 1. `write_header()` MUST be called exactly once before any `write_chunk()` calls
/// 2. `write_chunk()` MAY be called zero or more times
/// 3. `finalize()` MUST be called exactly once to complete the output
/// 4. After `finalize()`, no further calls are allowed
/// 5. Implementors SHOULD return errors rather than panic on contract violations
///
/// # Troubleshooting
///
/// If you encounter OOM errors:
/// 1. Reduce chunk sizes when calling `write_chunk()` (max: 10,000 rows)
/// 2. For Parquet, reduce `row_group_size` (default: 10,000 rows)
/// 3. Check for large blob/text columns that inflate row size
///
/// # Example
///
/// ```ignore
/// let mut writer = StreamingCSVWriter::new(file);
/// writer.write_header(&metadata)?;
///
/// for chunk in result_stream.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
#[cfg(feature = "state_machine")]
pub trait StreamingWriter: Send {
    /// Initialize writer with column metadata (write header if applicable)
    ///
    /// Called once before any data is written. For CSV, this writes the header row.
    /// For Parquet, this initializes the Arrow schema.
    fn write_header(&mut self, metadata: &QueryMetadata) -> Result<(), OutputError>;

    /// Write a chunk of rows (called multiple times during streaming)
    ///
    /// Returns the number of rows actually written (may differ from input if
    /// the writer buffers internally, e.g., Parquet row groups).
    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError>;

    /// Finalize output (flush buffers, write footer, close resources)
    ///
    /// Called once after all data has been written. For Parquet, this writes
    /// the final row group and file footer.
    fn finalize(&mut self) -> Result<(), OutputError>;

    /// Get count of rows written so far
    ///
    /// Intended for progress reporting; not yet integrated into export functions.
    #[allow(dead_code)]
    fn rows_written(&self) -> u64;

    /// Get bytes written so far (if trackable)
    ///
    /// Intended for progress reporting; not yet integrated into export functions.
    #[allow(dead_code)]
    fn bytes_written(&self) -> Option<u64> {
        None
    }
}

/// Progress information for streaming operations
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct StreamingProgress {
    /// Total rows written so far
    pub rows_written: u64,
    /// Total bytes written so far (if known)
    pub bytes_written: Option<u64>,
    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Estimated total rows (if known)
    pub total_rows_hint: Option<u64>,
}

#[allow(dead_code)]
impl StreamingProgress {
    /// Calculate progress percentage (if total is known)
    pub fn percent(&self) -> Option<f64> {
        self.total_rows_hint.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.rows_written as f64 / total as f64) * 100.0
            }
        })
    }

    /// Calculate rows per second throughput
    pub fn rows_per_second(&self) -> f64 {
        if self.elapsed_ms == 0 {
            0.0
        } else {
            (self.rows_written as f64) / (self.elapsed_ms as f64 / 1000.0)
        }
    }
}

/// Write output to target (stdout or file) with atomic file writes
///
/// For file targets, this uses atomic write semantics:
/// 1. Write to a temporary file in the same directory
/// 2. Sync to disk for durability
/// 3. Rename to final path (atomic on POSIX systems)
///
/// # Arguments
/// * `content` - The bytes to write
/// * `target` - Where to write (stdout or file path)
/// * `overwrite` - If true, overwrite existing files; if false, error on existing
///
/// # Errors
/// Returns `OutputError` on I/O failures, missing directories, or file exists conflicts
pub fn write_to_target(
    content: &[u8],
    target: &OutputTarget,
    overwrite: bool,
) -> Result<(), OutputError> {
    match target {
        OutputTarget::Stdout => {
            std::io::stdout()
                .write_all(content)
                .map_err(OutputError::Io)?;
            std::io::stdout().flush().map_err(OutputError::Io)
        }
        OutputTarget::File(path) => write_atomic(path, content, overwrite),
    }
}

/// Write to file atomically (write to temp, rename on success)
///
/// This ensures that interrupted writes don't leave partial files.
fn write_atomic(path: &Path, content: &[u8], overwrite: bool) -> Result<(), OutputError> {
    // Validate parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(OutputError::DirectoryNotFound(parent.to_path_buf()));
        }
    }

    // Check if file exists when overwrite is false
    if !overwrite && path.exists() {
        return Err(OutputError::FileExists(path.to_path_buf()));
    }

    // Generate temp file path in same directory (required for atomic rename)
    let extension = path
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let temp_path = path.with_extension(format!("{}.tmp.{}", extension, std::process::id()));

    // Write to temp file
    let write_result = (|| {
        let mut file = File::create(&temp_path).map_err(OutputError::Io)?;
        file.write_all(content).map_err(OutputError::Io)?;
        file.sync_all().map_err(OutputError::Io)?;
        Ok(())
    })();

    // If write failed, clean up temp file
    if let Err(e) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(e);
    }

    // Atomic rename (on POSIX, this is atomic if same filesystem)
    fs::rename(&temp_path, path).map_err(|e| {
        // Clean up temp file on rename failure
        let _ = fs::remove_file(&temp_path);
        OutputError::Io(e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_output_target_default_is_stdout() {
        let target = OutputTarget::default();
        assert!(matches!(target, OutputTarget::Stdout));
        assert!(!target.is_file());
    }

    #[test]
    fn test_output_target_file() {
        let target = OutputTarget::File(PathBuf::from("/tmp/test.json"));
        assert!(target.is_file());
        assert_eq!(target.path(), Some(&PathBuf::from("/tmp/test.json")));
    }

    #[test]
    fn test_write_to_file_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("output.json");

        let content = b"test content";
        let target = OutputTarget::File(file_path.clone());

        write_to_target(content, &target, false).unwrap();

        assert!(file_path.exists());
        let written = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(written, "test content");
    }

    #[test]
    fn test_write_to_file_respects_overwrite_false() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("existing.json");

        // Create existing file
        std::fs::write(&file_path, "existing content").unwrap();

        let content = b"new content";
        let target = OutputTarget::File(file_path.clone());

        let result = write_to_target(content, &target, false);
        assert!(matches!(result, Err(OutputError::FileExists(_))));

        // Original content should be preserved
        let written = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(written, "existing content");
    }

    #[test]
    fn test_write_to_file_respects_overwrite_true() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("existing.json");

        // Create existing file
        std::fs::write(&file_path, "existing content").unwrap();

        let content = b"new content";
        let target = OutputTarget::File(file_path.clone());

        write_to_target(content, &target, true).unwrap();

        // Content should be replaced
        let written = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(written, "new content");
    }

    #[test]
    fn test_write_to_file_missing_directory_error() {
        let file_path = PathBuf::from("/nonexistent/directory/output.json");
        let content = b"test content";
        let target = OutputTarget::File(file_path);

        let result = write_to_target(content, &target, false);
        assert!(matches!(result, Err(OutputError::DirectoryNotFound(_))));
    }

    #[test]
    fn test_write_atomic_no_temp_file_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("output.json");

        let content = b"test content";
        write_atomic(&file_path, content, false).unwrap();

        // Check no temp files left behind
        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name(), "output.json");
    }

    #[test]
    fn test_output_error_display() {
        let io_err = OutputError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "test error",
        ));
        assert!(io_err.to_string().contains("I/O error"));

        let dir_err = OutputError::DirectoryNotFound(PathBuf::from("/missing"));
        assert!(dir_err.to_string().contains("Directory not found"));

        let exists_err = OutputError::FileExists(PathBuf::from("/existing"));
        assert!(exists_err.to_string().contains("File already exists"));
        assert!(exists_err.to_string().contains("--overwrite"));

        let parquet_err = OutputError::ParquetRequiresFile;
        assert!(parquet_err.to_string().contains("Parquet format requires"));
        assert!(parquet_err.to_string().contains("--output"));
    }
}
