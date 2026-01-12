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
#[cfg(feature = "state_machine")]
pub mod parquet;
#[cfg(feature = "state_machine")]
pub mod table;
pub mod value_fmt;

#[cfg(feature = "state_machine")]
#[allow(unused_imports)]
pub use csv::CSVWriter;
#[cfg(feature = "state_machine")]
#[allow(unused_imports)]
pub use json::JSONWriter;
#[cfg(feature = "state_machine")]
#[allow(unused_imports)]
pub use parquet::ParquetWriter;
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
