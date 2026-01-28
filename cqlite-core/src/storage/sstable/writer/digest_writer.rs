//! Digest.crc32 writer - writes whole-file CRC32 checksums
//!
//! Generates the Digest.crc32 file containing a single CRC32 checksum
//! computed over the entire Data.db component file.
//!
//! The digest provides fast verification that component files have not
//! been corrupted during transfer or storage.
//!
//! # Format
//!
//! The Digest.crc32 file contains:
//! - A single decimal ASCII number (CRC32 value)
//! - No newline at end
//!
//! Example: `1041978312`
//!
//! # Usage
//!
//! ```rust,ignore
//! use std::fs::File;
//! use std::io::Read;
//!
//! // Read the component file and compute CRC32
//! let mut file = File::open("nb-1-big-Data.db")?;
//! let mut hasher = crc32fast::Hasher::new();
//! let mut buffer = vec![0u8; 64 * 1024];
//!
//! loop {
//!     let bytes_read = file.read(&mut buffer)?;
//!     if bytes_read == 0 {
//!         break;
//!     }
//!     hasher.update(&buffer[..bytes_read]);
//! }
//!
//! let crc32 = hasher.finalize();
//!
//! // Write the digest
//! let writer = DigestWriter::new(digest_path);
//! writer.write(crc32)?;
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::error::{Error, Result};

/// Digest.crc32 component writer
///
/// Writes a single CRC32 checksum value to the Digest.crc32 file.
/// This checksum is computed over the entire Data.db component and provides
/// a fast way to verify file integrity.
///
/// # Format
///
/// The file contains a single decimal ASCII number with no trailing newline.
/// For example: `1041978312`
///
/// # Critical Requirements
///
/// 1. Must be written BEFORE TOC.txt (part of the component set)
/// 2. Must be written AFTER Data.db (needs the full file to compute CRC32)
/// 3. The CRC32 is computed over the entire Data.db file contents
/// 4. Uses Java CRC32 algorithm (polynomial 0xEDB88320, same as crc32fast)
#[derive(Debug)]
pub struct DigestWriter {
    /// Path to the Digest.crc32 file to write
    path: PathBuf,
}

impl DigestWriter {
    /// Create a new Digest.crc32 writer
    ///
    /// # Arguments
    ///
    /// * `path` - Full path to the Digest.crc32 file (e.g., "nb-1-big-Digest.crc32")
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Write a CRC32 checksum to the Digest.crc32 file
    ///
    /// The checksum is written as a decimal ASCII string with no newline.
    ///
    /// # Arguments
    ///
    /// * `crc32_value` - The CRC32 checksum value to write
    ///
    /// # Returns
    ///
    /// Ok(()) on success, Error on I/O failure
    ///
    /// # Errors
    ///
    /// Returns Error::Storage if:
    /// - Unable to create the Digest.crc32 file
    /// - Unable to write the checksum value
    /// - Unable to flush/sync the file
    pub fn write(&self, crc32_value: u32) -> Result<()> {
        let file = File::create(&self.path).map_err(|e| {
            Error::storage(format!(
                "Failed to create Digest.crc32 file {:?}: {}",
                self.path, e
            ))
        })?;

        let mut writer = BufWriter::new(file);

        // Write CRC32 as decimal ASCII string (no newline)
        write!(writer, "{}", crc32_value).map_err(|e| {
            Error::storage(format!(
                "Failed to write CRC32 value to Digest.crc32: {}",
                e
            ))
        })?;

        // Flush buffer and sync to disk
        writer.flush().map_err(|e| {
            Error::storage(format!("Failed to flush Digest.crc32: {}", e))
        })?;

        let file = writer.into_inner().map_err(|e| {
            Error::storage(format!("Failed to extract file from buffer: {}", e))
        })?;

        file.sync_all().map_err(|e| {
            Error::storage(format!("Failed to sync Digest.crc32 to disk: {}", e))
        })?;

        Ok(())
    }

    /// Compute CRC32 checksum for a file
    ///
    /// Reads the entire file and computes a CRC32 checksum using the
    /// crc32fast algorithm (compatible with Java's CRC32).
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the file to checksum
    ///
    /// # Returns
    ///
    /// Ok(crc32_value) on success, Error on I/O failure
    ///
    /// # Errors
    ///
    /// Returns Error::Storage if:
    /// - Unable to open the file
    /// - Unable to read file contents
    pub fn compute_crc32(file_path: &PathBuf) -> Result<u32> {
        use std::io::Read;

        let mut file = File::open(file_path).map_err(|e| {
            Error::storage(format!("Failed to open file {:?} for CRC32: {}", file_path, e))
        })?;

        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer for efficient reading

        loop {
            let bytes_read = file.read(&mut buffer).map_err(|e| {
                Error::storage(format!("Failed to read file {:?} for CRC32: {}", file_path, e))
            })?;

            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);
        }

        Ok(hasher.finalize())
    }

    /// Compute and write CRC32 for a component file
    ///
    /// Convenience method that computes the CRC32 checksum for a file
    /// and writes it to the Digest.crc32 file in one call.
    ///
    /// # Arguments
    ///
    /// * `component_path` - Path to the component file (e.g., Data.db)
    ///
    /// # Returns
    ///
    /// Ok(crc32_value) on success, Error on I/O failure
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let writer = DigestWriter::new(digest_path);
    /// let crc32 = writer.write_for_file(&data_db_path)?;
    /// println!("Wrote CRC32: {}", crc32);
    /// ```
    pub fn write_for_file(&self, component_path: &PathBuf) -> Result<u32> {
        let crc32 = Self::compute_crc32(component_path)?;
        self.write(crc32)?;
        Ok(crc32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write as StdWrite;
    use tempfile::TempDir;

    #[test]
    fn test_digest_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let digest_path = temp_dir.path().join("nb-1-big-Digest.crc32");

        let writer = DigestWriter::new(digest_path.clone());
        writer.write(1041978312).unwrap();

        // Verify file exists
        assert!(digest_path.exists());

        // Read and verify contents
        let contents = fs::read_to_string(&digest_path).unwrap();
        assert_eq!(contents, "1041978312");

        // Verify no trailing newline
        let bytes = fs::read(&digest_path).unwrap();
        assert_eq!(bytes.len(), 10); // "1041978312" is 10 ASCII chars
        assert_ne!(bytes[bytes.len() - 1], b'\n');
    }

    #[test]
    fn test_digest_writer_zero() {
        let temp_dir = TempDir::new().unwrap();
        let digest_path = temp_dir.path().join("nb-1-big-Digest.crc32");

        let writer = DigestWriter::new(digest_path.clone());
        writer.write(0).unwrap();

        let contents = fs::read_to_string(&digest_path).unwrap();
        assert_eq!(contents, "0");
    }

    #[test]
    fn test_digest_writer_max_value() {
        let temp_dir = TempDir::new().unwrap();
        let digest_path = temp_dir.path().join("nb-1-big-Digest.crc32");

        let writer = DigestWriter::new(digest_path.clone());
        writer.write(u32::MAX).unwrap();

        let contents = fs::read_to_string(&digest_path).unwrap();
        assert_eq!(contents, format!("{}", u32::MAX));
    }

    #[test]
    fn test_compute_crc32() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.db");

        // Write some test data
        let test_data = b"Hello, World! This is test data for CRC32.";
        fs::write(&test_file, test_data).unwrap();

        // Compute CRC32
        let crc32 = DigestWriter::compute_crc32(&test_file).unwrap();

        // Verify against known CRC32 (computed with crc32fast)
        let expected_crc32 = crc32fast::hash(test_data);
        assert_eq!(crc32, expected_crc32);
    }

    #[test]
    fn test_compute_crc32_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("empty.db");

        // Create empty file
        File::create(&test_file).unwrap();

        // Compute CRC32 for empty file
        let crc32 = DigestWriter::compute_crc32(&test_file).unwrap();

        // CRC32 of empty data is 0
        assert_eq!(crc32, 0);
    }

    #[test]
    fn test_compute_crc32_large_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("large.db");

        // Create a file larger than the buffer size (64KB)
        let mut file = File::create(&test_file).unwrap();
        let chunk = vec![0x42u8; 1024]; // 1KB chunk
        for _ in 0..100 {
            // 100KB total
            file.write_all(&chunk).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        // Compute CRC32
        let crc32 = DigestWriter::compute_crc32(&test_file).unwrap();

        // Verify against reference computation
        let expected_chunk = vec![0x42u8; 1024];
        let mut hasher = crc32fast::Hasher::new();
        for _ in 0..100 {
            hasher.update(&expected_chunk);
        }
        let expected_crc32 = hasher.finalize();

        assert_eq!(crc32, expected_crc32);
    }

    #[test]
    fn test_write_for_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let digest_file = temp_dir.path().join("nb-1-big-Digest.crc32");

        // Write some test data
        let test_data = b"Test SSTable data component";
        fs::write(&data_file, test_data).unwrap();

        // Write digest
        let writer = DigestWriter::new(digest_file.clone());
        let crc32 = writer.write_for_file(&data_file).unwrap();

        // Verify the digest file was written correctly
        let contents = fs::read_to_string(&digest_file).unwrap();
        assert_eq!(contents, format!("{}", crc32));

        // Verify CRC32 matches expected value
        let expected_crc32 = crc32fast::hash(test_data);
        assert_eq!(crc32, expected_crc32);
    }

    #[test]
    fn test_compute_crc32_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let nonexistent = temp_dir.path().join("does-not-exist.db");

        let result = DigestWriter::compute_crc32(&nonexistent);
        assert!(result.is_err());
    }
}
