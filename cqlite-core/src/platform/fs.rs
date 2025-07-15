//! File system abstraction

use crate::{error::Error, Result};
use std::io::Write;
use std::path::Path;
use tokio::fs;

/// File system operations
#[derive(Debug)]
pub struct FileSystem;

impl FileSystem {
    /// Create a new file system
    pub async fn new() -> Result<Self> {
        Ok(Self)
    }

    /// Check if path exists
    pub async fn exists(&self, path: &Path) -> Result<bool> {
        Ok(fs::metadata(path).await.is_ok())
    }

    /// Create directory
    pub async fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)
            .await
            .map_err(Error::from)
    }

    /// Read directory
    pub async fn read_dir(&self, path: &Path) -> Result<fs::ReadDir> {
        fs::read_dir(path)
            .await
            .map_err(Error::from)
    }

    /// Remove file
    pub async fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
            .await
            .map_err(Error::from)
    }

    /// Copy file
    pub async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        fs::copy(from, to)
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    /// Create file
    pub async fn create_file(&self, path: &Path) -> Result<Box<dyn Write + Send>> {
        let file = std::fs::File::create(path).map_err(Error::from)?;
        Ok(Box::new(file))
    }

    /// Open file for reading
    pub async fn open_file(&self, path: &Path) -> Result<std::fs::File> {
        std::fs::File::open(path).map_err(Error::from)
    }

    /// Read file contents
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        fs::read(path).await.map_err(Error::from)
    }

    /// Write file contents
    pub async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        fs::write(path, contents)
            .await
            .map_err(Error::from)
    }

    /// Get file size
    pub async fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = fs::metadata(path)
            .await
            .map_err(Error::from)?;
        Ok(metadata.len())
    }

    /// Get file metadata
    pub async fn metadata(&self, path: &Path) -> Result<std::fs::Metadata> {
        fs::metadata(path)
            .await
            .map_err(Error::from)
    }
}
