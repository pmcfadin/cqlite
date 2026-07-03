//! Platform abstraction layer for cross-platform compatibility

use crate::{Config, Result};

pub mod fs;

/// Opt-in filesystem seam for cross-platform compatibility.
///
/// NOTE: This is an opt-in `fs` seam only; most code paths use `std`/`tokio`
/// fs directly — this is NOT an enforced I/O boundary. The `Platform` handle is
/// threaded through many call sites, but only [`Platform::fs`] is ever read.
/// Full removal of that threading is a possible later refactor (out of scope).
#[derive(Debug)]
pub struct Platform {
    /// File system abstraction
    fs: fs::FileSystem,

    /// Configuration
    config: Config,
}

impl Platform {
    /// Create a new platform abstraction
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            fs: fs::FileSystem::new().await?,
            config: config.clone(),
        })
    }

    /// Get file system abstraction
    pub fn fs(&self) -> &fs::FileSystem {
        &self.fs
    }

    /// Get configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_platform_creation() {
        let config = Config::default();
        let platform = Platform::new(&config).await.unwrap();

        assert!(platform.fs().exists(Path::new(".")).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_system() {
        let config = Config::default();
        let platform = Platform::new(&config).await.unwrap();
        let fs = platform.fs();

        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        let content = b"Hello, World!";

        // Write file
        fs.write_file(&test_file, content).await.unwrap();

        // Check exists
        assert!(fs.exists(&test_file).await.unwrap());

        // Read file
        let read_content = fs.read_file(&test_file).await.unwrap();
        assert_eq!(read_content, content);

        // Get file size
        let size = fs.file_size(&test_file).await.unwrap();
        assert_eq!(size, content.len() as u64);

        // Remove file
        fs.remove_file(&test_file).await.unwrap();
        assert!(!fs.exists(&test_file).await.unwrap());
    }
}
