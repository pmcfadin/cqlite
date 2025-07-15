//! Platform abstraction layer for cross-platform compatibility

use crate::{Config, Result};

pub mod fs;
pub mod threading;
pub mod time;

/// Platform abstraction layer
#[derive(Debug)]
pub struct Platform {
    /// File system abstraction
    fs: fs::FileSystem,

    /// Time utilities
    time: time::TimeProvider,

    /// Threading utilities
    threading: threading::ThreadingProvider,

    /// Configuration
    config: Config,
}

impl Platform {
    /// Create a new platform abstraction
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            fs: fs::FileSystem::new().await?,
            time: time::TimeProvider::new(),
            threading: threading::ThreadingProvider::new(),
            config: config.clone(),
        })
    }

    /// Get file system abstraction
    pub fn fs(&self) -> &fs::FileSystem {
        &self.fs
    }

    /// Get time provider
    pub fn time(&self) -> &time::TimeProvider {
        &self.time
    }

    /// Get threading provider
    pub fn threading(&self) -> &threading::ThreadingProvider {
        &self.threading
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

    #[test]
    fn test_time_provider() {
        let time = time::TimeProvider::new();

        let now_micros = time.now_micros();
        let now_millis = time.now_millis();
        let now_secs = time.now_secs();

        assert!(now_micros > 0);
        assert!(now_millis > 0);
        assert!(now_secs > 0);

        // Check relationships
        assert!(now_micros / 1000 >= now_millis);
        assert!(now_millis / 1000 >= now_secs);
    }

    #[tokio::test]
    async fn test_threading_provider() {
        let threading = threading::ThreadingProvider::new();

        let result = threading.execute_cpu_task(|| 42).await.unwrap();

        assert_eq!(result, 42);

        let result = threading
            .execute_io_task(|| "hello".to_string())
            .await
            .unwrap();

        assert_eq!(result, "hello");
    }
}
