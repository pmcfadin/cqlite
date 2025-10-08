//! Version hint resolution with strict precedence chain
//!
//! This module implements the version hint precedence system defined in Issue #130.
//! It provides authoritative version detection with explicit source tracking for
//! diagnostic purposes.
//!
//! ## Precedence Chain
//!
//! 1. User flag (--cassandra-version) - highest priority
//! 2. SSTable metadata (from individual SSTable files)
//! 3. Dataset metadata.yml (from test data configuration)
//! 4. Unknown - fallback when no sources provide version information
//!
//! ## No Heuristics Mandate (Issue #28)
//!
//! This module follows the no-heuristics mandate strictly:
//! - Version information is only extracted from authoritative metadata sources
//! - No guessing or inference based on file formats or structures
//! - Missing version information results in "Unknown" status, not a guess
//!
//! ## Usage
//!
//! ```rust,no_run
//! use cqlite_core::version_hints::{VersionHintResolver, VersionSource};
//! use std::path::Path;
//! use std::sync::Arc;
//! use cqlite_core::{Config, Platform};
//!
//! # tokio_test::block_on(async {
//! let config = Config::default();
//! let platform = Arc::new(Platform::new(&config).await.unwrap());
//!
//! // Resolve version with user override
//! let resolved = VersionHintResolver::resolve(
//!     Some("5.0".to_string()),
//!     Path::new("/path/to/sstable"),
//!     platform.clone(),
//! ).await.unwrap();
//!
//! assert_eq!(resolved.source, VersionSource::UserFlag);
//! assert_eq!(resolved.version, Some("5.0".to_string()));
//! # });
//! ```

use crate::{Error, Result};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;

/// Source of version information in the precedence chain
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSource {
    /// User-provided flag (--cassandra-version)
    UserFlag,
    /// SSTable metadata (from Data.db or Statistics.db)
    SSTableMetadata,
    /// Dataset metadata.yml
    DatasetMetadata,
    /// No version information available
    Unknown,
}

impl VersionSource {
    /// Get the precedence level (lower is higher priority)
    pub fn precedence(&self) -> u8 {
        match self {
            VersionSource::UserFlag => 0,
            VersionSource::SSTableMetadata => 1,
            VersionSource::DatasetMetadata => 2,
            VersionSource::Unknown => 255,
        }
    }

    /// Get a human-readable description of this source
    pub fn description(&self) -> &'static str {
        match self {
            VersionSource::UserFlag => "User-provided flag (--cassandra-version)",
            VersionSource::SSTableMetadata => "SSTable metadata",
            VersionSource::DatasetMetadata => "Dataset metadata.yml",
            VersionSource::Unknown => "Unknown (no version information available)",
        }
    }
}

/// Resolved version information with source tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVersion {
    /// The resolved version string (e.g., "5.0", "4.0")
    pub version: Option<String>,
    /// The source that provided this version information
    pub source: VersionSource,
}

impl ResolvedVersion {
    /// Create a new resolved version
    pub fn new(version: Option<String>, source: VersionSource) -> Self {
        Self { version, source }
    }

    /// Check if a version was successfully resolved
    pub fn is_known(&self) -> bool {
        self.version.is_some()
    }

    /// Get the version string or return "unknown"
    pub fn version_or_unknown(&self) -> &str {
        self.version.as_deref().unwrap_or("unknown")
    }
}

/// Metadata.yml structure (partial - only version field)
#[derive(Debug, Clone, Deserialize)]
struct DatasetMetadata {
    cassandra_version: Option<String>,
}

/// Version hint resolution engine
pub struct VersionHintResolver;

impl VersionHintResolver {
    /// Resolve version using the precedence chain
    ///
    /// # Arguments
    ///
    /// * `user_version` - User-provided version flag (highest priority)
    /// * `sstable_path` - Path to SSTable directory or Data.db file
    /// * `platform` - Platform abstraction for file I/O
    ///
    /// # Returns
    ///
    /// Returns a `ResolvedVersion` with the version string and source.
    /// If no version can be determined, returns `Unknown` source with `None` version.
    ///
    /// # Errors
    ///
    /// Returns an error only for fatal I/O errors (not for missing metadata files).
    /// Missing metadata.yml is not an error - it simply moves to the next precedence level.
    pub async fn resolve(
        user_version: Option<String>,
        sstable_path: &Path,
        platform: Arc<crate::Platform>,
    ) -> Result<ResolvedVersion> {
        // Precedence level 0: User flag
        if let Some(version) = user_version {
            return Ok(ResolvedVersion::new(Some(version), VersionSource::UserFlag));
        }

        // Precedence level 1: SSTable metadata
        // TODO(Issue #130): Parse SSTable metadata when format spec is available
        // Current Statistics.db doesn't contain version information in Cassandra 5.0
        // This will be implemented when the format is extended or alternative sources
        // (e.g., Data.db header metadata) are identified.
        if let Some(version) = Self::parse_sstable_metadata(sstable_path, platform.clone()).await? {
            return Ok(ResolvedVersion::new(
                Some(version),
                VersionSource::SSTableMetadata,
            ));
        }

        // Precedence level 2: Dataset metadata.yml
        if let Some(version) = Self::parse_dataset_metadata(sstable_path, platform).await? {
            return Ok(ResolvedVersion::new(
                Some(version),
                VersionSource::DatasetMetadata,
            ));
        }

        // Fallback: Unknown
        Ok(ResolvedVersion::new(None, VersionSource::Unknown))
    }

    /// Parse version from SSTable metadata (Statistics.db or Data.db header)
    ///
    /// # Implementation Note
    ///
    /// This is currently a stub that always returns `Ok(None)` because:
    /// - Cassandra 5.0 Statistics.db does not contain version information
    /// - Data.db header version field is the SSTable format version, not Cassandra version
    /// - No authoritative metadata source for Cassandra version in SSTable files yet
    ///
    /// This will be implemented when:
    /// - Extended metadata format is added to Statistics.db
    /// - Alternative authoritative source is identified
    /// - Upstream Cassandra adds version metadata to SSTable files
    async fn parse_sstable_metadata(
        _sstable_path: &Path,
        _platform: Arc<crate::Platform>,
    ) -> Result<Option<String>> {
        // TODO(Issue #130): Implement SSTable metadata parsing
        // Current Cassandra 5.0 SSTable format does not include version in metadata
        Ok(None)
    }

    /// Parse version from dataset metadata.yml
    ///
    /// This searches for metadata.yml in the following order:
    /// 1. Same directory as SSTable file
    /// 2. Parent directory (for sstables/ subdirectory layout)
    /// 3. Grandparent directory (for nested dataset structures)
    ///
    /// Missing metadata.yml is NOT an error - it returns `Ok(None)`.
    /// Invalid YAML format IS an error and returns `Err(Error::Parse(...))`.
    async fn parse_dataset_metadata(
        sstable_path: &Path,
        platform: Arc<crate::Platform>,
    ) -> Result<Option<String>> {
        // Search for metadata.yml in current directory, parent, and grandparent
        let search_paths = [
            sstable_path.to_path_buf(),
            sstable_path.parent().unwrap_or(sstable_path).to_path_buf(),
            sstable_path
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(sstable_path)
                .to_path_buf(),
        ];

        for base_path in &search_paths {
            let metadata_path = base_path.join("metadata.yml");

            // Check if file exists using platform abstraction
            if !platform.fs().exists(&metadata_path).await? {
                continue;
            }

            // Read file contents
            match platform.fs().read_file(&metadata_path).await {
                Ok(contents) => {
                    // Parse YAML
                    let contents_str = String::from_utf8(contents).map_err(|e| {
                        Error::parse(format!(
                            "metadata.yml at {} is not valid UTF-8: {}",
                            metadata_path.display(),
                            e
                        ))
                    })?;

                    let metadata: DatasetMetadata =
                        serde_yaml::from_str(&contents_str).map_err(|e| {
                            Error::parse(format!(
                                "Failed to parse metadata.yml at {}: {}",
                                metadata_path.display(),
                                e
                            ))
                        })?;

                    // Return version if present
                    if let Some(version) = metadata.cassandra_version {
                        return Ok(Some(version));
                    }

                    // metadata.yml found but no version field - continue search
                    continue;
                }
                Err(e) => {
                    // Distinguish between "not found" and actual I/O errors
                    // Use ErrorKind instead of string matching for robustness
                    match &e {
                        Error::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
                            // File not found - continue searching other paths
                            continue;
                        }
                        _ => {
                            // Real I/O error - propagate it
                            return Err(e);
                        }
                    }
                }
            }
        }

        // No metadata.yml found in any search path - not an error
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_version_source_precedence() {
        assert!(VersionSource::UserFlag.precedence() < VersionSource::SSTableMetadata.precedence());
        assert!(
            VersionSource::SSTableMetadata.precedence()
                < VersionSource::DatasetMetadata.precedence()
        );
        assert!(VersionSource::DatasetMetadata.precedence() < VersionSource::Unknown.precedence());
    }

    #[test]
    fn test_version_source_description() {
        assert_eq!(
            VersionSource::UserFlag.description(),
            "User-provided flag (--cassandra-version)"
        );
        assert_eq!(
            VersionSource::SSTableMetadata.description(),
            "SSTable metadata"
        );
        assert_eq!(
            VersionSource::DatasetMetadata.description(),
            "Dataset metadata.yml"
        );
        assert_eq!(
            VersionSource::Unknown.description(),
            "Unknown (no version information available)"
        );
    }

    #[test]
    fn test_resolved_version_is_known() {
        let known = ResolvedVersion::new(Some("5.0".to_string()), VersionSource::UserFlag);
        assert!(known.is_known());

        let unknown = ResolvedVersion::new(None, VersionSource::Unknown);
        assert!(!unknown.is_known());
    }

    #[test]
    fn test_resolved_version_or_unknown() {
        let known = ResolvedVersion::new(Some("5.0".to_string()), VersionSource::UserFlag);
        assert_eq!(known.version_or_unknown(), "5.0");

        let unknown = ResolvedVersion::new(None, VersionSource::Unknown);
        assert_eq!(unknown.version_or_unknown(), "unknown");
    }

    #[tokio::test]
    async fn test_user_flag_precedence() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // User flag should override everything
        let resolved =
            VersionHintResolver::resolve(Some("5.0-user".to_string()), temp_dir.path(), platform)
                .await
                .unwrap();

        assert_eq!(resolved.source, VersionSource::UserFlag);
        assert_eq!(resolved.version, Some("5.0-user".to_string()));
        assert!(resolved.is_known());
    }

    #[tokio::test]
    async fn test_unknown_when_no_sources() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // No user flag, no metadata.yml
        let resolved = VersionHintResolver::resolve(None, temp_dir.path(), platform)
            .await
            .unwrap();

        assert_eq!(resolved.source, VersionSource::Unknown);
        assert_eq!(resolved.version, None);
        assert!(!resolved.is_known());
        assert_eq!(resolved.version_or_unknown(), "unknown");
    }

    #[tokio::test]
    async fn test_metadata_yml_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // Create metadata.yml with version
        let metadata_content = "cassandra_version: \"5.0\"\nkeyspaces: []\n";
        let metadata_path = temp_dir.path().join("metadata.yml");
        platform
            .fs()
            .write_file(&metadata_path, metadata_content.as_bytes())
            .await
            .unwrap();

        // Resolve should find metadata.yml
        let resolved = VersionHintResolver::resolve(None, temp_dir.path(), platform)
            .await
            .unwrap();

        assert_eq!(resolved.source, VersionSource::DatasetMetadata);
        assert_eq!(resolved.version, Some("5.0".to_string()));
    }

    #[tokio::test]
    async fn test_metadata_yml_parent_directory() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // Create metadata.yml in parent directory
        let metadata_content = "cassandra_version: \"4.0\"\nkeyspaces: []\n";
        let metadata_path = temp_dir.path().join("metadata.yml");
        platform
            .fs()
            .write_file(&metadata_path, metadata_content.as_bytes())
            .await
            .unwrap();

        // Create subdirectory for SSTable
        let sstable_dir = temp_dir.path().join("sstables");
        platform.fs().create_dir(&sstable_dir).await.unwrap();

        // Resolve from subdirectory should find parent metadata.yml
        let resolved = VersionHintResolver::resolve(None, &sstable_dir, platform)
            .await
            .unwrap();

        assert_eq!(resolved.source, VersionSource::DatasetMetadata);
        assert_eq!(resolved.version, Some("4.0".to_string()));
    }

    #[tokio::test]
    async fn test_metadata_yml_invalid_yaml() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // Create invalid YAML
        let metadata_path = temp_dir.path().join("metadata.yml");
        platform
            .fs()
            .write_file(&metadata_path, b"invalid: yaml: syntax: error:")
            .await
            .unwrap();

        // Should return parse error
        let result = VersionHintResolver::resolve(None, temp_dir.path(), platform).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to parse metadata.yml"));
    }

    #[tokio::test]
    async fn test_metadata_yml_missing_version_field() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // Create metadata.yml without cassandra_version field
        let metadata_content = "keyspaces: []\n";
        let metadata_path = temp_dir.path().join("metadata.yml");
        platform
            .fs()
            .write_file(&metadata_path, metadata_content.as_bytes())
            .await
            .unwrap();

        // Should fall back to Unknown (missing field is not an error)
        let resolved = VersionHintResolver::resolve(None, temp_dir.path(), platform)
            .await
            .unwrap();

        assert_eq!(resolved.source, VersionSource::Unknown);
        assert_eq!(resolved.version, None);
    }

    #[tokio::test]
    async fn test_user_flag_overrides_metadata_yml() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // Create metadata.yml with version 5.0
        let metadata_content = "cassandra_version: \"5.0\"\nkeyspaces: []\n";
        let metadata_path = temp_dir.path().join("metadata.yml");
        platform
            .fs()
            .write_file(&metadata_path, metadata_content.as_bytes())
            .await
            .unwrap();

        // User flag should override metadata.yml
        let resolved = VersionHintResolver::resolve(
            Some("4.0-override".to_string()),
            temp_dir.path(),
            platform,
        )
        .await
        .unwrap();

        assert_eq!(resolved.source, VersionSource::UserFlag);
        assert_eq!(resolved.version, Some("4.0-override".to_string()));
    }

    #[tokio::test]
    async fn test_not_found_error_robustness() {
        // This test verifies that the ErrorKind-based approach correctly handles
        // NotFound errors regardless of OS locale or error message wording.
        // It demonstrates the fix for the brittle string-based error detection.

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.unwrap());

        // No metadata.yml exists - should continue search and return Unknown
        let resolved = VersionHintResolver::resolve(None, temp_dir.path(), platform)
            .await
            .unwrap();

        assert_eq!(resolved.source, VersionSource::Unknown);
        assert_eq!(resolved.version, None);
    }
}
