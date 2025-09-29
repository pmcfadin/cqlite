//! Common test utilities and shared test infrastructure
//!
//! This module provides reusable components for integration and unit tests
//! across the CQLite test suite.

pub mod enhanced_test_context;
pub mod sstable_test_utils;

// Re-export commonly used types and utilities
#[allow(unused_imports)]
pub use sstable_test_utils::TestContext;

// Re-export enhanced test infrastructure
#[cfg(feature = "test-infrastructure")]
pub use enhanced_test_context::{
    CoverageTracker, E2ESubcategory, EnhancedTestContext, EnhancedTestContextBuilder,
    EnhancedTestMetrics, IntegrationSubcategory, PerformanceSubcategory, PropertySubcategory,
    PropertyTestConfig, QualityGate, SchemaValidationConfig, TestCategory, UnitSubcategory,
};

// Common test configuration and setup helpers
use cqlite_core::Config;

/// Create a default test configuration optimized for testing
pub fn create_test_config() -> Config {
    let mut config = Config::default();

    // Optimize for testing - smaller cache sizes
    config.memory.block_cache.max_size = 8 * 1024 * 1024; // 8MB
    config.memory.row_cache.max_size = 4 * 1024 * 1024; // 4MB
    config.storage.max_sstable_size = 16 * 1024 * 1024; // 16MB for tests

    config
}

/// Initialize logging for tests if not already configured
pub fn init_test_logging() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();
}

/// Common test constants
pub mod constants {
    /// Default timeout for async operations in tests
    pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

    /// Expected minimum cache hit rate for performance tests
    pub const MIN_CACHE_HIT_RATE: f64 = 75.0;

    /// Maximum memory usage for tests (in MB)
    pub const MAX_MEMORY_USAGE_MB: usize = 256;

    /// Number of concurrent operations for stress tests
    pub const CONCURRENT_OPERATIONS: usize = 10;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = create_test_config();
        assert_eq!(config.memory.block_cache.max_size, 8 * 1024 * 1024);
        assert_eq!(config.memory.row_cache.max_size, 4 * 1024 * 1024);
    }

    #[test]
    fn test_logging_init() {
        // Should not panic
        init_test_logging();
    }
}
