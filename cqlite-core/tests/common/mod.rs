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
// Note: These re-exports are available when the test-infrastructure feature is enabled
// Currently commented out as they are not yet used in tests
#[cfg(feature = "test-infrastructure")]
#[allow(unused_imports)]
pub use enhanced_test_context::{
    CoverageTracker, EnhancedTestContext, EnhancedTestContextBuilder, EnhancedTestMetrics,
    IntegrationSubcategory, QualityGate, TestCategory, UnitSubcategory,
};

// Common test configuration and setup helpers
use cqlite_core::Config;

/// Create a default test configuration optimized for testing
pub fn create_test_config() -> Config {
    let mut config = Config::default();

    // Optimize for testing - smaller cache sizes. `row_cache` was removed in
    // issue #1568; `block_cache.max_size` is the single real cache knob.
    config.memory.block_cache.max_size = 8 * 1024 * 1024; // 8MB
    config.storage.max_sstable_size = 16 * 1024 * 1024; // 16MB for tests

    config
}

/// Initialize logging for tests if not already configured
pub fn init_test_logging() {
    let _ = env_logger::builder()
        .parse_filters("debug")
        .is_test(true)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = create_test_config();
        assert_eq!(config.memory.block_cache.max_size, 8 * 1024 * 1024);
    }

    #[test]
    fn test_logging_init() {
        // Should not panic
        init_test_logging();
    }
}
