//! Comprehensive Test Library for CQLite Cassandra 5+ Compatibility
//!
//! This library provides a complete test suite for validating CQLite's
//! compatibility with Cassandra 5+ SSTable format and functionality.

pub mod compatibility_framework;
pub mod integration_runner;
pub mod performance_benchmarks;
pub mod sstable_format_tests;
pub mod type_system_tests;

// Existing modules
pub mod integration {
    pub mod cli_tests;
}

pub mod benchmarks {
    // Benchmark modules are defined as separate files with [[bench]] sections
}

pub mod fixtures {
    pub mod helpers;
    pub mod test_data;
}

pub mod performance_monitor;

/// Comprehensive parser validation tests against real Cassandra 5+ data
pub mod parser_validation;

/// BTI (Big Trie Index) format validation tests for Cassandra 5.0+
pub mod bti_validation;

// Re-export main test runner functions
pub use integration_runner::{
    run_compatibility_validation, run_performance_validation, run_quick_compatibility_check,
    IntegrationTestConfig, IntegrationTestResults, IntegrationTestRunner,
};

// Re-export test framework components
pub use compatibility_framework::{
    CompatibilityTestConfig, CompatibilityTestFramework, CompatibilityTestResult,
};

pub use performance_benchmarks::{BenchmarkConfig, BenchmarkResult, PerformanceBenchmarks};
pub use sstable_format_tests::SSTableFormatTests;
pub use type_system_tests::TypeSystemTests;

// Re-export commonly used test utilities
pub use fixtures::{helpers, test_data};
pub use performance_monitor::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_test_crate_loads() {
        // Basic smoke test to ensure the crate loads properly
        assert!(true);
    }

    #[tokio::test]
    async fn test_compatibility_framework_creation() {
        let config = CompatibilityTestConfig::default();
        let framework = CompatibilityTestFramework::new(config);
        assert!(framework.is_ok());
    }
}
