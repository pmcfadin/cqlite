//! Comprehensive Test Library for CQLite Cassandra 5+ Compatibility
//!
//! This library provides a complete test suite for validating CQLite's
//! compatibility with Cassandra 5+ SSTable format and functionality.

pub mod compatibility_framework;
pub mod integration_runner;
pub mod performance_benchmarks;
pub mod sstable_format_tests;
pub mod type_system_tests;

// SSTable validation and testing modules
pub mod sstable_validator;
pub mod format_verifier;
pub mod sstable_benchmark;
pub mod complex_data_test;

// Advanced Performance Testing Modules
pub mod performance_benchmark_runner;
pub mod performance_regression_framework;
pub mod performance_validation_suite;

// Existing modules
pub mod integration {
    pub mod cli_tests;
}

// End-to-end integration tests
pub mod integration_e2e;

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

// Re-export advanced performance testing components
pub use performance_benchmark_runner::{
    BenchmarkRunnerConfig, PerformanceBenchmarkRunner, PerformanceResults,
};
pub use performance_regression_framework::{
    PerformanceRegressionFramework, RegressionTestConfig, RegressionTestResult,
};
pub use performance_validation_suite::{
    PerformanceValidationConfig, PerformanceValidationResults, PerformanceValidationSuite,
};

// Re-export commonly used test utilities
pub use fixtures::{helpers, test_data};
pub use performance_monitor::*;

// Edge case testing modules for comprehensive compatibility validation
pub mod edge_case_data_types;
pub mod edge_case_runner;
pub mod edge_case_sstable_corruption;
pub mod edge_case_stress_testing;

// Real SSTable compatibility testing against actual Cassandra 5 files
pub mod real_sstable_compatibility_test;

// Re-export edge case testing components
pub use edge_case_runner::{
    run_comprehensive_edge_case_tests, run_edge_case_tests_with_config, EdgeCaseConfig,
    EdgeCaseResults, EdgeCaseRunner,
};

// Comprehensive integration testing modules
pub mod cli_integration_tests;
pub mod collection_compatibility_tests;
pub mod comprehensive_integration_tests;
pub mod real_sstable_test_fixtures;
pub mod validation_test_runner;

// Re-export comprehensive integration testing components
pub use cli_integration_tests::{CLIIntegrationTestSuite, CLITestConfig, CLITestResult};
pub use collection_compatibility_tests::{
    CollectionCompatibilityTester, TestResult as CollectionTestResult, 
    PerformanceMetrics as CollectionPerformanceMetrics,
};
pub use comprehensive_integration_tests::{
    ComprehensiveIntegrationTestSuite, IntegrationTestConfig, IntegrationTestResults,
    PerformanceMetrics, TestReport, TestStatus,
};
pub use real_sstable_test_fixtures::{
    SSTableTestFixture, SSTableTestFixtureConfig, SSTableTestFixtureGenerator,
    SSTableTestFixtureValidator, ValidationResult,
};
pub use validation_test_runner::{
    CLIValidationResult, FixtureValidationResult, PerformanceValidationResult, ReportFormat,
    ValidationTestConfig, ValidationTestResults, ValidationTestRunner,
};

// Complex Type Validation Modules - M3 Validation Engineer
pub mod complex_type_validation_suite;
pub mod real_cassandra_data_validator;
pub mod performance_complex_types_benchmark;

// Re-export complex type validation components
pub use complex_type_validation_suite::{
    ComplexTypeValidationConfig, ComplexTypeValidationResults, ComplexTypeValidationSuite,
};
pub use real_cassandra_data_validator::{
    RealCassandraDataValidator, RealDataValidationConfig, RealDataValidationResults,
};
pub use performance_complex_types_benchmark::{
    ComplexTypePerformanceBenchmark, ComplexTypeBenchmarkConfig, ComplexTypeBenchmarkResults,
};

// Re-export SSTable validation components
pub use sstable_validator::{run_validation, SSTableValidator};
pub use format_verifier::{verify_sstable_format, SSTableFormatVerifier, FormatVerificationResult};
pub use sstable_benchmark::{run_comprehensive_benchmark, BenchmarkConfig, BenchmarkResults, SSTableBenchmark};
pub use complex_data_test::{run_complex_data_tests, ComplexDataTestSuite, ComplexDataTestResults};

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
