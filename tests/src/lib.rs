//! Comprehensive Test Library for CQLite Cassandra 5+ Compatibility
//!
//! This library provides a complete test suite for validating CQLite's
//! compatibility with Cassandra 5+ SSTable format and functionality.

// REPL Testing Modules
pub mod repl_integration_tests;
pub mod repl_quality_gates;

pub mod compatibility_framework;
pub mod integration_runner;
#[cfg(feature = "benchmarks")]
pub mod performance_benchmarks;
pub mod sstable_format_tests;
pub mod type_system_tests;

// SSTable validation and testing modules
pub mod complex_data_test;
pub mod format_verifier;
#[cfg(feature = "benchmarks")]
pub mod sstable_benchmark;
pub mod sstable_validator;

// Advanced Performance Testing Modules
#[cfg(feature = "benchmarks")]
pub mod performance_benchmark_runner;
#[cfg(feature = "benchmarks")]
pub mod performance_regression_framework;
#[cfg(feature = "benchmarks")]
pub mod performance_validation_suite;

// Existing modules
pub mod integration {
    pub mod cli_tests;
}

// End-to-end integration tests
pub mod integration_e2e;
pub mod smoke_test_baseline;

pub mod benchmarks {
    // Benchmark modules are defined as separate files with [[bench]] sections
}

pub mod fixtures {
    pub mod helpers;
    pub mod test_data;
}

#[cfg(feature = "benchmarks")]
pub mod performance_monitor;

/// Comprehensive parser validation tests against real Cassandra 5+ data
pub mod parser_validation;

pub mod bti_comprehensive_validation;
pub mod bti_encoder_tests;
pub mod bti_integration_tests;
pub mod bti_tdd_tests;
pub mod bti_test_data;
/// BTI (Big Trie Index) format validation tests for Cassandra 5.0+
pub mod bti_validation;

/// Comprehensive SSTable test suite for Issue #17
pub mod comprehensive_sstable_test_suite;

// Re-export main test runner functions
pub use integration_runner::{
    IntegrationTestConfig, IntegrationTestResults, IntegrationTestRunner,
    run_compatibility_validation, run_performance_validation, run_quick_compatibility_check,
};

// Re-export test framework components
pub use compatibility_framework::{
    CompatibilityTestConfig, CompatibilityTestFramework, CompatibilityTestResult,
};

// Re-export CLI integration test components
pub use cli_integration_tests::{CLIIntegrationTestSuite, CLITestConfig, CLITestResult};

// Re-export SSTable test fixture components (initial export)
// Note: real_sstable_test_fixtures is exported again below with additional types

#[cfg(feature = "benchmarks")]
pub use performance_benchmarks::{
    BenchmarkConfig, BenchmarkResult as PerfBenchmarkResult, PerformanceBenchmarks,
};
pub use sstable_format_tests::SSTableFormatTests;
pub use type_system_tests::TypeSystemTests;

// Re-export advanced performance testing components
#[cfg(feature = "benchmarks")]
pub use performance_benchmark_runner::{
    BenchmarkRunnerConfig, PerformanceBenchmarkRunner, PerformanceResults, TestConfiguration,
};
#[cfg(feature = "benchmarks")]
pub use performance_regression_framework::{
    PerformanceRegressionFramework, RegressionTestConfig, RegressionTestResult,
};
#[cfg(feature = "benchmarks")]
pub use performance_validation_suite::{
    PerformanceValidationConfig, PerformanceValidationResults, PerformanceValidationSuite,
};

// Re-export commonly used test utilities
pub use fixtures::{helpers, test_data};
#[cfg(feature = "benchmarks")]
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
    EdgeCaseConfig, EdgeCaseResults, EdgeCaseRunner, run_comprehensive_edge_case_tests,
    run_edge_case_tests_with_config,
};

// Comprehensive integration testing modules
pub mod cli_integration_tests;
pub mod collection_compatibility_tests;
pub mod comprehensive_integration_tests;
pub mod real_sstable_test_fixtures;
pub mod validation_test_runner;

// Validation modules
pub mod validation;

// Re-export comprehensive integration testing components (commented out to avoid conflicts with cli_integration_tests)
// pub use integration::cli_tests::{get_cli_binary, create_temp_db};
pub use collection_compatibility_tests::{
    CollectionCompatibilityTester, PerformanceMetrics as CollectionPerformanceMetrics,
    TestResult as CollectionTestResult,
};
pub use comprehensive_sstable_test_suite::{
    ComprehensiveSSTableTestSuite, PerformanceMetrics, TestDetails, TestResult, TestStatus,
    TestSuiteReport, run_comprehensive_sstable_tests,
};
pub use real_sstable_test_fixtures::{
    SSTableTestFixture, SSTableTestFixtureConfig, SSTableTestFixtureGenerator,
    SSTableTestFixtureValidator, ValidationResult as SSTableValidationResult,
};
pub use validation_test_runner::{
    CLIValidationResult, FixtureValidationResult, PerformanceValidationResult, ReportFormat,
    ValidationTestConfig, ValidationTestResults, ValidationTestRunner,
};

// Complex Type Validation Modules - M3 Validation Engineer
pub mod complex_type_validation_suite;
#[cfg(feature = "benchmarks")]
pub mod performance_complex_types_benchmark;
pub mod real_cassandra_data_validator;

// Re-export complex type validation components
pub use complex_type_validation_suite::{
    ComplexTypeValidationConfig, ComplexTypeValidationResults, ComplexTypeValidationSuite,
};
#[cfg(feature = "benchmarks")]
pub use performance_complex_types_benchmark::{
    ComplexTypeBenchmarkConfig, ComplexTypeBenchmarkResults, ComplexTypePerformanceBenchmark,
};
pub use real_cassandra_data_validator::{
    RealCassandraDataValidator, RealDataValidationConfig, RealDataValidationResults,
};

// Re-export BTI validation components for Issue #36 (temporarily disabled for Issue #35 compilation)
// pub use bti_validation::{
//     BtiValidationSuite, BtiValidationConfig, BtiTestDataset, BtiTestValue,
//     BtiDatasetValidationResult, ValidationStatus, TrieTraversalResult,
//     RowsDecodingResult, ByteComparableValidationResult, BtiPerformanceMetrics,
// };
// pub use bti_comprehensive_validation::{
//     BtiComprehensiveValidator, BtiValidationConfig as ComprehensiveBtiValidationConfig,
// };

// New comprehensive integration test suite
pub mod comprehensive_integration_test_suite;
pub mod integration_test_harness;

// Re-export new comprehensive testing components
pub use comprehensive_integration_test_suite::{
    ComprehensiveIntegrationTestSuite,
    IntegrationTestConfig as ComprehensiveTestConfig, IntegrationTestSuiteResults,
    print_integration_test_results, run_comprehensive_integration_tests,
    run_quick_integration_tests,
};
pub use integration_test_harness::{
    MemoryMonitor, PerformanceMeasurement, PerformanceMeasurer, SSTableFileFinder, TableInfo,
    TestCase, TestCaseBuilder, TestDataValidator, TestEnvironmentStatus, TestOutcome,
    TestResultAggregator, TestSummary, TestTimer,
};

// Re-export SSTable validation components
pub use complex_data_test::{ComplexDataTestResults, ComplexDataTestSuite, run_complex_data_tests};
pub use format_verifier::{FormatVerificationResult, SSTableFormatVerifier, verify_sstable_format};
#[cfg(feature = "benchmarks")]
pub use sstable_benchmark::{
    BenchmarkConfig as SSTableBenchmarkConfig, BenchmarkResults, SSTableBenchmark,
    run_comprehensive_benchmark,
};
pub use sstable_validator::{SSTableValidator, run_validation};

// Minimal smoke tests for baseline
#[cfg(test)]
pub mod minimal_smoke_tests;

// CQL Schema Validation Modules
pub mod cql_integration_tests;
pub mod cql_parser_validation_suite;
#[cfg(feature = "benchmarks")]
pub mod cql_performance_benchmarks;
pub mod cql_test_data_fixtures;

// ComparatorType Testing Module
#[cfg(test)]
pub mod comparator_type_tests;

// Re-export CQL validation components
pub use cql_integration_tests::{
    CqlIntegrationTestSuite, IntegrationTestReport, IntegrationTestResult,
};
pub use cql_parser_validation_suite::{
    CqlParserValidationSuite, PerformanceMetric, ValidationReport,
    ValidationResult as CqlValidationResult,
};
#[cfg(feature = "benchmarks")]
pub use cql_performance_benchmarks::{
    BenchmarkReport, BenchmarkResult as CqlBenchmarkResult, CqlPerformanceBenchmarkSuite,
    PerformanceTargets,
};
pub use cql_test_data_fixtures::{
    CompatibilityTestFixtures, CqlTestCase, ErrorTestCase, JsonSchemaFixtures, PerformanceTestData,
    TypeTestCase,
};

// Issue #35 specific validation modules
// Main working tests:
pub mod issue_35_key_encoding_tests;
pub mod issue_35_minimal_tests;

// Shared test support utilities
pub mod support;

// Issue #28a - Removal of header heuristics and blob fallbacks
#[cfg(test)]
pub mod issue_28a_heuristics_removal_tests;

// Re-enabled modules for Issue #35
pub mod issue_35_validation_tests;
#[cfg(test)]
pub mod issue_35_live_integration_tests;
#[cfg(test)]
pub mod issue_35_sstabledump_validation;
#[cfg(test)]
pub mod wide_partition_test_generator;

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
