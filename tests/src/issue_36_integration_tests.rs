//! Integration Tests for Issue #36: BTI Validation Suite Implementation
//! 
//! This module provides integration tests that validate the comprehensive BTI validation suite
//! implementation against all requirements specified in Issue #36.

use crate::bti_comprehensive_validation::{BtiComprehensiveValidator, BtiValidationConfig};
use crate::bti_validation::{ValidationStatus, BtiValidationSuite};
use std::time::Duration;

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_issue_36_comprehensive_bti_validation() {
        println!("🚀 Running Issue #36 comprehensive BTI validation integration test");
        
        let config = BtiValidationConfig {
            enable_sstabledump_parity: true,
            test_complex_scenarios: true,
            generate_test_data: true,
            max_test_data_size_mb: 50, // Reduced for CI
            enable_performance_tests: true,
            require_zero_diff_parity: true,
            enable_trie_traversal_validation: true,
            enable_byte_comparable_validation: true,
        };

        let mut validator = BtiComprehensiveValidator::new(config)
            .expect("Failed to create BTI comprehensive validator");

        let results = validator.run_comprehensive_validation()
            .expect("Failed to run comprehensive BTI validation");

        // Verify that all required datasets were tested
        assert!(!results.is_empty(), "No validation results generated");
        
        // Verify required dataset coverage for Issue #36
        let dataset_names: Vec<&String> = results.iter().map(|r| &r.dataset_name).collect();
        assert!(dataset_names.contains(&&"multi_component_partition_keys".to_string()), 
            "Missing multi-component partition keys test");
        assert!(dataset_names.contains(&&"nested_collections_udts".to_string()), 
            "Missing nested collections/UDTs test");
        assert!(dataset_names.contains(&&"wide_partitions".to_string()), 
            "Missing wide partitions test");
        assert!(dataset_names.contains(&&"cep25_type_hierarchy".to_string()), 
            "Missing CEP-25 type hierarchy test");
        assert!(dataset_names.contains(&&"range_tombstones_metadata".to_string()), 
            "Missing range tombstones test");

        // Verify that critical validations passed
        let failed_results: Vec<_> = results.iter()
            .filter(|r| r.status == ValidationStatus::Failed)
            .collect();
        
        if !failed_results.is_empty() {
            eprintln!("❌ Failed BTI validation datasets:");
            for result in &failed_results {
                eprintln!("  - {}: {:?}", result.dataset_name, result.status);
                for error in &result.validation_errors {
                    eprintln!("    Error: {}", error.message);
                }
            }
        }
        
        // For CI, we allow some partial passes but no complete failures
        let completely_failed_count = failed_results.len();
        assert!(completely_failed_count == 0, 
            "Found {} completely failed BTI validations - this blocks Issue #36 completion", 
            completely_failed_count);

        // Verify performance requirements
        for result in &results {
            assert!(result.performance_metrics.total_time_ms < 30000, 
                "Validation took too long: {}ms for dataset {}", 
                result.performance_metrics.total_time_ms, result.dataset_name);
            
            if result.performance_metrics.throughput_ops_per_sec > 0.0 {
                assert!(result.performance_metrics.throughput_ops_per_sec >= 100.0, 
                    "Throughput too low: {} ops/sec for dataset {}", 
                    result.performance_metrics.throughput_ops_per_sec, result.dataset_name);
            }
        }

        println!("✅ Issue #36 comprehensive BTI validation integration test completed successfully");
        println!("📊 Tested {} datasets with comprehensive coverage", results.len());
    }

    #[test]
    fn test_issue_36_requirements_coverage() {
        println!("🔍 Testing Issue #36 requirements coverage");
        
        let suite = BtiValidationSuite::new();
        
        // Test comprehensive dataset generation
        let datasets = suite.generate_comprehensive_test_datasets()
            .expect("Failed to generate comprehensive test datasets");
        
        assert!(!datasets.is_empty(), "No test datasets generated");
        
        // Verify Issue #36 requirement coverage
        let mut has_multi_component = false;
        let mut has_complex_types = false;
        let mut has_wide_partitions = false;
        let mut has_range_tombstones = false;
        
        for dataset in &datasets {
            // Check for multi-component partition keys
            if dataset.partition_keys.iter().any(|pk| pk.len() > 1) {
                has_multi_component = true;
            }
            
            // Check for complex types (UDTs, collections)
            if dataset.name.contains("udts") || dataset.name.contains("collections") {
                has_complex_types = true;
            }
            
            // Check for wide partitions
            if dataset.has_wide_partitions {
                has_wide_partitions = true;
            }
            
            // Check for range tombstones
            if dataset.has_range_tombstones {
                has_range_tombstones = true;
            }
        }
        
        assert!(has_multi_component, "Missing multi-component partition key coverage");
        assert!(has_complex_types, "Missing complex types coverage");
        assert!(has_wide_partitions, "Missing wide partitions coverage");
        assert!(has_range_tombstones, "Missing range tombstones coverage");
        
        println!("✅ All Issue #36 requirements covered in test datasets");
    }

    #[test]
    fn test_bti_validation_performance_guardrails() {
        println!("📊 Testing BTI validation performance guardrails");
        
        let config = BtiValidationConfig::default();
        let validator = BtiComprehensiveValidator::new(config)
            .expect("Failed to create validator");
        
        // Test dataset generation performance
        let start = std::time::Instant::now();
        let datasets = validator.generate_comprehensive_test_datasets()
            .expect("Failed to generate datasets");
        let generation_time = start.elapsed();
        
        // Performance guardrails
        assert!(generation_time < Duration::from_secs(5), 
            "Dataset generation took too long: {:?}", generation_time);
        assert!(datasets.len() >= 5, 
            "Not enough test datasets generated: {}", datasets.len());
        
        // Memory usage check (approximate)
        let total_data_points = datasets.iter()
            .map(|d| d.partition_keys.len() + d.clustering_keys.len())
            .sum::<usize>();
        
        assert!(total_data_points >= 1000, 
            "Not enough test data points generated: {}", total_data_points);
        assert!(total_data_points <= 100000, 
            "Too many test data points (memory concern): {}", total_data_points);
        
        println!("✅ BTI validation performance guardrails passed");
        println!("  - Dataset generation: {:?}", generation_time);
        println!("  - Total datasets: {}", datasets.len());
        println!("  - Total data points: {}", total_data_points);
    }

    #[test]
    fn test_bti_validation_error_handling() {
        println!("🚧 Testing BTI validation error handling");
        
        let config = BtiValidationConfig {
            enable_sstabledump_parity: false, // Disable to test without external deps
            test_complex_scenarios: true,
            generate_test_data: true,
            max_test_data_size_mb: 10,
            enable_performance_tests: false,
            require_zero_diff_parity: false,
            enable_trie_traversal_validation: false, // Disable to test partial validation
            enable_byte_comparable_validation: true,
        };

        let validator = BtiComprehensiveValidator::new(config);
        assert!(validator.is_ok(), "Should be able to create validator with partial config");
        
        let mut validator = validator.unwrap();
        
        // Test that validation runs even with some components disabled
        let results = validator.run_comprehensive_validation();
        assert!(results.is_ok(), "Validation should succeed even with partial configuration");
        
        let results = results.unwrap();
        assert!(!results.is_empty(), "Should still generate validation results");
        
        // Verify that disabled validations are handled gracefully
        for result in &results {
            // Trie traversal should be skipped/mocked but not fail
            assert!(result.trie_traversal_result.nodes_visited == 0 || 
                    result.trie_traversal_result.traversal_complete,
                "Trie traversal should be handled gracefully when disabled");
        }
        
        println!("✅ BTI validation error handling test passed");
    }

    #[test] 
    fn test_bti_ci_integration_readiness() {
        println!("🔗 Testing BTI CI integration readiness for Issue #36");
        
        // Test that all required components are available for CI
        let config = BtiValidationConfig::default();
        let validator = BtiComprehensiveValidator::new(config);
        assert!(validator.is_ok(), "BTI validator should be ready for CI");
        
        // Test that validation can run without external dependencies in CI mode
        let ci_config = BtiValidationConfig {
            enable_sstabledump_parity: false, // CI might not have Cassandra tools
            test_complex_scenarios: true,
            generate_test_data: true,
            max_test_data_size_mb: 25, // Reasonable for CI
            enable_performance_tests: true,
            require_zero_diff_parity: false, // Can't enforce without sstabledump
            enable_trie_traversal_validation: true,
            enable_byte_comparable_validation: true,
        };
        
        let mut ci_validator = BtiComprehensiveValidator::new(ci_config)
            .expect("Should be able to create CI-compatible validator");
        
        let ci_results = ci_validator.run_comprehensive_validation()
            .expect("CI validation should run successfully");
        
        assert!(!ci_results.is_empty(), "CI should generate validation results");
        
        // Verify that CI results are meaningful even without full parity validation
        let successful_validations = ci_results.iter()
            .filter(|r| matches!(r.status, ValidationStatus::Passed | ValidationStatus::PartiallyPassed))
            .count();
        
        assert!(successful_validations > 0, 
            "CI should have at least some successful validations");
        
        // Verify performance is reasonable for CI
        let max_time = ci_results.iter()
            .map(|r| r.performance_metrics.total_time_ms)
            .max()
            .unwrap_or(0);
        
        assert!(max_time < 10000, 
            "CI validation should complete within 10 seconds per dataset, got {}ms", max_time);
        
        println!("✅ BTI validation ready for CI integration");
        println!("  - {} datasets validated", ci_results.len());
        println!("  - {} successful validations", successful_validations);
        println!("  - Max validation time: {}ms", max_time);
    }
}

#[cfg(test)]
mod performance_integration_tests {
    use super::*;

    #[test]
    #[ignore] // Run with --ignored for performance testing
    fn test_bti_validation_full_performance_suite() {
        println!("🏎️  Running full BTI validation performance suite");
        
        let config = BtiValidationConfig {
            enable_sstabledump_parity: true,
            test_complex_scenarios: true,
            generate_test_data: true,
            max_test_data_size_mb: 200, // Larger dataset for performance testing
            enable_performance_tests: true,
            require_zero_diff_parity: true,
            enable_trie_traversal_validation: true,
            enable_byte_comparable_validation: true,
        };

        let mut validator = BtiComprehensiveValidator::new(config)
            .expect("Failed to create performance validator");

        let start_time = std::time::Instant::now();
        let results = validator.run_comprehensive_validation()
            .expect("Performance validation failed");
        let total_time = start_time.elapsed();

        println!("📊 Full performance validation results:");
        println!("  - Total time: {:?}", total_time);
        println!("  - Datasets tested: {}", results.len());
        
        // Performance assertions for full suite
        assert!(total_time < Duration::from_secs(300), 
            "Full validation should complete within 5 minutes, took {:?}", total_time);
        
        let total_operations: usize = results.iter()
            .map(|r| r.byte_comparable_result.keys_tested + r.rows_decoding_result.rows_processed)
            .sum();
        
        let ops_per_second = total_operations as f64 / total_time.as_secs_f64();
        
        println!("  - Total operations: {}", total_operations);
        println!("  - Operations per second: {:.2}", ops_per_second);
        
        assert!(ops_per_second >= 1000.0, 
            "Full validation should achieve at least 1000 ops/sec, got {:.2}", ops_per_second);
        
        println!("✅ Full BTI validation performance suite completed successfully");
    }
}

/// Helper functions for CI integration
pub mod ci_helpers {
    use super::*;

    /// Run BTI validation suite optimized for CI environment
    pub fn run_ci_optimized_bti_validation() -> Result<(), Box<dyn std::error::Error>> {
        println!("🔄 Running CI-optimized BTI validation for Issue #36");
        
        let ci_config = BtiValidationConfig {
            enable_sstabledump_parity: false, // Skip if Cassandra tools not available
            test_complex_scenarios: true,
            generate_test_data: true,
            max_test_data_size_mb: 30, // Reasonable for CI
            enable_performance_tests: true,
            require_zero_diff_parity: false,
            enable_trie_traversal_validation: true,
            enable_byte_comparable_validation: true,
        };
        
        let mut validator = BtiComprehensiveValidator::new(ci_config)?;
        let results = validator.run_comprehensive_validation()?;
        
        // CI success criteria
        let failed_count = results.iter()
            .filter(|r| r.status == ValidationStatus::Failed)
            .count();
        
        if failed_count > 0 {
            eprintln!("❌ CI BTI validation failed: {} datasets failed", failed_count);
            return Err("BTI validation failures detected".into());
        }
        
        let partial_count = results.iter()
            .filter(|r| r.status == ValidationStatus::PartiallyPassed)
            .count();
        
        if partial_count > results.len() / 2 {
            eprintln!("⚠️  CI BTI validation warning: {} datasets partially passed", partial_count);
        }
        
        println!("✅ CI BTI validation completed successfully");
        println!("  - {} total datasets", results.len());
        println!("  - {} passed", results.len() - failed_count - partial_count);
        println!("  - {} partially passed", partial_count);
        println!("  - {} failed", failed_count);
        
        Ok(())
    }
    
    /// Check if BTI validation environment is properly set up for CI
    pub fn check_ci_environment() -> bool {
        // Check required directories exist
        let test_dirs = [
            "tests/data",
            "test-env",
        ];
        
        for dir in &test_dirs {
            if !std::path::Path::new(dir).exists() {
                eprintln!("⚠️  Missing directory for BTI validation: {}", dir);
                // Don't fail - we can create these directories
                if let Err(e) = std::fs::create_dir_all(dir) {
                    eprintln!("❌ Failed to create directory {}: {}", dir, e);
                    return false;
                }
            }
        }
        
        // Check that BTI validation modules are available
        let config = BtiValidationConfig::default();
        match BtiComprehensiveValidator::new(config) {
            Ok(_) => {
                println!("✅ BTI validation environment ready");
                true
            },
            Err(e) => {
                eprintln!("❌ BTI validation environment not ready: {}", e);
                false
            }
        }
    }
}