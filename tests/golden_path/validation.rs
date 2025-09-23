//! Validation Engine for Golden-Path Testing
//!
//! This module provides comprehensive validation of test results against
//! golden expectations, including data correctness, performance benchmarks,
//! and component integration validation.

use std::collections::HashMap;
use std::time::Duration;

use cqlite_core::{RowKey, Value, Result};

use super::{
    GoldenPathConfig, TestMetrics, ValidationResults,
    scenarios::{TestScenario, TestExpectations, ExpectedResult, ValidationCriteria, PerformanceRequirements},
    harness::{ScenarioExecutionResult, ScenarioResults},
};

/// Validates scenario execution results against expectations
pub async fn validate_scenario_result(
    scenario: &TestScenario,
    execution_result: &ScenarioExecutionResult,
    config: &GoldenPathConfig,
) -> ValidationResults {
    let mut validation = ValidationResults {
        data_correct: true,
        performance_acceptable: true,
        integration_valid: true,
        messages: Vec::new(),
    };

    // Validate data correctness
    validate_data_correctness(scenario, execution_result, &mut validation).await;

    // Validate performance
    validate_performance_requirements(scenario, execution_result, config, &mut validation).await;

    // Validate integration (if applicable)
    if config.validate_integration {
        validate_integration_requirements(scenario, execution_result, &mut validation).await;
    }

    validation
}

/// Validate data correctness against expectations
async fn validate_data_correctness(
    scenario: &TestScenario,
    execution_result: &ScenarioExecutionResult,
    validation: &mut ValidationResults,
) {
    let expectations = &scenario.expectations;

    // Check if operation should have succeeded
    let operation_succeeded = execution_result.operations_count > 0;
    if expectations.should_succeed && !operation_succeeded {
        validation.data_correct = false;
        validation.messages.push("Operation expected to succeed but failed".to_string());
        return;
    }

    // Validate specific results based on operation type
    match &execution_result.results {
        ScenarioResults::Get(get_results) => {
            validate_get_results(get_results, expectations, validation).await;
        }
        ScenarioResults::Scan(scan_results) => {
            validate_scan_results(scan_results, expectations, validation).await;
        }
        ScenarioResults::Lookup(lookup_results) => {
            validate_lookup_results(lookup_results, expectations, validation).await;
        }
        ScenarioResults::Integration(integration_results) => {
            validate_integration_results(integration_results, expectations, validation).await;
        }
    }
}

/// Validate get operation results
async fn validate_get_results(
    get_results: &[Option<Value>],
    expectations: &TestExpectations,
    validation: &mut ValidationResults,
) {
    // Check expected count
    if let Some(expected_count) = expectations.expected_count {
        if get_results.len() != expected_count {
            validation.data_correct = false;
            validation.messages.push(format!(
                "Expected {} results, got {}",
                expected_count,
                get_results.len()
            ));
        }
    }

    // Validate specific expected results
    for (i, expected) in expectations.expected_results.iter().enumerate() {
        if i >= get_results.len() {
            validation.data_correct = false;
            validation.messages.push(format!(
                "Missing result for key: {}",
                format_row_key(&expected.key)
            ));
            continue;
        }

        let actual_result = &get_results[i];
        if !validate_expected_result(actual_result, expected, validation).await {
            validation.data_correct = false;
        }
    }
}

/// Validate scan operation results
async fn validate_scan_results(
    scan_results: &[(RowKey, Value)],
    expectations: &TestExpectations,
    validation: &mut ValidationResults,
) {
    // Check expected count
    if let Some(expected_count) = expectations.expected_count {
        if scan_results.len() != expected_count {
            validation.data_correct = false;
            validation.messages.push(format!(
                "Scan expected {} results, got {}",
                expected_count,
                scan_results.len()
            ));
        }
    }

    // Validate that results are sorted (scans should return sorted results)
    for i in 1..scan_results.len() {
        if scan_results[i - 1].0 > scan_results[i].0 {
            validation.data_correct = false;
            validation.messages.push("Scan results are not properly sorted".to_string());
            break;
        }
    }

    // Validate specific expected results if provided
    for expected in &expectations.expected_results {
        let found = scan_results.iter().find(|(key, _)| key == &expected.key);
        match (found, &expected.value) {
            (Some((_, actual_value)), Some(expected_value)) => {
                if !values_match(actual_value, expected_value, &expected.validation_criteria) {
                    validation.data_correct = false;
                    validation.messages.push(format!(
                        "Value mismatch for key {}: expected {:?}, got {:?}",
                        format_row_key(&expected.key),
                        expected_value,
                        actual_value
                    ));
                }
            }
            (None, Some(_)) => {
                validation.data_correct = false;
                validation.messages.push(format!(
                    "Expected key {} not found in scan results",
                    format_row_key(&expected.key)
                ));
            }
            (Some(_), None) => {
                validation.data_correct = false;
                validation.messages.push(format!(
                    "Unexpected key {} found in scan results",
                    format_row_key(&expected.key)
                ));
            }
            (None, None) => {
                // Both are None, this is correct
            }
        }
    }
}

/// Validate lookup operation results
async fn validate_lookup_results(
    lookup_results: &[super::harness::PartitionLookupResult],
    expectations: &TestExpectations,
    validation: &mut ValidationResults,
) {
    // Check expected count
    if let Some(expected_count) = expectations.expected_count {
        if lookup_results.len() != expected_count {
            validation.data_correct = false;
            validation.messages.push(format!(
                "Lookup expected {} results, got {}",
                expected_count,
                lookup_results.len()
            ));
        }
    }

    // Validate that lookups found expected partitions
    for lookup_result in lookup_results {
        if lookup_result.found {
            if lookup_result.data_offset.is_none() || lookup_result.partition_size.is_none() {
                validation.data_correct = false;
                validation.messages.push(format!(
                    "Lookup found partition {} but missing offset/size information",
                    format_row_key(&lookup_result.key)
                ));
            }
        }
    }

    validation.messages.push(format!(
        "Lookup validation completed: {}/{} partitions found",
        lookup_results.iter().filter(|r| r.found).count(),
        lookup_results.len()
    ));
}

/// Validate integration test results
async fn validate_integration_results(
    integration_results: &super::harness::IntegrationTestResult,
    _expectations: &TestExpectations,
    validation: &mut ValidationResults,
) {
    if !integration_results.summary_index_ok {
        validation.integration_valid = false;
        validation.messages.push("Summary→Index coordination failed".to_string());
    }

    if !integration_results.index_data_ok {
        validation.integration_valid = false;
        validation.messages.push("Index→Data coordination failed".to_string());
    }

    if !integration_results.end_to_end_ok {
        validation.integration_valid = false;
        validation.messages.push("End-to-end coordination failed".to_string());
    }

    // Add coordination details to messages
    for detail in &integration_results.coordination_details {
        validation.messages.push(format!("Integration: {}", detail));
    }
}

/// Validate an individual expected result
async fn validate_expected_result(
    actual_result: &Option<Value>,
    expected: &ExpectedResult,
    validation: &mut ValidationResults,
) -> bool {
    match (actual_result, &expected.value) {
        (Some(actual_value), Some(expected_value)) => {
            if values_match(actual_value, expected_value, &expected.validation_criteria) {
                validation.messages.push(format!(
                    "✓ Value match for key: {}",
                    format_row_key(&expected.key)
                ));
                true
            } else {
                validation.messages.push(format!(
                    "✗ Value mismatch for key {}: expected {:?}, got {:?}",
                    format_row_key(&expected.key),
                    expected_value,
                    actual_value
                ));
                false
            }
        }
        (None, None) => {
            validation.messages.push(format!(
                "✓ Correctly returned None for key: {}",
                format_row_key(&expected.key)
            ));
            true
        }
        (Some(actual_value), None) => {
            validation.messages.push(format!(
                "✗ Expected None for key {}, got {:?}",
                format_row_key(&expected.key),
                actual_value
            ));
            false
        }
        (None, Some(expected_value)) => {
            validation.messages.push(format!(
                "✗ Expected {:?} for key {}, got None",
                expected_value,
                format_row_key(&expected.key)
            ));
            false
        }
    }
}

/// Check if two values match according to validation criteria
fn values_match(actual: &Value, expected: &Value, criteria: &[ValidationCriteria]) -> bool {
    // If no specific criteria, do exact match
    if criteria.is_empty() {
        return actual == expected;
    }

    for criterion in criteria {
        match criterion {
            ValidationCriteria::ExactMatch => {
                if actual != expected {
                    return false;
                }
            }
            ValidationCriteria::Contains(substring) => {
                if let (Value::Text(actual_str), Value::Text(_)) = (actual, expected) {
                    if !actual_str.contains(substring) {
                        return false;
                    }
                } else {
                    return false; // Can only check contains on text values
                }
            }
            ValidationCriteria::Range { min, max } => {
                if let (Value::BigInt(actual_int), Value::BigInt(_)) = (actual, expected) {
                    if *actual_int < *min || *actual_int > *max {
                        return false;
                    }
                } else {
                    return false; // Can only check range on integer values
                }
            }
            ValidationCriteria::Custom(function_name) => {
                // For now, just log that custom validation was requested
                // In a real implementation, this would call custom validation functions
                log::info!("Custom validation requested: {}", function_name);
            }
        }
    }

    true
}

/// Validate performance requirements
async fn validate_performance_requirements(
    scenario: &TestScenario,
    execution_result: &ScenarioExecutionResult,
    config: &GoldenPathConfig,
    validation: &mut ValidationResults,
) {
    let perf_req = &scenario.performance_requirements;

    // Calculate actual metrics (placeholder values for now)
    let actual_latency_ms = execution_result.coordination_metrics.total_coordination_time.as_millis() as u64;
    let operations_count = execution_result.operations_count as f64;
    let total_time_secs = execution_result.coordination_metrics.total_coordination_time.as_secs_f64();
    let actual_throughput = if total_time_secs > 0.0 {
        operations_count / total_time_secs
    } else {
        0.0
    };

    // Check latency requirements
    if actual_latency_ms > perf_req.max_latency_ms {
        let regression_percent = ((actual_latency_ms as f64 - perf_req.max_latency_ms as f64) / perf_req.max_latency_ms as f64) * 100.0;
        if regression_percent > config.performance_threshold {
            validation.performance_acceptable = false;
            validation.messages.push(format!(
                "Latency regression: {}ms > {}ms ({}% regression)",
                actual_latency_ms,
                perf_req.max_latency_ms,
                regression_percent
            ));
        } else {
            validation.messages.push(format!(
                "Latency slightly elevated but within threshold: {}ms",
                actual_latency_ms
            ));
        }
    } else {
        validation.messages.push(format!(
            "✓ Latency requirement met: {}ms <= {}ms",
            actual_latency_ms,
            perf_req.max_latency_ms
        ));
    }

    // Check throughput requirements
    if actual_throughput < perf_req.min_throughput {
        let throughput_degradation = ((perf_req.min_throughput - actual_throughput) / perf_req.min_throughput) * 100.0;
        if throughput_degradation > config.performance_threshold {
            validation.performance_acceptable = false;
            validation.messages.push(format!(
                "Throughput degradation: {:.1} ops/sec < {:.1} ops/sec ({:.1}% degradation)",
                actual_throughput,
                perf_req.min_throughput,
                throughput_degradation
            ));
        } else {
            validation.messages.push(format!(
                "Throughput slightly below target but within threshold: {:.1} ops/sec",
                actual_throughput
            ));
        }
    } else {
        validation.messages.push(format!(
            "✓ Throughput requirement met: {:.1} ops/sec >= {:.1} ops/sec",
            actual_throughput,
            perf_req.min_throughput
        ));
    }

    // Memory and cache requirements would be validated here with actual metrics
    validation.messages.push(format!(
        "Performance validation completed for scenario: {}",
        scenario.name
    ));
}

/// Validate integration requirements
async fn validate_integration_requirements(
    scenario: &TestScenario,
    execution_result: &ScenarioExecutionResult,
    validation: &mut ValidationResults,
) {
    // Check that component coordination timing is reasonable
    let coord_metrics = &execution_result.coordination_metrics;

    if coord_metrics.total_coordination_time > Duration::from_millis(1000) {
        validation.integration_valid = false;
        validation.messages.push(format!(
            "Component coordination took too long: {}ms",
            coord_metrics.total_coordination_time.as_millis()
        ));
    }

    // Ensure coordination calls match expected operations
    if coord_metrics.component_calls != execution_result.operations_count {
        validation.integration_valid = false;
        validation.messages.push(format!(
            "Component call count mismatch: {} calls for {} operations",
            coord_metrics.component_calls,
            execution_result.operations_count
        ));
    }

    validation.messages.push(format!(
        "Integration validation completed for scenario: {}",
        scenario.name
    ));
}

/// Helper function to format RowKey for display
fn format_row_key(key: &RowKey) -> String {
    // This is a simplified formatting - in practice you'd want proper key formatting
    format!("{:?}", key)
}

/// Performance benchmark comparison
pub struct PerformanceBenchmark {
    /// Scenario name
    pub scenario: String,
    /// Historical baseline metrics
    pub baseline: BenchmarkMetrics,
    /// Current test metrics
    pub current: BenchmarkMetrics,
    /// Performance comparison result
    pub comparison: PerformanceComparison,
}

/// Benchmark metrics for comparison
#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// Throughput in operations per second
    pub throughput_ops_sec: f64,
    /// Peak memory usage in KB
    pub peak_memory_kb: usize,
    /// Cache hit rate percentage
    pub cache_hit_rate: f64,
}

/// Performance comparison result
#[derive(Debug, Clone)]
pub enum PerformanceComparison {
    /// Performance improved
    Improved { improvement_percent: f64 },
    /// Performance degraded
    Degraded { degradation_percent: f64 },
    /// Performance is stable (within threshold)
    Stable,
}

/// Compare current performance against baseline
pub fn compare_performance(
    baseline: &BenchmarkMetrics,
    current: &BenchmarkMetrics,
    threshold_percent: f64,
) -> PerformanceComparison {
    // Calculate latency change (lower is better)
    let latency_change = ((current.avg_latency_ms - baseline.avg_latency_ms) / baseline.avg_latency_ms) * 100.0;

    // Calculate throughput change (higher is better)
    let throughput_change = ((current.throughput_ops_sec - baseline.throughput_ops_sec) / baseline.throughput_ops_sec) * 100.0;

    // Use throughput as primary metric (negative latency change is improvement)
    let overall_change = throughput_change - (latency_change * 0.5); // Weight latency at 50%

    if overall_change > threshold_percent {
        PerformanceComparison::Improved {
            improvement_percent: overall_change,
        }
    } else if overall_change < -threshold_percent {
        PerformanceComparison::Degraded {
            degradation_percent: -overall_change,
        }
    } else {
        PerformanceComparison::Stable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::golden_path::scenarios::*;

    #[test]
    fn test_values_match_exact() {
        let value1 = Value::Text("test".to_string());
        let value2 = Value::Text("test".to_string());
        let criteria = vec![ValidationCriteria::ExactMatch];

        assert!(values_match(&value1, &value2, &criteria));
    }

    #[test]
    fn test_values_match_contains() {
        let value1 = Value::Text("test_string".to_string());
        let value2 = Value::Text("test".to_string());
        let criteria = vec![ValidationCriteria::Contains("test".to_string())];

        assert!(values_match(&value1, &value2, &criteria));
    }

    #[test]
    fn test_values_match_range() {
        let value1 = Value::BigInt(50);
        let value2 = Value::BigInt(100);
        let criteria = vec![ValidationCriteria::Range { min: 0, max: 100 }];

        assert!(values_match(&value1, &value2, &criteria));
    }

    #[test]
    fn test_performance_comparison() {
        let baseline = BenchmarkMetrics {
            avg_latency_ms: 100.0,
            throughput_ops_sec: 100.0,
            peak_memory_kb: 1024,
            cache_hit_rate: 0.8,
        };

        let improved = BenchmarkMetrics {
            avg_latency_ms: 80.0,
            throughput_ops_sec: 120.0,
            peak_memory_kb: 1024,
            cache_hit_rate: 0.8,
        };

        let comparison = compare_performance(&baseline, &improved, 10.0);
        match comparison {
            PerformanceComparison::Improved { improvement_percent } => {
                assert!(improvement_percent > 0.0);
            }
            _ => panic!("Expected improvement"),
        }
    }
}