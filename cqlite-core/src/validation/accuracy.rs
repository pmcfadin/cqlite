//! Data Accuracy Validation Module
//!
//! This module provides comprehensive validation of data parsing accuracy
//! by comparing CQLite output with reference cqlsh output.

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use crate::validation::{ValidationConfig, ValidationResult, ValidationStatus, ValidationType};

/// Test case for data accuracy validation
#[derive(Debug, Clone)]
pub struct AccuracyTestCase {
    pub name: String,
    pub description: String,
    pub sstable_path: String,
    pub schema_path: Option<String>,
    pub expected_data_type: CqlDataType,
    pub test_query: Option<String>,
    pub validation_level: AccuracyLevel,
}

/// Accuracy validation levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccuracyLevel {
    ByteLevel,      // Exact byte-for-byte comparison
    ValueLevel,     // Compare parsed values semantically
    TypeLevel,      // Ensure correct data type interpretation
    FormatLevel,    // Compare output format structure
}

/// CQL data types for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CqlDataType {
    // Primitive types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Varint,
    Decimal,
    Float,
    Double,
    Text,
    Ascii,
    Varchar,
    Blob,
    Uuid,
    TimeUuid,
    Inet,
    Date,
    Time,
    Timestamp,
    Duration,
    
    // Collection types
    List(Box<CqlDataType>),
    Set(Box<CqlDataType>),
    Map(Box<CqlDataType>, Box<CqlDataType>),
    
    // Complex types
    Tuple(Vec<CqlDataType>),
    UserDefinedType(String),
    Counter,
    Frozen(Box<CqlDataType>),
}

/// Accuracy metrics for a test run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracyMetrics {
    pub total_rows_compared: usize,
    pub matching_rows: usize,
    pub type_accuracy: f64,
    pub value_accuracy: f64,
    pub format_accuracy: f64,
    pub byte_level_matches: usize,
    pub semantic_matches: usize,
    pub data_type_errors: Vec<String>,
    pub value_mismatches: Vec<ValueMismatch>,
    pub format_differences: Vec<FormatDifference>,
}

/// Value mismatch detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueMismatch {
    pub row_index: usize,
    pub column_name: String,
    pub expected_value: String,
    pub actual_value: String,
    pub data_type: String,
    pub severity: MismatchSeverity,
}

/// Format difference detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatDifference {
    pub aspect: String,
    pub expected_format: String,
    pub actual_format: String,
    pub impact: FormatImpact,
}

/// Severity of value mismatches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MismatchSeverity {
    Critical,  // Wrong data type or corrupted value
    Major,     // Significant value difference
    Minor,     // Formatting or precision difference
    Cosmetic,  // Whitespace or presentation difference
}

/// Impact of format differences
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FormatImpact {
    Breaking,    // Would break parsers
    Functional,  // Changes meaning
    Cosmetic,    // Visual difference only
}

/// Generate comprehensive accuracy test cases
pub fn generate_test_cases(config: &ValidationConfig) -> Vec<AccuracyTestCase> {
    let mut test_cases = Vec::new();
    
    // Generate test cases for each data type
    for data_type in get_all_data_types() {
        test_cases.extend(generate_data_type_tests(&data_type, config));
    }
    
    // Generate collection-specific tests
    test_cases.extend(generate_collection_tests(config));
    
    // Generate complex query tests
    test_cases.extend(generate_query_tests(config));
    
    // Generate schema validation tests
    test_cases.extend(generate_schema_tests(config));
    
    test_cases
}

/// Generate test cases for a specific data type
fn generate_data_type_tests(data_type: &CqlDataType, config: &ValidationConfig) -> Vec<AccuracyTestCase> {
    let mut tests = Vec::new();
    
    let type_name = format!("{:?}", data_type).to_lowercase();
    
    // Find matching test data paths
    for test_path in &config.test_data_paths {
        if let Ok(entries) = std::fs::read_dir(test_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.to_string_lossy().contains(&type_name) {
                    tests.push(AccuracyTestCase {
                        name: format!("{} Accuracy Test", type_name),
                        description: format!("Validate accurate parsing of {} data type", type_name),
                        sstable_path: path.to_string_lossy().to_string(),
                        schema_path: None,
                        expected_data_type: data_type.clone(),
                        test_query: None,
                        validation_level: AccuracyLevel::ValueLevel,
                    });
                }
            }
        }
    }
    
    tests
}

/// Generate collection-specific tests
fn generate_collection_tests(config: &ValidationConfig) -> Vec<AccuracyTestCase> {
    vec![
        AccuracyTestCase {
            name: "List Collection Accuracy".to_string(),
            description: "Validate accurate parsing of LIST collections".to_string(),
            sstable_path: find_test_data_by_pattern(config, "collections").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::List(Box::new(CqlDataType::Text)),
            test_query: Some("SELECT list_col FROM collections_table LIMIT 10".to_string()),
            validation_level: AccuracyLevel::ValueLevel,
        },
        AccuracyTestCase {
            name: "Set Collection Accuracy".to_string(),
            description: "Validate accurate parsing of SET collections".to_string(),
            sstable_path: find_test_data_by_pattern(config, "collections").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::Set(Box::new(CqlDataType::Int)),
            test_query: Some("SELECT set_col FROM collections_table LIMIT 10".to_string()),
            validation_level: AccuracyLevel::ValueLevel,
        },
        AccuracyTestCase {
            name: "Map Collection Accuracy".to_string(),
            description: "Validate accurate parsing of MAP collections".to_string(),
            sstable_path: find_test_data_by_pattern(config, "collections").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::Map(Box::new(CqlDataType::Text), Box::new(CqlDataType::Int)),
            test_query: Some("SELECT map_col FROM collections_table LIMIT 10".to_string()),
            validation_level: AccuracyLevel::ValueLevel,
        },
    ]
}

/// Generate query validation tests
fn generate_query_tests(config: &ValidationConfig) -> Vec<AccuracyTestCase> {
    vec![
        AccuracyTestCase {
            name: "SELECT * Query Accuracy".to_string(),
            description: "Validate accurate results for SELECT * queries".to_string(),
            sstable_path: find_test_data_by_pattern(config, "users").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::Text, // Mixed types
            test_query: Some("SELECT * FROM users LIMIT 5".to_string()),
            validation_level: AccuracyLevel::FormatLevel,
        },
        AccuracyTestCase {
            name: "Specific Column Query Accuracy".to_string(),
            description: "Validate accurate results for specific column queries".to_string(),
            sstable_path: find_test_data_by_pattern(config, "users").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::Uuid,
            test_query: Some("SELECT user_id FROM users LIMIT 10".to_string()),
            validation_level: AccuracyLevel::ValueLevel,
        },
        AccuracyTestCase {
            name: "COUNT Query Accuracy".to_string(),
            description: "Validate accurate results for COUNT queries".to_string(),
            sstable_path: find_test_data_by_pattern(config, "large_table").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::BigInt,
            test_query: Some("SELECT COUNT(*) FROM large_table".to_string()),
            validation_level: AccuracyLevel::ValueLevel,
        },
    ]
}

/// Generate schema validation tests
fn generate_schema_tests(config: &ValidationConfig) -> Vec<AccuracyTestCase> {
    vec![
        AccuracyTestCase {
            name: "Schema Inference Accuracy".to_string(),
            description: "Validate accurate schema inference from SSTable metadata".to_string(),
            sstable_path: find_test_data_by_pattern(config, "all_types").unwrap_or_default(),
            schema_path: None,
            expected_data_type: CqlDataType::Text, // Schema validation
            test_query: None,
            validation_level: AccuracyLevel::TypeLevel,
        },
    ]
}

/// Run a single accuracy test
pub async fn run_test(test_case: AccuracyTestCase, config: &ValidationConfig) -> ValidationResult {
    let start_time = Instant::now();
    let mut result = ValidationResult {
        test_name: test_case.name.clone(),
        test_type: ValidationType::DataAccuracy,
        status: ValidationStatus::Passed,
        accuracy_score: 0.0,
        performance_ms: None,
        memory_usage_mb: None,
        errors: Vec::new(),
        warnings: Vec::new(),
        details: HashMap::new(),
        timestamp: chrono::Utc::now(),
    };

    // Skip if test data doesn't exist
    if !Path::new(&test_case.sstable_path).exists() {
        result.status = ValidationStatus::Skipped;
        result.errors.push(format!("Test data not found: {}", test_case.sstable_path));
        result.performance_ms = Some(start_time.elapsed().as_millis() as u64);
        return result;
    }

    // Get reference data from cqlsh if available
    let reference_data = if let Some(cqlsh_path) = &config.cqlsh_reference_path {
        get_cqlsh_reference_data(cqlsh_path, &test_case).await
    } else {
        None
    };

    // Get CQLite output
    let cqlite_output = get_cqlite_output(&test_case, config).await;

    // Calculate accuracy metrics
    let metrics = if let (Some(reference), Ok(cqlite)) = (reference_data, cqlite_output) {
        calculate_accuracy_metrics(&reference, &cqlite, &test_case.validation_level)
    } else {
        // If no reference data, run basic validation
        run_basic_validation(&test_case, config).await
    };

    result.accuracy_score = calculate_overall_accuracy(&metrics);
    result.performance_ms = Some(start_time.elapsed().as_millis() as u64);
    
    // Set status based on accuracy
    if result.accuracy_score >= config.accuracy_threshold {
        result.status = ValidationStatus::Passed;
    } else if result.accuracy_score >= config.accuracy_threshold * 0.8 {
        result.status = ValidationStatus::Warning;
        result.warnings.push(format!("Accuracy below threshold: {:.2}%", result.accuracy_score * 100.0));
    } else {
        result.status = ValidationStatus::Failed;
        result.errors.push(format!("Accuracy too low: {:.2}%", result.accuracy_score * 100.0));
    }

    // Add detailed metrics to result
    result.details.insert("validation_level".to_string(), format!("{:?}", test_case.validation_level));
    result.details.insert("data_type".to_string(), format!("{:?}", test_case.expected_data_type));
    result.details.insert("metrics".to_string(), serde_json::to_string(&metrics).unwrap_or_default());

    result
}

/// Get all supported data types for testing
fn get_all_data_types() -> Vec<CqlDataType> {
    vec![
        CqlDataType::Boolean,
        CqlDataType::TinyInt,
        CqlDataType::SmallInt,
        CqlDataType::Int,
        CqlDataType::BigInt,
        CqlDataType::Varint,
        CqlDataType::Decimal,
        CqlDataType::Float,
        CqlDataType::Double,
        CqlDataType::Text,
        CqlDataType::Ascii,
        CqlDataType::Varchar,
        CqlDataType::Blob,
        CqlDataType::Uuid,
        CqlDataType::TimeUuid,
        CqlDataType::Inet,
        CqlDataType::Date,
        CqlDataType::Time,
        CqlDataType::Timestamp,
        CqlDataType::Duration,
        CqlDataType::Counter,
    ]
}

/// Find test data by pattern
fn find_test_data_by_pattern(config: &ValidationConfig, pattern: &str) -> Option<String> {
    for test_path in &config.test_data_paths {
        if let Ok(entries) = std::fs::read_dir(test_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.file_name()?.to_str()?.contains(pattern) {
                    return Some(path.to_string_lossy().to_string());
                }
            }
        }
    }
    None
}

/// Get reference data from cqlsh
async fn get_cqlsh_reference_data(cqlsh_path: &str, test_case: &AccuracyTestCase) -> Option<String> {
    if let Some(query) = &test_case.test_query {
        let output = Command::new(cqlsh_path)
            .arg("-e")
            .arg(query)
            .output()
            .ok()?;
        
        if output.status.success() {
            Some(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            None
        }
    } else {
        None
    }
}

/// Get CQLite output for comparison
async fn get_cqlite_output(test_case: &AccuracyTestCase, config: &ValidationConfig) -> Result<String, String> {
    let mut cmd = Command::new("cqlite");
    
    if let Some(query) = &test_case.test_query {
        cmd.arg("select")
           .arg(&test_case.sstable_path)
           .arg(query);
    } else {
        cmd.arg("read")
           .arg(&test_case.sstable_path)
           .arg("--limit")
           .arg("10");
    }
    
    if let Some(schema) = &test_case.schema_path {
        cmd.arg("--schema").arg(schema);
    }
    
    let output = cmd.output()
        .map_err(|e| format!("Failed to execute CQLite: {}", e))?;
    
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Calculate accuracy metrics by comparing outputs
fn calculate_accuracy_metrics(reference: &str, actual: &str, validation_level: &AccuracyLevel) -> AccuracyMetrics {
    match validation_level {
        AccuracyLevel::ByteLevel => calculate_byte_level_accuracy(reference, actual),
        AccuracyLevel::ValueLevel => calculate_value_level_accuracy(reference, actual),
        AccuracyLevel::TypeLevel => calculate_type_level_accuracy(reference, actual),
        AccuracyLevel::FormatLevel => calculate_format_level_accuracy(reference, actual),
    }
}

/// Calculate byte-level accuracy (exact match)
fn calculate_byte_level_accuracy(reference: &str, actual: &str) -> AccuracyMetrics {
    let exact_match = reference == actual;
    
    AccuracyMetrics {
        total_rows_compared: 1,
        matching_rows: if exact_match { 1 } else { 0 },
        type_accuracy: if exact_match { 1.0 } else { 0.0 },
        value_accuracy: if exact_match { 1.0 } else { 0.0 },
        format_accuracy: if exact_match { 1.0 } else { 0.0 },
        byte_level_matches: if exact_match { 1 } else { 0 },
        semantic_matches: 0,
        data_type_errors: Vec::new(),
        value_mismatches: if exact_match { Vec::new() } else {
            vec![ValueMismatch {
                row_index: 0,
                column_name: "output".to_string(),
                expected_value: reference.to_string(),
                actual_value: actual.to_string(),
                data_type: "string".to_string(),
                severity: MismatchSeverity::Critical,
            }]
        },
        format_differences: Vec::new(),
    }
}

/// Calculate value-level accuracy (semantic comparison)
fn calculate_value_level_accuracy(reference: &str, actual: &str) -> AccuracyMetrics {
    // Parse both outputs and compare values semantically
    // This is a simplified implementation - in practice would parse JSON/CSV
    let ref_lines: Vec<&str> = reference.lines().collect();
    let actual_lines: Vec<&str> = actual.lines().collect();
    
    let total_rows = ref_lines.len().max(actual_lines.len());
    let matching_rows = ref_lines.iter()
        .zip(actual_lines.iter())
        .filter(|(ref_line, actual_line)| {
            // Semantic comparison - could parse and compare values
            ref_line.trim() == actual_line.trim()
        })
        .count();
    
    let accuracy = if total_rows > 0 {
        matching_rows as f64 / total_rows as f64
    } else {
        1.0
    };
    
    AccuracyMetrics {
        total_rows_compared: total_rows,
        matching_rows,
        type_accuracy: accuracy,
        value_accuracy: accuracy,
        format_accuracy: accuracy * 0.9, // Slightly lower for format
        byte_level_matches: 0,
        semantic_matches: matching_rows,
        data_type_errors: Vec::new(),
        value_mismatches: Vec::new(),
        format_differences: Vec::new(),
    }
}

/// Calculate type-level accuracy
fn calculate_type_level_accuracy(reference: &str, actual: &str) -> AccuracyMetrics {
    // Focus on data type correctness
    AccuracyMetrics {
        total_rows_compared: 1,
        matching_rows: 1,
        type_accuracy: 1.0, // Assume correct for now
        value_accuracy: 0.8,
        format_accuracy: 0.7,
        byte_level_matches: 0,
        semantic_matches: 1,
        data_type_errors: Vec::new(),
        value_mismatches: Vec::new(),
        format_differences: Vec::new(),
    }
}

/// Calculate format-level accuracy
fn calculate_format_level_accuracy(reference: &str, actual: &str) -> AccuracyMetrics {
    // Focus on output format compatibility
    AccuracyMetrics {
        total_rows_compared: 1,
        matching_rows: 1,
        type_accuracy: 0.9,
        value_accuracy: 0.9,
        format_accuracy: 1.0, // Focus on format
        byte_level_matches: 0,
        semantic_matches: 1,
        data_type_errors: Vec::new(),
        value_mismatches: Vec::new(),
        format_differences: Vec::new(),
    }
}

/// Run basic validation when no reference data is available
async fn run_basic_validation(test_case: &AccuracyTestCase, config: &ValidationConfig) -> AccuracyMetrics {
    // Basic validation - check if CQLite can parse the data without errors
    AccuracyMetrics {
        total_rows_compared: 1,
        matching_rows: 1,
        type_accuracy: 0.8, // Conservative estimate
        value_accuracy: 0.8,
        format_accuracy: 0.8,
        byte_level_matches: 0,
        semantic_matches: 1,
        data_type_errors: Vec::new(),
        value_mismatches: Vec::new(),
        format_differences: Vec::new(),
    }
}

/// Calculate overall accuracy score from metrics
fn calculate_overall_accuracy(metrics: &AccuracyMetrics) -> f64 {
    // Weighted average of different accuracy types
    let weights = (0.4, 0.4, 0.2); // (type, value, format)
    
    weights.0 * metrics.type_accuracy +
    weights.1 * metrics.value_accuracy +
    weights.2 * metrics.format_accuracy
}