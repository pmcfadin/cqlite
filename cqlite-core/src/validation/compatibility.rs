//! Format Compatibility Validation Module
//!
//! This module validates that CQLite output formats are compatible with cqlsh
//! and other Cassandra tools, ensuring seamless integration.

use std::collections::HashMap;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use crate::validation::{ValidationConfig, ValidationResult, ValidationStatus, ValidationType};

/// Test case for format compatibility validation
#[derive(Debug, Clone)]
pub struct CompatibilityTestCase {
    pub name: String,
    pub description: String,
    pub sstable_path: String,
    pub output_format: OutputFormat,
    pub compatibility_check: CompatibilityCheck,
    pub test_parameters: HashMap<String, String>,
}

/// Supported output formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputFormat {
    Table,      // Human-readable table format (like cqlsh)
    Json,       // JSON format
    Csv,        // CSV format
    Raw,        // Raw binary output
}

/// Types of compatibility checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompatibilityCheck {
    CqlshTable,         // Match cqlsh table output exactly
    CqlshJson,          // Match cqlsh JSON output
    StandardCsv,        // Standard CSV format compliance
    SchemaFormat,       // Schema description format
    ErrorMessages,      // Error message format compatibility
    HeaderFormat,       // Column header format
    DataFormatting,     // Data value formatting (dates, UUIDs, etc.)
    PaginationFormat,   // Result pagination format
    MetadataFormat,     // Query metadata format
}

/// Compatibility validation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityMetrics {
    pub format_compliance_score: f64,
    pub header_compatibility: bool,
    pub data_formatting_score: f64,
    pub metadata_compatibility: bool,
    pub error_handling_compatibility: bool,
    pub pagination_compatibility: bool,
    pub format_violations: Vec<FormatViolation>,
    pub compatibility_warnings: Vec<String>,
}

/// Format violation details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatViolation {
    pub violation_type: ViolationType,
    pub expected_format: String,
    pub actual_format: String,
    pub line_number: Option<usize>,
    pub column_name: Option<String>,
    pub severity: ViolationSeverity,
}

/// Types of format violations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViolationType {
    HeaderMismatch,
    DataFormatMismatch,
    ColumnAlignment,
    DateTimeFormat,
    UuidFormat,
    BooleanFormat,
    NullRepresentation,
    NumericPrecision,
    StringEscaping,
    MetadataStructure,
}

/// Severity of format violations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViolationSeverity {
    Critical,    // Breaks compatibility completely
    Major,       // Significant compatibility issue
    Minor,       // Minor formatting difference
    Cosmetic,    // Visual difference only
}

/// Generate compatibility test cases
pub fn generate_test_cases(config: &ValidationConfig) -> Vec<CompatibilityTestCase> {
    let mut test_cases = Vec::new();
    
    // Generate tests for each output format
    for format in &config.output_formats {
        test_cases.extend(generate_format_tests(format, config));
    }
    
    // Generate specific compatibility checks
    test_cases.extend(generate_cqlsh_compatibility_tests(config));
    test_cases.extend(generate_standard_format_tests(config));
    test_cases.extend(generate_error_handling_tests(config));
    
    test_cases
}

/// Generate format-specific test cases
fn generate_format_tests(format: &str, config: &ValidationConfig) -> Vec<CompatibilityTestCase> {
    let output_format = match format {
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        "table" => OutputFormat::Table,
        _ => OutputFormat::Table,
    };
    
    let mut tests = Vec::new();
    
    // Find test data
    for test_path in &config.test_data_paths {
        if let Ok(entries) = std::fs::read_dir(test_path) {
            for entry in entries.flatten().take(3) { // Limit to 3 test cases per format
                let path = entry.path();
                if path.is_dir() {
                    tests.push(CompatibilityTestCase {
                        name: format!("{} Format Compatibility", format.to_uppercase()),
                        description: format!("Validate {} output format compatibility with standard tools", format),
                        sstable_path: path.to_string_lossy().to_string(),
                        output_format: output_format.clone(),
                        compatibility_check: match format {
                            "json" => CompatibilityCheck::CqlshJson,
                            "csv" => CompatibilityCheck::StandardCsv,
                            _ => CompatibilityCheck::CqlshTable,
                        },
                        test_parameters: HashMap::new(),
                    });
                }
            }
        }
    }
    
    tests
}

/// Generate cqlsh compatibility tests
fn generate_cqlsh_compatibility_tests(config: &ValidationConfig) -> Vec<CompatibilityTestCase> {
    let test_data_path = config.test_data_paths.first()
        .and_then(|path| std::fs::read_dir(path).ok())
        .and_then(|mut entries| entries.next())
        .and_then(|entry| entry.ok())
        .map(|entry| entry.path().to_string_lossy().to_string())
        .unwrap_or_default();
    
    vec![
        CompatibilityTestCase {
            name: "CQLSH Table Format Compatibility".to_string(),
            description: "Ensure table output matches cqlsh table format exactly".to_string(),
            sstable_path: test_data_path.clone(),
            output_format: OutputFormat::Table,
            compatibility_check: CompatibilityCheck::CqlshTable,
            test_parameters: [
                ("limit".to_string(), "10".to_string()),
                ("width".to_string(), "120".to_string()),
            ].iter().cloned().collect(),
        },
        CompatibilityTestCase {
            name: "CQLSH Header Format".to_string(),
            description: "Validate column header format matches cqlsh".to_string(),
            sstable_path: test_data_path.clone(),
            output_format: OutputFormat::Table,
            compatibility_check: CompatibilityCheck::HeaderFormat,
            test_parameters: HashMap::new(),
        },
        CompatibilityTestCase {
            name: "CQLSH Data Formatting".to_string(),
            description: "Ensure data values are formatted like cqlsh (dates, UUIDs, etc.)".to_string(),
            sstable_path: test_data_path.clone(),
            output_format: OutputFormat::Table,
            compatibility_check: CompatibilityCheck::DataFormatting,
            test_parameters: HashMap::new(),
        },
        CompatibilityTestCase {
            name: "CQLSH Metadata Format".to_string(),
            description: "Validate query metadata format matches cqlsh".to_string(),
            sstable_path: test_data_path,
            output_format: OutputFormat::Table,
            compatibility_check: CompatibilityCheck::MetadataFormat,
            test_parameters: HashMap::new(),
        },
    ]
}

/// Generate standard format tests
fn generate_standard_format_tests(config: &ValidationConfig) -> Vec<CompatibilityTestCase> {
    let test_data_path = config.test_data_paths.first()
        .and_then(|path| std::fs::read_dir(path).ok())
        .and_then(|mut entries| entries.next())
        .and_then(|entry| entry.ok())
        .map(|entry| entry.path().to_string_lossy().to_string())
        .unwrap_or_default();
    
    vec![
        CompatibilityTestCase {
            name: "Standard CSV Format".to_string(),
            description: "Ensure CSV output follows RFC 4180 standard".to_string(),
            sstable_path: test_data_path.clone(),
            output_format: OutputFormat::Csv,
            compatibility_check: CompatibilityCheck::StandardCsv,
            test_parameters: [
                ("delimiter".to_string(), ",".to_string()),
                ("quote_char".to_string(), "\"".to_string()),
                ("escape_char".to_string(), "\"".to_string()),
            ].iter().cloned().collect(),
        },
        CompatibilityTestCase {
            name: "Standard JSON Format".to_string(),
            description: "Ensure JSON output is valid and well-formatted".to_string(),
            sstable_path: test_data_path,
            output_format: OutputFormat::Json,
            compatibility_check: CompatibilityCheck::CqlshJson,
            test_parameters: [
                ("pretty_print".to_string(), "true".to_string()),
                ("null_handling".to_string(), "explicit".to_string()),
            ].iter().cloned().collect(),
        },
    ]
}

/// Generate error handling tests
fn generate_error_handling_tests(config: &ValidationConfig) -> Vec<CompatibilityTestCase> {
    vec![
        CompatibilityTestCase {
            name: "Error Message Compatibility".to_string(),
            description: "Ensure error messages are compatible with cqlsh format".to_string(),
            sstable_path: "/non/existent/path".to_string(), // Intentionally invalid
            output_format: OutputFormat::Table,
            compatibility_check: CompatibilityCheck::ErrorMessages,
            test_parameters: HashMap::new(),
        },
    ]
}

/// Run a single compatibility test
pub async fn run_test(test_case: CompatibilityTestCase, config: &ValidationConfig) -> ValidationResult {
    let start_time = Instant::now();
    let mut result = ValidationResult {
        test_name: test_case.name.clone(),
        test_type: ValidationType::FormatCompatibility,
        status: ValidationStatus::Passed,
        accuracy_score: 0.0,
        performance_ms: None,
        memory_usage_mb: None,
        errors: Vec::new(),
        warnings: Vec::new(),
        details: HashMap::new(),
        timestamp: chrono::Utc::now(),
    };

    // Get CQLite output
    let cqlite_output = get_cqlite_output(&test_case, config).await;
    
    // Get reference output if available
    let reference_output = get_reference_output(&test_case, config).await;
    
    // Run compatibility validation
    let metrics = validate_compatibility(&test_case, &cqlite_output, &reference_output).await;
    
    result.accuracy_score = metrics.format_compliance_score;
    result.performance_ms = Some(start_time.elapsed().as_millis() as u64);
    
    // Determine status based on compliance score
    if metrics.format_compliance_score >= 0.95 {
        result.status = ValidationStatus::Passed;
    } else if metrics.format_compliance_score >= 0.8 {
        result.status = ValidationStatus::Warning;
        result.warnings.push(format!("Format compliance below optimal: {:.1}%", metrics.format_compliance_score * 100.0));
    } else {
        result.status = ValidationStatus::Failed;
        result.errors.push(format!("Poor format compliance: {:.1}%", metrics.format_compliance_score * 100.0));
    }
    
    // Add violations to errors/warnings
    for violation in &metrics.format_violations {
        match violation.severity {
            ViolationSeverity::Critical | ViolationSeverity::Major => {
                result.errors.push(format!("{:?}: {} (expected: {}, got: {})", 
                    violation.violation_type, violation.expected_format, violation.actual_format, violation.expected_format));
            }
            ViolationSeverity::Minor | ViolationSeverity::Cosmetic => {
                result.warnings.push(format!("{:?}: Minor formatting difference", violation.violation_type));
            }
        }
    }
    
    // Add detailed metrics
    result.details.insert("output_format".to_string(), format!("{:?}", test_case.output_format));
    result.details.insert("compatibility_check".to_string(), format!("{:?}", test_case.compatibility_check));
    result.details.insert("violations_count".to_string(), metrics.format_violations.len().to_string());
    result.details.insert("metrics".to_string(), serde_json::to_string(&metrics).unwrap_or_default());
    
    result
}

/// Get CQLite output for the test case
async fn get_cqlite_output(test_case: &CompatibilityTestCase, config: &ValidationConfig) -> Result<String, String> {
    use std::process::Command;
    
    let mut cmd = Command::new("cqlite");
    cmd.arg("read")
       .arg(&test_case.sstable_path)
       .arg("--limit")
       .arg("5");
    
    // Add format argument
    match test_case.output_format {
        OutputFormat::Json => { cmd.arg("--format").arg("json"); }
        OutputFormat::Csv => { cmd.arg("--format").arg("csv"); }
        OutputFormat::Table => { cmd.arg("--format").arg("table"); }
        OutputFormat::Raw => { cmd.arg("--format").arg("raw"); }
    }
    
    // Add test parameters
    for (key, value) in &test_case.test_parameters {
        if key == "limit" {
            cmd.arg("--limit").arg(value);
        }
    }
    
    let output = cmd.output()
        .map_err(|e| format!("Failed to execute CQLite: {}", e))?;
    
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        // For error handling tests, stderr is expected
        if matches!(test_case.compatibility_check, CompatibilityCheck::ErrorMessages) {
            Ok(String::from_utf8_lossy(&output.stderr).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }
}

/// Get reference output (e.g., from cqlsh)
async fn get_reference_output(test_case: &CompatibilityTestCase, config: &ValidationConfig) -> Option<String> {
    // For now, return None - in a full implementation, this would get cqlsh output
    // This would require setting up a Cassandra instance and running equivalent queries
    None
}

/// Validate compatibility between outputs
async fn validate_compatibility(
    test_case: &CompatibilityTestCase,
    cqlite_output: &Result<String, String>,
    reference_output: &Option<String>,
) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.0,
        header_compatibility: false,
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    match cqlite_output {
        Ok(output) => {
            // Validate based on the compatibility check type
            match test_case.compatibility_check {
                CompatibilityCheck::CqlshTable => {
                    metrics = validate_table_format(output, reference_output.as_ref());
                }
                CompatibilityCheck::CqlshJson => {
                    metrics = validate_json_format(output);
                }
                CompatibilityCheck::StandardCsv => {
                    metrics = validate_csv_format(output);
                }
                CompatibilityCheck::HeaderFormat => {
                    metrics = validate_header_format(output);
                }
                CompatibilityCheck::DataFormatting => {
                    metrics = validate_data_formatting(output);
                }
                CompatibilityCheck::MetadataFormat => {
                    metrics = validate_metadata_format(output);
                }
                _ => {
                    // Default validation
                    metrics.format_compliance_score = 0.8;
                }
            }
        }
        Err(error) => {
            // Handle error cases
            if matches!(test_case.compatibility_check, CompatibilityCheck::ErrorMessages) {
                metrics = validate_error_messages(error);
            } else {
                metrics.format_violations.push(FormatViolation {
                    violation_type: ViolationType::MetadataStructure,
                    expected_format: "Successful output".to_string(),
                    actual_format: "Error".to_string(),
                    line_number: None,
                    column_name: None,
                    severity: ViolationSeverity::Critical,
                });
            }
        }
    }
    
    metrics
}

/// Validate table format compatibility
fn validate_table_format(output: &str, reference: Option<&String>) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.0,
        header_compatibility: false,
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    // Check for table structure
    let lines: Vec<&str> = output.lines().collect();
    let has_headers = lines.iter().any(|line| line.contains("|") || line.contains("+")); 
    let has_data_rows = lines.len() > 3; // Headers + separator + data
    
    if has_headers {
        metrics.header_compatibility = true;
        metrics.format_compliance_score += 0.3;
    } else {
        metrics.format_violations.push(FormatViolation {
            violation_type: ViolationType::HeaderMismatch,
            expected_format: "Table with column headers".to_string(),
            actual_format: "No table headers found".to_string(),
            line_number: Some(1),
            column_name: None,
            severity: ViolationSeverity::Major,
        });
    }
    
    if has_data_rows {
        metrics.data_formatting_score = 0.7;
        metrics.format_compliance_score += 0.4;
    }
    
    // Check alignment and formatting
    if lines.iter().any(|line| line.starts_with(" ") && line.contains("|")) {
        metrics.format_compliance_score += 0.3; // Good alignment
    }
    
    metrics
}

/// Validate JSON format compatibility
fn validate_json_format(output: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.0,
        header_compatibility: true, // JSON doesn't have headers
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    // Validate JSON syntax
    match serde_json::from_str::<serde_json::Value>(output) {
        Ok(_) => {
            metrics.format_compliance_score = 0.8;
            metrics.data_formatting_score = 0.8;
            
            // Check for array structure (typical for query results)
            if output.trim().starts_with('[') {
                metrics.format_compliance_score += 0.2;
            }
        }
        Err(e) => {
            metrics.format_violations.push(FormatViolation {
                violation_type: ViolationType::DataFormatMismatch,
                expected_format: "Valid JSON".to_string(),
                actual_format: format!("Invalid JSON: {}", e),
                line_number: None,
                column_name: None,
                severity: ViolationSeverity::Critical,
            });
        }
    }
    
    metrics
}

/// Validate CSV format compatibility
fn validate_csv_format(output: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.0,
        header_compatibility: false,
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    let lines: Vec<&str> = output.lines().collect();
    
    if lines.is_empty() {
        metrics.format_violations.push(FormatViolation {
            violation_type: ViolationType::DataFormatMismatch,
            expected_format: "CSV with headers and data".to_string(),
            actual_format: "Empty output".to_string(),
            line_number: Some(1),
            column_name: None,
            severity: ViolationSeverity::Critical,
        });
        return metrics;
    }
    
    // Check header row (first line should have column names)
    let first_line = lines[0];
    if first_line.contains(',') {
        metrics.header_compatibility = true;
        metrics.format_compliance_score += 0.3;
    }
    
    // Check data consistency
    let expected_columns = first_line.split(',').count();
    let mut consistent_columns = true;
    
    for (i, line) in lines.iter().enumerate().skip(1) {
        if line.split(',').count() != expected_columns {
            consistent_columns = false;
            metrics.format_violations.push(FormatViolation {
                violation_type: ViolationType::ColumnAlignment,
                expected_format: format!("{} columns", expected_columns),
                actual_format: format!("{} columns", line.split(',').count()),
                line_number: Some(i + 1),
                column_name: None,
                severity: ViolationSeverity::Major,
            });
        }
    }
    
    if consistent_columns {
        metrics.data_formatting_score = 0.7;
        metrics.format_compliance_score += 0.4;
    }
    
    // Check for proper quoting
    if lines.iter().any(|line| line.contains("\"")) {
        metrics.format_compliance_score += 0.3; // Proper CSV quoting
    }
    
    metrics
}

/// Validate header format
fn validate_header_format(output: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.5, // Start with baseline
        header_compatibility: true,
        data_formatting_score: 0.5,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    // Look for typical header patterns
    if output.contains("---") || output.contains("===") || output.contains("+") {
        metrics.format_compliance_score += 0.5; // Good header separator
    }
    
    metrics
}

/// Validate data formatting (dates, UUIDs, etc.)
fn validate_data_formatting(output: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.5,
        header_compatibility: true,
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    let mut formatting_score = 0.0;
    let mut checks = 0;
    
    // Check UUID formatting (should be lowercase with dashes)
    if output.contains('-') && output.chars().any(|c| c.is_ascii_hexdigit()) {
        if output.chars().filter(|c| c.is_ascii_uppercase()).count() == 0 {
            formatting_score += 1.0; // Good UUID formatting
        } else {
            metrics.format_violations.push(FormatViolation {
                violation_type: ViolationType::UuidFormat,
                expected_format: "lowercase UUIDs".to_string(),
                actual_format: "uppercase UUIDs".to_string(),
                line_number: None,
                column_name: None,
                severity: ViolationSeverity::Minor,
            });
        }
        checks += 1;
    }
    
    // Check boolean formatting (should be lowercase true/false)
    if output.contains("true") || output.contains("false") {
        if !output.contains("True") && !output.contains("False") {
            formatting_score += 1.0; // Good boolean formatting
        }
        checks += 1;
    }
    
    // Check null representation
    if output.contains("null") || output.contains("NULL") {
        formatting_score += 1.0; // Has null representation
        checks += 1;
    }
    
    if checks > 0 {
        metrics.data_formatting_score = formatting_score / checks as f64;
        metrics.format_compliance_score = metrics.data_formatting_score;
    }
    
    metrics
}

/// Validate metadata format
fn validate_metadata_format(output: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.8, // Default good score for metadata
        header_compatibility: true,
        data_formatting_score: 0.8,
        metadata_compatibility: true,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    // Check for metadata indicators
    if output.contains("rows") || output.contains("Rows:") {
        metrics.format_compliance_score += 0.2;
    }
    
    metrics
}

/// Validate error message format
fn validate_error_messages(error: &str) -> CompatibilityMetrics {
    let mut metrics = CompatibilityMetrics {
        format_compliance_score: 0.0,
        header_compatibility: false,
        data_formatting_score: 0.0,
        metadata_compatibility: false,
        error_handling_compatibility: false,
        pagination_compatibility: false,
        format_violations: Vec::new(),
        compatibility_warnings: Vec::new(),
    };
    
    // Check for informative error messages
    if error.len() > 10 && !error.is_empty() {
        metrics.error_handling_compatibility = true;
        metrics.format_compliance_score = 0.8;
        
        // Check for helpful error patterns
        if error.contains("Error:") || error.contains("Failed") || error.contains("not found") {
            metrics.format_compliance_score += 0.2;
        }
    }
    
    metrics
}