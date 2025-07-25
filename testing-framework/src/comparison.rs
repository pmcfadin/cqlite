//! Automated comparison between cqlsh and cqlite outputs
//!
//! This module provides comprehensive comparison functionality to validate
//! that cqlite produces output identical to cqlsh for the same queries.

use std::collections::HashMap;
use std::fmt;
use serde::{Deserialize, Serialize};
use crate::output::QueryOutput;
use crate::config::ComparisonConfig;

/// Comparison result between cqlsh and cqlite outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResult {
    pub query: String,
    pub status: ComparisonStatus,
    pub score: f64,
    pub cqlsh_output: QueryOutput,
    pub cqlite_output: QueryOutput,
    pub differences: Vec<Difference>,
    pub summary: ComparisonSummary,
    pub recommendations: Vec<String>,
}

/// Status of the comparison
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ComparisonStatus {
    Perfect,      // Identical outputs
    Minor,        // Small acceptable differences
    Major,        // Significant differences
    Failed,       // Completely different or errors
}

/// Individual difference found during comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    pub category: DifferenceCategory,
    pub severity: DifferenceSeverity,
    pub description: String,
    pub cqlsh_value: String,
    pub cqlite_value: String,
    pub location: DifferenceLocation,
}

/// Category of difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifferenceCategory {
    Format,           // Table formatting differences
    Data,             // Data value differences
    Headers,          // Column header differences
    RowCount,         // Number of rows differs
    Order,            // Row or column order differs
    Timing,           // Execution time differences
    Errors,           // Error handling differences
}

/// Severity of difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifferenceSeverity {
    Critical,         // Breaks functionality
    Major,            // Significant user impact
    Minor,            // Cosmetic or acceptable
    Negligible,       // Can be ignored
}

/// Location of difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifferenceLocation {
    pub row: Option<usize>,
    pub column: Option<usize>,
    pub field: Option<String>,
}

/// Summary of comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonSummary {
    pub total_comparisons: usize,
    pub perfect_matches: usize,
    pub minor_differences: usize,
    pub major_differences: usize,
    pub failures: usize,
    pub average_score: f64,
    pub data_accuracy: f64,
    pub format_accuracy: f64,
}

/// Automated comparison engine
pub struct ComparisonEngine {
    config: ComparisonConfig,
}

impl ComparisonEngine {
    /// Create a new comparison engine
    pub fn new(config: ComparisonConfig) -> Self {
        Self { config }
    }

    /// Compare cqlsh and cqlite outputs for a single query
    pub fn compare_outputs(&self, query: &str, cqlsh_output: QueryOutput, cqlite_output: QueryOutput) -> ComparisonResult {
        println!("🔍 Comparing outputs for query: {}", query);
        
        let mut differences = Vec::new();
        
        // Normalize outputs for comparison
        let mut normalized_cqlsh = cqlsh_output.clone();
        let mut normalized_cqlite = cqlite_output.clone();
        
        normalized_cqlsh.normalize(&self.config);
        normalized_cqlite.normalize(&self.config);
        
        // Compare different aspects
        self.compare_basic_structure(&normalized_cqlsh, &normalized_cqlite, &mut differences);
        self.compare_headers(&normalized_cqlsh, &normalized_cqlite, &mut differences);
        self.compare_data_rows(&normalized_cqlsh, &normalized_cqlite, &mut differences);
        self.compare_formatting(&normalized_cqlsh, &normalized_cqlite, &mut differences);
        self.compare_metadata(&normalized_cqlsh, &normalized_cqlite, &mut differences);
        
        // Calculate overall score
        let score = self.calculate_comparison_score(&differences);
        
        // Determine status
        let status = self.determine_status(&differences, score);
        
        // Generate recommendations
        let recommendations = self.generate_recommendations(&differences);
        
        // Create summary
        let summary = ComparisonSummary {
            total_comparisons: 1,
            perfect_matches: if status == ComparisonStatus::Perfect { 1 } else { 0 },
            minor_differences: if status == ComparisonStatus::Minor { 1 } else { 0 },
            major_differences: if status == ComparisonStatus::Major { 1 } else { 0 },
            failures: if status == ComparisonStatus::Failed { 1 } else { 0 },
            average_score: score,
            data_accuracy: self.calculate_data_accuracy(&differences),
            format_accuracy: self.calculate_format_accuracy(&differences),
        };
        
        println!("📊 Comparison complete - Status: {:?}, Score: {:.2}", status, score);
        
        ComparisonResult {
            query: query.to_string(),
            status,
            score,
            cqlsh_output,
            cqlite_output,
            differences,
            summary,
            recommendations,
        }
    }

    /// Compare basic structure (row count, column count, etc.)
    fn compare_basic_structure(&self, cqlsh: &QueryOutput, cqlite: &QueryOutput, differences: &mut Vec<Difference>) {
        // Compare row counts
        let cqlsh_rows = cqlsh.row_count.unwrap_or(cqlsh.rows.len());
        let cqlite_rows = cqlite.row_count.unwrap_or(cqlite.rows.len());
        
        if cqlsh_rows != cqlite_rows {
            differences.push(Difference {
                category: DifferenceCategory::RowCount,
                severity: DifferenceSeverity::Critical,
                description: "Row count mismatch".to_string(),
                cqlsh_value: cqlsh_rows.to_string(),
                cqlite_value: cqlite_rows.to_string(),
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("row_count".to_string()),
                },
            });
        }
        
        // Compare column counts
        let cqlsh_cols = cqlsh.column_headers.len();
        let cqlite_cols = cqlite.column_headers.len();
        
        if cqlsh_cols != cqlite_cols {
            differences.push(Difference {
                category: DifferenceCategory::Headers,
                severity: DifferenceSeverity::Major,
                description: "Column count mismatch".to_string(),
                cqlsh_value: cqlsh_cols.to_string(),
                cqlite_value: cqlite_cols.to_string(),
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("column_count".to_string()),
                },
            });
        }
    }

    /// Compare column headers
    fn compare_headers(&self, cqlsh: &QueryOutput, cqlite: &QueryOutput, differences: &mut Vec<Difference>) {
        let min_headers = cqlsh.column_headers.len().min(cqlite.column_headers.len());
        
        for i in 0..min_headers {
            let cqlsh_header = &cqlsh.column_headers[i];
            let cqlite_header = &cqlite.column_headers[i];
            
            if cqlsh_header != cqlite_header {
                differences.push(Difference {
                    category: DifferenceCategory::Headers,
                    severity: DifferenceSeverity::Major,
                    description: format!("Header mismatch at column {}", i),
                    cqlsh_value: cqlsh_header.clone(),
                    cqlite_value: cqlite_header.clone(),
                    location: DifferenceLocation {
                        row: None,
                        column: Some(i),
                        field: Some("header".to_string()),
                    },
                });
            }
        }
    }

    /// Compare data rows
    fn compare_data_rows(&self, cqlsh: &QueryOutput, cqlite: &QueryOutput, differences: &mut Vec<Difference>) {
        let min_rows = cqlsh.rows.len().min(cqlite.rows.len());
        
        for row_idx in 0..min_rows {
            let cqlsh_row = &cqlsh.rows[row_idx];
            let cqlite_row = &cqlite.rows[row_idx];
            
            let min_cols = cqlsh_row.len().min(cqlite_row.len());
            
            for col_idx in 0..min_cols {
                let cqlsh_value = &cqlsh_row[col_idx];
                let cqlite_value = &cqlite_row[col_idx];
                
                if cqlsh_value != cqlite_value {
                    let severity = self.classify_data_difference(cqlsh_value, cqlite_value);
                    
                    differences.push(Difference {
                        category: DifferenceCategory::Data,
                        severity,
                        description: format!("Data mismatch at row {}, column {}", row_idx, col_idx),
                        cqlsh_value: cqlsh_value.clone(),
                        cqlite_value: cqlite_value.clone(),
                        location: DifferenceLocation {
                            row: Some(row_idx),
                            column: Some(col_idx),
                            field: None,
                        },
                    });
                }
            }
        }
    }

    /// Compare formatting aspects
    fn compare_formatting(&self, cqlsh: &QueryOutput, cqlite: &QueryOutput, differences: &mut Vec<Difference>) {
        if !self.config.ignore_formatting {
            // Compare raw output format
            let cqlsh_format = self.extract_format_structure(&cqlsh.raw_output);
            let cqlite_format = self.extract_format_structure(&cqlite.raw_output);
            
            if cqlsh_format != cqlite_format {
                differences.push(Difference {
                    category: DifferenceCategory::Format,
                    severity: DifferenceSeverity::Minor,
                    description: "Table formatting differences".to_string(),
                    cqlsh_value: format!("Structure: {:?}", cqlsh_format),
                    cqlite_value: format!("Structure: {:?}", cqlite_format),
                    location: DifferenceLocation {
                        row: None,
                        column: None,
                        field: Some("formatting".to_string()),
                    },
                });
            }
        }
    }

    /// Compare metadata
    fn compare_metadata(&self, cqlsh: &QueryOutput, cqlite: &QueryOutput, differences: &mut Vec<Difference>) {
        // Compare execution times (if not ignoring timing)
        if !self.config.ignore_timing {
            let time_diff = (cqlsh.execution_time_ms as i64 - cqlite.execution_time_ms as i64).abs() as u64;
            if time_diff > self.config.timing_tolerance_ms {
                differences.push(Difference {
                    category: DifferenceCategory::Timing,
                    severity: DifferenceSeverity::Negligible,
                    description: "Execution time difference".to_string(),
                    cqlsh_value: format!("{}ms", cqlsh.execution_time_ms),
                    cqlite_value: format!("{}ms", cqlite.execution_time_ms),
                    location: DifferenceLocation {
                        row: None,
                        column: None,
                        field: Some("execution_time".to_string()),
                    },
                });
            }
        }
        
        // Compare error states
        let cqlsh_has_error = cqlsh.has_error();
        let cqlite_has_error = cqlite.has_error();
        
        if cqlsh_has_error != cqlite_has_error {
            differences.push(Difference {
                category: DifferenceCategory::Errors,
                severity: DifferenceSeverity::Critical,
                description: "Error state mismatch".to_string(),
                cqlsh_value: if cqlsh_has_error { "Error" } else { "Success" }.to_string(),
                cqlite_value: if cqlite_has_error { "Error" } else { "Success" }.to_string(),
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("error_state".to_string()),
                },
            });
        }
    }

    /// Classify the severity of a data difference
    fn classify_data_difference(&self, cqlsh_value: &str, cqlite_value: &str) -> DifferenceSeverity {
        // Check if it's a case difference
        if cqlsh_value.to_lowercase() == cqlite_value.to_lowercase() {
            return DifferenceSeverity::Minor;
        }
        
        // Check if it's a whitespace difference
        if cqlsh_value.trim() == cqlite_value.trim() {
            return DifferenceSeverity::Minor;
        }
        
        // Check if it's a numeric precision difference
        if let (Ok(cqlsh_num), Ok(cqlite_num)) = (cqlsh_value.parse::<f64>(), cqlite_value.parse::<f64>()) {
            let diff = (cqlsh_num - cqlite_num).abs();
            if diff < self.config.numeric_precision_tolerance {
                return DifferenceSeverity::Minor;
            }
        }
        
        // Check if it's a UUID format difference
        if self.is_uuid_format_difference(cqlsh_value, cqlite_value) {
            return DifferenceSeverity::Minor;
        }
        
        // Default to major for unclassified differences
        DifferenceSeverity::Major
    }

    /// Check if difference is just UUID formatting (case, etc.)
    fn is_uuid_format_difference(&self, val1: &str, val2: &str) -> bool {
        let uuid_pattern = regex::Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$").unwrap();
        
        if uuid_pattern.is_match(val1) && uuid_pattern.is_match(val2) {
            return val1.to_lowercase() == val2.to_lowercase();
        }
        
        false
    }

    /// Extract format structure for comparison
    fn extract_format_structure(&self, raw_output: &str) -> FormatStructure {
        FormatStructure {
            has_headers: raw_output.contains('|') && raw_output.contains('-'),
            has_separators: raw_output.contains("---") || raw_output.contains("+++"),
            line_count: raw_output.lines().count(),
            has_row_count: raw_output.contains(" rows)"),
        }
    }

    /// Calculate overall comparison score
    fn calculate_comparison_score(&self, differences: &[Difference]) -> f64 {
        if differences.is_empty() {
            return 1.0;
        }
        
        let mut penalty = 0.0;
        
        for diff in differences {
            let weight = match diff.severity {
                DifferenceSeverity::Critical => 0.5,
                DifferenceSeverity::Major => 0.2,
                DifferenceSeverity::Minor => 0.05,
                DifferenceSeverity::Negligible => 0.01,
            };
            penalty += weight;
        }
        
        (1.0 - penalty).max(0.0)
    }

    /// Determine comparison status
    fn determine_status(&self, differences: &[Difference], score: f64) -> ComparisonStatus {
        let critical_count = differences.iter().filter(|d| matches!(d.severity, DifferenceSeverity::Critical)).count();
        let major_count = differences.iter().filter(|d| matches!(d.severity, DifferenceSeverity::Major)).count();
        
        if critical_count > 0 || score < 0.5 {
            ComparisonStatus::Failed
        } else if major_count > 0 || score < 0.9 {
            ComparisonStatus::Major
        } else if !differences.is_empty() {
            ComparisonStatus::Minor
        } else {
            ComparisonStatus::Perfect
        }
    }

    /// Generate recommendations for improvements
    fn generate_recommendations(&self, differences: &[Difference]) -> Vec<String> {
        let mut recommendations = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        for diff in differences {
            let recommendation = match diff.category {
                DifferenceCategory::Format => "Implement cqlsh-compatible table formatting with proper column alignment",
                DifferenceCategory::Data => "Review data parsing logic to ensure exact value extraction",
                DifferenceCategory::Headers => "Verify column header extraction and ordering",
                DifferenceCategory::RowCount => "Check SSTable reading completeness and filtering",
                DifferenceCategory::Order => "Consider implementing result ordering to match cqlsh",
                DifferenceCategory::Timing => "Performance optimization may be needed",
                DifferenceCategory::Errors => "Improve error handling to match cqlsh behavior",
            };
            
            if seen.insert(recommendation) {
                recommendations.push(recommendation.to_string());
            }
        }
        
        if recommendations.is_empty() {
            recommendations.push("No specific recommendations - outputs match well".to_string());
        }
        
        recommendations
    }

    /// Calculate data accuracy score
    fn calculate_data_accuracy(&self, differences: &[Difference]) -> f64 {
        let data_differences = differences.iter()
            .filter(|d| matches!(d.category, DifferenceCategory::Data))
            .count();
        
        if data_differences == 0 {
            1.0
        } else {
            (1.0 - (data_differences as f64 * 0.1)).max(0.0)
        }
    }

    /// Calculate format accuracy score
    fn calculate_format_accuracy(&self, differences: &[Difference]) -> f64 {
        let format_differences = differences.iter()
            .filter(|d| matches!(d.category, DifferenceCategory::Format))
            .count();
        
        if format_differences == 0 {
            1.0
        } else {
            (1.0 - (format_differences as f64 * 0.1)).max(0.0)
        }
    }

    /// Generate detailed comparison report
    pub fn generate_report(&self, results: &[ComparisonResult]) -> ComparisonReport {
        let total = results.len();
        let perfect = results.iter().filter(|r| r.status == ComparisonStatus::Perfect).count();
        let minor = results.iter().filter(|r| r.status == ComparisonStatus::Minor).count();
        let major = results.iter().filter(|r| r.status == ComparisonStatus::Major).count();
        let failed = results.iter().filter(|r| r.status == ComparisonStatus::Failed).count();
        
        let average_score = if total > 0 {
            results.iter().map(|r| r.score).sum::<f64>() / total as f64
        } else {
            0.0
        };
        
        ComparisonReport {
            summary: ComparisonSummary {
                total_comparisons: total,
                perfect_matches: perfect,
                minor_differences: minor,
                major_differences: major,
                failures: failed,
                average_score,
                data_accuracy: self.calculate_overall_data_accuracy(results),
                format_accuracy: self.calculate_overall_format_accuracy(results),
            },
            results: results.to_vec(),
            recommendations: self.generate_overall_recommendations(results),
        }
    }

    fn calculate_overall_data_accuracy(&self, results: &[ComparisonResult]) -> f64 {
        if results.is_empty() {
            return 0.0;
        }
        
        results.iter().map(|r| r.summary.data_accuracy).sum::<f64>() / results.len() as f64
    }

    fn calculate_overall_format_accuracy(&self, results: &[ComparisonResult]) -> f64 {
        if results.is_empty() {
            return 0.0;
        }
        
        results.iter().map(|r| r.summary.format_accuracy).sum::<f64>() / results.len() as f64
    }

    fn generate_overall_recommendations(&self, results: &[ComparisonResult]) -> Vec<String> {
        let mut all_recommendations = Vec::new();
        for result in results {
            all_recommendations.extend(result.recommendations.clone());
        }
        
        // Deduplicate and prioritize
        let mut unique_recommendations: Vec<String> = all_recommendations.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique_recommendations.sort();
        unique_recommendations
    }
}

/// Format structure for comparison
#[derive(Debug, PartialEq)]
struct FormatStructure {
    has_headers: bool,
    has_separators: bool,
    line_count: usize,
    has_row_count: bool,
}

/// Complete comparison report
#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub summary: ComparisonSummary,
    pub results: Vec<ComparisonResult>,
    pub recommendations: Vec<String>,
}

impl fmt::Display for ComparisonResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Query: {}", self.query)?;
        writeln!(f, "Status: {:?} (Score: {:.2})", self.status, self.score)?;
        writeln!(f, "Differences: {}", self.differences.len())?;
        
        for diff in &self.differences {
            writeln!(f, "  {:?} - {}: {} -> {}", 
                diff.severity, diff.description, diff.cqlsh_value, diff.cqlite_value)?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ComparisonConfig;

    #[test]
    fn test_perfect_match() {
        let config = ComparisonConfig::default();
        let engine = ComparisonEngine::new(config);
        
        let output1 = QueryOutput {
            rows: vec![vec!["1".to_string(), "John".to_string()]],
            column_headers: vec!["id".to_string(), "name".to_string()],
            row_count: Some(1),
            ..Default::default()
        };
        
        let output2 = output1.clone();
        
        let result = engine.compare_outputs("SELECT * FROM test", output1, output2);
        assert_eq!(result.status, ComparisonStatus::Perfect);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_data_mismatch() {
        let config = ComparisonConfig::default();
        let engine = ComparisonEngine::new(config);
        
        let output1 = QueryOutput {
            rows: vec![vec!["1".to_string(), "John".to_string()]],
            column_headers: vec!["id".to_string(), "name".to_string()],
            ..Default::default()
        };
        
        let output2 = QueryOutput {
            rows: vec![vec!["1".to_string(), "Jane".to_string()]],
            column_headers: vec!["id".to_string(), "name".to_string()],
            ..Default::default()
        };
        
        let result = engine.compare_outputs("SELECT * FROM test", output1, output2);
        assert!(result.differences.len() > 0);
        assert!(result.score < 1.0);
    }
}