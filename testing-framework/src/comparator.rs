//! Output comparison logic for testing framework

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::config::ComparisonConfig;
use crate::output::QueryOutput;

/// Compares query outputs between cqlsh and cqlite
pub struct OutputComparator {
    config: ComparisonConfig,
}

/// Result of comparing two query outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResult {
    /// Whether the outputs match according to the configuration
    pub is_match: bool,
    
    /// Overall confidence score (0.0 to 1.0)
    pub confidence_score: f64,
    
    /// Detailed comparison results by category
    pub details: ComparisonDetails,
    
    /// Human-readable summary of the comparison
    pub summary: String,
    
    /// Differences found (if any)
    pub differences: Vec<Difference>,
    
    /// Metadata about the comparison process
    pub metadata: HashMap<String, String>,
}

/// Detailed comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonDetails {
    /// Structure comparison (columns, row count, etc.)
    pub structure: StructureComparison,
    
    /// Data content comparison
    pub content: ContentComparison,
    
    /// Performance comparison
    pub performance: PerformanceComparison,
    
    /// Error comparison (if applicable)
    pub errors: ErrorComparison,
}

/// Structure comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructureComparison {
    pub column_headers_match: bool,
    pub row_count_match: bool,
    pub column_count_match: bool,
    pub data_types_match: bool,
}

/// Content comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentComparison {
    pub identical_rows: usize,
    pub different_rows: usize,
    pub missing_rows: usize,
    pub extra_rows: usize,
    pub content_similarity: f64, // 0.0 to 1.0
}

/// Performance comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceComparison {
    pub cqlsh_time_ms: u64,
    pub cqlite_time_ms: u64,
    pub time_difference_ms: i64,
    pub relative_performance: f64, // cqlite_time / cqlsh_time
}

/// Error comparison results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorComparison {
    pub both_succeeded: bool,
    pub both_failed: bool,
    pub error_types_match: bool,
    pub error_messages_similar: bool,
}

/// Individual difference found during comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    pub category: DifferenceCategory,
    pub description: String,
    pub severity: DifferenceSeverity,
    pub location: DifferenceLocation,
    pub expected: String,
    pub actual: String,
}

/// Categories of differences
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifferenceCategory {
    Structure,
    Content,
    Performance,
    Error,
    Format,
    Metadata,
}

/// Severity levels for differences
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DifferenceSeverity {
    Info,
    Minor,
    Major,
    Critical,
}

/// Location of a difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifferenceLocation {
    pub row: Option<usize>,
    pub column: Option<usize>,
    pub field: Option<String>,
}

impl OutputComparator {
    /// Create a new output comparator
    pub fn new(config: &ComparisonConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Compare two query outputs
    pub fn compare(&self, cqlsh_output: &QueryOutput, cqlite_output: &QueryOutput) -> Result<ComparisonResult> {
        let mut differences = Vec::new();
        let mut confidence_score = 1.0;
        
        // Normalize outputs for comparison
        let mut normalized_cqlsh = cqlsh_output.clone();
        let mut normalized_cqlite = cqlite_output.clone();
        
        normalized_cqlsh.normalize(&self.config);
        normalized_cqlite.normalize(&self.config);

        // Compare structure
        let structure = self.compare_structure(&normalized_cqlsh, &normalized_cqlite, &mut differences)?;
        if !structure.column_headers_match || !structure.row_count_match {
            confidence_score *= 0.7;
        }

        // Compare content
        let content = self.compare_content(&normalized_cqlsh, &normalized_cqlite, &mut differences)?;
        if content.different_rows > 0 {
            confidence_score *= content.content_similarity;
        }

        // Compare performance
        let performance = self.compare_performance(cqlsh_output, cqlite_output, &mut differences);

        // Compare errors
        let errors = self.compare_errors(cqlsh_output, cqlite_output, &mut differences)?;
        if !errors.both_succeeded && !errors.both_failed {
            confidence_score *= 0.3; // Major issue if one succeeds and other fails
        }

        let details = ComparisonDetails {
            structure,
            content,
            performance,
            errors,
        };

        let is_match = self.determine_overall_match(&details, &differences);
        let summary = self.generate_summary(&details, &differences, is_match);
        
        let mut metadata = HashMap::new();
        metadata.insert("comparison_timestamp".to_string(), chrono::Utc::now().to_rfc3339());
        metadata.insert("config_hash".to_string(), format!("{:?}", self.config));
        metadata.insert("difference_count".to_string(), differences.len().to_string());

        Ok(ComparisonResult {
            is_match,
            confidence_score,
            details,
            summary,
            differences,
            metadata,
        })
    }

    /// Compare structural aspects of the outputs
    fn compare_structure(
        &self,
        cqlsh: &QueryOutput,
        cqlite: &QueryOutput,
        differences: &mut Vec<Difference>,
    ) -> Result<StructureComparison> {
        let column_headers_match = if self.config.ignore_column_order {
            let mut cqlsh_headers = cqlsh.column_headers.clone();
            let mut cqlite_headers = cqlite.column_headers.clone();
            cqlsh_headers.sort();
            cqlite_headers.sort();
            cqlsh_headers == cqlite_headers
        } else {
            cqlsh.column_headers == cqlite.column_headers
        };

        if !column_headers_match {
            differences.push(Difference {
                category: DifferenceCategory::Structure,
                description: "Column headers do not match".to_string(),
                severity: DifferenceSeverity::Major,
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("headers".to_string()),
                },
                expected: format!("{:?}", cqlsh.column_headers),
                actual: format!("{:?}", cqlite.column_headers),
            });
        }

        let row_count_match = cqlsh.rows.len() == cqlite.rows.len();
        if !row_count_match {
            differences.push(Difference {
                category: DifferenceCategory::Structure,
                description: "Row count mismatch".to_string(),
                severity: DifferenceSeverity::Major,
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("row_count".to_string()),
                },
                expected: cqlsh.rows.len().to_string(),
                actual: cqlite.rows.len().to_string(),
            });
        }

        let column_count_match = cqlsh.column_headers.len() == cqlite.column_headers.len();
        if !column_count_match {
            differences.push(Difference {
                category: DifferenceCategory::Structure,
                description: "Column count mismatch".to_string(),
                severity: DifferenceSeverity::Major,
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("column_count".to_string()),
                },
                expected: cqlsh.column_headers.len().to_string(),
                actual: cqlite.column_headers.len().to_string(),
            });
        }

        Ok(StructureComparison {
            column_headers_match,
            row_count_match,
            column_count_match,
            data_types_match: true, // TODO: Implement type checking
        })
    }

    /// Compare content of the outputs
    fn compare_content(
        &self,
        cqlsh: &QueryOutput,
        cqlite: &QueryOutput,
        differences: &mut Vec<Difference>,
    ) -> Result<ContentComparison> {
        let mut identical_rows = 0;
        let mut different_rows = 0;
        let max_rows = std::cmp::max(cqlsh.rows.len(), cqlite.rows.len());
        
        for i in 0..max_rows {
            match (cqlsh.rows.get(i), cqlite.rows.get(i)) {
                (Some(cqlsh_row), Some(cqlite_row)) => {
                    if cqlsh_row == cqlite_row {
                        identical_rows += 1;
                    } else {
                        different_rows += 1;
                        differences.push(Difference {
                            category: DifferenceCategory::Content,
                            description: format!("Row {} content differs", i),
                            severity: DifferenceSeverity::Minor,
                            location: DifferenceLocation {
                                row: Some(i),
                                column: None,
                                field: None,
                            },
                            expected: format!("{:?}", cqlsh_row),
                            actual: format!("{:?}", cqlite_row),
                        });
                    }
                }
                (Some(_), None) => {
                    differences.push(Difference {
                        category: DifferenceCategory::Content,
                        description: format!("Missing row {} in cqlite output", i),
                        severity: DifferenceSeverity::Major,
                        location: DifferenceLocation {
                            row: Some(i),
                            column: None,
                            field: None,
                        },
                        expected: "Present".to_string(),
                        actual: "Missing".to_string(),
                    });
                }
                (None, Some(_)) => {
                    differences.push(Difference {
                        category: DifferenceCategory::Content,
                        description: format!("Extra row {} in cqlite output", i),
                        severity: DifferenceSeverity::Major,
                        location: DifferenceLocation {
                            row: Some(i),
                            column: None,
                            field: None,
                        },
                        expected: "Missing".to_string(),
                        actual: "Present".to_string(),
                    });
                }
                (None, None) => break,
            }
        }

        let missing_rows = if cqlsh.rows.len() > cqlite.rows.len() {
            cqlsh.rows.len() - cqlite.rows.len()
        } else {
            0
        };
        
        let extra_rows = if cqlite.rows.len() > cqlsh.rows.len() {
            cqlite.rows.len() - cqlsh.rows.len()
        } else {
            0
        };

        let content_similarity = if max_rows == 0 {
            1.0
        } else {
            identical_rows as f64 / max_rows as f64
        };

        Ok(ContentComparison {
            identical_rows,
            different_rows,
            missing_rows,
            extra_rows,
            content_similarity,
        })
    }

    /// Compare performance metrics
    fn compare_performance(
        &self,
        cqlsh: &QueryOutput,
        cqlite: &QueryOutput,
        differences: &mut Vec<Difference>,
    ) -> PerformanceComparison {
        let time_difference_ms = cqlite.execution_time_ms as i64 - cqlsh.execution_time_ms as i64;
        let relative_performance = if cqlsh.execution_time_ms == 0 {
            1.0
        } else {
            cqlite.execution_time_ms as f64 / cqlsh.execution_time_ms as f64
        };

        // Add performance difference as info if significant
        if time_difference_ms.abs() > 1000 { // More than 1 second difference
            differences.push(Difference {
                category: DifferenceCategory::Performance,
                description: format!("Significant execution time difference: {} ms", time_difference_ms),
                severity: DifferenceSeverity::Info,
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("execution_time".to_string()),
                },
                expected: format!("{} ms", cqlsh.execution_time_ms),
                actual: format!("{} ms", cqlite.execution_time_ms),
            });
        }

        PerformanceComparison {
            cqlsh_time_ms: cqlsh.execution_time_ms,
            cqlite_time_ms: cqlite.execution_time_ms,
            time_difference_ms,
            relative_performance,
        }
    }

    /// Compare error conditions
    fn compare_errors(
        &self,
        cqlsh: &QueryOutput,
        cqlite: &QueryOutput,
        differences: &mut Vec<Difference>,
    ) -> Result<ErrorComparison> {
        let cqlsh_has_error = cqlsh.has_error();
        let cqlite_has_error = cqlite.has_error();
        
        let both_succeeded = !cqlsh_has_error && !cqlite_has_error;
        let both_failed = cqlsh_has_error && cqlite_has_error;
        
        if cqlsh_has_error != cqlite_has_error {
            differences.push(Difference {
                category: DifferenceCategory::Error,
                description: "Error status mismatch".to_string(),
                severity: DifferenceSeverity::Critical,
                location: DifferenceLocation {
                    row: None,
                    column: None,
                    field: Some("error_status".to_string()),
                },
                expected: if cqlsh_has_error { "Error" } else { "Success" }.to_string(),
                actual: if cqlite_has_error { "Error" } else { "Success" }.to_string(),
            });
        }

        Ok(ErrorComparison {
            both_succeeded,
            both_failed,
            error_types_match: both_failed, // TODO: Implement detailed error type comparison
            error_messages_similar: both_failed, // TODO: Implement error message similarity
        })
    }

    /// Determine if outputs match overall
    fn determine_overall_match(&self, details: &ComparisonDetails, differences: &[Difference]) -> bool {
        // If there are critical differences, it's not a match
        if differences.iter().any(|d| d.severity == DifferenceSeverity::Critical) {
            return false;
        }

        // Must have matching success/failure status
        if !details.errors.both_succeeded && !details.errors.both_failed {
            return false;
        }

        // Structure must match for basic compatibility
        if !details.structure.column_headers_match || !details.structure.row_count_match {
            return false;
        }

        // Content similarity must be high enough
        if details.content.content_similarity < 0.95 {
            return false;
        }

        true
    }

    /// Generate human-readable summary
    fn generate_summary(&self, details: &ComparisonDetails, differences: &[Difference], is_match: bool) -> String {
        if is_match {
            format!(
                "✅ Outputs match! {} identical rows, {:.1}% content similarity, {}ms vs {}ms execution time",
                details.content.identical_rows,
                details.content.content_similarity * 100.0,
                details.performance.cqlsh_time_ms,
                details.performance.cqlite_time_ms
            )
        } else {
            let critical_count = differences.iter().filter(|d| d.severity == DifferenceSeverity::Critical).count();
            let major_count = differences.iter().filter(|d| d.severity == DifferenceSeverity::Major).count();
            
            format!(
                "❌ Outputs differ: {} critical, {} major differences. Content similarity: {:.1}%",
                critical_count,
                major_count,
                details.content.content_similarity * 100.0
            )
        }
    }
}

impl ComparisonResult {
    /// Check if the comparison indicates a match
    pub fn is_match(&self) -> bool {
        self.is_match
    }

    /// Create an error result
    pub fn error(message: &str) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("error".to_string(), message.to_string());
        
        Self {
            is_match: false,
            confidence_score: 0.0,
            details: ComparisonDetails {
                structure: StructureComparison {
                    column_headers_match: false,
                    row_count_match: false,
                    column_count_match: false,
                    data_types_match: false,
                },
                content: ContentComparison {
                    identical_rows: 0,
                    different_rows: 0,
                    missing_rows: 0,
                    extra_rows: 0,
                    content_similarity: 0.0,
                },
                performance: PerformanceComparison {
                    cqlsh_time_ms: 0,
                    cqlite_time_ms: 0,
                    time_difference_ms: 0,
                    relative_performance: 0.0,
                },
                errors: ErrorComparison {
                    both_succeeded: false,
                    both_failed: false,
                    error_types_match: false,
                    error_messages_similar: false,
                },
            },
            summary: format!("❌ Comparison failed: {}", message),
            differences: vec![],
            metadata,
        }
    }

    /// Get the most severe difference level
    pub fn max_severity(&self) -> Option<DifferenceSeverity> {
        self.differences.iter().map(|d| &d.severity).max().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ComparisonConfig;

    #[test]
    fn test_comparator_creation() {
        let config = ComparisonConfig {
            ignore_whitespace: true,
            ignore_case: false,
            ignore_column_order: false,
            ignore_row_order: false,
            ignore_timestamps: false,
            timestamp_tolerance_ms: 1000,
            numeric_precision_tolerance: 0.0001,
            normalize_uuids: true,
            custom_normalizers: vec![],
        };
        
        let comparator = OutputComparator::new(&config);
        assert!(true); // Just test creation doesn't panic
    }

    #[test]
    fn test_identical_outputs() {
        let config = ComparisonConfig {
            ignore_whitespace: true,
            ignore_case: false,
            ignore_column_order: false,
            ignore_row_order: false,
            ignore_timestamps: false,
            timestamp_tolerance_ms: 1000,
            numeric_precision_tolerance: 0.0001,
            normalize_uuids: true,
            custom_normalizers: vec![],
        };
        
        let comparator = OutputComparator::new(&config);
        
        let output1 = QueryOutput {
            raw_output: "test".to_string(),
            rows: vec![vec!["1".to_string(), "test".to_string()]],
            column_headers: vec!["id".to_string(), "name".to_string()],
            ..Default::default()
        };
        
        let output2 = output1.clone();
        
        let result = comparator.compare(&output1, &output2).unwrap();
        assert!(result.is_match());
        assert_eq!(result.details.content.identical_rows, 1);
        assert_eq!(result.details.content.different_rows, 0);
    }
}