use crate::comparator::{CellDifference, ComparisonResult, DifferenceSeverity};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationReport {
    pub sstable_path: PathBuf,
    pub timestamp: String,
    pub comparison_result: ComparisonResult,
    pub summary: ValidationSummary,
    pub recommendations: Vec<String>,
    pub fail_on_diff: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub overall_status: ValidationStatus,
    pub compatibility_percentage: f64,
    pub critical_issues: usize,
    pub high_issues: usize,
    pub medium_issues: usize,
    pub low_issues: usize,
    pub total_cells_compared: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ValidationStatus {
    Perfect,      // No differences found
    Compatible,   // Minor differences that don't affect functionality
    Incompatible, // Significant differences that may cause issues
    Failed,       // Critical differences or errors
}

// ReportFormat removed - unused enum

impl ValidationReport {
    pub fn new(
        sstable_path: PathBuf,
        comparison_result: ComparisonResult,
        _detailed: bool,
        fail_on_diff: bool,
    ) -> Self {
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Categorize differences by severity
        let mut critical_issues = 0;
        let mut high_issues = 0;
        let mut medium_issues = 0;
        let mut low_issues = 0;

        for diff in &comparison_result.differences {
            match diff.severity {
                DifferenceSeverity::Critical => critical_issues += 1,
                DifferenceSeverity::High => high_issues += 1,
                DifferenceSeverity::Medium => medium_issues += 1,
                DifferenceSeverity::Low => low_issues += 1,
                DifferenceSeverity::Info => {} // Don't count info-level
            }
        }

        // Determine overall status
        let overall_status = if critical_issues > 0 {
            ValidationStatus::Failed
        } else if high_issues > 0 {
            ValidationStatus::Incompatible
        } else if medium_issues > 0 || low_issues > 0 {
            ValidationStatus::Compatible
        } else {
            ValidationStatus::Perfect
        };

        // Generate recommendations
        let recommendations = Self::generate_recommendations(
            &comparison_result.differences,
            critical_issues,
            high_issues,
        );

        let summary = ValidationSummary {
            overall_status,
            compatibility_percentage: comparison_result.summary.compatibility_score * 100.0,
            critical_issues,
            high_issues,
            medium_issues,
            low_issues,
            total_cells_compared: comparison_result.summary.total_cells_compared,
        };

        Self {
            sstable_path,
            timestamp,
            comparison_result,
            summary,
            recommendations,
            fail_on_diff,
        }
    }

    pub fn has_differences(&self) -> bool {
        !self.comparison_result.differences.is_empty()
    }

    pub fn difference_count(&self) -> usize {
        self.comparison_result.differences.len()
    }

    #[allow(dead_code)]
    pub fn should_fail_ci(&self) -> bool {
        self.fail_on_diff && self.has_differences()
    }

    #[allow(dead_code)]
    pub fn format_as_text(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str("🔍 SSTABLEDUMP VALIDATION REPORT\n");
        output.push_str("=".repeat(50).as_str());
        output.push('\n');
        output.push_str(&format!("📁 SSTable: {:?}\n", self.sstable_path));
        output.push_str(&format!("⏰ Timestamp: {}\n", self.timestamp));
        output.push_str(&format!("🎯 Zero Tolerance Mode: {}\n", self.fail_on_diff));
        output.push('\n');

        // Status indicator
        let status_emoji = match self.summary.overall_status {
            ValidationStatus::Perfect => "✅",
            ValidationStatus::Compatible => "⚠️",
            ValidationStatus::Incompatible => "❌",
            ValidationStatus::Failed => "🚨",
        };

        output.push_str(&format!(
            "{} OVERALL STATUS: {:?}\n",
            status_emoji, self.summary.overall_status
        ));
        output.push_str(&format!(
            "📊 Compatibility: {:.2}%\n",
            self.summary.compatibility_percentage
        ));
        output.push('\n');

        // Statistics
        output.push_str("📈 COMPARISON STATISTICS\n");
        output.push_str("-".repeat(30).as_str());
        output.push('\n');
        output.push_str(&format!(
            "Total cells compared: {}\n",
            self.summary.total_cells_compared
        ));
        output.push_str(&format!(
            "Matching cells: {}\n",
            self.comparison_result.summary.matching_cells
        ));
        output.push_str(&format!(
            "Different cells: {}\n",
            self.comparison_result.summary.different_cells
        ));
        output.push_str(&format!(
            "Missing in Cassandra: {}\n",
            self.comparison_result.summary.missing_in_cassandra
        ));
        output.push_str(&format!(
            "Missing in CQLite: {}\n",
            self.comparison_result.summary.missing_in_cqlite
        ));
        output.push('\n');

        // Issue breakdown
        if self.has_differences() {
            output.push_str("🚨 ISSUES FOUND\n");
            output.push_str("-".repeat(20).as_str());
            output.push('\n');
            output.push_str(&format!("🔴 Critical: {}\n", self.summary.critical_issues));
            output.push_str(&format!("🟠 High: {}\n", self.summary.high_issues));
            output.push_str(&format!("🟡 Medium: {}\n", self.summary.medium_issues));
            output.push_str(&format!("🔵 Low: {}\n", self.summary.low_issues));
            output.push('\n');

            // Show first few critical differences
            let critical_diffs: Vec<_> = self
                .comparison_result
                .differences
                .iter()
                .filter(|d| d.severity == DifferenceSeverity::Critical)
                .take(5)
                .collect();

            if !critical_diffs.is_empty() {
                output.push_str("🔴 CRITICAL DIFFERENCES (First 5)\n");
                output.push_str("-".repeat(35).as_str());
                output.push('\n');

                for (i, diff) in critical_diffs.iter().enumerate() {
                    output.push_str(&format!("{}. {}\n", i + 1, self._format_difference(diff)));
                }
                output.push('\n');
            }
        }

        // Recommendations
        if !self.recommendations.is_empty() {
            output.push_str("💡 RECOMMENDATIONS\n");
            output.push_str("-".repeat(20).as_str());
            output.push('\n');
            for (i, rec) in self.recommendations.iter().enumerate() {
                output.push_str(&format!("{}. {}\n", i + 1, rec));
            }
            output.push('\n');
        }

        // CI failure warning
        if self.should_fail_ci() {
            output.push_str("🚨 CI FAILURE WARNING\n");
            output.push_str("=".repeat(25).as_str());
            output.push('\n');
            output.push_str("Zero tolerance mode is enabled and differences were found.\n");
            output.push_str("This validation will FAIL the CI pipeline as requested.\n");
            output.push('\n');
        }

        // Performance info
        output.push_str("⚡ PERFORMANCE\n");
        output.push_str("-".repeat(15).as_str());
        output.push('\n');
        output.push_str(&format!(
            "Comparison time: {}ms\n",
            self.comparison_result.statistics.comparison_duration_ms
        ));
        output.push_str(&format!(
            "Cells/second: {:.0}\n",
            self.summary.total_cells_compared as f64
                / (self.comparison_result.statistics.comparison_duration_ms as f64 / 1000.0)
        ));

        output
    }

    pub fn _format_as_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn _format_as_csv(&self) -> String {
        let mut output = String::new();

        // CSV header
        output.push_str("location,difference_type,severity,cassandra_value,cqlite_value\n");

        // CSV rows
        for diff in &self.comparison_result.differences {
            let location = format!(
                "{}/{}/{}",
                diff.location.partition_key,
                diff.location.clustering_key.as_deref().unwrap_or(""),
                diff.location.column_name
            );

            let cassandra_value = diff
                .cassandra_value
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "NULL".to_string());

            let cqlite_value = diff
                .cqlite_value
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "NULL".to_string());

            output.push_str(&format!(
                "{},{:?},{:?},\"{}\",\"{}\"\n",
                location,
                diff.difference_type,
                diff.severity,
                cassandra_value.replace("\"", "\"\""), // Escape quotes
                cqlite_value.replace("\"", "\"\"")
            ));
        }

        output
    }

    pub fn _format_as_junit(&self) -> String {
        let test_name = format!(
            "sstabledump_validation_{}",
            self.sstable_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
        );

        let mut output = String::new();
        output.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        output.push_str("<testsuite name=\"SSTableDump Validation\" tests=\"1\">\n");

        if self.should_fail_ci() {
            output.push_str(&format!(
                "  <testcase name=\"{test_name}\" classname=\"ValidationHarness\">\n"
            ));
            output.push_str("    <failure message=\"Cell-by-cell comparison failed\">\n");
            output.push_str(&format!(
                "      Found {} differences in zero-tolerance mode.\n",
                self.difference_count()
            ));
            output.push_str(&format!(
                "      Compatibility: {:.2}%\n",
                self.summary.compatibility_percentage
            ));
            output.push_str("    </failure>\n");
            output.push_str("  </testcase>\n");
        } else {
            output.push_str(&format!(
                "  <testcase name=\"{test_name}\" classname=\"ValidationHarness\">\n"
            ));
            if self.has_differences() {
                output.push_str("    <skipped message=\"Differences found but not in zero-tolerance mode\" />\n");
            }
            output.push_str("  </testcase>\n");
        }

        output.push_str("</testsuite>\n");
        output
    }

    fn _format_difference(&self, diff: &CellDifference) -> String {
        let location = format!(
            "{}/{}/{}",
            diff.location.partition_key,
            diff.location
                .clustering_key
                .as_deref()
                .unwrap_or("(no clustering)"),
            diff.location.column_name
        );

        match &diff.difference_type {
            crate::comparator::DifferenceType::ValueMismatch => {
                format!(
                    "{}: Value mismatch - Cassandra: {:?}, CQLite: {:?}",
                    location,
                    diff.cassandra_value
                        .as_ref()
                        .unwrap_or(&crate::parser::CellValue::Null),
                    diff.cqlite_value
                        .as_ref()
                        .unwrap_or(&crate::parser::CellValue::Null)
                )
            }
            crate::comparator::DifferenceType::MissingInCqlite => {
                format!(
                    "{}: Missing in CQLite - Cassandra has: {:?}",
                    location,
                    diff.cassandra_value
                        .as_ref()
                        .unwrap_or(&crate::parser::CellValue::Null)
                )
            }
            crate::comparator::DifferenceType::MissingInCassandra => {
                format!(
                    "{}: Missing in Cassandra - CQLite has: {:?}",
                    location,
                    diff.cqlite_value
                        .as_ref()
                        .unwrap_or(&crate::parser::CellValue::Null)
                )
            }
            other => {
                format!("{location}: {other:?}")
            }
        }
    }

    fn generate_recommendations(
        differences: &[CellDifference],
        critical_issues: usize,
        high_issues: usize,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if critical_issues > 0 {
            recommendations.push(
                "🚨 CRITICAL: Fix data reading incompatibilities before production use".to_string(),
            );
            recommendations
                .push("Review SSTable parsing implementation for format compliance".to_string());
        }

        if high_issues > 0 {
            recommendations.push("⚠️  Address high-priority compatibility issues".to_string());
            recommendations
                .push("Verify timestamp and TTL handling matches Cassandra behavior".to_string());
        }

        // Check for specific patterns
        let has_missing_data = differences.iter().any(|d| {
            matches!(
                d.difference_type,
                crate::comparator::DifferenceType::MissingInCqlite
                    | crate::comparator::DifferenceType::MissingInCassandra
            )
        });

        if has_missing_data {
            recommendations.push(
                "🔍 Investigate missing data - check partition/row parsing logic".to_string(),
            );
        }

        let has_type_mismatches = differences.iter().any(|d| {
            matches!(
                d.difference_type,
                crate::comparator::DifferenceType::TypeMismatch
            )
        });

        if has_type_mismatches {
            recommendations.push("🔧 Fix data type interpretation mismatches".to_string());
        }

        if differences.is_empty() {
            recommendations.push(
                "✅ Perfect compatibility! CQLite output matches Cassandra exactly".to_string(),
            );
        } else {
            recommendations.push(format!(
                "📊 Monitor compatibility score: {:.1}% - target is 100%",
                (differences.len() as f64 / (differences.len() + 100) as f64) * 100.0
            ));
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::*;
    use crate::parser::*;
    use std::path::PathBuf;

    /// Helper for floating point comparison with epsilon tolerance
    fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() <= eps.max(1e-12)
    }

    #[test]
    fn test_mixed_sections_status_priority() {
        // Test priority: Failed > Incompatible > Compatible > Perfect
        // Scenario: 1 matching cell out of 3 total cells = 33.333...%
        let comparison_result = ComparisonResult {
            summary: ComparisonSummary {
                total_cells_compared: 3,
                matching_cells: 1,
                different_cells: 2,
                missing_in_cassandra: 0,
                missing_in_cqlite: 0,
                compatibility_score: 1.0 / 3.0, // 0.3333...
            },
            differences: vec![
                // One critical issue -> should trigger Failed status
                CellDifference {
                    location: CellLocation {
                        partition_key: "pk1".to_string(),
                        clustering_key: None,
                        column_name: "col1".to_string(),
                        row_index: 0,
                        cell_index: 0,
                    },
                    difference_type: DifferenceType::ValueMismatch,
                    cassandra_value: Some(CellValue::Text("a".to_string())),
                    cqlite_value: Some(CellValue::Text("b".to_string())),
                    severity: DifferenceSeverity::Critical,
                },
                // One high issue (should not override Critical)
                CellDifference {
                    location: CellLocation {
                        partition_key: "pk1".to_string(),
                        clustering_key: None,
                        column_name: "col2".to_string(),
                        row_index: 0,
                        cell_index: 1,
                    },
                    difference_type: DifferenceType::TtlMismatch,
                    cassandra_value: Some(CellValue::Integer(100)),
                    cqlite_value: Some(CellValue::Integer(200)),
                    severity: DifferenceSeverity::High,
                },
            ],
            statistics: ComparisonStatistics {
                cassandra_partitions: 1,
                cqlite_partitions: 1,
                cassandra_rows: 1,
                cqlite_rows: 1,
                cassandra_cells: 3,
                cqlite_cells: 3,
                comparison_duration_ms: 100,
            },
        };

        let report = ValidationReport::new(
            PathBuf::from("/test/sstable.db"),
            comparison_result,
            true,  // detailed
            false, // fail_on_diff
        );

        // Verify status priority: Critical issues -> Failed
        assert_eq!(report.summary.overall_status, ValidationStatus::Failed);

        // Verify issue counts
        assert_eq!(report.summary.critical_issues, 1);
        assert_eq!(report.summary.high_issues, 1);
        assert_eq!(report.summary.medium_issues, 0);
        assert_eq!(report.summary.low_issues, 0);

        // Verify compatibility percentage using approx_eq
        // 1/3 * 100 = 33.333333333333336 (IEEE 754)
        let expected_percentage = 100.0 / 3.0; // 33.333...
        assert!(
            approx_eq(
                report.summary.compatibility_percentage,
                expected_percentage,
                1e-9
            ),
            "Expected compatibility_percentage ~{}, got {}",
            expected_percentage,
            report.summary.compatibility_percentage
        );
    }

    #[test]
    fn test_validation_report_creation() {
        let comparison_result = ComparisonResult {
            summary: ComparisonSummary {
                total_cells_compared: 100,
                matching_cells: 95,
                different_cells: 5,
                missing_in_cassandra: 1,
                missing_in_cqlite: 2,
                compatibility_score: 0.95,
            },
            differences: vec![CellDifference {
                location: CellLocation {
                    partition_key: "test_partition".to_string(),
                    clustering_key: None,
                    column_name: "test_column".to_string(),
                    row_index: 0,
                    cell_index: 0,
                },
                difference_type: DifferenceType::ValueMismatch,
                cassandra_value: Some(CellValue::Text("cassandra".to_string())),
                cqlite_value: Some(CellValue::Text("cqlite".to_string())),
                severity: DifferenceSeverity::Critical,
            }],
            statistics: ComparisonStatistics {
                cassandra_partitions: 1,
                cqlite_partitions: 1,
                cassandra_rows: 1,
                cqlite_rows: 1,
                cassandra_cells: 100,
                cqlite_cells: 100,
                comparison_duration_ms: 1000,
            },
        };

        let report = ValidationReport::new(
            PathBuf::from("/test/sstable.db"),
            comparison_result,
            true,
            true,
        );

        assert_eq!(report.summary.overall_status, ValidationStatus::Failed);
        assert_eq!(report.summary.critical_issues, 1);
        assert!(report.should_fail_ci());
    }

    #[test]
    fn test_text_format_output() {
        let comparison_result = ComparisonResult {
            summary: ComparisonSummary {
                total_cells_compared: 100,
                matching_cells: 100,
                different_cells: 0,
                missing_in_cassandra: 0,
                missing_in_cqlite: 0,
                compatibility_score: 1.0,
            },
            differences: vec![],
            statistics: ComparisonStatistics {
                cassandra_partitions: 1,
                cqlite_partitions: 1,
                cassandra_rows: 1,
                cqlite_rows: 1,
                cassandra_cells: 100,
                cqlite_cells: 100,
                comparison_duration_ms: 500,
            },
        };

        let report = ValidationReport::new(
            PathBuf::from("/test/sstable.db"),
            comparison_result,
            false,
            false,
        );

        let text_output = report.format_as_text();
        assert!(text_output.contains("✅ OVERALL STATUS: Perfect"));
        assert!(text_output.contains("100.00%"));
    }
}
