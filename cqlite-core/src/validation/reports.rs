//! Validation Report Generation Framework
//!
//! This module provides comprehensive report generation for Issue #17 validation results.

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

/// Validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub title: String,
    pub sections: Vec<ValidationSection>,
    pub overall_status: ValidationReportStatus,
    pub summary: ValidationSummary,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Individual validation section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSection {
    pub name: String,
    pub status: ValidationSectionStatus,
    pub details: String,
    pub metrics: HashMap<String, f64>,
    pub recommendations: Vec<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Status of a validation section
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationSectionStatus {
    Passed,
    Failed,
    Warning,
    Error,
    Skipped,
}

/// Overall status of the validation report
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationReportStatus {
    Passed,
    Failed,
    Warning,
}

/// Summary statistics for the validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub total_sections: usize,
    pub passed_sections: usize,
    pub failed_sections: usize,
    pub warning_sections: usize,
    pub error_sections: usize,
    pub skipped_sections: usize,
    pub success_rate: f64,
    pub total_duration_ms: u64,
}

/// Report generator
#[derive(Debug)]
pub struct ReportGenerator {
    /// Report format
    format: ReportFormat,
}

/// Format for report generation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReportFormat {
    /// Comprehensive text report
    Comprehensive,
    /// Summary report
    Summary,
    /// JSON format
    Json,
    /// Markdown format
    Markdown,
}

impl ValidationReport {
    /// Create a new validation report
    pub fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            sections: Vec::new(),
            overall_status: ValidationReportStatus::Passed,
            summary: ValidationSummary {
                total_sections: 0,
                passed_sections: 0,
                failed_sections: 0,
                warning_sections: 0,
                error_sections: 0,
                skipped_sections: 0,
                success_rate: 0.0,
                total_duration_ms: 0,
            },
            timestamp: chrono::Utc::now(),
        }
    }

    /// Add a validation section
    pub fn add_section(&mut self, name: &str, section: ValidationSection) {
        self.sections.push(section);
        self.update_summary();
    }

    /// Update the summary statistics
    fn update_summary(&mut self) {
        let total = self.sections.len();
        let passed = self.sections.iter().filter(|s| s.status == ValidationSectionStatus::Passed).count();
        let failed = self.sections.iter().filter(|s| s.status == ValidationSectionStatus::Failed).count();
        let warning = self.sections.iter().filter(|s| s.status == ValidationSectionStatus::Warning).count();
        let error = self.sections.iter().filter(|s| s.status == ValidationSectionStatus::Error).count();
        let skipped = self.sections.iter().filter(|s| s.status == ValidationSectionStatus::Skipped).count();

        let success_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        // Determine overall status
        self.overall_status = if failed > 0 || error > 0 {
            ValidationReportStatus::Failed
        } else if warning > 0 {
            ValidationReportStatus::Warning
        } else {
            ValidationReportStatus::Passed
        };

        self.summary = ValidationSummary {
            total_sections: total,
            passed_sections: passed,
            failed_sections: failed,
            warning_sections: warning,
            error_sections: error,
            skipped_sections: skipped,
            success_rate,
            total_duration_ms: 0, // Would be calculated from actual duration
        };
    }

    /// Get overall success status
    pub fn is_successful(&self) -> bool {
        self.overall_status == ValidationReportStatus::Passed
    }

    /// Get failed sections
    pub fn get_failed_sections(&self) -> Vec<&ValidationSection> {
        self.sections.iter()
            .filter(|s| s.status == ValidationSectionStatus::Failed || s.status == ValidationSectionStatus::Error)
            .collect()
    }

    /// Get all recommendations
    pub fn get_all_recommendations(&self) -> Vec<String> {
        self.sections.iter()
            .flat_map(|s| s.recommendations.iter())
            .cloned()
            .collect()
    }
}

impl ReportGenerator {
    /// Create a new report generator
    pub fn new(format: ReportFormat) -> Result<Self> {
        Ok(Self { format })
    }

    /// Generate a validation report
    pub async fn generate_report(&self, report: ValidationReport) -> Result<ValidationReport> {
        // For now, just return the report as-is
        // In a real implementation, this would format the report according to self.format
        Ok(report)
    }

    /// Generate report in text format
    pub fn generate_text_report(&self, report: &ValidationReport) -> String {
        let mut output = String::new();
        
        // Header
        output.push_str(&format!("# {}\n\n", report.title));
        output.push_str(&format!("Generated: {}\n", report.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
        output.push_str(&format!("Overall Status: {:?}\n\n", report.overall_status));

        // Summary
        output.push_str("## Summary\n\n");
        output.push_str(&format!("- Total Sections: {}\n", report.summary.total_sections));
        output.push_str(&format!("- Passed: {} ({:.1}%)\n", 
                                report.summary.passed_sections, 
                                report.summary.success_rate));
        output.push_str(&format!("- Failed: {}\n", report.summary.failed_sections));
        output.push_str(&format!("- Warnings: {}\n", report.summary.warning_sections));
        output.push_str(&format!("- Errors: {}\n", report.summary.error_sections));
        output.push_str(&format!("- Skipped: {}\n\n", report.summary.skipped_sections));

        // Sections
        output.push_str("## Validation Results\n\n");
        for section in &report.sections {
            let status_icon = match section.status {
                ValidationSectionStatus::Passed => "✅",
                ValidationSectionStatus::Failed => "❌",
                ValidationSectionStatus::Warning => "⚠️",
                ValidationSectionStatus::Error => "🚨",
                ValidationSectionStatus::Skipped => "⏭️",
            };

            output.push_str(&format!("### {} {} - {:?}\n\n", status_icon, section.name, section.status));
            output.push_str(&format!("{}\n\n", section.details));

            if !section.metrics.is_empty() {
                output.push_str("**Metrics:**\n");
                for (key, value) in &section.metrics {
                    output.push_str(&format!("- {}: {:.2}\n", key, value));
                }
                output.push('\n');
            }

            if !section.recommendations.is_empty() {
                output.push_str("**Recommendations:**\n");
                for recommendation in &section.recommendations {
                    output.push_str(&format!("- {}\n", recommendation));
                }
                output.push('\n');
            }
        }

        // Overall Recommendations
        let all_recommendations = report.get_all_recommendations();
        if !all_recommendations.is_empty() {
            output.push_str("## Overall Recommendations\n\n");
            for recommendation in all_recommendations {
                output.push_str(&format!("- {}\n", recommendation));
            }
        }

        output
    }

    /// Generate report in JSON format
    pub fn generate_json_report(&self, report: &ValidationReport) -> Result<String> {
        serde_json::to_string_pretty(report)
            .map_err(|e| Error::serialization(format!("Failed to serialize report to JSON: {}", e)))
    }

    /// Generate report in Markdown format
    pub fn generate_markdown_report(&self, report: &ValidationReport) -> String {
        let mut output = String::new();
        
        // Header
        output.push_str(&format!("# {}\n\n", report.title));
        output.push_str(&format!("**Generated:** {}\n", report.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
        output.push_str(&format!("**Overall Status:** {:?}\n\n", report.overall_status));

        // Summary table
        output.push_str("## Summary\n\n");
        output.push_str("| Metric | Count | Percentage |\n");
        output.push_str("|--------|-------|------------|\n");
        output.push_str(&format!("| Total Sections | {} | 100.0% |\n", report.summary.total_sections));
        output.push_str(&format!("| ✅ Passed | {} | {:.1}% |\n", 
                                report.summary.passed_sections, 
                                report.summary.success_rate));
        output.push_str(&format!("| ❌ Failed | {} | {:.1}% |\n", 
                                report.summary.failed_sections,
                                (report.summary.failed_sections as f64 / report.summary.total_sections as f64) * 100.0));
        output.push_str(&format!("| ⚠️ Warnings | {} | {:.1}% |\n", 
                                report.summary.warning_sections,
                                (report.summary.warning_sections as f64 / report.summary.total_sections as f64) * 100.0));
        output.push_str(&format!("| 🚨 Errors | {} | {:.1}% |\n", 
                                report.summary.error_sections,
                                (report.summary.error_sections as f64 / report.summary.total_sections as f64) * 100.0));
        output.push_str(&format!("| ⏭️ Skipped | {} | {:.1}% |\n\n", 
                                report.summary.skipped_sections,
                                (report.summary.skipped_sections as f64 / report.summary.total_sections as f64) * 100.0));

        // Sections
        output.push_str("## Validation Results\n\n");
        for section in &report.sections {
            let status_icon = match section.status {
                ValidationSectionStatus::Passed => "✅",
                ValidationSectionStatus::Failed => "❌",
                ValidationSectionStatus::Warning => "⚠️",
                ValidationSectionStatus::Error => "🚨",
                ValidationSectionStatus::Skipped => "⏭️",
            };

            output.push_str(&format!("### {} {}\n\n", status_icon, section.name));
            output.push_str(&format!("**Status:** {:?}\n\n", section.status));
            output.push_str(&format!("{}\n\n", section.details));

            if !section.metrics.is_empty() {
                output.push_str("**Metrics:**\n\n");
                for (key, value) in &section.metrics {
                    output.push_str(&format!("- **{}:** {:.2}\n", key, value));
                }
                output.push('\n');
            }

            if !section.recommendations.is_empty() {
                output.push_str("**Recommendations:**\n\n");
                for recommendation in &section.recommendations {
                    output.push_str(&format!("- {}\n", recommendation));
                }
                output.push('\n');
            }
        }

        output
    }

    /// Generate a summary report
    pub fn generate_summary_report(&self, report: &ValidationReport) -> String {
        let mut output = String::new();
        
        output.push_str(&format!("{} - {}\n", report.title, report.overall_status.status_text()));
        output.push_str(&format!("Success Rate: {:.1}% ({}/{} sections passed)\n", 
                                report.summary.success_rate,
                                report.summary.passed_sections,
                                report.summary.total_sections));

        if report.summary.failed_sections > 0 {
            output.push_str(&format!("Failed Sections: {}\n", report.summary.failed_sections));
        }

        if report.summary.warning_sections > 0 {
            output.push_str(&format!("Warning Sections: {}\n", report.summary.warning_sections));
        }

        if report.summary.error_sections > 0 {
            output.push_str(&format!("Error Sections: {}\n", report.summary.error_sections));
        }

        output
    }
}

impl ValidationReportStatus {
    /// Get status text
    pub fn status_text(&self) -> &'static str {
        match self {
            ValidationReportStatus::Passed => "PASSED",
            ValidationReportStatus::Failed => "FAILED",
            ValidationReportStatus::Warning => "WARNING",
        }
    }
}

impl ValidationSectionStatus {
    /// Get status text
    pub fn status_text(&self) -> &'static str {
        match self {
            ValidationSectionStatus::Passed => "PASSED",
            ValidationSectionStatus::Failed => "FAILED",
            ValidationSectionStatus::Warning => "WARNING",
            ValidationSectionStatus::Error => "ERROR",
            ValidationSectionStatus::Skipped => "SKIPPED",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_report_creation() {
        let report = ValidationReport::new("Test Report");
        assert_eq!(report.title, "Test Report");
        assert_eq!(report.sections.len(), 0);
        assert_eq!(report.overall_status, ValidationReportStatus::Passed);
    }

    #[test]
    fn test_add_section_updates_summary() {
        let mut report = ValidationReport::new("Test Report");
        
        let section = ValidationSection {
            name: "Test Section".to_string(),
            status: ValidationSectionStatus::Passed,
            details: "Test details".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Test recommendation".to_string()],
            timestamp: chrono::Utc::now(),
        };
        
        report.add_section("test", section);
        
        assert_eq!(report.summary.total_sections, 1);
        assert_eq!(report.summary.passed_sections, 1);
        assert_eq!(report.summary.success_rate, 100.0);
        assert_eq!(report.overall_status, ValidationReportStatus::Passed);
    }

    #[test]
    fn test_failed_section_affects_overall_status() {
        let mut report = ValidationReport::new("Test Report");
        
        let failed_section = ValidationSection {
            name: "Failed Section".to_string(),
            status: ValidationSectionStatus::Failed,
            details: "This section failed".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Fix the failure".to_string()],
            timestamp: chrono::Utc::now(),
        };
        
        report.add_section("failed", failed_section);
        
        assert_eq!(report.summary.total_sections, 1);
        assert_eq!(report.summary.failed_sections, 1);
        assert_eq!(report.summary.success_rate, 0.0);
        assert_eq!(report.overall_status, ValidationReportStatus::Failed);
    }

    #[test]
    fn test_warning_section_affects_overall_status() {
        let mut report = ValidationReport::new("Test Report");
        
        let warning_section = ValidationSection {
            name: "Warning Section".to_string(),
            status: ValidationSectionStatus::Warning,
            details: "This section has warnings".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Address the warning".to_string()],
            timestamp: chrono::Utc::now(),
        };
        
        report.add_section("warning", warning_section);
        
        assert_eq!(report.summary.total_sections, 1);
        assert_eq!(report.summary.warning_sections, 1);
        assert_eq!(report.overall_status, ValidationReportStatus::Warning);
    }

    #[test]
    fn test_mixed_sections_status_priority() {
        let mut report = ValidationReport::new("Test Report");
        
        // Add passed section
        let passed_section = ValidationSection {
            name: "Passed Section".to_string(),
            status: ValidationSectionStatus::Passed,
            details: "All good".to_string(),
            metrics: HashMap::new(),
            recommendations: Vec::new(),
            timestamp: chrono::Utc::now(),
        };
        report.add_section("passed", passed_section);
        
        // Add warning section
        let warning_section = ValidationSection {
            name: "Warning Section".to_string(),
            status: ValidationSectionStatus::Warning,
            details: "Some warnings".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Address warnings".to_string()],
            timestamp: chrono::Utc::now(),
        };
        report.add_section("warning", warning_section);
        
        // Should be warning status (not failed)
        assert_eq!(report.overall_status, ValidationReportStatus::Warning);
        
        // Add failed section
        let failed_section = ValidationSection {
            name: "Failed Section".to_string(),
            status: ValidationSectionStatus::Failed,
            details: "Failed".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Fix failure".to_string()],
            timestamp: chrono::Utc::now(),
        };
        report.add_section("failed", failed_section);
        
        // Should now be failed status (failure takes priority)
        assert_eq!(report.overall_status, ValidationReportStatus::Failed);
        assert_eq!(report.summary.success_rate, 33.333333333333336); // 1 out of 3 passed
    }

    #[test]
    fn test_get_failed_sections() {
        let mut report = ValidationReport::new("Test Report");
        
        let passed_section = ValidationSection {
            name: "Passed Section".to_string(),
            status: ValidationSectionStatus::Passed,
            details: "All good".to_string(),
            metrics: HashMap::new(),
            recommendations: Vec::new(),
            timestamp: chrono::Utc::now(),
        };
        report.add_section("passed", passed_section);
        
        let failed_section = ValidationSection {
            name: "Failed Section".to_string(),
            status: ValidationSectionStatus::Failed,
            details: "Failed".to_string(),
            metrics: HashMap::new(),
            recommendations: Vec::new(),
            timestamp: chrono::Utc::now(),
        };
        report.add_section("failed", failed_section);
        
        let error_section = ValidationSection {
            name: "Error Section".to_string(),
            status: ValidationSectionStatus::Error,
            details: "Error".to_string(),
            metrics: HashMap::new(),
            recommendations: Vec::new(),
            timestamp: chrono::Utc::now(),
        };
        report.add_section("error", error_section);
        
        let failed_sections = report.get_failed_sections();
        assert_eq!(failed_sections.len(), 2); // Failed + Error sections
        assert!(failed_sections.iter().any(|s| s.name == "Failed Section"));
        assert!(failed_sections.iter().any(|s| s.name == "Error Section"));
    }

    #[test]
    fn test_get_all_recommendations() {
        let mut report = ValidationReport::new("Test Report");
        
        let section1 = ValidationSection {
            name: "Section 1".to_string(),
            status: ValidationSectionStatus::Passed,
            details: "Details".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Recommendation 1".to_string(), "Recommendation 2".to_string()],
            timestamp: chrono::Utc::now(),
        };
        report.add_section("section1", section1);
        
        let section2 = ValidationSection {
            name: "Section 2".to_string(),
            status: ValidationSectionStatus::Warning,
            details: "Details".to_string(),
            metrics: HashMap::new(),
            recommendations: vec!["Recommendation 3".to_string()],
            timestamp: chrono::Utc::now(),
        };
        report.add_section("section2", section2);
        
        let all_recommendations = report.get_all_recommendations();
        assert_eq!(all_recommendations.len(), 3);
        assert!(all_recommendations.contains(&"Recommendation 1".to_string()));
        assert!(all_recommendations.contains(&"Recommendation 2".to_string()));
        assert!(all_recommendations.contains(&"Recommendation 3".to_string()));
    }

    #[test]
    fn test_report_generator_creation() {
        let generator = ReportGenerator::new(ReportFormat::Comprehensive);
        assert!(generator.is_ok());
    }

    #[tokio::test]
    async fn test_generate_report() {
        let generator = ReportGenerator::new(ReportFormat::Comprehensive).unwrap();
        let report = ValidationReport::new("Test Report");
        
        let result = generator.generate_report(report.clone()).await;
        assert!(result.is_ok());
        
        let generated_report = result.unwrap();
        assert_eq!(generated_report.title, "Test Report");
    }

    #[test]
    fn test_status_text() {
        assert_eq!(ValidationReportStatus::Passed.status_text(), "PASSED");
        assert_eq!(ValidationReportStatus::Failed.status_text(), "FAILED");
        assert_eq!(ValidationReportStatus::Warning.status_text(), "WARNING");
        
        assert_eq!(ValidationSectionStatus::Passed.status_text(), "PASSED");
        assert_eq!(ValidationSectionStatus::Failed.status_text(), "FAILED");
        assert_eq!(ValidationSectionStatus::Warning.status_text(), "WARNING");
        assert_eq!(ValidationSectionStatus::Error.status_text(), "ERROR");
        assert_eq!(ValidationSectionStatus::Skipped.status_text(), "SKIPPED");
    }
}