//! Validation report generation and analysis
//!
//! This module provides comprehensive reporting capabilities for
//! validation results, performance metrics, and analysis summaries.

use cqlite_core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Report format options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReportFormat {
    Json,
    Markdown,
    Console,
    Html,
}

/// Validation report summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub test_suite: String,
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub performance_metrics: HashMap<String, f64>,
    pub recommendations: Vec<String>,
}

/// Report section for detailed analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSection {
    pub title: String,
    pub content: String,
    pub status: SectionStatus,
    pub metrics: HashMap<String, f64>,
}

/// Status of a report section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SectionStatus {
    Pass,
    Fail,
    Warning,
    Info,
}

/// Report generator and formatter
pub struct ReportGenerator {
    format: ReportFormat,
    sections: Vec<ReportSection>,
}

impl ReportGenerator {
    /// Create a new report generator
    pub fn new(format: ReportFormat) -> Self {
        Self {
            format,
            sections: Vec::new(),
        }
    }

    /// Add a section to the report
    pub fn add_section(&mut self, section: ReportSection) {
        self.sections.push(section);
    }

    /// Generate the complete report
    pub fn generate(&self, report_data: &ValidationReport) -> Result<String> {
        match self.format {
            ReportFormat::Json => self.generate_json_report(report_data),
            ReportFormat::Markdown => self.generate_markdown_report(report_data),
            ReportFormat::Console => self.generate_console_report(report_data),
            ReportFormat::Html => self.generate_html_report(report_data),
        }
    }

    /// Generate JSON format report
    fn generate_json_report(&self, report_data: &ValidationReport) -> Result<String> {
        serde_json::to_string_pretty(report_data)
            .map_err(|e| Error::Parse(format!("JSON serialization failed: {}", e)))
    }

    /// Generate Markdown format report
    fn generate_markdown_report(&self, report_data: &ValidationReport) -> Result<String> {
        let mut output = String::new();

        output.push_str("# Validation Report\n\n");
        output.push_str(&format!(
            "**Date**: {}\n",
            report_data.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        ));
        output.push_str(&format!("**Test Suite**: {}\n\n", report_data.test_suite));

        output.push_str("## Summary\n\n");
        output.push_str(&format!("- Total Tests: {}\n", report_data.total_tests));
        output.push_str(&format!("- Passed: {}\n", report_data.passed_tests));
        output.push_str(&format!("- Failed: {}\n", report_data.failed_tests));

        if !report_data.errors.is_empty() {
            output.push_str("\n## Errors\n\n");
            for error in &report_data.errors {
                output.push_str(&format!("- {}\n", error));
            }
        }

        if !report_data.warnings.is_empty() {
            output.push_str("\n## Warnings\n\n");
            for warning in &report_data.warnings {
                output.push_str(&format!("- {}\n", warning));
            }
        }

        Ok(output)
    }

    /// Generate console format report
    fn generate_console_report(&self, report_data: &ValidationReport) -> Result<String> {
        let mut output = String::new();

        output.push_str(&format!(
            "🔍 Validation Report - {}\n",
            report_data.test_suite
        ));
        output.push_str(&format!(
            "📅 Generated: {}\n\n",
            report_data.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        ));

        let pass_rate = (report_data.passed_tests as f64 / report_data.total_tests as f64) * 100.0;
        output.push_str(&format!(
            "📊 Results: {}/{} tests passed ({:.1}%)\n",
            report_data.passed_tests, report_data.total_tests, pass_rate
        ));

        if report_data.failed_tests > 0 {
            output.push_str(&format!("❌ {} tests failed\n", report_data.failed_tests));
        }

        Ok(output)
    }

    /// Generate HTML format report
    fn generate_html_report(&self, report_data: &ValidationReport) -> Result<String> {
        let mut output = String::new();

        output.push_str("<!DOCTYPE html>\n<html>\n<head>\n");
        output.push_str("<title>CQLite Validation Report</title>\n");
        output.push_str("<meta charset=\"utf-8\">\n");
        output.push_str("</head>\n<body>\n");

        output.push_str(&format!(
            "<h1>Validation Report: {}</h1>\n",
            report_data.test_suite
        ));
        output.push_str(&format!(
            "<p><strong>Date:</strong> {}</p>\n",
            report_data.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        ));

        output.push_str("<h2>Summary</h2>\n");
        output.push_str(&format!(
            "<p>Total Tests: {}</p>\n",
            report_data.total_tests
        ));
        output.push_str(&format!("<p>Passed: {}</p>\n", report_data.passed_tests));
        output.push_str(&format!("<p>Failed: {}</p>\n", report_data.failed_tests));

        output.push_str("</body>\n</html>\n");

        Ok(output)
    }
}

impl ValidationReport {
    /// Create a new validation report
    pub fn new(test_suite: String) -> Self {
        Self {
            timestamp: chrono::Utc::now(),
            test_suite,
            total_tests: 0,
            passed_tests: 0,
            failed_tests: 0,
            warnings: Vec::new(),
            errors: Vec::new(),
            performance_metrics: HashMap::new(),
            recommendations: Vec::new(),
        }
    }

    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_tests == 0 {
            0.0
        } else {
            (self.passed_tests as f64 / self.total_tests as f64) * 100.0
        }
    }

    /// Check if all tests passed
    pub fn is_successful(&self) -> bool {
        self.failed_tests == 0 && self.errors.is_empty()
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidationReport: {} ({}/{} passed)",
            self.test_suite, self.passed_tests, self.total_tests
        )
    }
}

impl Default for ReportGenerator {
    fn default() -> Self {
        Self::new(ReportFormat::Console)
    }
}
