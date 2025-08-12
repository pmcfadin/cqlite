//! Main validation orchestrator for Cassandra 5+ SSTable format validation

use crate::analyzer::FormatAnalyzer;
use crate::checker::FormatChecker;
use crate::detector::FormatDeviationDetector;
use crate::{FormatValidator, SSTableFileType, ValidationError, ValidationResult};
use std::path::Path;
use std::time::Instant;

/// Comprehensive SSTable format validator
#[derive(Debug)]
pub struct SSTableValidator {
    pub analyzer: FormatAnalyzer,
    pub checker: FormatChecker,
    pub detector: FormatDeviationDetector,
    pub validate_checksums: bool,
    pub detect_deviations: bool,
    pub verbose_analysis: bool,
}

impl Default for SSTableValidator {
    fn default() -> Self {
        Self {
            analyzer: FormatAnalyzer::new(),
            checker: FormatChecker::new(),
            detector: FormatDeviationDetector::new(),
            validate_checksums: true,
            detect_deviations: true,
            verbose_analysis: false,
        }
    }
}

impl SSTableValidator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_checksum_validation(mut self, validate: bool) -> Self {
        self.validate_checksums = validate;
        self.checker = self.checker.with_checksum_validation(validate);
        self
    }

    pub fn with_deviation_detection(mut self, detect: bool) -> Self {
        self.detect_deviations = detect;
        self
    }

    pub fn with_verbose_analysis(mut self, verbose: bool) -> Self {
        self.verbose_analysis = verbose;
        self.analyzer = self.analyzer.with_verbose(verbose);
        self
    }

    pub fn with_strict_mode(mut self, strict: bool) -> Self {
        self.checker = self.checker.with_strict_mode(strict);
        self
    }

    /// Perform comprehensive validation of an SSTable file
    pub fn validate_comprehensive(&self, path: &Path) -> Result<ValidationResult, ValidationError> {
        let start_time = Instant::now();

        // Start with basic format checking
        let mut result = self.checker.validate(path)?;

        // Add detailed analysis if requested
        if self.verbose_analysis {
            let analysis_result = self.analyzer.analyze_file(path)?;

            // Merge analysis results
            result.warnings.extend(analysis_result.warnings);
            result.errors.extend(analysis_result.errors);

            // Use more detailed statistics if available
            if analysis_result.statistics.file_size > 0 {
                result.statistics = analysis_result.statistics;
            }
        }

        // Detect format deviations if requested
        if self.detect_deviations {
            match self.detector.detect_file_deviations(path) {
                Ok(deviations) => {
                    for deviation in deviations {
                        result
                            .warnings
                            .push(format!("Format deviation: {}", deviation));
                    }
                }
                Err(e) => {
                    result.errors.push(e);
                }
            }
        }

        // Update validation status
        result.is_valid = result.errors.is_empty();

        // Record timing
        result.statistics.validation_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(result)
    }

    /// Validate multiple files in batch
    pub fn validate_batch(
        &self,
        paths: &[&Path],
    ) -> Vec<Result<ValidationResult, ValidationError>> {
        paths
            .iter()
            .map(|path| self.validate_comprehensive(path))
            .collect()
    }

    /// Quick validation with minimal checks
    pub fn validate_quick(&self, path: &Path) -> Result<bool, ValidationError> {
        let result = self.checker.validate(path)?;
        Ok(result.is_valid)
    }

    /// Validate file contents from memory
    pub fn validate_memory(
        &self,
        data: &[u8],
        file_type: SSTableFileType,
    ) -> Result<ValidationResult, ValidationError> {
        let start_time = Instant::now();

        let mut result = self.checker.validate_bytes(data, file_type)?;

        // Add deviation detection for memory validation
        if self.detect_deviations {
            let deviations = self.detector.detect_format_deviations(data);
            for deviation in deviations {
                result
                    .warnings
                    .push(format!("Format deviation: {}", deviation));
            }
        }

        result.is_valid = result.errors.is_empty();
        result.statistics.validation_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(result)
    }

    /// Generate a detailed validation report
    pub fn generate_report(&self, results: &[ValidationResult]) -> String {
        let mut report = String::new();

        report.push_str("=== SSTable Format Validation Report ===\n\n");

        let total_files = results.len();
        let valid_files = results.iter().filter(|r| r.is_valid).count();
        let invalid_files = total_files - valid_files;

        report.push_str(&format!("Total files validated: {}\n", total_files));
        report.push_str(&format!("Valid files: {}\n", valid_files));
        report.push_str(&format!("Invalid files: {}\n", invalid_files));

        if total_files > 0 {
            let success_rate = (valid_files as f64 / total_files as f64) * 100.0;
            report.push_str(&format!("Success rate: {:.1}%\n", success_rate));
        }

        report.push_str("\n=== File Details ===\n");

        for result in results {
            report.push_str(&format!("\nFile: {}\n", result.file_path));
            report.push_str(&format!(
                "Status: {}\n",
                if result.is_valid { "VALID" } else { "INVALID" }
            ));

            if let Some(ref version) = result.format_version {
                report.push_str(&format!("Format: {}\n", version));
            }

            report.push_str(&format!("Size: {} bytes\n", result.statistics.file_size));
            report.push_str(&format!(
                "Validation time: {} ms\n",
                result.statistics.validation_time_ms
            ));

            if !result.errors.is_empty() {
                report.push_str("Errors:\n");
                for error in &result.errors {
                    report.push_str(&format!("  - {}\n", error));
                }
            }

            if !result.warnings.is_empty() {
                report.push_str("Warnings:\n");
                for warning in &result.warnings {
                    report.push_str(&format!("  - {}\n", warning));
                }
            }
        }

        report.push_str("\n=== Summary ===\n");

        if invalid_files == 0 {
            report.push_str("✅ All files passed validation!\n");
        } else {
            report.push_str(&format!("❌ {} files failed validation\n", invalid_files));
        }

        report
    }
}

impl FormatValidator for SSTableValidator {
    fn validate(&self, file_path: &Path) -> Result<ValidationResult, ValidationError> {
        self.validate_comprehensive(file_path)
    }

    fn validate_bytes(
        &self,
        data: &[u8],
        file_type: SSTableFileType,
    ) -> Result<ValidationResult, ValidationError> {
        self.validate_memory(data, file_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_validator_creation() {
        let validator = SSTableValidator::new();
        assert!(validator.validate_checksums);
        assert!(validator.detect_deviations);
        assert!(!validator.verbose_analysis);
    }

    #[test]
    fn test_validator_configuration() {
        let validator = SSTableValidator::new()
            .with_checksum_validation(false)
            .with_deviation_detection(false)
            .with_verbose_analysis(true)
            .with_strict_mode(false);

        assert!(!validator.validate_checksums);
        assert!(!validator.detect_deviations);
        assert!(validator.verbose_analysis);
    }

    #[test]
    fn test_memory_validation() {
        let validator = SSTableValidator::new();

        // Test with valid BigFormat data
        let mut data = vec![0x6F, 0x61, 0x00, 0x00]; // Magic
        data.extend_from_slice(&[0x00, 0x01]); // Version
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Checksum placeholder
        data.extend_from_slice(&b"test data");

        let result = validator.validate_memory(&data, SSTableFileType::Data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_report_generation() {
        let validator = SSTableValidator::new();

        let results = vec![
            ValidationResult {
                file_path: "test1.db".to_string(),
                format_version: Some("BigFormat 'oa'".to_string()),
                is_valid: true,
                errors: vec![],
                warnings: vec!["Minor issue".to_string()],
                statistics: Default::default(),
            },
            ValidationResult {
                file_path: "test2.db".to_string(),
                format_version: None,
                is_valid: false,
                errors: vec![ValidationError::InvalidMagic {
                    expected: 0x6F610000,
                    found: 0xFFFFFFFF,
                }],
                warnings: vec![],
                statistics: Default::default(),
            },
        ];

        let report = validator.generate_report(&results);
        assert!(report.contains("Total files validated: 2"));
        assert!(report.contains("Valid files: 1"));
        assert!(report.contains("Invalid files: 1"));
        assert!(report.contains("Success rate: 50.0%"));
    }
}
