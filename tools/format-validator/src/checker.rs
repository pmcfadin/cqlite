//! Format compliance checker for Cassandra 5+ SSTable validation

use crate::{FormatValidator, SSTableFileType, ValidationError, ValidationResult};
use std::path::Path;

/// Comprehensive format compliance checker
#[derive(Debug)]
pub struct FormatChecker {
    pub strict_mode: bool,
    pub check_checksums: bool,
    pub validate_structure: bool,
}

impl Default for FormatChecker {
    fn default() -> Self {
        Self {
            strict_mode: true,
            check_checksums: true,
            validate_structure: true,
        }
    }
}

impl FormatChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_strict_mode(mut self, strict: bool) -> Self {
        self.strict_mode = strict;
        self
    }

    pub fn with_checksum_validation(mut self, validate: bool) -> Self {
        self.check_checksums = validate;
        self
    }

    pub fn with_structure_validation(mut self, validate: bool) -> Self {
        self.validate_structure = validate;
        self
    }

    /// Check file format compliance
    pub fn check_compliance(&self, path: &Path) -> Result<ValidationResult, ValidationError> {
        let file_type = SSTableFileType::from_path(path);
        let data = crate::utils::read_file_safe(path, 500 * 1024 * 1024)?; // 500MB limit

        let mut result = ValidationResult {
            file_path: path.to_string_lossy().to_string(),
            format_version: None,
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            statistics: Default::default(),
        };

        result.statistics.file_size = data.len() as u64;

        // Perform compliance checks based on file type
        match file_type {
            SSTableFileType::Data => self.check_data_compliance(&data, &mut result)?,
            SSTableFileType::Index => self.check_index_compliance(&data, &mut result)?,
            SSTableFileType::Statistics => self.check_statistics_compliance(&data, &mut result)?,
            SSTableFileType::Partitions => self.check_partitions_compliance(&data, &mut result)?,
            SSTableFileType::Rows => self.check_rows_compliance(&data, &mut result)?,
            SSTableFileType::Filter => self.check_filter_compliance(&data, &mut result)?,
            _ => {
                if self.strict_mode {
                    result.errors.push(ValidationError::StructureViolation {
                        reason: format!(
                            "Unsupported file type for strict compliance: {:?}",
                            file_type
                        ),
                    });
                } else {
                    result.warnings.push(format!(
                        "Skipping compliance check for file type: {:?}",
                        file_type
                    ));
                }
            }
        }

        result.is_valid = result.errors.is_empty();
        Ok(result)
    }

    fn check_data_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Check minimum file size
        if data.len() < 8 {
            result.errors.push(ValidationError::FileTruncated {
                expected: 8,
                found: data.len(),
            });
            return Ok(());
        }

        // Verify magic number
        if let Err(e) =
            crate::utils::verify_magic(data, crate::format_constants::BIG_FORMAT_OA_MAGIC)
        {
            if self.strict_mode {
                result.errors.push(e);
            } else {
                result
                    .warnings
                    .push(format!("Magic number verification failed: {}", e));
            }
        } else {
            result.format_version = Some("BigFormat 'oa'".to_string());
        }

        // Check version field
        if data.len() >= 6 {
            let version = u16::from_be_bytes([data[4], data[5]]);
            if version != crate::format_constants::SUPPORTED_VERSION {
                if self.strict_mode {
                    result.errors.push(ValidationError::UnsupportedVersion {
                        version: format!("0x{:04x}", version),
                    });
                } else {
                    result
                        .warnings
                        .push(format!("Unsupported version: 0x{:04x}", version));
                }
            }
        }

        // Checksum validation
        if self.check_checksums && data.len() >= 12 {
            let stored_checksum = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
            let calculated_checksum = crate::utils::calculate_crc32(&data[10..]);

            if stored_checksum != calculated_checksum {
                result.errors.push(ValidationError::ChecksumMismatch {
                    expected: stored_checksum,
                    calculated: calculated_checksum,
                });
            }
        }

        Ok(())
    }

    fn check_index_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        if data.is_empty() {
            result.errors.push(ValidationError::StructureViolation {
                reason: "Empty index file".to_string(),
            });
        }

        // Basic structure validation
        if self.validate_structure && data.len() < 16 {
            result
                .warnings
                .push("Index file appears to be very small".to_string());
        }

        Ok(())
    }

    fn check_statistics_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Check Statistics.db format
        if data.len() >= 4 {
            if let Err(e) =
                crate::utils::verify_magic(data, crate::format_constants::STATISTICS_MAGIC)
            {
                if self.strict_mode {
                    result.errors.push(e);
                } else {
                    result
                        .warnings
                        .push("Statistics magic number not found".to_string());
                }
            } else {
                result.format_version = Some("Statistics.db".to_string());
            }
        }

        Ok(())
    }

    fn check_partitions_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Check BTI Partitions.db format
        if data.len() >= 4 {
            if let Err(e) =
                crate::utils::verify_magic(data, crate::format_constants::BTI_FORMAT_DA_MAGIC)
            {
                if self.strict_mode {
                    result.errors.push(e);
                } else {
                    result
                        .warnings
                        .push("BTI 'da' magic number not found".to_string());
                }
            } else {
                result.format_version = Some("BTI 'da'".to_string());
            }
        }

        // BTI specific structure checks
        if self.validate_structure && data.len() >= 8 {
            let block_size = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
            if block_size == 0 || block_size > 1024 * 1024 {
                result
                    .warnings
                    .push(format!("Unusual BTI block size: {}", block_size));
            }
        }

        Ok(())
    }

    fn check_rows_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Similar to partitions compliance
        if data.len() >= 4 {
            if let Err(e) =
                crate::utils::verify_magic(data, crate::format_constants::BTI_FORMAT_DA_MAGIC)
            {
                if self.strict_mode {
                    result.errors.push(e);
                } else {
                    result
                        .warnings
                        .push("BTI 'da' magic number not found in Rows.db".to_string());
                }
            } else {
                result.format_version = Some("BTI 'da'".to_string());
            }
        }

        Ok(())
    }

    fn check_filter_compliance(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Basic filter file validation
        if data.is_empty() {
            result.errors.push(ValidationError::StructureViolation {
                reason: "Empty filter file".to_string(),
            });
        }

        // Filter files typically have specific size patterns
        if self.validate_structure && data.len() % 8 != 0 {
            result
                .warnings
                .push("Filter file size is not aligned to 8-byte boundary".to_string());
        }

        Ok(())
    }
}

impl FormatValidator for FormatChecker {
    fn validate(&self, file_path: &Path) -> Result<ValidationResult, ValidationError> {
        self.check_compliance(file_path)
    }

    fn validate_bytes(
        &self,
        data: &[u8],
        file_type: SSTableFileType,
    ) -> Result<ValidationResult, ValidationError> {
        let mut result = ValidationResult {
            file_path: "memory".to_string(),
            format_version: None,
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            statistics: Default::default(),
        };

        result.statistics.file_size = data.len() as u64;

        match file_type {
            SSTableFileType::Data => self.check_data_compliance(data, &mut result)?,
            SSTableFileType::Statistics => self.check_statistics_compliance(data, &mut result)?,
            SSTableFileType::Partitions | SSTableFileType::Rows => {
                self.check_partitions_compliance(data, &mut result)?
            }
            _ => {
                result.warnings.push(format!(
                    "Validation not implemented for file type: {:?}",
                    file_type
                ));
            }
        }

        result.is_valid = result.errors.is_empty();
        Ok(result)
    }
}
