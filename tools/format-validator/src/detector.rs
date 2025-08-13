//! Format deviation detection for Cassandra 5+ SSTable validation

use crate::{DeviationDetector, SSTableFileType, ValidationError};
use std::collections::HashMap;
use std::path::Path;

/// Format deviation detector for identifying non-compliance
#[derive(Debug)]
pub struct FormatDeviationDetector {
    pub tolerance_level: ToleranceLevel,
    pub reference_patterns: HashMap<SSTableFileType, Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToleranceLevel {
    Zero,     // No deviations allowed
    Minimal,  // Minor deviations allowed
    Standard, // Standard tolerance
    Relaxed,  // High tolerance
}

impl Default for FormatDeviationDetector {
    fn default() -> Self {
        Self {
            tolerance_level: ToleranceLevel::Standard,
            reference_patterns: HashMap::new(),
        }
    }
}

impl FormatDeviationDetector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tolerance(mut self, level: ToleranceLevel) -> Self {
        self.tolerance_level = level;
        self
    }

    pub fn add_reference_pattern(&mut self, file_type: SSTableFileType, pattern: Vec<u8>) {
        self.reference_patterns.insert(file_type, pattern);
    }

    pub fn detect_format_deviations(&self, data: &[u8]) -> Vec<String> {
        let mut deviations = Vec::new();

        // Check for known magic numbers
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            let is_known_magic = magic == crate::format_constants::BIG_FORMAT_OA_MAGIC
                || magic == crate::format_constants::BTI_FORMAT_DA_MAGIC
                || magic == crate::format_constants::STATISTICS_MAGIC;

            if !is_known_magic {
                deviations.push(format!("Unknown magic number: {magic:#x}"));
            }
        }

        // Check for structural anomalies
        if self.has_unusual_byte_patterns(data) {
            deviations.push("Unusual byte patterns detected".to_string());
        }

        self.filter_by_tolerance(deviations)
    }

    /// Detect format deviations in a file
    pub fn detect_file_deviations(&self, path: &Path) -> Result<Vec<String>, ValidationError> {
        let file_type = SSTableFileType::from_path(path);
        let data = crate::utils::read_file_safe(path, 100 * 1024 * 1024)?; // 100MB limit

        let mut deviations = Vec::new();

        // Check against known format patterns
        match file_type {
            SSTableFileType::Data => {
                deviations.extend(self.detect_data_deviations(&data)?);
            }
            SSTableFileType::Statistics => {
                deviations.extend(self.detect_statistics_deviations(&data)?);
            }
            SSTableFileType::Partitions | SSTableFileType::Rows => {
                deviations.extend(self.detect_bti_deviations(&data)?);
            }
            SSTableFileType::Index => {
                deviations.extend(self.detect_index_deviations(&data)?);
            }
            _ => {
                deviations.push(format!("Unknown file type pattern: {file_type:?}"));
            }
        }

        // Apply tolerance filtering
        let filtered_deviations = self.filter_by_tolerance(deviations);

        Ok(filtered_deviations)
    }

    fn detect_data_deviations(&self, data: &[u8]) -> Result<Vec<String>, ValidationError> {
        let mut deviations = Vec::new();

        // Check magic number compliance
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic != crate::format_constants::BIG_FORMAT_OA_MAGIC {
                deviations.push(format!(
                    "Unexpected magic number: {:#x} (expected {:#x})",
                    magic,
                    crate::format_constants::BIG_FORMAT_OA_MAGIC
                ));
            }
        }

        // Check version compliance
        if data.len() >= 6 {
            let version = u16::from_be_bytes([data[4], data[5]]);
            if version != crate::format_constants::SUPPORTED_VERSION {
                deviations.push(format!(
                    "Unsupported version: {:#x} (expected {:#x})",
                    version,
                    crate::format_constants::SUPPORTED_VERSION
                ));
            }
        }

        // Check for unusual patterns
        if self.has_unusual_byte_patterns(data) {
            deviations.push("Unusual byte patterns detected".to_string());
        }

        // Check alignment
        if data.len() % 8 != 0 {
            deviations.push("File size not aligned to 8-byte boundary".to_string());
        }

        Ok(deviations)
    }

    fn detect_statistics_deviations(&self, data: &[u8]) -> Result<Vec<String>, ValidationError> {
        let mut deviations = Vec::new();

        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic != crate::format_constants::STATISTICS_MAGIC {
                deviations.push(format!("Invalid Statistics.db magic: {magic:#x}"));
            }
        }

        // Statistics files should have predictable structure
        if data.len() < 16 {
            deviations.push("Statistics file too small".to_string());
        }

        if data.len() > 10 * 1024 * 1024 {
            deviations.push("Statistics file unusually large".to_string());
        }

        Ok(deviations)
    }

    fn detect_bti_deviations(&self, data: &[u8]) -> Result<Vec<String>, ValidationError> {
        let mut deviations = Vec::new();

        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic != crate::format_constants::BTI_FORMAT_DA_MAGIC {
                deviations.push(format!("Invalid BTI magic: {magic:#x}"));
            }
        }

        // Check BTI block size
        if data.len() >= 8 {
            let block_size = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
            if block_size == 0 {
                deviations.push("BTI block size is zero".to_string());
            } else if block_size > 1024 * 1024 {
                deviations.push(format!("BTI block size too large: {block_size}"));
            } else if !block_size.is_power_of_two()
                && block_size != crate::format_constants::BTI_DEFAULT_BLOCK_SIZE
            {
                deviations.push(format!("Non-standard BTI block size: {block_size}"));
            }
        }

        Ok(deviations)
    }

    fn detect_index_deviations(&self, data: &[u8]) -> Result<Vec<String>, ValidationError> {
        let mut deviations = Vec::new();

        if data.is_empty() {
            deviations.push("Empty index file".to_string());
        }

        // Index files should have some minimum structure
        if data.len() < 8 {
            deviations.push("Index file suspiciously small".to_string());
        }

        Ok(deviations)
    }

    fn has_unusual_byte_patterns(&self, data: &[u8]) -> bool {
        if data.len() < 100 {
            return false;
        }

        // Check for excessive repetition
        let mut byte_counts = [0u32; 256];
        for &byte in &data[..100.min(data.len())] {
            byte_counts[byte as usize] += 1;
        }

        // If any single byte appears more than 80% of the time in the first 100 bytes
        for count in &byte_counts {
            if *count > 80 {
                return true;
            }
        }

        false
    }

    fn filter_by_tolerance(&self, deviations: Vec<String>) -> Vec<String> {
        match self.tolerance_level {
            ToleranceLevel::Zero => deviations,
            ToleranceLevel::Minimal => deviations
                .into_iter()
                .filter(|d| !d.contains("alignment") && !d.contains("unusually"))
                .collect(),
            ToleranceLevel::Standard => deviations
                .into_iter()
                .filter(|d| d.contains("magic") || d.contains("version") || d.contains("empty"))
                .collect(),
            ToleranceLevel::Relaxed => deviations
                .into_iter()
                .filter(|d| d.contains("magic") || d.contains("empty"))
                .collect(),
        }
    }
}

impl DeviationDetector for FormatDeviationDetector {
    fn compare_with_reference(
        &self,
        file1: &Path,
        file2: &Path,
    ) -> Result<Vec<String>, ValidationError> {
        let data1 = crate::utils::read_file_safe(file1, 50 * 1024 * 1024)?;
        let data2 = crate::utils::read_file_safe(file2, 50 * 1024 * 1024)?;

        let mut differences = Vec::new();

        if data1.len() != data2.len() {
            differences.push(format!(
                "File sizes differ: {} vs {}",
                data1.len(),
                data2.len()
            ));
        }

        let min_len = data1.len().min(data2.len());
        let mut diff_count = 0;

        for i in 0..min_len {
            if data1[i] != data2[i] {
                diff_count += 1;
                if diff_count <= 10 {
                    // Report first 10 differences
                    differences.push(format!(
                        "Byte difference at offset {}: {:#x} vs {:#x}",
                        i, data1[i], data2[i]
                    ));
                }
            }
        }

        if diff_count > 10 {
            differences.push(format!("Total byte differences: {diff_count}"));
        }

        Ok(differences)
    }

    fn detect_format_deviations(&self, data: &[u8]) -> Vec<String> {
        let mut deviations = Vec::new();

        // Check for known magic numbers
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            let is_known_magic = magic == crate::format_constants::BIG_FORMAT_OA_MAGIC
                || magic == crate::format_constants::BTI_FORMAT_DA_MAGIC
                || magic == crate::format_constants::STATISTICS_MAGIC;

            if !is_known_magic {
                deviations.push(format!("Unknown magic number: {magic:#x}"));
            }
        }

        // Check for structural anomalies
        if self.has_unusual_byte_patterns(data) {
            deviations.push("Unusual byte patterns detected".to_string());
        }

        self.filter_by_tolerance(deviations)
    }
}
