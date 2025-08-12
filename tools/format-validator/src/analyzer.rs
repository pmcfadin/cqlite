//! Format analysis capabilities for Cassandra 5+ SSTable validation

use crate::{HexAnalyzer, SSTableFileType, ValidationError, ValidationResult};
use std::path::Path;

/// SSTable format analyzer for deep inspection
#[derive(Debug, Default)]
pub struct FormatAnalyzer {
    pub verbose: bool,
    pub max_depth: usize,
}

impl FormatAnalyzer {
    pub fn new() -> Self {
        Self {
            verbose: false,
            max_depth: 1000,
        }
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Analyze SSTable file structure and format compliance
    pub fn analyze_file(&self, path: &Path) -> Result<ValidationResult, ValidationError> {
        let file_type = SSTableFileType::from_path(path);
        let data = crate::utils::read_file_safe(path, 100 * 1024 * 1024)?; // 100MB limit

        let mut result = ValidationResult {
            file_path: path.to_string_lossy().to_string(),
            format_version: None,
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            statistics: Default::default(),
        };

        result.statistics.file_size = data.len() as u64;

        // Perform basic format analysis based on file type
        match file_type {
            SSTableFileType::Data => self.analyze_data_file(&data, &mut result)?,
            SSTableFileType::Index => self.analyze_index_file(&data, &mut result)?,
            SSTableFileType::Statistics => self.analyze_statistics_file(&data, &mut result)?,
            SSTableFileType::Partitions => self.analyze_partitions_file(&data, &mut result)?,
            SSTableFileType::Rows => self.analyze_rows_file(&data, &mut result)?,
            _ => {
                result.warnings.push(format!(
                    "Unsupported file type for analysis: {:?}",
                    file_type
                ));
            }
        }

        result.is_valid = result.errors.is_empty();
        Ok(result)
    }

    fn analyze_data_file(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Check for BigFormat magic number
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic == crate::format_constants::BIG_FORMAT_OA_MAGIC {
                result.format_version = Some("BigFormat 'oa'".to_string());
            } else {
                result
                    .warnings
                    .push(format!("Unexpected magic number: {:#x}", magic));
            }
        }
        Ok(())
    }

    fn analyze_index_file(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Basic index file validation
        if data.is_empty() {
            result.errors.push(ValidationError::StructureViolation {
                reason: "Empty index file".to_string(),
            });
        }
        Ok(())
    }

    fn analyze_statistics_file(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // Check Statistics.db magic
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic == crate::format_constants::STATISTICS_MAGIC {
                result.format_version = Some("Statistics.db".to_string());
            }
        }
        Ok(())
    }

    fn analyze_partitions_file(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // BTI Partitions.db analysis
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic == crate::format_constants::BTI_FORMAT_DA_MAGIC {
                result.format_version = Some("BTI 'da'".to_string());
            }
        }
        Ok(())
    }

    fn analyze_rows_file(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> Result<(), ValidationError> {
        // BTI Rows.db analysis
        if data.len() >= 4 {
            let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            if magic == crate::format_constants::BTI_FORMAT_DA_MAGIC {
                result.format_version = Some("BTI 'da'".to_string());
            }
        }
        Ok(())
    }
}

impl HexAnalyzer for FormatAnalyzer {
    fn analyze_hex(&self, data: &[u8], offset: usize, length: usize) -> String {
        crate::utils::format_hex_dump(data, offset, length)
    }

    fn find_magic_numbers(&self, data: &[u8]) -> Vec<(usize, u32)> {
        let mut magic_numbers = Vec::new();

        for i in 0..data.len().saturating_sub(3) {
            let magic = u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);

            // Check for known magic numbers
            if magic == crate::format_constants::BIG_FORMAT_OA_MAGIC
                || magic == crate::format_constants::BTI_FORMAT_DA_MAGIC
                || magic == crate::format_constants::STATISTICS_MAGIC
            {
                magic_numbers.push((i, magic));
            }
        }

        magic_numbers
    }

    fn analyze_vints(&self, data: &[u8]) -> Vec<(usize, i64, usize)> {
        let mut vints = Vec::new();
        let mut i = 0;

        while i < data.len() {
            if let Some((value, size)) = self.try_decode_vint(&data[i..]) {
                vints.push((i, value, size));
                i += size;
            } else {
                i += 1;
            }
        }

        vints
    }
}

impl FormatAnalyzer {
    fn try_decode_vint(&self, data: &[u8]) -> Option<(i64, usize)> {
        if data.is_empty() {
            return None;
        }

        let first_byte = data[0];
        let size = if first_byte & 0x80 == 0 {
            1
        } else if first_byte & 0x40 == 0 {
            2
        } else if first_byte & 0x20 == 0 {
            3
        } else if first_byte & 0x10 == 0 {
            4
        } else if first_byte & 0x08 == 0 {
            5
        } else if first_byte & 0x04 == 0 {
            6
        } else if first_byte & 0x02 == 0 {
            7
        } else if first_byte & 0x01 == 0 {
            8
        } else {
            9
        };

        if data.len() < size {
            return None;
        }

        let mut value = 0i64;
        for i in 0..size {
            value = (value << 8) | (data[i] as i64);
        }

        // Remove the size encoding bits from the first byte
        if size > 1 {
            let mask = (1u8 << (8 - size)) - 1;
            value = (value & !((0xFF & !mask as i64) << ((size - 1) * 8)));
        }

        Some((value, size))
    }
}
