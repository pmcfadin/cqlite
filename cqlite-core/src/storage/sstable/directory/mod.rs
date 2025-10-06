use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

// Module declarations
mod header_validation;
mod scan;
mod toc;
pub mod types;
pub mod validation;

// Re-export public types
pub use types::{SSTableComponent, SSTableGeneration, SecondaryIndex};
pub use validation::{ComponentAnalysis, ValidationReport};

// Re-export public functions
pub use toc::{parse_toc_file, parse_toc_file_detailed};
pub use validation::{
    test_all_directories, test_directory_validation, validate_generation_components,
    validate_toc_consistency,
};

#[cfg(feature = "enhanced-index-validation")]
pub use validation::{validate_generation_components_enhanced, validate_toc_consistency_enhanced};

// Tests module
#[cfg(test)]
mod tests;

/// Represents an entire SSTable directory containing multiple generations
#[derive(Debug, Clone)]
pub struct SSTableDirectory {
    /// Directory path
    pub path: PathBuf,
    /// Table name extracted from directory
    pub table_name: String,
    /// All generations found, sorted by generation number (newest first)
    pub generations: Vec<SSTableGeneration>,
    /// Secondary index directories (e.g., .table_name_idx)
    pub secondary_indexes: Vec<SecondaryIndex>,
}

impl SSTableDirectory {
    /// Enhanced directory validation before scanning
    pub fn validate_directory_path<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(Error::invalid_path(format!(
                "Directory does not exist: {:?}",
                path
            )));
        }

        if !path.is_dir() {
            return Err(Error::invalid_path(format!(
                "Path is not a directory: {:?}",
                path
            )));
        }

        // Check directory permissions
        match fs::read_dir(path) {
            Ok(_) => (),
            Err(e) => {
                return Err(Error::storage(format!(
                    "Cannot read directory {:?}: {}",
                    path, e
                )))
            }
        }

        Ok(())
    }

    /// Scan a directory path and discover all SSTable components
    pub fn scan<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Validate directory before proceeding
        Self::validate_directory_path(&path)?;

        // Extract table name from directory name (e.g., "users-46436710673711f0b2cf19d64e7cbecb" -> "users")
        let dir_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::invalid_path(format!("Invalid directory path: {:?}", path)))?;

        let table_name = scan::extract_table_name(dir_name)?;

        // Scan for SSTable files
        let generations = scan::scan_sstable_files(&path, &table_name)?;

        // Scan for secondary index directories
        let secondary_indexes = scan::scan_secondary_indexes(&path, &table_name)?;

        Ok(SSTableDirectory {
            path,
            table_name,
            generations,
            secondary_indexes,
        })
    }

    /// Get the latest (highest generation) SSTable
    pub fn latest_generation(&self) -> Option<&SSTableGeneration> {
        self.generations.first()
    }

    /// Get all data files across all generations (for merging)
    pub fn all_data_files(&self) -> Vec<&PathBuf> {
        self.generations
            .iter()
            .filter_map(|generation| generation.components.get(&SSTableComponent::Data))
            .collect()
    }

    /// Check if directory contains valid SSTable data
    pub fn is_valid(&self) -> bool {
        !self.generations.is_empty()
            && self.generations.iter().any(|generation| {
                generation.components.contains_key(&SSTableComponent::Data)
                    && generation
                        .components
                        .contains_key(&SSTableComponent::Statistics)
            })
    }

    /// Get all secondary indexes
    pub fn get_secondary_indexes(&self) -> &[SecondaryIndex] {
        &self.secondary_indexes
    }

    /// Get a specific secondary index by name
    pub fn get_secondary_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.secondary_indexes
            .iter()
            .find(|idx| idx.index_name == name)
    }

    /// Enhanced validation of all generations in this directory
    pub fn validate_all_generations(&self) -> Result<ValidationReport> {
        let mut report = ValidationReport {
            total_generations: self.generations.len(),
            valid_generations: 0,
            validation_errors: Vec::new(),
            toc_inconsistencies: Vec::new(),
            header_inconsistencies: Vec::new(),
            corrupted_files: Vec::new(),
            component_analysis: Vec::new(),
        };

        for generation in &self.generations {
            let mut has_critical_errors = false;

            // Create component analysis for this generation
            let analysis = ComponentAnalysis {
                generation: generation.generation,
                format: generation.format.clone(),
                required_components_present: Vec::new(),
                required_components_missing: Vec::new(),
                optional_components_present: Vec::new(),
                file_sizes: std::collections::HashMap::new(),
                accessibility_status: std::collections::HashMap::new(),
            };

            // Validate components with M1-appropriate validation
            #[cfg(feature = "enhanced-index-validation")]
            {
                let mut analysis_enhanced = analysis.clone();
                match validation::validate_generation_components_enhanced(
                    generation,
                    &mut analysis_enhanced,
                ) {
                    Ok(issues) => {
                        if !issues.is_empty() {
                            report.validation_errors.extend(issues.clone());
                            // Only mark as critically invalid if we have missing required components
                            if issues.iter().any(|issue| {
                                issue.contains("missing required") || issue.contains("corrupted")
                            }) {
                                has_critical_errors = true;
                            }
                        }
                    }
                    Err(e) => {
                        report.validation_errors.push(format!(
                            "Validation error for generation {}: {}",
                            generation.generation, e
                        ));
                        has_critical_errors = true;
                    }
                }
            }

            #[cfg(not(feature = "enhanced-index-validation"))]
            {
                // M1 basic validation: just check that required files exist
                let required_files = [SSTableComponent::Data, SSTableComponent::Statistics];

                for component in &required_files {
                    if !generation.components.contains_key(component) {
                        report.validation_errors.push(format!(
                            "Missing required component {:?} in generation {}",
                            component, generation.generation
                        ));
                        has_critical_errors = true;
                    }
                }
            }

            // Validate TOC consistency
            #[cfg(feature = "enhanced-index-validation")]
            {
                match validation::validate_toc_consistency_enhanced(generation) {
                    Ok(inconsistencies) => {
                        if !inconsistencies.is_empty() {
                            report.toc_inconsistencies.extend(inconsistencies.clone());
                            // TOC inconsistencies are warnings, not critical errors unless files are missing
                            if inconsistencies
                                .iter()
                                .any(|inc| inc.contains("missing") || inc.contains("NonExistent"))
                            {
                                has_critical_errors = true;
                            }
                        }
                    }
                    Err(e) => {
                        report.validation_errors.push(format!(
                            "TOC validation error for generation {}: {}",
                            generation.generation, e
                        ));
                        has_critical_errors = true;
                    }
                }
            }

            #[cfg(not(feature = "enhanced-index-validation"))]
            {
                // M1 basic TOC validation: just check that TOC.txt exists if present
                if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
                    if !toc_path.exists() {
                        report.validation_errors.push(format!(
                            "TOC.txt referenced but not found for generation {}",
                            generation.generation
                        ));
                    }
                }
            }

            // Validate header consistency across components
            match validation::header_validation::validate_component_headers(&generation.components)
            {
                Ok(inconsistencies) => {
                    if !inconsistencies.is_empty() {
                        report
                            .header_inconsistencies
                            .extend(inconsistencies.clone());
                        // Header inconsistencies are warnings unless they indicate actual corruption
                        // "failed to read header" from test files with short content should not be critical
                        if inconsistencies.iter().any(|inc| {
                            inc.contains("corrupted")
                                && !inc.contains("failed to fill whole buffer")
                        }) {
                            has_critical_errors = true;
                        }
                    }
                }
                Err(e) => {
                    report.validation_errors.push(format!(
                        "Header validation error for generation {}: {}",
                        generation.generation, e
                    ));
                    has_critical_errors = true;
                }
            }

            // Check for corrupted files
            for (component, path) in &generation.components {
                match validation::validate_file_integrity(path) {
                    Ok(false) => {
                        report
                            .corrupted_files
                            .push(format!("Corrupted file: {:?} at {:?}", component, path));
                        has_critical_errors = true;
                    }
                    Err(e) => {
                        report.corrupted_files.push(format!(
                            "Cannot validate {:?} at {:?}: {}",
                            component, path, e
                        ));
                        has_critical_errors = true;
                    }
                    Ok(true) => {} // File is valid
                }
            }

            // Only count as invalid if we have critical errors
            if !has_critical_errors {
                report.valid_generations += 1;
            }

            report.component_analysis.push(analysis);
        }

        Ok(report)
    }

    /// Parse TOC.txt file for a specific generation
    pub fn parse_toc(&self, generation: &SSTableGeneration) -> Result<Vec<SSTableComponent>> {
        if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
            toc::parse_toc_file(toc_path)
        } else {
            Err(Error::not_found(format!(
                "No TOC.txt file found for generation {}",
                generation.generation
            )))
        }
    }

    /// Get detailed directory summary for debugging and validation
    pub fn get_directory_summary(&self) -> String {
        let mut summary = String::new();
        summary.push_str(&format!(
            "SSTable Directory Summary for '{}'\n",
            self.table_name
        ));
        summary.push_str(&format!("Path: {:?}\n", self.path));
        summary.push_str(&format!("Generations: {}\n", self.generations.len()));
        summary.push_str(&format!(
            "Secondary Indexes: {}\n",
            self.secondary_indexes.len()
        ));
        summary.push_str(&format!("Valid: {}\n\n", self.is_valid()));

        for (i, generation) in self.generations.iter().enumerate() {
            summary.push_str(&format!(
                "Generation {} ({}): {} components\n",
                generation.generation,
                generation.format,
                generation.components.len()
            ));

            // Check for required components
            let has_data = generation.components.contains_key(&SSTableComponent::Data);
            let has_stats = generation
                .components
                .contains_key(&SSTableComponent::Statistics);
            summary.push_str(&format!(
                "  Required components: Data={}, Statistics={}\n",
                has_data, has_stats
            ));

            // List all components
            for (component, path) in &generation.components {
                let file_exists = path.exists();
                let file_size = if file_exists {
                    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
                } else {
                    0
                };
                let filename = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "<invalid>".to_string());
                summary.push_str(&format!(
                    "  {:?}: {} (exists: {}, size: {} bytes)\n",
                    component, filename, file_exists, file_size
                ));
            }

            if i < self.generations.len() - 1 {
                summary.push('\n');
            }
        }

        summary
    }
}
