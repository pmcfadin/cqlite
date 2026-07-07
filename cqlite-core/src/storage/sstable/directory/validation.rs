use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub(super) use super::header_validation;
use super::types::{SSTableComponent, SSTableGeneration};

#[cfg(feature = "enhanced-index-validation")]
use super::toc::parse_toc_file_detailed;

/// Validation report for SSTable directory scanning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Total number of generations found
    pub total_generations: usize,
    /// Number of valid generations (all required components present)
    pub valid_generations: usize,
    /// List of validation errors found
    pub validation_errors: Vec<String>,
    /// List of TOC.txt inconsistencies
    pub toc_inconsistencies: Vec<String>,
    /// List of header inconsistencies across components
    pub header_inconsistencies: Vec<String>,
    /// List of corrupted or inaccessible files
    pub corrupted_files: Vec<String>,
    /// Detailed component analysis per generation
    pub component_analysis: Vec<ComponentAnalysis>,
}

/// Detailed analysis of components for a single generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentAnalysis {
    /// Generation number
    pub generation: u32,
    /// Format (big, da, etc.)
    pub format: String,
    /// Required components present
    pub required_components_present: Vec<SSTableComponent>,
    /// Required components missing
    pub required_components_missing: Vec<SSTableComponent>,
    /// Optional components present
    pub optional_components_present: Vec<SSTableComponent>,
    /// File size analysis
    pub file_sizes: HashMap<SSTableComponent, u64>,
    /// Accessibility status
    pub accessibility_status: HashMap<SSTableComponent, bool>,
}

impl ValidationReport {
    /// Check if the validation passed (no critical errors)
    pub fn is_valid(&self) -> bool {
        // Critical errors: validation errors, TOC inconsistencies, corrupted files
        let has_critical_errors = !self.validation_errors.is_empty()
            || !self.toc_inconsistencies.is_empty()
            || !self.corrupted_files.is_empty();

        // Header inconsistencies are only critical if they indicate actual corruption
        let has_critical_header_issues = self
            .header_inconsistencies
            .iter()
            .any(|inc| inc.contains("corrupted") && !inc.contains("failed to fill whole buffer"));

        !has_critical_errors && !has_critical_header_issues
    }

    /// Get summary of validation results
    pub fn summary(&self) -> String {
        format!(
            "Validation Summary: {}/{} generations valid, {} errors, {} TOC inconsistencies, {} header issues, {} corrupted files",
            self.valid_generations,
            self.total_generations,
            self.validation_errors.len(),
            self.toc_inconsistencies.len(),
            self.header_inconsistencies.len(),
            self.corrupted_files.len()
        )
    }

    /// Get detailed validation report as formatted string
    pub fn detailed_report(&self) -> String {
        let mut report = String::new();
        report.push_str("=== SSTable Directory Validation Report ===\n\n");
        report.push_str(&format!("Total Generations: {}\n", self.total_generations));
        report.push_str(&format!("Valid Generations: {}\n", self.valid_generations));
        report.push_str(&format!(
            "Success Rate: {:.1}%\n\n",
            if self.total_generations > 0 {
                (self.valid_generations as f64 / self.total_generations as f64) * 100.0
            } else {
                0.0
            }
        ));

        if !self.validation_errors.is_empty() {
            report.push_str(&format!(
                "❌ Validation Errors ({}):\n",
                self.validation_errors.len()
            ));
            for error in &self.validation_errors {
                report.push_str(&format!("  • {}\n", error));
            }
            report.push('\n');
        }

        if !self.toc_inconsistencies.is_empty() {
            report.push_str(&format!(
                "📋 TOC Inconsistencies ({}):\n",
                self.toc_inconsistencies.len()
            ));
            for inconsistency in &self.toc_inconsistencies {
                report.push_str(&format!("  • {}\n", inconsistency));
            }
            report.push('\n');
        }

        if !self.header_inconsistencies.is_empty() {
            report.push_str(&format!(
                "🏷️ Header Inconsistencies ({}):\n",
                self.header_inconsistencies.len()
            ));
            for inconsistency in &self.header_inconsistencies {
                report.push_str(&format!("  • {}\n", inconsistency));
            }
            report.push('\n');
        }

        if !self.corrupted_files.is_empty() {
            report.push_str(&format!(
                "💥 Corrupted Files ({}):\n",
                self.corrupted_files.len()
            ));
            for file in &self.corrupted_files {
                report.push_str(&format!("  • {}\n", file));
            }
            report.push('\n');
        }

        if !self.component_analysis.is_empty() {
            report.push_str("📊 Component Analysis by Generation:\n");
            for analysis in &self.component_analysis {
                report.push_str(&format!(
                    "\n  Generation {} ({} format):\n",
                    analysis.generation, analysis.format
                ));
                report.push_str(&format!(
                    "    Required present: {:?}\n",
                    analysis.required_components_present
                ));
                if !analysis.required_components_missing.is_empty() {
                    report.push_str(&format!(
                        "    Required missing: {:?}\n",
                        analysis.required_components_missing
                    ));
                }
                report.push_str(&format!(
                    "    Optional present: {:?}\n",
                    analysis.optional_components_present
                ));
                report.push_str(&format!(
                    "    Total file size: {} bytes\n",
                    analysis.file_sizes.values().sum::<u64>()
                ));
            }
        }

        report
    }
}

/// Enhanced validation with detailed component analysis (M1: gated for post-M1)
#[cfg(feature = "enhanced-index-validation")]
pub fn validate_generation_components_enhanced(
    generation: &SSTableGeneration,
    analysis: &mut ComponentAnalysis,
) -> Result<Vec<String>> {
    let mut issues = Vec::new();

    // Define required components based on format
    let required_components = match generation.format.as_str() {
        "big" => vec![
            SSTableComponent::Data,
            SSTableComponent::Statistics,
            SSTableComponent::Index,
            SSTableComponent::Summary,
        ],
        "da" => vec![
            SSTableComponent::Data,
            SSTableComponent::Statistics,
            SSTableComponent::Partitions,
            SSTableComponent::Rows,
        ],
        _ => vec![SSTableComponent::Data, SSTableComponent::Statistics], // Minimal requirements
    };

    // Check required components
    for component in &required_components {
        if generation.components.contains_key(component) {
            analysis.required_components_present.push(component.clone());
        } else {
            analysis.required_components_missing.push(component.clone());
            issues.push(format!(
                "Missing required component: {:?} for generation {} (format: {})",
                component, generation.generation, generation.format
            ));
        }
    }

    // Analyze all present components
    for (component, path) in &generation.components {
        // Check if component is optional
        if !required_components.contains(component) {
            analysis.optional_components_present.push(component.clone());
        }

        // File existence and accessibility
        if !path.exists() {
            issues.push(format!(
                "Component file does not exist: {:?} at {:?} (generation {})",
                component, path, generation.generation
            ));
            analysis
                .accessibility_status
                .insert(component.clone(), false);
        } else {
            match fs::metadata(path) {
                Ok(metadata) => {
                    let file_size = metadata.len();
                    analysis.file_sizes.insert(component.clone(), file_size);

                    // Zero-size file validation
                    if file_size == 0 {
                        if component.is_required() {
                            issues.push(format!(
                                "Required component file is empty: {:?} at {:?} (generation {})",
                                component, path, generation.generation
                            ));
                        } else {
                            // Log warning for empty optional files
                            tracing::warn!("Optional component file is empty: {:?}", component);
                        }
                    }

                    // File readability test
                    match fs::File::open(path) {
                        Ok(_) => {
                            analysis
                                .accessibility_status
                                .insert(component.clone(), true);
                        }
                        Err(e) => {
                            issues.push(format!(
                                "Component file is not readable: {:?} at {:?} - {} (generation {})",
                                component, path, e, generation.generation
                            ));
                            analysis
                                .accessibility_status
                                .insert(component.clone(), false);
                        }
                    }
                }
                Err(e) => {
                    issues.push(format!(
                        "Cannot access component file metadata: {:?} at {:?} - {} (generation {})",
                        component, path, e, generation.generation
                    ));
                    analysis
                        .accessibility_status
                        .insert(component.clone(), false);
                }
            }
        }
    }

    Ok(issues)
}

/// Legacy function for backward compatibility (M1: basic validation only)
pub fn validate_generation_components(generation: &SSTableGeneration) -> Result<Vec<String>> {
    #[cfg(feature = "enhanced-index-validation")]
    {
        let mut dummy_analysis = ComponentAnalysis {
            generation: generation.generation,
            format: generation.format.clone(),
            required_components_present: Vec::new(),
            required_components_missing: Vec::new(),
            optional_components_present: Vec::new(),
            file_sizes: HashMap::new(),
            accessibility_status: HashMap::new(),
        };
        validate_generation_components_enhanced(generation, &mut dummy_analysis)
    }

    #[cfg(not(feature = "enhanced-index-validation"))]
    {
        // M1 basic validation: check required files exist
        let mut issues = Vec::new();
        let required_files = [SSTableComponent::Data, SSTableComponent::Statistics];

        for component in &required_files {
            if !generation.components.contains_key(component) {
                issues.push(format!(
                    "Missing required component {:?} in generation {}",
                    component, generation.generation
                ));
            }
        }

        Ok(issues)
    }
}

/// Enhanced TOC validation with detailed component analysis (M1: gated for post-M1)
#[cfg(feature = "enhanced-index-validation")]
pub fn validate_toc_consistency_enhanced(generation: &SSTableGeneration) -> Result<Vec<String>> {
    let mut inconsistencies = Vec::new();

    if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
        match parse_toc_file_detailed(toc_path) {
            Ok((toc_components, unknown_components)) => {
                // Validate TOC structure and completeness
                if toc_components.is_empty() && unknown_components.is_empty() {
                    inconsistencies
                        .push("TOC.txt is empty or contains no valid components".to_string());
                    return Ok(inconsistencies);
                }

                // Check for unknown/invalid components in TOC (like "NonExistent.db")
                if !unknown_components.is_empty() {
                    inconsistencies.push(format!(
                        "TOC.txt lists unknown/invalid components: [{}]",
                        unknown_components.join(", ")
                    ));
                }

                // Check that all TOC components have corresponding files
                let mut missing_files = Vec::new();
                for toc_component in &toc_components {
                    if !generation.components.contains_key(toc_component) {
                        missing_files.push(format!("{:?}", toc_component));
                    }
                }

                if !missing_files.is_empty() {
                    inconsistencies.push(format!(
                        "TOC.txt lists components without corresponding files: [{}]",
                        missing_files.join(", ")
                    ));
                }

                // Check that all files are listed in TOC (except TOC itself)
                let mut unlisted_files = Vec::new();
                for (file_component, path) in &generation.components {
                    if *file_component != SSTableComponent::TOC
                        && !toc_components.contains(file_component)
                    {
                        // Additional check: ensure the file actually exists before reporting as unlisted
                        if path.exists() {
                            unlisted_files.push(format!("{:?}", file_component));
                        }
                    }
                }

                if !unlisted_files.is_empty() {
                    inconsistencies.push(format!(
                        "Files exist but not listed in TOC.txt: [{}]",
                        unlisted_files.join(", ")
                    ));
                }

                // Validate expected components for format
                let expected_components = match generation.format.as_str() {
                    "big" => vec![
                        SSTableComponent::Data,
                        SSTableComponent::Statistics,
                        SSTableComponent::Index,
                        SSTableComponent::Summary,
                        SSTableComponent::TOC,
                    ],
                    "da" => vec![
                        SSTableComponent::Data,
                        SSTableComponent::Statistics,
                        SSTableComponent::Partitions,
                        SSTableComponent::Rows,
                        SSTableComponent::TOC,
                    ],
                    _ => vec![
                        SSTableComponent::Data,
                        SSTableComponent::Statistics,
                        SSTableComponent::TOC,
                    ],
                };

                let mut missing_expected = Vec::new();
                for expected in &expected_components {
                    if !toc_components.contains(expected) {
                        missing_expected.push(format!("{:?}", expected));
                    }
                }

                if !missing_expected.is_empty() {
                    inconsistencies.push(format!(
                        "TOC.txt missing expected components for {} format: [{}]",
                        generation.format,
                        missing_expected.join(", ")
                    ));
                }

                // Check for duplicate entries in TOC
                let mut seen_components = std::collections::HashSet::new();
                let mut duplicates = Vec::new();
                for component in &toc_components {
                    if !seen_components.insert(component) {
                        duplicates.push(format!("{:?}", component));
                    }
                }

                if !duplicates.is_empty() {
                    inconsistencies.push(format!(
                        "TOC.txt contains duplicate entries: [{}]",
                        duplicates.join(", ")
                    ));
                }
            }
            Err(e) => {
                inconsistencies.push(format!("Failed to parse TOC.txt: {}", e));
            }
        }
    } else {
        inconsistencies.push(format!(
            "No TOC.txt file found for generation {} (format: {})",
            generation.generation, generation.format
        ));
    }

    Ok(inconsistencies)
}

/// Legacy function for backward compatibility (M1: basic validation only)
pub fn validate_toc_consistency(generation: &SSTableGeneration) -> Result<Vec<String>> {
    #[cfg(feature = "enhanced-index-validation")]
    {
        validate_toc_consistency_enhanced(generation)
    }

    #[cfg(not(feature = "enhanced-index-validation"))]
    {
        // M1 basic validation: just check that TOC.txt exists and is readable
        let mut inconsistencies = Vec::new();

        if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
            if !toc_path.exists() {
                inconsistencies.push(format!(
                    "TOC.txt referenced but not found for generation {}",
                    generation.generation
                ));
            }
        }

        Ok(inconsistencies)
    }
}

/// Validate file integrity by checking basic file properties
pub(crate) fn validate_file_integrity(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let _metadata = fs::metadata(path)
        .map_err(|e| Error::storage(format!("Cannot read metadata for {:?}: {}", path, e)))?;

    // Check if file is readable
    let _file = fs::File::open(path)
        .map_err(|e| Error::storage(format!("Cannot open file for reading: {:?}: {}", path, e)))?;

    // For now, consider file valid if it exists and is readable
    // In a full implementation, this could include checksums, format validation, etc.
    Ok(true)
}

/// Test the enhanced validation against a specific SSTable directory
pub fn test_directory_validation<P: AsRef<Path>>(path: P) -> Result<ValidationReport> {
    let directory = super::SSTableDirectory::scan(path)?;
    directory.validate_all_generations()
}

/// Test all SSTable directories in the test environment
pub fn test_all_directories<P: AsRef<Path>>(
    base_path: P,
) -> Result<Vec<(String, ValidationReport)>> {
    let base_path = base_path.as_ref();
    let mut results = Vec::new();

    if !base_path.exists() {
        return Err(Error::invalid_path(format!(
            "Base test path does not exist: {:?}",
            base_path
        )));
    }

    let entries = fs::read_dir(base_path).map_err(|e| {
        Error::storage(format!(
            "Cannot read test directory: {:?}: {}",
            base_path, e
        ))
    })?;

    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            if let Some(dir_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                // Skip hidden directories and non-SSTable directories
                if !dir_name.starts_with('.') && dir_name.contains('-') {
                    match test_directory_validation(&entry_path) {
                        Ok(report) => {
                            results.push((dir_name.to_string(), report));
                        }
                        Err(e) => {
                            tracing::error!("Failed to validate directory {}: {}", dir_name, e);
                        }
                    }
                }
            }
        }
    }

    Ok(results)
}
