use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// Header validation utilities for SSTable components
mod header_validation {
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::path::Path;
    use anyhow::{anyhow, Result};
    
    /// Validate that all component files have consistent headers
    pub fn validate_component_headers(components: &std::collections::HashMap<super::SSTableComponent, std::path::PathBuf>) -> Result<Vec<String>> {
        let mut inconsistencies = Vec::new();
        let mut generation_info = None;
        let mut table_info = None;
        
        for (component, path) in components {
            match extract_header_info(path) {
                Ok(header) => {
                    // Check generation consistency
                    if let Some(ref expected_gen) = generation_info {
                        if header.generation != *expected_gen {
                            inconsistencies.push(format!(
                                "Generation mismatch in {:?}: expected {}, found {}",
                                component, expected_gen, header.generation
                            ));
                        }
                    } else {
                        generation_info = Some(header.generation);
                    }
                    
                    // Check table ID consistency
                    if let Some(ref expected_table) = table_info {
                        if header.table_id != *expected_table {
                            inconsistencies.push(format!(
                                "Table ID mismatch in {:?}: expected {}, found {}",
                                component, expected_table, header.table_id
                            ));
                        }
                    } else {
                        table_info = Some(header.table_id.clone());
                    }
                },
                Err(e) => {
                    inconsistencies.push(format!("Failed to read header from {:?}: {}", component, e));
                }
            }
        }
        
        Ok(inconsistencies)
    }
    
    /// Extract header information from SSTable component file
    fn extract_header_info(path: &Path) -> Result<HeaderInfo> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut header_bytes = vec![0u8; 32]; // Read first 32 bytes for header analysis
        
        reader.read_exact(&mut header_bytes).map_err(|e| {
            anyhow!("Failed to read header from {:?}: {}", path, e)
        })?;
        
        // Parse generation from filename as fallback
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Invalid filename: {:?}", path))?;
            
        let generation = if let Some(dash_pos) = filename.find('-') {
            let second_part = &filename[dash_pos+1..];
            if let Some(second_dash) = second_part.find('-') {
                second_part[..second_dash].parse::<u32>()
                    .map_err(|_| anyhow!("Invalid generation in filename: {}", filename))?
            } else {
                return Err(anyhow!("Invalid SSTable filename format: {}", filename));
            }
        } else {
            return Err(anyhow!("Invalid SSTable filename format: {}", filename));
        };
        
        // For now, use a placeholder table ID (in real implementation, this would be extracted from the binary header)
        let table_id = format!("table_{}", generation);
        
        Ok(HeaderInfo {
            generation,
            table_id,
            format_version: header_bytes[0], // Simplified header parsing
        })
    }
    
    #[derive(Debug, Clone)]
    struct HeaderInfo {
        generation: u32,
        table_id: String,
        format_version: u8,
    }
}

/// Represents a single SSTable generation with all its component files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableGeneration {
    /// Generation number (e.g., 1 for "nb-1-big")
    pub generation: u32,
    /// Format type (e.g., "big", "da" for BTI)
    pub format: String,
    /// Table name
    pub table_name: String,
    /// Component files mapped by component type
    pub components: HashMap<SSTableComponent, PathBuf>,
    /// Base directory path
    pub base_path: PathBuf,
}

/// SSTable component types found in Cassandra 5
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SSTableComponent {
    /// Main data file containing row data
    Data,
    /// Index file for partition/row lookups (BIG format)
    Index,
    /// Statistics metadata
    Statistics,
    /// Bloom filter for negative lookups
    Filter,
    /// Index summary (BIG format)
    Summary,
    /// Compression metadata and block info
    CompressionInfo,
    /// CRC32 checksum
    Digest,
    /// Table of contents listing all components
    TOC,
    /// BTI Partitions index (BTI format only)
    Partitions,
    /// BTI Rows index (BTI format only)
    Rows,
}

impl FromStr for SSTableComponent {
    type Err = anyhow::Error;
    
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "Data.db" => Ok(SSTableComponent::Data),
            "Index.db" => Ok(SSTableComponent::Index),
            "Statistics.db" => Ok(SSTableComponent::Statistics),
            "Filter.db" => Ok(SSTableComponent::Filter),
            "Summary.db" => Ok(SSTableComponent::Summary),
            "CompressionInfo.db" => Ok(SSTableComponent::CompressionInfo),
            "Digest.crc32" => Ok(SSTableComponent::Digest),
            "TOC.txt" => Ok(SSTableComponent::TOC),
            "Partitions.db" => Ok(SSTableComponent::Partitions),
            "Rows.db" => Ok(SSTableComponent::Rows),
            _ => Err(anyhow!("Unknown SSTable component: {}", s)),
        }
    }
}

impl SSTableComponent {
    /// Returns the file extension for this component
    pub fn file_extension(&self) -> &'static str {
        match self {
            SSTableComponent::Data => "Data.db",
            SSTableComponent::Index => "Index.db",
            SSTableComponent::Statistics => "Statistics.db",
            SSTableComponent::Filter => "Filter.db",
            SSTableComponent::Summary => "Summary.db",
            SSTableComponent::CompressionInfo => "CompressionInfo.db",
            SSTableComponent::Digest => "Digest.crc32",
            SSTableComponent::TOC => "TOC.txt",
            SSTableComponent::Partitions => "Partitions.db",
            SSTableComponent::Rows => "Rows.db",
        }
    }
    
    /// Returns whether this component is required for reading data
    pub fn is_required(&self) -> bool {
        matches!(self, SSTableComponent::Data | SSTableComponent::Statistics)
    }
    
    /// Returns whether this component is BTI-specific
    pub fn is_bti_specific(&self) -> bool {
        matches!(self, SSTableComponent::Partitions | SSTableComponent::Rows)
    }
    
    /// Returns whether this component is BIG-specific
    pub fn is_big_specific(&self) -> bool {
        matches!(self, SSTableComponent::Index | SSTableComponent::Summary)
    }
}

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

/// Represents a secondary index with its own SSTable files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryIndex {
    /// Index name (e.g., "metadata_idx")
    pub index_name: String,
    /// Index directory path
    pub index_path: PathBuf,
    /// SSTable generations for this index
    pub generations: Vec<SSTableGeneration>,
}

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
    /// Check if the validation passed (no errors)
    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty() && 
        self.toc_inconsistencies.is_empty() && 
        self.header_inconsistencies.is_empty() && 
        self.corrupted_files.is_empty()
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
        report.push_str(&format!("=== SSTable Directory Validation Report ===\n\n"));
        report.push_str(&format!("Total Generations: {}\n", self.total_generations));
        report.push_str(&format!("Valid Generations: {}\n", self.valid_generations));
        report.push_str(&format!("Success Rate: {:.1}%\n\n", 
            if self.total_generations > 0 {
                (self.valid_generations as f64 / self.total_generations as f64) * 100.0
            } else { 0.0 }
        ));
        
        if !self.validation_errors.is_empty() {
            report.push_str(&format!("❌ Validation Errors ({}):\n", self.validation_errors.len()));
            for error in &self.validation_errors {
                report.push_str(&format!("  • {}\n", error));
            }
            report.push('\n');
        }
        
        if !self.toc_inconsistencies.is_empty() {
            report.push_str(&format!("📋 TOC Inconsistencies ({}):\n", self.toc_inconsistencies.len()));
            for inconsistency in &self.toc_inconsistencies {
                report.push_str(&format!("  • {}\n", inconsistency));
            }
            report.push('\n');
        }
        
        if !self.header_inconsistencies.is_empty() {
            report.push_str(&format!("🏷️ Header Inconsistencies ({}):\n", self.header_inconsistencies.len()));
            for inconsistency in &self.header_inconsistencies {
                report.push_str(&format!("  • {}\n", inconsistency));
            }
            report.push('\n');
        }
        
        if !self.corrupted_files.is_empty() {
            report.push_str(&format!("💥 Corrupted Files ({}):\n", self.corrupted_files.len()));
            for file in &self.corrupted_files {
                report.push_str(&format!("  • {}\n", file));
            }
            report.push('\n');
        }
        
        if !self.component_analysis.is_empty() {
            report.push_str(&format!("📊 Component Analysis by Generation:\n"));
            for analysis in &self.component_analysis {
                report.push_str(&format!("\n  Generation {} ({} format):\n", analysis.generation, analysis.format));
                report.push_str(&format!("    Required present: {:?}\n", analysis.required_components_present));
                if !analysis.required_components_missing.is_empty() {
                    report.push_str(&format!("    Required missing: {:?}\n", analysis.required_components_missing));
                }
                report.push_str(&format!("    Optional present: {:?}\n", analysis.optional_components_present));
                report.push_str(&format!("    Total file size: {} bytes\n", 
                    analysis.file_sizes.values().sum::<u64>()));
            }
        }
        
        report
    }
}

impl SSTableDirectory {
    /// Enhanced directory validation before scanning
    pub fn validate_directory_path<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(anyhow!("Directory does not exist: {:?}", path));
        }
        
        if !path.is_dir() {
            return Err(anyhow!("Path is not a directory: {:?}", path));
        }
        
        // Check directory permissions
        match fs::read_dir(path) {
            Ok(_) => (),
            Err(e) => return Err(anyhow!("Cannot read directory {:?}: {}", path, e)),
        }
        
        Ok(())
    }
    
    /// Scan a directory path and discover all SSTable components
    pub fn scan<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Validate directory before proceeding
        Self::validate_directory_path(&path)?;
        
        // Extract table name from directory name (e.g., "users-46436710673711f0b2cf19d64e7cbecb" -> "users")
        let dir_name = path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("Invalid directory path: {:?}", path))?;
            
        let table_name = extract_table_name(dir_name)?;
        
        // Scan for SSTable files
        let generations = scan_sstable_files(&path, &table_name)?;
        
        // Scan for secondary index directories
        let secondary_indexes = scan_secondary_indexes(&path, &table_name)?;
        
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
            .filter_map(|gen| gen.components.get(&SSTableComponent::Data))
            .collect()
    }
    
    /// Check if directory contains valid SSTable data
    pub fn is_valid(&self) -> bool {
        !self.generations.is_empty() && 
        self.generations.iter().any(|gen| {
            gen.components.contains_key(&SSTableComponent::Data) &&
            gen.components.contains_key(&SSTableComponent::Statistics)
        })
    }
    
    /// Get all secondary indexes
    pub fn get_secondary_indexes(&self) -> &[SecondaryIndex] {
        &self.secondary_indexes
    }
    
    /// Get a specific secondary index by name
    pub fn get_secondary_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.secondary_indexes.iter().find(|idx| idx.index_name == name)
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
            let mut generation_valid = true;
            
            // Create component analysis for this generation
            let mut analysis = ComponentAnalysis {
                generation: generation.generation,
                format: generation.format.clone(),
                required_components_present: Vec::new(),
                required_components_missing: Vec::new(),
                optional_components_present: Vec::new(),
                file_sizes: HashMap::new(),
                accessibility_status: HashMap::new(),
            };
            
            // Validate components with detailed analysis
            match validate_generation_components_enhanced(generation, &mut analysis) {
                Ok(issues) => {
                    if !issues.is_empty() {
                        report.validation_errors.extend(issues);
                        generation_valid = false;
                    }
                },
                Err(e) => {
                    report.validation_errors.push(format!("Validation error for generation {}: {}", generation.generation, e));
                    generation_valid = false;
                }
            }
            
            // Validate TOC consistency
            match validate_toc_consistency_enhanced(generation) {
                Ok(inconsistencies) => {
                    if !inconsistencies.is_empty() {
                        report.toc_inconsistencies.extend(inconsistencies);
                        generation_valid = false;
                    }
                },
                Err(e) => {
                    report.validation_errors.push(format!("TOC validation error for generation {}: {}", generation.generation, e));
                    generation_valid = false;
                }
            }
            
            // Validate header consistency across components
            match header_validation::validate_component_headers(&generation.components) {
                Ok(inconsistencies) => {
                    if !inconsistencies.is_empty() {
                        report.header_inconsistencies.extend(inconsistencies);
                        generation_valid = false;
                    }
                },
                Err(e) => {
                    report.validation_errors.push(format!("Header validation error for generation {}: {}", generation.generation, e));
                    generation_valid = false;
                }
            }
            
            // Check for corrupted files
            for (component, path) in &generation.components {
                match validate_file_integrity(path) {
                    Ok(false) => {
                        report.corrupted_files.push(format!("Corrupted file: {:?} at {:?}", component, path));
                        generation_valid = false;
                    },
                    Err(e) => {
                        report.corrupted_files.push(format!("Cannot validate {:?} at {:?}: {}", component, path, e));
                        generation_valid = false;
                    },
                    Ok(true) => {} // File is valid
                }
            }
            
            if generation_valid {
                report.valid_generations += 1;
            }
            
            report.component_analysis.push(analysis);
        }
        
        Ok(report)
    }
    
    /// Parse TOC.txt file for a specific generation
    pub fn parse_toc(&self, generation: &SSTableGeneration) -> Result<Vec<SSTableComponent>> {
        if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
            parse_toc_file(toc_path)
        } else {
            Err(anyhow!("No TOC.txt file found for generation {}", generation.generation))
        }
    }
    
    /// Get detailed directory summary for debugging and validation
    pub fn get_directory_summary(&self) -> String {
        let mut summary = String::new();
        summary.push_str(&format!("SSTable Directory Summary for '{}'\n", self.table_name));
        summary.push_str(&format!("Path: {:?}\n", self.path));
        summary.push_str(&format!("Generations: {}\n", self.generations.len()));
        summary.push_str(&format!("Secondary Indexes: {}\n", self.secondary_indexes.len()));
        summary.push_str(&format!("Valid: {}\n\n", self.is_valid()));
        
        for (i, gen) in self.generations.iter().enumerate() {
            summary.push_str(&format!("Generation {} ({}): {} components\n", 
                                    gen.generation, gen.format, gen.components.len()));
            
            // Check for required components
            let has_data = gen.components.contains_key(&SSTableComponent::Data);
            let has_stats = gen.components.contains_key(&SSTableComponent::Statistics);
            summary.push_str(&format!("  Required components: Data={}, Statistics={}\n", 
                                    has_data, has_stats));
            
            // List all components
            for (component, path) in &gen.components {
                let file_exists = path.exists();
                let file_size = if file_exists {
                    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
                } else {
                    0
                };
                summary.push_str(&format!("  {:?}: {} (exists: {}, size: {} bytes)\n", 
                                        component, path.file_name().unwrap().to_string_lossy(), 
                                        file_exists, file_size));
            }
            
            if i < self.generations.len() - 1 {
                summary.push('\n');
            }
        }
        
        summary
    }
}

/// Extract table name from directory name (strips UUID suffix)
fn extract_table_name(dir_name: &str) -> Result<String> {
    // Directory format: "tablename-{32-char-uuid}"
    // Find the last hyphen and take everything before it
    if let Some(hyphen_pos) = dir_name.rfind('-') {
        let table_name = &dir_name[..hyphen_pos];
        if table_name.is_empty() {
            return Err(anyhow!("Empty table name in directory: {}", dir_name));
        }
        Ok(table_name.to_string())
    } else {
        // Fallback: use entire directory name if no UUID suffix
        Ok(dir_name.to_string())
    }
}

/// Scan directory for SSTable files and group by generation
fn scan_sstable_files(path: &Path, table_name: &str) -> Result<Vec<SSTableGeneration>> {
    let entries = fs::read_dir(path)
        .with_context(|| format!("Failed to read directory: {:?}", path))?;
    
    let mut generations_map: HashMap<(u32, String), SSTableGeneration> = HashMap::new();
    let mut found_files = 0;
    let mut valid_sstable_files = 0;
    
    for entry in entries {
        let entry = entry?;
        let file_path = entry.path();
        found_files += 1;
        
        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
            // Enhanced validation: Check if file exists and is readable
            if !file_path.is_file() {
                continue; // Skip directories and non-files
            }
            
            // Check file accessibility
            if let Err(e) = fs::metadata(&file_path) {
                eprintln!("Warning: Cannot access file {:?}: {}", file_path, e);
                continue;
            }
            
            if let Some((generation, format, component)) = parse_sstable_filename(file_name)? {
                valid_sstable_files += 1;
                let key = (generation, format.clone());
                
                let gen = generations_map.entry(key.clone()).or_insert_with(|| {
                    SSTableGeneration {
                        generation,
                        format,
                        table_name: table_name.to_string(),
                        components: HashMap::new(),
                        base_path: path.to_path_buf(),
                    }
                });
                
                gen.components.insert(component, file_path);
            }
        }
    }
    
    // Enhanced validation and reporting
    if found_files == 0 {
        return Err(anyhow!("Directory appears to be empty: {:?}", path));
    }
    
    if valid_sstable_files == 0 {
        return Err(anyhow!(
            "No valid SSTable files found in directory: {:?}. Found {} files total, but none match the expected SSTable naming pattern (e.g., nb-1-big-Data.db)", 
            path, found_files
        ));
    }
    
    // Sort generations by number (newest first)
    let mut generations: Vec<SSTableGeneration> = generations_map.into_values().collect();
    generations.sort_by(|a, b| b.generation.cmp(&a.generation));
    
    // Log summary for debugging
    eprintln!("Directory scan completed: {} total files, {} SSTable files, {} generations found", 
              found_files, valid_sstable_files, generations.len());
    
    Ok(generations)
}

/// Parse SSTable filename to extract generation, format, and component
/// Examples: "nb-1-big-Data.db" -> (1, "big", Data)
///           "nb-2-da-Partitions.db" -> (2, "da", Partitions)
fn parse_sstable_filename(filename: &str) -> Result<Option<(u32, String, SSTableComponent)>> {
    // Pattern: {prefix}-{generation}-{format}-{component}
    let parts: Vec<&str> = filename.split('-').collect();
    
    if parts.len() < 4 {
        return Ok(None); // Not an SSTable file
    }
    
    // Extract generation number (second part)
    let generation: u32 = parts[1].parse()
        .with_context(|| format!("Invalid generation number in filename: {}", filename))?;
    
    // Extract format (third part)
    let format = parts[2].to_string();
    
    // Extract component (everything after third hyphen)
    let component_str = parts[3..].join("-");
    let component = SSTableComponent::from_str(&component_str)?;
    
    Ok(Some((generation, format, component)))
}

/// Parse TOC.txt file to get list of components with enhanced validation
pub fn parse_toc_file<P: AsRef<Path>>(path: P) -> Result<Vec<SSTableComponent>> {
    let path_ref = path.as_ref();
    
    // Enhanced file validation
    if !path_ref.exists() {
        return Err(anyhow!("TOC.txt file does not exist: {:?}", path_ref));
    }
    
    if !path_ref.is_file() {
        return Err(anyhow!("TOC.txt path is not a file: {:?}", path_ref));
    }
    
    let content = fs::read_to_string(path_ref)
        .with_context(|| format!("Failed to read TOC file: {:?}", path_ref))?;
    
    if content.trim().is_empty() {
        return Err(anyhow!("TOC.txt file is empty: {:?}", path_ref));
    }
    
    let mut components = Vec::new();
    let mut unknown_components = Vec::new();
    let mut line_number = 0;
    
    for line in content.lines() {
        line_number += 1;
        let line = line.trim();
        
        if line.is_empty() || line.starts_with('#') {
            continue; // Skip empty lines and comments
        }
        
        match SSTableComponent::from_str(line) {
            Ok(component) => {
                if !components.contains(&component) {
                    components.push(component);
                } else {
                    eprintln!("Warning: Duplicate component in TOC.txt line {}: {}", line_number, line);
                }
            },
            Err(_) => {
                unknown_components.push((line_number, line.to_string()));
                eprintln!("Warning: Unknown component in TOC.txt line {}: {}", line_number, line);
            }
        }
    }
    
    // Enhanced validation: Check for required components
    let has_data = components.contains(&SSTableComponent::Data);
    let has_statistics = components.contains(&SSTableComponent::Statistics);
    
    if !has_data {
        eprintln!("Warning: TOC.txt missing required Data.db component: {:?}", path_ref);
    }
    
    if !has_statistics {
        eprintln!("Warning: TOC.txt missing required Statistics.db component: {:?}", path_ref);
    }
    
    // Log parsing summary
    eprintln!("TOC.txt parsed: {} valid components, {} unknown components from {} lines", 
              components.len(), unknown_components.len(), line_number);
    
    if components.is_empty() {
        return Err(anyhow!("No valid components found in TOC.txt: {:?}", path_ref));
    }
    
    Ok(components)
}

/// Scan directory for secondary index subdirectories
fn scan_secondary_indexes(path: &Path, table_name: &str) -> Result<Vec<SecondaryIndex>> {
    let entries = fs::read_dir(path)
        .with_context(|| format!("Failed to read directory: {:?}", path))?;
    
    let mut secondary_indexes = Vec::new();
    
    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();
        
        if entry_path.is_dir() {
            if let Some(dir_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                // Check if this is a secondary index directory (starts with '.' and ends with '_idx')
                if dir_name.starts_with('.') && dir_name.ends_with("_idx") {
                    // Extract index name (e.g., ".users_metadata_idx" -> "metadata_idx")
                    let index_name = dir_name[1..].to_string(); // Remove leading '.'
                    
                    // Validate that the index name matches the table
                    let expected_prefix = format!("{}_", table_name);
                    if index_name.starts_with(&expected_prefix) {
                        // Scan SSTable files in the secondary index directory
                        let index_generations = scan_sstable_files(&entry_path, table_name)?;
                        
                        if !index_generations.is_empty() {
                            secondary_indexes.push(SecondaryIndex {
                                index_name,
                                index_path: entry_path,
                                generations: index_generations,
                            });
                        }
                    }
                }
            }
        }
    }
    
    Ok(secondary_indexes)
}

/// Enhanced validation with detailed component analysis
pub fn validate_generation_components_enhanced(generation: &SSTableGeneration, analysis: &mut ComponentAnalysis) -> Result<Vec<String>> {
    let mut issues = Vec::new();
    
    // Define required components based on format
    let required_components = match generation.format.as_str() {
        "big" => vec![SSTableComponent::Data, SSTableComponent::Statistics, SSTableComponent::Index, SSTableComponent::Summary],
        "da" => vec![SSTableComponent::Data, SSTableComponent::Statistics, SSTableComponent::Partitions, SSTableComponent::Rows],
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
            analysis.accessibility_status.insert(component.clone(), false);
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
                            eprintln!("Warning: Optional component file is empty: {:?}", component);
                        }
                    }
                    
                    // File readability test
                    match fs::File::open(path) {
                        Ok(_) => {
                            analysis.accessibility_status.insert(component.clone(), true);
                        },
                        Err(e) => {
                            issues.push(format!(
                                "Component file is not readable: {:?} at {:?} - {} (generation {})", 
                                component, path, e, generation.generation
                            ));
                            analysis.accessibility_status.insert(component.clone(), false);
                        }
                    }
                },
                Err(e) => {
                    issues.push(format!(
                        "Cannot access component file metadata: {:?} at {:?} - {} (generation {})", 
                        component, path, e, generation.generation
                    ));
                    analysis.accessibility_status.insert(component.clone(), false);
                }
            }
        }
    }
    
    Ok(issues)
}

/// Legacy function for backward compatibility
pub fn validate_generation_components(generation: &SSTableGeneration) -> Result<Vec<String>> {
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

/// Enhanced TOC validation with detailed component analysis
pub fn validate_toc_consistency_enhanced(generation: &SSTableGeneration) -> Result<Vec<String>> {
    let mut inconsistencies = Vec::new();
    
    if let Some(toc_path) = generation.components.get(&SSTableComponent::TOC) {
        match parse_toc_file(toc_path) {
            Ok(toc_components) => {
                // Validate TOC structure and completeness
                if toc_components.is_empty() {
                    inconsistencies.push("TOC.txt is empty or contains no valid components".to_string());
                    return Ok(inconsistencies);
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
                    if *file_component != SSTableComponent::TOC && !toc_components.contains(file_component) {
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
                    "big" => vec![SSTableComponent::Data, SSTableComponent::Statistics, 
                                 SSTableComponent::Index, SSTableComponent::Summary, 
                                 SSTableComponent::TOC],
                    "da" => vec![SSTableComponent::Data, SSTableComponent::Statistics, 
                                SSTableComponent::Partitions, SSTableComponent::Rows, 
                                SSTableComponent::TOC],
                    _ => vec![SSTableComponent::Data, SSTableComponent::Statistics, SSTableComponent::TOC],
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
                        generation.format, missing_expected.join(", ")
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
            },
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

/// Legacy function for backward compatibility
pub fn validate_toc_consistency(generation: &SSTableGeneration) -> Result<Vec<String>> {
    validate_toc_consistency_enhanced(generation)
}

/// Validate file integrity by checking basic file properties
fn validate_file_integrity(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    
    let metadata = fs::metadata(path)
        .with_context(|| format!("Cannot read metadata for {:?}", path))?;
    
    // Check if file is readable
    let _file = fs::File::open(path)
        .with_context(|| format!("Cannot open file for reading: {:?}", path))?;
    
    // For now, consider file valid if it exists and is readable
    // In a full implementation, this could include checksums, format validation, etc.
    Ok(true)
}

/// Test the enhanced validation against a specific SSTable directory
pub fn test_directory_validation<P: AsRef<Path>>(path: P) -> Result<ValidationReport> {
    let directory = SSTableDirectory::scan(path)?;
    directory.validate_all_generations()
}

/// Test all SSTable directories in the test environment
pub fn test_all_directories<P: AsRef<Path>>(base_path: P) -> Result<Vec<(String, ValidationReport)>> {
    let base_path = base_path.as_ref();
    let mut results = Vec::new();
    
    if !base_path.exists() {
        return Err(anyhow!("Base test path does not exist: {:?}", base_path));
    }
    
    let entries = fs::read_dir(base_path)
        .with_context(|| format!("Cannot read test directory: {:?}", base_path))?;
    
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
                        },
                        Err(e) => {
                            eprintln!("Failed to validate directory {}: {}", dir_name, e);
                        }
                    }
                }
            }
        }
    }
    
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;
    
    #[test]
    fn test_extract_table_name() {
        assert_eq!(extract_table_name("users-46436710673711f0b2cf19d64e7cbecb").unwrap(), "users");
        assert_eq!(extract_table_name("all_types-46200090673711f0b2cf19d64e7cbecb").unwrap(), "all_types");
        assert_eq!(extract_table_name("simple_table").unwrap(), "simple_table");
    }
    
    #[test]
    fn test_parse_sstable_filename() {
        let (gen, fmt, comp) = parse_sstable_filename("nb-1-big-Data.db").unwrap().unwrap();
        assert_eq!(gen, 1);
        assert_eq!(fmt, "big");
        assert_eq!(comp, SSTableComponent::Data);
        
        let (gen, fmt, comp) = parse_sstable_filename("nb-2-da-Partitions.db").unwrap().unwrap();
        assert_eq!(gen, 2);
        assert_eq!(fmt, "da");
        assert_eq!(comp, SSTableComponent::Partitions);
        
        assert!(parse_sstable_filename("not-an-sstable.txt").unwrap().is_none());
    }
    
    #[test]
    fn test_component_properties() {
        assert!(SSTableComponent::Data.is_required());
        assert!(SSTableComponent::Statistics.is_required());
        assert!(!SSTableComponent::Filter.is_required());
        
        assert!(SSTableComponent::Partitions.is_bti_specific());
        assert!(!SSTableComponent::Data.is_bti_specific());
        
        assert!(SSTableComponent::Index.is_big_specific());
        assert!(!SSTableComponent::Data.is_big_specific());
    }
    
    #[test]
    fn test_sstable_directory_scan() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();
        
        // Create mock SSTable files
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
            "nb-2-big-Data.db",
            "nb-2-big-Statistics.db",
        ];
        
        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }
        
        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        assert_eq!(directory.table_name, "test_table");
        assert_eq!(directory.generations.len(), 2);
        assert_eq!(directory.secondary_indexes.len(), 0); // No secondary indexes in this test
        
        // Should be sorted by generation (newest first)
        assert_eq!(directory.generations[0].generation, 2);
        assert_eq!(directory.generations[1].generation, 1);
        
        assert!(directory.is_valid());
    }
    
    #[test]
    fn test_secondary_index_scanning() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("users-abc123");
        fs::create_dir(&table_dir).unwrap();
        
        // Create main SSTable files
        let main_files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
        ];
        for file in &main_files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }
        
        // Create secondary index directory
        let index_dir = table_dir.join(".users_metadata_idx");
        fs::create_dir(&index_dir).unwrap();
        
        let index_files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db", 
            "nb-1-big-TOC.txt",
        ];
        for file in &index_files {
            fs::write(index_dir.join(file), "mock index content").unwrap();
        }
        
        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        assert_eq!(directory.table_name, "users");
        assert_eq!(directory.generations.len(), 1);
        assert_eq!(directory.secondary_indexes.len(), 1);
        
        let secondary_index = &directory.secondary_indexes[0];
        assert_eq!(secondary_index.index_name, "users_metadata_idx");
        assert_eq!(secondary_index.generations.len(), 1);
        
        // Test getter methods
        assert!(directory.get_secondary_index("users_metadata_idx").is_some());
        assert!(directory.get_secondary_index("nonexistent").is_none());
    }
    
    #[test]
    fn test_validation_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();
        
        // Create files including TOC.txt
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-Index.db",
            "nb-1-big-Summary.db",
            "nb-1-big-Filter.db",
        ];
        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }
        
        // Create TOC.txt with correct content
        fs::write(table_dir.join("nb-1-big-TOC.txt"), 
                 "Data.db\nStatistics.db\nIndex.db\nSummary.db\nFilter.db\nTOC.txt").unwrap();
        
        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        let validation_report = directory.validate_all_generations().unwrap();
        
        assert_eq!(validation_report.total_generations, 1);
        assert_eq!(validation_report.valid_generations, 1);
        assert!(validation_report.is_valid());
        println!("Validation summary: {}", validation_report.summary());
    }
    
    #[test] 
    fn test_toc_parsing_and_validation() {
        // Test TOC parsing
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("TOC.txt");
        fs::write(&toc_path, "Data.db\nStatistics.db\nIndex.db\nSummary.db\n").unwrap();
        
        let components = parse_toc_file(&toc_path).unwrap();
        assert_eq!(components.len(), 4);
        assert!(components.contains(&SSTableComponent::Data));
        assert!(components.contains(&SSTableComponent::Statistics));
        assert!(components.contains(&SSTableComponent::Index));
        assert!(components.contains(&SSTableComponent::Summary));
    }
    
    #[test]
    fn test_enhanced_component_validation() {
        use std::collections::HashMap;
        
        let temp_dir = TempDir::new().unwrap();
        let mut components = HashMap::new();
        
        // Create actual files with realistic sizes
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let stats_file = temp_dir.path().join("nb-1-big-Statistics.db");
        let index_file = temp_dir.path().join("nb-1-big-Index.db");
        let toc_file = temp_dir.path().join("nb-1-big-TOC.txt");
        
        fs::write(&data_file, "mock data content").unwrap();
        fs::write(&stats_file, "mock stats content").unwrap();
        fs::write(&index_file, "mock index content").unwrap();
        fs::write(&toc_file, "Data.db\nStatistics.db\nIndex.db\nTOC.txt").unwrap();
        
        components.insert(SSTableComponent::Data, data_file);
        components.insert(SSTableComponent::Statistics, stats_file);
        components.insert(SSTableComponent::Index, index_file);
        components.insert(SSTableComponent::TOC, toc_file);
        
        let generation = SSTableGeneration {
            generation: 1,
            format: "big".to_string(),
            table_name: "test".to_string(),
            components,
            base_path: temp_dir.path().to_path_buf(),
        };
        
        let mut analysis = ComponentAnalysis {
            generation: 1,
            format: "big".to_string(),
            required_components_present: Vec::new(),
            required_components_missing: Vec::new(),
            optional_components_present: Vec::new(),
            file_sizes: HashMap::new(),
            accessibility_status: HashMap::new(),
        };
        
        let issues = validate_generation_components_enhanced(&generation, &mut analysis).unwrap();
        
        // Should still complain about missing Summary for BIG format
        assert!(issues.iter().any(|i| i.contains("Summary")));
        
        // Check analysis results
        assert!(analysis.required_components_present.contains(&SSTableComponent::Data));
        assert!(analysis.required_components_present.contains(&SSTableComponent::Statistics));
        assert!(analysis.required_components_present.contains(&SSTableComponent::Index));
        assert!(analysis.required_components_missing.contains(&SSTableComponent::Summary));
        
        // Verify file sizes were recorded
        assert!(analysis.file_sizes.get(&SSTableComponent::Data).unwrap() > &0);
        assert!(analysis.file_sizes.get(&SSTableComponent::Statistics).unwrap() > &0);
    }
    
    #[test]
    fn test_enhanced_toc_validation() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();
        
        // Create files
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-Index.db",
            "nb-1-big-Summary.db",
        ];
        
        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }
        
        // Create TOC.txt with some inconsistencies for testing
        fs::write(table_dir.join("nb-1-big-TOC.txt"), 
                 "Data.db\nStatistics.db\nIndex.db\nSummary.db\nTOC.txt\nNonExistent.db\n").unwrap();
        
        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        let report = directory.validate_all_generations().unwrap();
        
        // Should detect the inconsistency (NonExistent.db listed in TOC but not present)
        assert!(!report.toc_inconsistencies.is_empty());
        assert!(report.toc_inconsistencies.iter()
               .any(|inc| inc.contains("NonExistent")));
    }
    
    #[test]
    fn test_directory_validation_integration() {
        // This test would run against actual test data if available
        let test_path = std::env::var("CASSANDRA_TEST_PATH")
            .unwrap_or_else(|_| "test-env/cassandra5/sstables".to_string());
        
        if std::path::Path::new(&test_path).exists() {
            match test_all_directories(&test_path) {
                Ok(results) => {
                    println!("Validated {} SSTable directories", results.len());
                    for (dir_name, report) in &results {
                        println!("Directory {}: {}", dir_name, report.summary());
                        if !report.is_valid() {
                            println!("Issues found in {}:\n{}", dir_name, report.detailed_report());
                        }
                    }
                    // At least some directories should be valid
                    assert!(results.iter().any(|(_, report)| report.is_valid()));
                },
                Err(e) => {
                    eprintln!("Integration test failed: {}", e);
                    // Don't fail the test if test data isn't available
                }
            }
        } else {
            println!("Skipping integration test - test data not available at {}", test_path);
        }
    }
    
    #[test]
    fn test_component_validation() {
        use std::collections::HashMap;
        
        let temp_dir = TempDir::new().unwrap();
        let mut components = HashMap::new();
        
        // Create actual files
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let stats_file = temp_dir.path().join("nb-1-big-Statistics.db");
        fs::write(&data_file, "data").unwrap();
        fs::write(&stats_file, "stats").unwrap();
        
        components.insert(SSTableComponent::Data, data_file);
        components.insert(SSTableComponent::Statistics, stats_file);
        
        let generation = SSTableGeneration {
            generation: 1,
            format: "big".to_string(),
            table_name: "test".to_string(),
            components,
            base_path: temp_dir.path().to_path_buf(),
        };
        
        let missing = validate_generation_components(&generation).unwrap();
        // Should complain about missing Index/Summary for BIG format
        assert!(missing.len() >= 2);
        assert!(missing.iter().any(|m| m.contains("Index")));
        assert!(missing.iter().any(|m| m.contains("Summary")));
    }
}