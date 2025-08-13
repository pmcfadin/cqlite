// SSTable Info Command Implementation - Issue #26
// Comprehensive SSTable analysis and metadata display

use anyhow::{Context, Result};
use cqlite_core::{
    Config,
    schema::{TableSchema, parse_cql_schema},
    storage::sstable::{
        bulletproof_reader::BulletproofReader, directory::SSTableDirectory, reader::SSTableReader,
        statistics_reader::find_statistics_file,
    },
};
use csv;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

use crate::cli::{
    InfoOutputFormat, create_version_error, detect_sstable_version, validate_cassandra_version,
};

/// Main info command implementation
pub async fn execute_info_command(
    sstable_path: &Path,
    detailed: bool,
    format: InfoOutputFormat,
    validate: bool,
    schema_path: Option<&Path>,
    auto_detect: bool,
    cassandra_version: Option<String>,
) -> Result<()> {
    println!("🔍 CQLite SSTable Info Analysis");
    println!("================================");

    // Create progress bar for analysis
    let pb = create_progress_bar("Analyzing SSTable");
    pb.set_message("Detecting format...");

    // Version detection and validation
    let detected_version = if auto_detect {
        match detect_sstable_version(&sstable_path.to_path_buf()) {
            Ok(version) => {
                info!("Auto-detected SSTable version: {}", version);
                pb.set_message("Version detected");
                Some(version)
            }
            Err(e) => {
                warn!("Failed to auto-detect version: {}", e);
                pb.set_message("Version detection failed");
                None
            }
        }
    } else {
        None
    };

    // Validate provided Cassandra version if specified
    let validated_version = if let Some(ref version) = cassandra_version {
        match validate_cassandra_version(version) {
            Ok(v) => {
                info!("Using Cassandra version: {:?}", v);
                pb.set_message("Cassandra version validated");
                Some(v.to_string())
            }
            Err(e) => {
                pb.finish_with_message("❌ Version validation failed");
                return Err(anyhow::anyhow!(create_version_error(
                    &format!("Invalid Cassandra version: {}", e),
                    detected_version.as_deref(),
                    Some(version)
                )));
            }
        }
    } else {
        None
    };

    pb.set_message("Analyzing SSTable structure...");

    // Check if path is directory or file
    let info_result = if sstable_path.is_dir() {
        analyze_sstable_directory(
            sstable_path,
            detected_version.clone(),
            validated_version.clone(),
            detailed,
            validate,
            schema_path,
            &pb,
        )
        .await
    } else {
        analyze_sstable_file(
            sstable_path,
            detected_version.clone(),
            validated_version.clone(),
            detailed,
            validate,
            schema_path,
            &pb,
        )
        .await
    };

    match info_result {
        Ok(metadata) => {
            pb.finish_with_message("✅ Analysis complete");
            display_info_output(metadata, format).await
        }
        Err(e) => {
            pb.finish_with_message("❌ Analysis failed");
            Err(e)
        }
    }
}

/// Analyze SSTable directory structure
async fn analyze_sstable_directory(
    directory_path: &Path,
    detected_version: Option<String>,
    validated_version: Option<String>,
    _detailed: bool,
    validate: bool,
    schema_path: Option<&Path>,
    pb: &ProgressBar,
) -> Result<SSTableInfoMetadata> {
    pb.set_message("Validating directory structure...");

    // Validate directory structure
    SSTableDirectory::validate_directory_path(directory_path)
        .with_context(|| format!("Invalid SSTable directory: {}", directory_path.display()))?;

    pb.set_message("Scanning directory contents...");

    // Scan the directory
    let directory = SSTableDirectory::scan(directory_path).with_context(|| {
        format!(
            "Failed to scan SSTable directory: {}",
            directory_path.display()
        )
    })?;

    pb.set_message("Analyzing generations...");

    // Analyze each generation
    let mut total_size = 0u64;
    let mut total_components = 0;
    let mut generation_info = Vec::new();

    for generation in &directory.generations {
        let mut gen_size = 0u64;
        let mut component_details = Vec::new();

        for (component, path) in &generation.components {
            if let Ok(metadata) = std::fs::metadata(path) {
                gen_size += metadata.len();
                component_details.push(ComponentInfo {
                    component_type: format!("{:?}", component),
                    file_path: path.clone(),
                    file_size: metadata.len(),
                });
            }
            total_components += 1;
        }

        total_size += gen_size;
        generation_info.push(GenerationInfo {
            generation: generation.generation,
            format: generation.format.clone(),
            size: gen_size,
            components: component_details,
        });
    }

    // Load schema information if provided
    let schema_info = if let Some(schema_path) = schema_path {
        pb.set_message("Loading schema information...");
        load_schema_info(schema_path).await.ok()
    } else {
        None
    };

    // Perform validation if requested
    let validation_result = if validate {
        pb.set_message("Validating directory integrity...");
        Some(perform_directory_validation(&directory).await)
    } else {
        None
    };

    // Try to analyze with bulletproof reader for enhanced info
    let bulletproof_info = if let Some(data_path) = find_data_file_in_directory(directory_path) {
        pb.set_message("Analyzing with bulletproof reader...");
        analyze_with_bulletproof_reader(&data_path).await.ok()
    } else {
        None
    };

    Ok(SSTableInfoMetadata {
        file_path: directory_path.to_path_buf(),
        file_type: SSTableFileType::Directory,
        file_size: total_size,
        detected_version: detected_version.clone(),
        validated_version,
        format_features: get_format_features(detected_version.as_deref()),
        schema_info,
        validation_result,
        directory_info: Some(DirectoryInfo {
            table_name: directory.table_name.clone(),
            generations: directory.generations.len(),
            is_valid: directory.is_valid(),
            total_components,
            generation_info,
        }),
        bulletproof_info,
        statistics_file: None, // Directories don't have a single statistics file
        // Default stats for directories (would need aggregation)
        entry_count: 0,
        table_count: 0,
        block_count: 0,
        index_size: 0,
        bloom_filter_size: 0,
        compression_ratio: 0.0,
        cache_hit_rate: 0.0,
    })
}

/// Analyze single SSTable file
async fn analyze_sstable_file(
    file_path: &Path,
    detected_version: Option<String>,
    validated_version: Option<String>,
    _detailed: bool,
    validate: bool,
    schema_path: Option<&Path>,
    pb: &ProgressBar,
) -> Result<SSTableInfoMetadata> {
    pb.set_message("Opening SSTable file...");

    // Get file size
    let file_size = std::fs::metadata(file_path)
        .with_context(|| format!("Failed to get file metadata: {}", file_path.display()))?
        .len();

    // Try bulletproof reader first for enhanced analysis
    let bulletproof_info = {
        pb.set_message("Analyzing with bulletproof reader...");
        analyze_with_bulletproof_reader(file_path).await.ok()
    };

    // Open with standard reader for statistics
    pb.set_message("Loading SSTable reader...");
    let config = Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(file_path, &config, platform.clone())
        .await
        .with_context(|| format!("Failed to open SSTable: {}", file_path.display()))?;

    pb.set_message("Reading SSTable statistics...");
    let reader_stats = reader.stats().await?;

    // Load schema information if provided
    let schema_info = if let Some(schema_path) = schema_path {
        pb.set_message("Loading schema information...");
        load_schema_info(schema_path).await.ok()
    } else {
        None
    };

    // Check for Statistics.db file
    let statistics_file = {
        pb.set_message("Looking for Statistics.db...");
        find_statistics_file(file_path)
            .await
            .map(|path| (path.to_path_buf(), "Statistics.db file found".to_string()))
    };

    // Perform validation if requested
    let validation_result = if validate {
        pb.set_message("Validating file integrity...");
        perform_file_validation(file_path, &reader).await.ok()
    } else {
        None
    };

    Ok(SSTableInfoMetadata {
        file_path: file_path.to_path_buf(),
        file_type: SSTableFileType::SingleFile,
        file_size,
        detected_version: detected_version.clone(),
        validated_version,
        format_features: get_format_features(detected_version.as_deref()),
        schema_info,
        validation_result,
        directory_info: None,
        bulletproof_info,
        statistics_file,
        // Extract individual fields from reader_stats
        entry_count: reader_stats.entry_count,
        table_count: reader_stats.table_count as u32,
        block_count: reader_stats.block_count as u32,
        index_size: reader_stats.index_size,
        bloom_filter_size: reader_stats.bloom_filter_size,
        compression_ratio: reader_stats.compression_ratio,
        cache_hit_rate: reader_stats.cache_hit_rate,
    })
}

/// Analyze SSTable with bulletproof reader for enhanced information
async fn analyze_with_bulletproof_reader(file_path: &Path) -> Result<BulletproofInfo> {
    let mut reader = BulletproofReader::open(file_path).with_context(|| {
        format!(
            "Failed to open with bulletproof reader: {}",
            file_path.display()
        )
    })?;

    // Get all information first to avoid borrowing conflicts
    let info = reader.info();
    let format_info = format!("{:?}", info.format);
    let generation = info.generation as u32;
    let detected_size = info.size.parse().unwrap_or(0);

    let compression_info = reader.compression_info().map(|c| CompressionInfo {
        algorithm: c.algorithm.clone(),
        chunk_length: c.chunk_length,
    });

    let integrity_check = reader.verify_integrity().await.ok();

    // Try to parse some data for analysis
    let data_sample = match reader.parse_sstable_data() {
        Ok(entries) => Some(DataSampleInfo {
            total_entries: entries.len(),
            sample_entries: entries
                .into_iter()
                .take(5)
                .map(|e| format!("{:?}", e))
                .collect(),
        }),
        Err(_) => None,
    };

    Ok(BulletproofInfo {
        format: format_info,
        generation,
        detected_size,
        compression_info,
        integrity_check,
        data_sample,
    })
}

/// Load schema information from schema file
pub async fn load_schema_info(schema_path: &Path) -> Result<SchemaInfo> {
    let schema = load_schema_file(schema_path)?;

    let keyspace = schema.keyspace.clone();
    let table = schema.table.clone();
    let partition_keys = schema
        .partition_keys
        .iter()
        .map(|k| k.name.clone())
        .collect();
    let clustering_keys = schema
        .clustering_keys
        .iter()
        .map(|k| k.name.clone())
        .collect();
    let columns = schema
        .columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            kind: determine_column_kind(&schema, &c.name),
        })
        .collect();

    Ok(SchemaInfo {
        keyspace,
        table,
        columns,
        partition_keys,
        clustering_keys,
    })
}

/// Load schema from file (simplified version)
fn load_schema_file(schema_path: &Path) -> Result<TableSchema> {
    let schema_content = std::fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {}", schema_path.display()))?;

    let extension = schema_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "json" => parse_json_schema(&schema_content),
        "cql" | "sql" | "" => {
            parse_cql_schema(&schema_content).with_context(|| "Failed to parse CQL schema")
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported schema file extension: .{}",
            extension
        )),
    }
}

/// Parse JSON schema (simplified)
fn parse_json_schema(schema_content: &str) -> Result<TableSchema> {
    // This is a simplified implementation - would need full JSON schema parsing
    let json: serde_json::Value = serde_json::from_str(schema_content)?;

    let keyspace = json["keyspace"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing keyspace in schema"))?;
    let table = json["table"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing table in schema"))?;

    // Create a basic schema structure
    Ok(TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        columns: Vec::new(), // Would need to parse columns from JSON
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        comments: HashMap::new(),
    })
}

/// Determine column kind based on schema
fn determine_column_kind(schema: &TableSchema, column_name: &str) -> String {
    if schema.partition_keys.iter().any(|k| k.name == column_name) {
        "PartitionKey".to_string()
    } else if schema.clustering_keys.iter().any(|k| k.name == column_name) {
        "ClusteringKey".to_string()
    } else {
        "Regular".to_string()
    }
}

/// Perform file validation
pub async fn perform_file_validation(
    _file_path: &Path,
    _reader: &SSTableReader,
) -> Result<ValidationResult> {
    // TODO: Implement actual file validation
    Ok(ValidationResult {
        is_valid: true,
        errors: vec![],
        warnings: vec!["File validation not yet fully implemented".to_string()],
        total_blocks_checked: 0,
        corrupted_blocks: 0,
    })
}

/// Perform directory validation
pub async fn perform_directory_validation(directory: &SSTableDirectory) -> ValidationResult {
    match directory.validate_all_generations() {
        Ok(report) => ValidationResult {
            is_valid: report.is_valid(),
            errors: report.validation_errors.clone(),
            warnings: report.toc_inconsistencies.clone(),
            total_blocks_checked: 0,
            corrupted_blocks: 0,
        },
        Err(e) => ValidationResult {
            is_valid: false,
            errors: vec![format!("Directory validation failed: {}", e)],
            warnings: vec![],
            total_blocks_checked: 0,
            corrupted_blocks: 0,
        },
    }
}

/// Find the Data.db file in a directory
fn find_data_file_in_directory(dir_path: &Path) -> Option<PathBuf> {
    let patterns = ["*-Data.db", "*-big-Data.db", "nb-*-big-Data.db"];

    for _pattern in &patterns {
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();

                // Simple pattern matching (could be improved)
                if file_name_str.ends_with("-Data.db") {
                    return Some(entry.path());
                }
            }
        }
    }

    None
}

/// Get format features based on detected version
pub fn get_format_features(version: Option<&str>) -> Vec<String> {
    match version {
        Some("3.11") => vec![
            "Legacy SSTable format (la)".to_string(),
            "Basic compression support".to_string(),
            "Simple bloom filters".to_string(),
            "Traditional indexing".to_string(),
        ],
        Some("4.0") => vec![
            "Modern compact format (mc)".to_string(),
            "Improved compression algorithms".to_string(),
            "Enhanced bloom filters".to_string(),
            "Streaming support".to_string(),
            "Better statistics".to_string(),
        ],
        Some("5.0") => vec![
            "New big format (nb)".to_string(),
            "Big Trie Index (bti) support".to_string(),
            "Advanced compression".to_string(),
            "Optimized I/O patterns".to_string(),
            "Enhanced metadata".to_string(),
            "Native ZSTD compression".to_string(),
        ],
        _ => vec!["Unknown format features".to_string()],
    }
}

/// Create a progress bar for analysis
fn create_progress_bar(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb
}

/// Display info output in specified format
pub async fn display_info_output(
    metadata: SSTableInfoMetadata,
    format: InfoOutputFormat,
) -> Result<()> {
    match format {
        InfoOutputFormat::Text => display_text_format(&metadata).await,
        InfoOutputFormat::Json => display_json_format(&metadata).await,
        InfoOutputFormat::Csv => display_csv_format(&metadata).await,
    }
}

/// Display metadata in human-readable text format
async fn display_text_format(metadata: &SSTableInfoMetadata) -> Result<()> {
    println!();
    println!("📊 SSTable Analysis Results");
    println!("===========================");

    // Basic information
    println!("📂 Path: {}", metadata.file_path.display());
    println!(
        "📄 Type: {}",
        match metadata.file_type {
            SSTableFileType::SingleFile => "Single SSTable File",
            SSTableFileType::Directory => "SSTable Directory",
        }
    );
    println!(
        "📏 Size: {} bytes ({:.2} MB)",
        metadata.file_size,
        metadata.file_size as f64 / 1_048_576.0
    );

    // Version information
    if let Some(ref version) = metadata.detected_version {
        println!("🔍 Detected Version: {}", version);
    }
    if let Some(ref version) = metadata.validated_version {
        println!("✅ Cassandra Version: {}", version);
    }

    // Format features
    println!("\n🚀 Format Features:");
    for feature in &metadata.format_features {
        println!("  • {}", feature);
    }

    // Bulletproof reader information
    if let Some(ref bp_info) = metadata.bulletproof_info {
        println!("\n🛡️ Bulletproof Reader Analysis:");
        println!("  Format: {}", bp_info.format);
        println!("  Generation: {}", bp_info.generation);
        println!("  Detected Size: {} bytes", bp_info.detected_size);

        if let Some(ref compression) = bp_info.compression_info {
            println!(
                "  Compression: {} (chunk size: {})",
                compression.algorithm, compression.chunk_length
            );
        }

        if let Some(integrity) = bp_info.integrity_check {
            println!(
                "  Integrity: {}",
                if integrity {
                    "✅ Valid"
                } else {
                    "❌ Issues detected"
                }
            );
        }

        if let Some(ref sample) = bp_info.data_sample {
            println!("  Data Entries: {} total", sample.total_entries);
            if !sample.sample_entries.is_empty() {
                println!(
                    "  Sample Keys: {}",
                    sample
                        .sample_entries
                        .iter()
                        .take(3)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
    }

    // Schema information
    if let Some(ref schema) = metadata.schema_info {
        println!("\n📋 Schema Information:");
        println!("  Keyspace: {}", schema.keyspace);
        println!("  Table: {}", schema.table);
        println!("  Columns: {}", schema.columns.len());
        println!("  Partition Keys: {}", schema.partition_keys.join(", "));
        if !schema.clustering_keys.is_empty() {
            println!("  Clustering Keys: {}", schema.clustering_keys.join(", "));
        }

        // Column details
        if !schema.columns.is_empty() {
            println!("  Column Details:");
            for col in &schema.columns {
                println!("    {} ({}) - {}", col.name, col.data_type, col.kind);
            }
        }
    }

    // Directory-specific information
    if let Some(ref dir_info) = metadata.directory_info {
        println!("\n📁 Directory Information:");
        println!("  Table Name: {}", dir_info.table_name);
        println!("  Generations: {}", dir_info.generations);
        println!("  Total Components: {}", dir_info.total_components);
        println!(
            "  Valid: {}",
            if dir_info.is_valid {
                "✅ Yes"
            } else {
                "❌ No"
            }
        );

        // Generation details
        if !dir_info.generation_info.is_empty() {
            println!("\n  📊 Generation Details:");
            for generation in &dir_info.generation_info {
                println!(
                    "    Generation {}: {} format, {} bytes, {} components",
                    generation.generation,
                    generation.format,
                    generation.size,
                    generation.components.len()
                );

                for component in &generation.components {
                    println!(
                        "      {} - {} bytes",
                        component.component_type, component.file_size
                    );
                }
            }
        }
    }

    // Reader statistics
    if metadata.entry_count > 0 || metadata.table_count > 0 {
        println!("\n📈 SSTable Statistics:");
        println!("  Entry Count: {}", metadata.entry_count);
        println!("  Table Count: {}", metadata.table_count);
        println!("  Block Count: {}", metadata.block_count);
        println!("  Index Size: {} bytes", metadata.index_size);
        println!("  Bloom Filter Size: {} bytes", metadata.bloom_filter_size);
        println!(
            "  Compression Ratio: {:.2}%",
            metadata.compression_ratio * 100.0
        );
        println!("  Cache Hit Rate: {:.2}%", metadata.cache_hit_rate * 100.0);
    }

    // Statistics file information
    if let Some((ref path, ref status)) = metadata.statistics_file {
        println!("\n📊 Statistics File:");
        println!("  Path: {}", path.display());
        println!("  Status: {}", status);
    }

    // Validation results
    if let Some(ref validation) = metadata.validation_result {
        println!("\n🔍 Validation Results:");
        println!(
            "  Status: {}",
            if validation.is_valid {
                "✅ Valid"
            } else {
                "❌ Invalid"
            }
        );

        if validation.total_blocks_checked > 0 {
            println!("  Blocks Checked: {}", validation.total_blocks_checked);
            println!("  Corrupted Blocks: {}", validation.corrupted_blocks);
        }

        if !validation.errors.is_empty() {
            println!("  Errors:");
            for error in &validation.errors {
                println!("    ❌ {}", error);
            }
        }

        if !validation.warnings.is_empty() {
            println!("  Warnings:");
            for warning in &validation.warnings {
                println!("    ⚠️ {}", warning);
            }
        }
    }

    println!("\n✅ Analysis Complete");
    Ok(())
}

/// Display metadata in JSON format
async fn display_json_format(metadata: &SSTableInfoMetadata) -> Result<()> {
    let json = serde_json::to_string_pretty(metadata)?;
    println!("{}", json);
    Ok(())
}

/// Display metadata in CSV format
async fn display_csv_format(metadata: &SSTableInfoMetadata) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    // Write headers
    wtr.write_record(&[
        "path",
        "type",
        "size_bytes",
        "detected_version",
        "cassandra_version",
        "generations",
        "components",
        "entry_count",
        "table_count",
        "block_count",
        "compression_ratio",
        "cache_hit_rate",
        "is_valid",
        "errors",
        "warnings",
    ])?;

    // Prepare data
    let file_type = match metadata.file_type {
        SSTableFileType::SingleFile => "file",
        SSTableFileType::Directory => "directory",
    };

    let generations = metadata
        .directory_info
        .as_ref()
        .map(|d| d.generations.to_string())
        .unwrap_or_else(|| "1".to_string());

    let components = metadata
        .directory_info
        .as_ref()
        .map(|d| d.total_components.to_string())
        .unwrap_or_else(|| "1".to_string());

    let entry_count = metadata.entry_count.to_string();
    let table_count = metadata.table_count.to_string();
    let block_count = metadata.block_count.to_string();
    let compression_ratio = format!("{:.2}", metadata.compression_ratio * 100.0);
    let cache_hit_rate = format!("{:.2}", metadata.cache_hit_rate * 100.0);

    let is_valid = metadata
        .validation_result
        .as_ref()
        .map(|v| v.is_valid.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let errors = metadata
        .validation_result
        .as_ref()
        .map(|v| v.errors.join("; "))
        .unwrap_or_default();

    let warnings = metadata
        .validation_result
        .as_ref()
        .map(|v| v.warnings.join("; "))
        .unwrap_or_default();

    // Write data row
    wtr.write_record(&[
        &metadata.file_path.display().to_string(),
        file_type,
        &metadata.file_size.to_string(),
        metadata.detected_version.as_deref().unwrap_or("unknown"),
        metadata.validated_version.as_deref().unwrap_or("unknown"),
        &generations,
        &components,
        &entry_count,
        &table_count,
        &block_count,
        &compression_ratio,
        &cache_hit_rate,
        &is_valid,
        &errors,
        &warnings,
    ])?;

    wtr.flush()?;
    Ok(())
}

// Data structures for enhanced info output

/// Complete SSTable information metadata
#[derive(Debug, Clone, serde::Serialize)]
pub struct SSTableInfoMetadata {
    pub file_path: PathBuf,
    pub file_type: SSTableFileType,
    pub file_size: u64,
    pub detected_version: Option<String>,
    pub validated_version: Option<String>,
    pub format_features: Vec<String>,
    pub schema_info: Option<SchemaInfo>,
    pub validation_result: Option<ValidationResult>,
    pub directory_info: Option<DirectoryInfo>,
    pub bulletproof_info: Option<BulletproofInfo>,
    pub statistics_file: Option<(PathBuf, String)>,
    // Note: SSTableReaderStats doesn't implement Serialize, so we'll store the data separately
    pub entry_count: u64,
    pub table_count: u32,
    pub block_count: u32,
    pub index_size: u64,
    pub bloom_filter_size: u64,
    pub compression_ratio: f64,
    pub cache_hit_rate: f64,
}

/// File type enumeration
#[derive(Debug, Clone, serde::Serialize)]
pub enum SSTableFileType {
    SingleFile,
    Directory,
}

/// Schema information
#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaInfo {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
}

/// Column information
#[derive(Debug, Clone, serde::Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub kind: String,
}

/// Directory-specific information
#[derive(Debug, Clone, serde::Serialize)]
pub struct DirectoryInfo {
    pub table_name: String,
    pub generations: usize,
    pub is_valid: bool,
    pub total_components: usize,
    pub generation_info: Vec<GenerationInfo>,
}

/// Generation information
#[derive(Debug, Clone, serde::Serialize)]
pub struct GenerationInfo {
    pub generation: u32,
    pub format: String,
    pub size: u64,
    pub components: Vec<ComponentInfo>,
}

/// Component information
#[derive(Debug, Clone, serde::Serialize)]
pub struct ComponentInfo {
    pub component_type: String,
    pub file_path: PathBuf,
    pub file_size: u64,
}

/// Bulletproof reader information
#[derive(Debug, Clone, serde::Serialize)]
pub struct BulletproofInfo {
    pub format: String,
    pub generation: u32,
    pub detected_size: u64,
    pub compression_info: Option<CompressionInfo>,
    pub integrity_check: Option<bool>,
    pub data_sample: Option<DataSampleInfo>,
}

/// Compression information
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompressionInfo {
    pub algorithm: String,
    pub chunk_length: u32,
}

/// Data sample information
#[derive(Debug, Clone, serde::Serialize)]
pub struct DataSampleInfo {
    pub total_entries: usize,
    pub sample_entries: Vec<String>, // Convert to serializable format
}

/// Validation result
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub total_blocks_checked: usize,
    pub corrupted_blocks: usize,
}
