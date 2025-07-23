use crate::cli::{ExportFormat, ImportFormat, OutputFormat, detect_sstable_version, validate_cassandra_version, create_version_error};
use anyhow::{Context, Result};
use cqlite_core::{
    schema::{TableSchema, Column, KeyColumn, ClusteringColumn, parse_cql_schema},
    storage::sstable::{reader::SSTableReader, directory::SSTableDirectory, statistics_reader::{StatisticsReader, find_statistics_file, check_statistics_availability}},
};
use indicatif::{ProgressBar, ProgressStyle};
use prettytable::{Cell, Row, Table};
use serde_json;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{info, warn};

pub mod admin;
pub mod bench;
pub mod schema;

pub async fn execute_query(
    db_path: &Path,
    query: &str,
    explain: bool,
    timing: bool,
    format: OutputFormat,
) -> Result<()> {
    // TODO: Implement query execution
    println!("Executing query: {}", query);
    println!("Database: {}", db_path.display());
    println!(
        "Explain: {}, Timing: {}, Format: {}",
        explain, timing, format
    );

    // Placeholder implementation
    Ok(())
}

pub async fn import_data(
    db_path: &Path,
    file: &Path,
    format: ImportFormat,
    table: Option<&str>,
) -> Result<()> {
    // TODO: Implement data import
    println!("Importing data from: {}", file.display());
    println!("Database: {}", db_path.display());
    println!("Format: {}, Table: {:?}", format, table);

    // Placeholder implementation
    Ok(())
}

pub async fn export_data(
    db_path: &Path,
    source: &str,
    file: &Path,
    format: ExportFormat,
) -> Result<()> {
    // TODO: Implement data export
    println!("Exporting data from: {}", source);
    println!("Database: {}", db_path.display());
    println!("Output file: {}, Format: {}", file.display(), format);

    // Placeholder implementation
    Ok(())
}

/// Read and display SSTable directory or file data with schema
pub async fn read_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    limit: Option<usize>,
    skip: Option<usize>,
    generation: Option<u32>,
    format: OutputFormat,
    auto_detect: bool,
    cassandra_version: Option<String>,
) -> Result<()> {
    // Version detection and validation
    let detected_version = if auto_detect {
        match detect_sstable_version(&sstable_path.to_path_buf()) {
            Ok(version) => {
                info!("Auto-detected SSTable version: {}", version);
                Some(version)
            }
            Err(e) => {
                warn!("Failed to auto-detect version: {}", e);
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
                info!("Using Cassandra version: {}", v.to_string());
                Some(v)
            }
            Err(e) => {
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

    // Load schema with auto-detection
    let schema = load_schema_file(schema_path, detected_version.as_deref(), cassandra_version.as_deref())?;

    // Determine if input is directory or file and handle accordingly
    let actual_sstable_path = if sstable_path.is_dir() {
        println!("Detected SSTable directory: {}", sstable_path.display());
        
        // Validate directory first
        SSTableDirectory::validate_directory_path(sstable_path)
            .with_context(|| format!("Directory validation failed for: {}", sstable_path.display()))?;
        
        // Scan the directory structure with enhanced error reporting  
        let directory = SSTableDirectory::scan(sstable_path)
            .with_context(|| format!("Failed to scan SSTable directory: {} - Expected Cassandra 5.0 directory structure with files like nb-1-big-Data.db", sstable_path.display()))?;
        
        if !directory.is_valid() {
            return Err(anyhow::anyhow!("Directory does not contain valid SSTable data: {}", sstable_path.display()));
        }
        
        println!("Table: {}", directory.table_name);
        println!("Generations found: {}", directory.generations.len());
        
        // Select generation based on user input or use latest
        let selected_generation = if let Some(gen_num) = generation {
            directory.generations.iter()
                .find(|g| g.generation == gen_num)
                .ok_or_else(|| anyhow::anyhow!("Generation {} not found. Available generations: {:?}", 
                    gen_num, 
                    directory.generations.iter().map(|g| g.generation).collect::<Vec<_>>()))?
        } else {
            directory.latest_generation()
                .ok_or_else(|| anyhow::anyhow!("No valid generations found in directory"))?
        };
        
        println!("Using generation: {} (format: {})", selected_generation.generation, selected_generation.format);
        
        // Use the Data.db file from selected generation
        selected_generation.components.get(&cqlite_core::storage::sstable::directory::SSTableComponent::Data)
            .ok_or_else(|| anyhow::anyhow!("No Data.db file found in generation {}", selected_generation.generation))?
            .clone()
    } else {
        println!("Detected legacy SSTable file: {}", sstable_path.display());
        if generation.is_some() {
            println!("Warning: --generation ignored for single file input");
        }
        sstable_path.to_path_buf()
    };

    // Create SSTable reader with enhanced error handling
    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(&actual_sstable_path, &config, platform.clone())
        .await
        .with_context(|| {
            create_version_error(
                &format!("Failed to open SSTable: {}", actual_sstable_path.display()),
                detected_version.as_deref(),
                cassandra_version.as_deref()
            )
        })?;

    println!("Reading SSTable: {}", sstable_path.display());
    println!(
        "Schema: {}.{} ({} columns)",
        schema.keyspace,
        schema.table,
        schema.columns.len()
    );
    
    // Display version information
    if let Some(version) = detected_version.as_ref() {
        println!("Detected version: {}", version);
    }
    if let Some(version) = validated_version.as_ref() {
        println!("Using Cassandra compatibility: {}", version.to_string());
    }

    let skip_count = skip.unwrap_or(0);
    let limit_count = limit.unwrap_or(100); // Default limit to avoid overwhelming output

    // Create progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} rows processed")
            .unwrap(),
    );

    match format {
        OutputFormat::Table => {
            display_as_table(&reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Json => {
            display_as_json(&reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Csv => {
            display_as_csv(&reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Yaml => {
            display_as_yaml(&reader, &schema, skip_count, limit_count, &pb).await
        }
    }
}

/// Display SSTable directory or file information with enhanced statistics
pub async fn sstable_info(
    sstable_path: &Path,
    detailed: bool,
    auto_detect: bool,
    cassandra_version: Option<String>,
) -> Result<()> {
    // Version detection and validation
    let detected_version = if auto_detect {
        match detect_sstable_version(&sstable_path.to_path_buf()) {
            Ok(version) => {
                info!("Auto-detected SSTable version: {}", version);
                Some(version)
            }
            Err(e) => {
                warn!("Failed to auto-detect version: {}", e);
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
                info!("Using Cassandra version: {}", v.to_string());
                Some(v)
            }
            Err(e) => {
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

    // Handle directory vs file with enhanced error messages
    if sstable_path.is_dir() {
        // Directory mode - show comprehensive information
        println!("SSTable Directory Information");
        println!("============================");
        
        // Enhanced directory validation and scanning
        match SSTableDirectory::validate_directory_path(sstable_path) {
            Ok(_) => println!("✓ Directory validation passed"),
            Err(e) => {
                return Err(anyhow::anyhow!(create_version_error(
                    &format!("Directory validation failed: {}", e),
                    detected_version.as_deref(),
                    cassandra_version.as_deref()
                )));
            }
        }
        
        // Scan the directory structure with enhanced error handling
        let directory = match SSTableDirectory::scan(sstable_path) {
            Ok(dir) => {
                println!("✓ Directory scan completed successfully");
                dir
            },
            Err(e) => {
                eprintln!("❌ Directory scan failed: {}", e);
                return Err(anyhow::anyhow!(create_version_error(
                    &format!("Failed to scan SSTable directory: {} - This may indicate missing SSTable files, incorrect directory structure, or permission issues. Expected format: tablename-UUID with files like nb-1-big-Data.db", e),
                    detected_version.as_deref(),
                    cassandra_version.as_deref()
                )));
            }
        };
        
        println!("Directory: {}", sstable_path.display());
        println!("Table: {}", directory.table_name);
        println!("Valid SSTable data: {}", directory.is_valid());
        println!("Generations: {}", directory.generations.len());
        
        if detailed {
            println!("\n{}", directory.get_directory_summary());
        }
        
        // Run validation and display results
        match directory.validate_all_generations() {
            Ok(validation_report) => {
                println!("\n📋 Validation Report:");
                println!("{}", validation_report.summary());
                
                if !validation_report.is_valid() {
                    println!("\n⚠️  Validation Issues:");
                    for error in &validation_report.validation_errors {
                        println!("  • {}", error);
                    }
                    for inconsistency in &validation_report.toc_inconsistencies {
                        println!("  • TOC Issue: {}", inconsistency);
                    }
                }
            },
            Err(e) => {
                println!("\n❌ Validation failed: {}", e);
            }
        }
        
        println!();
        
        for generation in &directory.generations {
            println!("Generation {}: (format: {})", generation.generation, generation.format);
            println!("  Components: {}", generation.components.len());
            
            if detailed {
                for (component, path) in &generation.components {
                    let file_size = std::fs::metadata(path)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    println!("    {:?}: {} ({} bytes)", component, path.file_name().unwrap().to_string_lossy(), file_size);
                }
                
                // Show TOC.txt contents if available
                if let Ok(toc_components) = directory.parse_toc(generation) {
                    println!("  TOC.txt components: {:?}", toc_components);
                }
                
                // Check for Statistics.db and show summary
                if let Some(data_path) = generation.components.get(&cqlite_core::storage::sstable::directory::SSTableComponent::Data) {
                    if let Some(stats_path) = find_statistics_file(data_path).await {
                        let config = cqlite_core::Config::default();
                        let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
                        match StatisticsReader::open(&stats_path, platform).await {
                            Ok(stats_reader) => {
                                println!("  📊 Statistics: {}", stats_reader.compact_summary());
                            }
                            Err(_) => {
                                println!("  📊 Statistics.db found but parsing failed");
                            }
                        }
                    }
                }
            }
            println!();
        }
        
        // Display version information
        if let Some(version) = detected_version.as_ref() {
            println!("Detected version: {}", version);
        }
        if let Some(version) = validated_version.as_ref() {
            println!("Cassandra compatibility: {}", version.to_string());
        }
        
        return Ok(());
    }

    // File mode - original single file logic
    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(sstable_path, &config, platform.clone())
        .await
        .with_context(|| {
            create_version_error(
                &format!("Failed to open SSTable: {}", sstable_path.display()),
                detected_version.as_deref(),
                cassandra_version.as_deref()
            )
        })?;

    let stats = reader.stats().await?;
    let file_size = std::fs::metadata(sstable_path)
        .with_context(|| format!("Failed to get file metadata: {}", sstable_path.display()))?
        .len();

    println!("SSTable Information");
    println!("==================");
    println!("File: {}", sstable_path.display());
    println!("Size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1_048_576.0);

    // Display version information
    if let Some(version) = detected_version.as_ref() {
        println!("Detected version: {}", version);
    }
    if let Some(version) = validated_version.as_ref() {
        println!("Cassandra compatibility: {}", version.to_string());
    }

    println!("Entry count: {}", stats.entry_count);
    println!("Table count: {}", stats.table_count);
    println!("Block count: {}", stats.block_count);
    println!("Index size: {} bytes", stats.index_size);
    println!("Bloom filter size: {} bytes", stats.bloom_filter_size);
    println!("Compression ratio: {:.2}%", stats.compression_ratio * 100.0);
    println!("Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);

    // Try to load and display Statistics.db information
    if let Some(stats_path) = find_statistics_file(sstable_path).await {
        println!("\n📊 Statistics.db Analysis");
        println!("=========================");
        
        match StatisticsReader::open(&stats_path, platform.clone()).await {
            Ok(stats_reader) => {
                println!("Statistics file: {}", stats_path.display());
                println!("{}", stats_reader.compact_summary());
                
                if detailed {
                    println!("\n{}", stats_reader.generate_report(true));
                }
            }
            Err(e) => {
                println!("⚠️  Failed to parse Statistics.db: {}", e);
            }
        }
    } else {
        println!("\n📊 No Statistics.db file found for enhanced analysis");
    }

    // Provide version-specific information
    match detected_version.as_deref() {
        Some("3.11") => {
            println!("Format features: Legacy SSTable format, basic compression");
        }
        Some("4.0") => {
            println!("Format features: Enhanced metadata, improved compression, streaming support");
        }
        Some("5.0") => {
            println!("Format features: Advanced indexing, optimized I/O, native compression");
        }
        Some("unknown") | None => {
            println!("Format features: Unknown (try --auto-detect for version detection)");
        }
        _ => {}
    }

    Ok(())
}

/// Export SSTable data to file
pub async fn export_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    output_path: &Path,
    format: ExportFormat,
) -> Result<()> {
    // Load schema with auto-detection
    let schema = load_schema_file(schema_path, None, None)?;

    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(sstable_path, &config, platform)
        .await
        .with_context(|| format!("Failed to open SSTable: {}", sstable_path.display()))?;

    let mut output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;

    println!("Exporting SSTable: {}", sstable_path.display());
    println!("Output: {} ({})", output_path.display(), format);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} rows exported")
            .unwrap(),
    );

    match format {
        ExportFormat::Json => export_as_json(&reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Csv => export_as_csv(&reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Parquet => {
            anyhow::bail!("Parquet export not yet implemented");
        }
        ExportFormat::Sql => export_as_sql(&reader, &schema, &mut output_file, &pb).await,
    }
}

// Helper functions for different display formats
async fn display_as_table(
    reader: &SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let mut table = Table::new();

    // Add header row
    let mut header = Row::empty();
    for column in &schema.columns {
        header.add_cell(Cell::new(&column.name));
    }
    table.add_row(header);

    let entries = reader.get_all_entries().await?;
    let mut processed = 0;
    let mut displayed = 0;

    for (_table_id, _key, value) in entries {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let mut row = Row::empty();
        
        // Since we don't have parsed column values, we'll display the raw value
        // This is a simplified implementation - in a real scenario, you'd parse the value
        // according to the schema
        for (i, column) in schema.columns.iter().enumerate() {
            let cell_value = if i == 0 {
                format!("{:?}", value) // Display the raw value for now
            } else {
                "NULL".to_string()
            };
            row.add_cell(Cell::new(&cell_value));
        }

        table.add_row(row);
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    table.printstd();

    Ok(())
}

async fn display_as_json(
    reader: &SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    // Simplified implementation for now
    let entries = reader.get_all_entries().await?;
    let mut rows = Vec::new();
    let mut processed = 0;
    let mut displayed = 0;

    for (_table_id, key, value) in entries {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let mut row = serde_json::Map::new();
        row.insert("key".to_string(), serde_json::Value::String(format!("{:?}", key)));
        row.insert("value".to_string(), serde_json::Value::String(format!("{:?}", value)));

        rows.push(serde_json::Value::Object(row));
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    println!("{}", serde_json::to_string_pretty(&rows)?);

    Ok(())
}

async fn display_as_csv(
    reader: &SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    // Write header
    wtr.write_record(&["key", "value"])?;

    let entries = reader.get_all_entries().await?;
    let mut processed = 0;
    let mut displayed = 0;

    for (_table_id, key, value) in entries {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let record = vec![format!("{:?}", key), format!("{:?}", value)];
        wtr.write_record(&record)?;
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    wtr.flush()?;

    Ok(())
}

async fn display_as_yaml(
    reader: &SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    // Simplified implementation for now
    let entries = reader.get_all_entries().await?;
    let mut rows = Vec::new();
    let mut processed = 0;
    let mut displayed = 0;

    for (_table_id, key, value) in entries {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let mut row = serde_json::Map::new();
        row.insert("key".to_string(), serde_json::Value::String(format!("{:?}", key)));
        row.insert("value".to_string(), serde_json::Value::String(format!("{:?}", value)));

        rows.push(serde_json::Value::Object(row));
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    println!("{}", serde_yaml::to_string(&rows)?);

    Ok(())
}

// Export helper functions - simplified stubs for now
async fn export_as_json(
    _reader: &SSTableReader,
    _schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    writeln!(output, "[]")?; // Empty JSON array for now
    pb.finish_with_message("Export functionality not yet implemented".to_string());
    Ok(())
}

async fn export_as_csv(
    _reader: &SSTableReader,
    _schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    writeln!(output, "key,value")?; // Empty CSV for now
    pb.finish_with_message("Export functionality not yet implemented".to_string());
    Ok(())
}

async fn export_as_sql(
    _reader: &SSTableReader,
    schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    writeln!(output, "-- SQL export for table: {}.{}", schema.keyspace, schema.table)?;
    writeln!(output, "-- Export functionality not yet implemented")?;
    pb.finish_with_message("Export functionality not yet implemented".to_string());
    Ok(())
}

/// Load schema file with auto-detection of format (.json or .cql)
fn load_schema_file(schema_path: &Path, detected_version: Option<&str>, cassandra_version: Option<&str>) -> Result<TableSchema> {
    let extension = schema_path.extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_lowercase();

    eprintln!("DEBUG: Schema file path: {}", schema_path.display());
    eprintln!("DEBUG: Detected extension: '{}'", extension);

    match extension.as_str() {
        "json" => {
            eprintln!("DEBUG: Using JSON parser");
            load_json_schema(schema_path, detected_version, cassandra_version)
        },
        "cql" | "sql" => {
            eprintln!("DEBUG: Using CQL parser");
            load_cql_schema(schema_path, detected_version, cassandra_version)
        },
        _ => {
            // Try to auto-detect based on content
            let content = std::fs::read_to_string(schema_path)
                .with_context(|| {
                    create_version_error(
                        &format!("Failed to read schema file: {}", schema_path.display()),
                        detected_version,
                        cassandra_version
                    )
                })?;
            
            if content.trim_start().starts_with('{') {
                info!("Auto-detected JSON format for schema file");
                load_json_schema(schema_path, detected_version, cassandra_version)
            } else if content.to_uppercase().contains("CREATE TABLE") {
                info!("Auto-detected CQL DDL format for schema file");
                load_cql_schema(schema_path, detected_version, cassandra_version)
            } else {
                Err(anyhow::anyhow!(create_version_error(
                    &format!("Unable to determine schema file format: {}. Supported formats: .json (JSON schema) or .cql/.sql (CQL DDL)", schema_path.display()),
                    detected_version,
                    cassandra_version
                )))
            }
        }
    }
}

/// Load JSON schema file
fn load_json_schema(schema_path: &Path, detected_version: Option<&str>, cassandra_version: Option<&str>) -> Result<TableSchema> {
    let schema_content = std::fs::read_to_string(schema_path)
        .with_context(|| {
            create_version_error(
                &format!("Failed to read JSON schema file: {}", schema_path.display()),
                detected_version,
                cassandra_version
            )
        })?;

    let schema: TableSchema = serde_json::from_str(&schema_content)
        .with_context(|| {
            create_version_error(
                "Failed to parse schema JSON",
                detected_version,
                cassandra_version
            )
        })?;

    Ok(schema)
}

/// Load CQL DDL schema file
fn load_cql_schema(schema_path: &Path, detected_version: Option<&str>, cassandra_version: Option<&str>) -> Result<TableSchema> {
    let cql_content = std::fs::read_to_string(schema_path)
        .with_context(|| {
            create_version_error(
                &format!("Failed to read CQL schema file: {}", schema_path.display()),
                detected_version,
                cassandra_version
            )
        })?;

    parse_cql_schema(&cql_content)
        .with_context(|| {
            create_version_error(
                "Failed to parse CQL DDL",
                detected_version,
                cassandra_version
            )
        })
}

