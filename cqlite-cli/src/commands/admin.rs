use crate::cli_types::AdminCommands;
use anyhow::Result;
#[cfg(feature = "state_machine")]
use chrono;
use cqlite_core::Database;

#[cfg(feature = "state_machine")]
pub async fn handle_admin_command(database: &Database, command: AdminCommands) -> Result<()> {
    match command {
        AdminCommands::Info => show_database_info(database).await,
        AdminCommands::Compact { force: _ } => compact_database(database).await,
        AdminCommands::Backup {
            destination,
            compression: _,
        } => backup_database(database, &destination).await,
        AdminCommands::Restore { backup, force: _ } => restore_database(database, &backup).await,
    }
}

#[cfg(not(feature = "state_machine"))]
pub async fn handle_admin_command(_database: &Database, _command: AdminCommands) -> Result<()> {
    Err(anyhow::anyhow!(
        "Admin commands requiring query execution are not available in M1.\n\
         Build with --features state_machine or use SSTableReader directly.\n\
         See CLAUDE.md for M1 API examples."
    ))
}

#[cfg(feature = "state_machine")]
async fn show_database_info(database: &Database) -> Result<()> {
    println!("Database Information:");

    // Get database statistics
    match database.stats().await {
        Ok(stats) => {
            println!("Storage Engine Stats:");
            println!(
                "  - MemTable size: {} bytes",
                stats.storage_stats.memtable.size_bytes
            );
            println!(
                "  - MemTable entries: {}",
                stats.storage_stats.memtable.entry_count
            );
            println!(
                "  - SSTable count: {}",
                stats.storage_stats.sstables.sstable_count
            );
            println!(
                "  - Total entries: {}",
                stats.storage_stats.sstables.total_entries
            );

            println!("Memory Stats:");
            println!(
                "  - Total memory used: {} bytes",
                stats.memory_stats.total_memory_used
            );
            println!(
                "  - Block cache hits: {}",
                stats.memory_stats.block_cache_hits
            );

            #[cfg(feature = "state_machine")]
            {
                println!("Query Engine Stats:");
                println!("  - Total queries: {}", stats.query_stats.total_queries);
                println!(
                    "  - Average execution time: {} μs",
                    stats.query_stats.avg_execution_time_us
                );
            }
        }
        Err(e) => {
            println!("Failed to get database statistics: {}", e);
        }
    }

    Ok(())
}

#[cfg(feature = "state_machine")]
async fn compact_database(database: &Database) -> Result<()> {
    println!("Starting database compaction...");

    match database.compact().await {
        Ok(_) => {
            println!("Database compaction completed successfully");
        }
        Err(e) => {
            println!("Database compaction failed: {}", e);
            return Err(anyhow::anyhow!("Compaction failed: {}", e));
        }
    }

    Ok(())
}

#[cfg(feature = "state_machine")]
async fn backup_database(database: &Database, output: &std::path::Path) -> Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use serde_json;
    use std::fs::File;
    use std::io::{BufWriter, Write};

    println!("Backing up database to {}", output.display());

    // Create output directory if it doesn't exist
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("Failed to create backup directory: {}", e))?;
    }

    // Create backup file
    let backup_file =
        File::create(output).map_err(|e| anyhow::anyhow!("Failed to create backup file: {}", e))?;
    let mut writer = BufWriter::new(backup_file);

    // Get database statistics for progress indication
    let stats = match database.stats().await {
        Ok(stats) => stats,
        Err(e) => {
            println!("Warning: Could not get database statistics: {}", e);
            // Continue with backup anyway
            return create_basic_backup(database, &mut writer, output).await;
        }
    };

    // Create progress bar
    let total_entries = stats.storage_stats.sstables.total_entries
        + (stats.storage_stats.memtable.entry_count as u64);
    let pb = ProgressBar::new(total_entries);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "Backing up [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} entries ({eta})",
            )
            .unwrap()
            .progress_chars("=>-"),
    );

    // Create backup metadata
    let backup_metadata = serde_json::json!({
        "version": "1.0",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "cqlite_version": env!("CARGO_PKG_VERSION"),
        "total_entries": total_entries,
        "memtable_entries": stats.storage_stats.memtable.entry_count,
        "sstable_entries": stats.storage_stats.sstables.total_entries,
        "sstable_count": stats.storage_stats.sstables.sstable_count
    });

    // Write backup header
    writeln!(writer, "# CQLite Database Backup")?;
    writeln!(writer, "# {}", backup_metadata.to_string())?;
    writeln!(writer)?;

    let mut processed_entries = 0;

    // Export all data by querying system tables and user data
    // Start with schema information
    match database
        .execute(
            "SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system'",
        )
        .await
    {
        Ok(tables_result) => {
            for table_row in &tables_result.rows {
                if let (Some(keyspace), Some(table)) =
                    (table_row.get("keyspace_name"), table_row.get("table_name"))
                {
                    // Export table schema first
                    let create_table_query = format!("DESCRIBE TABLE {}.{}", keyspace, table);
                    match database.execute(&create_table_query).await {
                        Ok(schema_result) => {
                            writeln!(writer, "-- Schema for {}.{}", keyspace, table)?;
                            for schema_row in &schema_result.rows {
                                if let Some(ddl) = schema_row.get("create_statement") {
                                    writeln!(writer, "{}", ddl)?;
                                }
                            }
                            writeln!(writer)?;
                        }
                        Err(_) => {
                            // If DESCRIBE doesn't work, create a basic CREATE TABLE statement
                            writeln!(
                                writer,
                                "-- Table: {}.{} (schema extraction failed)",
                                keyspace, table
                            )?;
                            writeln!(
                                writer,
                                "-- CREATE TABLE {}.{} (...); -- Please recreate manually",
                                keyspace, table
                            )?;
                            writeln!(writer)?;
                        }
                    }

                    // Export table data
                    let select_query = format!("SELECT * FROM {}.{}", keyspace, table);
                    match database.execute(&select_query).await {
                        Ok(data_result) => {
                            writeln!(writer, "-- Data for {}.{}", keyspace, table)?;
                            for data_row in &data_result.rows {
                                // Convert row to INSERT statement
                                let columns: Vec<String> = data_row.column_names();
                                let values: Vec<String> = columns
                                    .iter()
                                    .map(|col| {
                                        data_row
                                            .get(col)
                                            .map(|v| format!("'{}'", v)) // Simple quote escaping
                                            .unwrap_or_else(|| "NULL".to_string())
                                    })
                                    .collect();

                                let insert_stmt = format!(
                                    "INSERT INTO {}.{} ({}) VALUES ({});",
                                    keyspace,
                                    table,
                                    columns.join(", "),
                                    values.join(", ")
                                );
                                writeln!(writer, "{}", insert_stmt)?;

                                processed_entries += 1;
                                pb.set_position(processed_entries);
                            }
                            writeln!(writer)?;
                        }
                        Err(e) => {
                            writeln!(
                                writer,
                                "-- Error exporting data for {}.{}: {}",
                                keyspace, table, e
                            )?;
                        }
                    }
                }
            }
        }
        Err(_) => {
            println!("Warning: Could not access system tables, creating basic backup");
            return create_basic_backup(database, &mut writer, output).await;
        }
    }

    pb.finish_with_message("Backup completed");
    writer.flush()?;

    let file_size = std::fs::metadata(output)?.len();
    println!("✓ Database backup completed successfully");
    println!("  Backup file: {}", output.display());
    println!("  Entries exported: {}", processed_entries);
    println!("  File size: {:.2} MB", file_size as f64 / 1_048_576.0);

    Ok(())
}

#[cfg(feature = "state_machine")]
async fn create_basic_backup(
    _database: &Database,
    writer: &mut std::io::BufWriter<std::fs::File>,
    output: &std::path::Path,
) -> Result<()> {
    use std::io::Write;

    // Basic backup without system table access
    writeln!(writer, "# CQLite Database Backup (Basic Mode)")?;
    writeln!(writer, "# Timestamp: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(writer, "# Version: {}", env!("CARGO_PKG_VERSION"))?;
    writeln!(writer)?;
    writeln!(
        writer,
        "-- Note: This is a basic backup. Full schema and data export requires system table access."
    )?;
    writeln!(
        writer,
        "-- Please use manual CQL queries to restore your data."
    )?;

    writer.flush()?;

    let file_size = std::fs::metadata(output)?.len();
    println!("✓ Basic database backup completed");
    println!("  Backup file: {}", output.display());
    println!("  File size: {} bytes", file_size);
    println!(
        "  Note: This backup contains minimal information. Consider upgrading core library for full backup support."
    );

    Ok(())
}

#[cfg(feature = "state_machine")]
async fn restore_database(database: &Database, input: &std::path::Path) -> Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    println!("Restoring database from {}", input.display());

    // Check if backup file exists
    if !input.exists() {
        return Err(anyhow::anyhow!(
            "Backup file not found: {}",
            input.display()
        ));
    }

    // Open backup file
    let backup_file =
        File::open(input).map_err(|e| anyhow::anyhow!("Failed to open backup file: {}", e))?;
    let reader = BufReader::new(backup_file);

    // Count total lines for progress
    let total_lines = reader.lines().count() as u64;

    // Reopen file for actual processing
    let backup_file = File::open(input)?;
    let reader = BufReader::new(backup_file);

    // Create progress bar
    let pb = ProgressBar::new(total_lines);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "Restoring [{elapsed_precise}] [{bar:40.green/blue}] {pos}/{len} lines ({eta})",
            )
            .unwrap()
            .progress_chars("=>-"),
    );

    let mut executed_statements = 0;
    let mut errors = 0;
    let mut line_number = 0;
    let mut current_statement = String::new();

    // Parse backup metadata if present
    let mut backup_version: Option<String> = None;
    let mut backup_timestamp: Option<String> = None;

    for line_result in reader.lines() {
        line_number += 1;
        pb.set_position(line_number);

        let line = line_result
            .map_err(|e| anyhow::anyhow!("Error reading line {}: {}", line_number, e))?;

        let trimmed_line = line.trim();

        // Skip empty lines and comments, but extract metadata
        if trimmed_line.is_empty() {
            continue;
        }

        if trimmed_line.starts_with('#') {
            // Extract metadata from comments
            if trimmed_line.contains("\"version\":") {
                // Try to parse JSON metadata
                if let Some(start) = trimmed_line.find('{') {
                    let json_str = &trimmed_line[start..];
                    if let Ok(metadata) = serde_json::from_str::<serde_json::Value>(json_str) {
                        backup_version = metadata["version"].as_str().map(|s| s.to_string());
                        backup_timestamp = metadata["timestamp"].as_str().map(|s| s.to_string());
                    }
                }
            }
            continue;
        }

        if trimmed_line.starts_with("--") {
            continue;
        }

        // Handle multi-line statements
        current_statement.push_str(&line);
        current_statement.push(' ');

        // Check if statement is complete (ends with semicolon)
        if trimmed_line.ends_with(';') {
            let statement = current_statement.trim().trim_end_matches(';');

            if !statement.is_empty() {
                // Execute the statement
                match database.execute(statement).await {
                    Ok(_result) => {
                        executed_statements += 1;
                        if line_number % 100 == 0 {
                            pb.set_message(format!("Executed {} statements", executed_statements));
                        }

                        // Log successful operations for important statements
                        if statement.to_uppercase().starts_with("CREATE") {
                            println!(
                                "✓ Executed: {}",
                                statement.chars().take(50).collect::<String>() + "..."
                            );
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        eprintln!(
                            "❌ Error executing statement at line {}: {}",
                            line_number, e
                        );
                        eprintln!(
                            "   Statement: {}",
                            statement.chars().take(100).collect::<String>() + "..."
                        );

                        // Continue with restore unless it's a critical error
                        if statement.to_uppercase().contains("CREATE KEYSPACE")
                            || statement.to_uppercase().contains("CREATE TABLE")
                        {
                            println!(
                                "   ⚠️  Schema error - continuing but data integrity may be affected"
                            );
                        }
                    }
                }
            }

            current_statement.clear();
        }
    }

    // Handle any remaining incomplete statement
    if !current_statement.trim().is_empty() {
        let statement = current_statement.trim();
        println!(
            "⚠️  Warning: Incomplete statement at end of file: {}",
            statement.chars().take(50).collect::<String>() + "..."
        );
    }

    pb.finish_with_message("Restore completed");

    // Show restore summary
    println!("\n📊 Restore Summary:");
    if let Some(version) = backup_version {
        println!("  Backup version: {}", version);
    }
    if let Some(timestamp) = backup_timestamp {
        println!("  Backup created: {}", timestamp);
    }
    println!("  Total lines processed: {}", line_number);
    println!("  Statements executed: {}", executed_statements);

    if errors > 0 {
        println!("  ❌ Errors encountered: {}", errors);
        println!("  ⚠️  Database restore completed with errors. Please verify data integrity.");

        // Return error if too many failures
        if errors > executed_statements / 2 {
            return Err(anyhow::anyhow!(
                "Restore failed: too many errors ({} errors out of {} statements)",
                errors,
                executed_statements
            ));
        }
    } else {
        println!("  ✅ Database restore completed successfully!");
    }

    Ok(())
}
