//! Data import command handler.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]

#[cfg(feature = "state_machine")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "state_machine")]
use cqlite_core::Database;
#[cfg(feature = "state_machine")]
use std::fs::File;
use std::path::Path;

#[cfg(feature = "state_machine")]
pub async fn import_data(
    database: &Database,
    file: &Path,
    format: crate::cli::ImportFormat,
    table: Option<&str>,
    quiet: bool,
) -> Result<()> {
    use crate::cli::ImportFormat;
    use std::io::IsTerminal;

    // Issue #1506 / #284: progress + status chatter is suppressed under --quiet
    // and when stdout is not a TTY. Genuine warnings/errors are still surfaced.
    let show_progress = !quiet && std::io::stdout().is_terminal();

    if show_progress {
        println!("Importing data from: {}", file.display());
        println!("Format: {format}, Target table: {table:?}");
    }

    // Validate input file exists
    if !file.exists() {
        return Err(anyhow::anyhow!("Import file not found: {}", file.display()));
    }

    // Determine target table
    let target_table = match table {
        Some(t) => t.to_string(),
        None => {
            // Try to infer table name from filename
            file.file_stem()
                .and_then(|stem| stem.to_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Could not determine target table name. Please specify --table option."
                    )
                })?
        }
    };

    // Try to validate target table exists, but don't fail if we can't verify
    let table_check_query =
        format!("SELECT table_name FROM system.tables WHERE table_name = '{target_table}'");
    match database.execute(&table_check_query).await {
        Ok(result) if result.rows.is_empty() => {
            if show_progress {
                println!(
                    "⚠️  Warning: Table '{}' not found in system catalog. Assuming it exists or will be created during import.",
                    target_table
                );
            }
        }
        Ok(_) => {
            if show_progress {
                println!("✓ Target table '{target_table}' found");
            }
        }
        Err(_) => {
            if show_progress {
                println!(
                    "⚠️  Warning: Could not verify table existence (system tables may not be implemented). Proceeding with import..."
                );
            }
        }
    }

    // Get table schema for validation
    let table_columns = get_table_columns(database, &target_table)
        .await
        .unwrap_or_else(|_| {
            if show_progress {
                println!("⚠️  Warning: Could not retrieve table schema. Import may fail if column types don't match.");
            }
            Vec::new()
        });

    let mut _imported_rows = 0;
    let error_count = 0;

    match format {
        ImportFormat::Csv => {
            _imported_rows =
                import_csv_data(database, file, &target_table, &table_columns, show_progress)
                    .await?;
        }
        ImportFormat::Json => {
            _imported_rows =
                import_json_data(database, file, &target_table, &table_columns, show_progress)
                    .await?;
        }
        ImportFormat::Parquet => {
            return Err(anyhow::anyhow!(
                "Parquet import not yet implemented. Please convert to CSV or JSON format first."
            ));
        }
    }

    if show_progress {
        println!("\n📊 Import Summary:");
        println!("  Rows imported: {_imported_rows}");
        if error_count > 0 {
            println!("  Errors: {error_count}");
        }
        println!("  ✅ Import completed successfully!");
    }

    Ok(())
}

#[cfg(not(feature = "state_machine"))]
pub async fn import_data(
    _database: &cqlite_core::Database,
    _file: &Path,
    _format: crate::cli::ImportFormat,
    _table: Option<&str>,
    _quiet: bool,
) -> Result<()> {
    Err(anyhow::anyhow!(
        "Data import is not available in M1.\n\
         Build with --features state_machine to enable this feature.\n\
         See CLAUDE.md for M1 API examples."
    ))
}

/// Import CSV data into the specified table
#[cfg(feature = "state_machine")]
async fn import_csv_data(
    database: &Database,
    file: &Path,
    table: &str,
    table_columns: &[String],
    show_progress: bool,
) -> Result<u64> {
    use csv::ReaderBuilder;

    let file_handle =
        File::open(file).with_context(|| format!("Failed to open CSV file: {}", file.display()))?;

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file_handle);

    // Get headers from CSV
    let headers = csv_reader
        .headers()
        .with_context(|| "Failed to read CSV headers")?;
    let csv_columns: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    if show_progress {
        println!("📋 CSV columns: {}", csv_columns.join(", "));
        if !table_columns.is_empty() {
            println!("📋 Table columns: {}", table_columns.join(", "));
        }
    }

    // Count total rows for progress
    let total_rows = csv_reader.records().count() as u64;

    // Reopen file for actual processing
    let file_handle = File::open(file)?;
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file_handle);

    let pb = make_bar(
        total_rows,
        "Importing CSV [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} rows ({eta})",
        show_progress,
    );

    let mut imported_count = 0;
    let mut batch_statements = Vec::new();
    let batch_size = 100; // Process in batches for better performance

    for (row_num, record_result) in csv_reader.records().enumerate() {
        pb.set_position(row_num as u64 + 1);

        let record = record_result
            .with_context(|| format!("Failed to parse CSV record at line {}", row_num + 2))?;

        // Create INSERT statement
        let values: Vec<String> = record
            .iter()
            .map(|field| {
                if field.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("'{}'", field.replace("'", "''")) // Escape single quotes
                }
            })
            .collect();

        let insert_stmt = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            csv_columns.join(", "),
            values.join(", ")
        );

        batch_statements.push(insert_stmt);

        // Execute batch when it reaches the batch size
        if batch_statements.len() >= batch_size {
            execute_batch_statements(database, &mut batch_statements, &mut imported_count).await?;
        }
    }

    // Execute remaining statements
    if !batch_statements.is_empty() {
        execute_batch_statements(database, &mut batch_statements, &mut imported_count).await?;
    }

    pb.finish_with_message(format!("Imported {imported_count} rows from CSV"));
    Ok(imported_count)
}

/// Import JSON data into the specified table
#[cfg(feature = "state_machine")]
async fn import_json_data(
    database: &Database,
    file: &Path,
    table: &str,
    _table_columns: &[String],
    show_progress: bool,
) -> Result<u64> {
    use std::fs;

    let file_content = fs::read_to_string(file)
        .with_context(|| format!("Failed to read JSON file: {}", file.display()))?;

    // Try to parse as array of objects or single object
    let json_data: serde_json::Value =
        serde_json::from_str(&file_content).with_context(|| "Failed to parse JSON file")?;

    let objects = match json_data {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(_) => vec![json_data],
        _ => {
            return Err(anyhow::anyhow!(
                "JSON file must contain an object or array of objects"
            ));
        }
    };

    if show_progress {
        println!("📋 Found {} JSON objects to import", objects.len());
    }

    let pb = make_bar(
        objects.len() as u64,
        "Importing JSON [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} objects ({eta})",
        show_progress,
    );

    let mut imported_count = 0;
    let mut batch_statements = Vec::new();
    let batch_size = 50;

    for (index, obj) in objects.iter().enumerate() {
        pb.set_position(index as u64 + 1);

        if let serde_json::Value::Object(map) = obj {
            let columns: Vec<String> = map.keys().cloned().collect();
            let values: Vec<String> = map
                .values()
                .map(|v| match v {
                    serde_json::Value::Null => "NULL".to_string(),
                    serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => format!("'{}'", v.to_string().replace("'", "''")),
                })
                .collect();

            let insert_stmt = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table,
                columns.join(", "),
                values.join(", ")
            );

            batch_statements.push(insert_stmt);

            if batch_statements.len() >= batch_size {
                execute_batch_statements(database, &mut batch_statements, &mut imported_count)
                    .await?;
            }
        } else {
            println!("⚠️  Skipping non-object JSON element at index {index}");
        }
    }

    // Execute remaining statements
    if !batch_statements.is_empty() {
        execute_batch_statements(database, &mut batch_statements, &mut imported_count).await?;
    }

    pb.finish_with_message(format!("Imported {imported_count} objects from JSON"));
    Ok(imported_count)
}

/// Build an import progress bar honoring the #284 quiet/tty contract.
///
/// Returns a hidden bar when `show` is false (quiet mode or non-TTY). The
/// `ProgressStyle::template(...)` result is handled without `unwrap()`/`expect()`:
/// on a template error we fall back to indicatif's default bar style so the
/// import still runs (the bar is cosmetic).
#[cfg(feature = "state_machine")]
fn make_bar(total: u64, template: &str, show: bool) -> indicatif::ProgressBar {
    use indicatif::{ProgressBar, ProgressStyle};

    if !show {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new(total);
    if let Ok(style) = ProgressStyle::default_bar().template(template) {
        pb.set_style(style.progress_chars("=>-"));
    }
    pb
}

/// Execute a batch of INSERT statements
#[cfg(feature = "state_machine")]
async fn execute_batch_statements(
    database: &Database,
    statements: &mut Vec<String>,
    imported_count: &mut u64,
) -> Result<()> {
    for statement in statements.drain(..) {
        match database.execute(&statement).await {
            Ok(_) => {
                *imported_count += 1;
            }
            Err(e) => {
                println!("⚠️  Error executing statement: {e}");
                println!(
                    "   Statement: {}",
                    statement.chars().take(100).collect::<String>() + "..."
                );
                // Continue with next statement rather than failing completely
            }
        }
    }
    Ok(())
}

/// Get table columns for schema validation
#[cfg(feature = "state_machine")]
async fn get_table_columns(database: &Database, table: &str) -> Result<Vec<String>> {
    let query = format!("SELECT column_name FROM system.columns WHERE table_name = '{table}'");
    match database.execute(&query).await {
        Ok(result) => {
            let columns = result
                .rows
                .iter()
                .filter_map(|row| row.get("column_name"))
                .map(|col| col.to_string())
                .collect();
            Ok(columns)
        }
        Err(e) => Err(anyhow::anyhow!("Failed to get table columns: {}", e)),
    }
}
