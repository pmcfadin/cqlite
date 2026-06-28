//! Data-directory resolution and scanning for the interactive REPL.
//!
//! These helpers walk a Cassandra-style data directory (`keyspace/table-uuid/`)
//! to enumerate keyspaces/tables and gather per-table SSTable statistics when
//! the system catalog is unavailable.

use super::TableInfo;
use anyhow::Result;
use colored::Colorize;
use std::collections::HashMap;
use std::path::Path;

pub(super) async fn scan_data_directory(data_dir: &Path, keyspace_filter: Option<&str>) -> Result<Vec<String>> {
    use std::fs;

    if !data_dir.exists() {
        return Err(anyhow::anyhow!("Data directory does not exist: {}", data_dir.display()));
    }

    let mut tables = Vec::new();

    // If keyspace filter is provided, scan only that keyspace directory
    if let Some(keyspace) = keyspace_filter {
        let keyspace_dir = data_dir.join(keyspace);
        if keyspace_dir.exists() {
            tables.extend(scan_keyspace_directory(&keyspace_dir, keyspace).await?);
        }
    } else {
        // Scan all keyspace directories
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let keyspace_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                // Skip common non-keyspace directories
                if keyspace_name.starts_with('.') || keyspace_name == "system" {
                    continue;
                }

                tables.extend(scan_keyspace_directory(&path, keyspace_name).await?);
            }
        }
    }

    Ok(tables)
}

async fn scan_keyspace_directory(keyspace_dir: &Path, keyspace_name: &str) -> Result<Vec<String>> {
    use std::fs;

    let mut tables = Vec::new();

    for entry in fs::read_dir(keyspace_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let dir_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            // SSTable directories usually follow pattern: tablename-uuid
            if let Some(table_name) = extract_table_name_from_directory(dir_name) {
                let qualified_name = format!("{}.{}", keyspace_name, table_name);
                if !tables.contains(&qualified_name) {
                    tables.push(qualified_name);
                }
            }
        }
    }

    Ok(tables)
}

fn extract_table_name_from_directory(dir_name: &str) -> Option<String> {
    // Expected format: tablename-uuid
    if let Some(dash_pos) = dir_name.find('-') {
        let table_part = &dir_name[..dash_pos];
        if !table_part.is_empty() && table_part.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(table_part.to_string());
        }
    }
    None
}

pub(super) fn display_table_list(tables: &[String], current_keyspace: Option<&str>) {
    let mut tables_by_keyspace: HashMap<String, Vec<String>> = HashMap::new();

    for table in tables {
        if let Some(dot_pos) = table.find('.') {
            let keyspace = &table[..dot_pos];
            let table_name = &table[dot_pos + 1..];
            tables_by_keyspace.entry(keyspace.to_string()).or_insert_with(Vec::new).push(table_name.to_string());
        }
    }

    for (keyspace, table_list) in tables_by_keyspace {
        let is_current = Some(keyspace.as_str()) == current_keyspace;
        let keyspace_display = if is_current {
            format!("📦 {} (current)", keyspace.green().bold())
        } else {
            format!("📦 {}", keyspace.yellow())
        };

        println!("\n{}", keyspace_display);
        for table in table_list {
            let indicator = if is_current { "→" } else { " " };
            println!("  {} 📄 {}", indicator, table.green());
        }
    }
}

pub(super) async fn scan_keyspaces_from_directory(data_dir: &Path) -> Result<Vec<String>> {
    use std::fs;

    let mut keyspaces = Vec::new();

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if !name.starts_with('.') && name != "system" {
                    keyspaces.push(name.to_string());
                }
            }
        }
    }

    keyspaces.sort();
    Ok(keyspaces)
}

pub(super) async fn find_table_in_directory(data_dir: &Path, keyspace: &str, table: &str) -> Result<Option<TableInfo>> {
    use std::fs;

    let keyspace_dir = data_dir.join(keyspace);
    if !keyspace_dir.exists() {
        return Ok(None);
    }

    for entry in fs::read_dir(&keyspace_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let dir_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            if let Some(extracted_table) = extract_table_name_from_directory(dir_name) {
                if extracted_table == table {
                    // Count SSTable files and calculate total size
                    let mut sstable_count = 0;
                    let mut total_size = 0u64;

                    for file_entry in fs::read_dir(&path)? {
                        let file_entry = file_entry?;
                        let file_path = file_entry.path();

                        if file_path.is_file() {
                            if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                                if file_name.contains("Data.db") {
                                    sstable_count += 1;
                                }
                            }

                            if let Ok(metadata) = file_entry.metadata() {
                                total_size += metadata.len();
                            }
                        }
                    }

                    return Ok(Some(TableInfo {
                        path: path.display().to_string(),
                        sstable_count,
                        total_size_mb: total_size as f64 / 1_048_576.0,
                    }));
                }
            }
        }
    }

    Ok(None)
}
