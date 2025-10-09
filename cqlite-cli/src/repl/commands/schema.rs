//! :schema list meta-command implementation

use anyhow::Result;
use colored::Colorize;
use std::path::PathBuf;

/// Execute the :schema list command
///
/// Displays loaded schema file paths with a simple numbered list.
/// Shows helpful hint when no schemas are loaded.
///
/// # Arguments
///
/// * `schema_paths` - Reference to the list of currently loaded schema file paths
///
/// # Returns
///
/// Returns Ok(()) on success, or an error if display operations fail
pub async fn execute_schema_list(schema_paths: &[PathBuf]) -> Result<()> {
    println!("{}", "=== Loaded Schemas ===".green().bold());
    println!();

    if schema_paths.is_empty() {
        println!("{}", "No schemas loaded".yellow());
        println!(
            "{}",
            "Hint: Use :schema load <path> to load schemas".dimmed()
        );
        return Ok(());
    }

    println!("Schema files ({}):", schema_paths.len());
    for (i, path) in schema_paths.iter().enumerate() {
        println!("  {}. {}", i + 1, path.display().to_string().green());
    }
    println!();

    Ok(())
}
