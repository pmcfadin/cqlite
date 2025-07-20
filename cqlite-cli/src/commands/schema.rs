use crate::SchemaCommands;
use anyhow::{Context, Result};
use cqlite_core::schema::TableSchema;
use serde_json;
use std::path::Path;

pub async fn handle_schema_command(db_path: &Path, command: SchemaCommands) -> Result<()> {
    match command {
        SchemaCommands::List => list_tables(db_path).await,
        SchemaCommands::Describe { table } => describe_table(db_path, &table).await,
        SchemaCommands::Create { file } => create_table_from_file(db_path, &file).await,
        SchemaCommands::Drop { table, force } => drop_table(db_path, &table, force).await,
        SchemaCommands::Validate { json } => validate_schema(&json).await,
    }
}

async fn list_tables(db_path: &Path) -> Result<()> {
    // TODO: Implement table listing
    println!("Tables in database: {}", db_path.display());
    println!("- users");
    println!("- orders");
    println!("- products");

    Ok(())
}

async fn describe_table(db_path: &Path, table: &str) -> Result<()> {
    // TODO: Implement table description
    println!(
        "Describing table '{}' in database: {}",
        table,
        db_path.display()
    );
    println!("Columns:");
    println!("- id: UUID (primary key)");
    println!("- name: TEXT");
    println!("- created_at: TIMESTAMP");

    Ok(())
}

async fn create_table_from_file(db_path: &Path, file: &Path) -> Result<()> {
    // TODO: Implement table creation from DDL file
    println!("Creating table from DDL file: {}", file.display());
    println!("Target database: {}", db_path.display());

    Ok(())
}

async fn drop_table(db_path: &Path, table: &str, force: bool) -> Result<()> {
    // TODO: Implement table dropping
    if force {
        println!(
            "Force dropping table '{}' from database: {}",
            table,
            db_path.display()
        );
    } else {
        println!("Are you sure you want to drop table '{}'? (y/N)", table);
        // TODO: Add confirmation logic
    }

    Ok(())
}

async fn validate_schema(json_path: &Path) -> Result<()> {
    println!("Validating schema: {}", json_path.display());

    // Read the JSON file
    let schema_content = std::fs::read_to_string(json_path)
        .with_context(|| format!("Failed to read schema file: {}", json_path.display()))?;

    // Try to parse it as a TableSchema
    match serde_json::from_str::<TableSchema>(&schema_content) {
        Ok(schema) => {
            println!("✅ Schema validation successful!");
            println!("Table: {}", schema.table_name());
            println!("Columns: {}", schema.columns().len());

            // Show column details
            for (i, column) in schema.columns().iter().enumerate() {
                println!("  {}. {} ({:?})", i + 1, column.name, column.data_type);
            }

            if let Some(key_columns) = schema.primary_key() {
                println!("Primary key: {}", key_columns.join(", "));
            }
        }
        Err(e) => {
            println!("❌ Schema validation failed!");
            println!("Error: {}", e);

            // Try to provide helpful error messages
            if e.to_string().contains("missing field") {
                println!("\nHint: Make sure all required fields are present:");
                println!("- table_name");
                println!("- columns (array)");
                println!("- primary_key (optional array)");
            } else if e.to_string().contains("unknown variant") {
                println!("\nHint: Check that all data types are valid CQL types");
            }

            return Err(e.into());
        }
    }

    Ok(())
}
