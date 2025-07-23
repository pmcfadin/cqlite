use crate::SchemaCommands;
use anyhow::{Context, Result};
use cqlite_core::schema::{TableSchema, Column, KeyColumn, ClusteringColumn, parse_cql_schema};
use serde_json;
use std::path::Path;
use std::collections::HashMap;

pub async fn handle_schema_command(db_path: &Path, command: SchemaCommands) -> Result<()> {
    match command {
        SchemaCommands::List => list_tables(db_path).await,
        SchemaCommands::Describe { table } => describe_table(db_path, &table).await,
        SchemaCommands::Create { file } => create_table_from_file(db_path, &file).await,
        SchemaCommands::Drop { table, force } => drop_table(db_path, &table, force).await,
        SchemaCommands::Validate { file } => validate_schema(&file).await,
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

async fn validate_schema(file_path: &Path) -> Result<()> {
    println!("Validating schema: {}", file_path.display());

    // Detect file format based on extension
    let extension = file_path.extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    match extension.to_lowercase().as_str() {
        "json" => validate_json_schema(file_path).await,
        "cql" | "sql" => validate_cql_schema(file_path).await,
        _ => {
            // Try to auto-detect based on content
            let content = std::fs::read_to_string(file_path)
                .with_context(|| format!("Failed to read schema file: {}", file_path.display()))?;
            
            if content.trim_start().starts_with('{') {
                println!("📝 Auto-detected JSON format");
                validate_json_schema(file_path).await
            } else if content.to_uppercase().contains("CREATE TABLE") {
                println!("📝 Auto-detected CQL DDL format");
                validate_cql_schema(file_path).await
            } else {
                println!("❌ Unable to determine file format. Supported formats:");
                println!("  - .json files: JSON schema format");
                println!("  - .cql/.sql files: CQL DDL format");
                println!("\nExample JSON schema:");
                println!("{{\n  \"keyspace\": \"example\",\n  \"table\": \"users\",\n  \"partition_keys\": [{{\"name\": \"id\", \"type\": \"uuid\", \"position\": 0}}],\n  \"clustering_keys\": [],\n  \"columns\": [{{\"name\": \"id\", \"type\": \"uuid\", \"nullable\": false}}]\n}}");
                println!("\nExample CQL DDL:");
                println!("CREATE TABLE example.users (\n  id uuid PRIMARY KEY,\n  name text,\n  email text\n);");
                Err(anyhow::anyhow!("Unsupported file format"))
            }
        }
    }
}

async fn validate_json_schema(json_path: &Path) -> Result<()> {
    // Read the JSON file
    let schema_content = std::fs::read_to_string(json_path)
        .with_context(|| format!("Failed to read JSON schema file: {}", json_path.display()))?;

    // Try to parse it as a TableSchema
    match serde_json::from_str::<TableSchema>(&schema_content) {
        Ok(schema) => {
            println!("✅ JSON Schema validation successful!");
            print_schema_details(&schema);
        }
        Err(e) => {
            println!("❌ JSON Schema validation failed!");
            println!("Error: {}", e);

            // Try to provide helpful error messages
            if e.to_string().contains("missing field") {
                println!("\n💡 Hint: Make sure all required fields are present:");
                println!("- keyspace (string)");
                println!("- table (string)");
                println!("- partition_keys (array)");
                println!("- clustering_keys (array)");
                println!("- columns (array)");
            } else if e.to_string().contains("unknown variant") {
                println!("\n💡 Hint: Check that all data types are valid CQL types");
                println!("Valid types: text, bigint, int, uuid, timestamp, etc.");
            }

            return Err(e.into());
        }
    }

    Ok(())
}

async fn validate_cql_schema(cql_path: &Path) -> Result<()> {
    // Read the CQL file
    let cql_content = std::fs::read_to_string(cql_path)
        .with_context(|| format!("Failed to read CQL schema file: {}", cql_path.display()))?;

    // Parse CQL DDL and convert to TableSchema
    match parse_cql_schema(&cql_content) {
        Ok(schema) => {
            println!("✅ CQL DDL validation successful!");
            print_schema_details(&schema);
        }
        Err(e) => {
            println!("❌ CQL DDL validation failed!");
            println!("Error: {}", e);
            println!("\n💡 Hints for CQL DDL:");
            println!("- Use CREATE TABLE keyspace.table_name syntax");
            println!("- Define PRIMARY KEY explicitly");
            println!("- Use valid CQL data types");
            println!("\nExample:");
            println!("CREATE TABLE example.users (");
            println!("  id uuid PRIMARY KEY,");
            println!("  name text,");
            println!("  created_at timestamp");
            println!(");");
            return Err(e.into());
        }
    }

    Ok(())
}

fn print_schema_details(schema: &TableSchema) {
    println!("📋 Table: {}.{}", schema.keyspace, schema.table);
    println!("📊 Columns: {}", schema.columns.len());

    // Show column details
    for (i, column) in schema.columns.iter().enumerate() {
        let nullable_str = if column.nullable { "nullable" } else { "not null" };
        println!("  {}. {} ({}, {})", i + 1, column.name, column.data_type, nullable_str);
    }

    if !schema.partition_keys.is_empty() {
        let key_names: Vec<String> = schema.partition_keys.iter().map(|k| k.name.clone()).collect();
        println!("🔑 Partition keys: {}", key_names.join(", "));
    }

    if !schema.clustering_keys.is_empty() {
        let clustering_names: Vec<String> = schema.clustering_keys.iter().map(|k| k.name.clone()).collect();
        println!("🔗 Clustering keys: {}", clustering_names.join(", "));
    }
}

/// Parse CQL DDL and convert to TableSchema
fn parse_cql_ddl(cql_content: &str) -> Result<TableSchema> {
    let cql_content = cql_content.trim().to_uppercase();
    
    // Find CREATE TABLE statement
    let create_table_start = cql_content.find("CREATE TABLE")
        .ok_or_else(|| anyhow::anyhow!("No CREATE TABLE statement found"))?;
    
    let table_part = &cql_content[create_table_start + 12..].trim(); // Skip "CREATE TABLE"
    
    // Find the opening parenthesis
    let paren_start = table_part.find('(')
        .ok_or_else(|| anyhow::anyhow!("Missing opening parenthesis in CREATE TABLE"))?;
    
    // Extract table name part
    let table_name_part = &table_part[..paren_start].trim();
    
    // Parse keyspace and table name
    let (keyspace, table_name) = if let Some(dot_pos) = table_name_part.find('.') {
        let keyspace = table_name_part[..dot_pos].trim().to_lowercase();
        let table = table_name_part[dot_pos + 1..].trim().to_lowercase();
        (keyspace, table)
    } else {
        ("default".to_string(), table_name_part.trim().to_lowercase())
    };
    
    // Find the matching closing parenthesis
    let mut paren_depth = 0;
    let mut column_end = paren_start;
    let table_chars: Vec<char> = table_part.chars().collect();
    
    for (i, &ch) in table_chars.iter().enumerate().skip(paren_start) {
        match ch {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    column_end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    
    if paren_depth != 0 {
        return Err(anyhow::anyhow!("Unmatched parentheses in CREATE TABLE"));
    }
    
    // Extract column definitions (between parentheses)
    let column_definitions = &table_part[paren_start + 1..column_end];
    
    // Parse column definitions
    let (columns, partition_keys, clustering_keys) = parse_column_definitions(column_definitions)?;
    
    let schema = TableSchema {
        keyspace,
        table: table_name,
        partition_keys,
        clustering_keys,
        columns,
        comments: HashMap::new(),
    };
    
    // Validate the parsed schema
    schema.validate().with_context(|| "Generated schema validation failed")?;
    
    Ok(schema)
}

/// Parse column definitions from CQL DDL
fn parse_column_definitions(definitions: &str) -> Result<(Vec<Column>, Vec<KeyColumn>, Vec<ClusteringColumn>)> {
    let mut columns = Vec::new();
    let mut partition_keys = Vec::new();
    let mut clustering_keys = Vec::new();
    let mut primary_key_found = false;
    
    // Split by commas, but be careful with nested types like map<text, int>
    let column_parts = split_column_definitions(definitions)?;
    
    for part in column_parts {
        let part = part.trim();
        
        if part.to_uppercase().starts_with("PRIMARY KEY") {
            // Parse PRIMARY KEY (col1, col2, ...)
            parse_primary_key_constraint(part, &columns, &mut partition_keys, &mut clustering_keys)?;
            primary_key_found = true;
        } else {
            // Parse column definition: name type [PRIMARY KEY]
            let column_parts: Vec<&str> = part.split_whitespace().collect();
            if column_parts.len() < 2 {
                return Err(anyhow::anyhow!("Invalid column definition: {}", part));
            }
            
            let column_name = column_parts[0].to_string();
            let column_type = column_parts[1].to_string();
            let is_primary_key = part.to_uppercase().contains("PRIMARY KEY");
            
            let column = Column {
                name: column_name.clone(),
                data_type: column_type.clone(),
                nullable: !is_primary_key, // Primary key columns are not nullable
                default: None,
            };
            
            columns.push(column);
            
            // If this column is marked as PRIMARY KEY, add it as partition key
            if is_primary_key && !primary_key_found {
                partition_keys.push(KeyColumn {
                    name: column_name,
                    data_type: column_type,
                    position: partition_keys.len(),
                });
            }
        }
    }
    
    // If no PRIMARY KEY constraint was found and no inline PRIMARY KEY, 
    // assume first column is the primary key
    if partition_keys.is_empty() && !columns.is_empty() {
        let first_col = &columns[0];
        partition_keys.push(KeyColumn {
            name: first_col.name.clone(),
            data_type: first_col.data_type.clone(),
            position: 0,
        });
        
        // Update the first column to be non-nullable
        if let Some(col) = columns.get_mut(0) {
            col.nullable = false;
        }
    }
    
    Ok((columns, partition_keys, clustering_keys))
}

/// Split column definitions while respecting nested types
fn split_column_definitions(definitions: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current_part = String::new();
    let mut paren_depth = 0;
    let mut angle_depth = 0;
    
    for ch in definitions.chars() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            ',' if paren_depth == 0 && angle_depth == 0 => {
                if !current_part.trim().is_empty() {
                    parts.push(current_part.trim().to_string());
                }
                current_part.clear();
                continue;
            }
            _ => {}
        }
        current_part.push(ch);
    }
    
    if !current_part.trim().is_empty() {
        parts.push(current_part.trim().to_string());
    }
    
    Ok(parts)
}

/// Parse PRIMARY KEY constraint like "PRIMARY KEY (id)" or "PRIMARY KEY ((user_id, tenant_id), created_at)"
fn parse_primary_key_constraint(
    constraint: &str,
    columns: &[Column],
    partition_keys: &mut Vec<KeyColumn>,
    clustering_keys: &mut Vec<ClusteringColumn>,
) -> Result<()> {
    // Find the opening parenthesis after PRIMARY KEY
    let paren_start = constraint.find('(')
        .ok_or_else(|| anyhow::anyhow!("Missing opening parenthesis in PRIMARY KEY"))?;
    
    // Find the matching closing parenthesis
    let mut paren_depth = 0;
    let mut paren_end = paren_start;
    let constraint_chars: Vec<char> = constraint.chars().collect();
    
    for (i, &ch) in constraint_chars.iter().enumerate().skip(paren_start) {
        match ch {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    paren_end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    
    if paren_depth != 0 {
        return Err(anyhow::anyhow!("Unmatched parentheses in PRIMARY KEY"));
    }
    
    // Extract the key specification (inside parentheses)
    let key_spec = &constraint[paren_start + 1..paren_end].trim();
    
    // Check if it's a composite primary key with partition and clustering keys
    // Format: ((partition_key1, partition_key2), clustering_key1, clustering_key2)
    if key_spec.trim_start().starts_with('(') && key_spec.contains("),") {
        // Parse composite key
        parse_composite_primary_key(key_spec, columns, partition_keys, clustering_keys)
    } else {
        // Simple primary key - all columns are partition keys
        let key_names: Vec<&str> = key_spec.split(',').map(|s| s.trim()).collect();
        
        for (position, key_name) in key_names.iter().enumerate() {
            let column = columns.iter()
                .find(|c| c.name == *key_name)
                .ok_or_else(|| anyhow::anyhow!("Primary key column '{}' not found in column definitions", key_name))?;
            
            partition_keys.push(KeyColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                position,
            });
        }
        
        Ok(())
    }
}

/// Parse composite primary key with explicit partition and clustering keys
fn parse_composite_primary_key(
    key_spec: &str,
    columns: &[Column],
    partition_keys: &mut Vec<KeyColumn>,
    clustering_keys: &mut Vec<ClusteringColumn>,
) -> Result<()> {
    // Find the end of the partition key specification
    let mut paren_depth = 0;
    let mut partition_end = 0;
    
    for (i, ch) in key_spec.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    partition_end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    
    if partition_end == 0 {
        return Err(anyhow::anyhow!("Invalid composite primary key format"));
    }
    
    // Extract partition keys (inside the first parentheses)
    let partition_spec = &key_spec[1..partition_end]; // Skip the opening '('
    let partition_names: Vec<&str> = partition_spec.split(',').map(|s| s.trim()).collect();
    
    for (position, key_name) in partition_names.iter().enumerate() {
        let column = columns.iter()
            .find(|c| c.name == *key_name)
            .ok_or_else(|| anyhow::anyhow!("Partition key column '{}' not found", key_name))?;
        
        partition_keys.push(KeyColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            position,
        });
    }
    
    // Extract clustering keys (after the first parentheses)
    let remaining = &key_spec[partition_end + 1..].trim();
    if remaining.starts_with(',') {
        let clustering_spec = &remaining[1..].trim(); // Skip the comma
        let clustering_names: Vec<&str> = clustering_spec.split(',').map(|s| s.trim()).collect();
        
        for (position, key_name) in clustering_names.iter().enumerate() {
            if key_name.is_empty() {
                continue;
            }
            
            let column = columns.iter()
                .find(|c| c.name == *key_name)
                .ok_or_else(|| anyhow::anyhow!("Clustering key column '{}' not found", key_name))?;
            
            clustering_keys.push(ClusteringColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                position,
                order: "ASC".to_string(), // Default to ASC
            });
        }
    }
    
    Ok(())
}
