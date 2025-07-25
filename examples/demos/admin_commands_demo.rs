#!/usr/bin/env rust
//! Admin Commands Demo for CQLite REPL
//! 
//! This demonstrates the enhanced admin commands functionality:
//! - .tables command with real database integration
//! - .schema [table] command with detailed schema display 
//! - .info command with comprehensive database statistics
//! 
//! This is a standalone demo showing the implementation that was added to interactive.rs

use colored::*;
use prettytable::{Cell, Row, Table};
use std::collections::HashMap;

// Mock structures for demonstration
#[derive(Debug, Clone)]
struct TableSchema {
    keyspace: String,
    table: String,
    columns: Vec<Column>,
    partition_keys: Vec<KeyColumn>,
    clustering_keys: Vec<ClusteringColumn>,
    comments: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct KeyColumn {
    name: String,
    data_type: String,
    position: usize,
}

#[derive(Debug, Clone)]
struct ClusteringColumn {
    name: String,
    data_type: String,
    position: usize,
    order: String,
}

impl TableSchema {
    fn is_partition_key(&self, name: &str) -> bool {
        self.partition_keys.iter().any(|k| k.name == name)
    }
    
    fn is_clustering_key(&self, name: &str) -> bool {
        self.clustering_keys.iter().any(|k| k.name == name)
    }
}

fn main() {
    println!("{}", "CQLite Admin Commands Demo".cyan().bold());
    println!("=================================\n");
    
    // Demo data
    let tables = create_demo_tables();
    
    // Demo .tables command
    println!("{}", "Demo: .tables command".green().bold());
    println!("---------------------");
    demo_tables_command(&tables);
    
    println!("\n{}", "Demo: .schema command (all tables)".green().bold());  
    println!("-----------------------------------");
    demo_schema_all_command(&tables);
    
    println!("\n{}", r#"Demo: .schema users command"#.green().bold());
    println!("---------------------------");
    demo_schema_specific_command(&tables, "users");
    
    println!("\n{}", "Demo: .info command".green().bold());
    println!("-------------------");
    demo_info_command();
    
    println!("\n{}", "Integration Status:".yellow().bold());
    println!("• ✅ Enhanced .tables command with structured output");
    println!("• ✅ Enhanced .schema [table] command with table-specific display");  
    println!("• ✅ Enhanced .info command with comprehensive statistics");
    println!("• ✅ Proper error handling for missing tables/schema");
    println!("• ✅ Integration with SchemaManager and StorageEngine patterns");
    println!("• ✅ Consistent colored output and user-friendly formatting");
}

fn create_demo_tables() -> Vec<TableSchema> {
    vec![
        TableSchema {
            keyspace: "ecommerce".to_string(),
            table: "users".to_string(),
            columns: vec![
                Column { name: "id".to_string(), data_type: "uuid".to_string(), nullable: false },
                Column { name: "email".to_string(), data_type: "text".to_string(), nullable: false },
                Column { name: "name".to_string(), data_type: "text".to_string(), nullable: true },
                Column { name: "created_at".to_string(), data_type: "timestamp".to_string(), nullable: false },
                Column { name: "last_login".to_string(), data_type: "timestamp".to_string(), nullable: true },
            ],
            partition_keys: vec![
                KeyColumn { name: "id".to_string(), data_type: "uuid".to_string(), position: 0 },
            ],
            clustering_keys: vec![],
            comments: HashMap::from([
                ("table".to_string(), "User account information".to_string()),
            ]),
        },
        TableSchema {
            keyspace: "ecommerce".to_string(),
            table: "orders".to_string(),
            columns: vec![
                Column { name: "id".to_string(), data_type: "uuid".to_string(), nullable: false },
                Column { name: "user_id".to_string(), data_type: "uuid".to_string(), nullable: false },
                Column { name: "created_at".to_string(), data_type: "timestamp".to_string(), nullable: false },
                Column { name: "total".to_string(), data_type: "decimal".to_string(), nullable: false },
                Column { name: "status".to_string(), data_type: "text".to_string(), nullable: false },
            ],
            partition_keys: vec![
                KeyColumn { name: "user_id".to_string(), data_type: "uuid".to_string(), position: 0 },
            ],
            clustering_keys: vec![
                ClusteringColumn { name: "created_at".to_string(), data_type: "timestamp".to_string(), position: 0, order: "DESC".to_string() },
            ],
            comments: HashMap::new(),
        },
        TableSchema {
            keyspace: "analytics".to_string(),
            table: "events".to_string(),
            columns: vec![
                Column { name: "session_id".to_string(), data_type: "uuid".to_string(), nullable: false },
                Column { name: "timestamp".to_string(), data_type: "timestamp".to_string(), nullable: false },
                Column { name: "event_type".to_string(), data_type: "text".to_string(), nullable: false },
                Column { name: "properties".to_string(), data_type: "map<text, text>".to_string(), nullable: true },
            ],
            partition_keys: vec![
                KeyColumn { name: "session_id".to_string(), data_type: "uuid".to_string(), position: 0 },
            ],
            clustering_keys: vec![
                ClusteringColumn { name: "timestamp".to_string(), data_type: "timestamp".to_string(), position: 0, order: "ASC".to_string() },
            ],
            comments: HashMap::new(),
        },
    ]
}

fn demo_tables_command(tables: &[TableSchema]) {
    println!("{}", "Tables:".cyan().bold());
    
    let mut table = Table::new();
    table.add_row(Row::new(vec![
        Cell::new("Table Name").style_spec("Fc"),
        Cell::new("Keyspace").style_spec("Fc"),
        Cell::new("Columns").style_spec("Fc"),
        Cell::new("Primary Key").style_spec("Fc"),
    ]));
    
    for table_info in tables {
        let pk_info = format_primary_key(table_info);
        table.add_row(Row::new(vec![
            Cell::new(&table_info.table),
            Cell::new(&table_info.keyspace),
            Cell::new(&table_info.columns.len().to_string()),
            Cell::new(&pk_info),
        ]));
    }
    
    table.printstd();
    println!("\n{} {}", "Total:".green(), tables.len().to_string().yellow());
}

fn demo_schema_all_command(tables: &[TableSchema]) {
    println!("{}", "Database Schema:".cyan().bold());
    println!();
    
    let mut current_keyspace = String::new();
    for table_schema in tables {
        if table_schema.keyspace != current_keyspace {
            println!("{}", format!("Keyspace: {}", table_schema.keyspace).yellow().bold());
            current_keyspace = table_schema.keyspace.clone();
        }
        
        display_table_schema(table_schema);
        println!();
    }
}

fn demo_schema_specific_command(tables: &[TableSchema], table_name: &str) {
    println!("{}", "Database Schema:".cyan().bold());
    println!();
    
    if let Some(table_schema) = tables.iter().find(|t| t.table == table_name) {
        println!("{}", format!("Table: {}.{}", table_schema.keyspace, table_schema.table).yellow().bold());
        display_table_schema(table_schema);
    } else {
        println!("{}", format!("Table '{}' not found", table_name).red());
        println!("Use {} to list available tables", ".tables".green());
    }
}

fn demo_info_command() {
    println!("{}", "Database Information:".cyan().bold());
    println!("  Path: {}", "/path/to/cqlite.db".yellow());
    println!("  Version: CQLite {}", "0.1.0".green());
    println!("  Directory size: {:.2} MB", 1.45);
    println!("  Last modified: {}", "2024-01-23 15:30:22");
    
    println!("");
    println!("{}", "Storage Statistics:".cyan().bold());
    println!("  SSTable count: {}", 3);
    println!("  MemTable entries: {}", 1250);
    println!("  MemTable size: {} bytes", 2048576);
    println!("  WAL entries: {}", 45);
    println!("  WAL size: {} bytes", 1024000);
    
    println!("");
    println!("{}", "Memory Statistics:".cyan().bold());
    println!("  Allocated: {} bytes", 8388608);
    println!("  Peak usage: {} bytes", 12582912);
    
    println!("");
    println!("{}", "Query Statistics:".cyan().bold());
    println!("  Total queries: {}", 1542);
    println!("  Cache hits: {}", 1203);
    println!("  Cache misses: {}", 339);
    
    println!("");
    println!("{}", "Schema Information:".cyan().bold());
    println!("  Tables: {}", 3);
    println!("  Table list:");
    println!("    • {}.{} ({} columns)", "ecommerce".green(), "users".yellow(), 5);
    println!("    • {}.{} ({} columns)", "ecommerce".green(), "orders".yellow(), 5);
    println!("    • {}.{} ({} columns)", "analytics".green(), "events".yellow(), 4);
}

fn format_primary_key(table_schema: &TableSchema) -> String {
    let mut parts = Vec::new();
    
    // Add partition keys
    for pk in &table_schema.partition_keys {
        parts.push(pk.name.clone());
    }
    
    // Add clustering keys
    if !table_schema.clustering_keys.is_empty() {
        let clustering: Vec<String> = table_schema.clustering_keys
            .iter()
            .map(|ck| ck.name.clone())
            .collect();
        parts.push(format!("({})", clustering.join(", ")));
    }
    
    if parts.is_empty() {
        "No primary key".to_string()
    } else {
        parts.join(", ")
    }
}

fn display_table_schema(table_schema: &TableSchema) {
    println!("  {}", "Columns:".cyan().bold());
    
    for column in &table_schema.columns {
        let mut constraints = Vec::new();
        
        if table_schema.is_partition_key(&column.name) {
            constraints.push("PARTITION KEY".bright_blue());
        }
        if table_schema.is_clustering_key(&column.name) {
            constraints.push("CLUSTERING KEY".bright_blue());
        }
        if !column.nullable {
            constraints.push("NOT NULL".bright_red());
        }
        
        let constraint_text = if constraints.is_empty() {
            String::new()
        } else {
            format!(" ({})", constraints.join(", "))
        };
        
        println!("    {} {}{}", 
            column.name.green(), 
            column.data_type.cyan(),
            constraint_text
        );
    }
    
    // Show primary key information
    if !table_schema.partition_keys.is_empty() {
        let pk_info = format_primary_key(table_schema);
        println!("  {}: {}", "Primary Key".cyan().bold(), pk_info.yellow());
    }
    
    // Show comments if any
    if !table_schema.comments.is_empty() {
        println!("  {}", "Comments:".cyan().bold());
        for (key, comment) in &table_schema.comments {
            println!("    {}: {}", key.green(), comment);
        }
    }
}