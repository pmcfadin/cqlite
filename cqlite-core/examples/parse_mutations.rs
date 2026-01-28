//! Example: Parsing CQL Mutation Statements (INSERT, UPDATE, DELETE)
//!
//! This example demonstrates how to use the CQL parser to parse mutation statements.
//! Requires the `write-support` feature to be enabled.
//!
//! Run with:
//! ```bash
//! cargo run --package cqlite-core --example parse_mutations --features write-support
//! ```

#[cfg(feature = "write-support")]
fn main() {
    use cqlite_core::cql::{ParserBackend, ParserConfig, ParserFactory, CqlStatement};

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // Create Nom parser (ANTLR doesn't support mutations yet)
        let config = ParserConfig::default().with_backend(ParserBackend::Nom);
        let parser = ParserFactory::create(config).expect("Failed to create parser");

        // Parse INSERT statement
        println!("=== INSERT Statement ===");
        let insert_cql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?) USING TTL 3600";
        match parser.parse(insert_cql).await {
            Ok(CqlStatement::Insert(insert)) => {
                println!("✓ Parsed INSERT statement:");
                println!("  Table: {}", insert.table.name.name);
                println!("  Columns: {:?}", insert.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
                println!("  IF NOT EXISTS: {}", insert.if_not_exists);
                println!("  Has TTL: {}", insert.using.as_ref().and_then(|u| u.ttl.as_ref()).is_some());
                println!();
            }
            Ok(_) => println!("✗ Expected INSERT statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse UPDATE statement
        println!("=== UPDATE Statement ===");
        let update_cql = "UPDATE users USING TIMESTAMP 12345 SET name = ?, email = ? WHERE id = ?";
        match parser.parse(update_cql).await {
            Ok(CqlStatement::Update(update)) => {
                println!("✓ Parsed UPDATE statement:");
                println!("  Table: {}", update.table.name.name);
                println!("  Assignments: {}", update.assignments.len());
                for (i, assignment) in update.assignments.iter().enumerate() {
                    println!("    {}. {} {:?} <value>", i + 1, assignment.column.name, assignment.operator);
                }
                println!("  Has TIMESTAMP: {}", update.using.as_ref().and_then(|u| u.timestamp.as_ref()).is_some());
                println!();
            }
            Ok(_) => println!("✗ Expected UPDATE statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse DELETE statement (entire row)
        println!("=== DELETE Statement (entire row) ===");
        let delete_cql = "DELETE FROM users WHERE id = ?";
        match parser.parse(delete_cql).await {
            Ok(CqlStatement::Delete(delete)) => {
                println!("✓ Parsed DELETE statement:");
                println!("  Table: {}", delete.table.name.name);
                println!("  Columns: {}", if delete.columns.is_empty() {
                    "entire row".to_string()
                } else {
                    format!("{:?}", delete.columns.iter().map(|c| &c.name).collect::<Vec<_>>())
                });
                println!();
            }
            Ok(_) => println!("✗ Expected DELETE statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse DELETE statement (specific columns)
        println!("=== DELETE Statement (specific columns) ===");
        let delete_columns_cql = "DELETE name, email FROM users WHERE id = ?";
        match parser.parse(delete_columns_cql).await {
            Ok(CqlStatement::Delete(delete)) => {
                println!("✓ Parsed DELETE statement:");
                println!("  Table: {}", delete.table.name.name);
                println!("  Columns: {:?}", delete.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
                println!();
            }
            Ok(_) => println!("✗ Expected DELETE statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse INSERT with literals
        println!("=== INSERT with Literal Values ===");
        let insert_literals = "INSERT INTO users (id, name, age, active) VALUES (123, 'Alice', 30, true)";
        match parser.parse(insert_literals).await {
            Ok(CqlStatement::Insert(insert)) => {
                println!("✓ Parsed INSERT with literals:");
                println!("  Table: {}", insert.table.name.name);
                println!("  Columns: {:?}", insert.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
                println!();
            }
            Ok(_) => println!("✗ Expected INSERT statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse UPDATE with compound WHERE
        println!("=== UPDATE with Compound WHERE ===");
        let update_compound = "UPDATE users SET name = ? WHERE id = ? AND age > 18";
        match parser.parse(update_compound).await {
            Ok(CqlStatement::Update(update)) => {
                println!("✓ Parsed UPDATE with compound WHERE:");
                println!("  Table: {}", update.table.name.name);
                println!("  Has compound WHERE clause");
                println!();
            }
            Ok(_) => println!("✗ Expected UPDATE statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        // Parse UPDATE with counter operations
        println!("=== UPDATE with Counter Operations ===");
        let update_counter = "UPDATE counters SET count += 1 WHERE id = ?";
        match parser.parse(update_counter).await {
            Ok(CqlStatement::Update(update)) => {
                println!("✓ Parsed UPDATE with counter operation:");
                println!("  Table: {}", update.table.name.name);
                println!("  Operation: {:?}", update.assignments[0].operator);
                println!();
            }
            Ok(_) => println!("✗ Expected UPDATE statement"),
            Err(e) => println!("✗ Parse error: {}", e),
        }

        println!("All examples completed successfully!");
    });
}

#[cfg(not(feature = "write-support"))]
fn main() {
    eprintln!("This example requires the 'write-support' feature to be enabled.");
    eprintln!("Run with: cargo run --package cqlite-core --example parse_mutations --features write-support");
    std::process::exit(1);
}
