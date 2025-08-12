//! Test basic parser functionality

use cqlite_core::parser::parse_cql_schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Testing CQLite Parser Abstraction");
    println!("=====================================");

    // Test basic CREATE TABLE parsing
    let cql = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, age INT)";
    println!("\n✅ Testing CQL: {}", cql);

    match parse_cql_schema(cql) {
        Ok((remaining, schema)) => {
            println!("✓ Parse successful!");
            println!("  Remaining input: '{}'", remaining);
            println!("  Table: {}.{}", schema.keyspace, schema.table);
            println!("  Columns: {}", schema.columns.len());
            println!("  Partition Keys: {}", schema.partition_keys.len());

            for pk in &schema.partition_keys {
                println!("    - {} ({})", pk.name, pk.data_type);
            }

            for col in &schema.columns {
                println!("    Column: {} -> {}", col.name, col.data_type);
            }
        }
        Err(e) => {
            println!("✗ Parse failed: {:?}", e);
            return Err(format!("Parse error: {:?}", e).into());
        }
    }

    println!("\n🎉 Basic parser test completed successfully!");
    Ok(())
}
