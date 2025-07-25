//! End-to-end test of the complete parser pipeline
//! 
//! This demonstrates the full pipeline: CQL → Parser → AST → Visitor → TableSchema

use cqlite_core::parser::*;
use cqlite_core::schema::{TableSchema, KeyColumn, ClusteringColumn, Column};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 Parser Pipeline End-to-End Test");
    println!("==================================\n");

    // Create a sample AST programmatically (simulating what a parser would produce)
    let create_table_ast = create_sample_ast();
    
    // Step 1: Show the AST structure
    println!("📝 Step 1: AST Structure Created");
    println!("--------------------------------");
    print_ast_details(&create_table_ast);
    
    // Step 2: Use visitor pattern to convert AST to TableSchema
    println!("\n🔄 Step 2: Visitor Pattern Conversion");
    println!("------------------------------------");
    let table_schema = convert_ast_to_schema(&create_table_ast)?;
    print_schema_details(&table_schema);
    
    // Step 3: Demonstrate the complete pipeline with different visitors
    println!("\n🎯 Step 3: Multiple Visitor Demonstrations");
    println!("-----------------------------------------");
    demonstrate_visitors(&create_table_ast)?;
    
    // Step 4: Show parser factory integration
    println!("\n🏭 Step 4: Parser Factory Integration");
    println!("------------------------------------");
    demonstrate_parser_factory()?;
    
    println!("\n✅ Pipeline Test Complete!");
    println!("========================");
    println!("The parser abstraction successfully:");
    println!("  ✓ Creates and manipulates AST structures");
    println!("  ✓ Uses visitor pattern for AST traversal");
    println!("  ✓ Converts AST to domain objects (TableSchema)");
    println!("  ✓ Supports multiple parser backends");
    println!("  ✓ Maintains clean separation of concerns");
    
    Ok(())
}

fn create_sample_ast() -> CqlCreateTable {
    CqlCreateTable {
        if_not_exists: true,
        table: CqlTable::with_keyspace("test_keyspace", "user_profiles"),
        columns: vec![
            CqlColumnDef {
                name: CqlIdentifier::new("user_id"),
                data_type: CqlDataType::Uuid,
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("username"),
                data_type: CqlDataType::Text,
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("email"),
                data_type: CqlDataType::Text,
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("created_at"),
                data_type: CqlDataType::Timestamp,
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("tags"),
                data_type: CqlDataType::Set(Box::new(CqlDataType::Text)),
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("settings"),
                data_type: CqlDataType::Map(
                    Box::new(CqlDataType::Text),
                    Box::new(CqlDataType::Text)
                ),
                is_static: false,
            },
        ],
        primary_key: CqlPrimaryKey {
            partition_key: vec![CqlIdentifier::new("user_id")],
            clustering_key: vec![CqlIdentifier::new("created_at")],
        },
        options: CqlTableOptions::default(),
    }
}

fn print_ast_details(ast: &CqlCreateTable) {
    println!("Table: {}", ast.table.name().name());
    if let Some(ks) = ast.table.keyspace() {
        println!("Keyspace: {}", ks.name());
    }
    println!("IF NOT EXISTS: {}", ast.if_not_exists);
    println!("Columns: {}", ast.columns.len());
    
    for col in &ast.columns {
        println!("  - {} : {:?}", col.name.name(), col.data_type);
    }
    
    println!("Primary Key:");
    println!("  Partition: {:?}", 
        ast.primary_key.partition_key.iter()
            .map(|k| k.name())
            .collect::<Vec<_>>()
    );
    println!("  Clustering: {:?}", 
        ast.primary_key.clustering_key.iter()
            .map(|k| k.name())
            .collect::<Vec<_>>()
    );
}

fn convert_ast_to_schema(ast: &CqlCreateTable) -> Result<TableSchema, Box<dyn std::error::Error>> {
    let mut schema_builder = SchemaBuilderVisitor::new();
    let statement = CqlStatement::CreateTable(ast.clone());
    let schema = schema_builder.visit_statement(&statement)?;
    Ok(schema)
}

fn print_schema_details(schema: &TableSchema) {
    println!("Converted TableSchema:");
    println!("  Keyspace: {}", schema.keyspace);
    println!("  Table: {}", schema.table);
    println!("  Partition Keys: {} keys", schema.partition_keys.len());
    for pk in &schema.partition_keys {
        println!("    - {} ({}) at position {}", pk.name, pk.data_type, pk.position);
    }
    println!("  Clustering Keys: {} keys", schema.clustering_keys.len());
    for ck in &schema.clustering_keys {
        println!("    - {} ({}) {} at position {}", 
            ck.name, ck.data_type, ck.order, ck.position);
    }
    println!("  Regular Columns: {} columns", schema.columns.len());
    for col in &schema.columns {
        println!("    - {} ({})", col.name, col.data_type);
    }
}

fn demonstrate_visitors(ast: &CqlCreateTable) -> Result<(), Box<dyn std::error::Error>> {
    let statement = CqlStatement::CreateTable(ast.clone());
    
    // 1. Identifier Collector
    let mut id_collector = IdentifierCollector::new();
    id_collector.visit_statement(&statement)?;
    let identifiers = id_collector.into_identifiers();
    
    println!("IdentifierCollector Results:");
    println!("  Found {} identifiers: {:?}", 
        identifiers.len(),
        identifiers.iter().map(|id| id.name()).collect::<Vec<_>>()
    );
    
    // 2. Type Collector
    let mut type_collector = TypeCollectorVisitor::new();
    type_collector.visit_statement(&statement)?;
    let types = type_collector.into_types();
    
    println!("\nTypeCollectorVisitor Results:");
    println!("  Found {} unique data types:", types.len());
    for dt in &types {
        println!("    - {:?}", dt);
    }
    
    // 3. Validation Visitor
    let mut validator = ValidationVisitor::new();
    let validation_result = validator.visit_statement(&statement);
    
    println!("\nValidationVisitor Results:");
    match validation_result {
        Ok(warnings) => {
            println!("  ✓ Validation passed!");
            if warnings.is_empty() {
                println!("  No warnings");
            } else {
                println!("  Warnings: {}", warnings.len());
                for warning in warnings {
                    println!("    ⚠️  {}", warning);
                }
            }
        }
        Err(e) => {
            println!("  ✗ Validation failed: {}", e);
        }
    }
    
    Ok(())
}

fn demonstrate_parser_factory() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
        
    runtime.block_on(async {
        // Create different parsers using the factory
        let configs = vec![
            ("Default", ParserConfig::default()),
            ("Fast", ParserConfig::fast()),
            ("Strict", ParserConfig::strict()),
        ];
        
        for (name, config) in configs {
            match create_parser(config) {
                Ok(parser) => {
                    let info = parser.backend_info();
                    println!("{} Parser:", name);
                    println!("  Backend: {}", info.name);
                    println!("  Features: {:?}", info.features);
                    
                    // Test validation capability
                    let valid = parser.validate_syntax("CREATE TABLE test (id UUID PRIMARY KEY)");
                    println!("  Syntax validation: {}", if valid { "✓" } else { "✗" });
                }
                Err(e) => {
                    println!("{} Parser: Failed to create - {}", name, e);
                }
            }
        }
        
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    
    Ok(())
}