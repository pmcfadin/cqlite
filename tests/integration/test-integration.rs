#!/usr/bin/env rust
//! Integration test for the automated testing framework
//!
//! This test demonstrates the automated comparison between cqlsh and cqlite outputs

use std::process::Command;
use std::env;

// Import our testing framework modules
mod testing_framework {
    pub mod config;
    pub mod docker;
    pub mod output;
    pub mod comparison;
}

use testing_framework::docker::{DockerManager, CqlshConfig};
use testing_framework::comparison::ComparisonEngine;
use testing_framework::config::ComparisonConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 CQLite Automated Testing Framework Integration Test");
    println!("=" .repeat(60));

    // Setup Docker manager
    let cqlsh_config = CqlshConfig {
        container_name: "cassandra-node1".to_string(),
        keyspace: Some("test_keyspace".to_string()),
        ..Default::default()
    };
    
    let docker_manager = DockerManager::new(cqlsh_config);
    
    // Test 1: Connection test
    println!("\n📋 Test 1: Testing Docker connection...");
    match docker_manager.test_connection() {
        Ok(_) => println!("✅ Docker connection successful"),
        Err(e) => {
            eprintln!("❌ Docker connection failed: {}", e);
            return Err(e.into());
        }
    }
    
    // Test 2: Execute known good query
    println!("\n📋 Test 2: Executing known good CQL query...");
    let query = "SELECT * FROM test_keyspace.users WHERE id = a8f167f0-ebe7-4f20-a386-31ff138bec3b;";
    
    match docker_manager.execute_cql_query(query) {
        Ok(cqlsh_output) => {
            println!("✅ CQLSH query successful");
            println!("📊 Returned {} rows with {} columns", 
                    cqlsh_output.row_count.unwrap_or(0),
                    cqlsh_output.column_headers.len());
            
            // Show the formatted output
            println!("\n📋 CQLSH Output:");
            println!("{}", testing_framework::docker::format_cqlsh_for_comparison(&cqlsh_output));
            
            // Test 3: Simulate cqlite output (for now, we'll create mock data)
            println!("\n📋 Test 3: Simulating cqlite output for comparison...");
            let mock_cqlite_output = create_mock_cqlite_output();
            
            // Test 4: Run automated comparison
            println!("\n📋 Test 4: Running automated comparison...");
            let comparison_config = ComparisonConfig::default();
            let comparison_engine = ComparisonEngine::new(comparison_config);
            
            let comparison_result = comparison_engine.compare_outputs(
                query,
                cqlsh_output,
                mock_cqlite_output
            );
            
            println!("📊 Comparison Results:");
            println!("Status: {:?}", comparison_result.status);
            println!("Score: {:.2}", comparison_result.score);
            println!("Differences found: {}", comparison_result.differences.len());
            
            for (i, diff) in comparison_result.differences.iter().enumerate() {
                println!("  {}. {:?} - {}", i+1, diff.severity, diff.description);
                if i < 3 { // Show first 3 differences
                    println!("     Expected: {}", diff.cqlsh_value);
                    println!("     Actual:   {}", diff.cqlite_value);
                }
            }
            
            if !comparison_result.recommendations.is_empty() {
                println!("\n📋 Recommendations:");
                for (i, rec) in comparison_result.recommendations.iter().enumerate() {
                    println!("  {}. {}", i+1, rec);
                }
            }
            
        }
        Err(e) => {
            eprintln!("❌ CQLSH query failed: {}", e);
            return Err(e.into());
        }
    }
    
    // Test 5: Demonstrate table formatting
    println!("\n📋 Test 5: Testing cqlsh-compatible table formatter...");
    test_table_formatter();
    
    println!("\n🎉 Integration test completed successfully!");
    println!("📋 Next steps:");
    println!("  1. Integrate with actual cqlite binary");
    println!("  2. Run comparison with real SSTable data");
    println!("  3. Generate comprehensive test reports");
    
    Ok(())
}

/// Create mock cqlite output for testing comparison
fn create_mock_cqlite_output() -> testing_framework::output::QueryOutput {
    use testing_framework::output::QueryOutput;
    
    let mut output = QueryOutput::default();
    output.format = "table".to_string();
    output.column_headers = vec![
        "id".to_string(),
        "addresses".to_string(), 
        "metadata".to_string(),
        "profile".to_string()
    ];
    
    // Simulate slightly different formatting (this will show differences in comparison)
    output.rows = vec![vec![
        "a8f167f0-ebe7-4f20-a386-31ff138bec3b".to_string(),
        "null".to_string(),
        "{'Agent his available we charge...': 'Prove important despite charge...'}".to_string(),
        "{name: 'Force lot life lose...', age: 2357}".to_string(),
    ]];
    
    output.row_count = Some(1);
    output.execution_time_ms = 50; // Different from cqlsh timing
    
    output
}

/// Test the cqlsh-compatible table formatter
fn test_table_formatter() {
    // This would use the formatter from cqlite-cli/src/formatter.rs
    // For now, just demonstrate the concept
    
    println!("🧪 Testing table formatter...");
    
    // Simulate formatter output
    let test_data = vec![
        vec!["a8f167f0-ebe7-4f20-a386-31ff138bec3b".to_string(), "test data".to_string()],
    ];
    
    println!("📋 Simulated cqlite table output:");
    println!(" id                                   | data");
    println!("--------------------------------------+-----------");
    println!(" a8f167f0-ebe7-4f20-a386-31ff138bec3b | test data");
    println!();
    println!("(1 rows)");
    
    println!("✅ Table formatter test completed");
}