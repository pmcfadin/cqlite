/// Test file for Docker cqlsh integration
use cqlite_core::docker::{DockerCqlshClient, DockerContainer};
use cqlite_core::testing::{CassandraTestRunner, TestSuiteResult};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🐳 CQLite Docker Integration Test");
    println!("=================================");

    // Test 1: Find Cassandra container
    println!("\n1. Finding Cassandra container...");
    match DockerCqlshClient::find_cassandra_container() {
        Ok(container) => {
            println!("✅ Found container:");
            println!("   ID: {}", container.id);
            println!("   Name: {}", container.name);
            println!("   Image: {}", container.image);
            
            // Test 2: Create client and check readiness
            println!("\n2. Testing container readiness...");
            let client = DockerCqlshClient::new(container);
            
            match client.wait_until_ready(30) {
                Ok(()) => {
                    println!("✅ Cassandra container is ready!");
                    
                    // Test 3: Execute basic query
                    println!("\n3. Testing basic query execution...");
                    match client.execute_cql("SELECT cluster_name, release_version FROM system.local;") {
                        Ok(output) => {
                            println!("✅ Query executed successfully:");
                            println!("{}", output);
                            
                            // Test 4: Parse cqlsh output
                            println!("\n4. Testing output parsing...");
                            let parsed = DockerCqlshClient::parse_cqlsh_output(&output);
                            println!("✅ Parsed output:");
                            println!("   Headers: {:?}", parsed.headers);
                            println!("   Rows: {} rows", parsed.rows.len());
                            for (i, row) in parsed.rows.iter().enumerate() {
                                println!("   Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => {
                            println!("❌ Query failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Container not ready: {}", e);
                    return Err(e.into());
                }
            }
        }
        Err(e) => {
            println!("❌ No Cassandra container found: {}", e);
            println!("💡 To start a container, run:");
            println!("   docker run --name cassandra-node1 -d -p 9042:9042 cassandra:5.0");
            return Err(e.into());
        }
    }

    // Test 5: Run test suite
    println!("\n5. Running comprehensive test suite...");
    match CassandraTestRunner::new() {
        Ok(runner) => {
            let test_queries = vec![
                "SELECT COUNT(*) FROM users;",
                "SELECT name, email FROM users WHERE age > 25 ALLOW FILTERING;",
                "SELECT * FROM users LIMIT 2;",
            ];
            
            match runner.run_test_suite(test_queries) {
                Ok(results) => {
                    print_test_results(&results);
                }
                Err(e) => {
                    println!("❌ Test suite failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to create test runner: {}", e);
        }
    }

    println!("\n🎉 Docker integration test complete!");
    Ok(())
}

fn print_test_results(results: &TestSuiteResult) {
    println!("📊 Test Suite Results:");
    println!("   Total tests: {}", results.total_tests);
    println!("   Passed: {} ✅", results.passed);
    println!("   Failed: {} ❌", results.failed);
    println!("   Success rate: {:.1}%", 
        (results.passed as f64 / results.total_tests as f64) * 100.0);
    
    if results.failed > 0 {
        println!("\n❌ Failed tests:");
        for result in &results.results {
            if !result.success {
                println!("   • {}", result.query);
                if let Some(error) = &result.error {
                    println!("     Error: {}", error);
                }
            }
        }
    }
    
    println!("\n✅ Successful tests:");
    for result in &results.results {
        if result.success {
            println!("   • {}", result.query);
            if let Some(output) = &result.output {
                println!("     Rows: {}", output.rows.len());
            }
        }
    }
}