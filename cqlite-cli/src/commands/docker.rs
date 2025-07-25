/// Docker integration commands for cqlite-cli
use clap::{Args, Subcommand};
use cqlite_core::docker::{DockerCqlshClient, DockerContainer};
use cqlite_core::testing::{CassandraTestRunner, TestSuiteResult};
use std::io;

#[derive(Args)]
pub struct DockerArgs {
    #[command(subcommand)]
    pub command: DockerCommand,
}

#[derive(Subcommand)]
pub enum DockerCommand {
    /// List available Cassandra containers
    List,
    /// Connect to Cassandra container and execute CQL
    Connect {
        /// CQL query to execute
        #[arg(short, long)]
        query: Option<String>,
        /// Container name or ID (optional, auto-detects if not provided)
        #[arg(short, long)]
        container: Option<String>,
    },
    /// Run test suite against Cassandra container
    Test {
        /// Test queries file path
        #[arg(short, long)]
        queries: Option<String>,
        /// Run basic test suite
        #[arg(long)]
        basic: bool,
    },
    /// Check if Cassandra container is ready
    Status,
}

pub fn handle_docker_command(args: DockerArgs) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        DockerCommand::List => list_containers(),
        DockerCommand::Connect { query, container } => connect_to_container(query, container),
        DockerCommand::Test { queries, basic } => run_tests(queries, basic),
        DockerCommand::Status => check_status(),
    }
}

fn list_containers() -> Result<(), Box<dyn std::error::Error>> {
    match DockerCqlshClient::find_cassandra_container() {
        Ok(container) => {
            println!("Found Cassandra container:");
            println!("  ID: {}", container.id);
            println!("  Name: {}", container.name);
            println!("  Image: {}", container.image);
        }
        Err(e) => {
            eprintln!("No Cassandra containers found: {}", e);
            return Err(e.into());
        }
    }
    Ok(())
}

fn connect_to_container(query: Option<String>, container: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let docker_container = if let Some(name) = container {
        // Use specified container
        DockerContainer {
            id: name.clone(),
            name: name.clone(),
            image: "cassandra".to_string(),
        }
    } else {
        // Auto-detect container
        DockerCqlshClient::find_cassandra_container()?
    };

    let client = DockerCqlshClient::new(docker_container);

    if let Some(cql) = query {
        // Execute single query
        match client.execute_cql(&cql) {
            Ok(result) => {
                println!("Query result:");
                println!("{}", result);
            }
            Err(e) => {
                eprintln!("Query failed: {}", e);
                return Err(e.into());
            }
        }
    } else {
        // Interactive mode
        println!("Entering interactive CQL mode. Type 'exit' to quit.");
        loop {
            print!("cql> ");
            use std::io::{self, Write};
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();

            if input.is_empty() {
                continue;
            }

            if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
                break;
            }

            match client.execute_cql(input) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }

    Ok(())
}

fn run_tests(queries_file: Option<String>, basic: bool) -> Result<(), Box<dyn std::error::Error>> {
    let runner = CassandraTestRunner::new()?;

    if basic {
        // Basic test queries
        let test_queries = vec![
            "SELECT COUNT(*) FROM users;",
            "SELECT name, email FROM users WHERE age > 25;",
            "SELECT * FROM users LIMIT 2;",
            "SELECT name FROM users ORDER BY age DESC;",
        ];
        
        println!("Running {} test queries...", test_queries.len());
        
        match runner.run_test_suite(test_queries) {
            Ok(results) => {
                print_test_results(&results);
            }
            Err(e) => {
                eprintln!("Test suite failed: {}", e);
                return Err(e.into());
            }
        }
    } else if let Some(file_path) = queries_file {
        // Load queries from file
        use std::fs;
        let content = fs::read_to_string(file_path)?;
        let test_queries: Vec<&str> = content.lines().filter(|line| !line.trim().is_empty()).collect();
        
        println!("Running {} test queries...", test_queries.len());
        
        match runner.run_test_suite(test_queries) {
            Ok(results) => {
                print_test_results(&results);
            }
            Err(e) => {
                eprintln!("Test suite failed: {}", e);
                return Err(e.into());
            }
        }
    } else {
        eprintln!("Please specify either --basic or --queries <file>");
        return Ok(());
    };

    Ok(())
}

fn check_status() -> Result<(), Box<dyn std::error::Error>> {
    let container = DockerCqlshClient::find_cassandra_container()?;
    let client = DockerCqlshClient::new(container.clone());

    println!("Checking Cassandra container status...");
    println!("Container: {} ({})", container.name, container.id);

    if client.is_ready() {
        println!("✅ Cassandra is ready and accepting connections");
        
        // Get some basic info
        match client.execute_cql("SELECT cluster_name, release_version FROM system.local;") {
            Ok(result) => {
                println!("\nCluster information:");
                println!("{}", result);
            }
            Err(e) => {
                println!("⚠️  Could not retrieve cluster info: {}", e);
            }
        }
    } else {
        println!("❌ Cassandra is not ready or not responding");
        return Err("Cassandra not ready".into());
    }

    Ok(())
}

fn print_test_results(results: &TestSuiteResult) {
    println!("\n{}", "=".repeat(50));
    println!("TEST RESULTS");
    println!("{}", "=".repeat(50));
    println!("Total tests: {}", results.total_tests);
    println!("Passed: {} ✅", results.passed);
    println!("Failed: {} ❌", results.failed);
    
    if results.failed > 0 {
        println!("\nFailed tests:");
        for result in &results.results {
            if !result.success {
                println!("  ❌ {}", result.query);
                if let Some(error) = &result.error {
                    println!("     Error: {}", error);
                }
            }
        }
    }

    println!("\nSuccess rate: {:.1}%", 
        (results.passed as f64 / results.total_tests as f64) * 100.0);
}