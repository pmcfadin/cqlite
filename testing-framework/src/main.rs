//! Main testing framework application
//!
//! Automated testing framework for comparing cqlsh and cqlite outputs

use std::env;
use std::process;
use clap::{Arg, Command};

mod config;
mod docker;
mod output;
mod comparison;

use docker::{DockerManager, CqlshConfig};

fn main() {
    let matches = Command::new("CQLite Testing Framework")
        .version("1.0.0")
        .about("Automated testing framework for comparing cqlsh and cqlite outputs")
        .arg(Arg::new("container")
            .short('c')
            .long("container")
            .value_name("NAME")
            .help("Docker container name")
            .default_value("cassandra-node1"))
        .arg(Arg::new("keyspace")
            .short('k')
            .long("keyspace")
            .value_name("KEYSPACE")
            .help("Cassandra keyspace to use")
            .default_value("cycling"))
        .arg(Arg::new("table")
            .short('t')
            .long("table")
            .value_name("TABLE")
            .help("Table name to test")
            .default_value("birthday_list"))
        .arg(Arg::new("verbose")
            .short('v')
            .long("verbose")
            .help("Enable verbose output")
            .action(clap::ArgAction::SetTrue))
        .subcommand(
            Command::new("test-connection")
                .about("Test connection to Cassandra container")
        )
        .subcommand(
            Command::new("run-comparison")
                .about("Run automated comparison between cqlsh and cqlite")
        )
        .subcommand(
            Command::new("analyze-sstables")
                .about("Analyze SSTable files and compare with cqlsh output")
                .arg(Arg::new("sstable-path")
                    .long("sstable-path")
                    .value_name("PATH")
                    .help("Path to SSTable directory")
                    .required(true))
        )
        .get_matches();

    // Configure logging based on verbosity
    if matches.get_flag("verbose") {
        env::set_var("RUST_LOG", "debug");
    } else {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    // Setup configuration
    let container_name = matches.get_one::<String>("container").unwrap();
    let keyspace = matches.get_one::<String>("keyspace").unwrap();
    let table = matches.get_one::<String>("table").unwrap();

    let cqlsh_config = CqlshConfig {
        container_name: container_name.to_string(),
        keyspace: Some(keyspace.to_string()),
        ..Default::default()
    };

    let docker_config = crate::config::DockerConfig {
        container_name: container_name.to_string(),
        keyspace: Some(keyspace.to_string()),
        ..Default::default()
    };
    let docker_manager = DockerManager::new(docker_config);

    match matches.subcommand() {
        Some(("test-connection", _)) => {
            println!("🔍 Testing connection to Cassandra container...");
            match docker_manager.test_connection() {
                Ok(_) => {
                    println!("✅ Connection test successful!");
                    process::exit(0);
                }
                Err(e) => {
                    eprintln!("❌ Connection test failed: {}", e);
                    process::exit(1);
                }
            }
        }
        
        Some(("run-comparison", _)) => {
            println!("🚀 Running automated comparison between cqlsh and cqlite...");
            
            if let Err(e) = run_automated_comparison(&docker_manager, keyspace, table) {
                eprintln!("❌ Comparison failed: {}", e);
                process::exit(1);
            }
        }
        
        Some(("analyze-sstables", sub_matches)) => {
            let sstable_path = sub_matches.get_one::<String>("sstable-path").unwrap();
            println!("🔍 Analyzing SSTable files at: {}", sstable_path);
            
            if let Err(e) = analyze_sstables_with_comparison(&docker_manager, keyspace, table, sstable_path) {
                eprintln!("❌ SSTable analysis failed: {}", e);
                process::exit(1);
            }
        }
        
        None | Some((_, _)) => {
            println!("📋 CQLite Testing Framework");
            println!("Use --help to see available commands");
            
            // Default: run basic connection test
            match docker_manager.test_connection() {
                Ok(_) => println!("✅ Basic connection test successful"),
                Err(e) => println!("❌ Connection issue: {}", e),
            }
        }
    }
}

/// Run automated comparison between cqlsh and cqlite
fn run_automated_comparison(docker_manager: &DockerManager, keyspace: &str, table: &str) -> Result<(), String> {
    println!("🔧 Executing test queries on cqlsh...");
    
    // Execute test queries using cqlsh
    let cqlsh_results = docker_manager.execute_test_queries(keyspace, table)?;
    
    println!("📊 CQLSH Results:");
    for (query, output) in &cqlsh_results {
        println!("\n📝 Query: {}", query);
        if output.has_error() {
            println!("❌ Error: {}", output.stderr);
        } else {
            println!("✅ Success: {} rows, {} columns", 
                output.row_count.unwrap_or(0), 
                output.column_headers.len());
            
            // Show formatted output
            let formatted = docker::format_cqlsh_for_comparison(output);
            println!("📋 Output:\n{}", formatted);
        }
    }
    
    // TODO: Run same queries using cqlite and compare
    println!("\n🔄 Next: Implement cqlite execution and comparison");
    println!("📋 This will require:");
    println!("  1. Execute same queries using cqlite");
    println!("  2. Parse cqlite output");
    println!("  3. Run automated comparison");
    println!("  4. Generate detailed report");
    
    Ok(())
}

/// Analyze SSTable files and compare with cqlsh output
fn analyze_sstables_with_comparison(
    docker_manager: &DockerManager, 
    keyspace: &str, 
    table: &str, 
    sstable_path: &str
) -> Result<(), String> {
    println!("🔍 Analyzing SSTable files at: {}", sstable_path);
    
    // First, get the expected output from cqlsh
    println!("📋 Getting expected output from cqlsh...");
    let query = format!("SELECT * FROM {}.{} LIMIT 10;", keyspace, table);
    let cqlsh_output = docker_manager.execute_cql_query(&query)?;
    
    if cqlsh_output.has_error() {
        return Err(format!("CQLSH query failed: {}", cqlsh_output.stderr));
    }
    
    println!("✅ CQLSH returned {} rows", cqlsh_output.row_count.unwrap_or(0));
    
    // Show expected format
    let formatted_cqlsh = docker::format_cqlsh_for_comparison(&cqlsh_output);
    println!("📋 Expected format:\n{}", formatted_cqlsh);
    
    // TODO: Read SSTable files using cqlite and compare
    println!("\n🔄 Next: Read SSTable files using cqlite");
    println!("📋 This will require:");
    println!("  1. Use cqlite to read SSTable files at {}", sstable_path);
    println!("  2. Format output using cqlsh-compatible formatter");
    println!("  3. Compare with expected output");
    println!("  4. Report differences and recommendations");
    
    Ok(())
}

/// Test the new table formatter
#[allow(dead_code)]
fn test_table_formatter() {
    println!("🧪 Testing cqlsh-compatible table formatter...");
    println!("📋 Table formatter functionality will be implemented later");
}