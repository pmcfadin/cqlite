#!/usr/bin/env cargo run --bin compatibility-checker --
//! Automated Cassandra compatibility checker for CQLite
//! 
//! Usage:
//!   compatibility-checker --version 5.1 --test-suite comprehensive
//!   compatibility-checker --matrix --output report.json
//!   compatibility-checker --monitor --interval 24h

use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::Result;
use serde_json;

use cqlite_compatibility::{
    run_compatibility_tests,
    quick_compatibility_check,
    CompatibilityTestSuite,
    CassandraVersionManager,
};

#[derive(Parser)]
#[command(name = "compatibility-checker")]
#[command(about = "Automated Cassandra compatibility checker for CQLite")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Output directory for test results
    #[arg(short, long, default_value = "./compatibility-results")]
    output: PathBuf,
    
    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run compatibility tests for a specific version
    Test {
        /// Cassandra version to test
        #[arg(short, long)]
        version: String,
        
        /// Test suite level
        #[arg(short, long, default_value = "comprehensive")]
        suite: String,
        
        /// Generate detailed report
        #[arg(long)]
        detailed: bool,
    },
    /// Run compatibility matrix across all versions  
    Matrix {
        /// Output format (json, yaml, markdown)
        #[arg(short, long, default_value = "json")]
        format: String,
        
        /// Skip slow tests
        #[arg(long)]
        fast: bool,
    },
    /// Monitor compatibility continuously
    Monitor {
        /// Check interval (1h, 24h, 7d)
        #[arg(short, long, default_value = "24h")]
        interval: String,
        
        /// Webhook URL for notifications
        #[arg(long)]
        webhook: Option<String>,
    },
    /// Detect format changes in SSTable files
    Detect {
        /// Directory containing SSTable files
        #[arg(short, long)]
        sstable_dir: PathBuf,
        
        /// Baseline version for comparison
        #[arg(short, long, default_value = "4.0")]
        baseline: String,
    },
    /// Generate test data for specific version
    Generate {
        /// Cassandra version
        #[arg(short, long)]
        version: String,
        
        /// Number of test rows
        #[arg(short, long, default_value = "10000")]
        rows: usize,
        
        /// Include complex data types
        #[arg(long)]
        complex: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    if cli.verbose {
        env_logger::init();
    }
    
    // Ensure output directory exists
    tokio::fs::create_dir_all(&cli.output).await?;
    
    match cli.command {
        Commands::Test { version, suite, detailed } => {
            println!("🧪 Testing Cassandra version {} with {} test suite", version, suite);
            
            let result = quick_compatibility_check(&version, cli.output.clone()).await?;
            
            println!("✅ Compatibility result: {}% compatible", result.compatibility_score);
            println!("{} Status: {}", result.status_emoji(), 
                if result.is_compatible() { "COMPATIBLE" } else { "ISSUES FOUND" });
            
            if !result.issues.is_empty() {
                println!("⚠️ Issues found:");
                for issue in &result.issues {
                    println!("  - {}", issue);
                }
            }
            
            if detailed {
                let json_result = serde_json::to_string_pretty(&result)?;
                let output_file = cli.output.join(format!("compatibility-{}.json", version));
                tokio::fs::write(output_file, json_result).await?;
                println!("📄 Detailed report saved to compatibility-{}.json", version);
            }
        },
        
        Commands::Matrix { format, fast } => {
            println!("🔄 Running compatibility matrix across all versions");
            
            if fast {
                println!("⚡ Running in fast mode (skipping slow tests)");
            }
            
            let report = run_compatibility_tests(cli.output.clone()).await?;
            
            println!("📊 Compatibility Matrix Results:");
            println!("  Overall compatibility: {:.1}%", report.overall_compatibility);
            println!("  ✅ Fully compatible: {}", report.summary.fully_compatible.len());
            println!("  🟡 Mostly compatible: {}", report.summary.mostly_compatible.len());
            println!("  🟠 Requires updates: {}", report.summary.requires_updates.len());
            println!("  ❌ Incompatible: {}", report.summary.incompatible.len());
            
            // Save report in requested format
            let output_file = match format.as_str() {
                "json" => {
                    let json_report = serde_json::to_string_pretty(&report)?;
                    let file = cli.output.join("compatibility-matrix.json");
                    tokio::fs::write(&file, json_report).await?;
                    file
                },
                "yaml" => {
                    let yaml_report = serde_yaml::to_string(&report)?;
                    let file = cli.output.join("compatibility-matrix.yaml");
                    tokio::fs::write(&file, yaml_report).await?;
                    file
                },
                "markdown" => {
                    let md_report = generate_markdown_report(&report);
                    let file = cli.output.join("compatibility-matrix.md");
                    tokio::fs::write(&file, md_report).await?;
                    file
                },
                _ => return Err(anyhow::anyhow!("Unsupported format: {}", format)),
            };
            
            println!("📄 Report saved to: {}", output_file.display());
        },
        
        Commands::Monitor { interval, webhook } => {
            println!("👁️ Starting continuous compatibility monitoring (interval: {})", interval);
            
            if let Some(webhook_url) = webhook {
                println!("📡 Notifications will be sent to: {}", webhook_url);
            }
            
            // Parse interval
            let duration = parse_interval(&interval)?;
            
            loop {
                println!("🔄 Running compatibility check...");
                
                match run_compatibility_tests(cli.output.clone()).await {
                    Ok(report) => {
                        if report.overall_compatibility < 95.0 {
                            println!("⚠️ Compatibility degradation detected: {:.1}%", 
                                report.overall_compatibility);
                            
                            if let Some(ref webhook_url) = webhook {
                                send_webhook_notification(webhook_url, &report).await?;
                            }
                        } else {
                            println!("✅ All systems compatible ({:.1}%)", report.overall_compatibility);
                        }
                    },
                    Err(e) => {
                        eprintln!("❌ Compatibility check failed: {}", e);
                    }
                }
                
                println!("😴 Sleeping for {}...", interval);
                tokio::time::sleep(duration).await;
            }
        },
        
        Commands::Detect { sstable_dir, baseline } => {
            println!("🔍 Detecting format changes in SSTable files");
            println!("  Directory: {}", sstable_dir.display());
            println!("  Baseline version: {}", baseline);
            
            // Scan for SSTable files
            let mut sstable_files = Vec::new();
            let mut entries = tokio::fs::read_dir(&sstable_dir).await?;
            
            while let Some(entry) = entries.next_entry().await? {
                if let Some(ext) = entry.path().extension() {
                    if ext == "db" {
                        sstable_files.push(entry.path());
                    }
                }
            }
            
            if sstable_files.is_empty() {
                println!("⚠️ No SSTable files found in directory");
                return Ok(());
            }
            
            println!("📁 Found {} SSTable files", sstable_files.len());
            
            let changes = cqlite_compatibility::detect_format_changes(sstable_files).await?;
            
            if changes.is_empty() {
                println!("✅ No format changes detected");
            } else {
                println!("🔍 Format changes detected:");
                for change in changes {
                    println!("  - {}", change);
                }
            }
        },
        
        Commands::Generate { version, rows, complex } => {
            println!("📊 Generating test data for Cassandra version {}", version);
            println!("  Rows: {}", rows);
            println!("  Complex types: {}", if complex { "enabled" } else { "disabled" });
            
            let mut data_generator = cqlite_compatibility::TestDataGenerator::new(cli.output.clone());
            let dataset = data_generator.generate_for_version(&version).await?;
            
            println!("✅ Generated test dataset:");
            println!("  SSTable files: {}", dataset.sstable_files.len());
            println!("  Total rows: {}", dataset.metadata.total_rows);
            println!("  Data size: {} bytes", dataset.metadata.data_size_bytes);
            println!("  Output directory: {}", cli.output.display());
        },
    }
    
    Ok(())
}

fn parse_interval(interval: &str) -> Result<tokio::time::Duration> {
    match interval {
        "1h" => Ok(tokio::time::Duration::from_secs(3600)),
        "24h" => Ok(tokio::time::Duration::from_secs(86400)),
        "7d" => Ok(tokio::time::Duration::from_secs(604800)),
        _ => {
            // Try to parse as seconds
            if let Ok(seconds) = interval.parse::<u64>() {
                Ok(tokio::time::Duration::from_secs(seconds))
            } else {
                Err(anyhow::anyhow!("Invalid interval format: {}", interval))
            }
        }
    }
}

async fn send_webhook_notification(webhook_url: &str, report: &cqlite_compatibility::CompatibilityReport) -> Result<()> {
    let client = reqwest::Client::new();
    
    let notification = serde_json::json!({
        "text": format!("🚨 CQLite Compatibility Alert: {:.1}% compatibility", report.overall_compatibility),
        "attachments": [{
            "color": if report.overall_compatibility >= 95.0 { "good" } else { "warning" },
            "fields": [
                {
                    "title": "Overall Compatibility",
                    "value": format!("{:.1}%", report.overall_compatibility),
                    "short": true
                },
                {
                    "title": "Issues Found",
                    "value": format!("{}", report.breaking_changes.len()),
                    "short": true
                }
            ]
        }]
    });
    
    client.post(webhook_url)
        .json(&notification)
        .send()
        .await?;
    
    Ok(())
}

fn generate_markdown_report(report: &cqlite_compatibility::CompatibilityReport) -> String {
    let mut md = String::new();
    
    md.push_str("# Cassandra Compatibility Report\n\n");
    md.push_str(&format!("**Generated:** {}\n", report.generated_at.format("%Y-%m-%d %H:%M:%S UTC")));
    md.push_str(&format!("**Baseline Version:** {}\n", report.baseline_version));
    md.push_str(&format!("**Overall Compatibility:** {:.1}%\n\n", report.overall_compatibility));
    
    md.push_str("## Summary\n\n");
    md.push_str("| Status | Versions |\n");
    md.push_str("|--------|----------|\n");
    md.push_str(&format!("| ✅ Fully Compatible | {} |\n", report.summary.fully_compatible.join(", ")));
    md.push_str(&format!("| 🟡 Mostly Compatible | {} |\n", report.summary.mostly_compatible.join(", ")));
    md.push_str(&format!("| 🟠 Requires Updates | {} |\n", report.summary.requires_updates.join(", ")));
    md.push_str(&format!("| ❌ Incompatible | {} |\n", report.summary.incompatible.join(", ")));
    
    if !report.breaking_changes.is_empty() {
        md.push_str("\n## Breaking Changes\n\n");
        for change in &report.breaking_changes {
            md.push_str(&format!("- ⚠️ {}\n", change));
        }
    }
    
    if !report.recommendations.is_empty() {
        md.push_str("\n## Recommendations\n\n");
        for rec in &report.recommendations {
            md.push_str(&format!("- 💡 {}\n", rec));
        }
    }
    
    md.push_str("\n## Detailed Results\n\n");
    for result in &report.detailed_results {
        md.push_str(&format!("### Cassandra {}\n\n", result.version));
        
        let status_emoji = match result.status {
            cqlite_compatibility::suite::TestStatus::Passed => "✅",
            cqlite_compatibility::suite::TestStatus::Failed(_) => "❌",
            cqlite_compatibility::suite::TestStatus::Warning(_) => "⚠️",
            cqlite_compatibility::suite::TestStatus::Skipped(_) => "⏭️",
        };
        
        md.push_str(&format!("**Status:** {} {:?}\n", status_emoji, result.status));
        md.push_str(&format!("**Duration:** {:?}\n", result.duration));
        md.push_str(&format!("**SSTable Parsing:** {}/{} files successful\n", 
            result.details.sstable_parsing.files_parsed_successfully,
            result.details.sstable_parsing.files_tested));
        md.push_str(&format!("**Query Compatibility:** {}/{} queries successful\n\n", 
            result.details.query_compatibility.queries_successful,
            result.details.query_compatibility.queries_tested));
    }
    
    md
}