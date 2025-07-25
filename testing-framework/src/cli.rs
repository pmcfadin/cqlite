//! Command-line interface for the testing framework

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use crate::config::TestConfig;
use crate::test_case::{TestCase, TestCaseLoader, TestFilter, TestCategory, TestPriority};

/// CQLite Testing Framework CLI
#[derive(Parser)]
#[command(name = "cqlite-test")]
#[command(about = "A comprehensive framework for testing CQLite compatibility with Cassandra")]
#[command(version = "0.1.0")]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Verbose output
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Output format
    #[arg(short, long, default_value = "text")]
    pub format: OutputFormat,

    #[command(subcommand)]
    pub command: Commands,
}

/// Available output formats
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Html,
    Csv,
}

/// Available CLI commands
#[derive(Subcommand)]
pub enum Commands {
    /// Run test cases
    Run {
        /// Test case files or directories
        #[arg(value_name = "PATHS")]
        paths: Vec<PathBuf>,
        
        /// Filter by category
        #[arg(long)]
        category: Option<TestCategory>,
        
        /// Minimum priority level
        #[arg(long)]
        priority: Option<TestPriority>,
        
        /// Filter by tags (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tags: Option<Vec<String>>,
        
        /// Filter by name pattern
        #[arg(long)]
        name_pattern: Option<String>,
        
        /// Maximum number of tests to run
        #[arg(long)]
        limit: Option<usize>,
        
        /// Run tests in parallel
        #[arg(long)]
        parallel: bool,
        
        /// Number of parallel workers
        #[arg(long, default_value = "4")]
        workers: usize,
        
        /// Stop on first failure
        #[arg(long)]
        fail_fast: bool,
        
        /// Generate detailed report
        #[arg(long)]
        detailed_report: bool,
    },
    
    /// List available test cases
    List {
        /// Test case files or directories
        #[arg(value_name = "PATHS")]
        paths: Vec<PathBuf>,
        
        /// Filter by category
        #[arg(long)]
        category: Option<TestCategory>,
        
        /// Minimum priority level
        #[arg(long)]
        priority: Option<TestPriority>,
        
        /// Filter by tags (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tags: Option<Vec<String>>,
        
        /// Filter by name pattern
        #[arg(long)]
        name_pattern: Option<String>,
        
        /// Show detailed information
        #[arg(long)]
        detailed: bool,
    },
    
    /// Validate configuration
    Config {
        /// Show current configuration
        #[arg(long)]
        show: bool,
        
        /// Validate configuration file
        #[arg(long)]
        validate: bool,
        
        /// Generate default configuration
        #[arg(long)]
        generate: Option<PathBuf>,
    },
    
    /// Setup Docker environment
    Setup {
        /// Skip Cassandra container setup
        #[arg(long)]
        skip_cassandra: bool,
        
        /// Force recreate containers
        #[arg(long)]
        force: bool,
        
        /// Cassandra version to use
        #[arg(long, default_value = "5.0")]
        cassandra_version: String,
    },
    
    /// Clean up test environment
    Cleanup {
        /// Remove Docker containers
        #[arg(long)]
        containers: bool,
        
        /// Remove test data
        #[arg(long)]
        data: bool,
        
        /// Remove generated reports
        #[arg(long)]
        reports: bool,
        
        /// Remove everything
        #[arg(long)]
        all: bool,
    },
    
    /// Generate test cases
    Generate {
        /// Output directory
        #[arg(short, long, default_value = "./test_cases")]
        output: PathBuf,
        
        /// Test case template
        #[arg(long, default_value = "basic")]
        template: String,
        
        /// Number of test cases to generate
        #[arg(long, default_value = "10")]
        count: usize,
        
        /// Include random data generation
        #[arg(long)]
        random_data: bool,
    },
    
    /// Analyze test results
    Analyze {
        /// Results file or directory
        #[arg(value_name = "PATH")]
        path: PathBuf,
        
        /// Analysis type
        #[arg(long, default_value = "summary")]
        analysis_type: AnalysisType,
        
        /// Generate recommendations
        #[arg(long)]
        recommendations: bool,
    },
}

/// Analysis types for test results
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum AnalysisType {
    Summary,
    Detailed,
    Performance,
    Compatibility,
    Trends,
}

/// CLI handler for processing commands
pub struct CliHandler {
    config: TestConfig,
    verbose: u8,
    format: OutputFormat,
}

impl CliHandler {
    /// Create a new CLI handler
    pub fn new(config: TestConfig, verbose: u8, format: OutputFormat) -> Self {
        Self {
            config,
            verbose,
            format,
        }
    }

    /// Handle CLI commands
    pub async fn handle_command(&mut self, command: Commands) -> Result<()> {
        match command {
            Commands::Run {
                paths,
                category,
                priority,
                tags,
                name_pattern,
                limit,
                parallel,
                workers,
                fail_fast,
                detailed_report,
            } => {
                self.handle_run_command(
                    paths,
                    category,
                    priority,
                    tags,
                    name_pattern,
                    limit,
                    parallel,
                    workers,
                    fail_fast,
                    detailed_report,
                ).await
            }
            Commands::List {
                paths,
                category,
                priority,
                tags,
                name_pattern,
                detailed,
            } => {
                self.handle_list_command(
                    paths,
                    category,
                    priority,
                    tags,
                    name_pattern,
                    detailed,
                ).await
            }
            Commands::Config { show, validate, generate } => {
                self.handle_config_command(show, validate, generate).await
            }
            Commands::Setup {
                skip_cassandra,
                force,
                cassandra_version,
            } => {
                self.handle_setup_command(skip_cassandra, force, cassandra_version).await
            }
            Commands::Cleanup {
                containers,
                data,
                reports,
                all,
            } => {
                self.handle_cleanup_command(containers, data, reports, all).await
            }
            Commands::Generate {
                output,
                template,
                count,
                random_data,
            } => {
                self.handle_generate_command(output, template, count, random_data).await
            }
            Commands::Analyze {
                path,
                analysis_type,
                recommendations,
            } => {
                self.handle_analyze_command(path, analysis_type, recommendations).await
            }
        }
    }

    /// Handle run command
    async fn handle_run_command(
        &mut self,
        paths: Vec<PathBuf>,
        category: Option<TestCategory>,
        priority: Option<TestPriority>,
        tags: Option<Vec<String>>,
        name_pattern: Option<String>,
        limit: Option<usize>,
        parallel: bool,
        workers: usize,
        fail_fast: bool,
        detailed_report: bool,
    ) -> Result<()> {
        self.log_info("Starting test execution");
        
        // Load test cases
        let mut test_cases = self.load_test_cases(paths).await?;
        
        // Apply filters
        let filter = TestFilter {
            categories: category.map(|c| vec![c]),
            min_priority: priority,
            required_tags: tags,
            name_pattern,
        };
        
        test_cases = test_cases.into_iter()
            .filter(|tc| tc.matches_filter(&filter))
            .collect();
        
        // Apply limit
        if let Some(limit) = limit {
            test_cases.truncate(limit);
        }
        
        self.log_info(&format!("Found {} test cases to run", test_cases.len()));
        
        // Update configuration based on CLI options
        if parallel {
            self.config.environment.parallel_execution = true;
            self.config.environment.max_concurrent_tests = workers;
        }
        
        // Create and run test framework
        let mut framework = crate::TestFramework::new(self.config.clone()).await?;
        let result = framework.run_test_suite(test_cases).await?;
        framework.cleanup().await?;
        
        // Output results
        self.output_test_results(&result).await?;
        
        // Exit with error code if tests failed
        if result.failed_tests > 0 && fail_fast {
            std::process::exit(1);
        }
        
        Ok(())
    }

    /// Handle list command
    async fn handle_list_command(
        &mut self,
        paths: Vec<PathBuf>,
        category: Option<TestCategory>,
        priority: Option<TestPriority>,
        tags: Option<Vec<String>>,
        name_pattern: Option<String>,
        detailed: bool,
    ) -> Result<()> {
        let test_cases = self.load_test_cases(paths).await?;
        
        let filter = TestFilter {
            categories: category.map(|c| vec![c]),
            min_priority: priority,
            required_tags: tags,
            name_pattern,
        };
        
        let filtered_cases: Vec<_> = test_cases.into_iter()
            .filter(|tc| tc.matches_filter(&filter))
            .collect();
        
        self.output_test_case_list(&filtered_cases, detailed).await?;
        
        Ok(())
    }

    /// Handle config command
    async fn handle_config_command(
        &mut self,
        show: bool,
        validate: bool,
        generate: Option<PathBuf>,
    ) -> Result<()> {
        if show {
            println!("{}", serde_json::to_string_pretty(&self.config)?);
        }
        
        if validate {
            match self.config.validate() {
                Ok(_) => self.log_info("Configuration is valid"),
                Err(e) => {
                    eprintln!("Configuration validation failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        if let Some(output_path) = generate {
            let default_config = TestConfig::default();
            default_config.save_to_file(&output_path)?;
            self.log_info(&format!("Default configuration saved to: {:?}", output_path));
        }
        
        Ok(())
    }

    /// Handle setup command
    async fn handle_setup_command(
        &mut self,
        skip_cassandra: bool,
        force: bool,
        cassandra_version: String,
    ) -> Result<()> {
        self.log_info("Setting up test environment");
        
        if !skip_cassandra {
            self.log_info(&format!("Setting up Cassandra {}", cassandra_version));
            
            // Update Docker config with specified version
            let mut docker_config = self.config.docker.clone();
            docker_config.image = format!("cassandra:{}", cassandra_version);
            
            let docker_manager = crate::docker::DockerManager::new(&docker_config).await?;
            
            if force {
                docker_manager.cleanup().await?;
            }
            
            docker_manager.ensure_cassandra_ready().await?;
            self.log_info("Cassandra setup completed");
        }
        
        Ok(())
    }

    /// Handle cleanup command
    async fn handle_cleanup_command(
        &mut self,
        containers: bool,
        data: bool,
        reports: bool,
        all: bool,
    ) -> Result<()> {
        self.log_info("Cleaning up test environment");
        
        if all || containers {
            let docker_manager = crate::docker::DockerManager::new(&self.config.docker).await?;
            docker_manager.cleanup().await?;
            self.log_info("Docker containers cleaned up");
        }
        
        if all || data {
            if self.config.cqlite.database_path.exists() {
                std::fs::remove_file(&self.config.cqlite.database_path)?;
                self.log_info("Test database cleaned up");
            }
        }
        
        if all || reports {
            if self.config.reporting.output_directory.exists() {
                std::fs::remove_dir_all(&self.config.reporting.output_directory)?;
                self.log_info("Test reports cleaned up");
            }
        }
        
        Ok(())
    }

    /// Handle generate command
    async fn handle_generate_command(
        &mut self,
        output: PathBuf,
        template: String,
        count: usize,
        random_data: bool,
    ) -> Result<()> {
        self.log_info(&format!("Generating {} test cases", count));
        
        std::fs::create_dir_all(&output)?;
        
        let test_cases = match template.as_str() {
            "basic" => TestCaseLoader::create_default_test_cases(),
            _ => {
                anyhow::bail!("Unknown template: {}", template);
            }
        };
        
        // Take only the requested count
        let test_cases: Vec<_> = test_cases.into_iter().take(count).collect();
        
        let output_file = output.join("generated_tests.json");
        let json_content = serde_json::to_string_pretty(&test_cases)?;
        std::fs::write(&output_file, json_content)?;
        
        self.log_info(&format!("Generated {} test cases in: {:?}", test_cases.len(), output_file));
        
        Ok(())
    }

    /// Handle analyze command
    async fn handle_analyze_command(
        &mut self,
        path: PathBuf,
        analysis_type: AnalysisType,
        recommendations: bool,
    ) -> Result<()> {
        self.log_info(&format!("Analyzing results at: {:?}", path));
        
        // TODO: Implement result analysis
        match analysis_type {
            AnalysisType::Summary => {
                println!("Summary analysis not yet implemented");
            }
            AnalysisType::Detailed => {
                println!("Detailed analysis not yet implemented");
            }
            AnalysisType::Performance => {
                println!("Performance analysis not yet implemented");
            }
            AnalysisType::Compatibility => {
                println!("Compatibility analysis not yet implemented");
            }
            AnalysisType::Trends => {
                println!("Trends analysis not yet implemented");
            }
        }
        
        if recommendations {
            println!("Recommendations feature not yet implemented");
        }
        
        Ok(())
    }

    /// Load test cases from specified paths
    async fn load_test_cases(&self, paths: Vec<PathBuf>) -> Result<Vec<TestCase>> {
        let mut all_test_cases = Vec::new();
        
        if paths.is_empty() {
            // Use default test data paths from config
            for path in &self.config.test_data.test_queries {
                let mut test_cases = TestCaseLoader::load_from_directory(path)?;
                all_test_cases.append(&mut test_cases);
            }
            
            // Also add default test cases if no files found
            if all_test_cases.is_empty() {
                let mut default_cases = TestCaseLoader::create_default_test_cases();
                all_test_cases.append(&mut default_cases);
            }
        } else {
            for path in paths {
                if path.is_dir() {
                    let mut test_cases = TestCaseLoader::load_from_directory(&path)?;
                    all_test_cases.append(&mut test_cases);
                } else if path.is_file() {
                    let extension = path.extension().and_then(|s| s.to_str());
                    let mut test_cases = match extension {
                        Some("json") => TestCaseLoader::load_from_json(&path)?,
                        Some("cql") => TestCaseLoader::load_from_cql(&path)?,
                        _ => {
                            self.log_warning(&format!("Unknown file type: {:?}", path));
                            continue;
                        }
                    };
                    all_test_cases.append(&mut test_cases);
                }
            }
        }
        
        Ok(all_test_cases)
    }

    /// Output test results in the specified format
    async fn output_test_results(&self, results: &crate::TestSuiteResult) -> Result<()> {
        match self.format {
            OutputFormat::Text => {
                println!("\n=== CQLite Test Results ===");
                println!("Suite: {}", results.suite_name);
                println!("Total Tests: {}", results.total_tests);
                println!("Passed: {} ({:.1}%)", results.passed_tests, results.success_rate);
                println!("Failed: {}", results.failed_tests);
                println!("Execution Time: {} ms", results.total_execution_time_ms);
                println!("Timestamp: {}", results.timestamp);
                
                if self.verbose > 0 {
                    println!("\n=== Test Details ===");
                    for test_result in &results.test_results {
                        let status = if test_result.success { "✅ PASS" } else { "❌ FAIL" };
                        println!("{} {} ({} ms)", status, test_result.test_name, test_result.execution_time_ms);
                        
                        if !test_result.success && self.verbose > 1 {
                            if let Some(ref error) = test_result.error {
                                println!("  Error: {}", error);
                            }
                        }
                    }
                }
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(results)?);
            }
            OutputFormat::Html | OutputFormat::Csv => {
                println!("HTML and CSV output formats are generated in the reports directory");
            }
        }
        
        Ok(())
    }

    /// Output test case list
    async fn output_test_case_list(&self, test_cases: &[TestCase], detailed: bool) -> Result<()> {
        println!("Found {} test cases:", test_cases.len());
        
        for test_case in test_cases {
            if detailed {
                println!("\n--- {} ---", test_case.name);
                println!("Description: {}", test_case.description);
                println!("Category: {:?}", test_case.category);
                println!("Priority: {:?}", test_case.priority);
                println!("Query: {}", test_case.query);
                if !test_case.tags.is_empty() {
                    println!("Tags: {}", test_case.tags.join(", "));
                }
            } else {
                println!(
                    "  {} [{:?}] {:?} - {}",
                    test_case.name,
                    test_case.category,
                    test_case.priority,
                    test_case.description
                );
            }
        }
        
        Ok(())
    }

    /// Log info message if verbose enough
    fn log_info(&self, message: &str) {
        if self.verbose > 0 {
            println!("[INFO] {}", message);
        }
    }

    /// Log warning message
    fn log_warning(&self, message: &str) {
        eprintln!("[WARN] {}", message);
    }
}

/// Parse command line arguments and create CLI instance
pub fn parse_args() -> Cli {
    Cli::parse()
}

/// Initialize logging based on verbosity level
pub fn init_logging(verbose: u8) {
    let log_level = match verbose {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    
    env_logger::Builder::from_default_env()
        .filter_level(log_level)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test basic parsing - this is mainly to ensure the CLI structure compiles
        let cli = Cli::try_parse_from(&[
            "cqlite-test",
            "run",
            "--parallel",
            "test_cases/"
        ]);
        
        assert!(cli.is_ok());
    }
}