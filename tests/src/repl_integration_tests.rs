//! Comprehensive REPL Integration Tests
//!
//! This module provides comprehensive testing for the CQLite REPL system,
//! validating all quality gates and user workflows.

use anyhow::Result;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// REPL test configuration
#[derive(Debug, Clone)]
pub struct ReplTestConfig {
    /// Path to cqlite binary
    pub binary_path: PathBuf,
    /// Timeout for each test
    pub timeout: Duration,
    /// Whether to capture verbose output
    pub verbose: bool,
    /// Test data directory
    pub test_data_dir: Option<PathBuf>,
}

impl Default for ReplTestConfig {
    fn default() -> Self {
        Self {
            binary_path: PathBuf::from("target/debug/cqlite"),
            timeout: Duration::from_secs(10),
            verbose: false,
            test_data_dir: None,
        }
    }
}

/// REPL test result
#[derive(Debug)]
pub struct ReplTestResult {
    /// Test name
    pub name: String,
    /// Whether test passed
    pub passed: bool,
    /// Test output
    pub output: String,
    /// Error message if failed
    pub error: Option<String>,
    /// Execution time
    pub duration: Duration,
}

/// Comprehensive REPL test suite
pub struct ReplTestSuite {
    config: ReplTestConfig,
    results: Vec<ReplTestResult>,
}

impl ReplTestSuite {
    /// Create new test suite
    pub fn new(config: ReplTestConfig) -> Self {
        Self {
            config,
            results: Vec::new(),
        }
    }

    /// Run all REPL tests
    pub fn run_all_tests(&mut self) -> Result<()> {
        println!("🧪 Running Comprehensive REPL Integration Tests");
        println!("===============================================");

        // Quality Gate 1: REPL Startup and Initialization
        self.test_repl_startup()?;
        self.test_repl_banner_display()?;
        self.test_repl_prompt_generation()?;

        // Quality Gate 2: Command System Functionality
        self.test_help_system()?;
        self.test_configuration_system()?;
        self.test_data_exploration_commands()?;
        self.test_meta_commands()?;

        // Quality Gate 3: CQL Query Execution
        self.test_basic_cql_queries()?;
        self.test_system_table_queries()?;
        self.test_query_error_handling()?;
        self.test_query_timing_and_performance()?;

        // Quality Gate 4: User Workflow Testing
        self.test_data_exploration_workflow()?;
        self.test_configuration_workflow()?;
        self.test_help_navigation_workflow()?;
        self.test_query_development_workflow()?;

        // Quality Gate 5: Real Data Compatibility
        self.test_real_cassandra_data_integration()?;
        self.test_keyspace_and_table_discovery()?;
        self.test_schema_introspection()?;

        // Quality Gate 6: Error Handling and Recovery
        self.test_graceful_error_handling()?;
        self.test_recovery_scenarios()?;
        self.test_invalid_input_handling()?;

        // Quality Gate 7: Session Management
        self.test_session_state_management()?;
        self.test_command_history()?;
        self.test_clean_exit()?;

        Ok(())
    }

    /// Print test summary
    pub fn print_summary(&self) {
        let total = self.results.len();
        let passed = self.results.iter().filter(|r| r.passed).count();
        let failed = total - passed;

        println!("\n📊 REPL Test Summary");
        println!("===================");
        println!("Total Tests: {}", total);
        println!("✅ Passed: {}", passed);
        if failed > 0 {
            println!("❌ Failed: {}", failed);
        }
        println!(
            "Success Rate: {:.1}%",
            (passed as f64 / total as f64) * 100.0
        );

        if failed > 0 {
            println!("\n❌ Failed Tests:");
            for result in &self.results {
                if !result.passed {
                    println!(
                        "  • {}: {}",
                        result.name,
                        result
                            .error
                            .as_ref()
                            .unwrap_or(&"Unknown error".to_string())
                    );
                }
            }
        }

        // Quality gate assessment
        self.assess_quality_gates();
    }

    /// Assess quality gates
    fn assess_quality_gates(&self) {
        println!("\n🎯 Quality Gate Assessment");
        println!("=========================");

        let gate1_tests = ["repl_startup", "banner_display", "prompt_generation"];
        let gate1_passed = self.count_passed_tests(&gate1_tests);
        self.print_gate_status("Gate 1: REPL Startup", gate1_passed, gate1_tests.len());

        let gate2_tests = [
            "help_system",
            "configuration_system",
            "data_exploration",
            "meta_commands",
        ];
        let gate2_passed = self.count_passed_tests(&gate2_tests);
        self.print_gate_status("Gate 2: Command System", gate2_passed, gate2_tests.len());

        let gate3_tests = ["basic_cql", "system_queries", "error_handling", "timing"];
        let gate3_passed = self.count_passed_tests(&gate3_tests);
        self.print_gate_status("Gate 3: CQL Execution", gate3_passed, gate3_tests.len());

        let gate4_tests = [
            "exploration_workflow",
            "config_workflow",
            "help_workflow",
            "query_workflow",
        ];
        let gate4_passed = self.count_passed_tests(&gate4_tests);
        self.print_gate_status("Gate 4: User Workflows", gate4_passed, gate4_tests.len());

        let gate5_tests = ["real_data", "discovery", "schema_introspection"];
        let gate5_passed = self.count_passed_tests(&gate5_tests);
        self.print_gate_status("Gate 5: Real Data", gate5_passed, gate5_tests.len());

        let gate6_tests = ["graceful_errors", "recovery", "invalid_input"];
        let gate6_passed = self.count_passed_tests(&gate6_tests);
        self.print_gate_status("Gate 6: Error Handling", gate6_passed, gate6_tests.len());

        let gate7_tests = ["session_state", "command_history", "clean_exit"];
        let gate7_passed = self.count_passed_tests(&gate7_tests);
        self.print_gate_status(
            "Gate 7: Session Management",
            gate7_passed,
            gate7_tests.len(),
        );
    }

    fn count_passed_tests(&self, test_names: &[&str]) -> usize {
        self.results
            .iter()
            .filter(|r| test_names.iter().any(|name| r.name.contains(name)) && r.passed)
            .count()
    }

    fn print_gate_status(&self, gate_name: &str, passed: usize, total: usize) {
        let status = if passed == total {
            "✅ PASS"
        } else {
            "❌ FAIL"
        };
        println!("{}: {} ({}/{} tests)", gate_name, status, passed, total);
    }

    /// Test REPL startup
    fn test_repl_startup(&mut self) -> Result<()> {
        println!("\n🚀 Testing REPL Startup...");

        let output = self.run_repl_command(":quit", "repl_startup")?;
        let passed = output.contains("CQLite Interactive Shell") || output.contains("cqlite>");

        self.record_result("repl_startup", passed, output, None);
        Ok(())
    }

    /// Test banner display
    fn test_repl_banner_display(&mut self) -> Result<()> {
        println!("📋 Testing Banner Display...");

        let output = self.run_repl_command(":quit", "banner_display")?;
        let passed = output.contains("CQLite Interactive Shell")
            && output.contains("High-Performance Cassandra Reader");

        self.record_result("banner_display", passed, output, None);
        Ok(())
    }

    /// Test prompt generation
    fn test_repl_prompt_generation(&mut self) -> Result<()> {
        println!("💬 Testing Prompt Generation...");

        let output = self.run_repl_command(":quit", "prompt_generation")?;
        let passed = output.contains("cqlite>");

        self.record_result("prompt_generation", passed, output, None);
        Ok(())
    }

    /// Test help system
    fn test_help_system(&mut self) -> Result<()> {
        println!("📚 Testing Help System...");

        // Test main help
        let output = self.run_repl_command(":help\n:quit", "help_system")?;
        let basic_help =
            output.contains("CQLite Interactive REPL") && output.contains("Meta Commands");

        // Test help topics
        let topics_output = self.run_repl_command(":help commands\n:quit", "help_topics")?;
        let topics_help = topics_output.contains("Meta-Commands Reference");

        let passed = basic_help && topics_help;
        self.record_result(
            "help_system",
            passed,
            format!("{}\n---\n{}", output, topics_output),
            None,
        );
        Ok(())
    }

    /// Test configuration system
    fn test_configuration_system(&mut self) -> Result<()> {
        println!("⚙️ Testing Configuration System...");

        // Test config display
        let output = self.run_repl_command(":config\n:quit", "configuration_system")?;
        let config_display = output.contains("Current Configuration");

        // Test timing toggle
        let timing_output = self.run_repl_command(":timing\n:config\n:quit", "timing_config")?;
        let timing_works = timing_output.contains("Timing is now");

        let passed = config_display && timing_works;
        self.record_result(
            "configuration_system",
            passed,
            format!("{}\n---\n{}", output, timing_output),
            None,
        );
        Ok(())
    }

    /// Test data exploration commands
    fn test_data_exploration_commands(&mut self) -> Result<()> {
        println!("🔍 Testing Data Exploration...");

        // Test keyspaces command
        let keyspaces_output = self.run_repl_command(":keyspaces\n:quit", "keyspaces_cmd")?;
        let keyspaces_work = keyspaces_output.contains("Available Keyspaces");

        // Test tables command
        let tables_output = self.run_repl_command(":tables\n:quit", "tables_cmd")?;
        let tables_work = tables_output.contains("Available Tables");

        let passed = keyspaces_work && tables_work;
        self.record_result(
            "data_exploration",
            passed,
            format!("{}\n---\n{}", keyspaces_output, tables_output),
            None,
        );
        Ok(())
    }

    /// Test meta commands
    fn test_meta_commands(&mut self) -> Result<()> {
        println!("🔧 Testing Meta Commands...");

        // Test history command
        let history_output = self.run_repl_command(":help\n:history\n:quit", "meta_commands")?;
        let history_works = history_output.contains("Command History");

        // Test clear command
        let clear_output = self.run_repl_command(":clear\n:quit", "clear_cmd")?;
        let clear_works = !clear_output.contains("Error");

        let passed = history_works && clear_works;
        self.record_result(
            "meta_commands",
            passed,
            format!("{}\n---\n{}", history_output, clear_output),
            None,
        );
        Ok(())
    }

    /// Test basic CQL queries
    fn test_basic_cql_queries(&mut self) -> Result<()> {
        println!("💾 Testing Basic CQL Queries...");

        let output = self.run_repl_command(
            "SELECT * FROM system.keyspaces LIMIT 1;\n:quit",
            "basic_cql",
        )?;
        let passed = output.contains("Executing") && !output.contains("failed");

        self.record_result("basic_cql", passed, output, None);
        Ok(())
    }

    /// Test system table queries
    fn test_system_table_queries(&mut self) -> Result<()> {
        println!("🗄️ Testing System Table Queries...");

        let queries = vec![
            "SELECT keyspace_name FROM system.keyspaces;",
            "SELECT table_name FROM system.tables WHERE keyspace_name = 'system' LIMIT 3;",
        ];

        let mut all_passed = true;
        let mut combined_output = String::new();

        for query in queries {
            let cmd = format!("{}\n:quit", query);
            let output = self.run_repl_command(&cmd, "system_queries")?;
            let passed = output.contains("Executing") && !output.contains("failed");
            if !passed {
                all_passed = false;
            }
            combined_output.push_str(&output);
            combined_output.push_str("\n---\n");
        }

        self.record_result("system_queries", all_passed, combined_output, None);
        Ok(())
    }

    /// Test query error handling
    fn test_query_error_handling(&mut self) -> Result<()> {
        println!("⚠️ Testing Query Error Handling...");

        let output = self.run_repl_command("INVALID SQL SYNTAX HERE;\n:quit", "error_handling")?;
        let passed = output.contains("Error") && output.contains("Hint");

        self.record_result("error_handling", passed, output, None);
        Ok(())
    }

    /// Test query timing and performance
    fn test_query_timing_and_performance(&mut self) -> Result<()> {
        println!("⏱️ Testing Query Timing...");

        let output = self.run_repl_command(
            ":timing\nSELECT * FROM system.keyspaces LIMIT 1;\n:quit",
            "timing",
        )?;
        let passed = output.contains("Execution time") || output.contains("Query completed");

        self.record_result("timing", passed, output, None);
        Ok(())
    }

    /// Test data exploration workflow
    fn test_data_exploration_workflow(&mut self) -> Result<()> {
        println!("🔍 Testing Data Exploration Workflow...");

        let workflow = ":keyspaces\n:tables\n:help examples\n:quit";
        let output = self.run_repl_command(workflow, "exploration_workflow")?;
        let passed = output.contains("Available Keyspaces")
            && output.contains("Available Tables")
            && output.contains("Common Usage Examples");

        self.record_result("exploration_workflow", passed, output, None);
        Ok(())
    }

    /// Test configuration workflow
    fn test_configuration_workflow(&mut self) -> Result<()> {
        println!("⚙️ Testing Configuration Workflow...");

        let workflow = ":config\n:config timing on\n:config page-size 25\n:config\n:quit";
        let output = self.run_repl_command(workflow, "config_workflow")?;
        let passed = output.contains("Current Configuration")
            && output.contains("enabled")
            && output.contains("25");

        self.record_result("config_workflow", passed, output, None);
        Ok(())
    }

    /// Test help navigation workflow
    fn test_help_navigation_workflow(&mut self) -> Result<()> {
        println!("📚 Testing Help Navigation Workflow...");

        let workflow = ":help\n:help commands\n:help config\n:help cql\n:quit";
        let output = self.run_repl_command(workflow, "help_workflow")?;
        let passed = output.contains("CQLite Interactive REPL")
            && output.contains("Meta-Commands Reference")
            && output.contains("Configuration System")
            && output.contains("CQL Query Support");

        self.record_result("help_workflow", passed, output, None);
        Ok(())
    }

    /// Test query development workflow
    fn test_query_development_workflow(&mut self) -> Result<()> {
        println!("💻 Testing Query Development Workflow...");

        let workflow =
            ":timing\n:keyspaces\nSELECT * FROM system.keyspaces LIMIT 1;\n:history\n:quit";
        let output = self.run_repl_command(workflow, "query_workflow")?;
        let passed = output.contains("Timing is now enabled")
            && output.contains("Executing")
            && output.contains("Command History");

        self.record_result("query_workflow", passed, output, None);
        Ok(())
    }

    /// Test real Cassandra data integration
    fn test_real_cassandra_data_integration(&mut self) -> Result<()> {
        println!("🗄️ Testing Real Data Integration...");

        // This test checks if REPL can handle data directory configuration
        let workflow = ":config data-dir /nonexistent\n:tables\n:quit";
        let output = self.run_repl_command(workflow, "real_data")?;
        let passed = output.contains("Directory does not exist")
            || output.contains("Could not scan data directory")
            || output.contains("No user tables found");

        self.record_result("real_data", passed, output, None);
        Ok(())
    }

    /// Test keyspace and table discovery
    fn test_keyspace_and_table_discovery(&mut self) -> Result<()> {
        println!("🔍 Testing Discovery Features...");

        let workflow = ":keyspaces\n:tables\n:info system\n:quit";
        let output = self.run_repl_command(workflow, "discovery")?;
        let passed = output.contains("Available Keyspaces") && output.contains("Available Tables");

        self.record_result("discovery", passed, output, None);
        Ok(())
    }

    /// Test schema introspection
    fn test_schema_introspection(&mut self) -> Result<()> {
        println!("📋 Testing Schema Introspection...");

        let workflow = ":schema\n:describe system.keyspaces\n:quit";
        let output = self.run_repl_command(workflow, "schema_introspection")?;
        let passed = output.contains("Table Schema")
            || output.contains("All Table Schemas")
            || output.contains("No user tables found");

        self.record_result("schema_introspection", passed, output, None);
        Ok(())
    }

    /// Test graceful error handling
    fn test_graceful_error_handling(&mut self) -> Result<()> {
        println!("🛡️ Testing Graceful Error Handling...");

        let errors = vec![
            "COMPLETELY INVALID SYNTAX HERE",
            "SELECT * FROM nonexistent_table;",
            ":invalid_command",
            "INSERT INTO", // Incomplete query
        ];

        let mut all_graceful = true;
        let mut combined_output = String::new();

        for error_query in errors {
            let cmd = format!("{}\n:quit", error_query);
            let output = self.run_repl_command(&cmd, "graceful_errors")?;
            let graceful = output.contains("Error")
                && !output.contains("panic")
                && !output.contains("crashed");
            if !graceful {
                all_graceful = false;
            }
            combined_output.push_str(&output);
            combined_output.push_str("\n---\n");
        }

        self.record_result("graceful_errors", all_graceful, combined_output, None);
        Ok(())
    }

    /// Test recovery scenarios
    fn test_recovery_scenarios(&mut self) -> Result<()> {
        println!("🔄 Testing Recovery Scenarios...");

        let workflow = "INVALID QUERY;\n:help\nSELECT * FROM system.keyspaces LIMIT 1;\n:quit";
        let output = self.run_repl_command(workflow, "recovery")?;
        let passed = output.contains("Error")
            && output.contains("CQLite Interactive REPL")
            && output.contains("Executing");

        self.record_result("recovery", passed, output, None);
        Ok(())
    }

    /// Test invalid input handling
    fn test_invalid_input_handling(&mut self) -> Result<()> {
        println!("🚫 Testing Invalid Input Handling...");

        let invalid_inputs = vec![
            "",         // Empty input
            "   ",      // Whitespace only
            ":unknown", // Unknown meta command
            ".invalid", // Invalid dot command
        ];

        let mut all_handled = true;
        let mut combined_output = String::new();

        for input in invalid_inputs {
            let cmd = format!("{}\n:quit", input);
            let output = self.run_repl_command(&cmd, "invalid_input")?;
            let handled = !output.contains("panic") && !output.contains("crashed");
            if !handled {
                all_handled = false;
            }
            combined_output.push_str(&output);
            combined_output.push_str("\n---\n");
        }

        self.record_result("invalid_input", all_handled, combined_output, None);
        Ok(())
    }

    /// Test session state management
    fn test_session_state_management(&mut self) -> Result<()> {
        println!("📝 Testing Session State Management...");

        let workflow = ":config timing on\n:config\n:timing\n:config\n:quit";
        let output = self.run_repl_command(workflow, "session_state")?;
        let passed = output.contains("enabled") && output.contains("disabled");

        self.record_result("session_state", passed, output, None);
        Ok(())
    }

    /// Test command history
    fn test_command_history(&mut self) -> Result<()> {
        println!("📜 Testing Command History...");

        let workflow = ":help\n:config\nSELECT 1;\n:history\n:quit";
        let output = self.run_repl_command(workflow, "command_history")?;
        let passed = output.contains("Command History")
            && (output.contains(":help") || output.contains("SELECT 1"));

        self.record_result("command_history", passed, output, None);
        Ok(())
    }

    /// Test clean exit
    fn test_clean_exit(&mut self) -> Result<()> {
        println!("🚪 Testing Clean Exit...");

        let commands = vec![":quit", ":exit", ":q"];
        let mut all_clean = true;
        let mut combined_output = String::new();

        for cmd in commands {
            let output = self.run_repl_command(cmd, "clean_exit")?;
            let clean = output.contains("Goodbye") || !output.contains("Error");
            if !clean {
                all_clean = false;
            }
            combined_output.push_str(&output);
            combined_output.push_str("\n---\n");
        }

        self.record_result("clean_exit", all_clean, combined_output, None);
        Ok(())
    }

    /// Run REPL command and capture output
    fn run_repl_command(&self, input: &str, test_name: &str) -> Result<String> {
        let start = Instant::now();

        let mut cmd = Command::new(&self.config.binary_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to start cqlite: {}", e))?;

        // Send input
        if let Some(stdin) = cmd.stdin.take() {
            let mut stdin = stdin;
            stdin.write_all(input.as_bytes())?;
            stdin.write_all(b"\n")?;
            drop(stdin); // Close stdin to signal EOF
        }

        // Wait for completion with timeout (using manual timeout approach)
        let output = {
            let start_time = Instant::now();
            loop {
                match cmd.try_wait()? {
                    Some(status) => {
                        let stdout = cmd.stdout.take().unwrap();
                        let stderr = cmd.stderr.take().unwrap();

                        let mut stdout_data = String::new();
                        let mut stderr_data = String::new();

                        BufReader::new(stdout).read_to_string(&mut stdout_data)?;
                        BufReader::new(stderr).read_to_string(&mut stderr_data)?;

                        break format!(
                            "Status: {}\nSTDOUT:\n{}\nSTDERR:\n{}",
                            status, stdout_data, stderr_data
                        );
                    }
                    None => {
                        if start_time.elapsed() > self.config.timeout {
                            cmd.kill()?;
                            cmd.wait()?;
                            break format!("TIMEOUT after {:?}", self.config.timeout);
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        };

        if self.config.verbose {
            println!(
                "Test {}: {} chars output in {:?}",
                test_name,
                output.len(),
                start.elapsed()
            );
        }

        Ok(output)
    }

    /// Record test result
    fn record_result(&mut self, name: &str, passed: bool, output: String, error: Option<String>) {
        let result = ReplTestResult {
            name: name.to_string(),
            passed,
            output,
            error,
            duration: Duration::from_millis(0), // Simplified for now
        };

        let status = if passed { "✅ PASS" } else { "❌ FAIL" };
        println!("  {} {}", status, name);

        self.results.push(result);
    }
}

/// Create test data files for REPL testing
pub fn create_test_data_files(test_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(test_dir)?;

    // Create sample CQL script
    let sample_cql = r#"
-- Sample CQL script for REPL testing
SELECT keyspace_name FROM system.keyspaces LIMIT 5;
SELECT table_name FROM system.tables WHERE keyspace_name = 'system' LIMIT 3;
DESCRIBE KEYSPACE system;
"#;

    fs::write(test_dir.join("sample_queries.cql"), sample_cql)?;

    // Create test configuration file
    let test_config = r#"
# Test configuration for REPL
[database]
path = "test.db"

[repl]
timing = true
page_size = 10
paging = true
"#;

    fs::write(test_dir.join("test_config.toml"), test_config)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::TempDir;

    #[test]
    fn test_repl_integration_basic() {
        // Only run if binary exists
        let binary_path = PathBuf::from("target/debug/cqlite");
        if !binary_path.exists() {
            println!("Skipping REPL integration test - binary not found");
            return;
        }

        let config = ReplTestConfig {
            binary_path,
            timeout: Duration::from_secs(5),
            verbose: true,
            test_data_dir: None,
        };

        let mut suite = ReplTestSuite::new(config);

        // Run just the basic tests
        suite.test_repl_startup().unwrap();
        suite.test_help_system().unwrap();
        suite.test_clean_exit().unwrap();

        suite.print_summary();

        // Check that at least basic functionality works
        let passed_count = suite.results.iter().filter(|r| r.passed).count();
        assert!(passed_count >= 2, "At least 2 basic tests should pass");
    }

    #[test]
    fn test_configuration_parsing() {
        let config = ReplTestConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert!(!config.verbose);
    }

    #[test]
    fn test_data_file_creation() {
        let temp_dir = TempDir::new().unwrap();
        let test_dir = temp_dir.path().to_path_buf();

        create_test_data_files(&test_dir).unwrap();

        assert!(test_dir.join("sample_queries.cql").exists());
        assert!(test_dir.join("test_config.toml").exists());

        let cql_content = fs::read_to_string(test_dir.join("sample_queries.cql")).unwrap();
        assert!(cql_content.contains("SELECT keyspace_name"));
    }
}
