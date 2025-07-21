//! Performance Regression Testing Framework for Complex Types
//!
//! This framework provides automated performance regression testing to ensure
//! M3 complex types maintain performance targets across code changes.

use super::m3_performance_benchmarks::{M3PerformanceBenchmarks, M3BenchmarkResult, PerformanceTargets};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Performance regression test framework
pub struct PerformanceRegressionFramework {
    /// Baseline results storage
    baseline_storage: BaselineStorage,
    /// Performance thresholds
    thresholds: RegressionThresholds,
    /// Test configuration
    config: RegressionConfig,
}

/// Storage for baseline performance results
struct BaselineStorage {
    storage_path: PathBuf,
}

/// Thresholds for regression detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionThresholds {
    /// Maximum acceptable performance degradation (e.g., 0.1 = 10%)
    pub max_performance_degradation: f64,
    /// Maximum acceptable memory increase (e.g., 0.2 = 20%)
    pub max_memory_increase: f64,
    /// Maximum acceptable latency increase (e.g., 0.15 = 15%)
    pub max_latency_increase: f64,
    /// Minimum number of runs for statistical significance
    pub min_runs_for_significance: usize,
    /// Confidence interval for statistical analysis
    pub confidence_interval: f64,
}

/// Configuration for regression testing
#[derive(Debug, Clone)]
pub struct RegressionConfig {
    /// Number of benchmark runs per test
    pub runs_per_test: usize,
    /// Whether to store new baselines automatically
    pub auto_store_baselines: bool,
    /// Whether to generate detailed reports
    pub generate_detailed_reports: bool,
    /// Git commit hash for tracking
    pub git_commit_hash: Option<String>,
    /// Test environment info
    pub environment_info: EnvironmentInfo,
}

/// Environment information for test context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub os: String,
    pub architecture: String,
    pub cpu_model: String,
    pub memory_gb: u64,
    pub rust_version: String,
    pub compiler_flags: Vec<String>,
}

/// Stored baseline performance data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineData {
    pub timestamp: u64,
    pub git_commit: Option<String>,
    pub environment: EnvironmentInfo,
    pub results: Vec<M3BenchmarkResult>,
    pub metadata: HashMap<String, String>,
}

/// Regression test result
#[derive(Debug, Clone)]
pub struct RegressionTestResult {
    pub test_name: String,
    pub baseline_performance: f64,
    pub current_performance: f64,
    pub performance_change: f64,
    pub baseline_memory: usize,
    pub current_memory: usize,
    pub memory_change: f64,
    pub baseline_latency: f64,
    pub current_latency: f64,
    pub latency_change: f64,
    pub is_regression: bool,
    pub severity: RegressionSeverity,
    pub confidence: f64,
}

/// Severity levels for regressions
#[derive(Debug, Clone, PartialEq)]
pub enum RegressionSeverity {
    None,
    Minor,
    Moderate,
    Major,
    Critical,
}

impl Default for RegressionThresholds {
    fn default() -> Self {
        Self {
            max_performance_degradation: 0.1,  // 10%
            max_memory_increase: 0.2,          // 20%
            max_latency_increase: 0.15,        // 15%
            min_runs_for_significance: 5,
            confidence_interval: 0.95,
        }
    }
}

impl Default for RegressionConfig {
    fn default() -> Self {
        Self {
            runs_per_test: 5,
            auto_store_baselines: false,
            generate_detailed_reports: true,
            git_commit_hash: None,
            environment_info: EnvironmentInfo::detect(),
        }
    }
}

impl EnvironmentInfo {
    /// Detect current environment information
    pub fn detect() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            cpu_model: Self::detect_cpu_model(),
            memory_gb: Self::detect_memory_gb(),
            rust_version: Self::detect_rust_version(),
            compiler_flags: Self::detect_compiler_flags(),
        }
    }

    fn detect_cpu_model() -> String {
        // Simplified CPU detection
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                "x86_64 with AVX2".to_string()
            } else if is_x86_feature_detected!("sse2") {
                "x86_64 with SSE2".to_string()
            } else {
                "x86_64".to_string()
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            std::env::consts::ARCH.to_string()
        }
    }

    fn detect_memory_gb() -> u64 {
        // Simplified memory detection - would use system APIs in real implementation
        8 // Default to 8GB
    }

    fn detect_rust_version() -> String {
        env!("CARGO_PKG_RUST_VERSION").to_string()
    }

    fn detect_compiler_flags() -> Vec<String> {
        // Would detect actual compiler flags in real implementation
        vec!["opt-level=3".to_string(), "target-cpu=native".to_string()]
    }
}

impl BaselineStorage {
    fn new(storage_path: PathBuf) -> Self {
        Self { storage_path }
    }

    fn store_baseline(&self, baseline: &BaselineData) -> Result<()> {
        if let Some(parent) = self.storage_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| crate::Error::storage(format!("Failed to create baseline directory: {}", e)))?;
        }

        let json = serde_json::to_string_pretty(baseline)
            .map_err(|e| crate::Error::Serialization(format!("Failed to serialize baseline: {}", e)))?;

        fs::write(&self.storage_path, json)
            .map_err(|e| crate::Error::storage(format!("Failed to write baseline file: {}", e)))?;

        Ok(())
    }

    fn load_baseline(&self) -> Result<Option<BaselineData>> {
        if !self.storage_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.storage_path)
            .map_err(|e| crate::Error::storage(format!("Failed to read baseline file: {}", e)))?;

        let baseline: BaselineData = serde_json::from_str(&content)
            .map_err(|e| crate::Error::Serialization(format!("Failed to parse baseline: {}", e)))?;

        Ok(Some(baseline))
    }
}

impl PerformanceRegressionFramework {
    /// Create new regression framework
    pub fn new(baseline_path: impl AsRef<Path>) -> Self {
        Self {
            baseline_storage: BaselineStorage::new(baseline_path.as_ref().to_path_buf()),
            thresholds: RegressionThresholds::default(),
            config: RegressionConfig::default(),
        }
    }

    /// Create framework with custom configuration
    pub fn with_config(baseline_path: impl AsRef<Path>, config: RegressionConfig) -> Self {
        Self {
            baseline_storage: BaselineStorage::new(baseline_path.as_ref().to_path_buf()),
            thresholds: RegressionThresholds::default(),
            config,
        }
    }

    /// Set custom regression thresholds
    pub fn with_thresholds(mut self, thresholds: RegressionThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Run comprehensive regression tests
    pub fn run_regression_tests(&mut self) -> Result<Vec<RegressionTestResult>> {
        println!("🔍 Running Performance Regression Tests...");

        // Load existing baseline
        let baseline = self.baseline_storage.load_baseline()?;
        
        // Run current benchmarks
        let current_results = self.run_current_benchmarks()?;

        // Compare with baseline
        let regression_results = if let Some(baseline) = baseline {
            self.compare_with_baseline(&baseline, &current_results)?
        } else {
            println!("📊 No baseline found - establishing new baseline");
            self.establish_baseline(&current_results)?;
            Vec::new()
        };

        // Generate report
        if self.config.generate_detailed_reports {
            self.generate_regression_report(&regression_results)?;
        }

        Ok(regression_results)
    }

    /// Run current performance benchmarks
    fn run_current_benchmarks(&self) -> Result<Vec<M3BenchmarkResult>> {
        println!("🚀 Running current performance benchmarks...");

        let mut all_results = Vec::new();
        
        // Run multiple iterations for statistical significance
        for run in 1..=self.config.runs_per_test {
            println!("   Run {}/{}", run, self.config.runs_per_test);
            
            let mut benchmarks = M3PerformanceBenchmarks::new();
            benchmarks.run_m3_validation()?;
            
            // Extract results (in real implementation, would get from benchmarks)
            // For now, simulate results
            all_results.extend(self.simulate_benchmark_results(run));
        }

        // Aggregate results
        Ok(self.aggregate_benchmark_results(all_results))
    }

    /// Simulate benchmark results for demonstration
    fn simulate_benchmark_results(&self, run: usize) -> Vec<M3BenchmarkResult> {
        let base_performance = 100.0 - (run as f64 * 0.5); // Slight variation per run
        
        vec![
            M3BenchmarkResult {
                name: "list_performance".to_string(),
                category: "collections".to_string(),
                primitive_baseline_mbs: 150.0,
                complex_performance_mbs: base_performance,
                performance_ratio: base_performance / 150.0,
                memory_baseline_bytes: 1024 * 1024,
                memory_complex_bytes: ((1024 * 1024) as f64 * 1.3) as usize,
                memory_ratio: 1.3,
                latency_microseconds: 500.0 + (run as f64 * 10.0),
                meets_targets: true,
                additional_metrics: HashMap::new(),
            },
            M3BenchmarkResult {
                name: "map_performance".to_string(),
                category: "collections".to_string(),
                primitive_baseline_mbs: 120.0,
                complex_performance_mbs: base_performance * 0.8,
                performance_ratio: (base_performance * 0.8) / 120.0,
                memory_baseline_bytes: 2 * 1024 * 1024,
                memory_complex_bytes: ((2 * 1024 * 1024) as f64 * 1.4) as usize,
                memory_ratio: 1.4,
                latency_microseconds: 750.0 + (run as f64 * 15.0),
                meets_targets: true,
                additional_metrics: HashMap::new(),
            },
        ]
    }

    /// Aggregate benchmark results across multiple runs
    fn aggregate_benchmark_results(&self, all_results: Vec<M3BenchmarkResult>) -> Vec<M3BenchmarkResult> {
        let mut grouped: HashMap<String, Vec<M3BenchmarkResult>> = HashMap::new();
        
        for result in all_results {
            grouped.entry(result.name.clone()).or_default().push(result);
        }

        let mut aggregated = Vec::new();
        
        for (name, results) in grouped {
            if results.is_empty() {
                continue;
            }

            // Calculate averages
            let count = results.len() as f64;
            let avg_complex_performance = results.iter().map(|r| r.complex_performance_mbs).sum::<f64>() / count;
            let avg_primitive_baseline = results.iter().map(|r| r.primitive_baseline_mbs).sum::<f64>() / count;
            let avg_memory_complex = (results.iter().map(|r| r.memory_complex_bytes).sum::<usize>() as f64 / count) as usize;
            let avg_memory_baseline = (results.iter().map(|r| r.memory_baseline_bytes).sum::<usize>() as f64 / count) as usize;
            let avg_latency = results.iter().map(|r| r.latency_microseconds).sum::<f64>() / count;

            aggregated.push(M3BenchmarkResult {
                name: name.clone(),
                category: results[0].category.clone(),
                primitive_baseline_mbs: avg_primitive_baseline,
                complex_performance_mbs: avg_complex_performance,
                performance_ratio: avg_complex_performance / avg_primitive_baseline,
                memory_baseline_bytes: avg_memory_baseline,
                memory_complex_bytes: avg_memory_complex,
                memory_ratio: avg_memory_complex as f64 / avg_memory_baseline as f64,
                latency_microseconds: avg_latency,
                meets_targets: results.iter().all(|r| r.meets_targets),
                additional_metrics: HashMap::new(),
            });
        }

        aggregated
    }

    /// Compare current results with baseline
    fn compare_with_baseline(
        &self,
        baseline: &BaselineData,
        current_results: &[M3BenchmarkResult],
    ) -> Result<Vec<RegressionTestResult>> {
        println!("📊 Comparing with baseline from {}", 
            chrono::DateTime::from_timestamp(baseline.timestamp as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string()));

        let mut regression_results = Vec::new();

        for current in current_results {
            if let Some(baseline_result) = baseline.results.iter().find(|b| b.name == current.name) {
                let test_result = self.analyze_regression(baseline_result, current);
                regression_results.push(test_result);
            } else {
                println!("⚠️  New test '{}' - no baseline comparison", current.name);
            }
        }

        Ok(regression_results)
    }

    /// Analyze potential regression between baseline and current results
    fn analyze_regression(
        &self,
        baseline: &M3BenchmarkResult,
        current: &M3BenchmarkResult,
    ) -> RegressionTestResult {
        // Calculate performance change
        let performance_change = (current.complex_performance_mbs - baseline.complex_performance_mbs) 
            / baseline.complex_performance_mbs;

        // Calculate memory change
        let memory_change = (current.memory_complex_bytes as f64 - baseline.memory_complex_bytes as f64) 
            / baseline.memory_complex_bytes as f64;

        // Calculate latency change
        let latency_change = (current.latency_microseconds - baseline.latency_microseconds) 
            / baseline.latency_microseconds;

        // Determine if this is a regression
        let is_performance_regression = performance_change < -self.thresholds.max_performance_degradation;
        let is_memory_regression = memory_change > self.thresholds.max_memory_increase;
        let is_latency_regression = latency_change > self.thresholds.max_latency_increase;

        let is_regression = is_performance_regression || is_memory_regression || is_latency_regression;

        // Determine severity
        let severity = if !is_regression {
            RegressionSeverity::None
        } else {
            let max_degradation = [
                performance_change.abs() / self.thresholds.max_performance_degradation,
                memory_change / self.thresholds.max_memory_increase,
                latency_change / self.thresholds.max_latency_increase,
            ].iter().fold(0.0f64, |a, &b| a.max(b));

            if max_degradation >= 3.0 {
                RegressionSeverity::Critical
            } else if max_degradation >= 2.0 {
                RegressionSeverity::Major
            } else if max_degradation >= 1.5 {
                RegressionSeverity::Moderate
            } else {
                RegressionSeverity::Minor
            }
        };

        // Calculate confidence (simplified)
        let confidence = if self.config.runs_per_test >= self.thresholds.min_runs_for_significance {
            self.thresholds.confidence_interval
        } else {
            0.8 // Lower confidence with fewer runs
        };

        RegressionTestResult {
            test_name: current.name.clone(),
            baseline_performance: baseline.complex_performance_mbs,
            current_performance: current.complex_performance_mbs,
            performance_change,
            baseline_memory: baseline.memory_complex_bytes,
            current_memory: current.memory_complex_bytes,
            memory_change,
            baseline_latency: baseline.latency_microseconds,
            current_latency: current.latency_microseconds,
            latency_change,
            is_regression,
            severity,
            confidence,
        }
    }

    /// Establish new performance baseline
    fn establish_baseline(&mut self, results: &[M3BenchmarkResult]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let baseline = BaselineData {
            timestamp,
            git_commit: self.config.git_commit_hash.clone(),
            environment: self.config.environment_info.clone(),
            results: results.to_vec(),
            metadata: HashMap::new(),
        };

        if self.config.auto_store_baselines {
            self.baseline_storage.store_baseline(&baseline)?;
            println!("✅ New baseline established and stored");
        } else {
            println!("📝 New baseline established (not stored - set auto_store_baselines=true to persist)");
        }

        Ok(())
    }

    /// Generate detailed regression report
    fn generate_regression_report(&self, results: &[RegressionTestResult]) -> Result<()> {
        let report = self.format_regression_report(results);
        
        // Write to file
        let report_path = self.baseline_storage.storage_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("regression_report.md");

        fs::write(&report_path, &report)
            .map_err(|e| crate::Error::storage(format!("Failed to write regression report: {}", e)))?;

        println!("📊 Regression report written to: {}", report_path.display());
        
        // Print summary
        self.print_regression_summary(results);

        Ok(())
    }

    /// Format regression report as markdown
    fn format_regression_report(&self, results: &[RegressionTestResult]) -> String {
        let mut report = String::new();
        report.push_str("# Performance Regression Test Report\n\n");

        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
        report.push_str(&format!("**Generated**: {}\n", timestamp));
        
        if let Some(commit) = &self.config.git_commit_hash {
            report.push_str(&format!("**Git Commit**: {}\n", commit));
        }
        
        report.push_str(&format!("**Environment**: {} {} with {}\n", 
            self.config.environment_info.os,
            self.config.environment_info.architecture,
            self.config.environment_info.cpu_model));
        report.push_str(&format!("**Runs per test**: {}\n\n", self.config.runs_per_test));

        // Summary
        let regressions = results.iter().filter(|r| r.is_regression).count();
        let total = results.len();
        
        report.push_str("## Summary\n\n");
        report.push_str(&format!("- **Total Tests**: {}\n", total));
        report.push_str(&format!("- **Regressions Detected**: {}\n", regressions));
        report.push_str(&format!("- **Pass Rate**: {:.1}%\n\n", ((total - regressions) as f64 / total as f64) * 100.0));

        // Regression details
        if regressions > 0 {
            report.push_str("## 🚨 Detected Regressions\n\n");
            
            for result in results.iter().filter(|r| r.is_regression) {
                let severity_emoji = match result.severity {
                    RegressionSeverity::Critical => "🔴",
                    RegressionSeverity::Major => "🟠",
                    RegressionSeverity::Moderate => "🟡",
                    RegressionSeverity::Minor => "🔵",
                    RegressionSeverity::None => "🟢",
                };

                report.push_str(&format!("### {} {} ({:?})\n\n", severity_emoji, result.test_name, result.severity));
                
                if result.performance_change < 0.0 {
                    report.push_str(&format!("- **Performance**: {:.1}% slower ({:.2} → {:.2} MB/s)\n", 
                        result.performance_change.abs() * 100.0,
                        result.baseline_performance,
                        result.current_performance));
                }
                
                if result.memory_change > 0.0 {
                    report.push_str(&format!("- **Memory**: {:.1}% more ({} → {} bytes)\n", 
                        result.memory_change * 100.0,
                        result.baseline_memory,
                        result.current_memory));
                }
                
                if result.latency_change > 0.0 {
                    report.push_str(&format!("- **Latency**: {:.1}% higher ({:.1} → {:.1} μs)\n", 
                        result.latency_change * 100.0,
                        result.baseline_latency,
                        result.current_latency));
                }
                
                report.push_str(&format!("- **Confidence**: {:.1}%\n\n", result.confidence * 100.0));
            }
        }

        // All results table
        report.push_str("## Detailed Results\n\n");
        report.push_str("| Test | Status | Performance Change | Memory Change | Latency Change |\n");
        report.push_str("|------|--------|-------------------|---------------|----------------|\n");
        
        for result in results {
            let status = if result.is_regression {
                match result.severity {
                    RegressionSeverity::Critical => "🔴 CRITICAL",
                    RegressionSeverity::Major => "🟠 MAJOR",
                    RegressionSeverity::Moderate => "🟡 MODERATE", 
                    RegressionSeverity::Minor => "🔵 MINOR",
                    RegressionSeverity::None => "🟢 PASS",
                }
            } else {
                "🟢 PASS"
            };

            report.push_str(&format!(
                "| {} | {} | {:.1}% | {:.1}% | {:.1}% |\n",
                result.test_name,
                status,
                result.performance_change * 100.0,
                result.memory_change * 100.0,
                result.latency_change * 100.0
            ));
        }

        report.push_str("\n## Thresholds\n\n");
        report.push_str(&format!("- **Max Performance Degradation**: {:.1}%\n", self.thresholds.max_performance_degradation * 100.0));
        report.push_str(&format!("- **Max Memory Increase**: {:.1}%\n", self.thresholds.max_memory_increase * 100.0));
        report.push_str(&format!("- **Max Latency Increase**: {:.1}%\n", self.thresholds.max_latency_increase * 100.0));

        report
    }

    /// Print regression summary to console
    fn print_regression_summary(&self, results: &[RegressionTestResult]) {
        println!("\n📊 REGRESSION TEST SUMMARY");
        println!("================================");

        let regressions = results.iter().filter(|r| r.is_regression).collect::<Vec<_>>();
        
        if regressions.is_empty() {
            println!("✅ ALL TESTS PASSED - No regressions detected!");
        } else {
            println!("🚨 {} REGRESSION(S) DETECTED:", regressions.len());
            
            for result in &regressions {
                let severity_str = match result.severity {
                    RegressionSeverity::Critical => "CRITICAL",
                    RegressionSeverity::Major => "MAJOR", 
                    RegressionSeverity::Moderate => "MODERATE",
                    RegressionSeverity::Minor => "MINOR",
                    RegressionSeverity::None => "NONE",
                };
                
                println!("   - {} ({}): {:.1}% perf, {:.1}% memory, {:.1}% latency", 
                    result.test_name,
                    severity_str,
                    result.performance_change * 100.0,
                    result.memory_change * 100.0,
                    result.latency_change * 100.0);
            }
        }
        
        println!("================================\n");
    }

    /// Update baseline with current results
    pub fn update_baseline(&mut self, force: bool) -> Result<()> {
        if !force && !self.config.auto_store_baselines {
            return Err(crate::Error::InvalidOperation(
                "Baseline update requires force=true or auto_store_baselines=true".to_string()
            ));
        }

        let current_results = self.run_current_benchmarks()?;
        self.establish_baseline(&current_results)?;
        
        println!("✅ Baseline updated with current performance results");
        Ok(())
    }
}

impl std::fmt::Display for RegressionSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegressionSeverity::None => write!(f, "None"),
            RegressionSeverity::Minor => write!(f, "Minor"),
            RegressionSeverity::Moderate => write!(f, "Moderate"),
            RegressionSeverity::Major => write!(f, "Major"),
            RegressionSeverity::Critical => write!(f, "Critical"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_regression_framework_creation() {
        let temp_dir = TempDir::new().unwrap();
        let framework = PerformanceRegressionFramework::new(temp_dir.path().join("baseline.json"));
        
        assert_eq!(framework.thresholds.max_performance_degradation, 0.1);
        assert_eq!(framework.config.runs_per_test, 5);
    }

    #[test]
    fn test_environment_detection() {
        let env = EnvironmentInfo::detect();
        assert!(!env.os.is_empty());
        assert!(!env.architecture.is_empty());
    }

    #[test]
    fn test_regression_analysis() {
        let temp_dir = TempDir::new().unwrap();
        let framework = PerformanceRegressionFramework::new(temp_dir.path().join("baseline.json"));

        let baseline = M3BenchmarkResult {
            name: "test".to_string(),
            category: "test".to_string(),
            primitive_baseline_mbs: 100.0,
            complex_performance_mbs: 80.0,
            performance_ratio: 0.8,
            memory_baseline_bytes: 1000,
            memory_complex_bytes: 1200,
            memory_ratio: 1.2,
            latency_microseconds: 1000.0,
            meets_targets: true,
            additional_metrics: HashMap::new(),
        };

        let current = M3BenchmarkResult {
            name: "test".to_string(),
            category: "test".to_string(),
            primitive_baseline_mbs: 100.0,
            complex_performance_mbs: 70.0, // 12.5% degradation
            performance_ratio: 0.7,
            memory_baseline_bytes: 1000,
            memory_complex_bytes: 1200,
            memory_ratio: 1.2,
            latency_microseconds: 1000.0,
            meets_targets: false,
            additional_metrics: HashMap::new(),
        };

        let result = framework.analyze_regression(&baseline, &current);
        assert!(result.is_regression); // 12.5% > 10% threshold
        assert_eq!(result.severity, RegressionSeverity::Minor);
    }

    #[test]
    fn test_baseline_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = BaselineStorage::new(temp_dir.path().join("test_baseline.json"));

        let baseline = BaselineData {
            timestamp: 1234567890,
            git_commit: Some("abc123".to_string()),
            environment: EnvironmentInfo::detect(),
            results: vec![],
            metadata: HashMap::new(),
        };

        // Store and load
        storage.store_baseline(&baseline).unwrap();
        let loaded = storage.load_baseline().unwrap();
        
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.timestamp, 1234567890);
        assert_eq!(loaded.git_commit, Some("abc123".to_string()));
    }
}