#!/usr/bin/env cargo

//! Performance Regression Testing Framework for CQLite - Issue #17
//!
//! This module implements comprehensive performance regression testing to ensure
//! CQLite maintains or improves performance across releases and code changes.
//!
//! CRITICAL SUCCESS FACTOR: Command-line test execution MUST work reliably!

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Arg, Command as ClapCommand};
use serde::{Deserialize, Serialize};

// Performance benchmark configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBenchmarkConfig {
    pub name: String,
    pub description: String,
    pub command: String,
    pub args: Vec<String>,
    pub timeout_seconds: u64,
    pub warmup_iterations: u32,
    pub benchmark_iterations: u32,
    pub baseline_threshold_percent: f64,
    pub regression_threshold_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub min_duration_ms: f64,
    pub max_duration_ms: f64,
    pub mean_duration_ms: f64,
    pub median_duration_ms: f64,
    pub std_dev_ms: f64,
    pub p95_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub throughput_ops_per_sec: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub config: PerformanceBenchmarkConfig,
    pub metrics: PerformanceMetrics,
    pub raw_durations_ms: Vec<f64>,
    pub success: bool,
    pub error_message: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceRegressionReport {
    pub baseline_file: Option<PathBuf>,
    pub current_results: Vec<BenchmarkResult>,
    pub baseline_results: Option<Vec<BenchmarkResult>>,
    pub regressions: Vec<RegressionAnalysis>,
    pub improvements: Vec<ImprovementAnalysis>,
    pub summary: PerformanceSummary,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionAnalysis {
    pub benchmark_name: String,
    pub metric: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub change_percent: f64,
    pub threshold_percent: f64,
    pub severity: RegressionSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImprovementAnalysis {
    pub benchmark_name: String,
    pub metric: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub improvement_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegressionSeverity {
    Minor,    // 5-15% regression
    Major,    // 15-30% regression
    Critical, // >30% regression
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub total_benchmarks: usize,
    pub successful_benchmarks: usize,
    pub failed_benchmarks: usize,
    pub regressions_count: usize,
    pub improvements_count: usize,
    pub overall_performance_change_percent: f64,
    pub recommendation: String,
}

pub struct PerformanceRegressionTester {
    config_file: PathBuf,
    baseline_file: Option<PathBuf>,
    output_dir: PathBuf,
    benchmarks: Vec<PerformanceBenchmarkConfig>,
    verbose: bool,
}

impl PerformanceRegressionTester {
    pub fn new(
        config_file: PathBuf,
        baseline_file: Option<PathBuf>,
        output_dir: PathBuf,
        verbose: bool,
    ) -> Result<Self> {
        let benchmarks = Self::load_benchmark_configs(&config_file)?;

        Ok(PerformanceRegressionTester {
            config_file,
            baseline_file,
            output_dir,
            benchmarks,
            verbose,
        })
    }

    fn load_benchmark_configs(config_file: &Path) -> Result<Vec<PerformanceBenchmarkConfig>> {
        let content = fs::read_to_string(config_file)
            .context("Failed to read benchmark configuration file")?;

        let configs: Vec<PerformanceBenchmarkConfig> =
            serde_json::from_str(&content).context("Failed to parse benchmark configuration")?;

        Ok(configs)
    }

    fn load_baseline_results(&self) -> Result<Option<Vec<BenchmarkResult>>> {
        if let Some(baseline_file) = &self.baseline_file {
            if baseline_file.exists() {
                let content = fs::read_to_string(baseline_file)
                    .context("Failed to read baseline results file")?;

                let report: PerformanceRegressionReport =
                    serde_json::from_str(&content).context("Failed to parse baseline results")?;

                return Ok(Some(report.current_results));
            }
        }
        Ok(None)
    }

    pub fn run_all_benchmarks(&self) -> Result<Vec<BenchmarkResult>> {
        println!("🚀 Starting Performance Regression Testing");
        println!("Configuration: {} benchmarks", self.benchmarks.len());
        println!("Output directory: {}", self.output_dir.display());
        println!();

        let mut results = Vec::new();

        for benchmark in &self.benchmarks {
            println!("🔍 Running benchmark: {}", benchmark.name);
            if self.verbose {
                println!("  Description: {}", benchmark.description);
                println!(
                    "  Command: {} {}",
                    benchmark.command,
                    benchmark.args.join(" ")
                );
            }

            match self.run_single_benchmark(benchmark) {
                Ok(result) => {
                    if result.success {
                        println!("  ✅ Completed successfully");
                        println!(
                            "    Mean: {:.2}ms, P99: {:.2}ms",
                            result.metrics.mean_duration_ms, result.metrics.p99_duration_ms
                        );
                        println!(
                            "    Throughput: {:.2} ops/sec",
                            result.metrics.throughput_ops_per_sec
                        );
                    } else {
                        println!(
                            "  ❌ Failed: {}",
                            result.error_message.as_deref().unwrap_or("Unknown error")
                        );
                    }
                    results.push(result);
                }
                Err(e) => {
                    println!("  💥 Benchmark execution failed: {}", e);
                    results.push(BenchmarkResult {
                        config: benchmark.clone(),
                        metrics: PerformanceMetrics {
                            min_duration_ms: 0.0,
                            max_duration_ms: 0.0,
                            mean_duration_ms: 0.0,
                            median_duration_ms: 0.0,
                            std_dev_ms: 0.0,
                            p95_duration_ms: 0.0,
                            p99_duration_ms: 0.0,
                            throughput_ops_per_sec: 0.0,
                            memory_usage_mb: 0.0,
                            cpu_usage_percent: 0.0,
                        },
                        raw_durations_ms: vec![],
                        success: false,
                        error_message: Some(e.to_string()),
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
            println!();
        }

        Ok(results)
    }

    fn run_single_benchmark(&self, config: &PerformanceBenchmarkConfig) -> Result<BenchmarkResult> {
        let mut durations = Vec::new();
        let total_iterations = config.warmup_iterations + config.benchmark_iterations;

        // Run warmup iterations
        if self.verbose {
            println!(
                "    Running {} warmup iterations...",
                config.warmup_iterations
            );
        }

        for i in 0..config.warmup_iterations {
            if self.verbose && i % 10 == 0 {
                println!(
                    "      Warmup iteration {}/{}",
                    i + 1,
                    config.warmup_iterations
                );
            }
            let _ = self.execute_benchmark_command(config)?;
        }

        // Run actual benchmark iterations
        if self.verbose {
            println!(
                "    Running {} benchmark iterations...",
                config.benchmark_iterations
            );
        }

        for i in 0..config.benchmark_iterations {
            if self.verbose && i % 10 == 0 {
                println!(
                    "      Benchmark iteration {}/{}",
                    i + 1,
                    config.benchmark_iterations
                );
            }

            let duration = self.execute_benchmark_command(config)?;
            durations.push(duration);
        }

        // Calculate metrics
        let metrics = self.calculate_performance_metrics(&durations);

        Ok(BenchmarkResult {
            config: config.clone(),
            metrics,
            raw_durations_ms: durations,
            success: true,
            error_message: None,
            timestamp: chrono::Utc::now(),
        })
    }

    fn execute_benchmark_command(&self, config: &PerformanceBenchmarkConfig) -> Result<f64> {
        let start_time = Instant::now();

        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args);
        cmd.stdout(if self.verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        });
        cmd.stderr(if self.verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        });

        let output = cmd
            .output()
            .context("Failed to execute benchmark command")?;

        let duration = start_time.elapsed();

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "Benchmark command failed with exit code: {:?}",
                output.status.code()
            ));
        }

        Ok(duration.as_millis() as f64)
    }

    fn calculate_performance_metrics(&self, durations: &[f64]) -> PerformanceMetrics {
        let mut sorted_durations = durations.to_vec();
        sorted_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted_durations.len();
        let min_duration = sorted_durations[0];
        let max_duration = sorted_durations[len - 1];
        let mean_duration = sorted_durations.iter().sum::<f64>() / len as f64;
        let median_duration = if len % 2 == 0 {
            (sorted_durations[len / 2 - 1] + sorted_durations[len / 2]) / 2.0
        } else {
            sorted_durations[len / 2]
        };

        // Calculate standard deviation
        let variance = sorted_durations
            .iter()
            .map(|x| (x - mean_duration).powi(2))
            .sum::<f64>()
            / len as f64;
        let std_dev = variance.sqrt();

        // Calculate percentiles
        let p95_index = (len as f64 * 0.95).ceil() as usize - 1;
        let p99_index = (len as f64 * 0.99).ceil() as usize - 1;
        let p95_duration = sorted_durations[p95_index.min(len - 1)];
        let p99_duration = sorted_durations[p99_index.min(len - 1)];

        // Calculate throughput (operations per second)
        let throughput_ops_per_sec = if mean_duration > 0.0 {
            1000.0 / mean_duration
        } else {
            0.0
        };

        // TODO: Implement actual memory and CPU monitoring
        let memory_usage_mb = 0.0;
        let cpu_usage_percent = 0.0;

        PerformanceMetrics {
            min_duration_ms: min_duration,
            max_duration_ms: max_duration,
            mean_duration_ms: mean_duration,
            median_duration_ms: median_duration,
            std_dev_ms: std_dev,
            p95_duration_ms: p95_duration,
            p99_duration_ms: p99_duration,
            throughput_ops_per_sec,
            memory_usage_mb,
            cpu_usage_percent,
        }
    }

    pub fn analyze_regressions(
        &self,
        current_results: &[BenchmarkResult],
    ) -> Result<PerformanceRegressionReport> {
        let baseline_results = self.load_baseline_results()?;
        let mut regressions = Vec::new();
        let mut improvements = Vec::new();

        if let Some(baseline) = &baseline_results {
            // Create lookup map for baseline results
            let baseline_map: HashMap<String, &BenchmarkResult> = baseline
                .iter()
                .map(|r| (r.config.name.clone(), r))
                .collect();

            for current in current_results {
                if let Some(baseline_result) = baseline_map.get(&current.config.name) {
                    self.analyze_single_benchmark_regression(
                        current,
                        baseline_result,
                        &mut regressions,
                        &mut improvements,
                    );
                }
            }
        }

        let summary =
            self.generate_performance_summary(current_results, &regressions, &improvements);

        Ok(PerformanceRegressionReport {
            baseline_file: self.baseline_file.clone(),
            current_results: current_results.to_vec(),
            baseline_results,
            regressions,
            improvements,
            summary,
            generated_at: chrono::Utc::now(),
        })
    }

    fn analyze_single_benchmark_regression(
        &self,
        current: &BenchmarkResult,
        baseline: &BenchmarkResult,
        regressions: &mut Vec<RegressionAnalysis>,
        improvements: &mut Vec<ImprovementAnalysis>,
    ) {
        let metrics_to_analyze = [
            (
                "mean_duration_ms",
                current.metrics.mean_duration_ms,
                baseline.metrics.mean_duration_ms,
            ),
            (
                "p95_duration_ms",
                current.metrics.p95_duration_ms,
                baseline.metrics.p95_duration_ms,
            ),
            (
                "p99_duration_ms",
                current.metrics.p99_duration_ms,
                baseline.metrics.p99_duration_ms,
            ),
            (
                "throughput_ops_per_sec",
                current.metrics.throughput_ops_per_sec,
                baseline.metrics.throughput_ops_per_sec,
            ),
        ];

        for (metric_name, current_value, baseline_value) in metrics_to_analyze {
            if baseline_value == 0.0 {
                continue; // Skip division by zero
            }

            let change_percent = ((current_value - baseline_value) / baseline_value) * 100.0;
            let threshold = current.config.regression_threshold_percent;

            // For throughput, higher is better, so invert the logic
            let is_regression = if metric_name == "throughput_ops_per_sec" {
                change_percent < -threshold
            } else {
                change_percent > threshold
            };

            let is_improvement = if metric_name == "throughput_ops_per_sec" {
                change_percent > 5.0 // At least 5% improvement
            } else {
                change_percent < -5.0 // At least 5% improvement (reduction in time)
            };

            if is_regression {
                let severity = if change_percent.abs() > 30.0 {
                    RegressionSeverity::Critical
                } else if change_percent.abs() > 15.0 {
                    RegressionSeverity::Major
                } else {
                    RegressionSeverity::Minor
                };

                regressions.push(RegressionAnalysis {
                    benchmark_name: current.config.name.clone(),
                    metric: metric_name.to_string(),
                    baseline_value,
                    current_value,
                    change_percent,
                    threshold_percent: threshold,
                    severity,
                });
            } else if is_improvement {
                improvements.push(ImprovementAnalysis {
                    benchmark_name: current.config.name.clone(),
                    metric: metric_name.to_string(),
                    baseline_value,
                    current_value,
                    improvement_percent: change_percent.abs(),
                });
            }
        }
    }

    fn generate_performance_summary(
        &self,
        current_results: &[BenchmarkResult],
        regressions: &[RegressionAnalysis],
        improvements: &[ImprovementAnalysis],
    ) -> PerformanceSummary {
        let total_benchmarks = current_results.len();
        let successful_benchmarks = current_results.iter().filter(|r| r.success).count();
        let failed_benchmarks = total_benchmarks - successful_benchmarks;

        // Calculate overall performance change (simplified)
        let overall_performance_change = if !regressions.is_empty() || !improvements.is_empty() {
            let regression_impact: f64 = regressions.iter().map(|r| r.change_percent.abs()).sum();
            let improvement_impact: f64 = improvements.iter().map(|i| i.improvement_percent).sum();
            improvement_impact - regression_impact
        } else {
            0.0
        };

        let recommendation = if regressions
            .iter()
            .any(|r| matches!(r.severity, RegressionSeverity::Critical))
        {
            "❌ CRITICAL REGRESSIONS DETECTED - Do not merge this change".to_string()
        } else if regressions
            .iter()
            .any(|r| matches!(r.severity, RegressionSeverity::Major))
        {
            "⚠️ MAJOR REGRESSIONS DETECTED - Investigate before merging".to_string()
        } else if !regressions.is_empty() {
            "🔍 Minor regressions detected - Review performance impact".to_string()
        } else if !improvements.is_empty() {
            "✅ Performance improvements detected - Good to merge".to_string()
        } else {
            "✅ No significant performance changes detected".to_string()
        };

        PerformanceSummary {
            total_benchmarks,
            successful_benchmarks,
            failed_benchmarks,
            regressions_count: regressions.len(),
            improvements_count: improvements.len(),
            overall_performance_change_percent: overall_performance_change,
            recommendation,
        }
    }

    pub fn save_results(&self, report: &PerformanceRegressionReport) -> Result<PathBuf> {
        fs::create_dir_all(&self.output_dir).context("Failed to create output directory")?;

        let output_file = self.output_dir.join(format!(
            "performance_regression_report_{}.json",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));

        let json = serde_json::to_string_pretty(report)
            .context("Failed to serialize regression report")?;

        fs::write(&output_file, json).context("Failed to write regression report")?;

        Ok(output_file)
    }

    pub fn generate_html_report(&self, report: &PerformanceRegressionReport) -> Result<PathBuf> {
        let html_file = self.output_dir.join(format!(
            "performance_regression_report_{}.html",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));

        let html_content = self.create_html_report_content(report);

        fs::write(&html_file, html_content).context("Failed to write HTML report")?;

        Ok(html_file)
    }

    fn create_html_report_content(&self, report: &PerformanceRegressionReport) -> String {
        format!(
            r#"
<!DOCTYPE html>
<html>
<head>
    <title>CQLite Performance Regression Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #f5f5f5; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .summary {{ background: #e7f3ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .regression {{ background: #ffebee; padding: 10px; border-left: 4px solid #f44336; margin: 10px 0; }}
        .improvement {{ background: #e8f5e8; padding: 10px; border-left: 4px solid #4caf50; margin: 10px 0; }}
        .critical {{ border-left-color: #d32f2f; }}
        .major {{ border-left-color: #f57c00; }}
        .minor {{ border-left-color: #fbc02d; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f2f2f2; }}
        .metric {{ font-family: monospace; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CQLite Performance Regression Report</h1>
        <p><strong>Generated:</strong> {}</p>
        <p><strong>Total Benchmarks:</strong> {}</p>
        <p><strong>Baseline File:</strong> {}</p>
    </div>
    
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Successful Benchmarks:</strong> {}</p>
        <p><strong>Failed Benchmarks:</strong> {}</p>
        <p><strong>Regressions:</strong> {}</p>
        <p><strong>Improvements:</strong> {}</p>
        <p><strong>Overall Performance Change:</strong> {:.2}%</p>
        <p><strong>Recommendation:</strong> {}</p>
    </div>
    
    <h2>Regressions</h2>
    {}
    
    <h2>Improvements</h2>
    {}
    
    <h2>Detailed Results</h2>
    <table>
        <thead>
            <tr>
                <th>Benchmark</th>
                <th>Status</th>
                <th>Mean Duration (ms)</th>
                <th>P99 Duration (ms)</th>
                <th>Throughput (ops/sec)</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
</body>
</html>
        "#,
            report.generated_at.format("%Y-%m-%d %H:%M:%S UTC"),
            report.summary.total_benchmarks,
            report
                .baseline_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or("None".to_string()),
            report.summary.successful_benchmarks,
            report.summary.failed_benchmarks,
            report.summary.regressions_count,
            report.summary.improvements_count,
            report.summary.overall_performance_change_percent,
            report.summary.recommendation,
            self.format_regressions_html(&report.regressions),
            self.format_improvements_html(&report.improvements),
            self.format_detailed_results_html(&report.current_results)
        )
    }

    fn format_regressions_html(&self, regressions: &[RegressionAnalysis]) -> String {
        if regressions.is_empty() {
            return "<p>No regressions detected.</p>".to_string();
        }

        regressions
            .iter()
            .map(|r| {
                let class = match r.severity {
                    RegressionSeverity::Critical => "regression critical",
                    RegressionSeverity::Major => "regression major",
                    RegressionSeverity::Minor => "regression minor",
                };
                format!(
                    r#"<div class="{}">
                        <strong>{}</strong> - {} regression in {}
                        <br>Baseline: {:.2}, Current: {:.2}, Change: {:.2}%
                    </div>"#,
                    class,
                    r.benchmark_name,
                    match r.severity {
                        RegressionSeverity::Critical => "CRITICAL",
                        RegressionSeverity::Major => "MAJOR",
                        RegressionSeverity::Minor => "Minor",
                    },
                    r.metric,
                    r.baseline_value,
                    r.current_value,
                    r.change_percent
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_improvements_html(&self, improvements: &[ImprovementAnalysis]) -> String {
        if improvements.is_empty() {
            return "<p>No improvements detected.</p>".to_string();
        }

        improvements
            .iter()
            .map(|i| {
                format!(
                    r#"<div class="improvement">
                    <strong>{}</strong> - {:.2}% improvement in {}
                    <br>Baseline: {:.2}, Current: {:.2}
                </div>"#,
                    i.benchmark_name,
                    i.improvement_percent,
                    i.metric,
                    i.baseline_value,
                    i.current_value
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_detailed_results_html(&self, results: &[BenchmarkResult]) -> String {
        results
            .iter()
            .map(|r| {
                format!(
                    r#"<tr>
                    <td>{}</td>
                    <td>{}</td>
                    <td class="metric">{:.2}</td>
                    <td class="metric">{:.2}</td>
                    <td class="metric">{:.2}</td>
                </tr>"#,
                    r.config.name,
                    if r.success {
                        "✅ Success"
                    } else {
                        "❌ Failed"
                    },
                    r.metrics.mean_duration_ms,
                    r.metrics.p99_duration_ms,
                    r.metrics.throughput_ops_per_sec
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// Default benchmark configurations
fn create_default_benchmark_config() -> Result<Vec<PerformanceBenchmarkConfig>> {
    Ok(vec![
        PerformanceBenchmarkConfig {
            name: "sstable_parsing_small".to_string(),
            description: "Parse small SSTable files (< 1MB)".to_string(),
            command: "cargo".to_string(),
            args: vec![
                "run",
                "--release",
                "--package",
                "cqlite-cli",
                "--",
                "info",
                "test-data/generated/v4.1/sstables/simple_table/na-1-big-Data.db",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            timeout_seconds: 60,
            warmup_iterations: 5,
            benchmark_iterations: 20,
            baseline_threshold_percent: 5.0,
            regression_threshold_percent: 10.0,
        },
        PerformanceBenchmarkConfig {
            name: "sstable_parsing_large".to_string(),
            description: "Parse large SSTable files (> 10MB)".to_string(),
            command: "cargo".to_string(),
            args: vec![
                "run",
                "--release",
                "--package",
                "cqlite-cli",
                "--",
                "info",
                "test-data/generated/v4.1/sstables/large_blob_table/na-1-big-Data.db",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            timeout_seconds: 120,
            warmup_iterations: 3,
            benchmark_iterations: 10,
            baseline_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        },
        PerformanceBenchmarkConfig {
            name: "collection_processing".to_string(),
            description: "Process collections and complex types".to_string(),
            command: "cargo".to_string(),
            args: vec![
                "run",
                "--release",
                "--package",
                "cqlite-cli",
                "--",
                "info",
                "test-data/generated/v4.1/sstables/collections_table/na-1-big-Data.db",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            timeout_seconds: 90,
            warmup_iterations: 5,
            benchmark_iterations: 15,
            baseline_threshold_percent: 5.0,
            regression_threshold_percent: 12.0,
        },
        PerformanceBenchmarkConfig {
            name: "comprehensive_test_suite".to_string(),
            description: "Run full test suite performance".to_string(),
            command: "cargo".to_string(),
            args: vec!["test", "--release", "--package", "tests"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            timeout_seconds: 300,
            warmup_iterations: 1,
            benchmark_iterations: 5,
            baseline_threshold_percent: 10.0,
            regression_threshold_percent: 20.0,
        },
    ])
}

fn main() -> Result<()> {
    let matches = ClapCommand::new("performance-regression-test-runner")
        .version("1.0.0")
        .about("Performance Regression Testing Framework for CQLite - Issue #17")
        .arg(
            Arg::new("config")
                .long("config")
                .short('c')
                .value_name("FILE")
                .help("Benchmark configuration file")
                .default_value("performance_benchmarks.json"),
        )
        .arg(
            Arg::new("baseline")
                .long("baseline")
                .short('b')
                .value_name("FILE")
                .help("Baseline results file for comparison"),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .value_name("DIR")
                .help("Output directory for results")
                .default_value("reports/performance"),
        )
        .arg(
            Arg::new("generate-config")
                .long("generate-config")
                .help("Generate default benchmark configuration")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("html")
                .long("html")
                .help("Generate HTML report")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose output")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let verbose = matches.get_flag("verbose");

    // Generate default config if requested
    if matches.get_flag("generate-config") {
        let config_file = matches.get_one::<String>("config").unwrap();
        let default_config = create_default_benchmark_config()?;
        let json = serde_json::to_string_pretty(&default_config)?;
        fs::write(config_file, json)?;
        println!(
            "✅ Generated default benchmark configuration: {}",
            config_file
        );
        return Ok(());
    }

    let config_file = PathBuf::from(matches.get_one::<String>("config").unwrap());
    let baseline_file = matches.get_one::<String>("baseline").map(PathBuf::from);
    let output_dir = PathBuf::from(matches.get_one::<String>("output").unwrap());
    let generate_html = matches.get_flag("html");

    // Check if config file exists
    if !config_file.exists() {
        eprintln!("❌ Configuration file not found: {}", config_file.display());
        eprintln!("💡 Use --generate-config to create a default configuration");
        std::process::exit(1);
    }

    // Create performance regression tester
    let tester = PerformanceRegressionTester::new(config_file, baseline_file, output_dir, verbose)?;

    // Run all benchmarks
    let results = tester.run_all_benchmarks()?;

    // Analyze for regressions
    let report = tester.analyze_regressions(&results)?;

    // Save results
    let json_file = tester.save_results(&report)?;
    println!("📄 Results saved to: {}", json_file.display());

    // Generate HTML report if requested
    if generate_html {
        let html_file = tester.generate_html_report(&report)?;
        println!("🌐 HTML report generated: {}", html_file.display());
    }

    // Print summary
    println!();
    println!("📊 Performance Regression Testing Summary");
    println!("=========================================");
    println!("Total benchmarks: {}", report.summary.total_benchmarks);
    println!("Successful: {}", report.summary.successful_benchmarks);
    println!("Failed: {}", report.summary.failed_benchmarks);
    println!("Regressions: {}", report.summary.regressions_count);
    println!("Improvements: {}", report.summary.improvements_count);
    println!(
        "Overall change: {:.2}%",
        report.summary.overall_performance_change_percent
    );
    println!();
    println!("Recommendation: {}", report.summary.recommendation);

    // Print detailed regressions
    if !report.regressions.is_empty() {
        println!();
        println!("🔍 Detected Regressions:");
        for regression in &report.regressions {
            println!(
                "  • {} ({}): {:.2}% regression in {}",
                regression.benchmark_name,
                match regression.severity {
                    RegressionSeverity::Critical => "CRITICAL",
                    RegressionSeverity::Major => "MAJOR",
                    RegressionSeverity::Minor => "Minor",
                },
                regression.change_percent,
                regression.metric
            );
        }
    }

    // Exit with appropriate code
    let exit_code = if report
        .regressions
        .iter()
        .any(|r| matches!(r.severity, RegressionSeverity::Critical))
    {
        2 // Critical regressions
    } else if report
        .regressions
        .iter()
        .any(|r| matches!(r.severity, RegressionSeverity::Major))
    {
        1 // Major regressions
    } else {
        0 // No significant regressions
    };

    std::process::exit(exit_code);
}
