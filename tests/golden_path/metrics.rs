//! Metrics Collection and Performance Tracking
//!
//! This module provides comprehensive performance metrics collection and
//! regression detection for golden-path testing scenarios.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::golden_path::{TestMetrics, ComponentTiming};

/// Comprehensive metrics collector for performance tracking
pub struct MetricsCollector {
    /// Whether detailed metrics collection is enabled
    detailed_enabled: bool,
    /// Active collection sessions
    active_sessions: Arc<Mutex<HashMap<String, MetricsSession>>>,
    /// Historical performance data
    historical_data: Arc<Mutex<HashMap<String, Vec<HistoricalMetrics>>>>,
}

/// Active metrics collection session
#[derive(Debug)]
struct MetricsSession {
    /// Session start time
    start_time: Instant,
    /// Scenario being measured
    scenario_name: String,
    /// Operation count
    operation_count: usize,
    /// Component timing breakdown
    component_timings: ComponentTimingTracker,
    /// Memory usage tracking
    memory_tracker: MemoryTracker,
    /// Cache performance tracking
    cache_tracker: CacheTracker,
}

/// Component timing tracker
#[derive(Debug)]
struct ComponentTimingTracker {
    /// Summary component timing
    summary_time: Duration,
    /// Index component timing
    index_time: Duration,
    /// Data component timing
    data_time: Duration,
    /// Coordination overhead
    coordination_overhead: Duration,
}

/// Memory usage tracker
#[derive(Debug)]
struct MemoryTracker {
    /// Initial memory usage
    initial_memory_kb: usize,
    /// Peak memory usage during test
    peak_memory_kb: usize,
    /// Current memory usage
    current_memory_kb: usize,
}

/// Cache performance tracker
#[derive(Debug)]
struct CacheTracker {
    /// Cache hits
    cache_hits: usize,
    /// Cache misses
    cache_misses: usize,
    /// Bloom filter hits
    bloom_filter_hits: usize,
    /// Bloom filter misses
    bloom_filter_misses: usize,
}

/// Historical metrics for comparison
#[derive(Debug, Clone)]
pub struct HistoricalMetrics {
    /// Timestamp of measurement
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Test scenario name
    pub scenario: String,
    /// Performance metrics
    pub metrics: TestMetrics,
    /// Environment information
    pub environment: EnvironmentInfo,
    /// Build/version information
    pub build_info: BuildInfo,
}

/// Environment information for context
#[derive(Debug, Clone)]
pub struct EnvironmentInfo {
    /// Operating system
    pub os: String,
    /// CPU information
    pub cpu: String,
    /// Available memory
    pub memory_gb: usize,
    /// Disk type (SSD/HDD)
    pub disk_type: String,
}

/// Build information for tracking changes
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// Git commit hash
    pub commit_hash: String,
    /// Git branch
    pub branch: String,
    /// Build timestamp
    pub build_time: String,
    /// Rust version used
    pub rust_version: String,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(detailed_enabled: bool) -> Self {
        Self {
            detailed_enabled,
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            historical_data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start collecting metrics for a scenario
    pub fn start_collection(&self, scenario_name: &str) -> MetricsSessionHandle {
        let session = MetricsSession {
            start_time: Instant::now(),
            scenario_name: scenario_name.to_string(),
            operation_count: 0,
            component_timings: ComponentTimingTracker {
                summary_time: Duration::ZERO,
                index_time: Duration::ZERO,
                data_time: Duration::ZERO,
                coordination_overhead: Duration::ZERO,
            },
            memory_tracker: MemoryTracker {
                initial_memory_kb: Self::get_current_memory_usage(),
                peak_memory_kb: 0,
                current_memory_kb: 0,
            },
            cache_tracker: CacheTracker {
                cache_hits: 0,
                cache_misses: 0,
                bloom_filter_hits: 0,
                bloom_filter_misses: 0,
            },
        };

        {
            let mut sessions = self.active_sessions.lock().unwrap();
            sessions.insert(scenario_name.to_string(), session);
        }

        MetricsSessionHandle {
            scenario_name: scenario_name.to_string(),
            collector: self,
        }
    }

    /// Stop collecting metrics and return results
    pub fn stop_collection(&self, scenario_name: &str) -> TestMetrics {
        let session = {
            let mut sessions = self.active_sessions.lock().unwrap();
            sessions.remove(scenario_name)
        };

        match session {
            Some(session) => self.finalize_metrics(session),
            None => {
                // Return default metrics if session not found
                TestMetrics {
                    avg_latency: Duration::from_millis(0),
                    peak_memory_kb: 0,
                    cache_hit_rate: 0.0,
                    coordination_timing: ComponentTiming {
                        summary_time: Duration::ZERO,
                        index_time: Duration::ZERO,
                        data_time: Duration::ZERO,
                        coordination_overhead: Duration::ZERO,
                    },
                    throughput: 0.0,
                }
            }
        }
    }

    /// Finalize metrics calculation
    fn finalize_metrics(&self, session: MetricsSession) -> TestMetrics {
        let total_duration = session.start_time.elapsed();
        let avg_latency = if session.operation_count > 0 {
            total_duration / session.operation_count as u32
        } else {
            Duration::ZERO
        };

        let throughput = if total_duration.as_secs_f64() > 0.0 {
            session.operation_count as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };

        let cache_hit_rate = {
            let total_cache_ops = session.cache_tracker.cache_hits + session.cache_tracker.cache_misses;
            if total_cache_ops > 0 {
                session.cache_tracker.cache_hits as f64 / total_cache_ops as f64
            } else {
                0.0
            }
        };

        let metrics = TestMetrics {
            avg_latency,
            peak_memory_kb: session.memory_tracker.peak_memory_kb,
            cache_hit_rate,
            coordination_timing: ComponentTiming {
                summary_time: session.component_timings.summary_time,
                index_time: session.component_timings.index_time,
                data_time: session.component_timings.data_time,
                coordination_overhead: session.component_timings.coordination_overhead,
            },
            throughput,
        };

        // Store historical data if detailed tracking is enabled
        if self.detailed_enabled {
            self.store_historical_metrics(&session.scenario_name, &metrics);
        }

        metrics
    }

    /// Store metrics for historical comparison
    fn store_historical_metrics(&self, scenario_name: &str, metrics: &TestMetrics) {
        let historical_entry = HistoricalMetrics {
            timestamp: chrono::Utc::now(),
            scenario: scenario_name.to_string(),
            metrics: metrics.clone(),
            environment: Self::get_environment_info(),
            build_info: Self::get_build_info(),
        };

        let mut historical_data = self.historical_data.lock().unwrap();
        historical_data
            .entry(scenario_name.to_string())
            .or_insert_with(Vec::new)
            .push(historical_entry);

        // Keep only last 100 entries per scenario to prevent unbounded growth
        if let Some(entries) = historical_data.get_mut(scenario_name) {
            if entries.len() > 100 {
                entries.remove(0);
            }
        }
    }

    /// Get current memory usage (placeholder implementation)
    fn get_current_memory_usage() -> usize {
        // In a real implementation, this would use system calls or libraries
        // like `sys-info` or `psutil` to get actual memory usage
        1024 // Placeholder: 1MB
    }

    /// Get environment information
    fn get_environment_info() -> EnvironmentInfo {
        EnvironmentInfo {
            os: std::env::consts::OS.to_string(),
            cpu: "Unknown CPU".to_string(), // Would use system detection
            memory_gb: 8, // Would detect actual memory
            disk_type: "SSD".to_string(), // Would detect disk type
        }
    }

    /// Get build information
    fn get_build_info() -> BuildInfo {
        BuildInfo {
            commit_hash: env!("VERGEN_GIT_SHA").unwrap_or("unknown").to_string(),
            branch: env!("VERGEN_GIT_BRANCH").unwrap_or("unknown").to_string(),
            build_time: env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown").to_string(),
            rust_version: env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown").to_string(),
        }
    }

    /// Get historical metrics for a scenario
    pub fn get_historical_metrics(&self, scenario_name: &str) -> Vec<HistoricalMetrics> {
        let historical_data = self.historical_data.lock().unwrap();
        historical_data
            .get(scenario_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Analyze performance regression
    pub fn analyze_regression(&self, scenario_name: &str, current_metrics: &TestMetrics) -> RegressionAnalysis {
        let historical_metrics = self.get_historical_metrics(scenario_name);

        if historical_metrics.len() < 2 {
            return RegressionAnalysis {
                has_regression: false,
                regression_type: None,
                regression_percentage: 0.0,
                baseline_metrics: None,
                analysis_details: vec!["Insufficient historical data for regression analysis".to_string()],
            };
        }

        // Use the average of the last 5 runs as baseline
        let baseline_count = std::cmp::min(5, historical_metrics.len());
        let baseline_entries = &historical_metrics[historical_metrics.len() - baseline_count..];

        let baseline_latency = baseline_entries.iter()
            .map(|m| m.metrics.avg_latency.as_millis() as f64)
            .sum::<f64>() / baseline_count as f64;

        let baseline_throughput = baseline_entries.iter()
            .map(|m| m.metrics.throughput)
            .sum::<f64>() / baseline_count as f64;

        let current_latency = current_metrics.avg_latency.as_millis() as f64;
        let current_throughput = current_metrics.throughput;

        // Calculate regression percentages
        let latency_regression = ((current_latency - baseline_latency) / baseline_latency) * 100.0;
        let throughput_regression = ((baseline_throughput - current_throughput) / baseline_throughput) * 100.0;

        // Determine if there's significant regression (>10% by default)
        const REGRESSION_THRESHOLD: f64 = 10.0;
        let has_latency_regression = latency_regression > REGRESSION_THRESHOLD;
        let has_throughput_regression = throughput_regression > REGRESSION_THRESHOLD;

        let (has_regression, regression_type, regression_percentage) = if has_latency_regression && has_throughput_regression {
            (true, Some("Both latency and throughput".to_string()), f64::max(latency_regression, throughput_regression))
        } else if has_latency_regression {
            (true, Some("Latency".to_string()), latency_regression)
        } else if has_throughput_regression {
            (true, Some("Throughput".to_string()), throughput_regression)
        } else {
            (false, None, 0.0)
        };

        let baseline_metrics = Some(TestMetrics {
            avg_latency: Duration::from_millis(baseline_latency as u64),
            peak_memory_kb: baseline_entries.iter().map(|m| m.metrics.peak_memory_kb).sum::<usize>() / baseline_count,
            cache_hit_rate: baseline_entries.iter().map(|m| m.metrics.cache_hit_rate).sum::<f64>() / baseline_count as f64,
            coordination_timing: ComponentTiming {
                summary_time: Duration::from_millis(
                    baseline_entries.iter().map(|m| m.metrics.coordination_timing.summary_time.as_millis()).sum::<u128>() / baseline_count as u128
                ),
                index_time: Duration::from_millis(
                    baseline_entries.iter().map(|m| m.metrics.coordination_timing.index_time.as_millis()).sum::<u128>() / baseline_count as u128
                ),
                data_time: Duration::from_millis(
                    baseline_entries.iter().map(|m| m.metrics.coordination_timing.data_time.as_millis()).sum::<u128>() / baseline_count as u128
                ),
                coordination_overhead: Duration::from_millis(
                    baseline_entries.iter().map(|m| m.metrics.coordination_timing.coordination_overhead.as_millis()).sum::<u128>() / baseline_count as u128
                ),
            },
            throughput: baseline_throughput,
        });

        let mut analysis_details = vec![
            format!("Baseline latency: {:.2}ms (avg of {} runs)", baseline_latency, baseline_count),
            format!("Current latency: {:.2}ms", current_latency),
            format!("Latency change: {:.2}%", latency_regression),
            format!("Baseline throughput: {:.2} ops/sec", baseline_throughput),
            format!("Current throughput: {:.2} ops/sec", current_throughput),
            format!("Throughput change: {:.2}%", -throughput_regression),
        ];

        if has_regression {
            analysis_details.push(format!("⚠️  Performance regression detected in {}", regression_type.as_ref().unwrap()));
        } else {
            analysis_details.push("✅ No significant performance regression detected".to_string());
        }

        RegressionAnalysis {
            has_regression,
            regression_type,
            regression_percentage,
            baseline_metrics,
            analysis_details,
        }
    }

    /// Generate performance report
    pub fn generate_performance_report(&self) -> PerformanceReport {
        let historical_data = self.historical_data.lock().unwrap();
        let mut scenario_summaries = HashMap::new();

        for (scenario_name, metrics_history) in historical_data.iter() {
            if !metrics_history.is_empty() {
                let latest_metrics = &metrics_history.last().unwrap().metrics;
                let regression_analysis = self.analyze_regression(scenario_name, latest_metrics);

                scenario_summaries.insert(scenario_name.clone(), ScenarioSummary {
                    scenario_name: scenario_name.clone(),
                    latest_metrics: latest_metrics.clone(),
                    historical_count: metrics_history.len(),
                    regression_analysis,
                });
            }
        }

        PerformanceReport {
            generated_at: chrono::Utc::now(),
            scenario_summaries,
            overall_health: self.calculate_overall_health(&scenario_summaries),
        }
    }

    /// Calculate overall system health
    fn calculate_overall_health(&self, summaries: &HashMap<String, ScenarioSummary>) -> HealthStatus {
        let total_scenarios = summaries.len();
        if total_scenarios == 0 {
            return HealthStatus::Unknown;
        }

        let regression_count = summaries.values()
            .filter(|s| s.regression_analysis.has_regression)
            .count();

        let regression_percentage = (regression_count as f64 / total_scenarios as f64) * 100.0;

        if regression_percentage > 50.0 {
            HealthStatus::Critical
        } else if regression_percentage > 25.0 {
            HealthStatus::Warning
        } else if regression_percentage > 0.0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }
}

/// Handle for metrics collection session
pub struct MetricsSessionHandle<'a> {
    scenario_name: String,
    collector: &'a MetricsCollector,
}

impl<'a> MetricsSessionHandle<'a> {
    /// Record an operation
    pub fn record_operation(&self) {
        if let Ok(mut sessions) = self.collector.active_sessions.lock() {
            if let Some(session) = sessions.get_mut(&self.scenario_name) {
                session.operation_count += 1;
                session.memory_tracker.current_memory_kb = MetricsCollector::get_current_memory_usage();
                session.memory_tracker.peak_memory_kb = std::cmp::max(
                    session.memory_tracker.peak_memory_kb,
                    session.memory_tracker.current_memory_kb,
                );
            }
        }
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        if let Ok(mut sessions) = self.collector.active_sessions.lock() {
            if let Some(session) = sessions.get_mut(&self.scenario_name) {
                session.cache_tracker.cache_hits += 1;
            }
        }
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        if let Ok(mut sessions) = self.collector.active_sessions.lock() {
            if let Some(session) = sessions.get_mut(&self.scenario_name) {
                session.cache_tracker.cache_misses += 1;
            }
        }
    }

    /// Record component timing
    pub fn record_component_timing(&self, component: &str, duration: Duration) {
        if let Ok(mut sessions) = self.collector.active_sessions.lock() {
            if let Some(session) = sessions.get_mut(&self.scenario_name) {
                match component {
                    "summary" => session.component_timings.summary_time += duration,
                    "index" => session.component_timings.index_time += duration,
                    "data" => session.component_timings.data_time += duration,
                    "coordination" => session.component_timings.coordination_overhead += duration,
                    _ => {}
                }
            }
        }
    }
}

/// Regression analysis result
#[derive(Debug, Clone)]
pub struct RegressionAnalysis {
    /// Whether regression was detected
    pub has_regression: bool,
    /// Type of regression (latency, throughput, etc.)
    pub regression_type: Option<String>,
    /// Regression percentage
    pub regression_percentage: f64,
    /// Baseline metrics for comparison
    pub baseline_metrics: Option<TestMetrics>,
    /// Detailed analysis information
    pub analysis_details: Vec<String>,
}

/// Performance report
#[derive(Debug)]
pub struct PerformanceReport {
    /// Report generation timestamp
    pub generated_at: chrono::DateTime<chrono::Utc>,
    /// Summary for each scenario
    pub scenario_summaries: HashMap<String, ScenarioSummary>,
    /// Overall system health
    pub overall_health: HealthStatus,
}

/// Summary for a single scenario
#[derive(Debug, Clone)]
pub struct ScenarioSummary {
    /// Scenario name
    pub scenario_name: String,
    /// Latest metrics
    pub latest_metrics: TestMetrics,
    /// Number of historical data points
    pub historical_count: usize,
    /// Regression analysis
    pub regression_analysis: RegressionAnalysis,
}

/// Overall health status
#[derive(Debug, Clone)]
pub enum HealthStatus {
    /// All scenarios performing well
    Healthy,
    /// Some scenarios have minor regressions
    Degraded,
    /// Multiple scenarios have significant regressions
    Warning,
    /// Major performance issues detected
    Critical,
    /// Insufficient data for assessment
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new(true);
        assert!(collector.detailed_enabled);
    }

    #[test]
    fn test_metrics_session_lifecycle() {
        let collector = MetricsCollector::new(true);
        let _handle = collector.start_collection("test_scenario");
        let metrics = collector.stop_collection("test_scenario");

        assert_eq!(metrics.avg_latency, Duration::from_millis(0));
        assert_eq!(metrics.throughput, 0.0);
    }

    #[test]
    fn test_regression_analysis_insufficient_data() {
        let collector = MetricsCollector::new(true);
        let metrics = TestMetrics {
            avg_latency: Duration::from_millis(100),
            peak_memory_kb: 1024,
            cache_hit_rate: 0.8,
            coordination_timing: ComponentTiming {
                summary_time: Duration::from_millis(10),
                index_time: Duration::from_millis(20),
                data_time: Duration::from_millis(30),
                coordination_overhead: Duration::from_millis(5),
            },
            throughput: 100.0,
        };

        let analysis = collector.analyze_regression("test_scenario", &metrics);
        assert!(!analysis.has_regression);
        assert!(analysis.analysis_details[0].contains("Insufficient historical data"));
    }
}