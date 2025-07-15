//! Performance Monitoring Utilities for CQLite Testing
//!
//! Provides real-time performance monitoring and analysis tools
//! for comprehensive testing and optimization.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};

/// Performance metrics collector
#[derive(Debug, Clone)]
pub struct PerformanceMonitor {
    metrics: Arc<RwLock<HashMap<String, MetricData>>>,
    start_time: Instant,
    sample_interval: Duration,
}

/// Individual metric data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricData {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub timestamp: u64,
    pub samples: Vec<f64>,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub p95: f64,
    pub p99: f64,
}

/// Performance targets for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTargets {
    pub write_ops_per_sec: f64,
    pub read_ops_per_sec: f64,
    pub memory_usage_mb: f64,
    pub compression_ratio: f64,
    pub max_latency_ms: f64,
    pub p99_latency_ms: f64,
}

impl Default for PerformanceTargets {
    fn default() -> Self {
        Self {
            write_ops_per_sec: 50_000.0,
            read_ops_per_sec: 100_000.0,
            memory_usage_mb: 128.0,
            compression_ratio: 0.3,
            max_latency_ms: 100.0,
            p99_latency_ms: 50.0,
        }
    }
}

/// Performance test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceResult {
    pub test_name: String,
    pub duration: Duration,
    pub metrics: HashMap<String, MetricData>,
    pub targets: PerformanceTargets,
    pub passed: bool,
    pub failures: Vec<String>,
}

/// Latency tracker for detailed latency analysis
pub struct LatencyTracker {
    samples: Arc<RwLock<Vec<Duration>>>,
    total_samples: AtomicUsize,
    total_latency: AtomicU64,
    min_latency: AtomicU64,
    max_latency: AtomicU64,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            samples: Arc::new(RwLock::new(Vec::new())),
            total_samples: AtomicUsize::new(0),
            total_latency: AtomicU64::new(0),
            min_latency: AtomicU64::new(u64::MAX),
            max_latency: AtomicU64::new(0),
        }
    }

    pub async fn record(&self, latency: Duration) {
        let latency_ns = latency.as_nanos() as u64;

        self.total_samples.fetch_add(1, Ordering::SeqCst);
        self.total_latency.fetch_add(latency_ns, Ordering::SeqCst);
        self.min_latency.fetch_min(latency_ns, Ordering::SeqCst);
        self.max_latency.fetch_max(latency_ns, Ordering::SeqCst);

        // Keep samples for percentile calculation
        let mut samples = self.samples.write().await;
        samples.push(latency);

        // Limit sample size to prevent memory issues
        if samples.len() > 100_000 {
            samples.drain(0..50_000);
        }
    }

    pub async fn get_stats(&self) -> LatencyStats {
        let samples = self.samples.read().await;
        let total_samples = self.total_samples.load(Ordering::SeqCst);
        let total_latency = self.total_latency.load(Ordering::SeqCst);
        let min_latency = self.min_latency.load(Ordering::SeqCst);
        let max_latency = self.max_latency.load(Ordering::SeqCst);

        let avg_latency = if total_samples > 0 {
            total_latency / total_samples as u64
        } else {
            0
        };

        // Calculate percentiles
        let mut sorted_samples: Vec<u64> = samples.iter().map(|d| d.as_nanos() as u64).collect();
        sorted_samples.sort_unstable();

        let p50 = if !sorted_samples.is_empty() {
            sorted_samples[sorted_samples.len() / 2]
        } else {
            0
        };

        let p95 = if !sorted_samples.is_empty() {
            sorted_samples[sorted_samples.len() * 95 / 100]
        } else {
            0
        };

        let p99 = if !sorted_samples.is_empty() {
            sorted_samples[sorted_samples.len() * 99 / 100]
        } else {
            0
        };

        LatencyStats {
            total_samples,
            avg_latency_ns: avg_latency,
            min_latency_ns: min_latency,
            max_latency_ns: max_latency,
            p50_latency_ns: p50,
            p95_latency_ns: p95,
            p99_latency_ns: p99,
        }
    }

    pub fn reset(&self) {
        self.total_samples.store(0, Ordering::SeqCst);
        self.total_latency.store(0, Ordering::SeqCst);
        self.min_latency.store(u64::MAX, Ordering::SeqCst);
        self.max_latency.store(0, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub total_samples: usize,
    pub avg_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
    pub p50_latency_ns: u64,
    pub p95_latency_ns: u64,
    pub p99_latency_ns: u64,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            sample_interval: Duration::from_millis(100),
        }
    }

    pub async fn record_metric(&self, name: &str, value: f64, unit: &str) {
        let timestamp = self.start_time.elapsed().as_millis() as u64;

        let mut metrics = self.metrics.write().await;
        let metric = metrics
            .entry(name.to_string())
            .or_insert_with(|| MetricData {
                name: name.to_string(),
                value,
                unit: unit.to_string(),
                timestamp,
                samples: Vec::new(),
                min: value,
                max: value,
                avg: value,
                p95: value,
                p99: value,
            });

        metric.value = value;
        metric.timestamp = timestamp;
        metric.samples.push(value);

        // Update statistics
        metric.min = metric.min.min(value);
        metric.max = metric.max.max(value);
        metric.avg = metric.samples.iter().sum::<f64>() / metric.samples.len() as f64;

        // Calculate percentiles
        if metric.samples.len() > 1 {
            let mut sorted = metric.samples.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let len = sorted.len();
            metric.p95 = sorted[len * 95 / 100];
            metric.p99 = sorted[len * 99 / 100];
        }

        // Limit sample size
        if metric.samples.len() > 10_000 {
            metric.samples.drain(0..5_000);
        }
    }

    pub async fn get_metrics(&self) -> HashMap<String, MetricData> {
        self.metrics.read().await.clone()
    }

    pub async fn get_metric(&self, name: &str) -> Option<MetricData> {
        self.metrics.read().await.get(name).cloned()
    }

    pub async fn validate_targets(&self, targets: &PerformanceTargets) -> PerformanceResult {
        let metrics = self.get_metrics().await;
        let mut failures = Vec::new();

        // Check write performance
        if let Some(write_metric) = metrics.get("write_ops_per_sec") {
            if write_metric.avg < targets.write_ops_per_sec {
                failures.push(format!(
                    "Write performance below target: {:.2} < {:.2}",
                    write_metric.avg, targets.write_ops_per_sec
                ));
            }
        }

        // Check read performance
        if let Some(read_metric) = metrics.get("read_ops_per_sec") {
            if read_metric.avg < targets.read_ops_per_sec {
                failures.push(format!(
                    "Read performance below target: {:.2} < {:.2}",
                    read_metric.avg, targets.read_ops_per_sec
                ));
            }
        }

        // Check memory usage
        if let Some(memory_metric) = metrics.get("memory_usage_mb") {
            if memory_metric.max > targets.memory_usage_mb {
                failures.push(format!(
                    "Memory usage above target: {:.2} > {:.2}",
                    memory_metric.max, targets.memory_usage_mb
                ));
            }
        }

        // Check compression ratio
        if let Some(compression_metric) = metrics.get("compression_ratio") {
            if compression_metric.avg > targets.compression_ratio {
                failures.push(format!(
                    "Compression ratio above target: {:.2} > {:.2}",
                    compression_metric.avg, targets.compression_ratio
                ));
            }
        }

        // Check latency
        if let Some(latency_metric) = metrics.get("latency_ms") {
            if latency_metric.max > targets.max_latency_ms {
                failures.push(format!(
                    "Max latency above target: {:.2} > {:.2}",
                    latency_metric.max, targets.max_latency_ms
                ));
            }

            if latency_metric.p99 > targets.p99_latency_ms {
                failures.push(format!(
                    "P99 latency above target: {:.2} > {:.2}",
                    latency_metric.p99, targets.p99_latency_ms
                ));
            }
        }

        PerformanceResult {
            test_name: "Performance Validation".to_string(),
            duration: self.start_time.elapsed(),
            metrics,
            targets: targets.clone(),
            passed: failures.is_empty(),
            failures,
        }
    }

    pub async fn start_monitoring(&self, duration: Duration) {
        let mut interval = interval(self.sample_interval);
        let end_time = Instant::now() + duration;

        while Instant::now() < end_time {
            interval.tick().await;

            // Record system metrics
            self.record_system_metrics().await;
        }
    }

    async fn record_system_metrics(&self) {
        // Record current timestamp
        let timestamp = self.start_time.elapsed().as_millis() as u64;

        // Record CPU usage (simplified)
        // In a real implementation, this would use system APIs
        self.record_metric("cpu_usage_percent", 0.0, "%").await;

        // Record memory usage (simplified)
        // In a real implementation, this would use system APIs
        self.record_metric("system_memory_mb", 0.0, "MB").await;

        // Record disk I/O (simplified)
        self.record_metric("disk_io_ops_per_sec", 0.0, "ops/sec")
            .await;
    }

    pub async fn generate_report(&self) -> String {
        let metrics = self.get_metrics().await;
        let mut report = String::new();

        report.push_str("# Performance Report\n\n");
        report.push_str(&format!(
            "**Duration:** {:.2}s\n",
            self.start_time.elapsed().as_secs_f64()
        ));
        report.push_str(&format!("**Metrics Count:** {}\n\n", metrics.len()));

        report.push_str("## Metrics Summary\n\n");
        report.push_str("| Metric | Value | Unit | Min | Max | Avg | P95 | P99 |\n");
        report.push_str("|--------|-------|------|-----|-----|-----|-----|-----|\n");

        for (name, metric) in metrics.iter() {
            report.push_str(&format!(
                "| {} | {:.2} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n",
                name,
                metric.value,
                metric.unit,
                metric.min,
                metric.max,
                metric.avg,
                metric.p95,
                metric.p99
            ));
        }

        report.push_str("\n## Performance Analysis\n\n");

        // Add analysis based on metrics
        if let Some(write_metric) = metrics.get("write_ops_per_sec") {
            if write_metric.avg >= 50_000.0 {
                report.push_str("✅ Write performance meets target\n");
            } else {
                report.push_str("❌ Write performance below target\n");
            }
        }

        if let Some(read_metric) = metrics.get("read_ops_per_sec") {
            if read_metric.avg >= 100_000.0 {
                report.push_str("✅ Read performance meets target\n");
            } else {
                report.push_str("❌ Read performance below target\n");
            }
        }

        if let Some(memory_metric) = metrics.get("memory_usage_mb") {
            if memory_metric.max <= 128.0 {
                report.push_str("✅ Memory usage within target\n");
            } else {
                report.push_str("❌ Memory usage above target\n");
            }
        }

        report
    }

    pub async fn reset(&self) {
        self.metrics.write().await.clear();
    }
}

/// Utility function to measure operation performance
pub async fn measure_operation<F, Fut, T>(
    monitor: &PerformanceMonitor,
    operation_name: &str,
    operation: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let result = operation().await;
    let duration = start.elapsed();

    monitor
        .record_metric(
            &format!("{}_latency_ms", operation_name),
            duration.as_millis() as f64,
            "ms",
        )
        .await;

    result
}

/// Utility function to measure throughput
pub async fn measure_throughput<F, Fut>(
    monitor: &PerformanceMonitor,
    operation_name: &str,
    operation_count: usize,
    operation: F,
) -> Duration
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let start = Instant::now();
    operation().await;
    let duration = start.elapsed();

    let ops_per_sec = operation_count as f64 / duration.as_secs_f64();
    monitor
        .record_metric(
            &format!("{}_ops_per_sec", operation_name),
            ops_per_sec,
            "ops/sec",
        )
        .await;

    duration
}

/// Batch performance measurement
pub struct BatchPerformanceMeasurer {
    monitor: PerformanceMonitor,
    latency_tracker: LatencyTracker,
    operation_count: AtomicUsize,
    start_time: Instant,
}

impl BatchPerformanceMeasurer {
    pub fn new() -> Self {
        Self {
            monitor: PerformanceMonitor::new(),
            latency_tracker: LatencyTracker::new(),
            operation_count: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    pub async fn record_operation(&self, latency: Duration) {
        self.latency_tracker.record(latency).await;
        self.operation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub async fn get_throughput(&self) -> f64 {
        let count = self.operation_count.load(Ordering::SeqCst);
        let duration = self.start_time.elapsed();
        count as f64 / duration.as_secs_f64()
    }

    pub async fn get_latency_stats(&self) -> LatencyStats {
        self.latency_tracker.get_stats().await
    }

    pub async fn finalize(&self) -> BatchPerformanceResult {
        let throughput = self.get_throughput().await;
        let latency_stats = self.get_latency_stats().await;
        let duration = self.start_time.elapsed();

        BatchPerformanceResult {
            duration,
            total_operations: self.operation_count.load(Ordering::SeqCst),
            throughput_ops_per_sec: throughput,
            latency_stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPerformanceResult {
    pub duration: Duration,
    pub total_operations: usize,
    pub throughput_ops_per_sec: f64,
    pub latency_stats: LatencyStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new();

        // Record some metrics
        monitor.record_metric("test_metric", 100.0, "ops/sec").await;
        monitor.record_metric("test_metric", 200.0, "ops/sec").await;
        monitor.record_metric("test_metric", 150.0, "ops/sec").await;

        let metric = monitor.get_metric("test_metric").await.unwrap();
        assert_eq!(metric.value, 150.0);
        assert_eq!(metric.min, 100.0);
        assert_eq!(metric.max, 200.0);
        assert_eq!(metric.avg, 150.0);
    }

    #[tokio::test]
    async fn test_latency_tracker() {
        let tracker = LatencyTracker::new();

        // Record some latencies
        tracker.record(Duration::from_millis(10)).await;
        tracker.record(Duration::from_millis(20)).await;
        tracker.record(Duration::from_millis(15)).await;

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.min_latency_ns, 10_000_000);
        assert_eq!(stats.max_latency_ns, 20_000_000);
    }

    #[tokio::test]
    async fn test_batch_performance_measurer() {
        let measurer = BatchPerformanceMeasurer::new();

        // Simulate operations
        for i in 0..10 {
            let latency = Duration::from_millis(i * 10);
            measurer.record_operation(latency).await;
        }

        let result = measurer.finalize().await;
        assert_eq!(result.total_operations, 10);
        assert!(result.throughput_ops_per_sec > 0.0);
    }
}
