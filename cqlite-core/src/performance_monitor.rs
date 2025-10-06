#![cfg(feature = "benchmarks")]

//! Performance Monitoring and Baseline System
//!
//! Provides continuous performance monitoring and regression detection
//! for CQLite to ensure performance targets are maintained.

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Performance baseline metrics stored for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub metric_name: String,
    pub baseline_value: f64,
    pub unit: String,
    pub target_value: f64,
    pub target_direction: TargetDirection, // Higher or Lower is better
    pub timestamp: u64,
    pub environment: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TargetDirection {
    Higher, // Higher values are better (throughput, ops/sec)
    Lower,  // Lower values are better (latency, memory usage)
}

/// Current performance measurement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMeasurement {
    pub metric_name: String,
    pub value: f64,
    pub unit: String,
    pub timestamp: u64,
    pub meets_target: bool,
    pub deviation_percent: f64,
}

/// Performance regression alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAlert {
    pub alert_type: AlertType,
    pub metric_name: String,
    pub current_value: f64,
    pub baseline_value: f64,
    pub deviation_percent: f64,
    pub timestamp: u64,
    pub severity: AlertSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    Regression,   // Performance got worse
    TargetMissed, // Failed to meet target
    Improvement,  // Performance got better
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Critical, // >25% degradation
    High,     // 15-25% degradation
    Medium,   // 10-15% degradation
    Low,      // 5-10% degradation
    Info,     // <5% or improvement
}

/// Performance monitoring system
pub struct PerformanceMonitor {
    baselines: Arc<Mutex<HashMap<String, PerformanceBaseline>>>,
    measurements: Arc<Mutex<Vec<PerformanceMeasurement>>>,
    alerts: Arc<Mutex<Vec<PerformanceAlert>>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            baselines: Arc::new(Mutex::new(HashMap::new())),
            measurements: Arc::new(Mutex::new(Vec::new())),
            alerts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Initialize with PRD performance targets as baselines
    pub fn initialize_prd_baselines(&self) {
        let baselines = vec![
            PerformanceBaseline {
                metric_name: "parse_speed_mb_per_sec".to_string(),
                baseline_value: 100.0,
                unit: "MB/s".to_string(),
                target_value: 100.0,
                target_direction: TargetDirection::Higher,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "memory_usage_mb".to_string(),
                baseline_value: 64.0,
                unit: "MB".to_string(),
                target_value: 128.0,
                target_direction: TargetDirection::Lower,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "query_latency_ms".to_string(),
                baseline_value: 0.5,
                unit: "ms".to_string(),
                target_value: 1.0,
                target_direction: TargetDirection::Lower,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "write_throughput_ops_per_sec".to_string(),
                baseline_value: 15000.0,
                unit: "ops/sec".to_string(),
                target_value: 100_000.0,
                target_direction: TargetDirection::Higher,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "read_throughput_ops_per_sec".to_string(),
                baseline_value: 75000.0,
                unit: "ops/sec".to_string(),
                target_value: 100_000.0,
                target_direction: TargetDirection::Higher,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "vint_encode_mb_per_sec".to_string(),
                baseline_value: 28.7,
                unit: "MB/s".to_string(),
                target_value: 100.0,
                target_direction: TargetDirection::Higher,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
            PerformanceBaseline {
                metric_name: "vint_decode_mb_per_sec".to_string(),
                baseline_value: 36.2,
                unit: "MB/s".to_string(),
                target_value: 100.0,
                target_direction: TargetDirection::Higher,
                timestamp: current_timestamp(),
                environment: get_environment_info(),
                version: "1.0.0".to_string(),
            },
        ];

        let mut baselines_map = self.baselines.lock().unwrap_or_else(|e| e.into_inner());
        for baseline in baselines {
            baselines_map.insert(baseline.metric_name.clone(), baseline);
        }
    }

    /// Record a performance measurement
    pub fn record_measurement(&self, metric_name: &str, value: f64, unit: &str) {
        let baseline = {
            let baselines = self.baselines.lock().unwrap_or_else(|e| e.into_inner());
            baselines.get(metric_name).cloned()
        };

        let (meets_target, deviation_percent) = if let Some(ref baseline) = baseline {
            let meets = match baseline.target_direction {
                TargetDirection::Higher => value >= baseline.target_value,
                TargetDirection::Lower => value <= baseline.target_value,
            };

            let deviation = ((value - baseline.baseline_value) / baseline.baseline_value) * 100.0;
            (meets, deviation)
        } else {
            (true, 0.0) // No baseline to compare against
        };

        let measurement = PerformanceMeasurement {
            metric_name: metric_name.to_string(),
            value,
            unit: unit.to_string(),
            timestamp: current_timestamp(),
            meets_target,
            deviation_percent,
        };

        // Store measurement
        {
            let mut measurements = self.measurements.lock().unwrap_or_else(|e| e.into_inner());
            measurements.push(measurement.clone());

            // Keep only last 1000 measurements per metric
            let metric_name = measurement.metric_name.clone();
            let to_keep: Vec<bool> = measurements
                .iter()
                .map(|m| {
                    let count = measurements
                        .iter()
                        .filter(|other| other.metric_name == metric_name)
                        .count();
                    m.metric_name != metric_name || count <= 1000
                })
                .collect();

            let mut i = 0;
            measurements.retain(|_| {
                let keep = to_keep[i];
                i += 1;
                keep
            });
        }

        // Check for regressions and generate alerts
        if let Some(baseline) = baseline {
            self.check_for_alerts(&measurement, &baseline);
        }
    }

    /// Check for performance regressions and generate alerts
    fn check_for_alerts(
        &self,
        measurement: &PerformanceMeasurement,
        baseline: &PerformanceBaseline,
    ) {
        let deviation = measurement.deviation_percent;

        // Determine if this is a regression based on target direction
        let is_regression = match baseline.target_direction {
            TargetDirection::Higher => deviation < -5.0, // 5% decrease is concerning
            TargetDirection::Lower => deviation > 5.0,   // 5% increase is concerning
        };

        let alert_type = if !measurement.meets_target {
            AlertType::TargetMissed
        } else if is_regression {
            AlertType::Regression
        } else if deviation.abs() > 5.0 {
            AlertType::Improvement
        } else {
            return; // No alert needed
        };

        let severity = match deviation.abs() {
            d if d > 25.0 => AlertSeverity::Critical,
            d if d > 15.0 => AlertSeverity::High,
            d if d > 10.0 => AlertSeverity::Medium,
            d if d > 5.0 => AlertSeverity::Low,
            _ => AlertSeverity::Info,
        };

        let alert = PerformanceAlert {
            alert_type,
            metric_name: measurement.metric_name.clone(),
            current_value: measurement.value,
            baseline_value: baseline.baseline_value,
            deviation_percent: deviation,
            timestamp: current_timestamp(),
            severity,
        };

        // Store alert
        {
            let mut alerts = self.alerts.lock().unwrap_or_else(|e| e.into_inner());
            alerts.push(alert.clone());
        }

        // Log alert
        self.log_alert(&alert);
    }

    /// Log performance alert
    fn log_alert(&self, alert: &PerformanceAlert) {
        let emoji = match alert.severity {
            AlertSeverity::Critical => "🚨",
            AlertSeverity::High => "⚠️",
            AlertSeverity::Medium => "⚡",
            AlertSeverity::Low => "📊",
            AlertSeverity::Info => "ℹ️",
        };

        let alert_text = match alert.alert_type {
            AlertType::Regression => "PERFORMANCE REGRESSION",
            AlertType::TargetMissed => "TARGET MISSED",
            AlertType::Improvement => "PERFORMANCE IMPROVEMENT",
        };

        println!(
            "{} {} - {}: {:.2} (baseline: {:.2}, deviation: {:.1}%)",
            emoji,
            alert_text,
            alert.metric_name,
            alert.current_value,
            alert.baseline_value,
            alert.deviation_percent
        );
    }

    /// Generate performance report
    pub fn generate_performance_report(&self) -> String {
        let baselines = self.baselines.lock().unwrap_or_else(|e| e.into_inner());
        let measurements = self.measurements.lock().unwrap_or_else(|e| e.into_inner());
        let alerts = self.alerts.lock().unwrap_or_else(|e| e.into_inner());

        let mut report = String::new();
        report.push_str("🎯 CQLite Performance Monitoring Report\n");
        report.push_str("======================================\n\n");

        // Summary
        let total_metrics = baselines.len();
        let recent_measurements: Vec<_> = measurements
            .iter()
            .filter(|m| current_timestamp() - m.timestamp < 3600) // Last hour
            .collect();
        let targets_met = recent_measurements
            .iter()
            .filter(|m| m.meets_target)
            .count();

        report.push_str("📊 Summary (Last Hour)\n");
        report.push_str("----------------------\n");
        report.push_str(&format!("Total Metrics Tracked: {}\n", total_metrics));
        report.push_str(&format!(
            "Recent Measurements: {}\n",
            recent_measurements.len()
        ));
        report.push_str(&format!(
            "Targets Met: {} ({:.1}%)\n",
            targets_met,
            if !recent_measurements.is_empty() {
                targets_met as f64 / recent_measurements.len() as f64 * 100.0
            } else {
                0.0
            }
        ));

        // Performance targets status
        report.push_str("\n🎯 Performance Targets Status\n");
        report.push_str("-----------------------------\n");

        for (metric_name, baseline) in baselines.iter() {
            let latest_measurement = measurements
                .iter()
                .filter(|m| m.metric_name == *metric_name)
                .max_by_key(|m| m.timestamp);

            if let Some(measurement) = latest_measurement {
                let status = if measurement.meets_target {
                    "✅ PASS"
                } else {
                    "❌ FAIL"
                };
                let direction = match baseline.target_direction {
                    TargetDirection::Higher => "↗️",
                    TargetDirection::Lower => "↘️",
                };

                report.push_str(&format!(
                    "{} {} {}: {:.2} {} (target: {} {:.2})\n",
                    status,
                    direction,
                    metric_name,
                    measurement.value,
                    measurement.unit,
                    match baseline.target_direction {
                        TargetDirection::Higher => "≥",
                        TargetDirection::Lower => "≤",
                    },
                    baseline.target_value
                ));
            } else {
                report.push_str(&format!(
                    "⏳ PENDING {}: No measurements yet\n",
                    metric_name
                ));
            }
        }

        // Recent alerts
        let recent_alerts: Vec<_> = alerts
            .iter()
            .filter(|a| current_timestamp() - a.timestamp < 86400) // Last 24 hours
            .collect();

        if !recent_alerts.is_empty() {
            report.push_str("\n🚨 Recent Alerts (24h)\n");
            report.push_str("----------------------\n");

            for alert in recent_alerts.iter().take(10) {
                // Show max 10 recent alerts
                let emoji = match alert.severity {
                    AlertSeverity::Critical => "🚨",
                    AlertSeverity::High => "⚠️",
                    AlertSeverity::Medium => "⚡",
                    AlertSeverity::Low => "📊",
                    AlertSeverity::Info => "ℹ️",
                };

                report.push_str(&format!(
                    "{} {:?} - {}: {:.2} ({:.1}% deviation)\n",
                    emoji,
                    alert.alert_type,
                    alert.metric_name,
                    alert.current_value,
                    alert.deviation_percent
                ));
            }
        }

        // Performance trends
        report.push_str("\n📈 Performance Trends\n");
        report.push_str("--------------------\n");

        for (metric_name, _baseline) in baselines.iter() {
            let metric_measurements: Vec<_> = measurements
                .iter()
                .filter(|m| m.metric_name == *metric_name)
                .collect();

            if metric_measurements.len() >= 2 {
                let recent = metric_measurements.iter().rev().take(5).collect::<Vec<_>>();
                let avg_recent = recent.iter().map(|m| m.value).sum::<f64>() / recent.len() as f64;

                let older = metric_measurements
                    .iter()
                    .rev()
                    .skip(5)
                    .take(5)
                    .collect::<Vec<_>>();
                if !older.is_empty() {
                    let avg_older = older.iter().map(|m| m.value).sum::<f64>() / older.len() as f64;
                    let trend = ((avg_recent - avg_older) / avg_older) * 100.0;

                    let trend_icon = if trend > 5.0 {
                        "📈"
                    } else if trend < -5.0 {
                        "📉"
                    } else {
                        "➡️"
                    };

                    report.push_str(&format!(
                        "{} {}: {:.1}% trend over recent measurements\n",
                        trend_icon, metric_name, trend
                    ));
                }
            }
        }

        report.push_str("\n💡 Recommendations\n");
        report.push_str("------------------\n");

        let critical_alerts = alerts
            .iter()
            .filter(|a| matches!(a.severity, AlertSeverity::Critical))
            .count();
        let high_alerts = alerts
            .iter()
            .filter(|a| matches!(a.severity, AlertSeverity::High))
            .count();

        if critical_alerts > 0 {
            report.push_str(
                "🚨 CRITICAL: Immediate attention required for performance regressions\n",
            );
        }
        if high_alerts > 0 {
            report.push_str("⚠️ HIGH: Performance issues detected, investigate soon\n");
        }
        if targets_met < recent_measurements.len() / 2 {
            report.push_str("📊 More than 50% of targets missed - review performance strategy\n");
        }
        if recent_measurements.is_empty() {
            report.push_str("⏳ No recent measurements - ensure monitoring is active\n");
        }

        report.push_str(
            "\n🔄 Automatic monitoring active - Run benchmarks regularly to maintain baselines\n",
        );

        report
    }

    /// Export baseline data for persistence
    pub fn export_baselines(&self) -> Vec<PerformanceBaseline> {
        let baselines = self.baselines.lock().unwrap_or_else(|e| e.into_inner());
        baselines.values().cloned().collect()
    }

    /// Import baseline data
    pub fn import_baselines(&self, baselines: Vec<PerformanceBaseline>) {
        let mut baselines_map = self.baselines.lock().unwrap_or_else(|e| e.into_inner());
        for baseline in baselines {
            baselines_map.insert(baseline.metric_name.clone(), baseline);
        }
    }

    /// Get recent alerts
    pub fn get_recent_alerts(&self, hours: u64) -> Vec<PerformanceAlert> {
        let alerts = self.alerts.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = current_timestamp() - (hours * 3600);
        alerts
            .iter()
            .filter(|a| a.timestamp >= cutoff)
            .cloned()
            .collect()
    }
}

// Helper functions
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn get_environment_info() -> String {
    format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        let monitor = Self::new();
        monitor.initialize_prd_baselines();
        monitor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_monitor_creation() {
        let monitor = PerformanceMonitor::new();
        monitor.initialize_prd_baselines();

        let baselines = monitor.baselines.lock().unwrap();
        assert!(!baselines.is_empty());
        assert!(baselines.contains_key("parse_speed_mb_per_sec"));
        assert!(baselines.contains_key("memory_usage_mb"));
    }

    #[test]
    fn test_record_measurement() {
        let monitor = PerformanceMonitor::new();
        monitor.initialize_prd_baselines();

        monitor.record_measurement("parse_speed_mb_per_sec", 120.0, "MB/s");

        let measurements = monitor.measurements.lock().unwrap();
        assert_eq!(measurements.len(), 1);
        assert_eq!(measurements[0].metric_name, "parse_speed_mb_per_sec");
        assert_eq!(measurements[0].value, 120.0);
        assert!(measurements[0].meets_target);
    }

    #[test]
    fn test_regression_detection() {
        let monitor = PerformanceMonitor::new();
        monitor.initialize_prd_baselines();

        // Record a measurement that should trigger a regression alert
        monitor.record_measurement("parse_speed_mb_per_sec", 50.0, "MB/s"); // Well below baseline

        let alerts = monitor.alerts.lock().unwrap();
        assert!(!alerts.is_empty());
        assert!(matches!(alerts[0].alert_type, AlertType::TargetMissed));
    }

    #[test]
    fn test_performance_report_generation() {
        let monitor = PerformanceMonitor::new();
        monitor.initialize_prd_baselines();

        monitor.record_measurement("parse_speed_mb_per_sec", 120.0, "MB/s");
        monitor.record_measurement("memory_usage_mb", 64.0, "MB");

        let report = monitor.generate_performance_report();
        assert!(report.contains("Performance Monitoring Report"));
        assert!(report.contains("PASS"));
        assert!(report.len() > 100); // Should be a substantial report
    }
}
