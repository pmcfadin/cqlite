//! Status Metrics Module for TUI and REPL Status Bars
//!
//! Provides shared infrastructure for collecting and formatting health,
//! memory, and data metrics for display in both TUI and REPL modes.
//!
//! Issue #242: Enhanced Status Bar for CLI

use cqlite_core::Database;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Test-only counter of how many times the shared `System` handle has been
/// constructed. sysinfo's intended pattern is construct-once + narrow refresh
/// per tick; this lets the TDD test assert exactly one construction across many
/// status-refresh ticks (Issue #1719).
#[cfg(test)]
static SYSTEM_CONSTRUCTIONS: AtomicUsize = AtomicUsize::new(0);

/// Shared, lazily-constructed `sysinfo::System` handle.
///
/// Constructed exactly once (on first status refresh) and reused for every
/// subsequent 5-second tick. Each refresh performs the narrowest possible
/// refresh for the current process only, keeping the reported memory value
/// fresh without re-enumerating system state each tick (Issue #1719). A
/// `Mutex` is sufficient at the 5s status cadence.
fn shared_system() -> &'static Mutex<sysinfo::System> {
    static SYSTEM: OnceLock<Mutex<sysinfo::System>> = OnceLock::new();
    SYSTEM.get_or_init(|| {
        #[cfg(test)]
        SYSTEM_CONSTRUCTIONS.fetch_add(1, Ordering::Relaxed);
        Mutex::new(sysinfo::System::new())
    })
}

/// Default metrics refresh interval (5 seconds)
pub const METRICS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Health indicator for status bar display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthIndicator {
    /// Everything is working correctly
    Ok,
    /// Minor issues or unconfigured state
    Warning,
    /// Critical errors or failures
    Error,
}

/// Collected metrics for status bar display
#[derive(Debug, Clone)]
pub struct StatusMetrics {
    /// Overall health status
    pub health: HealthIndicator,
    /// Current memory usage in bytes
    pub memory_bytes: u64,
    /// Total data size (SSTable bytes) visible
    pub data_bytes: u64,
    /// When these metrics were collected
    pub last_updated: Instant,
}

impl Default for StatusMetrics {
    fn default() -> Self {
        Self {
            health: HealthIndicator::Warning,
            memory_bytes: 0,
            data_bytes: 0,
            last_updated: Instant::now(),
        }
    }
}

impl StatusMetrics {
    /// Collect status metrics from the current state
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Optional path to the data directory
    /// * `database` - Optional reference to the Database for stats
    ///
    /// # Returns
    ///
    /// StatusMetrics with current health, memory, and data information
    pub async fn collect(data_dir: Option<&Path>, database: Option<&Database>) -> Self {
        let mut metrics = StatusMetrics::default();

        // Determine health status based on data directory
        metrics.health = Self::check_health(data_dir);

        // Get process memory usage (more reliable than database internal stats)
        metrics.memory_bytes = Self::get_process_memory();

        // Get data stats from database if available
        if let Some(db) = database {
            if let Ok(stats) = db.stats().await {
                metrics.data_bytes = stats.storage_stats.sstables.total_size;
            }
        }

        metrics.last_updated = Instant::now();
        metrics
    }

    /// Get current process memory usage in bytes
    ///
    /// Uses sysinfo crate for cross-platform memory reading.
    /// Returns RSS (Resident Set Size) in bytes, or 0 if unavailable.
    fn get_process_memory() -> u64 {
        use sysinfo::ProcessRefreshKind;

        // Get current process ID
        let pid = match sysinfo::get_current_pid() {
            Ok(pid) => pid,
            Err(_) => return 0,
        };

        // Reuse the shared System handle (constructed once) and refresh only
        // this process's memory — the reading stays fresh per tick without
        // re-enumerating all system state (Issue #1719).
        let mut system = match shared_system().lock() {
            Ok(guard) => guard,
            Err(_) => return 0,
        };

        system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_memory());

        // Get memory usage in bytes (RSS)
        system
            .process(pid)
            .map(|process| process.memory())
            .unwrap_or(0)
    }

    /// Check health status based on data directory state
    fn check_health(data_dir: Option<&Path>) -> HealthIndicator {
        match data_dir {
            None => HealthIndicator::Warning,
            Some(path) => {
                if !path.exists() {
                    HealthIndicator::Error
                } else if !path.is_dir() {
                    HealthIndicator::Error
                } else if std::fs::read_dir(path).is_err() {
                    HealthIndicator::Error
                } else {
                    HealthIndicator::Ok
                }
            }
        }
    }

    /// Format memory bytes as human-readable string
    ///
    /// Examples: "0 B", "512 B", "1.5 KB", "24.5 MB", "1.2 GB"
    pub fn format_memory(&self) -> String {
        format_bytes(self.memory_bytes)
    }

    /// Format data bytes as human-readable string
    ///
    /// Examples: "0 B", "512 B", "1.5 KB", "24.5 MB", "1.2 GB"
    pub fn format_data(&self) -> String {
        format_bytes(self.data_bytes)
    }

    /// Get the health indicator symbol for display
    ///
    /// Returns: "OK" (green), "WARN" (yellow), or "ERR" (red)
    #[allow(dead_code)] // Part of public API, may be used in future enhancements
    pub fn health_symbol(&self) -> &'static str {
        match self.health {
            HealthIndicator::Ok => "OK",
            HealthIndicator::Warning => "WARN",
            HealthIndicator::Error => "ERR",
        }
    }

    /// Check if metrics are stale and need refresh
    ///
    /// # Arguments
    ///
    /// * `max_age` - Maximum age before metrics are considered stale
    ///
    /// # Returns
    ///
    /// true if metrics are older than max_age
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.last_updated.elapsed() > max_age
    }
}

/// Format bytes as human-readable string with appropriate unit
///
/// Uses binary units (KiB = 1024) but displays as KB/MB/GB for readability
pub(crate) fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(25 * 1024 * 1024), "25.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 1024), "1.0 TB");
    }

    #[test]
    fn test_health_symbol() {
        let mut metrics = StatusMetrics::default();

        metrics.health = HealthIndicator::Ok;
        assert_eq!(metrics.health_symbol(), "OK");

        metrics.health = HealthIndicator::Warning;
        assert_eq!(metrics.health_symbol(), "WARN");

        metrics.health = HealthIndicator::Error;
        assert_eq!(metrics.health_symbol(), "ERR");
    }

    #[test]
    fn test_is_stale() {
        let metrics = StatusMetrics::default();

        // Fresh metrics should not be stale
        assert!(!metrics.is_stale(Duration::from_secs(5)));

        // With zero duration, anything is stale
        assert!(metrics.is_stale(Duration::from_nanos(0)));
    }

    #[test]
    fn test_check_health_no_data_dir() {
        let health = StatusMetrics::check_health(None);
        assert_eq!(health, HealthIndicator::Warning);
    }

    #[test]
    fn test_check_health_nonexistent_path() {
        let health = StatusMetrics::check_health(Some(Path::new("/nonexistent/path/12345")));
        assert_eq!(health, HealthIndicator::Error);
    }

    /// Issue #1719 (red on main): the shared `System` handle must be
    /// constructed exactly once across many status-refresh ticks, not once
    /// per tick. On main `get_process_memory` did `System::new()` every call,
    /// so this global total would be >= the number of calls and the assertion
    /// would fail.
    #[test]
    fn test_system_constructed_once_across_ticks() {
        // Simulate many 5s status-refresh ticks.
        for _ in 0..8 {
            let _ = StatusMetrics::get_process_memory();
        }
        // OnceLock guarantees the handle is initialized exactly once for the
        // whole process, regardless of how many ticks (or other tests) call in.
        assert_eq!(
            SYSTEM_CONSTRUCTIONS.load(Ordering::Relaxed),
            1,
            "System must be constructed exactly once across refresh ticks, not per tick"
        );
    }

    /// The cached handle must still report a fresh reading each tick: after
    /// allocating and touching a large buffer, the reported RSS must grow.
    #[test]
    fn test_memory_reading_stays_fresh_across_ticks() {
        let before = StatusMetrics::get_process_memory();
        assert!(before > 0, "process memory reading should be available");

        // Allocate ~100 MiB and touch every page so it becomes resident.
        let chunk = 100 * 1024 * 1024;
        let mut buf: Vec<u8> = vec![0u8; chunk];
        let page = 4096;
        let mut i = 0;
        while i < buf.len() {
            buf[i] = (i % 251) as u8;
            i += page;
        }
        // Read on the next tick using the same cached handle.
        let after = StatusMetrics::get_process_memory();

        // Prevent the allocation from being optimized away before the reading.
        std::hint::black_box(&buf);

        assert!(
            after > before,
            "reading must stay fresh per tick: before={before} after={after}"
        );
    }

    #[test]
    fn test_default_metrics() {
        let metrics = StatusMetrics::default();
        assert_eq!(metrics.health, HealthIndicator::Warning);
        assert_eq!(metrics.memory_bytes, 0);
        assert_eq!(metrics.data_bytes, 0);
    }
}
