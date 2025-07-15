//! Time utilities

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Time provider
#[derive(Debug)]
pub struct TimeProvider;

impl TimeProvider {
    /// Create a new time provider
    pub fn new() -> Self {
        Self
    }

    /// Get current time as microseconds since epoch
    pub fn now_micros(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Get current time as milliseconds since epoch
    pub fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Get current time as seconds since epoch
    pub fn now_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Get current system time
    pub fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    /// Create duration from milliseconds
    pub fn duration_from_millis(&self, millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// Create duration from seconds
    pub fn duration_from_secs(&self, secs: u64) -> Duration {
        Duration::from_secs(secs)
    }
}
