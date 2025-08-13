//! Performance testing utilities

use std::time::Instant;

/// Simple performance timer
pub struct PerformanceTimer {
    start: Instant,
    name: String,
}

impl PerformanceTimer {
    pub fn new(name: &str) -> Self {
        Self {
            start: Instant::now(),
            name: name.to_string(),
        }
    }
    
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
    
    pub fn finish(self) -> u64 {
        let elapsed = self.elapsed_ms();
        println!("   ⏱️  {}: {}ms", self.name, elapsed);
        elapsed
    }
}

/// Calculate throughput
pub fn calculate_throughput(count: usize, duration_ms: u64) -> f64 {
    if duration_ms == 0 {
        0.0
    } else {
        (count as f64) / (duration_ms as f64 / 1000.0)
    }
}