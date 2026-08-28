//! Per-run peak resident-set-size sampling (issue #2605, throughput-program M15
//! item 3).
//!
//! # Why sampling and not `VmHWM`
//!
//! `/proc/self/status`'s `VmHWM` is a PROCESS-wide high-water mark that never
//! decreases, so in a harness that runs several arms in one process it would
//! report the same (largest-so-far) number for every arm after the first — an
//! answer that looks per-arm but is not. A sampler bounded to one run's lifetime
//! attributes the peak to the run that caused it.
//!
//! # It reports an absence honestly
//!
//! On a platform without `/proc/self/statm`, or if a read fails, the sample is
//! `None` — never `0`. A fabricated zero would read as "this arm used no memory",
//! which is the opposite of the truth, and would silently satisfy the 512Mi pod
//! budget claim this measurement exists to test.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Sampling interval. Fine enough to catch a batch-sized allocation spike,
/// coarse enough that the sampler itself is not part of the measurement.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

/// A running RSS sampler. [`Self::finish`] stops it and returns the peak.
pub(crate) struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_bytes: Arc<AtomicU64>,
    /// Set when at least one sample was successfully read, so an unreadable
    /// `/proc` is distinguishable from a genuine zero.
    measured: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RssSampler {
    /// Start sampling. Takes one immediate sample so a run shorter than
    /// [`SAMPLE_INTERVAL`] still yields a reading.
    pub(crate) fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_bytes = Arc::new(AtomicU64::new(0));
        let measured = Arc::new(AtomicBool::new(false));
        sample_into(&peak_bytes, &measured);

        let (s, p, m) = (stop.clone(), peak_bytes.clone(), measured.clone());
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                sample_into(&p, &m);
                std::thread::sleep(SAMPLE_INTERVAL);
            }
            sample_into(&p, &m);
        });
        Self {
            stop,
            peak_bytes,
            measured,
            handle: Some(handle),
        }
    }

    /// Stop sampling and return the peak RSS in bytes, or `None` if RSS could
    /// not be measured on this platform.
    pub(crate) fn finish(mut self) -> Option<u64> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        if self.measured.load(Ordering::Relaxed) {
            Some(self.peak_bytes.load(Ordering::Relaxed))
        } else {
            None
        }
    }
}

/// Read current RSS and fold it into `peak`.
fn sample_into(peak: &AtomicU64, measured: &AtomicBool) {
    if let Some(bytes) = current_rss_bytes() {
        measured.store(true, Ordering::Relaxed);
        let _ = peak.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.max(bytes)));
    }
}

/// Current resident set size in bytes, from `/proc/self/statm` field 2
/// (resident pages). `None` when unavailable or unparsable.
fn current_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // `sysconf(_SC_PAGESIZE)` without a libc dependency: every Linux target this
    // harness runs on uses 4 KiB pages, and the value is only used for reporting.
    // Stated explicitly rather than assumed silently.
    Some(pages.saturating_mul(PAGE_SIZE_BYTES))
}

/// Assumed page size (see [`current_rss_bytes`]).
const PAGE_SIZE_BYTES: u64 = 4096;
