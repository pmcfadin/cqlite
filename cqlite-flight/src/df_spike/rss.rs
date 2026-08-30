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
//! On a platform without `/proc/self/status`, or if a read fails, the sample is
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

/// Current resident set size in bytes, from `/proc/self/status`'s `VmRSS`.
/// `None` when unavailable or unparsable.
///
/// # Why `VmRSS` and not `statm` field 2
///
/// They are the SAME quantity — current resident set size — but `statm` reports
/// it in PAGES, so converting it needs the page size, and this crate has no
/// libc dependency to call `sysconf(_SC_PAGESIZE)` with. The previous version
/// hardcoded 4096, which silently reports a 4x-low number on a 16 KiB-page
/// kernel (aarch64 servers ship such kernels) — a fabricated pass against the
/// 512Mi pod budget. `VmRSS` is already denominated in kB, so it needs no
/// page-size assumption at all.
///
/// # Why `VmRSS` and not `VmHWM`
///
/// `VmHWM` is the process-wide high-water mark and cannot attribute a peak to
/// one scenario/arm (see the module docs); `VmRSS` is CURRENT RSS, which is what
/// the interval sampler above folds into a per-run maximum.
fn current_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_vm_rss_bytes(&status)
}

/// Extract `VmRSS` (kB) from `/proc/self/status` contents and return bytes.
///
/// Split out so the parse is testable without a live `/proc`.
fn parse_vm_rss_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let kb: u64 = fields.next()?.parse().ok()?;
    // The kernel always emits kB for this field; refuse to guess if it ever
    // does not, rather than reporting a number in the wrong unit.
    if fields.next()? != "kB" {
        return None;
    }
    Some(kb.saturating_mul(1024))
}

#[cfg(test)]
mod tests {
    use super::parse_vm_rss_bytes;

    #[test]
    fn parses_vm_rss_in_kb() {
        let status = "Name:\tdf_spike_bench\nVmHWM:\t  999999 kB\nVmRSS:\t   51200 kB\n";
        assert_eq!(parse_vm_rss_bytes(status), Some(51_200 * 1024));
    }

    #[test]
    fn absent_or_unexpected_unit_is_none_not_zero() {
        assert_eq!(parse_vm_rss_bytes("VmHWM:\t 100 kB\n"), None);
        assert_eq!(parse_vm_rss_bytes("VmRSS:\t 100 MB\n"), None);
        assert_eq!(parse_vm_rss_bytes("VmRSS:\n"), None);
    }

    #[test]
    fn live_proc_reports_a_nonzero_rss() {
        // The harness only ever runs on Linux; if this ever compiles elsewhere
        // the sampler returns `None` and the report says `unmeasured`.
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            assert!(parse_vm_rss_bytes(&status).is_some_and(|b| b > 0));
        }
    }
}
