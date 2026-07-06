//! Test-only probe (issues #1143, #1333) for the windowed streaming scan.
//!
//! Records (a) the [`std::thread::ThreadId`] on which the windowed scan's
//! decompress+parse half actually ran, so a guard test can deterministically
//! prove that work executed on a `spawn_blocking` thread and NOT on a tokio async
//! worker (#1143); and (b) how many times the per-partition scratch buffer GREW
//! its backing store, so a guard test can prove the scratch is reused across
//! partitions rather than reallocated per partition (#1333).
//!
//! Included via `#[cfg(feature = "scan-offload-probe")] #[path =
//! "scan_stream_windowed_probe.rs"] pub mod probe;` in the parent module and
//! compiled ONLY under the non-default `scan-offload-probe` feature. In a
//! normal/default/release build this module, its statics, and its call-sites do
//! not exist at all — the probe never enters the crate's public surface and adds
//! zero cost (issue #1143 finding 1). Kept in a sibling file so the parent stays
//! under the campsite-rule size limit (epic #1116).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread::ThreadId;

static ARMED: AtomicBool = AtomicBool::new(false);
static LAST_PARSE_THREAD: Mutex<Option<ThreadId>> = Mutex::new(None);
/// The [`ThreadId`] on which the scan's raw chunk READ last ran (issue #1593,
/// F3). For a synchronously-faulting backend (mmap page fault / `O_DIRECT`
/// `pread`) the read must run on a `spawn_blocking` thread, NOT an async worker;
/// a guard test compares this against the enumerated async-worker set.
static LAST_IO_READ_THREAD: Mutex<Option<ThreadId>> = Mutex::new(None);
/// Number of times the per-partition scratch buffer GREW its backing store
/// during the armed scan (issue #1333). With the scratch hoisted out of the
/// per-partition loop and `.clear()`-reused this is a small bounded count
/// (the buffer grows only up to its high-water mark); if the buffer were
/// reallocated per partition it would grow from empty every partition, so this
/// count would scale with partition count.
static SCRATCH_ALLOCS: AtomicUsize = AtomicUsize::new(0);

/// Arm the probe and clear any previously recorded thread + scratch-growth
/// count. Call from a test before driving a scan.
pub fn arm() {
    if let Ok(mut g) = LAST_PARSE_THREAD.lock() {
        *g = None;
    }
    if let Ok(mut g) = LAST_IO_READ_THREAD.lock() {
        *g = None;
    }
    SCRATCH_ALLOCS.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
}

/// Disarm the probe (restores the production no-op state).
pub fn disarm() {
    ARMED.store(false, Ordering::SeqCst);
}

/// Record the current thread as the parse thread, if armed. Called from the
/// blocking parse half after a scan's parse work completes.
pub(super) fn record_parse_thread() {
    if ARMED.load(Ordering::Relaxed) {
        if let Ok(mut g) = LAST_PARSE_THREAD.lock() {
            *g = Some(std::thread::current().id());
        }
    }
}

/// The [`ThreadId`] recorded by the most recent parse, if any.
pub fn recorded_parse_thread() -> Option<ThreadId> {
    LAST_PARSE_THREAD.lock().ok().and_then(|g| *g)
}

/// Record the current thread as the raw-chunk READ thread, if armed. Called from
/// the windowed scan's I/O feed loop once per scan after the first chunk read
/// (issue #1593, F3). For a synchronously-faulting backend this must be a
/// `spawn_blocking` thread, not an async worker.
pub(super) fn record_io_read_thread() {
    if ARMED.load(Ordering::Relaxed) {
        if let Ok(mut g) = LAST_IO_READ_THREAD.lock() {
            *g = Some(std::thread::current().id());
        }
    }
}

/// The [`ThreadId`] on which the scan's raw chunk read last ran, if any (issue
/// #1593, F3).
pub fn recorded_io_read_thread() -> Option<ThreadId> {
    LAST_IO_READ_THREAD.lock().ok().and_then(|g| *g)
}

/// Record that the per-partition scratch buffer's capacity changed from
/// `before` to `after`. Counts one growth event whenever `after > before`
/// (a Vec grows its backing store only by reallocating). Called once per
/// partition from [`super::SSTableReader::drain_scan_window`]; a no-op unless
/// armed. Issue #1333 scratch-reuse guard.
pub(super) fn note_scratch_capacity(before: usize, after: usize) {
    if ARMED.load(Ordering::Relaxed) && after > before {
        SCRATCH_ALLOCS.fetch_add(1, Ordering::Relaxed);
    }
}

/// The number of scratch-buffer growth events recorded during the armed scan.
/// Bounded and independent of partition count when the scratch is reused
/// (issue #1333); scales with partition count if it is reallocated per
/// partition.
pub fn recorded_scratch_allocs() -> usize {
    SCRATCH_ALLOCS.load(Ordering::Relaxed)
}
