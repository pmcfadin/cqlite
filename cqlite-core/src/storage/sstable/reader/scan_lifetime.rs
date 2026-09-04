//! Reader-scoped scan-lifetime `madvise` seam (issue #3853).
//!
//! # What this is
//!
//! An in-flight scan COUNTER plus an RAII guard, both scoped to ONE
//! [`SSTableReader`](super::SSTableReader). When the first scan on that reader
//! begins, the reader's SCAN mapping is advised `MADV_WILLNEED`; when the last
//! one ends it is advised `MADV_DONTNEED`. Before this existed, an explicit
//! [`PrefetchMode::WillNeed`](crate::config::PrefetchMode::WillNeed) issued its
//! `MADV_WILLNEED` at reader OPEN — i.e. a reader that was opened and never
//! scanned paid a full-file read-ahead, and the advice was never withdrawn for
//! the reader's whole lifetime. That open-time site is gone; this module is
//! where the advice now lives.
//!
//! It is deliberately NOT a policy change:
//! [`PrefetchMode::Auto`](crate::config::PrefetchMode::Auto) — the default —
//! still issues NO madvise at all (issue #1143: `MADV_SEQUENTIAL`'s drop-behind
//! blew up the read-side p99 tail under concurrent write load), and
//! [`Sequential`](crate::config::PrefetchMode::Sequential) keeps its open-time
//! advice, which is an explicit drop-behind opt-in. Only `WillNeed` moved.
//!
//! # What `MADV_DONTNEED` does here, stated precisely
//!
//! The scan mapping is **shared and file-backed** (`MAP_SHARED | PROT_READ`), so
//! `MADV_DONTNEED` is an **RSS** control: it drops this process's resident
//! private page-table references to the range. It is NOT a page-cache eviction —
//! `madvise(2)` keeps the clean file pages in the page cache, and a later touch
//! repopulates them from there. So the effect being bought is resident-set
//! footprint after a scan finishes, never cache invalidation of any kind.
//!
//! # Why the advice is issued UNDER THE LOCK
//!
//! The counter is a [`std::sync::Mutex<u32>`] and both the transition test and
//! the resulting syscall happen while that lock is held. An `AtomicU32` would
//! leave a window in which a `1 -> 0` `DONTNEED` lands AFTER a concurrent
//! `0 -> 1` `WILLNEED` and drops the residency of a mapping a freshly-started
//! scan is already reading — exactly the interleaving issue #3853's constraint 3
//! forbids. Serializing the transition with its syscall removes the window.
//!
//! This is **not** a reintroduction of the issue #815 scan mutex. #815 removed a
//! lock held for a scan's ENTIRE DURATION, which serialized concurrent scans on
//! one reader. This lock is taken exactly TWICE PER SCAN — once at
//! [`ScanLifetime::begin`], once in [`ScanLifetimeGuard::drop`] — never per row,
//! per block or per page, and it covers only the counter transition plus the one
//! syscall whose ordering must be serialized with it. Two scans on one reader
//! still run fully in parallel.
//!
//! # Nesting needs no exemption plumbing
//!
//! Several scan entry points delegate to each other
//! (`iterate_all_partitions` -> `_cancellable` -> `_via_full_index` -> the
//! sequential fallback), so an inner guard simply raises the count and only the
//! OUTERMOST guard's drop releases. Contrast `scan_admission`'s
//! [`Exempt`](super::scan_stream_windowed::scan_admission::ScanAdmission::Exempt),
//! which exists because a bounded SEMAPHORE can hold-and-wait on itself; a
//! counter cannot, so every site can be wired unconditionally.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::SSTableReader;

/// Per-reader scan-lifetime state. `inner == None` means the seam is DISABLED
/// for this reader: [`begin`](Self::begin) hands out an inert guard, no syscall
/// is ever issued and both counters stay at zero.
pub(crate) struct ScanLifetime {
    inner: Option<Inner>,
}

struct Inner {
    /// The reader's SCAN mapping, or `None` for the counting-only test
    /// construction (and on non-unix, where `madvise` does not exist) — the
    /// counters still move so the transition logic is testable without I/O.
    #[cfg_attr(not(unix), allow(dead_code))]
    mmap: Option<Arc<memmap2::Mmap>>,
    /// Scans currently in flight on this reader. Guards the transition AND the
    /// syscall (see the module docs).
    in_flight: std::sync::Mutex<u32>,
    /// `MADV_WILLNEED` / `MADV_DONTNEED` ATTEMPTS (see
    /// [`SSTableReader::scan_lifetime_advice_counts`]).
    willneed: AtomicU64,
    dontneed: AtomicU64,
}

impl ScanLifetime {
    /// A disabled seam: no mapping, no syscalls, counters pinned at zero.
    pub(crate) fn disabled() -> Arc<Self> {
        Arc::new(Self { inner: None })
    }

    /// An ENABLED seam over `mmap`, the reader's scan mapping.
    ///
    /// Unix-only: `Mmap::advise` / `Mmap::unchecked_advise` are `#[cfg(unix)]`
    /// in memmap2, so on every other target the seam is disabled outright.
    #[cfg(unix)]
    pub(crate) fn for_scan_mapping(mmap: Arc<memmap2::Mmap>) -> Arc<Self> {
        Arc::new(Self {
            inner: Some(Inner::new(Some(mmap))),
        })
    }

    /// An ENABLED seam with NO mapping: the transition logic and the counters
    /// run, no syscall is issued. Lets the unit tests below pin the 0->1 / 1->0
    /// semantics with no file, no mapping and no I/O at all.
    #[cfg(test)]
    pub(crate) fn counting_only() -> Arc<Self> {
        Arc::new(Self {
            inner: Some(Inner::new(None)),
        })
    }

    /// Register one in-flight scan, advising `MADV_WILLNEED` on the 0->1
    /// transition, and return the RAII guard whose drop unregisters it.
    pub(crate) fn begin(self: &Arc<Self>) -> ScanLifetimeGuard {
        match &self.inner {
            None => ScanLifetimeGuard { lifetime: None },
            Some(inner) => {
                // Poisoned means a panic happened while the lock was held. The
                // guarded state is a `u32` plus one best-effort syscall, so
                // there is no invariant to protect by refusing to proceed;
                // recover the count and carry on (never `unwrap`).
                let mut count = inner
                    .in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                // `saturating_add` cannot wrap a live count back to 0 and so can
                // never fabricate a spurious "last scan ended" release. At the
                // (unreachable) `u32::MAX` ceiling the seam simply stops
                // releasing — it holds WILLNEED — which is the safe direction.
                *count = count.saturating_add(1);
                if *count == 1 {
                    inner.advise_willneed();
                }
                ScanLifetimeGuard {
                    lifetime: Some(Arc::clone(self)),
                }
            }
        }
    }

    /// Scans currently in flight (0 when the seam is disabled).
    pub(crate) fn in_flight(&self) -> u32 {
        match &self.inner {
            None => 0,
            Some(inner) => *inner
                .in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    /// `(willneed, dontneed)` advice ATTEMPTS ((0, 0) when disabled).
    pub(crate) fn advice_counts(&self) -> (u64, u64) {
        match &self.inner {
            None => (0, 0),
            Some(inner) => (
                inner.willneed.load(Ordering::Relaxed),
                inner.dontneed.load(Ordering::Relaxed),
            ),
        }
    }
}

impl Inner {
    fn new(mmap: Option<Arc<memmap2::Mmap>>) -> Self {
        Self {
            mmap,
            in_flight: std::sync::Mutex::new(0),
            willneed: AtomicU64::new(0),
            dontneed: AtomicU64::new(0),
        }
    }

    /// First scan started: ask the kernel to populate the mapping. Counted as an
    /// ATTEMPT before the call, so the counter reflects transitions taken rather
    /// than syscall outcomes; an advise failure is non-fatal and logged at
    /// `debug`, the same posture as the pre-existing madvise sites.
    fn advise_willneed(&self) {
        self.willneed.fetch_add(1, Ordering::Relaxed);
        #[cfg(unix)]
        if let Some(mmap) = &self.mmap {
            if let Err(e) = mmap.advise(memmap2::Advice::WillNeed) {
                tracing::debug!("madvise(WILLNEED) on scan mapping failed: {}", e);
            }
        }
    }

    /// Last scan ended: drop this process's residency for the scan mapping.
    fn advise_dontneed(&self) {
        self.dontneed.fetch_add(1, Ordering::Relaxed);
        #[cfg(unix)]
        if let Some(mmap) = &self.mmap {
            // SAFETY: memmap2 puts `DontNeed` behind `UncheckedAdvice` because
            // for an ANONYMOUS or PRIVATE mapping `MADV_DONTNEED` makes the next
            // touch read as ZERO-FILL, which would change observable content
            // under a live borrow. That case cannot arise here:
            // `MmapOptions::map` maps `PROT_READ | MAP_SHARED` over a FILE
            // (memmap2-0.9.11 `src/unix.rs:245-257`), and this reader's standing
            // contract is that the file is immutable for the reader's lifetime
            // (see `SSTableReader::map_file`). For a shared file-backed mapping
            // `MADV_DONTNEED` discards resident pages and the next touch
            // repopulates from the up-to-date file contents — the SAME bytes. So
            // no borrow can observe a change: outstanding zero-copy borrows into
            // this mapping (`value_borrow.rs`) stay valid, and the whole cost of
            // touching a released page under a stale borrow is a REFAULT, never
            // a content change.
            if let Err(e) = unsafe { mmap.unchecked_advise(memmap2::UncheckedAdvice::DontNeed) } {
                tracing::debug!("madvise(DONTNEED) on scan mapping failed: {}", e);
            }
        }
    }
}

/// RAII registration of ONE in-flight scan on one reader.
///
/// `Send + 'static` (it holds only an `Option<Arc<ScanLifetime>>`) so it can be
/// held across `await` points and moved into a spawned scan task, which is how
/// the streaming scan surfaces bind it to the task's lifetime. An inert guard
/// (disabled seam) costs one `None` check on drop.
pub(crate) struct ScanLifetimeGuard {
    lifetime: Option<Arc<ScanLifetime>>,
}

impl Drop for ScanLifetimeGuard {
    fn drop(&mut self) {
        let Some(lifetime) = self.lifetime.take() else {
            return;
        };
        let Some(inner) = &lifetime.inner else {
            return;
        };
        let mut count = inner
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *count = count.saturating_sub(1);
        if *count == 0 {
            inner.advise_dontneed();
        }
    }
}

impl SSTableReader {
    /// Register this scan with the reader's scan-lifetime seam (issue #3853).
    ///
    /// Called at the top of every scan entry point; the returned guard must be
    /// bound for the scan's whole duration (`let _scan = self.begin_scan();`).
    /// A no-op for every reader whose seam is disabled — which is every reader
    /// that is not mmap-backed at an explicit `PrefetchMode::WillNeed` with a
    /// dedicated point mapping.
    pub(crate) fn begin_scan(&self) -> ScanLifetimeGuard {
        self.scan_lifetime.begin()
    }

    /// `(willneed, dontneed)` scan-lifetime madvise ATTEMPTS on this reader.
    ///
    /// Counts ATTEMPTS, not successes: each counter is incremented on the
    /// counter transition that decides to advise, BEFORE the syscall, and an
    /// `EINVAL`/`ENOMEM` from `madvise` leaves the count raised (the failure is
    /// logged at `debug` and is non-fatal). So the pair reports the scan-lifetime
    /// TRANSITIONS this reader took, which is the observable property #3853 is
    /// about. Incremented once per scan-start / scan-end transition — never per
    /// row, block or page — so a reader under a nested or concurrent scan burst
    /// still reports `(1, 1)`.
    ///
    /// `(0, 0)` forever when the seam is disabled for this reader, including the
    /// default `PrefetchMode::Auto` (issue #1143) and every non-mmap backend.
    /// Unconditionally `pub` and NOT feature-gated on purpose: a cfg-gated
    /// assertion would be coverage that executes in no gate component (#3522).
    pub fn scan_lifetime_advice_counts(&self) -> (u64, u64) {
        self.scan_lifetime.advice_counts()
    }

    /// Scans currently in flight on this reader (0 when the seam is disabled).
    pub fn scan_lifetime_in_flight(&self) -> u32 {
        self.scan_lifetime.in_flight()
    }

    /// Whether the scan-lifetime seam is ENABLED for this reader, i.e. whether
    /// [`scan_lifetime_advice_counts`](Self::scan_lifetime_advice_counts) can
    /// ever move off `(0, 0)`.
    ///
    /// Exists so a test asserting "open issued NO advice" is not VACUOUS: a
    /// `(0, 0)` reading is also what a buffered reader, a non-unix target or an
    /// `Auto`-prefetch reader returns, so the `(0, 0)` assertion only carries
    /// information alongside a positive control that the seam was armed at all.
    /// Because the seam is armed only for an mmap-backed reader at an explicit
    /// `WillNeed` whose point plane holds a DIFFERENT mapping, a `true` here also
    /// proves the reader took the mmap backend.
    pub fn scan_lifetime_enabled(&self) -> bool {
        self.scan_lifetime.is_enabled()
    }
}

impl ScanLifetime {
    /// Whether this seam issues advice at all (see
    /// [`SSTableReader::scan_lifetime_enabled`]).
    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }
}

/// Resolve the scan-lifetime seam for a reader being opened (issue #3853).
///
/// ENABLED only when ALL THREE hold:
///
/// 1. the reader took the **mmap** backend
///    ([`ScanSource::Mapped`](super::source::ScanSource::Mapped)) — the other
///    backends have no per-mapping advice concept at all;
/// 2. the resolved prefetch mode is an explicit
///    [`PrefetchMode::WillNeed`](crate::config::PrefetchMode::WillNeed).
///    `Auto` must issue NOTHING (issue #1143) and `Sequential` keeps its
///    open-time drop-behind advice, which is a different, explicit opt-in;
/// 3. the POINT plane holds a DIFFERENT mapping from the scan plane, tested by
///    `Arc::ptr_eq`. `SSTableReader::point_read_mmap` returns the scan `Arc`
///    ITSELF when the file is below `POINT_MMAP_MADV_RANDOM_MIN_BYTES` (8 MiB)
///    or when the dedicated map / its `MADV_RANDOM` failed, so pointer identity
///    is an EXACT test of "does the point plane share the scan mapping". Same
///    `Arc` => DISABLED: releasing the scan mapping's residency would degrade
///    the point plane that is reading through the very same pages (issue #3853
///    AC bullet 3 / constraint 3).
///
/// Anything else yields [`ScanLifetime::disabled`].
#[cfg(unix)]
pub(crate) fn resolve(
    scan_source: &super::source::ScanSource,
    prefetch: crate::config::PrefetchMode,
    point_plane_mmap: Option<&Arc<memmap2::Mmap>>,
) -> Arc<ScanLifetime> {
    let scan_mmap = match scan_source {
        super::source::ScanSource::Mapped(mmap) => mmap,
        _ => return ScanLifetime::disabled(),
    };
    if !matches!(prefetch, crate::config::PrefetchMode::WillNeed) {
        return ScanLifetime::disabled();
    }
    match point_plane_mmap {
        Some(point) if !Arc::ptr_eq(point, scan_mmap) => {
            ScanLifetime::for_scan_mapping(Arc::clone(scan_mmap))
        }
        // Same allocation (sub-8-MiB file, or a failed dedicated map), or no
        // point mapping recorded at all: never release the scan mapping.
        _ => ScanLifetime::disabled(),
    }
}

/// Non-unix: `madvise` has no equivalent, so the seam is always disabled.
#[cfg(not(unix))]
pub(crate) fn resolve(
    _scan_source: &super::source::ScanSource,
    _prefetch: crate::config::PrefetchMode,
    _point_plane_mmap: Option<&Arc<memmap2::Mmap>>,
) -> Arc<ScanLifetime> {
    ScanLifetime::disabled()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn disabled_never_advises() {
        let lifetime = ScanLifetime::disabled();
        let guard = lifetime.begin();
        assert_eq!(lifetime.in_flight(), 0);
        drop(guard);
        assert_eq!(lifetime.advice_counts(), (0, 0));
    }

    #[test]
    fn first_begin_advises_willneed_and_last_end_advises_dontneed() {
        let lifetime = ScanLifetime::counting_only();
        let outer = lifetime.begin();
        assert_eq!(lifetime.advice_counts(), (1, 0));
        assert_eq!(lifetime.in_flight(), 1);
        let inner = lifetime.begin();
        assert_eq!(
            lifetime.advice_counts(),
            (1, 0),
            "nested begin must not re-advise"
        );
        assert_eq!(lifetime.in_flight(), 2);
        drop(inner);
        assert_eq!(
            lifetime.advice_counts(),
            (1, 0),
            "inner end must not release"
        );
        drop(outer);
        assert_eq!(lifetime.advice_counts(), (1, 1));
        assert_eq!(lifetime.in_flight(), 0);
        drop(lifetime.begin());
        assert_eq!(lifetime.advice_counts(), (2, 2), "a later scan re-advises");
    }

    #[test]
    fn overlapping_threads_advise_exactly_once() {
        const THREADS: usize = 8;
        let lifetime = ScanLifetime::counting_only();
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let lifetime = Arc::clone(&lifetime);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let guard = lifetime.begin();
                // Every thread holds its guard until ALL threads hold one, so the
                // overlap is a barrier fact, never a sleep race.
                barrier.wait();
                drop(guard);
            }));
        }
        for h in handles {
            assert!(h.join().is_ok());
        }
        assert_eq!(lifetime.advice_counts(), (1, 1));
        assert_eq!(lifetime.in_flight(), 0);
    }
}

#[cfg(all(test, unix))]
mod resolve_tests {
    use super::*;
    use crate::config::PrefetchMode;
    use std::io::Write;

    /// Map a small temp file twice and return `(scan_mmap, distinct_point_mmap)`.
    fn two_mappings() -> (
        Arc<memmap2::Mmap>,
        Arc<memmap2::Mmap>,
        tempfile::NamedTempFile,
    ) {
        let mut tmp = tempfile::NamedTempFile::new().expect("temp file");
        tmp.write_all(&[7u8; 4096]).expect("write");
        tmp.flush().expect("flush");
        let map = || {
            let f = std::fs::File::open(tmp.path()).expect("open");
            // SAFETY: read-only mapping of a file this test owns and does not
            // mutate while mapped (same contract as `SSTableReader::map_file`).
            Arc::new(unsafe { memmap2::MmapOptions::new().map(&f).expect("map") })
        };
        let scan = map();
        let point = map();
        assert!(!Arc::ptr_eq(&scan, &point), "two distinct mappings");
        (scan, point, tmp)
    }

    #[test]
    fn enabled_only_for_mapped_willneed_with_a_distinct_point_plane() {
        let (scan, point, _tmp) = two_mappings();
        let mapped = super::super::source::ScanSource::Mapped(Arc::clone(&scan));

        assert!(
            resolve(&mapped, PrefetchMode::WillNeed, Some(&point)).is_enabled(),
            "mmap + WillNeed + distinct point mapping must arm the seam"
        );

        // #1143: `Auto` issues nothing, ever. `Off`/`Sequential` are not this
        // seam's business either (Sequential keeps its open-time advice).
        for mode in [
            PrefetchMode::Auto,
            PrefetchMode::Off,
            PrefetchMode::Sequential,
        ] {
            assert!(
                !resolve(&mapped, mode, Some(&point)).is_enabled(),
                "prefetch {:?} must leave the seam disabled",
                mode
            );
        }

        // The point plane sharing the scan mapping (sub-8-MiB file, or a failed
        // dedicated map) must disable the seam, as must an absent point mapping.
        assert!(
            !resolve(&mapped, PrefetchMode::WillNeed, Some(&scan)).is_enabled(),
            "a shared point/scan mapping must disable the seam"
        );
        assert!(
            !resolve(&mapped, PrefetchMode::WillNeed, None).is_enabled(),
            "no recorded point mapping must disable the seam"
        );

        // Non-mmap backends have no per-mapping advice concept.
        let buffered = super::super::source::ScanSource::Buffered { file_len: 4096 };
        assert!(!resolve(&buffered, PrefetchMode::WillNeed, Some(&point)).is_enabled());
    }
}
