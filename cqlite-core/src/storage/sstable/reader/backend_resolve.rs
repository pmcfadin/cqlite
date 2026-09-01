//! Disk-access backend resolution for the SSTable reader.
//!
//! Split out of `reader/mod.rs` under the campsite rule (epic #1116): that file
//! is far over the 800-line source target, and these functions are one cohesive
//! responsibility — turning `StorageConfig` plus the `CQLITE_*` environment
//! overrides plus the file's size into the concrete backend
//! ([`crate::config::DiskAccessMode`]) and madvise advice the reader uses.
//!
//! Every parse/resolve step here is PURE (system memory and availability are
//! injected), so the decisions are unit-testable without mutating the
//! process-global environment — which would race concurrent `open()` tests.

use crate::config::{DiskAccessMode, PrefetchMode};

/// Returns `true` when memory-mapped reads are force-enabled via the
/// `CQLITE_USE_MMAP` environment variable.
///
/// Accepts `1`, `true`, `yes`, `on` (case-insensitive). Any other value — or
/// an unset variable — leaves the decision to [`Config`]. This is an opt-in
/// escape hatch for ad-hoc local use without threading a custom config.
pub(super) fn mmap_enabled_via_env() -> bool {
    std::env::var("CQLITE_USE_MMAP")
        .ok()
        .as_deref()
        .map(parse_truthy_env)
        .unwrap_or(false)
}

/// Parse a truthy environment-variable value (`1`/`true`/`yes`/`on`,
/// case-insensitive). Split out so it can be unit-tested without mutating the
/// process-global environment (which would race other `open()` tests).
pub(super) fn parse_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Parse a [`DiskAccessMode`] from a string (`auto`/`buffered`/`mmap`/`direct`,
/// case-insensitive). Returns `None` for unrecognized values so callers can
/// keep the configured default. Pure for unit-testing without env mutation.
pub(super) fn parse_disk_access_mode(value: &str) -> Option<DiskAccessMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Some(DiskAccessMode::Auto),
        "buffered" | "buffer" => Some(DiskAccessMode::Buffered),
        "mmap" | "mapped" => Some(DiskAccessMode::Mmap),
        "direct" | "directio" | "direct_io" | "o_direct" => Some(DiskAccessMode::Direct),
        _ => None,
    }
}

/// Parse a [`PrefetchMode`] from a string (`off`/`sequential`/`willneed`/`auto`).
/// Returns `None` for unrecognized values. Pure for unit-testing.
pub(super) fn parse_prefetch_mode(value: &str) -> Option<PrefetchMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "no" => Some(PrefetchMode::Off),
        "sequential" | "seq" => Some(PrefetchMode::Sequential),
        "willneed" | "will_need" | "will-need" => Some(PrefetchMode::WillNeed),
        "auto" => Some(PrefetchMode::Auto),
        _ => None,
    }
}

/// `CQLITE_DISK_ACCESS_MODE` override, if set to a recognized value.
pub(super) fn disk_access_mode_via_env() -> Option<DiskAccessMode> {
    std::env::var("CQLITE_DISK_ACCESS_MODE")
        .ok()
        .as_deref()
        .and_then(parse_disk_access_mode)
}

/// `CQLITE_PREFETCH` override, if set to a recognized value.
pub(super) fn prefetch_mode_via_env() -> Option<PrefetchMode> {
    std::env::var("CQLITE_PREFETCH")
        .ok()
        .as_deref()
        .and_then(parse_prefetch_mode)
}

/// Best-effort total physical RAM in bytes, or `None` when it cannot be
/// determined on this platform. Used by [`DiskAccessMode::Auto`] to decide when
/// a file is large enough to warrant page-cache-bypassing direct I/O.
pub(super) fn system_memory_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        // SAFETY: `sysconf` is a pure query with no pointer arguments.
        let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if pages > 0 && page_size > 0 {
            return Some((pages as u64).saturating_mul(page_size as u64));
        }
        None
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Decide which disk-access backend to use for a Data.db file.
///
/// Pure function (system memory injected) so the [`DiskAccessMode::Auto`]
/// heuristic can be unit-tested deterministically. Resolution rules:
/// - explicit `Buffered`/`Mmap`/`Direct` are returned unchanged (the caller
///   applies graceful fallback if the OS refuses the backend);
/// - `Auto` returns `Buffered` for files below `mmap_min_size_bytes`, `Direct`
///   when the file exceeds `memory_fraction` of `system_memory` (and memory is
///   known and direct I/O is available on this platform), otherwise `Mmap`.
///
/// The deprecated `use_mmap` flag / `CQLITE_USE_MMAP` env is folded in by the
/// caller (promoting `Buffered` to `Mmap`), not an input here. `memory_fraction`
/// is CLAMPED only as defense in depth: callers REJECT it instead (#1696 F2).
pub(super) fn resolve_disk_access_mode(
    configured: DiskAccessMode,
    file_size: u64,
    mmap_min_size_bytes: u64,
    memory_fraction: f64,
    system_memory: Option<u64>,
    direct_io_available: bool,
) -> DiskAccessMode {
    // Zero-length files cannot be mapped and have nothing to read directly;
    // always use buffered I/O for them regardless of the requested mode.
    if file_size == 0 {
        return DiskAccessMode::Buffered;
    }
    match configured {
        DiskAccessMode::Buffered => DiskAccessMode::Buffered,
        DiskAccessMode::Mmap => DiskAccessMode::Mmap,
        DiskAccessMode::Direct => DiskAccessMode::Direct,
        DiskAccessMode::Auto => {
            if file_size < mmap_min_size_bytes {
                return DiskAccessMode::Buffered;
            }
            let fraction = if memory_fraction.is_finite() && memory_fraction > 0.0 {
                memory_fraction.min(1.0)
            } else {
                0.5
            };
            if direct_io_available {
                if let Some(mem) = system_memory {
                    let threshold = (mem as f64 * fraction) as u64;
                    if file_size > threshold {
                        return DiskAccessMode::Direct;
                    }
                }
            }
            DiskAccessMode::Mmap
        }
    }
}

/// Whether the direct-I/O backend is compiled in for this platform.
pub(super) const fn direct_io_available() -> bool {
    cfg!(all(
        unix,
        any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos"
        )
    ))
}

/// Resolve a [`PrefetchMode`] into the concrete advice the mmap backend should
/// issue, or `None` for "no advice".
///
/// `memmap2::Advice` / `Mmap::advise` (madvise) are Unix-only, so this and its
/// single call site are gated to `#[cfg(unix)]`. On non-Unix targets the mmap
/// backend simply issues no read-ahead advice.
///
/// # Policy: `Auto` issues `MADV_WILLNEED`, never `MADV_SEQUENTIAL` (#2824, #1143)
///
/// [`PrefetchMode::Auto`] issues `MADV_WILLNEED` on the scan mapping (issue
/// #2824). Cold page-in is the largest term in scan wall-time (measured 60.17
/// us/row, ~98% of wall-time variance, #2605); `MADV_WILLNEED` queues
/// **asynchronous** read-ahead for the mapping so those pages are faulted in by
/// the kernel rather than one synchronous major fault at a time on the reading
/// thread.
///
/// `MADV_SEQUENTIAL` is PROHIBITED here (issue #1143) and this arm must never be
/// changed to emit it. Its harm is **drop-behind**: the kernel aggressively
/// evicts pages *behind* the read cursor, so under concurrent write load the
/// just-dropped pages are gone when an overlapping scan re-reads them, the
/// re-reads take synchronous major page faults on the tokio worker thread, and
/// the read-side p99 tail regresses ~2x. `MADV_WILLNEED` has **no** drop-behind
/// semantics — it queues read-ahead and nothing else — so #1143's mechanism does
/// not transfer to it. The two advices are not interchangeable. Callers who
/// genuinely want drop-behind must still ask for it explicitly
/// (`CQLITE_PREFETCH=sequential` / [`StorageConfig::prefetch`]).
///
/// # Which mapping this advises
///
/// The sole production call site (`build_block_sources`) applies this to the
/// **scan** mapping, the one held by `ScanSource::Mapped` and reused by
/// `scan_positional_source` for the Summary-guided walk and the windowed scan
/// feed (#2876) — i.e. the hot scan plane. It is never applied to the dedicated
/// `MADV_RANDOM` point mapping (#2210). Below
/// `POINT_MMAP_MADV_RANDOM_MIN_BYTES` (8 MiB) there is no second mapping and the
/// point path shares this one; read-ahead over a file that small is cheap, which
/// is #2210's own reasoning for not building a separate mapping there.
///
/// A failed `madvise` is non-fatal and logged at the call site: opening an
/// SSTable never fails because the kernel declined an advisory hint.
///
/// [`StorageConfig::prefetch`]: crate::config::StorageConfig::prefetch
#[cfg(unix)]
pub(super) fn mmap_advice_for(prefetch: PrefetchMode) -> Option<memmap2::Advice> {
    match prefetch {
        // Explicitly disabled: rely on the kernel's default read-ahead only.
        PrefetchMode::Off => None,
        // Default. Asynchronous read-ahead, NO drop-behind (issue #2824); see
        // the #1143 prohibition above — this arm must never emit `Sequential`.
        PrefetchMode::Auto => Some(memmap2::Advice::WillNeed),
        // Explicit opt-in to aggressive read-ahead + drop-behind. Best for a
        // one-shot full scan that will not be re-read and should not pin the
        // whole file in the page cache.
        PrefetchMode::Sequential => Some(memmap2::Advice::Sequential),
        PrefetchMode::WillNeed => Some(memmap2::Advice::WillNeed),
    }
}
