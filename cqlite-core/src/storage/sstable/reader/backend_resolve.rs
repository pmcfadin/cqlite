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
/// issue **AT OPEN**, or `None` for "no advice".
///
/// OPEN-TIME only, and that is the whole contract (issue #3853): every mode
/// except [`PrefetchMode::Sequential`] now yields `None` here.
/// [`PrefetchMode::WillNeed`] used to map to [`memmap2::Advice::WillNeed`] and
/// be applied at reader open, which meant a reader that was opened and never
/// scanned paid a full-file read-ahead and never had it withdrawn. That advice
/// moved to SCAN START, where it is paired with a `MADV_DONTNEED` when the last
/// in-flight scan on the reader ends — see [`super::scan_lifetime`]. It is NOT a
/// policy change for any other mode: `Auto` still issues nothing (#1143) and
/// `Sequential` keeps its open-time advice, because that mode is an explicit
/// opt-in to read-ahead WITH drop-behind for a one-shot scan, a different
/// contract from `WillNeed`'s "make it resident while I am scanning".
///
/// `memmap2::Advice` / `Mmap::advise` (madvise) are Unix-only, so this and its
/// single call site are gated to `#[cfg(unix)]`. On non-Unix targets the mmap
/// backend simply issues no read-ahead advice.
///
/// [`PrefetchMode::Auto`] deliberately issues **no** madvise (issue #1143).
/// `MADV_SEQUENTIAL` couples aggressive read-ahead with *drop-behind*: pages are
/// evicted from the page cache as soon as the scan moves past them. In isolation
/// that is fine (mmap scans are ~40% faster than buffered), but under concurrent
/// write load the page-cache pressure means the just-dropped pages are gone by
/// the time an overlapping scan needs them again, so re-reads take *synchronous*
/// major page faults on the tokio worker thread and the read-side p99 tail blows
/// up (~2x regression). Relying on the kernel's default read-ahead (no
/// drop-behind) keeps the isolated mmap win while letting the page cache retain
/// hot pages, which collapses that tail. Callers who genuinely want the
/// drop-behind behaviour can still request `Sequential` explicitly
/// (`CQLITE_PREFETCH=sequential` / [`StorageConfig::prefetch`]).
#[cfg(unix)]
pub(super) fn mmap_open_advice_for(prefetch: PrefetchMode) -> Option<memmap2::Advice> {
    match prefetch {
        // No madvise: rely on the kernel's default read-ahead. Chosen for `Auto`
        // to avoid `MADV_SEQUENTIAL` drop-behind evicting hot pages under
        // concurrent write load (issue #1143).
        PrefetchMode::Off | PrefetchMode::Auto => None,
        // Explicit opt-in to aggressive read-ahead + drop-behind. Best for a
        // one-shot full scan that will not be re-read and should not pin the
        // whole file in the page cache.
        PrefetchMode::Sequential => Some(memmap2::Advice::Sequential),
        // Issue #3853: no OPEN-time advice. `MADV_WILLNEED` for this mode is
        // issued by `scan_lifetime` when the first scan on the reader begins.
        PrefetchMode::WillNeed => None,
    }
}
