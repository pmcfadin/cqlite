//! SSTable reader implementation
//!
//! This module provides efficient reading of SSTable files in Cassandra 5+ format.
//! It supports:
//! - Block-based reading with compression
//! - Index-based lookups for efficient queries
//! - Memory-efficient streaming
//! - Bloom filter integration
//! - Multiple compression algorithms

// Submodules
mod block_io;
mod cache;
/// Per-element / per-cell compaction read contract (epic #899, Phase A).
pub mod compaction_row;
mod component_loading;
mod compression;
/// `CRC.db` reader for uncompressed BIG read-time integrity (issue #1396).
pub(crate) mod crc;
mod data_access;
/// Delta-scan record model (Epic #696, Issue #697).
/// Only compiled when the `delta-scan` feature is enabled.
#[cfg(feature = "delta-scan")]
pub mod delta_scan;
mod header;
mod header_helpers;
mod integrity;
mod key_digest;
pub(crate) mod parsing; // Needs to be accessible from row_cell_state_machine
mod partition_lookup;
// Windowed streaming-scan driver (issue #1143); `pub` ONLY under non-default
// `scan-offload-probe` so the #1143 guard reaches its probe, else private.
#[cfg(not(feature = "scan-offload-probe"))]
mod scan_stream_windowed;
#[cfg(feature = "scan-offload-probe")]
pub mod scan_stream_windowed;
mod source;
#[cfg(test)]
mod tests;
mod types;
// Sliding-window byte cursor + its test-only byte-movement probe (issue #1589);
// `pub` ONLY under the non-default `scan-offload-probe` feature so the guard test
// reaches `window_cursor::probe`, else crate-private.
#[cfg(not(feature = "scan-offload-probe"))]
pub(crate) mod window_cursor;
#[cfg(feature = "scan-offload-probe")]
pub mod window_cursor;

// Re-export public types
pub use types::{
    BlockMeta, CachedBlock, IntegrityCheckResult, IntegrityStatus, SSTableReader,
    SSTableReaderConfig, SSTableReaderHealthMetrics, SSTableReaderStats,
};
// Re-export the within-partition clustering-slice push-down spec (Issue #954).
pub use data_access::ClusteringSlice;
// Re-export the per-element compaction read contract (epic #899, Phase A).
pub use compaction_row::{
    CompactionRow, CompactionRowData, ComplexColumn, ComplexElement, SimpleCell,
};
// Re-export V5CompressedLegacyParser for integration testing (Issue #166 regression tests)
#[doc(hidden)]
pub use parsing::PublicV5CompressedLegacyParser as V5CompressedLegacyParser;

// Re-export compression utilities for testing (Issue #202)
#[doc(hidden)]
pub use compression::extract_sstable_base_name;

// Internal imports from submodules
use compression::detect_and_initialize_compression;
use header::{
    calculate_actual_header_size, extract_generation_from_path, parse_header_with_version_detection,
};

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use source::{BlockSource, ScanSource};

use crate::{
    config::{DiskAccessMode, PrefetchMode},
    parser::{header::CassandraVersion, SSTableHeader, SSTableParser},
    platform::Platform,
    schema::TableSchema,
    storage::sstable::{
        compression_info::CompressionInfo,
        version_gate::{BigVersionGates, VersionGates},
    },
    Config, Error, Result, RowKey, ScanRow, Value,
};

// Structured logging
use log::debug;

#[cfg(feature = "tombstones")]
use super::tombstone_merger::TombstoneMerger;

/// Returns `true` when memory-mapped reads are force-enabled via the
/// `CQLITE_USE_MMAP` environment variable.
///
/// Accepts `1`, `true`, `yes`, `on` (case-insensitive). Any other value — or
/// an unset variable — leaves the decision to [`Config`]. This is an opt-in
/// escape hatch for ad-hoc local use without threading a custom config.
fn mmap_enabled_via_env() -> bool {
    std::env::var("CQLITE_USE_MMAP")
        .ok()
        .as_deref()
        .map(parse_truthy_env)
        .unwrap_or(false)
}

/// Parse a truthy environment-variable value (`1`/`true`/`yes`/`on`,
/// case-insensitive). Split out so it can be unit-tested without mutating the
/// process-global environment (which would race other `open()` tests).
fn parse_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Parse a [`DiskAccessMode`] from a string (`auto`/`buffered`/`mmap`/`direct`,
/// case-insensitive). Returns `None` for unrecognized values so callers can
/// keep the configured default. Pure for unit-testing without env mutation.
fn parse_disk_access_mode(value: &str) -> Option<DiskAccessMode> {
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
fn parse_prefetch_mode(value: &str) -> Option<PrefetchMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "no" => Some(PrefetchMode::Off),
        "sequential" | "seq" => Some(PrefetchMode::Sequential),
        "willneed" | "will_need" | "will-need" => Some(PrefetchMode::WillNeed),
        "auto" => Some(PrefetchMode::Auto),
        _ => None,
    }
}

/// `CQLITE_DISK_ACCESS_MODE` override, if set to a recognized value.
fn disk_access_mode_via_env() -> Option<DiskAccessMode> {
    std::env::var("CQLITE_DISK_ACCESS_MODE")
        .ok()
        .as_deref()
        .and_then(parse_disk_access_mode)
}

/// `CQLITE_PREFETCH` override, if set to a recognized value.
fn prefetch_mode_via_env() -> Option<PrefetchMode> {
    std::env::var("CQLITE_PREFETCH")
        .ok()
        .as_deref()
        .and_then(parse_prefetch_mode)
}

/// Best-effort total physical RAM in bytes, or `None` when it cannot be
/// determined on this platform. Used by [`DiskAccessMode::Auto`] to decide when
/// a file is large enough to warrant page-cache-bypassing direct I/O.
fn system_memory_bytes() -> Option<u64> {
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
/// caller (it promotes a `Buffered` request to `Mmap`), so it is not an input
/// here; an explicit mode always takes precedence.
fn resolve_disk_access_mode(
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
const fn direct_io_available() -> bool {
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
fn mmap_advice_for(prefetch: PrefetchMode) -> Option<memmap2::Advice> {
    match prefetch {
        // No madvise: rely on the kernel's default read-ahead. Chosen for `Auto`
        // to avoid `MADV_SEQUENTIAL` drop-behind evicting hot pages under
        // concurrent write load (issue #1143).
        PrefetchMode::Off | PrefetchMode::Auto => None,
        // Explicit opt-in to aggressive read-ahead + drop-behind. Best for a
        // one-shot full scan that will not be re-read and should not pin the
        // whole file in the page cache.
        PrefetchMode::Sequential => Some(memmap2::Advice::Sequential),
        PrefetchMode::WillNeed => Some(memmap2::Advice::WillNeed),
    }
}

/// Process-wide count of currently-open [`SSTableReader`]s, the source value
/// for the [`SSTABLES_OPEN`](crate::observability::catalog::SSTABLES_OPEN)
/// gauge. Tracked PER FORMAT so the gauge — which carries the
/// [`cqlite.sstable.format`](crate::observability::catalog::attr::SSTABLE_FORMAT)
/// attribute — reports the correct live count for each `big`/`bti` label series
/// instead of a single global total stamped onto every series. Incremented when
/// [`SSTableReader::open`] succeeds and decremented when a reader is dropped, so
/// each gauge series reflects live readers regardless of the `observability`
/// feature state (the helper calls are no-ops when off).
static SSTABLES_OPEN_COUNT_BIG: AtomicI64 = AtomicI64::new(0);
static SSTABLES_OPEN_COUNT_BTI: AtomicI64 = AtomicI64::new(0);

/// Select the per-format open-reader counter for a `sstable_format_label()`
/// value (`"big"` / `"bti"`). Defaults to the BIG counter for any unexpected
/// label so the value is never silently dropped; the label set is bounded to
/// the two [`sstable_format_label`](SSTableReader::sstable_format_label) returns.
fn sstables_open_count_for(format: &str) -> &'static AtomicI64 {
    match format {
        "bti" => &SSTABLES_OPEN_COUNT_BTI,
        _ => &SSTABLES_OPEN_COUNT_BIG,
    }
}

impl SSTableReader {
    /// Open an SSTable file for reading.
    ///
    /// Instrumented (epic #1031 / #1034): wraps the open in a
    /// `sstable.reader.open` span, increments the [`SSTABLES_OPEN`] gauge on
    /// success, and records an error on the `reader` subsystem when open fails.
    ///
    /// [`SSTABLES_OPEN`]: crate::observability::catalog::SSTABLES_OPEN
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        use crate::observability::{self as obs, catalog};
        use tracing::Instrument as _;

        let span = tracing::debug_span!(
            "sstable.reader.open",
            file_size = tracing::field::Empty,
            sstable_format = tracing::field::Empty,
        );

        // Instrument the future rather than holding an entered guard across the
        // `.await`: entering a span guard and then awaiting can attach unrelated
        // async work scheduled on this task to the span. `Instrument` enters the
        // span only while this specific future is polled.
        let result = Self::open_inner(path, config, platform)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(reader) => {
                let format = reader.sstable_format_label();
                span.record("file_size", reader.stats.file_size);
                span.record("sstable_format", format);
                // SSTABLES_OPEN is a snapshot gauge of the live reader count;
                // record the current PER-FORMAT count after this open succeeds so
                // the format-attributed gauge series stays correct under mixed
                // BIG/BTI readers.
                let now = sstables_open_count_for(format).fetch_add(1, Ordering::Relaxed) + 1;
                obs::record_gauge(
                    catalog::SSTABLES_OPEN,
                    now,
                    &[(catalog::attr::SSTABLE_FORMAT, format.into())],
                );
            }
            Err(e) => {
                // Record the error WHILE the open span is current so
                // `mark_span_error` marks THIS `sstable.reader.open` span. The
                // instrumented future has already completed here, so the span is
                // no longer entered; `in_scope` re-enters it for the duration of
                // the error-recording call.
                span.in_scope(|| obs::record_error(e, "reader"));
            }
        }
        result
    }

    /// Open implementation; see [`open`](Self::open) for the instrumented wrapper.
    async fn open_inner(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        // Retain the open-time Config before any local `config` shadowing so
        // `perform_integrity_check` can delegate to `verify::verify_sstable`
        // (single source of truth, issue #1283) under the same config.
        let open_config = config.clone();
        // #1249 (spec R1): reject below-floor versions BEFORE any file I/O.
        // Gates derive solely from the filename, so this is the earliest point
        // that can enforce the na+ floor — a pre-`na` (BIG) or non-`da` (BTI)
        // descriptor is rejected with a typed `UnsupportedVersion` before we
        // open/mmap/read the body (it never surfaces an I/O/parse error). Only a
        // structurally-unparseable descriptor falls back to nb-compatible BIG
        // gates (preserving existing tolerance for odd-but-5.0 unit-test paths);
        // the gates do not change parsing decisions until VG3 flips behaviour.
        let version_gates = Arc::new(match VersionGates::from_path(path) {
            Ok(gates) => gates,
            // A parsed-but-below-floor version is FATAL — never degrade it.
            Err(e @ Error::UnsupportedVersion { .. }) => return Err(e),
            Err(e) => {
                log::debug!(
                    "SSTableReader::open: could not derive VersionGates from {:?} ({}); \
                     defaulting to nb-compatible BIG gates",
                    path,
                    e
                );
                VersionGates::Big(BigVersionGates::nb_fallback())
            }
        });

        // Resolve the disk-access backend (buffered / mmap / direct, or auto)
        // from config + environment overrides. See `Config::storage.disk_access_mode`.
        let mut reader_config = SSTableReaderConfig::default();
        reader_config.use_mmap = config.storage.use_mmap || mmap_enabled_via_env();
        reader_config.mmap_min_size_bytes = config.storage.mmap_min_size_bytes;

        let file_size = tokio::fs::metadata(path).await?.len();

        // The explicit mode comes from env > config. The legacy `use_mmap` flag
        // is folded in by promoting an otherwise-`Buffered` request to `Mmap` so
        // the old opt-in keeps working (`Auto` and explicit non-buffered modes
        // are left untouched, so a real backend decision always wins).
        let configured_mode = disk_access_mode_via_env().unwrap_or(config.storage.disk_access_mode);
        let configured_mode =
            if reader_config.use_mmap && matches!(configured_mode, DiskAccessMode::Buffered) {
                DiskAccessMode::Mmap
            } else {
                configured_mode
            };

        let resolved_mode = resolve_disk_access_mode(
            configured_mode,
            file_size,
            reader_config.mmap_min_size_bytes as u64,
            config.storage.direct_io_memory_fraction,
            system_memory_bytes(),
            direct_io_available(),
        );

        // Build both the shared point-read source and the per-scan factory from
        // the same backend decision so concurrent scans get independent cursors
        // (issue #815). `ScanSource::Mapped` shares the same `Arc<Mmap>` so no
        // extra mapping is created per scan. Every non-buffered backend degrades
        // gracefully to buffered I/O if the OS/filesystem refuses it.
        let prefetch = prefetch_mode_via_env().unwrap_or(config.storage.prefetch);
        let (source, scan_source) = Self::build_block_sources(
            path,
            file_size,
            resolved_mode,
            prefetch,
            config.storage.direct_io_prefetch_bytes,
        )
        .await?;
        let file = Arc::new(Mutex::new(source));

        // Parse header - read available bytes, not a fixed size
        // NOTE: For NB format files (Cassandra 4.x+), Data.db often contains compressed row data
        // with no embedded header. The header.rs module detects this via filename pattern and
        // returns a minimal header loaded from Statistics.db instead.
        let header_size = std::cmp::min(4096, file_size as usize);
        let mut header_buffer = vec![0u8; header_size];
        {
            let mut file_guard = file.lock().await;
            let bytes_read = file_guard.read(&mut header_buffer).await?;
            header_buffer.truncate(bytes_read);
        }

        // VG5 / Issue #831 / #909: BTI ("da") read support.
        //
        // BTI SSTables use a Partitions.db trie + Rows.db row index instead of
        // Index.db/Summary.db. We load BOTH (tiny) tries fully into memory here so
        // the point-lookup path (`lookup_partition_via_bti_trie` /
        // `bti_point_lookup`) can walk them for O(log n) partition resolution:
        //   - Partitions.db resolves a partition key to either a direct Data.db
        //     offset (NARROW partition) or a positive RowsOffset (WIDE partition).
        //   - Rows.db, indexed by that RowsOffset, recovers the wide partition's
        //     Data.db position (issue #909/#910).
        //
        // Loading Partitions.db + Rows.db is the ONLY BTI-specific step in open():
        // the rest of the flow (header / compression / Statistics-driven schema)
        // tolerates the absent Index.db/Summary.db gracefully (those loaders
        // return None).
        let (bti_partitions_db, bti_rows_db) = if matches!(*version_gates, VersionGates::Bti(_)) {
            let base = extract_sstable_base_name(path).ok_or_else(|| {
                Error::unsupported_format(format!(
                    "BTI (da) SSTable '{}' has a non-standard filename; cannot derive the \
                         sibling Partitions.db name required for trie point lookup (#831).",
                    path.display()
                ))
            })?;
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let partitions_path = parent.join(format!("{}-Partitions.db", base));
            let partitions_bytes = tokio::fs::read(&partitions_path).await.map_err(|e| {
                Error::unsupported_format(format!(
                    "BTI (da) SSTable '{}' is missing its sibling Partitions.db trie \
                         (expected '{}'): {}. BTI read support requires Partitions.db for \
                         partition-key point lookup (#831).",
                    path.display(),
                    partitions_path.display(),
                    e
                ))
            })?;

            // Rows.db carries the within-partition row index for WIDE
            // partitions (issue #909/#910). It is ALWAYS emitted for a BTI
            // SSTable (possibly 0 bytes for a narrow-only table), so a missing
            // Rows.db is a structural error. A 0-byte file is valid: no
            // partition resolved to a positive RowsOffset, so the point-lookup
            // path never indexes into it.
            let rows_path = parent.join(format!("{}-Rows.db", base));
            let rows_bytes = tokio::fs::read(&rows_path).await.map_err(|e| {
                Error::unsupported_format(format!(
                    "BTI (da) SSTable '{}' is missing its sibling Rows.db row-index trie \
                         (expected '{}'): {}. BTI read support requires Rows.db to resolve \
                         wide-partition point lookups (#909/#910).",
                    path.display(),
                    rows_path.display(),
                    e
                ))
            })?;

            (Some(Arc::new(partitions_bytes)), Some(Arc::new(rows_bytes)))
        } else {
            let none: Option<Arc<Vec<u8>>> = None;
            (none.clone(), none)
        };

        use tracing::Instrument;

        let config = crate::cql::config::ParserConfig::default();
        let parser = SSTableParser::new(config)?;
        // Parse the header using enhanced version detection - strict error propagation.
        // VersionGates are passed so VG3 can flip version-sensitive parsing decisions
        // inside header parsing without re-deriving gates from the filename.
        let header = parse_header_with_version_detection(&header_buffer, path, &version_gates)
            .instrument(tracing::debug_span!("sstable.reader.open.header_parse"))
            .await
            .map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse SSTable header for file '{}': {}. This indicates either \
                     file corruption or an unsupported SSTable format. File size: {} bytes, \
                     header buffer size: {} bytes.",
                    path.display(),
                    e,
                    file_size,
                    header_buffer.len()
                ))
            })?;
        let header_size = calculate_actual_header_size(&header, &header_buffer)?;

        // Schema extraction deferred until after Statistics.db columns are loaded (Issue #163)
        // See schema extraction code after statistics_reader loading below

        // Seek to start of data section
        {
            let mut file_guard = file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Initialize compression reader with improved format detection
        let compression_reader = detect_and_initialize_compression(&header, path).await?;

        // Load CompressionInfo.db for chunked decompression (if it exists)
        let compression_info = Self::load_compression_info_metadata(path, &platform).await?;

        // Load CRC.db per-chunk checksums for uncompressed BIG read-time integrity
        // (issue #1396). Only for uncompressed tables (compression_info None);
        // compressed tables carry inline per-chunk CRCs instead.
        let crc_reader = if compression_info.is_none() {
            Self::load_crc_reader(path, &header, file_size).await?
        } else {
            None
        };

        // Pre-validate component architecture for better error handling
        let components = Self::detect_component_files(path).await?;
        if !components.is_empty() {
            let integrity_issues = Self::validate_component_integrity(path, &components).await?;
            if !integrity_issues.is_empty() {
                log::warn!(
                    "Component integrity issues detected but proceeding with loading: {:?}",
                    integrity_issues
                );
            }
        }

        // Load index if available (supports both integrated and component-based)
        let index = Self::load_index(&file, &header, &platform, path)
            .instrument(tracing::debug_span!("sstable.reader.open.load_index"))
            .await?;

        // Load bloom filter if available (supports both integrated and component-based)
        let bloom_filter = Self::load_bloom_filter(&file, &header, &platform, path)
            .instrument(tracing::debug_span!("sstable.reader.open.load_bloom"))
            .await?;

        // Load spec readers for enhanced metadata and lookups
        let index_reader = Self::load_index_reader(path, &platform)
            .instrument(tracing::debug_span!(
                "sstable.reader.open.load_index_reader"
            ))
            .await;
        let summary_reader = Self::load_summary_reader(path, &platform)
            .instrument(tracing::debug_span!("sstable.reader.open.load_summary"))
            .await;
        let statistics_reader = Self::load_statistics_reader(path, &platform)
            .instrument(tracing::debug_span!("sstable.reader.open.load_statistics"))
            .await?;

        // Extract SerializationHeader columns from Statistics.db (Issue #163)
        // This enables schema extraction for V5CompressedLegacy format
        let mut header = header; // Make mutable to populate columns
        if let Some(ref stats_reader) = statistics_reader {
            let statistics = stats_reader.statistics();
            let partition_columns = &statistics.serialization_header_partition_keys;
            let clustering_columns = &statistics.serialization_header_clustering_keys;
            let regular_columns = &statistics.serialization_header_columns;

            if !partition_columns.is_empty()
                || !clustering_columns.is_empty()
                || !regular_columns.is_empty()
            {
                log::debug!(
                    "Populating header columns from Statistics.db SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
                    partition_columns.len(),
                    clustering_columns.len(),
                    regular_columns.len()
                );

                let mut merged_columns = Vec::with_capacity(
                    partition_columns.len() + clustering_columns.len() + regular_columns.len(),
                );
                merged_columns.extend_from_slice(partition_columns);
                merged_columns.extend_from_slice(clustering_columns);
                merged_columns.extend_from_slice(regular_columns);

                header.columns = merged_columns;
            }
        }

        // Extract schema from header for V5.0+ formats (after columns are populated)
        let schema = if matches!(
            header.cassandra_version,
            CassandraVersion::V5_0NewBig
                | CassandraVersion::V5_0Bti
                | CassandraVersion::V5_0DataFormat
                | CassandraVersion::V5_0FormatC
                | CassandraVersion::V5_0FormatD
                | CassandraVersion::V5_0FormatE
                | CassandraVersion::V5_0FormatF
                | CassandraVersion::V5_0FormatG
        ) {
            match TableSchema::from_sstable_header(&header) {
                Ok(s) => {
                    log::debug!(
                        "Extracted schema from SSTable header: {}.{} ({} columns, {} partition keys, {} clustering keys)",
                        s.keyspace,
                        s.table,
                        s.columns.len(),
                        s.partition_keys.len(),
                        s.clustering_keys.len()
                    );
                    Some(Arc::new(s))
                }
                Err(e) => {
                    log::warn!(
                        "Failed to extract schema from SSTable header for {}: {}. Schema-aware parsing will not be available.",
                        path.display(),
                        e
                    );
                    None
                }
            }
        } else {
            // Legacy formats don't have schema in header
            None
        };

        // Derive block_count from CompressionInfo.db when available — this is the
        // authoritative source for compressed SSTables (no-heuristics mandate #28).
        // Each entry in chunk_offsets corresponds to one compressed block in Data.db.
        let block_count = compression_info
            .as_ref()
            .map(|ci| ci.chunk_offsets.len() as u64)
            .unwrap_or(0);

        let stats = SSTableReaderStats {
            file_size,
            entry_count: header.stats.row_count,
            table_count: 1, // Will be updated as we discover tables
            block_count,
            index_size: 0,        // Will be updated if index is loaded
            bloom_filter_size: 0, // Will be updated if bloom filter is loaded
            compression_ratio: header.stats.compression_ratio,
            cache_hit_rate: 0.0,
        };

        // Extract generation from filename or use default
        let generation = extract_generation_from_path(path);

        Ok(Self {
            file_path: path.to_path_buf(),
            file,
            scan_source,
            header,
            parser,
            index,
            bloom_filter,
            compression_reader,
            block_meta_cache: HashMap::new(),
            block_cache: HashMap::new(),
            config: reader_config,
            open_config,
            platform,
            stats,
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            #[cfg(feature = "tombstones")]
            tombstone_merger: TombstoneMerger::new(),
            generation,
            actual_header_size: header_size,
            index_reader,
            summary_reader,
            statistics_reader,
            schema_registry: None, // Will be set by set_schema_registry() after construction
            schema,
            udt_registry: None, // Will be set when available for UDT-aware parsing
            compression_info: compression_info.map(Arc::new),
            crc_reader,
            verified_uncompressed_chunks: std::sync::Mutex::new(std::collections::HashSet::new()),
            version_gates,
            bti_partitions_db,
            bti_rows_db,
            bti_partition_offsets: std::sync::OnceLock::new(),
        })
    }

    /// Whether this reader's block source is backed by a memory map.
    ///
    /// Test-only hook used to verify that the `use_mmap` config / env wiring
    /// actually selects the intended backend end-to-end.
    #[cfg(test)]
    pub(crate) async fn is_mmap_backed(&self) -> bool {
        self.file.lock().await.is_mmap()
    }

    /// Whether this reader's block source is backed by direct I/O.
    ///
    /// Test-only hook used to verify the `disk_access_mode` config / env wiring
    /// selects the direct-I/O backend (issue: directio prefetch config).
    #[cfg(all(test, unix))]
    pub(crate) async fn is_direct_backed(&self) -> bool {
        self.file.lock().await.is_direct()
    }

    /// Open a buffered point-read source and its per-scan factory. Shared by the
    /// buffered backend and as the graceful fallback for mmap/direct.
    async fn open_buffered_sources(path: &Path) -> Result<(BlockSource, ScanSource)> {
        Ok((
            BlockSource::buffered(File::open(path).await?),
            ScanSource::Buffered,
        ))
    }

    /// Build the shared point-read [`BlockSource`] and the per-scan
    /// [`ScanSource`] for a resolved [`DiskAccessMode`].
    ///
    /// `prefetch` selects read-ahead advice for mmap and (together with
    /// `prefetch_bytes`) the direct-I/O read-ahead window. Each non-buffered
    /// backend degrades gracefully to buffered I/O if the OS or filesystem
    /// refuses it (mmap can fail on network mounts; direct I/O is unsupported on
    /// some platforms/filesystems), so opening never fails purely because a
    /// faster backend is unavailable.
    async fn build_block_sources(
        path: &Path,
        file_size: u64,
        mode: DiskAccessMode,
        prefetch: PrefetchMode,
        prefetch_bytes: usize,
    ) -> Result<(BlockSource, ScanSource)> {
        // With prefetch off, use the minimal aligned read-ahead the direct
        // backend still requires (a single block, rounded up inside the cursor).
        let direct_window = if matches!(prefetch, PrefetchMode::Off) {
            1
        } else {
            prefetch_bytes
        };
        match mode {
            DiskAccessMode::Buffered => Self::open_buffered_sources(path).await,
            DiskAccessMode::Mmap => match Self::map_file(path) {
                Ok(mmap) => {
                    log::debug!(
                        "Opened {} via memory map ({} bytes)",
                        path.display(),
                        file_size
                    );
                    // Best-effort read-ahead advice (Unix-only; madvise has no
                    // Windows equivalent here). Failure is non-fatal.
                    #[cfg(unix)]
                    if let Some(advice) = mmap_advice_for(prefetch) {
                        if let Err(e) = mmap.advise(advice) {
                            log::debug!(
                                "madvise({:?}) on {} failed: {}",
                                advice,
                                path.display(),
                                e
                            );
                        }
                    }
                    let mmap = Arc::new(mmap);
                    Ok((BlockSource::mapped(mmap.clone()), ScanSource::Mapped(mmap)))
                }
                Err(e) => {
                    log::warn!(
                        "Memory-mapping {} failed ({}); falling back to buffered I/O",
                        path.display(),
                        e
                    );
                    Self::open_buffered_sources(path).await
                }
            },
            DiskAccessMode::Direct => {
                #[cfg(unix)]
                {
                    match source::DirectCursor::open(path, direct_window) {
                        Ok(cursor) => {
                            log::debug!(
                                "Opened {} via direct I/O ({} bytes, {}-byte window)",
                                path.display(),
                                file_size,
                                direct_window
                            );
                            Ok((
                                BlockSource::direct(cursor),
                                ScanSource::Direct {
                                    window: direct_window,
                                },
                            ))
                        }
                        Err(e) => {
                            log::warn!(
                                "Direct I/O on {} failed ({}); falling back to buffered I/O",
                                path.display(),
                                e
                            );
                            Self::open_buffered_sources(path).await
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = direct_window;
                    log::warn!(
                        "Direct I/O is unavailable on this platform; using buffered I/O for {}",
                        path.display()
                    );
                    Self::open_buffered_sources(path).await
                }
            }
            // `resolve_disk_access_mode` never yields `Auto`; handle defensively.
            DiskAccessMode::Auto => Self::open_buffered_sources(path).await,
        }
    }

    /// Memory-map an SSTable file read-only.
    ///
    /// # Safety / correctness
    ///
    /// The returned [`Mmap`](memmap2::Mmap) aliases the file's bytes for its
    /// entire lifetime. SSTables are immutable once written, and CQLite treats
    /// them as read-only inputs, so this matches Cassandra's own mmap read
    /// strategy. Mutating the underlying file while a reader is open is
    /// undefined behaviour — callers must not do so.
    ///
    /// Note that only the initial mapping is fallible here. Once mapped, a later
    /// page fault — caused by truncation, deletion, or a network/overlay
    /// filesystem hiccup — raises `SIGBUS` and **cannot** be recovered as an
    /// `io::Error`. This is why mmap is opt-in and gated on immutable local
    /// files; see [`Config`]'s `storage.use_mmap` for the full constraints.
    fn map_file(path: &Path) -> Result<memmap2::Mmap> {
        let std_file = std::fs::File::open(path)?;
        // SAFETY: read-only mapping of a file assumed immutable for the
        // reader's lifetime; see the function-level note above.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std_file)? };
        Ok(mmap)
    }

    /// Load CompressionInfo.db metadata for chunked reading
    async fn load_compression_info_metadata(
        path: &Path,
        _platform: &Arc<Platform>,
    ) -> Result<Option<CompressionInfo>> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;

        // Try to find CompressionInfo.db in same directory.
        let parent_dir = path.parent().unwrap_or(Path::new("."));
        // Derive the base name via the descriptor parser, which handles
        // hyphenated-UUID ids (e.g. "da-00000000-0000-0000-0000-000000000001-bti").
        // A fixed parts[0..3] split mangles those and looks for the wrong
        // "*-CompressionInfo.db", silently treating compressed data as
        // uncompressed (roborev #970).
        let base_name = crate::storage::sstable::version_gate::SsTableDescriptor::parse(path)
            .ok()
            .map(|d| format!("{}-{}-{}", d.version, d.sstable_id, d.format.as_str()));

        if let Some(base) = base_name {
            let compression_info_path = parent_dir.join(format!("{}-CompressionInfo.db", base));
            if compression_info_path.exists() {
                let mut file = File::open(&compression_info_path).await?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await?;

                // Fail-fast (issue #1001): a CompressionInfo.db that is present but
                // names an unknown/unsupported compressor (or is otherwise malformed)
                // MUST hard-error at reader-open time, BEFORE any Data.db chunk is read —
                // never silently fall back to the uncompressed path. The error carries
                // the offending component path for diagnosis. A genuinely uncompressed
                // SSTable has no CompressionInfo.db at all and returns Ok(None) below.
                let info = CompressionInfo::parse(&buffer).map_err(|e| {
                    Error::UnsupportedFormat(format!(
                        "Failed to parse CompressionInfo.db at {}: {}",
                        compression_info_path.display(),
                        e
                    ))
                })?;
                log::debug!(
                    "Loaded CompressionInfo: algorithm={}, chunk_length={}, chunks={}",
                    info.algorithm,
                    info.chunk_length,
                    info.chunk_offsets.len()
                );
                return Ok(Some(info));
            }
        }

        Ok(None)
    }

    /// Load the `CRC.db` per-chunk checksum sidecar for read-time integrity of
    /// **uncompressed** BIG SSTables (issue #1396).
    ///
    /// Called only when `compression_info` is `None` (compressed tables carry
    /// inline per-chunk CRCs and no `CRC.db`). Returns:
    /// - `Some(crc)` for an uncompressed BIG SSTable that ships a `CRC.db`
    ///   (Cassandra writes one for every uncompressed BIG table) — verification is
    ///   then default-on for every uncompressed chunk read.
    /// - `None` for BTI (`da`) tables (Cassandra emits no `CRC.db`) or an
    ///   uncompressed BIG table whose `CRC.db` is absent. The absent case is the
    ///   owner-pinned **warn-and-proceed** decision (design D4): a `log::warn!` is
    ///   emitted so the missing integrity component is visible, and the read
    ///   proceeds unverified rather than hard-failing.
    ///
    /// A present-but-malformed `CRC.db` is a hard [`Error`] at open time (never a
    /// silent fall-through to unverified reads), mirroring the CompressionInfo.db
    /// fail-fast posture (#1001).
    async fn load_crc_reader(
        path: &Path,
        header: &SSTableHeader,
        data_len: u64,
    ) -> Result<Option<Arc<crc::CrcDb>>> {
        // BTI (`da`) never ships a CRC.db; nothing to load.
        if matches!(header.cassandra_version, CassandraVersion::V5_0Bti) {
            return Ok(None);
        }

        let parent_dir = path.parent().unwrap_or(Path::new("."));
        let Some(base) = compression::extract_sstable_base_name(path) else {
            return Ok(None);
        };
        let crc_path = parent_dir.join(format!("{}-CRC.db", base));
        if !crc_path.exists() {
            // Absent CRC.db on an uncompressed BIG SSTable: warn-and-proceed
            // (owner-pinned, design D4). Cassandra 5.0 writes a CRC.db for every
            // uncompressed BIG SSTable, so its absence is notable but not fatal —
            // reads proceed unverified rather than hard-failing.
            log::warn!(
                "CRC.db absent for uncompressed SSTable {} — proceeding without \
                 read-time per-chunk CRC verification (warn-and-proceed, issue #1396)",
                path.display()
            );
            return Ok(None);
        }

        let crc = crc::CrcDb::open(&crc_path, data_len).await.map_err(|e| {
            Error::corruption(format!(
                "Failed to parse CRC.db at {}: {}",
                crc_path.display(),
                e
            ))
        })?;
        log::debug!(
            "Loaded CRC.db for {}: chunk_size={}, chunks={}",
            path.display(),
            crc.chunk_size(),
            crc.chunk_count()
        );
        Ok(Some(Arc::new(crc)))
    }

    /// Set the schema registry for schema-driven operations
    #[cfg(feature = "state_machine")]
    pub fn set_schema_registry(
        &mut self,
        schema_registry: Arc<tokio::sync::RwLock<crate::schema::SchemaRegistry>>,
    ) {
        self.schema_registry = Some(schema_registry);
        log::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Set the schema registry for schema-driven operations (non-state_machine builds)
    #[cfg(not(feature = "state_machine"))]
    pub fn set_schema_registry(&mut self, schema_registry: Arc<crate::schema::SchemaRegistry>) {
        self.schema_registry = Some(schema_registry);
        log::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Set the UDT registry for UDT-aware parsing in collections
    ///
    /// This enables proper parsing of UDTs inside collections (List, Set, Map)
    /// by providing the UDT field definitions needed for nested type resolution.
    pub fn set_udt_registry(&mut self, registry: crate::schema::UdtRegistry) {
        self.udt_registry = Some(registry);
        log::debug!(
            "UDT registry set for {}.{} - enabling UDT-aware collection parsing",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Get reader statistics
    pub async fn stats(&self) -> Result<&SSTableReaderStats> {
        Ok(&self.stats)
    }

    /// Close the reader and release resources
    pub async fn close(mut self) -> Result<()> {
        debug!("Closing SSTable reader for {:?}", self.file_path);

        // Clear caches and log cache statistics
        let cache_entries = self.block_cache.len();
        let meta_entries = self.block_meta_cache.len();

        self.block_cache.clear();
        self.block_meta_cache.clear();

        debug!(
            "Cleared {} block cache entries and {} metadata entries",
            cache_entries, meta_entries
        );

        // File will be closed automatically when dropped
        Ok(())
    }

    /// Calculate header size based on format and actual header content
    pub fn calculate_header_size(&self) -> usize {
        self.actual_header_size
    }

    /// Get the Cassandra version from the SSTable header
    pub fn cassandra_version(&self) -> CassandraVersion {
        self.header.cassandra_version
    }

    /// Low-cardinality on-disk format family label for telemetry attributes
    /// (`"bti"` or `"big"`). Derived from the authoritative [`VersionGates`], so
    /// it stays bounded to exactly two values — safe to attach to metrics. NOT a
    /// substitute for [`format_version`](Self::format_version), which returns the
    /// exact on-disk version token.
    pub(crate) fn sstable_format_label(&self) -> &'static str {
        match *self.version_gates {
            VersionGates::Bti(_) => "bti",
            VersionGates::Big(_) => "big",
        }
    }

    /// Whether the on-disk `DeletionTime` uses the Cassandra 5.0 UNSIGNED
    /// `localDeletionTime` serializer (`oa`/`da`, `hasUIntDeletionTime`) rather
    /// than the legacy SIGNED `i32` form (`nb`).
    ///
    /// The verifier uses this to interpret a raw partition-level
    /// `localDeletionTime`: on the legacy signed form a NEGATIVE value (that is
    /// not the `i32::MAX` live sentinel) is unambiguously corrupt, whereas on the
    /// unsigned form a value in `[2^31, 2^32)` is a legitimate far-future
    /// deletion time and must NOT be flagged (no heuristics — the format decides).
    ///
    /// Authority: `BigFormat.java:409` (`hasUIntDeletionTime`), `DeletionTime.java`.
    pub fn has_uint_deletion_time(&self) -> bool {
        match *self.version_gates {
            VersionGates::Big(ref g) => g.has_uint_deletion_time,
            VersionGates::Bti(ref g) => g.has_uint_deletion_time,
        }
    }

    /// Get the SSTable format version string
    pub fn format_version(&self) -> Result<String> {
        let filename = self
            .file_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| {
                Error::InvalidPath(format!("Invalid SSTable filename: {:?}", self.file_path))
            })?;

        let parts: Vec<&str> = filename.split('-').collect();
        if parts.is_empty() {
            return Err(Error::InvalidFormat(format!(
                "Cannot extract format version from filename: {}",
                filename
            )));
        }

        Ok(parts[0].to_string())
    }

    /// Get a reference to the SSTable header
    pub fn header(&self) -> &SSTableHeader {
        &self.header
    }

    /// Get the table schema extracted from the SSTable header
    ///
    /// Returns `None` for legacy formats or if schema extraction failed.
    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_deref()
    }

    /// The effective table schema the read/verify paths decode with — the same
    /// four-tier resolution `partition_clustering_verify_scan` uses (issue #1282).
    ///
    /// Returned owned because the registry-fallback tier constructs a schema; the
    /// verifier needs it to drive the authoritative clustering comparator
    /// ([`crate::storage::write_engine::mutation::ClusteringKey::compare`]).
    pub fn effective_schema(&self) -> Option<TableSchema> {
        self.get_table_schema(None)
    }

    /// Extract write time from entry metadata
    pub fn extract_write_time_from_entry(&self, _key: &RowKey, row: &ScanRow) -> i64 {
        use log::warn;

        match row {
            ScanRow::Marker(Value::Tombstone(info)) => info.deletion_time,
            _ => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or_else(|e| {
                    warn!("Failed to get system time: {}; using fallback value 0", e);
                    0
                }),
        }
    }
}

impl Drop for SSTableReader {
    fn drop(&mut self) {
        use crate::observability::{self as obs, catalog};
        // Keep the per-format live-reader count and the SSTABLES_OPEN gauge in
        // sync as readers are released. Use a single atomic read-modify-write
        // (`fetch_sub`) on the SAME per-format counter `open()` incremented: a
        // load-then-store would race concurrent drops and lose decrements,
        // drifting the gauge permanently high. `open()` only ever increments on
        // success, so this never underflows in practice.
        let format = self.sstable_format_label();
        let now = sstables_open_count_for(format).fetch_sub(1, Ordering::Relaxed) - 1;
        obs::record_gauge(
            catalog::SSTABLES_OPEN,
            now,
            &[(catalog::attr::SSTABLE_FORMAT, format.into())],
        );
    }
}

impl std::fmt::Debug for SSTableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSTableReader")
            .field("file_path", &self.file_path)
            .field("header", &self.header)
            .field("has_index", &self.index.is_some())
            .field("has_bloom_filter", &self.bloom_filter.is_some())
            .field("compression", &self.header.compression.algorithm)
            .field("stats", &self.stats)
            .finish()
    }
}

/// Helper function to create a reader with default configuration
pub async fn open_sstable_reader(
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<SSTableReader> {
    SSTableReader::open(path, config, platform).await
}
